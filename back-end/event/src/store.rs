use crate::{
    types::{DottedVersionVector, Event, EventError, EventPayload},
    validation::EventValidator,
};
use sha2::Digest;
use sled::{Db, Tree};
use std::{path::Path, sync::Arc};
use tracing::{Span, debug, error, info, instrument, warn};

const METADATA_TREE: &[u8] = b"metadata";
const EVENTS_TREE: &[u8] = b"events";

const HEAD_KEY: &[u8] = b"head_offset";
const CAUSALITY_KEY: &[u8] = b"causality_dvv";
const PREV_HASH_KEY: &[u8] = b"prev_hash";

/// A durable, indexed, append-only event store powered by Sled.
pub struct EventStore {
    db: Db,
    metadata: Tree,
    events: Tree,
    validator: Arc<EventValidator>,
    stream_id: String,
    space_id: String,
}

impl EventStore {
    /// Opens or creates a new event store at the given path.
    #[instrument(skip(path, validator), fields(stream_id, space_id))]
    pub fn new<P: AsRef<Path>>(
        path: P,
        stream_id: String,
        space_id: String,
        validator: EventValidator,
    ) -> Result<Self, EventError> {
        let path_str = path.as_ref().display().to_string();

        info!(
            path = %path_str, stream_id = %stream_id, space_id = %space_id,
            "Opening event store"
        );

        let db = sled::open(path).map_err(|e| {
            error!(path = %path_str, error = %e, "Failed to open Sled database");
            e
        })?;

        let metadata = db.open_tree(METADATA_TREE)?;
        let events = db.open_tree(EVENTS_TREE)?;

        let event_count = events.len();
        info!(
            path = %path_str,
            stream_id = %stream_id,
            space_id = %space_id,
            event_count = event_count,
            "Event store opened successfully"
        );

        Ok(Self {
            db,
            metadata,
            events,
            validator: Arc::new(validator),
            stream_id,
            space_id,
        })
    }

    pub fn db(&self) -> &Db {
        &self.db
    }

    /// Appends a new event to the store in a single atomic transaction.
    ///
    ///
    /// This method:
    /// 1. Validates the payload against its schema (outside transaction)
    /// 2. Atomically reads current state, creates event, and persists it
    /// 3. Returns the created event with its assigned ID and metadata
    #[instrument(
        skip(self, payload),
        fields(
            event_type = P::TYPE,
            schema_version = P::VERSION,
            signer_did,
            event_id,
            offset
        )
    )]
    pub fn append<P: EventPayload>(
        &self,
        signer_did: &str,
        payload: P,
    ) -> Result<Event, EventError> {
        Span::current().record("signer_did", signer_did);

        debug!("Serializing Event payload");
        let payload_value = serde_json::to_value(&payload)?;
        debug!("Validating Event payload");
        self.validator
            .validate(&payload_value, P::TYPE, P::VERSION)?;

        let event_id = ulid::Ulid::new();
        let timestamp = chrono::Utc::now();

        Span::current().record("event_id", &event_id.to_string());
        debug!(event_id = %event_id, "Generated event ID and timestamp");

        // 2. Perform the append operation within a transaction
        let result = self
            .db
            .transaction(|_tx_db| {
                let metadata = self.db.open_tree(METADATA_TREE)?;
                let events = self.db.open_tree(EVENTS_TREE)?;

                // Get current log state from metadata
                let head_offset = metadata
                    .get(HEAD_KEY)?
                    .map(|bytes| u64::from_be_bytes(bytes.as_ref().try_into().unwrap()))
                    .unwrap_or(0);

                let causality: DottedVersionVector = metadata
                    .get(CAUSALITY_KEY)?
                    .and_then(|bytes| serde_json::from_slice(&bytes).ok())
                    .unwrap_or_default();

                let prev_hash = metadata
                    .get(PREV_HASH_KEY)?
                    .map(|bytes| String::from_utf8(bytes.to_vec()).unwrap())
                    .unwrap_or_else(|| "0".repeat(64));

                // Prepare the new event
                let new_offset = if head_offset == 0 && events.is_empty() {
                    0
                } else {
                    head_offset + 1
                };

                // Increment causality
                let mut new_causality = causality;
                new_causality.increment(&signer_did);

                // Create the event directly (validation already done)
                let event = Event {
                    id: event_id,
                    ts: timestamp,
                    stream_id: self.stream_id.clone(),
                    space_id: self.space_id.clone(),
                    event_type: P::TYPE.to_string(),
                    schema_version: P::VERSION,
                    payload: payload_value.clone(),
                    causality: new_causality.clone(),
                    prev_hash,
                    signer: signer_did.to_string(),
                    sig: "placeholder_signature".to_string(),
                    trust_refs: vec![],
                    redactable: false,
                };

                // Serialize the event for storage and hashing
                let event_bytes = serde_json::to_vec(&event).map_err(|e| {
                    sled::Error::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e.to_string(),
                    ))
                })?;
                let new_hash = format!("{:x}", sha2::Sha256::digest(&event_bytes));

                // Stage all writes for the transaction
                metadata.insert(HEAD_KEY, &new_offset.to_be_bytes())?;
                metadata.insert(
                    CAUSALITY_KEY,
                    serde_json::to_vec(&new_causality)
                        .map_err(|e| {
                            sled::Error::Io(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                e.to_string(),
                            ))
                        })?
                        .as_slice(),
                )?;
                metadata.insert(PREV_HASH_KEY, new_hash.as_bytes())?;
                events.insert(&new_offset.to_be_bytes(), event_bytes)?;

                Span::current().record("offset", new_offset);

                Ok((event, new_offset))
            })
            .map_err(|e: sled::transaction::TransactionError| {
                error!(event_id = %event_id, error = ?e, "Transaction failed");
                EventError::Database(sled::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Transaction error: {:?}", e),
                )))
            })?;

        // Ensure the data is written to disk before returning
        self.db.flush()?;
        info!(
            event_id = %result.0.id, offset = result.1, event_type = %result.0.event_type,
            "Event appended successfully"
        );
        Ok(result.0)
    }

    /// Fetches a single event by its offset.
    #[instrument(skip(self), fields(offset))]
    pub fn get_event(&self, offset: u64) -> Result<Option<Event>, EventError> {
        debug!(offset = offset, "Fetching event");

        let bytes = self.events.get(offset.to_be_bytes())?;
        let result = bytes
            .map(|b| serde_json::from_slice(&b).map_err(EventError::Json))
            .transpose()?;

        if result.is_some() {
            debug!(offset = offset, "Event found");
        } else {
            debug!(offset = offset, "Event not found");
        }

        Ok(result)
    }

    /// Gets the current head offset of the event log.
    #[instrument(skip(self))]
    pub fn get_head_offset(&self) -> Result<u64, EventError> {
        let offset = self
            .metadata
            .get(HEAD_KEY)?
            .map(|bytes| u64::from_be_bytes(bytes.as_ref().try_into().unwrap()))
            .ok_or(EventError::LogIsEmpty)?;
        debug!(head_offset = offset, "Retrieved head offset");
        Ok(offset)
    }

    /// Gets the current causality state.
    #[instrument(skip(self))]
    pub fn get_causality(&self) -> Result<DottedVersionVector, EventError> {
        let causality = self
            .metadata
            .get(CAUSALITY_KEY)?
            .map(|bytes| serde_json::from_slice(&bytes).map_err(EventError::Json))
            .transpose()?
            .unwrap_or_else(|| DottedVersionVector::default());

        debug!(
            num_actors = causality.clocks.len(),
            "Retrieved causality state"
        );

        Ok(causality)
    }

    /// Gets the previous hash.
    #[instrument(skip(self))]
    pub fn get_prev_hash(&self) -> Result<String, EventError> {
        let hash = self
            .metadata
            .get(PREV_HASH_KEY)?
            .map(|bytes| String::from_utf8(bytes.to_vec()).unwrap())
            .unwrap_or_else(|| "0".repeat(64));

        debug!(prev_hash = %hash, "Retrieved previous hash");
        Ok(hash)
    }

    /// Returns an iterator over all events in the store.
    pub fn iter_events(&self) -> impl Iterator<Item = Result<Event, EventError>> + '_ {
        self.events.iter().map(|result| {
            result
                .map_err(EventError::Database)
                .and_then(|(_, bytes)| serde_json::from_slice(&bytes).map_err(EventError::Json))
        })
    }

    /// Returns the number of events in the store.
    #[instrument(skip(self))]
    pub fn event_count(&self) -> Result<usize, EventError> {
        let count = self.events.len();
        debug!(count = count, "Retrieved event count");
        Ok(count)
    }

    /// Returns a forward-only iterator over events starting from a given offset.
    #[instrument(skip(self))]
    pub fn iter_from(&self, start_offset: u64) -> EventIterator {
        debug!(start_offset = start_offset, "Creating event iterator");
        EventIterator {
            tree: self.events.clone(),
            current_offset: start_offset.saturating_sub(1),
        }
    }

    /// Returns an iterator over a range of events [start, end).
    #[instrument(skip(self))]
    pub fn iter_range(&self, start_offset: u64, end_offset: u64) -> EventRangeIterator {
        debug!(
            start_offset = start_offset,
            end_offset = end_offset,
            "Creating range iterator"
        );
        EventRangeIterator {
            tree: self.events.clone(),
            current_offset: start_offset.saturating_sub(1),
            end_offset,
        }
    }
}

/// An iterator for traversing events in the Sled store.
pub struct EventIterator {
    tree: Tree,
    current_offset: u64,
}

impl Iterator for EventIterator {
    type Item = Result<(u64, Event), EventError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.tree.get_gt(&self.current_offset.to_be_bytes()) {
            Ok(Some((offset_bytes, event_bytes))) => {
                let offset = u64::from_be_bytes(offset_bytes.as_ref().try_into().unwrap());
                self.current_offset = offset;

                let event_result =
                    serde_json::from_slice::<Event>(&event_bytes).map_err(EventError::Json);

                Some(event_result.map(|event| (offset, event)))
            }
            Ok(None) => None,              // End of iteration
            Err(e) => Some(Err(e.into())), // Database error
        }
    }
}

/// An iterator for traversing a range of events in the Sled store.
pub struct EventRangeIterator {
    tree: Tree,
    current_offset: u64,
    end_offset: u64,
}

impl Iterator for EventRangeIterator {
    type Item = Result<(u64, Event), EventError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_offset >= self.end_offset {
            return None;
        }

        match self.tree.get_gt(&self.current_offset.to_be_bytes()) {
            Ok(Some((offset_bytes, event_bytes))) => {
                let offset = u64::from_be_bytes(offset_bytes.as_ref().try_into().unwrap());

                // Check if we've exceeded the end offset
                if offset >= self.end_offset {
                    return None;
                }

                self.current_offset = offset;

                // Use serde_json for consistency
                let event_result =
                    serde_json::from_slice::<Event>(&event_bytes).map_err(EventError::Json);

                Some(event_result.map(|event| (offset, event)))
            }
            Ok(None) => None,
            Err(e) => Some(Err(e.into())),
        }
    }
}
