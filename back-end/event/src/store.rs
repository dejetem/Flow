use crate::{
    types::{DottedVersionVector, Event, EventError, EventPayload},
    validation::EventValidator,
};
use sha2::Digest;
use sled::{Db, Tree};
use std::{path::Path, sync::Arc};

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
    pub fn new<P: AsRef<Path>>(
        path: P,
        stream_id: String,
        space_id: String,
        validator: EventValidator,
    ) -> Result<Self, EventError> {
        let db = sled::open(path)?;
        let metadata = db.open_tree(METADATA_TREE)?;
        let events = db.open_tree(EVENTS_TREE)?;

        Ok(Self {
            db,
            metadata,
            events,
            validator: Arc::new(validator),
            stream_id,
            space_id,
        })
    }

    /// Appends a new event to the store in a single atomic transaction.
    ///
    ///
    /// This method:
    /// 1. Validates the payload against its schema (outside transaction)
    /// 2. Atomically reads current state, creates event, and persists it
    /// 3. Returns the created event with its assigned ID and metadata
    pub fn append<P: EventPayload>(
        &self,
        signer_did: &str,
        payload: P,
    ) -> Result<Event, EventError> {
        let payload_value = serde_json::to_value(&payload)?;
        self.validator
            .validate(&payload_value, P::TYPE, P::VERSION)?;

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
                    id: ulid::Ulid::new(),
                    stream_id: self.stream_id.clone(),
                    space_id: self.space_id.clone(),
                    event_type: P::TYPE.to_string(),
                    schema_version: P::VERSION,
                    payload: payload_value.clone(),
                    ts: chrono::Utc::now(),
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

                Ok(event)
            })
            .map_err(|e: sled::transaction::TransactionError| {
                EventError::Database(sled::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Transaction error: {:?}", e),
                )))
            })?;

        // Ensure the data is written to disk before returning
        self.db.flush()?;
        Ok(result)
    }

    /// Fetches a single event by its offset.
    pub fn get_event(&self, offset: u64) -> Result<Option<Event>, EventError> {
        let bytes = self.events.get(offset.to_be_bytes())?;
        bytes
            .map(|b| serde_json::from_slice(&b).map_err(EventError::Json))
            .transpose()
    }

    /// Gets the current head offset of the event log.
    pub fn get_head_offset(&self) -> Result<u64, EventError> {
        self.metadata
            .get(HEAD_KEY)?
            .map(|bytes| u64::from_be_bytes(bytes.as_ref().try_into().unwrap()))
            .ok_or(EventError::LogIsEmpty)
    }

    /// Gets the current causality state.
    pub fn get_causality(&self) -> Result<DottedVersionVector, EventError> {
        Ok(self
            .metadata
            .get(CAUSALITY_KEY)?
            .map(|bytes| serde_json::from_slice(&bytes).map_err(EventError::Json))
            .transpose()?
            .unwrap_or_else(|| DottedVersionVector::default()))
    }

    /// Gets the previous hash.
    pub fn get_prev_hash(&self) -> Result<String, EventError> {
        Ok(self
            .metadata
            .get(PREV_HASH_KEY)?
            .map(|bytes| String::from_utf8(bytes.to_vec()).unwrap())
            .unwrap_or_else(|| "0".repeat(64)))
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
    pub fn event_count(&self) -> Result<usize, EventError> {
        Ok(self.events.len())
    }

    /// Returns a forward-only iterator over events starting from a given offset.
    pub fn iter_from(&self, start_offset: u64) -> EventIterator {
        EventIterator {
            tree: self.events.clone(),
            current_offset: start_offset.saturating_sub(1),
        }
    }

    /// Returns an iterator over a range of events [start, end).
    pub fn iter_range(&self, start_offset: u64, end_offset: u64) -> EventRangeIterator {
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

                // Use serde_json for consistency with the rest of the codebase
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
