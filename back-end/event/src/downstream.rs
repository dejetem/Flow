use crate::{
    store::EventStore,
    types::{Event, EventError},
};
use sled::{
    Db, Tree,
    transaction::{ConflictableTransactionError, TransactionError},
};
use tracing::{Span, debug, error, info, instrument, warn};

const DEFAULT_BATCH_SIZE: usize = 100;
const BOOKMARK_KEY: &[u8] = b"bookmark";
const IDEMPOTENCY_KEY_PREFIX: &[u8] = b"idempotency_";

pub trait EventHandler: Send {
    /// Unique name for this handler; used to isolate state in Sled.
    fn name(&self) -> &str;

    /// Optional event filter. Default: accept all.
    fn filter_event(&self, _event: &Event) -> bool {
        true
    }

    /// Handle a single event at the given offset.
    /// Must be idempotent; this function may be called more than once.
    fn handle(&mut self, offset: u64, event: &Event) -> Result<(), EventError>;
}

/// Persistent subscription that manages bookmark and idempotency state per handler.
pub struct PersistentSubscription<H: EventHandler + Send> {
    db: Db,
    state_tree: Tree,
    handler: H,
    idempotency_window_size: u64,
}

impl<H: EventHandler> PersistentSubscription<H> {
    #[instrument(skip(db, handler), fields(handler_name = handler.name()))]
    pub fn new(db: &Db, handler: H, idempotency_window_size: u64) -> Result<Self, EventError> {
        let handler_name = handler.name();

        info!(
            handler_name = handler_name,
            idempotency_window_size = idempotency_window_size,
            "Creating persistent subscription"
        );

        let state_tree = db.open_tree(format!("_listener_state_{}", handler_name))?;

        Ok(Self {
            db: db.clone(),
            state_tree,
            handler,
            idempotency_window_size,
        })
    }

    #[instrument(skip(self), fields(handler_name = self.handler.name()))]
    pub fn bookmark(&self) -> Result<u64, EventError> {
        let bookmark = self
            .state_tree
            .get(BOOKMARK_KEY)?
            .map(|bytes| u64::from_be_bytes(bytes.as_ref().try_into().unwrap()))
            .unwrap_or(0);

        debug!(
            handler_name = self.handler.name(),
            bookmark = bookmark,
            "Retrieved bookmark"
        );

        Ok(bookmark)
    }

    /// Process a batch of events with guaranteed idempotence.
    ///
    /// This method ensures:
    /// - Each event is processed at most once per handler (atomic claim)
    /// - Handler is called only after successful claim (no duplicate calls)
    /// - Bookmark advances monotonically
    /// - Idempotency window is maintained with bounded overhead
    #[instrument(
        skip(self, events),
        fields(
            handler_name = self.handler.name(),
            batch_size = events.len(),
            processed = 0,
            skipped = 0
        )
    )]
    pub fn process_batch(&mut self, events: &[(u64, Event)]) -> Result<(), EventError> {
        if events.is_empty() {
            debug!("Empty batch, skipping");
            return Ok(());
        }

        const PRUNE_INTERVAL: u64 = 100;
        let mut processed_count = 0;
        let mut skipped_count = 0;

        info!(
            handler_name = self.handler.name(),
            batch_size = events.len(),
            "Processing event batch"
        );

        for (offset, event) in events {
            // Optional filter
            if !self.handler.filter_event(event) {
                debug!(
                    offset = offset,
                    event_id = %event.id,
                    "Event filtered out"
                );
                skipped_count += 1;
                continue;
            }

            // Build idempotency key: prefix + offset (BE) + event.id
            let id_key = {
                let mut k = Vec::with_capacity(IDEMPOTENCY_KEY_PREFIX.len() + 8 + 16);
                k.extend_from_slice(IDEMPOTENCY_KEY_PREFIX);
                k.extend_from_slice(&offset.to_be_bytes());
                k.extend_from_slice(&event.id.to_bytes());
                k
            };

            // ATOMIC CLAIM: Try to insert idempotency marker
            // Returns true if WE claimed it (insert succeeded), false if already processed
            debug!(
                offset = offset,
                event_id = %event.id,
                "Attempting to claim event"
            );
            let claimed = self
                .db
                .transaction(|_tx_db| {
                    let tx_state = self.db.open_tree(self.state_tree.name())?;

                    // insert() returns the previous value
                    // None = we claimed it, Some(_) = already processed
                    match tx_state.insert(&id_key, &[])? {
                        None => Ok(true),     // We successfully claimed this event
                        Some(_) => Ok(false), // Already processed by another worker
                    }
                })
                .map_err(|e: TransactionError<ConflictableTransactionError>| {
                    error!(
                        offset = offset,
                        event_id = %event.id,
                        error = ?e,
                        "Claim transaction failed"
                    );
                    EventError::Database(sled::Error::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Claim transaction error: {:?}", e),
                    )))
                })?;

            // If we didn't claim it, skip (already processed)
            if !claimed {
                debug!(
                    offset = offset,
                    event_id = %event.id,
                    "Event already processed, skipping"
                );
                skipped_count += 1;
                continue;
            }

            // ONLY ONE THREAD reaches here per event
            // Invoke handler (must be idempotent for external side effects)
            debug!(
                offset = offset, event_id = %event.id, event_type = %event.event_type,
                "Invoking Event handler"
            );
            self.handler.handle(*offset, event).map_err(|e| {
                error!(
                    offset = offset, event_id = %event.id, error = %e,
                    "Handler failed"
                );
                e
            })?;

            processed_count += 1;

            // After successful handling, update bookmark and prune if needed
            let should_prune = *offset % PRUNE_INTERVAL == 0;

            self.db
                .transaction(|_tx_db| {
                    let tx_state = self.db.open_tree(self.state_tree.name())?;

                    // Advance bookmark to this offset (monotonic)
                    tx_state.insert(BOOKMARK_KEY, &offset.to_be_bytes())?;

                    // Amortized pruning: only check every PRUNE_INTERVAL events
                    if should_prune {
                        let total_seen =
                            tx_state.scan_prefix(IDEMPOTENCY_KEY_PREFIX).keys().count() as u64;

                        if total_seen > self.idempotency_window_size {
                            let to_remove = total_seen - self.idempotency_window_size;

                            debug!(
                                offset = offset,
                                total_seen = total_seen,
                                to_remove = to_remove,
                                "Pruning idempotency set"
                            );

                            // Remove oldest keys (lexicographically first due to offset prefix)
                            for key_res in tx_state
                                .scan_prefix(IDEMPOTENCY_KEY_PREFIX)
                                .keys()
                                .take(to_remove as usize)
                            {
                                tx_state.remove(key_res?)?;
                            }
                        }
                    }

                    Ok(())
                })
                .map_err(|e: TransactionError<ConflictableTransactionError>| {
                    error!(
                        offset = offset, event_id = %event.id, error = ?e,
                        "Bookmark transaction failed"
                    );
                    EventError::Database(sled::Error::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Bookmark transaction error: {:?}", e),
                    )))
                })?;
        }

        self.db.flush()?;

        Span::current().record("processed", processed_count);
        Span::current().record("skipped", skipped_count);

        info!(
            handler_name = self.handler.name(),
            processed = processed_count,
            skipped = skipped_count,
            "Batch processing complete"
        );

        Ok(())
    }
}

/// Dispatcher that delivers events (post-persistence) to a persistent subscription in batches.
pub struct Dispatcher<'a> {
    store: &'a EventStore,
    batch_size: usize,
}

impl<'a> Dispatcher<'a> {
    pub fn new(store: &'a EventStore) -> Self {
        Self {
            store,
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size.max(1);
        self
    }

    /// Polls for new events and delivers them in batches.
    /// Delivery is guaranteed after persistence since we read from `EventStore`.
    /// Idempotence is guaranteed by the subscription state.
    ///
    ///
    /// # Concurrency
    ///
    /// Do NOT call `poll()` concurrently on the same `PersistentSubscription`.
    /// Multiple concurrent calls will duplicate work and may violate idempotence.
    ///
    /// Safe patterns:
    /// - Single-threaded polling loop
    /// - One subscription per handler per worker
    #[instrument(
        skip(self, subscription),
        fields(
            handler_name = subscription.handler.name(),
            batch_size = self.batch_size,
            batches_processed = 0,
            total_events = 0
        )
    )]
    pub fn poll<H: EventHandler>(
        &self,
        subscription: &mut PersistentSubscription<H>,
    ) -> Result<(), EventError> {
        let mut bookmark = subscription.bookmark()?;
        let mut batches_processed = 0;
        let mut total_events = 0;

        info!(
            handler_name = subscription.handler.name(),
            starting_bookmark = bookmark,
            "Starting Event poll"
        );

        loop {
            debug!(
                handler_name = subscription.handler.name(),
                bookmark = bookmark,
                batch_size = self.batch_size,
                "Fetching Event batch",
            );

            let batch: Vec<(u64, Event)> = self
                .store
                .iter_from(bookmark)
                .take(self.batch_size)
                .collect::<Result<Vec<_>, _>>()?;

            if batch.is_empty() {
                debug!("No more events, poll complete");
                break;
            }

            let batch_len = batch.len();
            debug!(batch_len = batch_len, "Processing batch");

            subscription.process_batch(&batch)?;

            batches_processed += 1;
            total_events += batch_len;

            // Next starting position is the last delivered offset + 1
            if let Some((last_offset, _)) = batch.last() {
                bookmark = *last_offset + 1;
            }
        }

        Span::current().record("batches_processed", batches_processed);
        Span::current().record("total_events", total_events);

        info!(
            handler_name = subscription.handler.name(),
            batches_processed = batches_processed,
            total_events = total_events,
            final_bookmark = bookmark,
            "Event Poll complete"
        );

        Ok(())
    }
}
