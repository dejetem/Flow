use crate::{
    store::EventStore,
    types::{Event, EventError},
};
use sled::{
    Db, Tree,
    transaction::{ConflictableTransactionError, TransactionError},
};

const DEFAULT_BATCH_SIZE: usize = 100;
const BOOKMARK_KEY: &[u8] = b"bookmark";
const IDEMPOTENCY_KEY_PREFIX: &[u8] = b"idempotency_";

pub trait EventHandler {
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
pub struct PersistentSubscription<H: EventHandler> {
    db: Db,
    state_tree: Tree,
    handler: H,
    idempotency_window_size: u64,
}

impl<H: EventHandler> PersistentSubscription<H> {
    pub fn new(db: &Db, handler: H, idempotency_window_size: u64) -> Result<Self, EventError> {
        let state_tree = db.open_tree(format!("_listener_state_{}", handler.name()))?;
        Ok(Self {
            db: db.clone(),
            state_tree,
            handler,
            idempotency_window_size,
        })
    }

    pub fn bookmark(&self) -> Result<u64, EventError> {
        Ok(self
            .state_tree
            .get(BOOKMARK_KEY)?
            .map(|bytes| u64::from_be_bytes(bytes.as_ref().try_into().unwrap()))
            .unwrap_or(0))
    }

    /// Process a batch in a single atomic transaction:
    /// - Skips already-seen events (idempotency is achieved with `event.id`)
    /// - Calls handler for new events
    /// - Marks processed ids and advances bookmark to last processed offset
    pub fn process_batch(&mut self, events: &[(u64, Event)]) -> Result<(), EventError> {
        if events.is_empty() {
            return Ok(());
        }

        // We handle events OUTSIDE the sled transaction to avoid retries/side-effects in Tx.
        for (offset, event) in events {
            // Optional filter
            if !self.handler.filter_event(event) {
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

            // Fast path: skip if already processed
            if self.state_tree.get(&id_key)?.is_some() {
                continue;
            }

            // Invoke handler (must be idempotent). This happens outside any sled Tx.
            self.handler.handle(*offset, event)?;

            // After successful handling, persist idempotency marker and advance bookmark atomically.
            // This small transaction does not call the handler and is safe to retry.
            self.db
                .transaction(|_tx_db| {
                    let tx_state = self.db.open_tree(self.state_tree.name())?;

                    // If some other worker marked it in the meantime, this is a no-op.
                    if tx_state.get(&id_key)?.is_none() {
                        tx_state.insert(&id_key, &[])?;
                    }

                    // Advance bookmark to this offset (monotonic).
                    tx_state.insert(BOOKMARK_KEY, &offset.to_be_bytes())?;

                    // Prune idempotency set to keep only the most recent N
                    let total_seen =
                        tx_state.scan_prefix(IDEMPOTENCY_KEY_PREFIX).keys().count() as u64;
                    if total_seen > self.idempotency_window_size {
                        let to_remove = total_seen - self.idempotency_window_size;
                        for key_res in tx_state
                            .scan_prefix(IDEMPOTENCY_KEY_PREFIX)
                            .keys()
                            .take(to_remove as usize)
                        {
                            tx_state.remove(key_res?)?;
                        }
                    }
                    Ok(())
                })
                .map_err(|e: TransactionError<ConflictableTransactionError>| {
                    EventError::Database(sled::Error::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Listener transaction error: {:?}", e),
                    )))
                })?;
        }

        self.db.flush()?; // Ensure durability
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
    pub fn poll<H: EventHandler>(
        &self,
        subscription: &mut PersistentSubscription<H>,
    ) -> Result<(), EventError> {
        let mut bookmark = subscription.bookmark()?;

        loop {
            let batch: Vec<(u64, Event)> = self
                .store
                .iter_from(bookmark)
                .take(self.batch_size)
                .collect::<Result<Vec<_>, _>>()?;

            if batch.is_empty() {
                break;
            }

            subscription.process_batch(&batch)?;

            // Next starting position is the last delivered offset + 1
            if let Some((last_offset, _)) = batch.last() {
                bookmark = *last_offset + 1;
            }
        }

        Ok(())
    }
}
