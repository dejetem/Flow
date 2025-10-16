use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use thiserror::Error;
use ulid::Ulid;

#[derive(Error, Debug)]
pub enum EventError {
    #[error("validation failed: {0}")]
    Validation(String),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("schema not found for type '{event_type}' and version '{version}'")]
    SchemaNotFound { event_type: String, version: u32 },
    #[error("invalid payload for event type '{0}'")]
    InvalidPayload(String),
    #[error("database error: {0}")]
    Database(#[from] sled::Error),
    #[error("binary encoding error: {0}")]
    BinEncode(#[from] Box<bincode::error::EncodeError>),
    #[error("binary decoding error: {0}")]
    BinDecode(#[from] Box<bincode::error::DecodeError>),
    #[error("Log is empty, cannot determine previous state")]
    LogIsEmpty,
    #[error("TransactionError '{0}'")]
    TransactionError(String),
}

/// A trait that all event payloads must implement.
/// This decouples the generic event system from specific payload types.
pub trait EventPayload: Serialize + for<'de> Deserialize<'de> {
    /// The unique type name for this event, e.g., "SpaceCreated".
    const TYPE: &'static str;
    /// The schema version for this event payload struct.
    const VERSION: u32;
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct DottedVersionVector {
    // A map from an actor's DID to their latest sequence number.
    // Full implementation with dot-handling will be fleshed out later.
    pub clocks: HashMap<String, u64>,
    pub dots: HashSet<(String, u64)>,
}

impl Default for DottedVersionVector {
    fn default() -> Self {
        Self {
            clocks: HashMap::new(),
            dots: HashSet::new(),
        }
    }
}

impl DottedVersionVector {
    // Check if this DVV causally precedes another
    pub fn precedes(&self, other: &DottedVersionVector) -> bool {
        // For each actor in self, their clock must be <= other's clock
        for (actor, clock) in &self.clocks {
            if let Some(other_clock) = other.clocks.get(actor) {
                if clock > other_clock {
                    return false;
                }
            } else {
                return false;
            }
        }
        true
    }

    // Check if two DVVs are concurrent (neither precedes the other)
    pub fn concurrent(&self, other: &DottedVersionVector) -> bool {
        !self.precedes(other) && !other.precedes(self)
    }

    pub fn merge(&mut self, other: &DottedVersionVector) {
        for (actor, clock) in &other.clocks {
            let current_clock = self.clocks.get(actor).unwrap_or(&0);
            if clock > current_clock {
                self.clocks.insert(actor.clone(), *clock);
            }
        }

        // Merge dots
        for dot in &other.dots {
            self.dots.insert(dot.clone());
        }
    }

    /// Advances the clock for a given actor, creating a new dot.
    pub fn increment(&mut self, actor_did: &str) {
        let new_clock = self.clocks.entry(actor_did.to_string()).or_insert(0);
        *new_clock += 1;
        self.dots.clear();
        self.dots.insert((actor_did.to_string(), *new_clock));
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Event {
    pub id: Ulid,
    pub stream_id: String,
    pub space_id: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub schema_version: u32,
    pub payload: Value,
    pub ts: DateTime<Utc>,
    pub causality: DottedVersionVector,
    pub prev_hash: String,
    pub signer: String,
    pub sig: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub trust_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub redactable: bool,
}
