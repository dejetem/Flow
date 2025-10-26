use crate::types::EventError;
use jsonschema::validator_for;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::RwLock;
use tracing::{debug, info, instrument, warn};

/// A validator that holds compiled JSON schemas provided by the application at runtime.
#[derive(Default)]
pub struct EventValidator {
    schemas: RwLock<HashMap<String, Box<dyn Fn(&Value) -> Result<(), String> + Send + Sync>>>,
}

impl EventValidator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a new schema with the validator.
    /// The application is responsible for loading the schema JSON.
    #[instrument(skip(self), fields(title, version))]
    pub fn register_schema(&self, schema_json: Value) -> Result<(), String> {
        let title = schema_json
            .get("title")
            .and_then(Value::as_str)
            .ok_or("Schema must have a 'title' field")?;
        let version = schema_json
            .get("version")
            .and_then(Value::as_u64)
            .ok_or("Schema must have a 'version' field")?;

        tracing::Span::current().record("title", title);
        tracing::Span::current().record("version", version);

        let key = format!("{}.{}", title, version);
        debug!(schema_key = %key, "Compiling JSON schema");

        let compiled_schema = validator_for(&schema_json).map_err(|e| {
            warn!(schema_key = %key, error = %e, "Failed to compile schema");
            e.to_string()
        })?;

        // Store a closure that validates against this schema
        self.schemas.write().unwrap().insert(
            key.clone(),
            Box::new(move |value: &Value| {
                compiled_schema.validate(value).map_err(|e| e.to_string())
            }),
        );
        info!(schema_key = %key, "Successfully registered schema");

        Ok(())
    }

    /// Validates a serde_json Value against a specific event type and version.
    pub fn validate(
        &self,
        payload: &Value,
        event_type: &str,
        version: u32,
    ) -> Result<(), EventError> {
        let key = format!("{}.{}", event_type, version);
        debug!(schema_key = %key, "Validating event payload");

        let schemas = self.schemas.read().unwrap();
        let validator = schemas.get(&key).ok_or_else(|| {
            warn!(schema_key = %key, available_schemas = ?schemas.keys().collect::<Vec<_>>(), "Schema not found");
            EventError::SchemaNotFound {
                event_type: event_type.to_string(),
                version,
            }
        })?;

        validator(payload).map_err(|e| {
            warn!(schema_key = %key, error = %e,"Validation failed");
            EventError::Validation(e)
        })?;
        debug!(schema_key = %key, "Validation successful");

        Ok(())
    }
}
