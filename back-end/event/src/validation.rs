use crate::types::EventError;
use jsonschema::validator_for;
use serde_json::Value;
use std::collections::HashMap;

/// A validator that holds compiled JSON schemas provided by the application at runtime.
#[derive(Default)]
pub struct EventValidator {
    schemas: HashMap<String, Box<dyn Fn(&Value) -> Result<(), String>>>,
}

impl EventValidator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a new schema with the validator.
    /// The application is responsible for loading the schema JSON.
    pub fn register_schema(&mut self, schema_json: Value) -> Result<(), String> {
        let title = schema_json
            .get("title")
            .and_then(Value::as_str)
            .ok_or("Schema must have a 'title' field")?;
        let version = schema_json
            .get("version")
            .and_then(Value::as_u64)
            .ok_or("Schema must have a 'version' field")?;

        let key = format!("{}.{}", title, version);

        let compiled_schema = validator_for(&schema_json).map_err(|e| e.to_string())?;

        // Store a closure that validates against this schema
        self.schemas.insert(
            key,
            Box::new(move |value: &Value| {
                compiled_schema.validate(value).map_err(|e| e.to_string())
            }),
        );

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
        let validator = self
            .schemas
            .get(&key)
            .ok_or_else(|| EventError::SchemaNotFound {
                event_type: event_type.to_string(),
                version,
            })?;

        validator(payload).map_err(EventError::Validation)
    }
}
