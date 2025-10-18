#[path = "fixtures/mod.rs"]
mod fixtures;

use event::EventValidator;
use fixtures::*;
use serde_json::json;

#[test]
fn test_validator_creation() {
    let _validator = EventValidator::new();
    // Should not panic
    assert!(true);
}

#[test]
fn test_register_schema_success() {
    let validator = EventValidator::new();

    let schema = json!({
        "title": "MyEvent",
        "version": 1,
        "type": "object",
        "properties": {
            "name": { "type": "string" }
        }
    });

    assert!(validator.register_schema(schema).is_ok());
}

#[test]
fn test_register_schema_missing_title() {
    let validator = EventValidator::new();

    let schema = json!({
        "version": 1,
        "type": "object"
    });

    let result = validator.register_schema(schema);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("title"));
}

#[test]
fn test_register_schema_missing_version() {
    let validator = EventValidator::new();

    let schema = json!({
        "title": "MyEvent",
        "type": "object"
    });

    let result = validator.register_schema(schema);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("version"));
}

#[test]
fn test_validate_success() {
    let validator = create_test_validator();

    let payload = json!({
        "message": "Hello",
        "count": 42
    });

    let result = validator.validate(&payload, "TestEvent", 1);
    assert!(result.is_ok());
}

#[test]
fn test_validate_missing_required_field() {
    let validator = create_test_validator();

    let payload = json!({
        "message": "Hello"
        // missing 'count'
    });

    let result = validator.validate(&payload, "TestEvent", 1);
    assert!(result.is_err());
}

#[test]
fn test_validate_wrong_type() {
    let validator = create_test_validator();

    let payload = json!({
        "message": "Hello",
        "count": "not_a_number"  // should be integer
    });

    let result = validator.validate(&payload, "TestEvent", 1);
    assert!(result.is_err());
}

#[test]
fn test_validate_schema_not_found() {
    let validator = EventValidator::new();

    let payload = json!({"anything": "here"});

    let result = validator.validate(&payload, "NonExistentEvent", 1);
    assert!(result.is_err());

    match result {
        Err(event::EventError::SchemaNotFound {
            event_type,
            version,
        }) => {
            assert_eq!(event_type, "NonExistentEvent");
            assert_eq!(version, 1);
        }
        _ => panic!("Expected SchemaNotFound error"),
    }
}

#[test]
fn test_validate_complex_nested() {
    let validator = create_test_validator();

    let payload = json!({
        "nested": {
            "field1": "value",
            "field2": 100
        },
        "list": ["a", "b", "c"],
        "optional": 42
    });

    let result = validator.validate(&payload, "ComplexEvent", 1);
    assert!(result.is_ok());
}

#[test]
fn test_validate_optional_field_missing() {
    let validator = create_test_validator();

    let payload = json!({
        "nested": {
            "field1": "value",
            "field2": 100
        },
        "list": ["a", "b", "c"]
        // 'optional' field missing - should be OK
    });

    let result = validator.validate(&payload, "ComplexEvent", 1);
    assert!(result.is_ok());
}

#[test]
fn test_multiple_schemas_same_type_different_versions() {
    let validator = create_test_validator();

    // V1 payload (only name)
    let payload_v1 = json!({"name": "Test"});
    assert!(validator.validate(&payload_v1, "VersionedEvent", 1).is_ok());

    // V2 payload (name + description)
    let payload_v2 = json!({
        "name": "Test",
        "description": "Description"
    });
    assert!(validator.validate(&payload_v2, "VersionedEvent", 2).is_ok());

    // V1 payload against V2 schema should fail
    assert!(validator
        .validate(&payload_v1, "VersionedEvent", 2)
        .is_err());
}

#[test]
fn test_validator_thread_safety() {
    use std::sync::Arc;
    use std::thread;

    let validator = Arc::new(create_test_validator());
    let mut handles = vec![];

    for i in 0..10 {
        let validator = Arc::clone(&validator);
        let handle = thread::spawn(move || {
            let payload = json!({
                "message": format!("Thread {}", i),
                "count": i
            });
            validator.validate(&payload, "TestEvent", 1)
        });
        handles.push(handle);
    }

    for handle in handles {
        assert!(handle.join().unwrap().is_ok());
    }
}

#[test]
fn test_register_schema_overwrite() {
    let validator = EventValidator::new();

    let schema_v1 = json!({
        "title": "MyEvent",
        "version": 1,
        "type": "object",
        "properties": {
            "field1": { "type": "string" }
        }
    });

    let schema_v1_updated = json!({
        "title": "MyEvent",
        "version": 1,
        "type": "object",
        "properties": {
            "field1": { "type": "string" },
            "field2": { "type": "integer" }
        }
    });

    validator.register_schema(schema_v1).unwrap();
    validator.register_schema(schema_v1_updated).unwrap();

    // Should use the updated schema
    let payload = json!({
        "field1": "value",
        "field2": 42
    });
    assert!(validator.validate(&payload, "MyEvent", 1).is_ok());
}
