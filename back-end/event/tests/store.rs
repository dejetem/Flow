#[path = "fixtures/mod.rs"]
mod fixtures;

use event::{store::SignedPayload, EventStore};
use fixtures::*;
use tempfile::TempDir;

#[test]
fn test_store_creation() {
    let temp_dir = TempDir::new().unwrap();
    let validator = create_test_validator();

    let store = EventStore::new(
        temp_dir.path(),
        "test_stream".to_string(),
        "test_space".to_string(),
        validator,
    );

    assert!(store.is_ok());
}

#[test]
fn test_store_creation_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().to_path_buf();

    // Create and drop store
    {
        let validator = create_test_validator();
        let _store = EventStore::new(
            &path,
            "test_stream".to_string(),
            "test_space".to_string(),
            validator,
        )
        .unwrap();
    }

    // Reopen
    let validator = create_test_validator();
    let store = EventStore::new(
        &path,
        "test_stream".to_string(),
        "test_space".to_string(),
        validator,
    );

    assert!(store.is_ok());
}

#[test]
fn test_append_single_event() {
    let (store, _temp_dir) = create_test_store();

    let signed_payload = SignedPayload {
        payload: TestPayload {
            message: "Hello, World!".to_string(),
            count: 42,
        },
        signer_did: test_did("alice"),
        signature: fake_signature("test"),
        signature_type: "Ed25519".to_string(),
    };

    let result = store.append(signed_payload);
    assert!(result.is_ok());

    let event = result.unwrap();
    assert_eq!(event.signer, test_did("alice"));
    assert_eq!(event.event_type, "TestEvent");
    assert_eq!(event.schema_version, 1);
}

#[test]
fn test_append_multiple_events() {
    let (store, _temp_dir) = create_test_store();

    for i in 0..10 {
        let signed_payload = SignedPayload {
            payload: TestPayload {
                message: format!("Message {}", i),
                count: i,
            },
            signer_did: test_did("alice"),
            signature: fake_signature(&format!("sig_{}", i)),
            signature_type: "Ed25519".to_string(),
        };

        assert!(store.append(signed_payload).is_ok());
    }

    let count = store.event_count().unwrap();
    assert_eq!(count, 10);
}

#[test]
fn test_append_creates_hash_chain() {
    let (store, _temp_dir) = create_test_store();

    let event1 = store.append(create_signed_payload("Message 1", 1)).unwrap();

    let event2 = store.append(create_signed_payload("Message 2", 2)).unwrap();

    let event3 = store.append(create_signed_payload("Message 3", 3)).unwrap();

    // Verify chain
    assert!(event2.verify_chain(&event1).unwrap());
    assert!(event3.verify_chain(&event2).unwrap());
}

#[test]
fn test_append_advances_causality() {
    let (store, _temp_dir) = create_test_store();

    let event1 = store.append(create_signed_payload("Msg 1", 1)).unwrap();

    let event2 = store.append(create_signed_payload("Msg 2", 2)).unwrap();

    let alice = test_did("alice");

    assert_eq!(event1.causality.clocks.get(&alice), Some(&1));
    assert_eq!(event2.causality.clocks.get(&alice), Some(&2));
}

#[test]
fn test_get_event_success() {
    let (store, _temp_dir) = create_test_store();

    let original = store.append(create_signed_payload("Test", 42)).unwrap();

    let retrieved = store.get_event(0).unwrap();
    assert!(retrieved.is_some());

    let retrieved = retrieved.unwrap();
    assert_eq!(retrieved.id, original.id);
    assert_eq!(retrieved.signer, original.signer);
}

#[test]
fn test_get_event_not_found() {
    let (store, _temp_dir) = create_test_store();

    let result = store.get_event(999).unwrap();
    assert!(result.is_none());
}

#[test]
fn test_get_head_offset_empty() {
    let (store, _temp_dir) = create_test_store();

    let result = store.get_head_offset();
    assert!(result.is_err()); // Empty log
}

#[test]
fn test_get_head_offset() {
    let (store, _temp_dir) = create_test_store();

    store.append(create_signed_payload("Msg 1", 1)).unwrap();
    store.append(create_signed_payload("Msg 2", 2)).unwrap();
    store.append(create_signed_payload("Msg 3", 3)).unwrap();

    let head = store.get_head_offset().unwrap();
    assert_eq!(head, 2); // 0-indexed, so 3rd event is at offset 2
}

#[test]
fn test_get_causality() {
    let (store, _temp_dir) = create_test_store();

    store.append(create_signed_payload("Msg 1", 1)).unwrap();
    store.append(create_signed_payload("Msg 2", 2)).unwrap();

    let causality = store.get_causality().unwrap();
    assert_eq!(causality.clocks.get(&test_did("alice")), Some(&2));
}

#[test]
fn test_get_prev_hash_genesis() {
    let (store, _temp_dir) = create_test_store();

    let prev_hash = store.get_prev_hash().unwrap();
    assert_eq!(prev_hash, "0".repeat(64)); // Genesis
}

#[test]
fn test_get_prev_hash_after_append() {
    let (store, _temp_dir) = create_test_store();

    let event = store.append(create_signed_payload("Msg", 1)).unwrap();
    let expected_hash = event.compute_hash().unwrap();

    let prev_hash = store.get_prev_hash().unwrap();
    assert_eq!(prev_hash, expected_hash);
}

#[test]
fn test_iter_events() {
    let (store, _temp_dir) = create_test_store();

    // Append 5 events
    for i in 0..5 {
        store
            .append(create_signed_payload(&format!("Msg {}", i), i))
            .unwrap();
    }

    let events: Vec<_> = store.iter_events().collect();
    assert_eq!(events.len(), 5);

    // All should succeed
    for event_result in events {
        assert!(event_result.is_ok());
    }
}

#[test]
fn test_iter_from_offset() {
    let (store, _temp_dir) = create_test_store();

    for i in 0..5 {
        store
            .append(create_signed_payload(&format!("Msg {}", i), i))
            .unwrap();
    }

    let events: Vec<_> = store.iter_from(2).collect();

    // Should get events from offset 2 onwards
    assert!(events.len() >= 3);
}

#[test]
fn test_iter_range() {
    let (store, _temp_dir) = create_test_store();

    for i in 0..10 {
        store
            .append(create_signed_payload(&format!("Msg {}", i), i))
            .unwrap();
    }

    let events: Vec<_> = store.iter_range(2, 5).collect();

    // Should get offsets 2, 3, 4 (5 is exclusive)
    assert_eq!(events.len(), 3);
}

#[test]
fn test_event_count() {
    let (store, _temp_dir) = create_test_store();

    assert_eq!(store.event_count().unwrap(), 0);

    store.append(create_signed_payload("Msg 1", 1)).unwrap();
    assert_eq!(store.event_count().unwrap(), 1);

    store.append(create_signed_payload("Msg 2", 2)).unwrap();
    assert_eq!(store.event_count().unwrap(), 2);
}

#[test]
fn test_persistence_across_reopens() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().to_path_buf();

    // Create store and add events
    {
        let validator = create_test_validator();
        let store = EventStore::new(
            &path,
            "test_stream".to_string(),
            "test_space".to_string(),
            validator,
        )
        .unwrap();

        for i in 0..5 {
            store
                .append(create_signed_payload(&format!("Msg {}", i), i))
                .unwrap();
        }
    }

    // Reopen and verify
    {
        let validator = create_test_validator();
        let store = EventStore::new(
            &path,
            "test_stream".to_string(),
            "test_space".to_string(),
            validator,
        )
        .unwrap();

        assert_eq!(store.event_count().unwrap(), 5);
        let head = store.get_head_offset().unwrap();
        assert_eq!(head, 4);
    }
}

#[test]
fn test_concurrent_append_different_actors() {
    let (store, _temp_dir) = create_test_store();

    let payload_alice = SignedPayload {
        payload: TestPayload {
            message: "Alice".to_string(),
            count: 1,
        },
        signer_did: test_did("alice"),
        signature: fake_signature("alice"),
        signature_type: "Ed25519".to_string(),
    };

    let payload_bob = SignedPayload {
        payload: TestPayload {
            message: "Bob".to_string(),
            count: 2,
        },
        signer_did: test_did("bob"),
        signature: fake_signature("bob"),
        signature_type: "Ed25519".to_string(),
    };

    store.append(payload_alice).unwrap();
    store.append(payload_bob).unwrap();

    let causality = store.get_causality().unwrap();
    assert_eq!(causality.clocks.get(&test_did("alice")), Some(&1));
    assert_eq!(causality.clocks.get(&test_did("bob")), Some(&1));
}

// Helper functions

fn create_test_store() -> (EventStore, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let validator = create_test_validator();

    let store = EventStore::new(
        temp_dir.path(),
        "test_stream".to_string(),
        "test_space".to_string(),
        validator,
    )
    .unwrap();

    (store, temp_dir)
}

fn create_signed_payload(message: &str, count: i32) -> SignedPayload<TestPayload> {
    SignedPayload {
        payload: TestPayload {
            message: message.to_string(),
            count,
        },
        signer_did: test_did("alice"),
        signature: fake_signature(message),
        signature_type: "Ed25519".to_string(),
    }
}
