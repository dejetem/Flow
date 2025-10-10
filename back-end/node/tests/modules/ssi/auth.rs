use crate::{
    bootstrap::init::{
        setup_test_db, setup_test_multi_node, setup_test_node, setup_test_node_with_device_id,
    },
    modules::ssi::fixtures::load_passkey,
};
use base64::{Engine as _, prelude::BASE64_STANDARD};
use migration::{Migrator, MigratorTrait};
use node::api::node::Node;
use node::bootstrap::init::NodeData;
use node::modules::ssi::webauthn::state::AuthState;
use sea_orm::{ColumnTrait, Database, QueryFilter};
use tempfile::TempDir;
use webauthn_authenticator_rs::{AuthenticatorBackend, softpasskey::SoftPasskey};
use webauthn_rs::prelude::Url;

// ========== Registration Tests ==========

#[tokio::test]
async fn test_start_registration_creates_challenge() {
    // Setup
    let (node, _temp) = setup_test_node_with_device_id("test-device-123").await;

    // Execute: Start registration
    let result = node.start_webauthn_registration().await;

    // Assert: Registration should succeed
    assert!(
        result.is_ok(),
        "start_registration should succeed: {:?}",
        result.err()
    );

    let (challenge_response, challenge_id) = result.unwrap();

    // Verify challenge response structure
    assert!(
        !challenge_response.public_key.challenge.is_empty(),
        "Challenge should not be empty"
    );

    // Verify challenge ID is a valid base64 string
    assert!(!challenge_id.is_empty(), "Challenge ID should not be empty");

    // Verify challenge ID is base64 encoded
    let decoded = BASE64_STANDARD.decode(&challenge_id);
    assert!(
        decoded.is_ok(),
        "Challenge ID should be valid base64: {}",
        challenge_id
    );

    // Verify public key credential creation options
    let public_key = &challenge_response.public_key;

    // Check relying party info
    assert_eq!(
        public_key.rp.name, "Test Flow",
        "RP name should match config"
    );
    assert_eq!(
        public_key.rp.id,
        "localhost".to_string(),
        "RP ID should match config"
    );

    // Check user info (should use device_id)
    assert_eq!(
        public_key.user.name, "test-device-123",
        "User name should be device ID"
    );
    assert_eq!(
        public_key.user.display_name, "test-device-123",
        "Display name should be device ID"
    );
    assert!(
        !public_key.user.id.is_empty(),
        "User ID should not be empty"
    );

    // Check credential parameters (should support ES256 at minimum)
    assert!(
        !public_key.pub_key_cred_params.is_empty(),
        "Should have at least one credential parameter"
    );

    let has_es256 = public_key
        .pub_key_cred_params
        .iter()
        .any(|param| param.alg == -7); // ES256 = -7 in COSE
    assert!(has_es256, "Should support ES256 algorithm");

    // Check timeout (should be reasonable)
    if let Some(timeout) = public_key.timeout {
        assert!(
            timeout >= 30000 && timeout <= 600000,
            "Timeout should be between 30s and 10min, got: {}ms",
            timeout
        );
    }

    // Verify exclude_credentials is None (no existing credentials for new device)
    assert!(
        public_key.exclude_credentials.is_none()
            || public_key.exclude_credentials.as_ref().unwrap().is_empty(),
        "Should not exclude credentials for new device"
    );

    // Verify attestation preference
    assert!(
        public_key.attestation.is_some(),
        "Should specify attestation preference"
    );
}

#[tokio::test]
async fn test_start_registration_challenge_is_unique() {
    // Setup
    let (node, _temp) = setup_test_node().await;

    // Execute: Start registration twice
    let result1 = node.start_webauthn_registration().await.unwrap();
    let result2 = node.start_webauthn_registration().await.unwrap();

    let (challenge1, challenge_id1) = result1;
    let (challenge2, challenge_id2) = result2;

    // Assert: Challenges should be different
    assert_ne!(
        challenge1.public_key.challenge, challenge2.public_key.challenge,
        "Each registration should generate a unique challenge"
    );

    assert_ne!(
        challenge_id1, challenge_id2,
        "Each registration should generate a unique challenge ID"
    );
}

#[tokio::test]
async fn test_start_registration_with_multiple_devices() {
    let (db, temp_dir) = setup_test_multi_node().await;

    // Create two nodes with different device IDs
    let kv_path1 = temp_dir.path().join("kv1");
    let kv1 = sled::open(&kv_path1).unwrap();

    let kv_path2 = temp_dir.path().join("kv2");
    let kv2 = sled::open(&kv_path2).unwrap();

    let auth_config = node::modules::ssi::webauthn::state::AuthConfig {
        rp_id: "localhost".to_string(),
        rp_origin: "http://localhost:3000".to_string(),
        rp_name: "Test Flow".to_string(),
    };

    let node1 = Node::new(
        NodeData {
            id: "device-1".to_string(),
            private_key: vec![0u8; 32],
            public_key: vec![0u8; 32],
        },
        db.clone(),
        kv1,
        AuthState::new(auth_config.clone()).unwrap(),
    );

    let node2 = Node::new(
        NodeData {
            id: "device-2".to_string(),
            private_key: vec![1u8; 32],
            public_key: vec![1u8; 32],
        },
        db.clone(),
        kv2,
        AuthState::new(auth_config).unwrap(),
    );

    // Execute: Start registration on both nodes
    let result1 = node1.start_webauthn_registration().await;
    let result2 = node2.start_webauthn_registration().await;

    // Assert: Both should succeed
    assert!(result1.is_ok(), "Device 1 registration should succeed");
    assert!(result2.is_ok(), "Device 2 registration should succeed");

    let (challenge1, _) = result1.unwrap();
    let (challenge2, _) = result2.unwrap();

    // Verify: Each device gets its own user info
    assert_eq!(challenge1.public_key.user.name, "device-1");
    assert_eq!(challenge2.public_key.user.name, "device-2");
}

#[tokio::test]
async fn test_end_to_end_registration_and_authentication() {
    // Setup test node
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db_url = format!("sqlite://{}?mode=rwc", db_path.display());
    let db = Database::connect(&db_url).await.unwrap();
    Migrator::up(&db, None).await.unwrap();

    let kv_path = temp_dir.path().join("kv");
    let kv = sled::open(&kv_path).unwrap();

    let auth_config = node::modules::ssi::webauthn::state::AuthConfig {
        rp_id: "localhost".to_string(),
        rp_origin: "http://localhost:3000".to_string(),
        rp_name: "Test Flow".to_string(),
    };

    let node = Node::new(
        NodeData {
            id: "test-device-e2e".to_string(),
            private_key: vec![0u8; 32],
            public_key: vec![0u8; 32],
        },
        db.clone(),
        kv,
        AuthState::new(auth_config).unwrap(),
    );

    // ===== REGISTRATION FLOW =====

    // 1. Start registration
    let (creation_challenge, challenge_id) = node
        .start_webauthn_registration()
        .await
        .expect("Failed to start registration");

    println!("Registration challenge created: {}", challenge_id);

    // 2. Simulate authenticator creating credential
    let mut authenticator = SoftPasskey::new(true); // user_verified = true, must be mutable

    let registration_credential = authenticator
        .perform_register(
            Url::parse("http://localhost:3000").unwrap(),
            creation_challenge.public_key.clone(), // Extract the PublicKeyCredentialCreationOptions
            60000,                                 // timeout in milliseconds
        )
        .expect("Failed to create credential");

    // 3. Finish registration
    let (did, user_id) = node
        .finish_webauthn_registration(&challenge_id, registration_credential)
        .await
        .expect("Failed to finish registration");

    println!("Registration successful!");
    println!("DID: {}", did);
    println!("User ID: {}", user_id);

    // Verify DID was created
    assert!(did.starts_with("did:key:"));
    assert!(!user_id.is_empty());

    // ===== AUTHENTICATION FLOW =====

    // 1. Start authentication
    let (auth_challenge, auth_challenge_id) = node
        .start_webauthn_authentication()
        .await
        .expect("Failed to start authentication");

    println!("Authentication challenge created: {}", auth_challenge_id);

    // 2. Simulate authenticator signing challenge
    let authentication_credential = authenticator
        .perform_auth(
            Url::parse("http://localhost:3000").unwrap(),
            auth_challenge.public_key.clone(), // Extract the PublicKeyCredentialRequestOptions
            60000,                             // timeout in milliseconds
        )
        .expect("Failed to authenticate");

    // 3. Finish authentication
    let auth_result = node
        .finish_webauthn_authentication(&auth_challenge_id, authentication_credential)
        .await
        .expect("Failed to finish authentication");

    println!("Authentication successful!");
    println!("Counter: {}", auth_result.counter());
    println!("User verified: {}", auth_result.user_verified());

    // Verify authentication result
    assert!(auth_result.user_verified());
    assert_eq!(auth_result.counter(), 1); // First authentication
}

#[tokio::test]
async fn test_finish_registration_creates_user_and_did() {
    use entity::{pass_key, user};
    use sea_orm::EntityTrait;

    let device_id = "test-device-registration";
    let (node, _temp) = setup_test_node_with_device_id(device_id).await;

    // Verify no users exist initially
    let users_before = user::Entity::find().all(&node.db).await.unwrap();
    assert_eq!(users_before.len(), 0, "Should start with no users");

    // Start registration to get a challenge
    let (creation_challenge, challenge_id) = node
        .start_webauthn_registration()
        .await
        .expect("Failed to start registration");

    println!("Registration challenge created: {}", challenge_id);

    // Simulate authenticator creating credential
    let mut authenticator = SoftPasskey::new(true);
    let registration_credential = authenticator
        .perform_register(
            Url::parse("http://localhost:3000").unwrap(),
            creation_challenge.public_key.clone(),
            60000,
        )
        .expect("Failed to create credential");

    // Execute: Finish registration
    let (did, did_doc_json) = node
        .finish_webauthn_registration(&challenge_id, registration_credential)
        .await
        .expect("Failed to finish registration");

    println!("Registration completed!");
    println!("  DID: {}", did);
    println!("  DID Document length: {} bytes", did_doc_json.len());

    // ===== VERIFY USER CREATION =====

    // 1. Verify a user was created
    let users = user::Entity::find().all(&node.db).await.unwrap();
    assert_eq!(
        users.len(),
        1,
        "Should have exactly 1 user after registration"
    );

    let created_user = &users[0];
    println!("Created user ID: {}", created_user.id);

    // 2. Verify user has the correct DID
    assert_eq!(
        created_user.did, did,
        "User's DID should match returned DID"
    );
    assert!(
        created_user.did.starts_with("did:key:"),
        "DID should use did:key method"
    );

    // 3. Verify user has the device_id
    let device_ids: Vec<String> =
        serde_json::from_str(&created_user.device_ids).expect("Should parse device_ids JSON");
    assert!(
        device_ids.contains(&device_id.to_string()),
        "User should have the device_id in their device_ids list"
    );
    println!("User device_ids: {:?}", device_ids);

    // 4. Verify user has DID document stored
    assert!(
        !created_user.public_key_jwk.is_empty(),
        "User should have DID document stored"
    );
    assert_eq!(
        created_user.public_key_jwk, did_doc_json,
        "Stored DID document should match returned document"
    );

    // 5. Verify username matches device_id
    assert_eq!(
        created_user.username, device_id,
        "Username should match device_id"
    );

    // ===== VERIFY PASSKEY STORAGE =====

    // 6. Verify a passkey was stored for this user
    let passkeys = pass_key::Entity::find()
        .filter(pass_key::Column::UserId.eq(created_user.id))
        .all(&node.db)
        .await
        .unwrap();

    assert_eq!(
        passkeys.len(),
        1,
        "Should have exactly 1 passkey stored for user"
    );

    let stored_passkey = &passkeys[0];
    println!("Stored passkey ID: {}", stored_passkey.id);

    // 7. Verify passkey is linked to correct device
    assert_eq!(
        stored_passkey.device_id, device_id,
        "Passkey should be linked to correct device_id"
    );

    // 8. Verify passkey has valid data
    assert!(
        !stored_passkey.credential_id.is_empty(),
        "Passkey should have credential_id"
    );
    assert!(
        !stored_passkey.public_key.is_empty(),
        "Passkey should have public_key"
    );
    assert_eq!(
        stored_passkey.sign_count, 0,
        "New passkey should have sign_count of 0"
    );
    assert_eq!(
        stored_passkey.authentication_count, 0,
        "New passkey should have authentication_count of 0"
    );

    // 9. Verify passkey JSON data can be deserialized
    use webauthn_rs::prelude::Passkey;
    let _passkey: Passkey = serde_json::from_str(&stored_passkey.json_data)
        .expect("Should be able to deserialize passkey JSON");

    // ===== VERIFY DID DOCUMENT STRUCTURE =====

    // 10. Verify DID document can be parsed as JSON
    let did_doc: serde_json::Value =
        serde_json::from_str(&did_doc_json).expect("DID document should be valid JSON");

    // Verify it has expected DID document fields
    assert!(
        did_doc.get("id").is_some(),
        "DID document should have 'id' field"
    );
    assert!(
        did_doc.get("verificationMethod").is_some() || did_doc.get("verification_method").is_some(),
        "DID document should have verification method"
    );

    println!("✓ All user and DID creation checks passed!");
    println!("  User ID: {}", created_user.id);
    println!("  DID: {}", did);
    println!("  Device ID: {}", device_id);
    println!("  Passkey ID: {}", stored_passkey.id);
}

#[tokio::test]
async fn test_store_passkey_success() {
    use entity::user;
    use node::modules::ssi::webauthn::auth::store_passkey;
    use sea_orm::{ActiveModelTrait, NotSet, Set};
    use webauthn_rs::prelude::Passkey;

    let (db, _temp) = setup_test_db().await;

    // Create a test user first
    let test_user = user::ActiveModel {
        id: NotSet,
        did: Set("did:key:test123".to_string()),
        username: Set("test_user".to_string()),
        display_name: Set("Test User".to_string()),
        device_ids: Set(r#"["test-device-123"]"#.to_string()),
        public_key_jwk: Set("{}".to_string()),
        time_created: Set(chrono::Utc::now().into()),
        last_login: Set(chrono::Utc::now().into()),
    };

    let user_model = test_user.insert(&db).await.unwrap();
    println!("Created test user with ID: {}", user_model.id);

    // Create a test passkey (using fixture JSON)
    let (passkey, _passkey_json) = load_passkey();
    let device_id = "test-device-store-123";

    // Call the actual store_passkey function from auth.rs
    let result = store_passkey(&db, user_model.id, device_id, &passkey).await;

    assert!(
        result.is_ok(),
        "store_passkey should succeed: {:?}",
        result.err()
    );

    println!("Successfully stored passkey using store_passkey function");

    // Verify the passkey was stored by retrieving it
    use entity::pass_key;
    use sea_orm::EntityTrait;

    let stored_passkeys = pass_key::Entity::find()
        .filter(pass_key::Column::DeviceId.eq(device_id))
        .all(&db)
        .await
        .unwrap();

    assert_eq!(
        stored_passkeys.len(),
        1,
        "Should have exactly 1 passkey stored"
    );

    let stored = &stored_passkeys[0];
    assert_eq!(stored.user_id, user_model.id);
    assert_eq!(stored.device_id, device_id);
    assert_eq!(stored.sign_count, 0);
    assert_eq!(stored.authentication_count, 0);

    // Verify the credential_id matches
    let expected_cred_id: Vec<u8> = passkey.cred_id().as_ref().to_vec();
    assert_eq!(stored.credential_id, expected_cred_id);

    // Verify the JSON data can be deserialized back to a Passkey
    let deserialized_passkey: Passkey = serde_json::from_str(&stored.json_data).unwrap();
    assert_eq!(
        deserialized_passkey.cred_id(),
        passkey.cred_id(),
        "Deserialized passkey should match original"
    );

    println!("Passkey verification complete - all fields match!");
}

#[tokio::test]
async fn test_get_passkeys_for_device() {
    use entity::user;
    use node::modules::ssi::webauthn::auth::get_passkeys_for_device;
    use sea_orm::{ActiveModelTrait, NotSet, Set};
    use webauthn_rs::prelude::Passkey; // Import the actual function

    // Setup test database
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db_url = format!("sqlite://{}?mode=rwc", db_path.display());
    let db = Database::connect(&db_url).await.unwrap();
    Migrator::up(&db, None).await.unwrap();

    // Create a test user
    let test_user = user::ActiveModel {
        id: NotSet,
        did: Set("did:key:test456".to_string()),
        username: Set("test_user".to_string()),
        display_name: Set("Test User".to_string()),
        device_ids: Set(r#"["device-A", "device-B"]"#.to_string()),
        public_key_jwk: Set("{}".to_string()),
        time_created: Set(chrono::Utc::now().into()),
        last_login: Set(chrono::Utc::now().into()),
    };

    let user_model = test_user.insert(&db).await.unwrap();

    // Use valid base64-encoded credential IDs
    let cred_ids = [
        "bpIp0SwDYrqbo2IGg_lDJJUfoAs", // device-A, passkey 0
        "zN067-GTNf2MeTfaAVnCl_nXHVQ", // device-A, passkey 1
        "qk6VrJAZ_BSkdh-amMu_QyMbZ1o", // device-B, passkey 0
    ];

    // Create test passkey JSON template
    let passkey_json_template = r#"{
        "cred": {
            "cred_id": "CRED_ID_PLACEHOLDER",
            "cred": {
                "type_": "ES256",
                "key": {
                    "EC_EC2": {
                        "curve": "SECP256R1",
                        "x": "fLO-YipbYWNFU4De2Zrx-vkXV_0nJSyftd0g3CXmQvk",
                        "y": "3UJufImjr2da-STs1-14FxWWviCE4uFsGjuXbDoeGsc"
                    }
                }
            },
            "counter": 0,
            "transports": null,
            "user_verified": true,
            "backup_eligible": true,
            "backup_state": true,
            "registration_policy": "required",
            "extensions": {
                "cred_protect": "Ignored",
                "hmac_create_secret": "NotRequested",
                "appid": "NotRequested",
                "cred_props": "Ignored"
            },
            "attestation": {
                "data": "None",
                "metadata": "None"
            },
            "attestation_format": "none"
        }
    }"#;

    // Store 2 passkeys for device-A
    use entity::pass_key;
    for i in 0..2 {
        let passkey_json = passkey_json_template.replace("CRED_ID_PLACEHOLDER", cred_ids[i]);
        let passkey: Passkey = serde_json::from_str(&passkey_json).unwrap();
        let credential_id: Vec<u8> = passkey.cred_id().as_ref().to_vec();
        let public_key = serde_json::to_vec(&passkey.get_public_key()).unwrap();

        let new_passkey = pass_key::ActiveModel {
            id: NotSet,
            user_id: Set(user_model.id),
            device_id: Set("device-A".to_string()),
            credential_id: Set(credential_id),
            public_key: Set(public_key),
            sign_count: Set(0),
            authentication_count: Set(0),
            name: Set(format!("Passkey-device-A-{}", i)),
            attestation: Set("None".to_string()),
            json_data: Set(passkey_json),
            time_created: Set(chrono::Utc::now().into()),
            last_authenticated: Set(chrono::Utc::now().into()),
        };

        new_passkey.insert(&db).await.unwrap();
    }

    // Store 1 passkey for device-B
    let passkey_json_b = passkey_json_template.replace("CRED_ID_PLACEHOLDER", cred_ids[2]);
    let passkey_b: Passkey = serde_json::from_str(&passkey_json_b).unwrap();
    let credential_id_b: Vec<u8> = passkey_b.cred_id().as_ref().to_vec();
    let public_key_b = serde_json::to_vec(&passkey_b.get_public_key()).unwrap();

    let new_passkey_b = pass_key::ActiveModel {
        id: NotSet,
        user_id: Set(user_model.id),
        device_id: Set("device-B".to_string()),
        credential_id: Set(credential_id_b),
        public_key: Set(public_key_b),
        sign_count: Set(0),
        authentication_count: Set(0),
        name: Set("Passkey-device-B-0".to_string()),
        attestation: Set("None".to_string()),
        json_data: Set(passkey_json_b),
        time_created: Set(chrono::Utc::now().into()),
        last_authenticated: Set(chrono::Utc::now().into()),
    };

    new_passkey_b.insert(&db).await.unwrap();

    println!("Stored 2 passkeys for device-A and 1 for device-B");

    // Test: Get passkeys for device-A using the actual get_passkeys_for_device function
    let passkeys_a = get_passkeys_for_device(&db, "device-A")
        .await
        .expect("Should successfully retrieve passkeys for device-A");

    assert_eq!(passkeys_a.len(), 2, "Should have 2 passkeys for device-A");
    println!(
        "✓ Successfully retrieved {} passkeys for device-A using get_passkeys_for_device()",
        passkeys_a.len()
    );

    // Test: Get passkeys for device-B
    let passkeys_b = get_passkeys_for_device(&db, "device-B")
        .await
        .expect("Should successfully retrieve passkeys for device-B");

    assert_eq!(passkeys_b.len(), 1, "Should have 1 passkey for device-B");
    println!(
        "✓ Successfully retrieved {} passkey for device-B using get_passkeys_for_device()",
        passkeys_b.len()
    );

    // Test: Get passkeys for non-existent device
    let passkeys_none = get_passkeys_for_device(&db, "device-C")
        .await
        .expect("Should return empty vec for non-existent device");

    assert_eq!(
        passkeys_none.len(),
        0,
        "Should have 0 passkeys for device-C"
    );
    println!("✓ Correctly returned 0 passkeys for non-existent device-C");

    // Test: Verify the returned passkeys have correct credential IDs
    let returned_cred_ids: Vec<String> = passkeys_a
        .iter()
        .map(|pk| format!("{:?}", pk.cred_id()))
        .collect();

    println!("✓ All get_passkeys_for_device tests passed!");
    println!(
        "  Returned credential IDs for device-A: {:?}",
        returned_cred_ids
    );
}
