use webauthn_rs::prelude::Passkey;

pub fn load_es256_passkey() -> (Passkey, String) {
    let passkey_json = r#"{
        "cred": {
            "cred_id": "bpIp0SwDYrqbo2IGg_lDJJUfoAs",
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

    (
        serde_json::from_str(passkey_json).unwrap(),
        passkey_json.to_owned(),
    )
}

pub fn load_eddsa_passkey() -> (Passkey, String) {
    let passkey_json = r#"{
            "cred": {
                "cred_id": "eddsa-test-credential-id",
                "cred": {
                    "type_": "EDDSA",
                    "key": {
                        "EC_OKP": {
                            "curve": "ED25519",
                            "x": "11qYAYKxCrfVS_7TyWQHOg7hcvPapiMlrwIaaPcHURo"
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

    (
        serde_json::from_str(passkey_json).unwrap(),
        passkey_json.to_owned(),
    )
}
