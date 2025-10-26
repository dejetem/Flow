use thiserror::Error;

#[derive(Debug, Error)]
pub enum PeerDidError {
    #[error("Invalid did:peer format")]
    InvalidFormat,

    #[error("Unsupported numalgo: {0}")]
    UnsupportedNumalgo(u8),

    #[error("Invalid encoding: {0}")]
    InvalidEncoding(String),

    #[error("Unsupported key type")]
    UnsupportedKeyType,

    #[error("Multibase decode error: {0}")]
    MultibaseError(#[from] multibase::Error),

    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("Base64 error: {0}")]
    Base64Error(#[from] base64::DecodeError),

    #[error("DID parse error: {0}")]
    DidParseError(String),
}

impl From<PeerDidError> for crate::modules::ssi::did::resolvers::types::ResolutionError {
    fn from(err: PeerDidError) -> Self {
        match err {
            PeerDidError::InvalidFormat => {
                crate::modules::ssi::did::resolvers::types::ResolutionError::InvalidDid(
                    err.to_string(),
                )
            }
            PeerDidError::UnsupportedNumalgo(_) => {
                crate::modules::ssi::did::resolvers::types::ResolutionError::MethodNotSupported(
                    "peer".to_string(),
                )
            }
            _ => crate::modules::ssi::did::resolvers::types::ResolutionError::ResolutionFailed(
                err.to_string(),
            ),
        }
    }
}
