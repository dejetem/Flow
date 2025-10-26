mod document;
mod error;
pub mod generator;
mod parser;

use document::create_did_document;
pub use error::PeerDidError;
use parser::ParsedPeerDid;

use crate::modules::ssi::did::resolvers::types::{ResolutionError, ResolutionResult};
use crate::modules::ssi::did::types::{
    DocumentMetadata, RegistryProof, ResolutionMetadata, ResolutionOptions, VdrInfo,
};
use chrono::Utc;

/// Resolve a did:peer DID
pub async fn resolve_peer_did(
    did: &str,
    _options: &ResolutionOptions,
) -> Result<ResolutionResult, ResolutionError> {
    let start = std::time::Instant::now();

    // Parse the did:peer
    let parsed = ParsedPeerDid::parse(did)?;

    // Create DID Document
    let document = create_did_document(did, parsed)?;

    let duration_ms = start.elapsed().as_millis() as u64;

    // Build resolution metadata
    let did_resolution_metadata = ResolutionMetadata {
        content_type: Some("application/did+json".to_string()),
        error: None,
        verifiable_data_registry: Some(VdrInfo {
            registry_type: "peer-to-peer".to_string(),
            registry_endpoint: None,
            verified: true,
            registry_proof: Some(RegistryProof::CryptographicProof {
                signature: "self-certifying".to_string(),
                signature_algorithm: "embedded-peer".to_string(),
                public_key_id: did.to_string(),
                signed_data: did.to_string(),
            }),
            registry_version: Some("did:peer:2".to_string()),
        }),
        duration: Some(duration_ms),
        from_cache: Some(false),
        cache_ttl: None, // Local only, cache indefinitely. #TODO: extend this...
        resolved_at: Some(Utc::now()),
        did_method: Some("peer".to_string()),
        additional: None,
    };

    Ok(ResolutionResult {
        did_document: Some(document),
        did_resolution_metadata,
        did_document_metadata: DocumentMetadata::default(),
    })
}
