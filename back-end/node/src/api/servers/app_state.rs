use crate::api::node::Node;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct AppState {
    pub node: Arc<RwLock<Node>>,
}

impl AppState {
    pub fn new(node: Node) -> Self {
        Self {
            node: Arc::new(RwLock::new(node)),
        }
    }
}
