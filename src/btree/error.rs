use thiserror::Error;

use super::BPlusKey;
use super::node::NodeId;

/// Errors that can occur during B+ tree operations
#[derive(Debug, Clone, Error)]
pub enum BPlusTreeError {
    #[error("Key not found: {0}")]
    KeyNotFound(BPlusKey),

    #[error("Entry not found: key={0}")]
    EntryNotFound(BPlusKey),

    #[error("Invalid tree state: {0}")]
    InvalidState(String),

    #[error("Node not found: {0}")]
    NodeNotFound(NodeId),

    #[error("Invalid order: {0} (must be >= 3)")]
    InvalidOrder(usize),
}

pub type BPlusTreeResult<T> = Result<T, BPlusTreeError>;
