use thiserror::Error;

use crate::btree::BPlusTreeError;
use crate::file::FileError;

/// Result type for index operations
pub type IndexResult<T> = Result<T, IndexError>;

/// Errors that can occur during index operations
#[derive(Debug, Error)]
pub enum IndexError {
    #[error("File error: {0}")]
    FileError(#[from] FileError),

    #[error("B+ tree error: {0}")]
    BPlusTreeError(#[from] BPlusTreeError),

    #[error("Invalid magic number in index file header")]
    InvalidMagic,

    #[error("Unsupported index file version: {0}")]
    UnsupportedVersion(u32),

    #[error("Corrupted node data at page {0}")]
    CorruptedNode(usize),

    #[error("Index file does not exist: {0}")]
    IndexNotFound(String),

    #[error("Index already exists: {0}")]
    IndexAlreadyExists(String),

    #[error("Invalid node type: {0}")]
    InvalidNodeType(u8),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Deserialization error: {0}")]
    DeserializationError(String),

    #[error("Index not open: {0}")]
    IndexNotOpen(String),
}
