use crate::file::FileError;
use std::io;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RecordError {
    #[error("File error: {0}")]
    File(#[from] FileError),

    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("Table not open: {0}")]
    TableNotOpen(String),

    #[error("Invalid record: {0}")]
    InvalidRecord(String),

    #[error("Invalid slot: page_id={0}, slot_id={1}")]
    InvalidSlot(usize, usize),

    #[error("Page full: page_id={0}")]
    PageFull(usize),

    #[error("No free pages available")]
    NoFreePages,

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Deserialization error: {0}")]
    Deserialization(String),

    #[error("Schema mismatch: {0}")]
    SchemaMismatch(String),

    #[error("NULL value for NOT NULL column: {0}")]
    NullConstraintViolation(String),

    #[error("Type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },
}

pub type RecordResult<T> = Result<T, RecordError>;
