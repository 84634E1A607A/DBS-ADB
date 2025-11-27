use std::io;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum FileError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("File not found: {0}")]
    FileNotFound(String),

    #[error("File already exists: {0}")]
    FileAlreadyExists(String),

    #[error("Invalid file handle: {0}")]
    InvalidHandle(usize),

    #[error("Page not found: page_id={0}")]
    PageNotFound(usize),

    #[error("Buffer pool is full")]
    BufferPoolFull,

    #[error("Invalid page size: expected {expected}, got {actual}")]
    InvalidPageSize { expected: usize, actual: usize },

    #[error("File handle limit reached")]
    TooManyOpenFiles,
}

pub type FileResult<T> = Result<T, FileError>;
