mod buffer_manager;
mod error;
mod file_manager;

pub use buffer_manager::BufferManager;
pub use error::{FileError, FileResult};
pub use file_manager::{FileHandle, PagedFileManager};

/// Page size in bytes (8KB)
pub const PAGE_SIZE: usize = 8192;

/// Number of pages in the buffer pool
/// With 3000 pages × 8KB = 24MB, leaving room for other data structures
/// in systems with 256MB memory limit
pub const BUFFER_POOL_SIZE: usize = 10000;

/// Page ID type
pub type PageId = usize;
