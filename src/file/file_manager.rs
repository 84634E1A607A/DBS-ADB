use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use super::error::{FileError, FileResult};
use super::{PAGE_SIZE, PageId};

/// Handle to an open file
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FileHandle(usize);

impl FileHandle {
    pub fn as_usize(&self) -> usize {
        self.0
    }
}

/// Manages paged file operations
pub struct PagedFileManager {
    /// Map from file handles to open files
    open_files: HashMap<FileHandle, FileEntry>,
    /// Map from file paths to handles (for checking if already open)
    path_to_handle: HashMap<PathBuf, FileHandle>,
    /// Next available file handle
    next_handle: usize,
    /// Maximum number of open files
    max_open_files: usize,
}

struct FileEntry {
    file: File,
    path: PathBuf,
}

impl PagedFileManager {
    /// Create a new paged file manager
    pub fn new() -> Self {
        Self::with_max_files(128)
    }

    /// Create a new paged file manager with specified max open files
    pub fn with_max_files(max_open_files: usize) -> Self {
        Self {
            open_files: HashMap::new(),
            path_to_handle: HashMap::new(),
            next_handle: 0,
            max_open_files,
        }
    }

    /// Create a new file
    pub fn create_file<P: AsRef<Path>>(&mut self, path: P) -> FileResult<()> {
        let path = path.as_ref();

        if path.exists() {
            return Err(FileError::FileAlreadyExists(path.display().to_string()));
        }

        // Create parent directories if they don't exist
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Create the file
        File::create(path)?;
        Ok(())
    }

    /// Open an existing file
    pub fn open_file<P: AsRef<Path>>(&mut self, path: P) -> FileResult<FileHandle> {
        let path_ref = path.as_ref();
        let path = path_ref
            .canonicalize()
            .map_err(|_| FileError::FileNotFound(path_ref.display().to_string()))?;

        // Check if file is already open
        if let Some(&handle) = self.path_to_handle.get(&path) {
            return Ok(handle);
        }

        // Check if we've reached the max open files limit
        if self.open_files.len() >= self.max_open_files {
            return Err(FileError::TooManyOpenFiles);
        }

        // Open the file for reading and writing
        let file = OpenOptions::new().read(true).write(true).open(&path)?;

        let handle = FileHandle(self.next_handle);
        self.next_handle += 1;

        self.open_files.insert(
            handle,
            FileEntry {
                file,
                path: path.clone(),
            },
        );
        self.path_to_handle.insert(path, handle);

        Ok(handle)
    }

    /// Close a file
    pub fn close_file(&mut self, handle: FileHandle) -> FileResult<()> {
        let entry = self
            .open_files
            .remove(&handle)
            .ok_or(FileError::InvalidHandle(handle.0))?;

        self.path_to_handle.remove(&entry.path);
        Ok(())
    }

    /// Remove (delete) a file
    pub fn remove_file<P: AsRef<Path>>(&mut self, path: P) -> FileResult<()> {
        let path = path.as_ref();

        // If file is open, close it first
        if let Ok(canonical_path) = path.canonicalize() {
            if let Some(&handle) = self.path_to_handle.get(&canonical_path) {
                self.close_file(handle)?;
            }
        }

        std::fs::remove_file(path)?;
        Ok(())
    }

    /// Read a page from a file
    pub fn read_page(
        &mut self,
        handle: FileHandle,
        page_id: PageId,
        buffer: &mut [u8],
    ) -> FileResult<()> {
        if buffer.len() != PAGE_SIZE {
            return Err(FileError::InvalidPageSize {
                expected: PAGE_SIZE,
                actual: buffer.len(),
            });
        }

        let entry = self
            .open_files
            .get_mut(&handle)
            .ok_or(FileError::InvalidHandle(handle.0))?;

        let offset = (page_id * PAGE_SIZE) as u64;
        entry.file.seek(SeekFrom::Start(offset))?;

        let bytes_read = entry.file.read(buffer)?;

        // If we read less than PAGE_SIZE, fill the rest with zeros
        if bytes_read < PAGE_SIZE {
            buffer[bytes_read..].fill(0);
        }

        Ok(())
    }

    /// Write a page to a file
    pub fn write_page(
        &mut self,
        handle: FileHandle,
        page_id: PageId,
        buffer: &[u8],
    ) -> FileResult<()> {
        if buffer.len() != PAGE_SIZE {
            return Err(FileError::InvalidPageSize {
                expected: PAGE_SIZE,
                actual: buffer.len(),
            });
        }

        let entry = self
            .open_files
            .get_mut(&handle)
            .ok_or(FileError::InvalidHandle(handle.0))?;

        let offset = (page_id * PAGE_SIZE) as u64;
        let required_size = offset + PAGE_SIZE as u64;
        
        // Extend file if necessary to ensure we can write at this offset
        let current_size = entry.file.metadata()?.len();
        if current_size < required_size {
            entry.file.set_len(required_size)?;
        }
        
        entry.file.seek(SeekFrom::Start(offset))?;
        entry.file.write_all(buffer)?;
        // Note: Don't sync on every write - let the OS buffer and batch writes
        // Sync will be called by flush_all() or when buffer manager drops

        Ok(())
    }

    /// Get the number of pages in a file
    pub fn get_page_count(&mut self, handle: FileHandle) -> FileResult<usize> {
        let entry = self
            .open_files
            .get_mut(&handle)
            .ok_or(FileError::InvalidHandle(handle.0))?;

        let file_size = entry.file.metadata()?.len();
        let page_count = file_size.div_ceil(PAGE_SIZE as u64) as usize;
        Ok(page_count)
    }

    /// Sync a file to disk (flush all OS buffers)
    pub fn sync_file(&mut self, handle: FileHandle) -> FileResult<()> {
        let entry = self
            .open_files
            .get_mut(&handle)
            .ok_or(FileError::InvalidHandle(handle.0))?;

        entry.file.sync_data()?;
        Ok(())
    }

    /// Sync all open files to disk
    pub fn sync_all(&mut self) -> FileResult<()> {
        for entry in self.open_files.values_mut() {
            entry.file.sync_data()?;
        }
        Ok(())
    }

    /// Check if a file is open
    pub fn is_file_open(&self, handle: FileHandle) -> bool {
        self.open_files.contains_key(&handle)
    }

    /// Get the number of currently open files
    pub fn open_file_count(&self) -> usize {
        self.open_files.len()
    }
}

impl Default for PagedFileManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup_test_dir() -> TempDir {
        tempfile::tempdir().unwrap()
    }

    #[test]
    fn test_create_file() {
        let temp_dir = setup_test_dir();
        let test_file = temp_dir.path().join("test.db");
        let mut manager = PagedFileManager::new();

        assert!(manager.create_file(&test_file).is_ok());
        assert!(test_file.exists());
    }

    #[test]
    fn test_create_file_already_exists() {
        let temp_dir = setup_test_dir();
        let test_file = temp_dir.path().join("test.db");
        let mut manager = PagedFileManager::new();

        manager.create_file(&test_file).unwrap();
        let result = manager.create_file(&test_file);
        assert!(matches!(result, Err(FileError::FileAlreadyExists(_))));
    }

    #[test]
    fn test_open_close_file() {
        let temp_dir = setup_test_dir();
        let test_file = temp_dir.path().join("test.db");
        let mut manager = PagedFileManager::new();

        manager.create_file(&test_file).unwrap();
        let handle = manager.open_file(&test_file).unwrap();
        assert!(manager.is_file_open(handle));

        manager.close_file(handle).unwrap();
        assert!(!manager.is_file_open(handle));
    }

    #[test]
    fn test_open_nonexistent_file() {
        let temp_dir = setup_test_dir();
        let test_file = temp_dir.path().join("nonexistent.db");
        let mut manager = PagedFileManager::new();

        let result = manager.open_file(&test_file);
        assert!(matches!(result, Err(FileError::FileNotFound(_))));
    }

    #[test]
    fn test_open_same_file_twice() {
        let temp_dir = setup_test_dir();
        let test_file = temp_dir.path().join("test.db");
        let mut manager = PagedFileManager::new();

        manager.create_file(&test_file).unwrap();
        let handle1 = manager.open_file(&test_file).unwrap();
        let handle2 = manager.open_file(&test_file).unwrap();

        assert_eq!(handle1, handle2);
        assert_eq!(manager.open_file_count(), 1);
    }

    #[test]
    fn test_read_write_page() {
        let temp_dir = setup_test_dir();
        let test_file = temp_dir.path().join("test.db");
        let mut manager = PagedFileManager::new();

        manager.create_file(&test_file).unwrap();
        let handle = manager.open_file(&test_file).unwrap();

        // Write a page
        let mut write_buffer = vec![0u8; PAGE_SIZE];
        write_buffer[0] = 42;
        write_buffer[100] = 99;
        write_buffer[PAGE_SIZE - 1] = 255;

        manager.write_page(handle, 0, &write_buffer).unwrap();

        // Read the page back
        let mut read_buffer = vec![0u8; PAGE_SIZE];
        manager.read_page(handle, 0, &mut read_buffer).unwrap();

        assert_eq!(read_buffer, write_buffer);
    }

    #[test]
    fn test_write_multiple_pages() {
        let temp_dir = setup_test_dir();
        let test_file = temp_dir.path().join("test.db");
        let mut manager = PagedFileManager::new();

        manager.create_file(&test_file).unwrap();
        let handle = manager.open_file(&test_file).unwrap();

        // Write multiple pages
        for page_id in 0..10 {
            let mut buffer = vec![0u8; PAGE_SIZE];
            buffer[0] = page_id as u8;
            manager.write_page(handle, page_id, &buffer).unwrap();
        }

        // Read them back and verify
        for page_id in 0..10 {
            let mut buffer = vec![0u8; PAGE_SIZE];
            manager.read_page(handle, page_id, &mut buffer).unwrap();
            assert_eq!(buffer[0], page_id as u8);
        }
    }

    #[test]
    fn test_read_nonexistent_page() {
        let temp_dir = setup_test_dir();
        let test_file = temp_dir.path().join("test.db");
        let mut manager = PagedFileManager::new();

        manager.create_file(&test_file).unwrap();
        let handle = manager.open_file(&test_file).unwrap();

        // Reading a page that doesn't exist should return zeros
        let mut buffer = vec![0u8; PAGE_SIZE];
        manager.read_page(handle, 100, &mut buffer).unwrap();
        assert!(buffer.iter().all(|&b| b == 0));
    }

    #[test]
    fn test_get_page_count() {
        let temp_dir = setup_test_dir();
        let test_file = temp_dir.path().join("test.db");
        let mut manager = PagedFileManager::new();

        manager.create_file(&test_file).unwrap();
        let handle = manager.open_file(&test_file).unwrap();

        assert_eq!(manager.get_page_count(handle).unwrap(), 0);

        // Write some pages
        let buffer = vec![0u8; PAGE_SIZE];
        manager.write_page(handle, 0, &buffer).unwrap();
        assert_eq!(manager.get_page_count(handle).unwrap(), 1);

        manager.write_page(handle, 1, &buffer).unwrap();
        assert_eq!(manager.get_page_count(handle).unwrap(), 2);

        manager.write_page(handle, 5, &buffer).unwrap();
        assert_eq!(manager.get_page_count(handle).unwrap(), 6);
    }

    #[test]
    fn test_remove_file() {
        let temp_dir = setup_test_dir();
        let test_file = temp_dir.path().join("test.db");
        let mut manager = PagedFileManager::new();

        manager.create_file(&test_file).unwrap();
        assert!(test_file.exists());

        manager.remove_file(&test_file).unwrap();
        assert!(!test_file.exists());
    }

    #[test]
    fn test_remove_open_file() {
        let temp_dir = setup_test_dir();
        let test_file = temp_dir.path().join("test.db");
        let mut manager = PagedFileManager::new();

        manager.create_file(&test_file).unwrap();
        let handle = manager.open_file(&test_file).unwrap();
        assert!(manager.is_file_open(handle));

        manager.remove_file(&test_file).unwrap();
        assert!(!test_file.exists());
        assert!(!manager.is_file_open(handle));
    }

    #[test]
    fn test_invalid_buffer_size() {
        let temp_dir = setup_test_dir();
        let test_file = temp_dir.path().join("test.db");
        let mut manager = PagedFileManager::new();

        manager.create_file(&test_file).unwrap();
        let handle = manager.open_file(&test_file).unwrap();

        let mut small_buffer = vec![0u8; PAGE_SIZE - 1];
        let result = manager.read_page(handle, 0, &mut small_buffer);
        assert!(matches!(result, Err(FileError::InvalidPageSize { .. })));

        let large_buffer = vec![0u8; PAGE_SIZE + 1];
        let result = manager.write_page(handle, 0, &large_buffer);
        assert!(matches!(result, Err(FileError::InvalidPageSize { .. })));
    }

    #[test]
    fn test_max_open_files() {
        let temp_dir = setup_test_dir();
        let mut manager = PagedFileManager::with_max_files(2);

        let file1 = temp_dir.path().join("test1.db");
        let file2 = temp_dir.path().join("test2.db");
        let file3 = temp_dir.path().join("test3.db");

        manager.create_file(&file1).unwrap();
        manager.create_file(&file2).unwrap();
        manager.create_file(&file3).unwrap();

        manager.open_file(&file1).unwrap();
        manager.open_file(&file2).unwrap();

        let result = manager.open_file(&file3);
        assert!(matches!(result, Err(FileError::TooManyOpenFiles)));
    }
}
