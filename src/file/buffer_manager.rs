use lru::LruCache;
use std::num::NonZeroUsize;

use super::error::{FileError, FileResult};
use super::file_manager::{FileHandle, PagedFileManager};
use super::{BUFFER_POOL_SIZE, PAGE_SIZE, PageId};

/// A key identifying a page in the buffer pool
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct BufferKey {
    file: FileHandle,
    page_id: PageId,
}

/// Entry in the buffer pool
struct BufferEntry {
    /// The actual page data
    data: Vec<u8>,
    /// Whether this page has been modified
    dirty: bool,
}

/// Manages a buffer pool with LRU eviction policy
pub struct BufferManager {
    /// Underlying file manager
    file_manager: PagedFileManager,
    /// Combined buffer pool and LRU tracker: single data structure for both storage and eviction policy
    /// This eliminates redundant hash lookups - every operation now hits only ONE hash table
    buffer_pool: LruCache<BufferKey, BufferEntry>,
    /// Maximum size of the buffer pool
    max_pool_size: usize,
    /// Reusable buffer for loading pages (avoids allocation on every load)
    load_buffer: Vec<u8>,
}

impl BufferManager {
    /// Create a new buffer manager
    pub fn new(file_manager: PagedFileManager) -> Self {
        Self::with_capacity(file_manager, BUFFER_POOL_SIZE)
    }

    /// Create a new buffer manager with specified capacity
    pub fn with_capacity(file_manager: PagedFileManager, capacity: usize) -> Self {
        Self {
            file_manager,
            buffer_pool: LruCache::new(NonZeroUsize::new(capacity).unwrap()),
            max_pool_size: capacity,
            load_buffer: vec![0u8; PAGE_SIZE], // Allocate once, reuse for all page loads
        }
    }

    /// Get a reference to the file manager
    pub fn file_manager(&self) -> &PagedFileManager {
        &self.file_manager
    }

    /// Get a mutable reference to the file manager
    pub fn file_manager_mut(&mut self) -> &mut PagedFileManager {
        &mut self.file_manager
    }

    /// Get a page from the buffer pool, loading it from disk if necessary
    pub fn get_page(&mut self, file: FileHandle, page_id: PageId) -> FileResult<&[u8]> {
        let key = BufferKey { file, page_id };

        // Try to get the page - single hash lookup with peek to avoid borrow issues
        if self.buffer_pool.peek(&key).is_none() {
            // Page not in buffer, need to load it
            self.load_page(file, page_id)?;
        }

        // Now get the page (this updates LRU automatically)
        Ok(&self.buffer_pool.get(&key).unwrap().data)
    }

    /// Get a mutable reference to a page, loading it if necessary
    /// This automatically marks the page as dirty
    pub fn get_page_mut(&mut self, file: FileHandle, page_id: PageId) -> FileResult<&mut [u8]> {
        let key = BufferKey { file, page_id };

        // Try to get the page first - single hash lookup
        if self.buffer_pool.get_mut(&key).is_none() {
            // Page not in buffer, load it
            self.load_page(file, page_id)?;
        }

        // Get mutable reference again (this updates LRU automatically)
        let entry = self.buffer_pool.get_mut(&key).unwrap();
        entry.dirty = true;
        Ok(&mut entry.data)
    }

    /// Mark a page as dirty (modified)
    pub fn mark_dirty(&mut self, file: FileHandle, page_id: PageId) -> FileResult<()> {
        let key = BufferKey { file, page_id };

        let entry = self
            .buffer_pool
            .get_mut(&key)
            .ok_or(FileError::PageNotFound(page_id))?;

        entry.dirty = true;
        Ok(())
    }

    /// Flush a specific page to disk if it's dirty
    pub fn flush_page(&mut self, file: FileHandle, page_id: PageId) -> FileResult<()> {
        let key = BufferKey { file, page_id };

        // Single peek operation - doesn't update LRU since we're just flushing
        if let Some(entry) = self.buffer_pool.peek_mut(&key)
            && entry.dirty
        {
            self.file_manager.write_page(file, page_id, &entry.data)?;
            entry.dirty = false;
        }

        Ok(())
    }

    /// Flush all dirty pages to disk
    pub fn flush_all(&mut self) -> FileResult<()> {
        // Iterate and flush all dirty pages without collecting keys first
        // Use iter() to avoid updating LRU order during flush
        let mut dirty_pages = Vec::new();

        for (key, entry) in self.buffer_pool.iter() {
            if entry.dirty {
                dirty_pages.push(*key);
            }
        }

        for key in dirty_pages {
            // Use peek_mut to avoid LRU update during flush
            if let Some(entry) = self.buffer_pool.peek_mut(&key)
                && entry.dirty
            {
                self.file_manager
                    .write_page(key.file, key.page_id, &entry.data)?;
                entry.dirty = false;
            }
        }

        // Sync all files to ensure data is persisted to disk
        self.file_manager.sync_all()?;

        Ok(())
    }

    /// Flush all dirty pages and clear the entire buffer pool
    /// This releases all cached memory - use when memory is constrained
    pub fn flush_and_clear(&mut self) -> FileResult<()> {
        // Flush all dirty pages first
        self.flush_all()?;

        // Clear the combined buffer pool and LRU cache
        self.buffer_pool.clear();

        Ok(())
    }

    /// Remove a page from the buffer pool
    pub fn evict_page(&mut self, file: FileHandle, page_id: PageId) -> FileResult<()> {
        let key = BufferKey { file, page_id };

        // Only evict if the page is actually in the buffer
        if self.buffer_pool.peek(&key).is_some() {
            // Flush if dirty
            self.flush_page(file, page_id)?;

            // Remove from buffer pool (LRU is automatically updated)
            self.buffer_pool.pop(&key);
        }

        Ok(())
    }

    /// Load a page from disk into the buffer pool
    fn load_page(&mut self, file: FileHandle, page_id: PageId) -> FileResult<()> {
        let key = BufferKey { file, page_id };

        // Check if buffer pool is full - evict until we have space
        while self.buffer_pool.len() >= self.max_pool_size {
            self.evict_lru_page()?;
        }

        // Ensure load_buffer has correct capacity (in case it was never initialized or shrunk)
        if self.load_buffer.len() != PAGE_SIZE {
            self.load_buffer = vec![0u8; PAGE_SIZE];
        }

        // Load page from disk into reusable buffer (no allocation!)
        self.file_manager
            .read_page(file, page_id, &mut self.load_buffer)?;

        // Move the loaded data into buffer pool using mem::take
        // This swaps ownership without copying - the buffer moves into the pool
        // and we get back an empty Vec (which will be replaced on next eviction or reused)
        let data = std::mem::take(&mut self.load_buffer);

        // Single operation: insert into LRU cache (which handles eviction automatically)
        self.buffer_pool
            .put(key, BufferEntry { data, dirty: false });

        Ok(())
    }

    /// Evict the least recently used page from the buffer pool
    fn evict_lru_page(&mut self) -> FileResult<()> {
        // Pop LRU entry from the cache (single operation)
        if let Some((key, entry)) = self.buffer_pool.pop_lru() {
            // Flush if dirty before evicting
            if entry.dirty {
                self.file_manager
                    .write_page(key.file, key.page_id, &entry.data)?;
            }

            // Recycle the evicted buffer for future page loads (avoid allocation)
            self.load_buffer = entry.data;
        }

        Ok(())
    }

    /// Get the number of pages currently in the buffer pool
    pub fn buffer_pool_size(&self) -> usize {
        self.buffer_pool.len()
    }

    /// Check if a page is in the buffer pool
    pub fn is_page_cached(&self, file: FileHandle, page_id: PageId) -> bool {
        let key = BufferKey { file, page_id };
        self.buffer_pool.contains(&key)
    }

    /// Get the number of dirty pages in the buffer pool
    pub fn dirty_page_count(&self) -> usize {
        self.buffer_pool.iter().filter(|(_, e)| e.dirty).count()
    }
}

impl Drop for BufferManager {
    fn drop(&mut self) {
        // Flush all dirty pages when the buffer manager is dropped
        let _ = self.flush_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup_test_env() -> (TempDir, BufferManager, FileHandle) {
        let temp_dir = tempfile::tempdir().unwrap();
        let test_file = temp_dir.path().join("test.db");

        let mut file_manager = PagedFileManager::new();
        file_manager.create_file(&test_file).unwrap();
        let handle = file_manager.open_file(&test_file).unwrap();

        let buffer_manager = BufferManager::new(file_manager);

        (temp_dir, buffer_manager, handle)
    }

    #[test]
    fn test_get_page() {
        let (_temp_dir, mut bm, handle) = setup_test_env();

        // Write a page directly through file manager
        let mut write_buffer = vec![0u8; PAGE_SIZE];
        write_buffer[0] = 42;
        bm.file_manager_mut()
            .write_page(handle, 0, &write_buffer)
            .unwrap();

        // Read through buffer manager
        let page = bm.get_page(handle, 0).unwrap();
        assert_eq!(page[0], 42);
        assert_eq!(bm.buffer_pool_size(), 1);
    }

    #[test]
    fn test_get_page_cached() {
        let (_temp_dir, mut bm, handle) = setup_test_env();

        // First access - should load from disk
        assert!(!bm.is_page_cached(handle, 0));
        bm.get_page(handle, 0).unwrap();
        assert!(bm.is_page_cached(handle, 0));

        // Second access - should be cached
        bm.get_page(handle, 0).unwrap();
        assert_eq!(bm.buffer_pool_size(), 1);
    }

    #[test]
    fn test_get_page_mut() {
        let (_temp_dir, mut bm, handle) = setup_test_env();

        // Get mutable reference and modify
        {
            let page = bm.get_page_mut(handle, 0).unwrap();
            page[0] = 99;
        }

        // Verify the modification
        let page = bm.get_page(handle, 0).unwrap();
        assert_eq!(page[0], 99);
        assert_eq!(bm.dirty_page_count(), 1);
    }

    #[test]
    fn test_mark_dirty() {
        let (_temp_dir, mut bm, handle) = setup_test_env();

        bm.get_page(handle, 0).unwrap();
        assert_eq!(bm.dirty_page_count(), 0);

        bm.mark_dirty(handle, 0).unwrap();
        assert_eq!(bm.dirty_page_count(), 1);
    }

    #[test]
    fn test_flush_page() {
        let (_temp_dir, mut bm, handle) = setup_test_env();

        // Modify a page
        {
            let page = bm.get_page_mut(handle, 0).unwrap();
            page[0] = 55;
        }
        assert_eq!(bm.dirty_page_count(), 1);

        // Flush it
        bm.flush_page(handle, 0).unwrap();
        assert_eq!(bm.dirty_page_count(), 0);

        // Verify it was written to disk by evicting and reloading
        bm.evict_page(handle, 0).unwrap();
        let page = bm.get_page(handle, 0).unwrap();
        assert_eq!(page[0], 55);
    }

    #[test]
    fn test_flush_all() {
        let (_temp_dir, mut bm, handle) = setup_test_env();

        // Modify multiple pages
        for i in 0..5 {
            let page = bm.get_page_mut(handle, i).unwrap();
            page[0] = i as u8;
        }
        assert_eq!(bm.dirty_page_count(), 5);

        // Flush all
        bm.flush_all().unwrap();
        assert_eq!(bm.dirty_page_count(), 0);

        // Verify all were written
        for i in 0..5 {
            bm.evict_page(handle, i).unwrap();
            let page = bm.get_page(handle, i).unwrap();
            assert_eq!(page[0], i as u8);
        }
    }

    #[test]
    fn test_evict_page() {
        let (_temp_dir, mut bm, handle) = setup_test_env();

        bm.get_page(handle, 0).unwrap();
        assert_eq!(bm.buffer_pool_size(), 1);

        bm.evict_page(handle, 0).unwrap();
        assert_eq!(bm.buffer_pool_size(), 0);
        assert!(!bm.is_page_cached(handle, 0));
    }

    #[test]
    fn test_lru_eviction() {
        let temp_dir = tempfile::tempdir().unwrap();
        let test_file = temp_dir.path().join("test.db");

        let mut file_manager = PagedFileManager::new();
        file_manager.create_file(&test_file).unwrap();
        let handle = file_manager.open_file(&test_file).unwrap();

        // Create buffer manager with small capacity
        let mut bm = BufferManager::with_capacity(file_manager, 3);

        // Load 3 pages
        bm.get_page(handle, 0).unwrap();
        bm.get_page(handle, 1).unwrap();
        bm.get_page(handle, 2).unwrap();
        assert_eq!(bm.buffer_pool_size(), 3);

        // Load a 4th page - should evict page 0 (LRU)
        bm.get_page(handle, 3).unwrap();
        assert_eq!(bm.buffer_pool_size(), 3);
        assert!(!bm.is_page_cached(handle, 0));
        assert!(bm.is_page_cached(handle, 1));
        assert!(bm.is_page_cached(handle, 2));
        assert!(bm.is_page_cached(handle, 3));
    }

    #[test]
    fn test_lru_update_on_access() {
        let temp_dir = tempfile::tempdir().unwrap();
        let test_file = temp_dir.path().join("test.db");

        let mut file_manager = PagedFileManager::new();
        file_manager.create_file(&test_file).unwrap();
        let handle = file_manager.open_file(&test_file).unwrap();

        let mut bm = BufferManager::with_capacity(file_manager, 3);

        // Load 3 pages
        bm.get_page(handle, 0).unwrap();
        bm.get_page(handle, 1).unwrap();
        bm.get_page(handle, 2).unwrap();

        // Access page 0 again to make it recently used
        bm.get_page(handle, 0).unwrap();

        // Load a 4th page - should evict page 1 (now LRU)
        bm.get_page(handle, 3).unwrap();
        assert!(bm.is_page_cached(handle, 0));
        assert!(!bm.is_page_cached(handle, 1));
        assert!(bm.is_page_cached(handle, 2));
        assert!(bm.is_page_cached(handle, 3));
    }

    #[test]
    fn test_dirty_page_flushed_on_eviction() {
        let temp_dir = tempfile::tempdir().unwrap();
        let test_file = temp_dir.path().join("test.db");

        let mut file_manager = PagedFileManager::new();
        file_manager.create_file(&test_file).unwrap();
        let handle = file_manager.open_file(&test_file).unwrap();

        let mut bm = BufferManager::with_capacity(file_manager, 2);

        // Modify a page
        {
            let page = bm.get_page_mut(handle, 0).unwrap();
            page[0] = 77;
        }

        // Load enough pages to trigger eviction
        bm.get_page(handle, 1).unwrap();
        bm.get_page(handle, 2).unwrap(); // Should evict page 0

        // Reload page 0 and verify it was saved
        bm.evict_page(handle, 1).unwrap();
        let page = bm.get_page(handle, 0).unwrap();
        assert_eq!(page[0], 77);
    }

    #[test]
    fn test_multiple_files() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file1 = temp_dir.path().join("test1.db");
        let file2 = temp_dir.path().join("test2.db");

        let mut file_manager = PagedFileManager::new();
        file_manager.create_file(&file1).unwrap();
        file_manager.create_file(&file2).unwrap();
        let handle1 = file_manager.open_file(&file1).unwrap();
        let handle2 = file_manager.open_file(&file2).unwrap();

        let mut bm = BufferManager::new(file_manager);

        // Write to different files
        {
            let page1 = bm.get_page_mut(handle1, 0).unwrap();
            page1[0] = 11;
        }
        {
            let page2 = bm.get_page_mut(handle2, 0).unwrap();
            page2[0] = 22;
        }

        // Verify they're separate
        assert_eq!(bm.get_page(handle1, 0).unwrap()[0], 11);
        assert_eq!(bm.get_page(handle2, 0).unwrap()[0], 22);
    }

    #[test]
    fn test_drop_flushes_dirty_pages() {
        let temp_dir = tempfile::tempdir().unwrap();
        let test_file = temp_dir.path().join("test.db");

        let mut file_manager = PagedFileManager::new();
        file_manager.create_file(&test_file).unwrap();
        let handle = file_manager.open_file(&test_file).unwrap();

        {
            let mut bm = BufferManager::new(file_manager);
            let page = bm.get_page_mut(handle, 0).unwrap();
            page[0] = 88;
            // bm is dropped here, should flush
        }

        // Create new buffer manager and verify data was saved
        let mut file_manager = PagedFileManager::new();
        let handle = file_manager.open_file(&test_file).unwrap();
        let mut bm = BufferManager::new(file_manager);
        let page = bm.get_page(handle, 0).unwrap();
        assert_eq!(page[0], 88);
    }
}
