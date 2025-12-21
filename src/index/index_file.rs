//! Index file management

use crate::file::BufferManager;
use crate::record::RecordId;

use super::error::{IndexError, IndexResult};
use super::persistent_btree::PersistentBPlusTree;

/// Manages a single index file on disk
pub struct IndexFile {
    /// Persistent B+ tree
    btree: PersistentBPlusTree,

    /// Index metadata
    table_name: String,
    column_name: String,
}

impl IndexFile {
    /// Create a new index file
    pub fn create(
        buffer_mgr: &mut BufferManager,
        db_path: &str,
        table_name: &str,
        column_name: &str,
        order: usize,
    ) -> IndexResult<Self> {
        let file_path = Self::index_file_path(db_path, table_name, column_name);

        // Check if file already exists
        if std::path::Path::new(&file_path).exists() {
            return Err(IndexError::IndexAlreadyExists(file_path));
        }

        let btree = PersistentBPlusTree::create(buffer_mgr, &file_path, order)?;

        Ok(Self {
            btree,
            table_name: table_name.to_string(),
            column_name: column_name.to_string(),
        })
    }

    /// Open an existing index file
    pub fn open(
        buffer_mgr: &mut BufferManager,
        db_path: &str,
        table_name: &str,
        column_name: &str,
    ) -> IndexResult<Self> {
        let file_path = Self::index_file_path(db_path, table_name, column_name);

        // Check if file exists
        if !std::path::Path::new(&file_path).exists() {
            return Err(IndexError::IndexNotFound(file_path));
        }

        let btree = PersistentBPlusTree::open(buffer_mgr, &file_path)?;

        Ok(Self {
            btree,
            table_name: table_name.to_string(),
            column_name: column_name.to_string(),
        })
    }

    /// Close the index file
    pub fn close(self, buffer_mgr: &mut BufferManager) -> IndexResult<()> {
        self.btree.close(buffer_mgr)
    }

    /// Flush changes to disk
    pub fn flush(&mut self, buffer_mgr: &mut BufferManager) -> IndexResult<()> {
        self.btree.flush(buffer_mgr)
    }

    /// Insert a key-value pair
    pub fn insert(&mut self, key: i64, rid: RecordId) -> IndexResult<()> {
        self.btree.insert(key, rid)
    }

    /// Efficiently build index from pre-sorted entries
    ///
    /// This method is significantly faster than repeated individual inserts
    /// when loading a large amount of data.
    ///
    /// # Arguments
    /// * `entries` - Iterator of (key, value) pairs in ascending key order
    ///
    /// # Important
    /// Entries MUST be sorted by key. Unsorted entries will cause an error.
    ///
    /// # Example
    /// ```ignore
    /// // Collect data from table
    /// let mut entries: Vec<(i64, RecordId)> = /* scan table */;
    /// // Sort by key
    /// entries.sort_by_key(|e| e.0);
    /// // Bulk load into index
    /// index_file.bulk_load(entries.into_iter())?;
    /// ```
    pub fn bulk_load<I>(&mut self, entries: I) -> IndexResult<()>
    where
        I: Iterator<Item = (i64, RecordId)>,
    {
        self.btree.bulk_load(entries)
    }

    /// Bulk load from a pre-sorted slice (more memory efficient)
    pub fn bulk_load_from_slice(&mut self, entries: &[(i64, RecordId)]) -> IndexResult<()> {
        self.btree.bulk_load_from_slice(entries)
    }

    /// Bulk load from an iterator (most memory efficient for external sort)
    pub fn bulk_load_from_iter<I>(&mut self, entries: I) -> IndexResult<()>
    where
        I: Iterator<Item = (i64, RecordId)>,
    {
        self.btree.bulk_load(entries)
    }

    /// Delete all entries with the given key
    /// Returns whether any entries were deleted
    pub fn delete(&mut self, key: i64) -> IndexResult<bool> {
        self.btree.delete(key)
    }

    /// Delete a specific key-value pair
    pub fn delete_entry(&mut self, key: i64, rid: RecordId) -> IndexResult<bool> {
        self.btree.delete_entry(key, rid)
    }

    /// Search for a key (returns first match)
    pub fn search(&self, key: i64) -> Option<RecordId> {
        self.btree.search(key)
    }

    /// Search for all entries with the given key
    pub fn search_all(&self, key: i64) -> Vec<RecordId> {
        self.btree.search_all(key)
    }

    /// Range search [lower, upper]
    pub fn range_search(&self, lower: i64, upper: i64) -> Vec<(i64, RecordId)> {
        self.btree.range_search(lower, upper)
    }

    /// Update a specific entry
    pub fn update(
        &mut self,
        old_key: i64,
        old_value: RecordId,
        new_key: i64,
        new_value: RecordId,
    ) -> IndexResult<()> {
        self.btree.update(old_key, old_value, new_key, new_value)
    }

    /// Get number of entries
    pub fn len(&self) -> usize {
        self.btree.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.btree.is_empty()
    }

    /// Get table name
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Get column name
    pub fn column_name(&self) -> &str {
        &self.column_name
    }

    /// Generate index file path
    fn index_file_path(db_path: &str, table_name: &str, column_name: &str) -> String {
        format!("{}/{}_{}.idx", db_path, table_name, column_name)
    }
}
