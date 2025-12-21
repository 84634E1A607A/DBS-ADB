//! Index management module

mod error;
mod index_file;
mod persistent_btree;
mod serialization;
#[cfg(test)]
mod tests;

pub use error::{IndexError, IndexResult};
pub use index_file::IndexFile;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::btree::DEFAULT_ORDER;
use crate::file::{BufferManager, PagedFileManager};
use crate::record::RecordId;

/// High-level index manager
pub struct IndexManager {
    /// Buffer manager
    buffer_manager: Arc<Mutex<BufferManager>>,

    /// Open indexes: (table_name, column_name) -> IndexFile
    open_indexes: HashMap<(String, String), IndexFile>,
}

impl IndexManager {
    /// Create a new index manager
    pub fn new(buffer_manager: Arc<Mutex<BufferManager>>) -> Self {
        Self {
            buffer_manager,
            open_indexes: HashMap::new(),
        }
    }

    /// Create a new index
    pub fn create_index(
        &mut self,
        db_path: &str,
        table_name: &str,
        column_name: &str,
    ) -> IndexResult<()> {
        // Use default order optimized for 8KB pages
        let mut buffer_manager = self.buffer_manager.lock().unwrap();
        let index_file = IndexFile::create(
            &mut *buffer_manager,
            db_path,
            table_name,
            column_name,
            DEFAULT_ORDER,
        )?;
        drop(buffer_manager);

        // Store in open indexes
        self.open_indexes.insert(
            (table_name.to_string(), column_name.to_string()),
            index_file,
        );

        Ok(())
    }

    /// Create an index on existing table data (efficient bulk loading)
    ///
    /// This method scans the table, extracts values from the specified column,
    /// sorts them, and builds the B+ tree using an efficient bottom-up algorithm.
    ///
    /// # Arguments
    /// * `db_path` - Database directory path
    /// * `table_name` - Name of the table
    /// * `column_name` - Name of the column to index
    /// * `table_data` - Iterator of (RecordId, column_value) tuples from table scan
    ///
    /// # Performance
    /// - Calculates optimal tree depth before construction
    /// - Uses bulk loading (O(n)) instead of repeated inserts (O(n log n))
    /// - Automatically sorts data if needed
    ///
    /// # Example
    /// ```ignore
    /// // Scan table and extract column values
    /// let table_data: Vec<(RecordId, i64)> = /* scan table */;
    ///
    /// // Create index with automatic sorting and optimal tree construction
    /// index_manager.create_index_from_table(
    ///     "/path/to/db",
    ///     "users",
    ///     "age",
    ///     table_data.into_iter()
    /// )?;
    /// ```
    pub fn create_index_from_table<I>(
        &mut self,
        db_path: &str,
        table_name: &str,
        column_name: &str,
        table_data: I,
    ) -> IndexResult<()>
    where
        I: Iterator<Item = (RecordId, i64)>,
    {
        // For large datasets, we need to be memory-efficient
        // Collect entries but with capacity hint to avoid reallocations
        let mut entries: Vec<(i64, RecordId)> =
            table_data.map(|(rid, value)| (value, rid)).collect();

        let entry_count = entries.len();

        // If no entries, just create empty index
        if entry_count == 0 {
            let mut buffer_manager = self.buffer_manager.lock().unwrap();
            let index_file = IndexFile::create(
                &mut *buffer_manager,
                db_path,
                table_name,
                column_name,
                DEFAULT_ORDER,
            )?;
            drop(buffer_manager);

            self.open_indexes.insert(
                (table_name.to_string(), column_name.to_string()),
                index_file,
            );
            return Ok(());
        }

        // Calculate optimal tree depth for informational purposes
        let optimal_depth =
            crate::btree::BPlusTree::calculate_optimal_depth(entry_count, DEFAULT_ORDER);

        // Sort entries by key (required for bulk load)
        // This is the main memory cost, but unavoidable for bulk loading
        entries.sort_unstable_by_key(|e| e.0);

        // Create the index file
        let mut buffer_manager = self.buffer_manager.lock().unwrap();
        let mut index_file = IndexFile::create(
            &mut *buffer_manager,
            db_path,
            table_name,
            column_name,
            DEFAULT_ORDER,
        )?;

        // Use bulk load for efficient tree construction
        // Note: bulk_load will also collect the iterator, but we've already
        // collected and sorted, so we pass in an iterator over our vec
        index_file.bulk_load(entries.into_iter())?;

        // Flush to disk
        index_file.flush(&mut *buffer_manager)?;

        drop(buffer_manager);

        // Store in open indexes
        self.open_indexes.insert(
            (table_name.to_string(), column_name.to_string()),
            index_file,
        );

        // Log success with tree statistics
        eprintln!(
            "✓ Created index on {}.{} with {} entries (optimal depth: {})",
            table_name, column_name, entry_count, optimal_depth
        );

        Ok(())
    }

    /// Drop an index
    pub fn drop_index(
        &mut self,
        db_path: &str,
        table_name: &str,
        column_name: &str,
    ) -> IndexResult<()> {
        // Close the index if open
        let key = (table_name.to_string(), column_name.to_string());
        if let Some(index_file) = self.open_indexes.remove(&key) {
            let mut buffer_manager = self.buffer_manager.lock().unwrap();
            index_file.close(&mut *buffer_manager)?;
        }

        // Delete the file
        let file_path = format!("{}/{}_{}.idx", db_path, table_name, column_name);
        std::fs::remove_file(&file_path).map_err(|e| {
            IndexError::SerializationError(format!("Failed to delete index file: {}", e))
        })?;

        Ok(())
    }

    /// Open an index
    pub fn open_index(
        &mut self,
        db_path: &str,
        table_name: &str,
        column_name: &str,
    ) -> IndexResult<()> {
        let key = (table_name.to_string(), column_name.to_string());

        // Don't open if already open
        if self.open_indexes.contains_key(&key) {
            return Ok(());
        }

        let mut buffer_manager = self.buffer_manager.lock().unwrap();
        let index_file = IndexFile::open(&mut *buffer_manager, db_path, table_name, column_name)?;
        drop(buffer_manager);

        self.open_indexes.insert(key, index_file);

        Ok(())
    }

    /// Close an index and flush to disk
    pub fn close_index(&mut self, table_name: &str, column_name: &str) -> IndexResult<()> {
        let key = (table_name.to_string(), column_name.to_string());

        if let Some(mut index_file) = self.open_indexes.remove(&key) {
            // Flush before closing to ensure all changes are persisted
            let mut buffer_manager = self.buffer_manager.lock().unwrap();
            index_file.flush(&mut *buffer_manager)?;
            drop(buffer_manager);
            let mut buffer_manager = self.buffer_manager.lock().unwrap();
            index_file.close(&mut *buffer_manager)?;
        }

        Ok(())
    }

    /// Close all indexes
    pub fn close_all(&mut self) -> IndexResult<()> {
        let keys: Vec<_> = self.open_indexes.keys().cloned().collect();

        for (table_name, column_name) in keys {
            self.close_index(&table_name, &column_name)?;
        }

        Ok(())
    }

    /// Flush an index to disk
    pub fn flush_index(&mut self, table_name: &str, column_name: &str) -> IndexResult<()> {
        let key = (table_name.to_string(), column_name.to_string());

        if let Some(mut index_file) = self.open_indexes.remove(&key) {
            // Flush before closing to ensure all changes are persisted
            let mut buffer_manager = self.buffer_manager.lock().unwrap();
            index_file.flush(&mut *buffer_manager)?;
        }

        Ok(())
    }

    /// Flush all indexes to disk
    pub fn flush_all(&mut self) -> IndexResult<()> {
        let mut buffer_manager = self.buffer_manager.lock().unwrap();
        for index_file in self.open_indexes.values_mut() {
            index_file.flush(&mut *buffer_manager)?;
        }

        Ok(())
    }

    /// Get a reference to an open index
    pub fn get_index(&self, table_name: &str, column_name: &str) -> Option<&IndexFile> {
        let key = (table_name.to_string(), column_name.to_string());
        self.open_indexes.get(&key)
    }

    /// Get a mutable reference to an open index
    pub fn get_index_mut(&mut self, table_name: &str, column_name: &str) -> Option<&mut IndexFile> {
        let key = (table_name.to_string(), column_name.to_string());
        self.open_indexes.get_mut(&key)
    }

    /// Insert into index
    pub fn insert(
        &mut self,
        table_name: &str,
        column_name: &str,
        key: i64,
        rid: RecordId,
    ) -> IndexResult<()> {
        let index_key = (table_name.to_string(), column_name.to_string());

        let index_file = self
            .open_indexes
            .get_mut(&index_key)
            .ok_or_else(|| IndexError::IndexNotOpen(format!("{}_{}", table_name, column_name)))?;

        index_file.insert(key, rid)
    }

    /// Delete from index
    /// Returns whether any entries were deleted
    pub fn delete(&mut self, table_name: &str, column_name: &str, key: i64) -> IndexResult<bool> {
        let index_key = (table_name.to_string(), column_name.to_string());

        let index_file = self
            .open_indexes
            .get_mut(&index_key)
            .ok_or_else(|| IndexError::IndexNotOpen(format!("{}_{}", table_name, column_name)))?;

        index_file.delete(key)
    }

    /// Delete specific entry from index
    pub fn delete_entry(
        &mut self,
        table_name: &str,
        column_name: &str,
        key: i64,
        rid: RecordId,
    ) -> IndexResult<bool> {
        let index_key = (table_name.to_string(), column_name.to_string());

        let index_file = self
            .open_indexes
            .get_mut(&index_key)
            .ok_or_else(|| IndexError::IndexNotOpen(format!("{}_{}", table_name, column_name)))?;

        index_file.delete_entry(key, rid)
    }

    /// Search index
    pub fn search(&self, table_name: &str, column_name: &str, key: i64) -> Option<RecordId> {
        let index_key = (table_name.to_string(), column_name.to_string());
        self.open_indexes
            .get(&index_key)
            .and_then(|index| index.search(key))
    }

    /// Search all matching entries in index
    pub fn search_all(&self, table_name: &str, column_name: &str, key: i64) -> Vec<RecordId> {
        let index_key = (table_name.to_string(), column_name.to_string());
        self.open_indexes
            .get(&index_key)
            .map(|index| index.search_all(key))
            .unwrap_or_default()
    }

    /// Range search index
    pub fn range_search(
        &self,
        table_name: &str,
        column_name: &str,
        lower: i64,
        upper: i64,
    ) -> Vec<(i64, RecordId)> {
        let index_key = (table_name.to_string(), column_name.to_string());
        self.open_indexes
            .get(&index_key)
            .map(|index| index.range_search(lower, upper))
            .unwrap_or_default()
    }

    /// Update entry in index
    pub fn update(
        &mut self,
        table_name: &str,
        column_name: &str,
        old_key: i64,
        old_value: RecordId,
        new_key: i64,
        new_value: RecordId,
    ) -> IndexResult<()> {
        let index_key = (table_name.to_string(), column_name.to_string());

        let index_file = self
            .open_indexes
            .get_mut(&index_key)
            .ok_or_else(|| IndexError::IndexNotOpen(format!("{}_{}", table_name, column_name)))?;

        index_file.update(old_key, old_value, new_key, new_value)
    }
}

impl Drop for IndexManager {
    fn drop(&mut self) {
        // Try to close all indexes cleanly
        let _ = self.close_all();
    }
}
