//! Index management module

mod error;
mod index_file;
mod persistent_btree;
mod serialization;
#[cfg(test)]
mod tests;

pub use error::{IndexError, IndexResult};
pub use index_file::IndexFile;

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::btree::DEFAULT_ORDER;
use crate::file::BufferManager;
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
            &mut buffer_manager,
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
    /// Create an index from table data using external sorting for large datasets
    ///
    /// Uses external merge sort to handle datasets larger than available memory.
    /// Data is sorted in chunks that fit in memory, written to temporary files,
    /// then merged using k-way merge.
    ///
    /// # Arguments
    /// * `db_path` - Path to the database directory
    /// * `table_name` - Name of the table
    /// * `column_name` - Name of the column to index
    /// * `table_data` - Iterator over (RecordId, value) pairs
    ///
    /// # Example
    /// ```ignore
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
        use std::fs::File;
        use std::io::{BufRead, BufReader, BufWriter, Write};
        use std::path::PathBuf;

        // Memory limit for external sort: ~150MB for sorting chunks
        // Each entry is (i64, RecordId) = 8 + 8 = 16 bytes
        const MEMORY_LIMIT_BYTES: usize = 150 * 1024 * 1024;
        const ENTRY_SIZE: usize = 16;
        const CHUNK_SIZE: usize = MEMORY_LIMIT_BYTES / ENTRY_SIZE; // ~9.8M entries per chunk

        // Temporary directory for sorted chunks
        let temp_dir = PathBuf::from(db_path).join(".tmp_index_sort");
        std::fs::create_dir_all(&temp_dir).map_err(|e| IndexError::IoError(e.to_string()))?;

        // Phase 1: Split into sorted chunks
        let mut chunk_files = Vec::new();
        let mut current_chunk = Vec::with_capacity(CHUNK_SIZE);
        let mut total_entries = 0;

        eprintln!(
            "Creating index on {}.{} using external sort...",
            table_name, column_name
        );

        for (rid, value) in table_data {
            current_chunk.push((value, rid));
            total_entries += 1;

            if current_chunk.len() >= CHUNK_SIZE {
                // Sort this chunk
                current_chunk.sort_unstable_by_key(|e| e.0);

                // Write to temporary file
                let chunk_path = temp_dir.join(format!("chunk_{}.dat", chunk_files.len()));
                let file =
                    File::create(&chunk_path).map_err(|e| IndexError::IoError(e.to_string()))?;
                let mut writer = BufWriter::new(file);

                for (key, rid) in &current_chunk {
                    // Write as binary: 8 bytes for key, 8 bytes for rid
                    writer
                        .write_all(&key.to_le_bytes())
                        .map_err(|e| IndexError::IoError(e.to_string()))?;
                    writer
                        .write_all(&(rid.page_id as u64).to_le_bytes())
                        .map_err(|e| IndexError::IoError(e.to_string()))?;
                    writer
                        .write_all(&(rid.slot_id as u64).to_le_bytes())
                        .map_err(|e| IndexError::IoError(e.to_string()))?;
                }
                writer
                    .flush()
                    .map_err(|e| IndexError::IoError(e.to_string()))?;

                chunk_files.push(chunk_path);
                current_chunk.clear();
                eprintln!(
                    "  Sorted chunk {} ({} total entries so far)",
                    chunk_files.len(),
                    total_entries
                );
            }
        }

        // Handle remaining entries
        if !current_chunk.is_empty() {
            current_chunk.sort_unstable_by_key(|e| e.0);

            let chunk_path = temp_dir.join(format!("chunk_{}.dat", chunk_files.len()));
            let file = File::create(&chunk_path).map_err(|e| IndexError::IoError(e.to_string()))?;
            let mut writer = BufWriter::new(file);

            for (key, rid) in &current_chunk {
                writer
                    .write_all(&key.to_le_bytes())
                    .map_err(|e| IndexError::IoError(e.to_string()))?;
                writer
                    .write_all(&(rid.page_id as u64).to_le_bytes())
                    .map_err(|e| IndexError::IoError(e.to_string()))?;
                writer
                    .write_all(&(rid.slot_id as u64).to_le_bytes())
                    .map_err(|e| IndexError::IoError(e.to_string()))?;
            }
            writer
                .flush()
                .map_err(|e| IndexError::IoError(e.to_string()))?;

            chunk_files.push(chunk_path);
            current_chunk.clear();
            eprintln!(
                "  Sorted chunk {} ({} total entries)",
                chunk_files.len(),
                total_entries
            );
        }

        if total_entries == 0 {
            // No entries, create empty index
            let mut buffer_manager = self.buffer_manager.lock().unwrap();
            let index_file = IndexFile::create(
                &mut buffer_manager,
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

            // Clean up temp directory
            let _ = std::fs::remove_dir_all(&temp_dir);
            return Ok(());
        }

        // Phase 2: K-way merge of sorted chunks
        eprintln!("  Merging {} sorted chunks...", chunk_files.len());

        let merged_iter = if chunk_files.len() == 1 {
            // Only one chunk, read it directly
            read_sorted_chunk(&chunk_files[0])?
        } else {
            // Multiple chunks, perform k-way merge
            k_way_merge(chunk_files)?
        };

        // Phase 3: Build B+ tree from sorted data
        let mut buffer_manager = self.buffer_manager.lock().unwrap();
        let mut index_file = IndexFile::create(
            &mut buffer_manager,
            db_path,
            table_name,
            column_name,
            DEFAULT_ORDER,
        )?;

        // Bulk load from the merged iterator
        index_file.bulk_load_from_iter(merged_iter)?;

        // Flush to disk
        index_file.flush(&mut buffer_manager)?;
        drop(buffer_manager);

        // Store in open indexes
        self.open_indexes.insert(
            (table_name.to_string(), column_name.to_string()),
            index_file,
        );

        // Clean up temporary files
        let _ = std::fs::remove_dir_all(&temp_dir);

        eprintln!(
            "✓ Created index on {}.{} with {} entries",
            table_name, column_name, total_entries
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
            index_file.close(&mut buffer_manager)?;
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
        let index_file = IndexFile::open(&mut buffer_manager, db_path, table_name, column_name)?;
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
            index_file.flush(&mut buffer_manager)?;
            drop(buffer_manager);
            let mut buffer_manager = self.buffer_manager.lock().unwrap();
            index_file.close(&mut buffer_manager)?;
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
            index_file.flush(&mut buffer_manager)?;
        }

        Ok(())
    }

    /// Flush all indexes to disk
    pub fn flush_all(&mut self) -> IndexResult<()> {
        let mut buffer_manager = self.buffer_manager.lock().unwrap();
        for index_file in self.open_indexes.values_mut() {
            index_file.flush(&mut buffer_manager)?;
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

// Helper functions for external sorting

/// Read a sorted chunk file and return an iterator
fn read_sorted_chunk(path: &PathBuf) -> IndexResult<Box<dyn Iterator<Item = (i64, RecordId)>>> {
    let file = File::open(path).map_err(|e| IndexError::IoError(e.to_string()))?;
    let reader = BufReader::new(file);

    Ok(Box::new(ChunkReader { reader }))
}

struct ChunkReader {
    reader: BufReader<File>,
}

impl Iterator for ChunkReader {
    type Item = (i64, RecordId);

    fn next(&mut self) -> Option<Self::Item> {
        let mut key_bytes = [0u8; 8];
        let mut page_bytes = [0u8; 8];
        let mut slot_bytes = [0u8; 8];

        if self.reader.read_exact(&mut key_bytes).is_err() {
            return None;
        }
        if self.reader.read_exact(&mut page_bytes).is_err() {
            return None;
        }
        if self.reader.read_exact(&mut slot_bytes).is_err() {
            return None;
        }

        let key = i64::from_le_bytes(key_bytes);
        let page_id = u64::from_le_bytes(page_bytes) as usize;
        let slot_id = u64::from_le_bytes(slot_bytes) as usize;

        Some((key, RecordId::new(page_id, slot_id)))
    }
}

/// K-way merge of sorted chunk files
fn k_way_merge(
    chunk_files: Vec<PathBuf>,
) -> IndexResult<Box<dyn Iterator<Item = (i64, RecordId)>>> {
    let mut readers = Vec::new();

    for path in chunk_files {
        readers.push(read_sorted_chunk(&path)?);
    }

    Ok(Box::new(KWayMergeIterator::new(readers)))
}

struct KWayMergeIterator {
    heap: BinaryHeap<Reverse<MergeItem>>,
    readers: Vec<Box<dyn Iterator<Item = (i64, RecordId)>>>,
}

#[derive(Eq, PartialEq)]
struct MergeItem {
    key: i64,
    rid: RecordId,
    reader_idx: usize,
}

impl Ord for MergeItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key
            .cmp(&other.key)
            .then_with(|| self.rid.page_id.cmp(&other.rid.page_id))
            .then_with(|| self.rid.slot_id.cmp(&other.rid.slot_id))
    }
}

impl PartialOrd for MergeItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl KWayMergeIterator {
    fn new(mut readers: Vec<Box<dyn Iterator<Item = (i64, RecordId)>>>) -> Self {
        let mut heap = BinaryHeap::new();

        // Initialize heap with first element from each reader
        for (idx, reader) in readers.iter_mut().enumerate() {
            if let Some((key, rid)) = reader.next() {
                heap.push(Reverse(MergeItem {
                    key,
                    rid,
                    reader_idx: idx,
                }));
            }
        }

        Self { heap, readers }
    }
}

impl Iterator for KWayMergeIterator {
    type Item = (i64, RecordId);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(Reverse(item)) = self.heap.pop() {
            let result = (item.key, item.rid);

            // Try to get next item from the same reader
            if let Some((key, rid)) = self.readers[item.reader_idx].next() {
                self.heap.push(Reverse(MergeItem {
                    key,
                    rid,
                    reader_idx: item.reader_idx,
                }));
            }

            Some(result)
        } else {
            None
        }
    }
}
