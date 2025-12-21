mod error;
mod page;
mod record;
mod schema;
mod table_file;
mod value;

pub use error::{RecordError, RecordResult};
pub use page::{Page, PageHeader};
pub use record::{Record, RecordId, SlotId};
pub use schema::{ColumnDef, TableSchema};
pub use table_file::TableFile;
pub use value::{DataType, Value};

use crate::file::BufferManager;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// High-level record manager for all tables
pub struct RecordManager {
    buffer_manager: Arc<Mutex<BufferManager>>,
    open_tables: HashMap<String, TableFile>,
}

impl RecordManager {
    /// Create a new record manager
    pub fn new(buffer_manager: Arc<Mutex<BufferManager>>) -> Self {
        Self {
            buffer_manager,
            open_tables: HashMap::new(),
        }
    }

    /// Create a new table file
    pub fn create_table(&mut self, path: &str, schema: TableSchema) -> RecordResult<()> {
        let mut buffer_manager = self.buffer_manager.lock().unwrap();
        let table_file = TableFile::create(&mut buffer_manager, path, schema)?;
        drop(buffer_manager);
        self.open_tables
            .insert(table_file.table_name().to_string(), table_file);
        Ok(())
    }

    /// Open an existing table file
    pub fn open_table(&mut self, path: &str, schema: TableSchema) -> RecordResult<()> {
        // Don't re-open if already open - this would reset page_count!
        if self.open_tables.contains_key(schema.table_name()) {
            return Ok(());
        }

        let mut buffer_manager = self.buffer_manager.lock().unwrap();
        let table_file = TableFile::open(&mut buffer_manager, path, schema)?;
        drop(buffer_manager);
        self.open_tables
            .insert(table_file.table_name().to_string(), table_file);
        Ok(())
    }

    /// Close a table
    pub fn close_table(&mut self, table_name: &str) -> RecordResult<()> {
        self.open_tables.remove(table_name);
        Ok(())
    }

    /// Insert a record into a table
    pub fn insert(&mut self, table_name: &str, record: Record) -> RecordResult<RecordId> {
        let mut buffer_manager = self.buffer_manager.lock().unwrap();
        let table = self
            .open_tables
            .get_mut(table_name)
            .ok_or_else(|| RecordError::TableNotOpen(table_name.to_string()))?;
        table.insert_record(&mut buffer_manager, &record)
    }

    /// Bulk insert multiple records into a table
    /// This is much more efficient than calling insert repeatedly as it:
    /// - Acquires the buffer_manager lock only once
    /// - Allows the table file to optimize insertions (e.g., fill pages before writing)
    pub fn bulk_insert(
        &mut self,
        table_name: &str,
        records: Vec<Record>,
    ) -> RecordResult<Vec<RecordId>> {
        let mut buffer_manager = self.buffer_manager.lock().unwrap();
        let table = self
            .open_tables
            .get_mut(table_name)
            .ok_or_else(|| RecordError::TableNotOpen(table_name.to_string()))?;

        let mut record_ids = Vec::with_capacity(records.len());
        for record in &records {
            let rid = table.insert_record(&mut buffer_manager, record)?;
            record_ids.push(rid);
        }

        Ok(record_ids)
    }

    /// Delete a record from a table
    pub fn delete(&mut self, table_name: &str, rid: RecordId) -> RecordResult<()> {
        let mut buffer_manager = self.buffer_manager.lock().unwrap();
        let table = self
            .open_tables
            .get_mut(table_name)
            .ok_or_else(|| RecordError::TableNotOpen(table_name.to_string()))?;
        table.delete_record(&mut buffer_manager, rid)
    }

    /// Update a record in a table
    pub fn update(&mut self, table_name: &str, rid: RecordId, record: Record) -> RecordResult<()> {
        let mut buffer_manager = self.buffer_manager.lock().unwrap();
        let table = self
            .open_tables
            .get_mut(table_name)
            .ok_or_else(|| RecordError::TableNotOpen(table_name.to_string()))?;
        table.update_record(&mut buffer_manager, rid, &record)
    }

    /// Get a record from a table
    pub fn get(&mut self, table_name: &str, rid: RecordId) -> RecordResult<Record> {
        let mut buffer_manager = self.buffer_manager.lock().unwrap();
        let table = self
            .open_tables
            .get_mut(table_name)
            .ok_or_else(|| RecordError::TableNotOpen(table_name.to_string()))?;
        table.get_record(&mut buffer_manager, rid)
    }

    /// Scan all records in a table
    pub fn scan(&mut self, table_name: &str) -> RecordResult<Vec<(RecordId, Record)>> {
        let mut buffer_manager = self.buffer_manager.lock().unwrap();
        let table = self
            .open_tables
            .get_mut(table_name)
            .ok_or_else(|| RecordError::TableNotOpen(table_name.to_string()))?;
        table.scan(&mut buffer_manager)
    }
}
