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

/// High-level record manager for all tables
pub struct RecordManager {
    buffer_manager: BufferManager,
    open_tables: HashMap<String, TableFile>,
}

impl RecordManager {
    /// Create a new record manager
    pub fn new(buffer_manager: BufferManager) -> Self {
        Self {
            buffer_manager,
            open_tables: HashMap::new(),
        }
    }

    /// Create a new table file
    pub fn create_table(&mut self, path: &str, schema: TableSchema) -> RecordResult<()> {
        let table_file = TableFile::create(&mut self.buffer_manager, path, schema)?;
        self.open_tables
            .insert(table_file.table_name().to_string(), table_file);
        Ok(())
    }

    /// Open an existing table file
    pub fn open_table(&mut self, path: &str, schema: TableSchema) -> RecordResult<()> {
        let table_file = TableFile::open(&mut self.buffer_manager, path, schema)?;
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
        let table = self
            .open_tables
            .get_mut(table_name)
            .ok_or_else(|| RecordError::TableNotOpen(table_name.to_string()))?;
        table.insert_record(&mut self.buffer_manager, &record)
    }

    /// Delete a record from a table
    pub fn delete(&mut self, table_name: &str, rid: RecordId) -> RecordResult<()> {
        let table = self
            .open_tables
            .get_mut(table_name)
            .ok_or_else(|| RecordError::TableNotOpen(table_name.to_string()))?;
        table.delete_record(&mut self.buffer_manager, rid)
    }

    /// Update a record in a table
    pub fn update(&mut self, table_name: &str, rid: RecordId, record: Record) -> RecordResult<()> {
        let table = self
            .open_tables
            .get_mut(table_name)
            .ok_or_else(|| RecordError::TableNotOpen(table_name.to_string()))?;
        table.update_record(&mut self.buffer_manager, rid, &record)
    }

    /// Get a record from a table
    pub fn get(&mut self, table_name: &str, rid: RecordId) -> RecordResult<Record> {
        let table = self
            .open_tables
            .get_mut(table_name)
            .ok_or_else(|| RecordError::TableNotOpen(table_name.to_string()))?;
        table.get_record(&mut self.buffer_manager, rid)
    }

    /// Scan all records in a table
    pub fn scan(&mut self, table_name: &str) -> RecordResult<Vec<(RecordId, Record)>> {
        let table = self
            .open_tables
            .get_mut(table_name)
            .ok_or_else(|| RecordError::TableNotOpen(table_name.to_string()))?;
        table.scan(&mut self.buffer_manager)
    }

    /// Get mutable reference to buffer manager
    pub fn buffer_manager_mut(&mut self) -> &mut BufferManager {
        &mut self.buffer_manager
    }
}
