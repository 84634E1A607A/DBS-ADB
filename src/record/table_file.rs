use super::error::RecordResult;
use super::page::Page;
use super::record::{Record, RecordId};
use super::schema::TableSchema;
use crate::file::{BufferManager, FileHandle, PageId};
use std::sync::{Arc, Mutex};

/// Manages a table's file with multiple pages
pub struct TableFile {
    file_handle: FileHandle,
    schema: TableSchema,
    first_page_id: PageId,
    page_count: usize,
    last_insert_page_id: PageId, // Track last page used for insertion to optimize sequential inserts
}

impl TableFile {
    /// Create a new table file
    pub fn create(
        buffer_mgr: &mut BufferManager,
        path: &str,
        schema: TableSchema,
    ) -> RecordResult<Self> {
        // Create the file
        buffer_mgr.file_manager_mut().create_file(path)?;
        let file_handle = buffer_mgr.file_manager_mut().open_file(path)?;

        // Create the first page - zero-copy directly in buffer
        let page_buffer = buffer_mgr.get_page_mut(file_handle, 0)?;
        Page::new(page_buffer, schema.record_size())?;

        Ok(Self {
            file_handle,
            schema,
            first_page_id: 0,
            page_count: 1,
            last_insert_page_id: 0,
        })
    }

    /// Open an existing table file
    pub fn open(
        buffer_mgr: &mut BufferManager,
        path: &str,
        schema: TableSchema,
    ) -> RecordResult<Self> {
        let file_handle = buffer_mgr.file_manager_mut().open_file(path)?;
        let page_count = buffer_mgr.file_manager_mut().get_page_count(file_handle)?;

        Ok(Self {
            file_handle,
            schema,
            first_page_id: 0,
            page_count: if page_count > 0 { page_count } else { 1 },
            last_insert_page_id: if page_count > 0 { page_count - 1 } else { 0 },
        })
    }

    /// Get table name
    pub fn table_name(&self) -> &str {
        self.schema.table_name()
    }

    /// Get schema
    pub fn schema(&self) -> &TableSchema {
        &self.schema
    }

    /// Insert a record into the table
    pub fn insert_record(
        &mut self,
        buffer_mgr: &mut BufferManager,
        record: &Record,
    ) -> RecordResult<RecordId> {
        self.insert_record_with_hint(buffer_mgr, record, false)
    }

    /// Insert a record with a hint about whether to skip searching old pages
    /// When bulk_insert_hint is true, always allocates a new page instead of
    /// searching from the beginning - critical optimization for bulk loading
    pub fn insert_record_with_hint(
        &mut self,
        buffer_mgr: &mut BufferManager,
        record: &Record,
        bulk_insert_hint: bool,
    ) -> RecordResult<RecordId> {
        // Validate record
        self.schema.validate_record(record.values())?;

        // Serialize record
        let record_bytes = record.serialize(&self.schema)?;

        // Start search from last insertion point (optimization for sequential inserts)
        let mut page_id = self.last_insert_page_id;
        let mut checked_from_start = false;

        loop {
            // Load the page directly as mutable (avoid double load)
            let page_buffer = buffer_mgr.get_page_mut(self.file_handle, page_id)?;
            let mut page = Page::from_buffer(page_buffer)?;

            // Check if page has free space
            if let Some(slot_id) = page.find_free_slot() {
                // Found free slot, insert record
                page.set_record(slot_id, &record_bytes)?;
                page.mark_slot_used(slot_id)?;
                // No need to write back - page modified buffer in-place!

                // Update last insert location for next time
                self.last_insert_page_id = page_id;

                return Ok(RecordId::new(page_id, slot_id));
            }

            // Check if there's a next page
            let next_page = page.next_page();
            if next_page == 0 {
                // No next page
                if !bulk_insert_hint && !checked_from_start && page_id != self.first_page_id {
                    // We started from last_insert_page_id, try from beginning
                    // BUT: skip this in bulk insert mode to avoid loading old pages
                    page_id = self.first_page_id;
                    checked_from_start = true;
                } else {
                    // Need to allocate a new page
                    let new_page_id = self.allocate_new_page(buffer_mgr, page_id)?;
                    self.last_insert_page_id = new_page_id;
                    page_id = new_page_id;
                    // Loop will try again with the new page
                }
            } else {
                page_id = next_page;
            }
        }
    }

    /// Delete a record from the table
    pub fn delete_record(
        &mut self,
        buffer_mgr: &mut BufferManager,
        rid: RecordId,
    ) -> RecordResult<()> {
        // Load the page as mutable
        let page_buffer = buffer_mgr.get_page_mut(self.file_handle, rid.page_id)?;
        let mut page = Page::from_buffer(page_buffer)?;

        // Mark slot as free (modifies buffer in-place)
        page.mark_slot_free(rid.slot_id)?;

        Ok(())
    }

    /// Update a record in the table (in-place for fixed-length records)
    pub fn update_record(
        &mut self,
        buffer_mgr: &mut BufferManager,
        rid: RecordId,
        record: &Record,
    ) -> RecordResult<()> {
        // Validate record
        self.schema.validate_record(record.values())?;

        // Serialize record
        let record_bytes = record.serialize(&self.schema)?;

        // Load the page as mutable
        let page_buffer = buffer_mgr.get_page_mut(self.file_handle, rid.page_id)?;
        let mut page = Page::from_buffer(page_buffer)?;

        // Update record in slot (modifies buffer in-place)
        page.set_record(rid.slot_id, &record_bytes)?;

        Ok(())
    }

    /// Get a record from the table
    pub fn get_record(
        &mut self,
        buffer_mgr: &mut BufferManager,
        rid: RecordId,
    ) -> RecordResult<Record> {
        // Load the page - for reads we need to work around immutable borrow
        // We'll use get_page_mut but treat it as read-only
        let page_buffer = buffer_mgr.get_page_mut(self.file_handle, rid.page_id)?;
        let page = Page::from_buffer(page_buffer)?;

        // Get record bytes
        let record_bytes = page.get_record(rid.slot_id)?;

        // Deserialize record
        Record::deserialize(record_bytes, &self.schema)
    }

    /// Scan all records in the table
    pub fn scan(
        &mut self,
        buffer_mgr: &mut BufferManager,
    ) -> RecordResult<Vec<(RecordId, Record)>> {
        let mut results = Vec::new();
        let mut page_id = self.first_page_id;

        loop {
            // Check if page exists
            if page_id >= self.page_count {
                break;
            }

            // Load the page - use get_page_mut for consistency
            let page_buffer = buffer_mgr.get_page_mut(self.file_handle, page_id)?;
            let page = Page::from_buffer(page_buffer)?;

            // Scan all slots in this page
            for slot_id in 0..page.slot_count() {
                if page.is_slot_used(slot_id) {
                    let record_bytes = page.get_record(slot_id)?;
                    let record = Record::deserialize(record_bytes, &self.schema)?;
                    results.push((RecordId::new(page_id, slot_id), record));
                }
            }

            // Move to next page
            let next_page = page.next_page();
            if next_page == 0 {
                break;
            }
            page_id = next_page;
        }

        Ok(results)
    }

    /// Create a streaming iterator over all records in the table.
    /// This avoids loading the entire table into memory at once.
    pub fn scan_iter(&self, buffer_manager: Arc<Mutex<BufferManager>>) -> TableScanIter {
        TableScanIter::new(self, buffer_manager)
    }

    /// Allocate a new page and link it from the previous page
    fn allocate_new_page(
        &mut self,
        buffer_mgr: &mut BufferManager,
        prev_page_id: PageId,
    ) -> RecordResult<PageId> {
        let new_page_id = self.page_count;
        self.page_count += 1;

        // Create new page directly in buffer
        let page_buffer = buffer_mgr.get_page_mut(self.file_handle, new_page_id)?;
        Page::new(page_buffer, self.schema.record_size())?;

        // Update previous page's next_page pointer
        let prev_page_buffer = buffer_mgr.get_page_mut(self.file_handle, prev_page_id)?;
        let mut prev_page = Page::from_buffer(prev_page_buffer)?;
        prev_page.set_next_page(new_page_id);

        Ok(new_page_id)
    }
}

/// Streaming table scan iterator (yields records one-by-one).
pub struct TableScanIter {
    file_handle: FileHandle,
    schema: TableSchema,
    page_count: usize,
    buffer_manager: Arc<Mutex<BufferManager>>,
    page_id: PageId,
    slot_id: usize,
    done: bool,
}

impl TableScanIter {
    fn new(table: &TableFile, buffer_manager: Arc<Mutex<BufferManager>>) -> Self {
        Self {
            file_handle: table.file_handle,
            schema: table.schema.clone(),
            page_count: table.page_count,
            page_id: table.first_page_id,
            buffer_manager,
            slot_id: 0,
            done: false,
        }
    }
}

impl Iterator for TableScanIter {
    type Item = RecordResult<(RecordId, Record)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        loop {
            if self.page_id >= self.page_count {
                self.done = true;
                return None;
            }

            let mut buffer_manager = self.buffer_manager.lock().unwrap();
            let page_buffer = match buffer_manager.get_page_mut(self.file_handle, self.page_id) {
                Ok(buf) => buf,
                Err(err) => return Some(Err(err.into())),
            };

            let page = match Page::from_buffer(page_buffer) {
                Ok(page) => page,
                Err(err) => return Some(Err(err)),
            };

            for slot_id in self.slot_id..page.slot_count() {
                if page.is_slot_used(slot_id) {
                    let record_bytes = match page.get_record(slot_id) {
                        Ok(bytes) => bytes,
                        Err(err) => return Some(Err(err)),
                    };
                    let record = match Record::deserialize(record_bytes, &self.schema) {
                        Ok(record) => record,
                        Err(err) => return Some(Err(err)),
                    };

                    self.slot_id = slot_id + 1;
                    return Some(Ok((RecordId::new(self.page_id, slot_id), record)));
                }
            }

            let next_page = page.next_page();
            self.slot_id = 0;
            if next_page == 0 {
                self.done = true;
                return None;
            }
            self.page_id = next_page;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::PagedFileManager;
    use crate::record::{ColumnDef, DataType, Value};
    use tempfile::TempDir;

    fn create_test_schema() -> TableSchema {
        TableSchema::new(
            "test_table".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DataType::Int, true, Value::Null),
                ColumnDef::new("name".to_string(), DataType::Char(20), false, Value::Null),
                ColumnDef::new("score".to_string(), DataType::Float, false, Value::Null),
            ],
        )
    }

    fn setup_test_env() -> (TempDir, BufferManager) {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_manager = PagedFileManager::new();
        let buffer_manager = BufferManager::new(file_manager);
        (temp_dir, buffer_manager)
    }

    #[test]
    fn test_create_table_file() {
        let (temp_dir, mut buffer_mgr) = setup_test_env();
        let test_file = temp_dir.path().join("test.tbl");
        let schema = create_test_schema();

        let table =
            TableFile::create(&mut buffer_mgr, test_file.to_str().unwrap(), schema).unwrap();
        assert_eq!(table.table_name(), "test_table");
        assert_eq!(table.page_count, 1);
    }

    #[test]
    fn test_insert_and_get_record() {
        let (temp_dir, mut buffer_mgr) = setup_test_env();
        let test_file = temp_dir.path().join("test.tbl");
        let schema = create_test_schema();

        let mut table =
            TableFile::create(&mut buffer_mgr, test_file.to_str().unwrap(), schema).unwrap();

        // Insert a record
        let record = Record::new(vec![
            Value::Int(1),
            Value::String("Alice".to_string()),
            Value::Float(95.5),
        ]);

        let rid = table.insert_record(&mut buffer_mgr, &record).unwrap();

        // Get the record back
        let retrieved = table.get_record(&mut buffer_mgr, rid).unwrap();
        assert_eq!(record, retrieved);
    }

    #[test]
    fn test_insert_multiple_records() {
        let (temp_dir, mut buffer_mgr) = setup_test_env();
        let test_file = temp_dir.path().join("test.tbl");
        let schema = create_test_schema();

        let mut table =
            TableFile::create(&mut buffer_mgr, test_file.to_str().unwrap(), schema).unwrap();

        // Insert multiple records
        let mut rids = Vec::new();
        for i in 0..10 {
            let record = Record::new(vec![
                Value::Int(i),
                Value::String(format!("User{}", i)),
                Value::Float(i as f64 * 10.0),
            ]);
            let rid = table.insert_record(&mut buffer_mgr, &record).unwrap();
            rids.push((rid, record));
        }

        // Verify all records
        for (rid, expected) in &rids {
            let retrieved = table.get_record(&mut buffer_mgr, *rid).unwrap();
            assert_eq!(*expected, retrieved);
        }
    }

    #[test]
    fn test_delete_record() {
        let (temp_dir, mut buffer_mgr) = setup_test_env();
        let test_file = temp_dir.path().join("test.tbl");
        let schema = create_test_schema();

        let mut table =
            TableFile::create(&mut buffer_mgr, test_file.to_str().unwrap(), schema).unwrap();

        // Insert a record
        let record = Record::new(vec![
            Value::Int(1),
            Value::String("Alice".to_string()),
            Value::Float(95.5),
        ]);
        let rid = table.insert_record(&mut buffer_mgr, &record).unwrap();

        // Delete it
        table.delete_record(&mut buffer_mgr, rid).unwrap();

        // Try to get it (should fail because slot is free)
        let result = table.get_record(&mut buffer_mgr, rid);
        assert!(result.is_err());
    }

    #[test]
    fn test_update_record() {
        let (temp_dir, mut buffer_mgr) = setup_test_env();
        let test_file = temp_dir.path().join("test.tbl");
        let schema = create_test_schema();

        let mut table =
            TableFile::create(&mut buffer_mgr, test_file.to_str().unwrap(), schema).unwrap();

        // Insert a record
        let record = Record::new(vec![
            Value::Int(1),
            Value::String("Alice".to_string()),
            Value::Float(95.5),
        ]);
        let rid = table.insert_record(&mut buffer_mgr, &record).unwrap();

        // Update it
        let updated = Record::new(vec![
            Value::Int(1),
            Value::String("Bob".to_string()),
            Value::Float(85.0),
        ]);
        table.update_record(&mut buffer_mgr, rid, &updated).unwrap();

        // Verify the update
        let retrieved = table.get_record(&mut buffer_mgr, rid).unwrap();
        assert_eq!(updated, retrieved);
    }

    #[test]
    fn test_scan_records() {
        let (temp_dir, mut buffer_mgr) = setup_test_env();
        let test_file = temp_dir.path().join("test.tbl");
        let schema = create_test_schema();

        let mut table =
            TableFile::create(&mut buffer_mgr, test_file.to_str().unwrap(), schema).unwrap();

        // Insert multiple records
        let mut expected = Vec::new();
        for i in 0..5 {
            let record = Record::new(vec![
                Value::Int(i),
                Value::String(format!("User{}", i)),
                Value::Float(i as f64 * 10.0),
            ]);
            let rid = table.insert_record(&mut buffer_mgr, &record).unwrap();
            expected.push((rid, record));
        }

        // Scan all records
        let scanned = table.scan(&mut buffer_mgr).unwrap();
        assert_eq!(scanned.len(), expected.len());

        // Verify all records (order should match)
        for ((rid1, rec1), (rid2, rec2)) in scanned.iter().zip(expected.iter()) {
            assert_eq!(rid1, rid2);
            assert_eq!(rec1, rec2);
        }
    }

    #[test]
    fn test_multi_page_insertion() {
        let (temp_dir, mut buffer_mgr) = setup_test_env();
        let test_file = temp_dir.path().join("test.tbl");

        // Create a schema with small records to fit many per page
        let schema = TableSchema::new(
            "test".to_string(),
            vec![ColumnDef::new(
                "id".to_string(),
                DataType::Int,
                true,
                Value::Null,
            )],
        );

        let mut table =
            TableFile::create(&mut buffer_mgr, test_file.to_str().unwrap(), schema).unwrap();

        // Insert enough records to span multiple pages
        let slot_count = Page::calculate_slot_count(5); // 5 bytes: 1 bitmap + 4 int
        let insert_count = slot_count + 10; // More than one page

        let mut rids = Vec::new();
        for i in 0..insert_count {
            let record = Record::new(vec![Value::Int(i as i32)]);
            let rid = table.insert_record(&mut buffer_mgr, &record).unwrap();
            rids.push(rid);
        }

        // Verify we used multiple pages
        assert!(table.page_count > 1);

        // Verify all records are accessible
        for (i, rid) in rids.iter().enumerate() {
            let record = table.get_record(&mut buffer_mgr, *rid).unwrap();
            assert_eq!(record.values()[0], Value::Int(i as i32));
        }
    }
}
