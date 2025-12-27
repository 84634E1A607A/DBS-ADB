use csv::ReaderBuilder;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use thiserror::Error;

use crate::catalog::{
    CatalogError, ColumnMetadata, DatabaseMetadata, ForeignKeyMetadata, TableMetadata,
};
use crate::file::{BufferManager, PagedFileManager};
use crate::index::IndexManager;
use crate::lexer_parser::{
    AlterStatement, CreateTableField, DBStatement, Operator, SelectClause, Selectors,
    TableStatement, Value as ParserValue, WhereClause,
};
use crate::record::{
    ColumnDef, Record, RecordId, RecordManager, TableFile, TableScanIter, TableSchema,
    Value as RecordValue,
};

#[derive(Debug, Error)]
pub enum DatabaseError {
    #[error("Database {0} already exists")]
    DatabaseExists(String),

    #[error("Database {0} not found")]
    DatabaseNotFound(String),

    #[error("No database selected")]
    NoDatabaseSelected,

    #[error("Table {0} already exists")]
    TableExists(String),

    #[error("Table {0} not found")]
    TableNotFound(String),

    #[error("Column {0} not found in table {1}")]
    ColumnNotFound(String, String),

    #[error("Type mismatch: {0}")]
    TypeMismatch(String),

    #[error("Cannot read/write file: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Catalog error: {0}")]
    CatalogError(#[from] CatalogError),

    #[error("Record error: {0}")]
    RecordError(#[from] crate::record::RecordError),

    #[error("Index error: {0}")]
    IndexError(#[from] crate::index::IndexError),

    #[error("File error: {0}")]
    FileError(#[from] crate::file::FileError),

    #[error("Primary key violation: duplicate key value")]
    PrimaryKeyViolation,

    #[error("Foreign key violation")]
    ForeignKeyViolation,

    #[error("Not null constraint violation for column {0}")]
    NotNullViolation(String),
}

pub type DatabaseResult<T> = Result<T, DatabaseError>;

pub struct DatabaseManager {
    data_dir: PathBuf,
    current_db: Option<String>,
    current_metadata: Option<DatabaseMetadata>,
    buffer_manager: Arc<Mutex<BufferManager>>,
    record_manager: RecordManager,
    index_manager: IndexManager,
}

struct TableIntColumnIter {
    scan_iter: TableScanIter,
    col_idx: usize,
}

impl TableIntColumnIter {
    fn new(scan_iter: TableScanIter, col_idx: usize) -> Self {
        Self { scan_iter, col_idx }
    }
}

impl Iterator for TableIntColumnIter {
    type Item = crate::index::IndexResult<(RecordId, i64)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let item = self.scan_iter.next()?;
            match item {
                Ok((rid, record)) => {
                    if let Some(RecordValue::Int(val)) = record.get(self.col_idx) {
                        return Some(Ok((rid, *val as i64)));
                    }
                }
                Err(err) => {
                    return Some(Err(crate::index::IndexError::SerializationError(
                        err.to_string(),
                    )));
                }
            }
        }
    }
}

impl DatabaseManager {
    pub fn new(data_dir: &str) -> DatabaseResult<Self> {
        let data_path = PathBuf::from(data_dir);
        fs::create_dir_all(&data_path)?;

        let file_manager = PagedFileManager::new();
        let buffer_manager = Arc::new(Mutex::new(BufferManager::new(file_manager)));
        let record_manager = RecordManager::new(buffer_manager.clone());
        let index_manager = IndexManager::new(buffer_manager.clone());

        Ok(Self {
            data_dir: data_path,
            current_db: None,
            current_metadata: None,
            buffer_manager,
            record_manager,
            index_manager,
        })
    }

    // Database operations
    pub fn create_database(&mut self, name: &str) -> DatabaseResult<()> {
        let db_path = self.data_dir.join(name);
        if db_path.exists() {
            return Err(DatabaseError::DatabaseExists(name.to_string()));
        }

        fs::create_dir(&db_path)?;

        let metadata = DatabaseMetadata::new(name.to_string());
        metadata.save(&db_path)?;

        Ok(())
    }

    pub fn drop_database(&mut self, name: &str) -> DatabaseResult<()> {
        let db_path = self.data_dir.join(name);
        if !db_path.exists() {
            return Err(DatabaseError::DatabaseNotFound(name.to_string()));
        }

        // Close current database if it's the one being dropped
        if self.current_db.as_ref() == Some(&name.to_string()) {
            self.current_db = None;
            self.current_metadata = None;
        }

        fs::remove_dir_all(&db_path)?;
        Ok(())
    }

    pub fn show_databases(&self) -> DatabaseResult<Vec<String>> {
        let mut databases = Vec::new();
        for entry in fs::read_dir(&self.data_dir)? {
            let entry = entry?;
            if entry.file_type()?.is_dir()
                && let Some(name) = entry.file_name().to_str()
            {
                databases.push(name.to_string());
            }
        }
        databases.sort();
        Ok(databases)
    }

    pub fn use_database(&mut self, name: &str) -> DatabaseResult<()> {
        let db_path = self.data_dir.join(name);
        if !db_path.exists() {
            return Err(DatabaseError::DatabaseNotFound(name.to_string()));
        }

        let metadata = DatabaseMetadata::load(&db_path)?;
        self.current_db = Some(name.to_string());
        self.current_metadata = Some(metadata);

        Ok(())
    }

    // Table operations
    pub fn create_table(
        &mut self,
        name: &str,
        fields: Vec<CreateTableField>,
    ) -> DatabaseResult<()> {
        {
            let metadata = self
                .current_metadata
                .as_ref()
                .ok_or(DatabaseError::NoDatabaseSelected)?;

            if metadata.tables.contains_key(name) {
                return Err(DatabaseError::TableExists(name.to_string()));
            }
        }

        let mut columns = Vec::new();
        let mut primary_key = None;
        let mut foreign_keys = Vec::new();

        for field in fields {
            match field {
                CreateTableField::Col(col_name, col_type, not_null, default) => {
                    columns.push(ColumnMetadata::from_parser(
                        col_name, col_type, not_null, default,
                    ));
                }
                CreateTableField::Pkey(alter_stmt) => {
                    if let AlterStatement::AddPKey(_, pk_cols) = *alter_stmt {
                        primary_key = Some(pk_cols);
                    }
                }
                CreateTableField::Fkey(alter_stmt) => {
                    if let AlterStatement::AddFKey(_, fk_name, fk_cols, ref_table, ref_cols) =
                        *alter_stmt
                    {
                        foreign_keys.push(ForeignKeyMetadata {
                            name: fk_name.unwrap_or_else(|| format!("fk_{}", name)),
                            columns: fk_cols,
                            ref_table,
                            ref_columns: ref_cols,
                        });
                    }
                }
            }
        }

        // Primary key columns are implicitly NOT NULL
        if let Some(pk_cols) = &primary_key {
            for col in &mut columns {
                if pk_cols.contains(&col.name) {
                    col.not_null = true;
                }
            }
        }

        let table_metadata = TableMetadata {
            name: name.to_string(),
            columns,
            primary_key,
            foreign_keys,
            indexes: Vec::new(),
        };

        // Create the table file
        let db_name = self.current_db.as_ref().unwrap();
        let table_path = self.table_path(db_name, name);
        let schema = self.metadata_to_schema(&table_metadata);
        self.record_manager
            .create_table(&table_path.to_string_lossy(), schema)?;

        // Add to metadata
        let metadata = self.current_metadata.as_mut().unwrap();
        metadata.add_table(table_metadata);
        self.save_current_metadata()?;

        Ok(())
    }

    pub fn drop_table(&mut self, name: &str) -> DatabaseResult<()> {
        let metadata = self
            .current_metadata
            .as_mut()
            .ok_or(DatabaseError::NoDatabaseSelected)?;

        metadata.remove_table(name)?;

        // Delete the table file
        let db_name = self.current_db.as_ref().unwrap();
        let table_path = self.table_path(db_name, name);
        if table_path.exists() {
            fs::remove_file(&table_path)?;
        }

        self.save_current_metadata()?;
        Ok(())
    }

    pub fn show_tables(&self) -> DatabaseResult<Vec<String>> {
        let metadata = self
            .current_metadata
            .as_ref()
            .ok_or(DatabaseError::NoDatabaseSelected)?;

        let mut tables: Vec<String> = metadata.tables.keys().cloned().collect();
        tables.sort();
        Ok(tables)
    }

    pub fn describe_table(&self, name: &str) -> DatabaseResult<TableMetadata> {
        let metadata = self
            .current_metadata
            .as_ref()
            .ok_or(DatabaseError::NoDatabaseSelected)?;

        Ok(metadata.get_table(name)?.clone())
    }

    // Data operations
    pub fn insert(&mut self, table: &str, rows: Vec<Vec<ParserValue>>) -> DatabaseResult<usize> {
        self.bulk_insert(table, rows, false)
    }

    /// Optimized bulk insert function
    ///
    /// This function provides significant performance improvements for bulk inserts by:
    /// 1. Checking for primary key duplicates within the batch itself using a HashSet (O(n) instead of O(n²))
    /// 2. Using B-tree index lookups for single-column integer primary keys (O(log n) per lookup)
    /// 3. Scanning the table only once for all rows instead of once per row
    /// 4. Optionally skipping primary key checks when data is known to be valid (e.g., from LOAD DATA INFILE)
    ///
    /// When skip_pk_check is true, primary key checking is skipped entirely for maximum performance.
    /// Use this only when data is known to be valid and without duplicates.
    pub fn bulk_insert(
        &mut self,
        table: &str,
        rows: Vec<Vec<ParserValue>>,
        skip_pk_check: bool,
    ) -> DatabaseResult<usize> {
        let (table_meta, schema) = {
            let metadata = self
                .current_metadata
                .as_ref()
                .ok_or(DatabaseError::NoDatabaseSelected)?;

            let table_meta = metadata.get_table(table)?.clone();
            let schema = self.metadata_to_schema(&table_meta);
            (table_meta, schema)
        };

        let db_name = self.current_db.as_ref().unwrap();
        let table_path = self.table_path(db_name, table);
        let table_path_str = table_path.to_string_lossy().to_string();

        // Try to open table if not already open (ignore error if already open)
        let _ = self
            .record_manager
            .open_table(&table_path_str, schema.clone());

        // For primary key checking, collect all keys in the batch first
        let mut batch_pk_set = HashSet::new();
        let pk_indices: Option<Vec<usize>> = table_meta.primary_key.as_ref().map(|pk_cols| {
            pk_cols
                .iter()
                .map(|col_name| {
                    table_meta
                        .columns
                        .iter()
                        .position(|c| &c.name == col_name)
                        .unwrap()
                })
                .collect()
        });

        // Convert all rows and check for duplicates within batch
        let mut records = Vec::with_capacity(rows.len());

        for row in rows {
            // Convert parser values to record values
            let mut record_values = Vec::with_capacity(table_meta.columns.len());

            for (i, value) in row.iter().enumerate() {
                if i >= table_meta.columns.len() {
                    break;
                }
                let col = &table_meta.columns[i];

                // Check NOT NULL constraint
                if matches!(value, ParserValue::Null) && col.not_null {
                    return Err(DatabaseError::NotNullViolation(col.name.clone()));
                }

                record_values.push(self.parser_value_to_record_value(value, &col.to_data_type()));
            }

            let record = Record::new(record_values);

            // Check for duplicates within the batch itself
            if !skip_pk_check && let Some(ref indices) = pk_indices {
                let pk_key: Vec<String> = indices
                    .iter()
                    .map(|&idx| format!("{:?}", record.get(idx).unwrap()))
                    .collect();
                let pk_string = pk_key.join("|");

                if !batch_pk_set.insert(pk_string) {
                    return Err(DatabaseError::PrimaryKeyViolation);
                }
            }

            records.push(record);
        }

        // Check against existing records using index if available (only for single-column integer PKs)
        if !skip_pk_check && let Some(ref pk_cols) = table_meta.primary_key {
            if pk_cols.len() == 1 {
                let pk_col_name = &pk_cols[0];
                let pk_col_idx = table_meta
                    .columns
                    .iter()
                    .position(|c| &c.name == pk_col_name)
                    .unwrap();

                // Try to use index for primary key checking
                let db_path = self.data_dir.join(db_name.as_str());
                let _index_key = (table.to_string(), pk_col_name.clone());

                // Try to open the index if it exists
                let has_index = self
                    .index_manager
                    .open_index(&db_path.to_string_lossy(), table, pk_col_name)
                    .is_ok();

                if has_index {
                    // Use index for fast lookup
                    for record in &records {
                        if let RecordValue::Int(pk_val) = record.get(pk_col_idx).unwrap()
                            && self
                                .index_manager
                                .search(table, pk_col_name, *pk_val as i64)
                                .is_some()
                        {
                            return Err(DatabaseError::PrimaryKeyViolation);
                        }
                    }
                } else {
                    // Fallback: scan existing records (only once for the whole batch)
                    let existing_records = self.record_manager.scan(table)?;
                    for record in &records {
                        for (_, existing_record) in &existing_records {
                            let mut is_duplicate = true;
                            for &pk_idx in pk_indices.as_ref().unwrap() {
                                if record.get(pk_idx) != existing_record.get(pk_idx) {
                                    is_duplicate = false;
                                    break;
                                }
                            }
                            if is_duplicate {
                                return Err(DatabaseError::PrimaryKeyViolation);
                            }
                        }
                    }
                }
            } else {
                // Multi-column PK: use table scan (but only once for the whole batch)
                let existing_records = self.record_manager.scan(table)?;
                for record in &records {
                    for (_, existing_record) in &existing_records {
                        let mut is_duplicate = true;
                        for &pk_idx in pk_indices.as_ref().unwrap() {
                            if record.get(pk_idx) != existing_record.get(pk_idx) {
                                is_duplicate = false;
                                break;
                            }
                        }
                        if is_duplicate {
                            return Err(DatabaseError::PrimaryKeyViolation);
                        }
                    }
                }
            }
        }

        // Insert all records in one batch - much faster as it holds the lock only once
        let _record_ids = self.record_manager.bulk_insert(table, records)?;
        Ok(_record_ids.len())
    }

    pub fn delete(
        &mut self,
        table: &str,
        where_clauses: Option<Vec<WhereClause>>,
    ) -> DatabaseResult<usize> {
        let metadata = self
            .current_metadata
            .as_ref()
            .ok_or(DatabaseError::NoDatabaseSelected)?;

        let table_meta = metadata.get_table(table)?;
        let schema = self.metadata_to_schema(table_meta);

        let db_name = self.current_db.as_ref().unwrap();
        let table_path = self.table_path(db_name, table);
        let table_path_str = table_path.to_string_lossy().to_string();

        // Try to open table if not already open (ignore error if already open)
        let _ = self
            .record_manager
            .open_table(&table_path_str, schema.clone());

        let mut deleted = 0;
        let scan_iter = self.record_manager.scan_iter(table)?;
        for item in scan_iter {
            let (rid, record) = item?;
            let should_delete = match &where_clauses {
                None => true,
                Some(clauses) => self.evaluate_where(&record, &schema, clauses)?,
            };

            if should_delete {
                self.record_manager.delete(table, rid)?;
                deleted += 1;
            }
        }

        Ok(deleted)
    }

    pub fn update(
        &mut self,
        table: &str,
        updates: Vec<(String, ParserValue)>,
        where_clauses: Option<Vec<WhereClause>>,
    ) -> DatabaseResult<usize> {
        let (_table_meta, schema) = {
            let metadata = self
                .current_metadata
                .as_ref()
                .ok_or(DatabaseError::NoDatabaseSelected)?;

            let table_meta = metadata.get_table(table)?.clone();
            let schema = self.metadata_to_schema(&table_meta);
            (table_meta, schema)
        };

        let db_name = self.current_db.as_ref().unwrap();
        let table_path = self.table_path(db_name, table);
        let table_path_str = table_path.to_string_lossy().to_string();

        // Try to open table if not already open (ignore error if already open)
        let _ = self
            .record_manager
            .open_table(&table_path_str, schema.clone());

        // Build update map
        let mut update_map = HashMap::new();
        for (col_name, value) in updates {
            let col_idx = schema
                .columns
                .iter()
                .position(|c| c.name == col_name)
                .ok_or_else(|| {
                    DatabaseError::ColumnNotFound(col_name.clone(), table.to_string())
                })?;
            update_map.insert(col_idx, value);
        }

        let mut updated = 0;
        let scan_iter = self.record_manager.scan_iter(table)?;
        for item in scan_iter {
            let (rid, mut record) = item?;
            let should_update = match &where_clauses {
                None => true,
                Some(clauses) => self.evaluate_where(&record, &schema, clauses)?,
            };

            if should_update {
                // Apply updates
                for (col_idx, new_value) in &update_map {
                    let data_type = &schema.columns[*col_idx].data_type;
                    let record_value = self.parser_value_to_record_value(new_value, data_type);
                    record.set(*col_idx, record_value);
                }

                self.record_manager.update(table, rid, record)?;
                updated += 1;
            }
        }

        Ok(updated)
    }

    pub fn select(
        &mut self,
        clause: SelectClause,
    ) -> DatabaseResult<(Vec<String>, Vec<Vec<String>>)> {
        let metadata = self
            .current_metadata
            .as_ref()
            .ok_or(DatabaseError::NoDatabaseSelected)?;

        // For now, only support single table queries
        if clause.table.len() != 1 {
            return Err(DatabaseError::TypeMismatch(
                "Multi-table queries not yet supported".to_string(),
            ));
        }

        let table_name = &clause.table[0];
        let table_meta = metadata.get_table(table_name)?;
        let schema = self.metadata_to_schema(table_meta);

        let db_name = self.current_db.as_ref().unwrap();
        let table_path = self.table_path(db_name, table_name);
        let table_path_str = table_path.to_string_lossy().to_string();

        // Open table if not already open
        self.record_manager
            .open_table(&table_path_str, schema.clone())?;

        // Determine selected columns
        let selected_columns = match &clause.selectors {
            Selectors::All => schema.columns.iter().map(|c| c.name.clone()).collect(),
            Selectors::List(selectors) => {
                let mut cols = Vec::new();
                for selector in selectors {
                    match selector {
                        crate::lexer_parser::Selector::Column(tc) => {
                            cols.push(tc.column.clone());
                        }
                        _ => {
                            return Err(DatabaseError::TypeMismatch(
                                "Aggregates not yet supported".to_string(),
                            ));
                        }
                    }
                }
                cols
            }
        };

        // Get column indices
        let col_indices: Vec<usize> = selected_columns
            .iter()
            .map(|col_name| {
                schema
                    .columns
                    .iter()
                    .position(|c| c.name == *col_name)
                    .ok_or_else(|| {
                        DatabaseError::ColumnNotFound(col_name.clone(), table_name.clone())
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Scan table using streaming iterator
        let scan_iter = self.record_manager.scan_iter(table_name)?;

        let mut result_rows = Vec::new();
        for item in scan_iter {
            let (_rid, record) = item?;
            // Evaluate WHERE clause
            let matches = match &clause.where_clauses {
                clauses if clauses.is_empty() => true,
                clauses => self.evaluate_where(&record, &schema, clauses)?,
            };

            if matches {
                // Project selected columns
                let mut row = Vec::new();
                for &idx in &col_indices {
                    let value = record.get(idx).unwrap();
                    row.push(self.format_value(value));
                }
                result_rows.push(row);
            }
        }

        Ok((selected_columns, result_rows))
    }

    pub fn load_data_infile(
        &mut self,
        file_path: &str,
        table: &str,
        delimiter: char,
    ) -> DatabaseResult<usize> {
        // Get table metadata to know column types and indexes
        let table_meta = {
            let metadata = self
                .current_metadata
                .as_ref()
                .ok_or(DatabaseError::NoDatabaseSelected)?;
            metadata.get_table(table)?.clone()
        };

        let db_name = self.current_db.as_ref().unwrap();
        let db_path = self.data_dir.join(db_name);
        let db_path_str = db_path.to_string_lossy().to_string();

        // Step 1: Collect all index information before dropping
        let mut index_columns = Vec::new();

        // Add primary key index(es) if exists
        if let Some(ref pk_cols) = table_meta.primary_key {
            for col_name in pk_cols {
                index_columns.push(col_name.clone());
            }
        }

        // Add other indexes (indexes may be on multiple columns, take first for single-column indexes)
        for index_meta in &table_meta.indexes {
            for col_name in &index_meta.columns {
                if !index_columns.contains(col_name) {
                    index_columns.push(col_name.clone());
                }
            }
        }

        // Step 2: Drop all indexes (including primary key index)
        for col_name in &index_columns {
            // Attempt to drop - ignore error if index doesn't exist
            let _ = self.index_manager.drop_index(&db_path_str, table, col_name);
        }

        // Step 3: Clear all data from the table efficiently
        // Instead of deleting records, close and recreate the table file
        // This is much faster and uses minimal memory
        let schema = self.metadata_to_schema(&table_meta);
        let table_path = self.table_path(db_name, table);
        let table_path_str = table_path.to_string_lossy().to_string();

        // Step 3a: Flush all buffers first - this ensures all OTHER tables' data is safe
        self.buffer_manager.lock().unwrap().flush_all()?;

        // Step 3b: Close the table (remove from open_tables)
        self.record_manager.close_table(table)?;

        // Step 3c: Delete the old table file using the file manager
        // This ensures the file handle is properly closed before deletion
        if table_path.exists() {
            let mut buffer_manager = self.buffer_manager.lock().unwrap();
            let _ = buffer_manager.file_manager_mut().remove_file(&table_path);
        }

        // Step 3d: Recreate empty table file
        self.record_manager
            .create_table(&table_path_str, schema.clone())?;

        eprintln!(
            "Cleared all data from table {} by recreating the table file",
            table
        );

        // Step 4: Load data without index maintenance
        // Use csv crate for efficient parsing
        let mut reader = ReaderBuilder::new()
            .delimiter(delimiter as u8)
            .has_headers(false)
            .flexible(true) // Allow varying number of fields per row
            .from_path(file_path)
            .map_err(|e| std::io::Error::other(e.to_string()))?;

        let num_columns = table_meta.columns.len();
        const BATCH_SIZE: usize = 50000; // Larger batches reduce overhead from allocating/deallocating vectors
        const PROGRESS_INTERVAL: usize = 50_000; // Report progress every 100k lines

        let mut total_inserted = 0;
        let mut batch_rows = Vec::with_capacity(BATCH_SIZE);

        // Pre-allocate string buffer to avoid reallocations for string fields
        // Most CSV fields are small, so this avoids most allocations
        let mut string_buffer = String::with_capacity(256);

        // Process records in batches - use schema to parse types directly
        for result in reader.records() {
            let record = result.map_err(|e| std::io::Error::other(e.to_string()))?;

            let mut values = Vec::with_capacity(num_columns);

            for (idx, field) in record.iter().enumerate() {
                if idx >= table_meta.columns.len() {
                    break; // Skip extra fields
                }

                let col = &table_meta.columns[idx];
                let trimmed = field.trim();

                // Parse according to the column's data type - much faster than guessing!
                let value = if trimmed.eq_ignore_ascii_case("null") {
                    ParserValue::Null
                } else {
                    match col.to_data_type() {
                        crate::record::DataType::Int => match trimmed.parse::<i64>() {
                            Ok(i) => ParserValue::Integer(i),
                            Err(_) => ParserValue::Null,
                        },
                        crate::record::DataType::Float => match trimmed.parse::<f64>() {
                            Ok(f) => ParserValue::Float(f),
                            Err(_) => ParserValue::Null,
                        },
                        crate::record::DataType::Char(_) => {
                            // Reuse string buffer to avoid allocation for each string
                            string_buffer.clear();
                            string_buffer.push_str(field);
                            ParserValue::String(string_buffer.clone())
                        }
                    }
                };

                values.push(value);
            }

            if !values.is_empty() {
                batch_rows.push(values);

                // Insert batch when it reaches BATCH_SIZE
                if batch_rows.len() >= BATCH_SIZE {
                    total_inserted +=
                        self.bulk_insert(table, std::mem::take(&mut batch_rows), true)?;
                    batch_rows.reserve(BATCH_SIZE); // Prepare for next batch

                    // Flush and clear buffer pool periodically to prevent memory buildup
                    // During bulk insert, we're appending to the end of the file, so old pages
                    // won't be accessed again. Flushing frees up memory and prevents thrashing.
                    self.buffer_manager.lock().unwrap().flush_and_clear()?;
                }
            }
        }

        // Insert remaining rows
        if !batch_rows.is_empty() {
            total_inserted += self.bulk_insert(table, batch_rows, true)?;
            // Final flush after last batch
            self.buffer_manager.lock().unwrap().flush_and_clear()?;
        }

        eprintln!(
            "Loaded {} rows from file {} into table {} without indexes",
            total_inserted, file_path, table
        );

        // Step 5: Reconstruct all indexes using bulk create
        for col_name in &index_columns {
            // Find the column index
            let col_idx = table_meta
                .columns
                .iter()
                .position(|c| &c.name == col_name)
                .ok_or_else(|| {
                    DatabaseError::ColumnNotFound(col_name.clone(), table.to_string())
                })?;

            // Scan table and extract values for this column using a streaming iterator
            // to avoid loading all records into memory at once.
            let table_file = {
                let mut buffer_manager = self.buffer_manager.lock().unwrap();
                TableFile::open(&mut buffer_manager, &table_path_str, schema.clone())?
            };
            let scan_iter = table_file.scan_iter(self.buffer_manager.clone());
            let table_data = TableIntColumnIter::new(scan_iter, col_idx);

            // Use bulk create index function - it will consume the iterator
            self.index_manager.create_index_from_table(
                &db_path_str,
                table,
                col_name,
                table_data,
            )?;

            // Flush and close the index immediately to free memory before building the next one.
            self.index_manager.close_index(table, col_name)?;

            // Flush and clear buffer pool after each index creation
            // This releases ALL cached pages to prevent memory buildup.
            self.buffer_manager.lock().unwrap().flush_and_clear()?;
        }

        // Step 6: Flush buffer manager to release page cache
        // This ensures we're not holding onto all the index pages in memory
        {
            let mut buffer_manager = self.buffer_manager.lock().unwrap();
            // Ignore flush errors - they're not critical for correctness
            let _ = buffer_manager.flush_all();
        }

        Ok(total_inserted)
    }

    // Helper methods
    fn table_path(&self, db: &str, table: &str) -> PathBuf {
        self.data_dir.join(db).join(format!("{}.tbl", table))
    }

    fn save_current_metadata(&self) -> DatabaseResult<()> {
        if let (Some(db_name), Some(metadata)) = (&self.current_db, &self.current_metadata) {
            let db_path = self.data_dir.join(db_name);
            metadata.save(&db_path)?;
        }
        Ok(())
    }

    fn metadata_to_schema(&self, table_meta: &TableMetadata) -> TableSchema {
        let columns = table_meta
            .columns
            .iter()
            .map(|col| ColumnDef {
                name: col.name.clone(),
                data_type: col.to_data_type(),
                not_null: col.not_null,
                default_value: col.parse_default_value(),
            })
            .collect();

        TableSchema::new(table_meta.name.clone(), columns)
    }

    fn parser_value_to_record_value(
        &self,
        value: &ParserValue,
        _data_type: &crate::record::DataType,
    ) -> RecordValue {
        match value {
            ParserValue::Null => RecordValue::Null,
            ParserValue::Integer(i) => RecordValue::Int(*i as i32),
            ParserValue::Float(f) => RecordValue::Float(*f),
            ParserValue::String(s) => RecordValue::String(s.clone()),
        }
    }

    fn evaluate_where(
        &self,
        record: &Record,
        schema: &TableSchema,
        where_clauses: &[WhereClause],
    ) -> DatabaseResult<bool> {
        // All clauses must be true (AND logic)
        for clause in where_clauses {
            match clause {
                WhereClause::Op(col, op, expr) => {
                    let col_idx = schema
                        .columns
                        .iter()
                        .position(|c| c.name == col.column)
                        .ok_or_else(|| {
                            DatabaseError::ColumnNotFound(
                                col.column.clone(),
                                schema.table_name.clone(),
                            )
                        })?;

                    let left_val = record.get(col_idx).unwrap();

                    let right_val = match expr {
                        crate::lexer_parser::Expression::Value(v) => {
                            let data_type = &schema.columns[col_idx].data_type;
                            self.parser_value_to_record_value(v, data_type)
                        }
                        crate::lexer_parser::Expression::Column(_) => {
                            return Err(DatabaseError::TypeMismatch(
                                "Column expressions not yet supported".to_string(),
                            ));
                        }
                    };

                    if !self.compare_values(left_val, op, &right_val) {
                        return Ok(false);
                    }
                }
                WhereClause::Null(col) => {
                    let col_idx = schema
                        .columns
                        .iter()
                        .position(|c| c.name == col.column)
                        .ok_or_else(|| {
                            DatabaseError::ColumnNotFound(
                                col.column.clone(),
                                schema.table_name.clone(),
                            )
                        })?;
                    if !record.get(col_idx).unwrap().is_null() {
                        return Ok(false);
                    }
                }
                WhereClause::NotNull(col) => {
                    let col_idx = schema
                        .columns
                        .iter()
                        .position(|c| c.name == col.column)
                        .ok_or_else(|| {
                            DatabaseError::ColumnNotFound(
                                col.column.clone(),
                                schema.table_name.clone(),
                            )
                        })?;
                    if record.get(col_idx).unwrap().is_null() {
                        return Ok(false);
                    }
                }
                _ => {
                    return Err(DatabaseError::TypeMismatch(
                        "WHERE clause type not yet supported".to_string(),
                    ));
                }
            }
        }

        Ok(true)
    }

    fn compare_values(&self, left: &RecordValue, op: &Operator, right: &RecordValue) -> bool {
        use std::cmp::Ordering;

        // Handle NULL comparisons
        if left.is_null() || right.is_null() {
            return false; // NULL comparisons are always false
        }

        let cmp = match (left, right) {
            (RecordValue::Int(l), RecordValue::Int(r)) => l.cmp(r),
            (RecordValue::Float(l), RecordValue::Float(r)) => {
                if l < r {
                    Ordering::Less
                } else if l > r {
                    Ordering::Greater
                } else {
                    Ordering::Equal
                }
            }
            (RecordValue::String(l), RecordValue::String(r)) => l.cmp(r),
            _ => return false, // Type mismatch
        };

        match op {
            Operator::Eq => cmp == Ordering::Equal,
            Operator::Ne => cmp != Ordering::Equal,
            Operator::Lt => cmp == Ordering::Less,
            Operator::Le => cmp != Ordering::Greater,
            Operator::Gt => cmp == Ordering::Greater,
            Operator::Ge => cmp != Ordering::Less,
        }
    }

    fn format_value(&self, value: &RecordValue) -> String {
        match value {
            RecordValue::Null => "NULL".to_string(),
            RecordValue::Int(i) => i.to_string(),
            RecordValue::Float(f) => format!("{:.2}", f),
            RecordValue::String(s) => s.clone(),
        }
    }

    pub fn execute_db_statement(&mut self, stmt: DBStatement) -> DatabaseResult<QueryResult> {
        match stmt {
            DBStatement::CreateDatabase(name) => {
                self.create_database(&name)?;
                Ok(QueryResult::Empty)
            }
            DBStatement::DropDatabase(name) => {
                self.drop_database(&name)?;
                Ok(QueryResult::Empty)
            }
            DBStatement::ShowDatabases => {
                let dbs = self.show_databases()?;
                let rows = dbs.into_iter().map(|db| vec![db]).collect();
                Ok(QueryResult::ResultSet(vec!["DATABASES".to_string()], rows))
            }
            DBStatement::UseDatabase(name) => {
                self.use_database(&name)?;
                Ok(QueryResult::Empty)
            }
            DBStatement::ShowTables => {
                let tables = self.show_tables()?;
                let rows = tables.into_iter().map(|t| vec![t]).collect();
                Ok(QueryResult::ResultSet(vec!["TABLES".to_string()], rows))
            }
            DBStatement::ShowIndexes => {
                // TODO: Implement show indexes
                Ok(QueryResult::Empty)
            }
        }
    }

    pub fn execute_table_statement(&mut self, stmt: TableStatement) -> DatabaseResult<QueryResult> {
        match stmt {
            TableStatement::CreateTable(name, fields) => {
                self.create_table(&name, fields)?;
                Ok(QueryResult::Empty)
            }
            TableStatement::DropTable(name) => {
                self.drop_table(&name)?;
                Ok(QueryResult::Empty)
            }
            TableStatement::DescribeTable(name) => {
                let meta = self.describe_table(&name)?;
                Ok(QueryResult::TableDescription(meta))
            }
            TableStatement::LoadDataInfile(path, table, delim) => {
                let count = self.load_data_infile(&path, &table, delim)?;
                Ok(QueryResult::RowsAffected(count))
            }
            TableStatement::InsertInto(table, rows) => {
                let count = self.insert(&table, rows)?;
                Ok(QueryResult::RowsAffected(count))
            }
            TableStatement::DeleteFrom(table, where_clauses) => {
                let count = self.delete(&table, where_clauses)?;
                Ok(QueryResult::RowsAffected(count))
            }
            TableStatement::Update(table, updates, where_clauses) => {
                let count = self.update(&table, updates, where_clauses)?;
                Ok(QueryResult::RowsAffected(count))
            }
            TableStatement::Select(clause) => {
                let (headers, rows) = self.select(clause)?;
                Ok(QueryResult::ResultSet(headers, rows))
            }
        }
    }

    pub fn execute_alter_statement(
        &mut self,
        _stmt: AlterStatement,
    ) -> DatabaseResult<QueryResult> {
        // TODO: Implement alter statements
        Ok(QueryResult::Empty)
    }
}

#[derive(Debug)]
pub enum QueryResult {
    Empty,
    RowsAffected(usize),
    ResultSet(Vec<String>, Vec<Vec<String>>),
    List(Vec<String>),
    TableDescription(TableMetadata),
}

#[cfg(test)]
mod tests;
