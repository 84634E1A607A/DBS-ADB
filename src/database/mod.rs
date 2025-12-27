use csv::ReaderBuilder;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use thiserror::Error;

use crate::catalog::{
    CatalogError, ColumnMetadata, DatabaseMetadata, ForeignKeyMetadata, IndexMetadata,
    TableMetadata,
};
use crate::file::{BufferManager, PagedFileManager};
use crate::index::{IndexError, IndexManager};
use crate::lexer_parser::{
    AlterStatement, CreateTableField, DBStatement, Expression, Operator, SelectClause, Selector,
    Selectors, TableColumn, TableStatement, Value as ParserValue, WhereClause,
};
use crate::record::{
    ColumnDef, DataType, Record, RecordId, RecordManager, TableFile, TableScanIter, TableSchema,
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

    #[error("primary")]
    PrimaryKeyError,

    #[error("Foreign key violation: {0}")]
    ForeignKeyViolation(String),

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

struct ForeignKeyCheck {
    table_name: String,
    column_names: Vec<String>,
    column_indices: Vec<usize>,
    ref_table: String,
    ref_column_names: Vec<String>,
    ref_column_indices: Vec<usize>,
    ref_schema: TableSchema,
}

#[derive(Clone)]
struct ReferencingForeignKeyCheck {
    child_table: String,
    child_column_names: Vec<String>,
    child_column_indices: Vec<usize>,
    child_schema: TableSchema,
    parent_table: String,
    parent_column_names: Vec<String>,
    parent_column_indices: Vec<usize>,
    fk_name: String,
}

#[derive(Clone, Copy)]
enum JoinSide {
    Left,
    Right,
}

#[derive(Clone, Copy)]
struct JoinColumnRef {
    side: JoinSide,
    index: usize,
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
        self.bulk_insert(table, rows, false, false, false)
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
        skip_fk_check: bool,
        skip_index_update: bool,
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
        let db_path = self.data_dir.join(db_name);
        let db_path_str = db_path.to_string_lossy().to_string();

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

        if !skip_fk_check {
            self.validate_foreign_keys_for_records(&table_meta, &records)?;
        }

        let indexed_columns = if skip_index_update {
            Vec::new()
        } else {
            self.open_indexed_columns(&db_path_str, &table_meta)?
        };
        let mut index_keys: Vec<Vec<Option<i64>>> = Vec::new();
        if !indexed_columns.is_empty() {
            index_keys.reserve(records.len());
            for record in &records {
                let mut row_keys = Vec::with_capacity(indexed_columns.len());
                for (_, col_idx) in &indexed_columns {
                    let key = match record.get(*col_idx) {
                        Some(RecordValue::Int(val)) => Some(*val as i64),
                        _ => None,
                    };
                    row_keys.push(key);
                }
                index_keys.push(row_keys);
            }
        }

        // Insert all records in one batch - much faster as it holds the lock only once
        let record_ids = self.record_manager.bulk_insert(table, records)?;

        if !indexed_columns.is_empty() {
            for (row_idx, rid) in record_ids.iter().enumerate() {
                let row_keys = &index_keys[row_idx];
                for (col_idx, (col_name, _)) in indexed_columns.iter().enumerate() {
                    if let Some(key) = row_keys[col_idx] {
                        self.index_manager.insert(table, col_name, key, *rid)?;
                    }
                }
            }
        }

        Ok(record_ids.len())
    }

    pub fn delete(
        &mut self,
        table: &str,
        where_clauses: Option<Vec<WhereClause>>,
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
        let db_path = self.data_dir.join(db_name);
        let db_path_str = db_path.to_string_lossy().to_string();
        let table_path = self.table_path(db_name, table);
        let table_path_str = table_path.to_string_lossy().to_string();

        // Try to open table if not already open (ignore error if already open)
        let _ = self
            .record_manager
            .open_table(&table_path_str, schema.clone());

        let referencing_checks = self.build_referencing_fk_checks(&table_meta)?;
        if !referencing_checks.is_empty() {
            let db_name = self
                .current_db
                .clone()
                .ok_or(DatabaseError::NoDatabaseSelected)?;
            let scan_iter = self.record_manager.scan_iter(table)?;
            for item in scan_iter {
                let (_rid, record) = item?;
                let should_delete = match &where_clauses {
                    None => true,
                    Some(clauses) => self.evaluate_where(&record, &schema, clauses)?,
                };

                if should_delete {
                    self.validate_foreign_keys_on_delete_record(
                        &db_name,
                        &db_path_str,
                        &referencing_checks,
                        &record,
                    )?;
                }
            }
        }

        let indexed_columns = self.open_indexed_columns(&db_path_str, &table_meta)?;
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
                if !indexed_columns.is_empty() {
                    for (col_name, col_idx) in &indexed_columns {
                        if let Some(RecordValue::Int(val)) = record.get(*col_idx) {
                            let _ = self
                                .index_manager
                                .delete_entry(table, col_name, *val as i64, rid)?;
                        }
                    }
                }
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
        let db_path = self.data_dir.join(db_name);
        let db_path_str = db_path.to_string_lossy().to_string();

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

        let fk_checks = self.build_foreign_key_checks(&table_meta)?;
        let update_indices: HashSet<usize> = update_map.keys().copied().collect();
        let should_check_fk = !fk_checks.is_empty()
            && fk_checks.iter().any(|fk| {
                fk.column_indices
                    .iter()
                    .any(|idx| update_indices.contains(idx))
            });
        let referencing_checks = self.build_referencing_fk_checks(&table_meta)?;
        let should_check_referencing = !referencing_checks.is_empty()
            && referencing_checks.iter().any(|fk| {
                fk.parent_column_indices
                    .iter()
                    .any(|idx| update_indices.contains(idx))
            });

        let indexed_columns = if update_indices.is_empty() {
            Vec::new()
        } else {
            self.open_indexed_columns(&db_path_str, &table_meta)?
        };
        let mut updated = 0;
        let scan_iter = self.record_manager.scan_iter(table)?;
        for item in scan_iter {
            let (rid, mut record) = item?;
            let should_update = match &where_clauses {
                None => true,
                Some(clauses) => self.evaluate_where(&record, &schema, clauses)?,
            };

            if should_update {
                let original = record.clone();
                // Apply updates
                for (col_idx, new_value) in &update_map {
                    let data_type = &schema.columns[*col_idx].data_type;
                    let record_value = self.parser_value_to_record_value(new_value, data_type);
                    record.set(*col_idx, record_value);
                }

                if should_check_referencing {
                    let mut changed_fks = Vec::new();
                    for fk in &referencing_checks {
                        if !fk
                            .parent_column_indices
                            .iter()
                            .any(|idx| update_indices.contains(idx))
                        {
                            continue;
                        }
                        let mut changed = false;
                        for idx in &fk.parent_column_indices {
                            let old_value = original.get(*idx).unwrap();
                            let new_value = record.get(*idx).unwrap();
                            if old_value != new_value {
                                changed = true;
                                break;
                            }
                        }
                        if changed {
                            changed_fks.push(fk.clone());
                        }
                    }

                    if !changed_fks.is_empty() {
                        let db_name = self
                            .current_db
                            .clone()
                            .ok_or(DatabaseError::NoDatabaseSelected)?;
                        self.validate_foreign_keys_on_delete_record(
                            &db_name,
                            &db_path_str,
                            &changed_fks,
                            &original,
                        )?;
                    }
                }

                if should_check_fk {
                    let db_name = self
                        .current_db
                        .clone()
                        .ok_or(DatabaseError::NoDatabaseSelected)?;
                    self.validate_foreign_keys_for_record(
                        &db_name,
                        &fk_checks,
                        &record,
                        Some(&update_indices),
                    )?;
                }

                let updated_record = record.clone();
                self.record_manager.update(table, rid, updated_record)?;
                if !indexed_columns.is_empty() {
                    for (col_name, col_idx) in &indexed_columns {
                        if !update_indices.contains(col_idx) {
                            continue;
                        }
                        let old_val = original.get(*col_idx).unwrap();
                        let new_val = record.get(*col_idx).unwrap();
                        match (old_val, new_val) {
                            (RecordValue::Int(old_key), RecordValue::Int(new_key)) => {
                                if old_key != new_key {
                                    let _ = self.index_manager.delete_entry(
                                        table,
                                        col_name,
                                        *old_key as i64,
                                        rid,
                                    )?;
                                    self.index_manager.insert(
                                        table,
                                        col_name,
                                        *new_key as i64,
                                        rid,
                                    )?;
                                }
                            }
                            (RecordValue::Int(old_key), _) => {
                                let _ = self.index_manager.delete_entry(
                                    table,
                                    col_name,
                                    *old_key as i64,
                                    rid,
                                )?;
                            }
                            (_, RecordValue::Int(new_key)) => {
                                self.index_manager.insert(
                                    table,
                                    col_name,
                                    *new_key as i64,
                                    rid,
                                )?;
                            }
                            _ => {}
                        }
                    }
                }
                updated += 1;
            }
        }

        Ok(updated)
    }

    pub fn select(
        &mut self,
        clause: SelectClause,
    ) -> DatabaseResult<(Vec<String>, Vec<Vec<String>>)> {
        match clause.table.len() {
            1 => self.select_single_table(clause),
            2 => self.select_two_table_join(clause),
            _ => Err(DatabaseError::TypeMismatch(
                "Only single-table and two-table queries are supported".to_string(),
            )),
        }
    }

    fn select_single_table(
        &mut self,
        clause: SelectClause,
    ) -> DatabaseResult<(Vec<String>, Vec<Vec<String>>)> {
        let metadata = self
            .current_metadata
            .as_ref()
            .ok_or(DatabaseError::NoDatabaseSelected)?;

        let table_name = &clause.table[0];
        let table_meta = metadata.get_table(table_name)?;
        let schema = self.metadata_to_schema(table_meta);

        let db_name = self.current_db.as_ref().unwrap();
        let table_path = self.table_path(db_name, table_name);
        let table_path_str = table_path.to_string_lossy().to_string();

        // Open table if not already open
        self.record_manager
            .open_table(&table_path_str, schema.clone())?;

        let (selected_columns, col_indices) = match &clause.selectors {
            Selectors::All => {
                let columns = schema.columns.iter().map(|c| c.name.clone()).collect();
                let indices = (0..schema.columns.len()).collect();
                (columns, indices)
            }
            Selectors::List(selectors) => {
                let mut columns = Vec::new();
                let mut indices = Vec::new();
                for selector in selectors {
                    match selector {
                        Selector::Column(tc) => {
                            let col_idx = self.resolve_single_column_index(&schema, tc)?;
                            columns.push(tc.column.clone());
                            indices.push(col_idx);
                        }
                        _ => {
                            return Err(DatabaseError::TypeMismatch(
                                "Aggregates not yet supported".to_string(),
                            ));
                        }
                    }
                }
                (columns, indices)
            }
        };

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

    fn select_two_table_join(
        &mut self,
        clause: SelectClause,
    ) -> DatabaseResult<(Vec<String>, Vec<Vec<String>>)> {
        let (left_name, right_name) = (&clause.table[0], &clause.table[1]);

        let (left_meta, right_meta) = {
            let metadata = self
                .current_metadata
                .as_ref()
                .ok_or(DatabaseError::NoDatabaseSelected)?;
            (
                metadata.get_table(left_name)?.clone(),
                metadata.get_table(right_name)?.clone(),
            )
        };

        let left_schema = self.metadata_to_schema(&left_meta);
        let right_schema = self.metadata_to_schema(&right_meta);

        let db_name = self.current_db.as_ref().unwrap();
        let left_path = self.table_path(db_name, left_name);
        let right_path = self.table_path(db_name, right_name);
        let left_path_str = left_path.to_string_lossy().to_string();
        let right_path_str = right_path.to_string_lossy().to_string();

        // Open tables if not already open
        self.record_manager
            .open_table(&left_path_str, left_schema.clone())?;
        self.record_manager
            .open_table(&right_path_str, right_schema.clone())?;

        // Materialize the right table to keep join logic simple.
        let right_rows = self.record_manager.scan(right_name)?;
        let right_records: Vec<Record> = right_rows.into_iter().map(|(_, r)| r).collect();

        let (selected_columns, col_refs) = match &clause.selectors {
            Selectors::All => {
                let mut columns = Vec::new();
                let mut refs = Vec::new();
                for (idx, col) in left_schema.columns.iter().enumerate() {
                    columns.push(col.name.clone());
                    refs.push(JoinColumnRef {
                        side: JoinSide::Left,
                        index: idx,
                    });
                }
                for (idx, col) in right_schema.columns.iter().enumerate() {
                    columns.push(col.name.clone());
                    refs.push(JoinColumnRef {
                        side: JoinSide::Right,
                        index: idx,
                    });
                }
                (columns, refs)
            }
            Selectors::List(selectors) => {
                let mut columns = Vec::new();
                let mut refs = Vec::new();
                for selector in selectors {
                    match selector {
                        Selector::Column(tc) => {
                            let col_ref = self.resolve_join_column_ref(
                                tc,
                                left_name,
                                &left_schema,
                                right_name,
                                &right_schema,
                            )?;
                            columns.push(tc.column.clone());
                            refs.push(col_ref);
                        }
                        _ => {
                            return Err(DatabaseError::TypeMismatch(
                                "Aggregates not yet supported".to_string(),
                            ));
                        }
                    }
                }
                (columns, refs)
            }
        };

        let scan_iter = self.record_manager.scan_iter(left_name)?;
        let mut result_rows = Vec::new();

        for item in scan_iter {
            let (_rid, left_record) = item?;
            for right_record in &right_records {
                let matches = if clause.where_clauses.is_empty() {
                    true
                } else {
                    self.evaluate_join_where(
                        &left_record,
                        &left_schema,
                        left_name,
                        right_record,
                        &right_schema,
                        right_name,
                        &clause.where_clauses,
                    )?
                };

                if matches {
                    let mut row = Vec::new();
                    for col_ref in &col_refs {
                        let value = match col_ref.side {
                            JoinSide::Left => left_record.get(col_ref.index).unwrap(),
                            JoinSide::Right => right_record.get(col_ref.index).unwrap(),
                        };
                        row.push(self.format_value(value));
                    }
                    result_rows.push(row);
                }
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
                    total_inserted += self.bulk_insert(
                        table,
                        std::mem::take(&mut batch_rows),
                        true,
                        true,
                        true,
                    )?;
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
            total_inserted += self.bulk_insert(table, batch_rows, true, true, true)?;
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

    fn build_foreign_key_check(
        &self,
        table_meta: &TableMetadata,
        fk: &ForeignKeyMetadata,
    ) -> DatabaseResult<ForeignKeyCheck> {
        if fk.columns.len() != fk.ref_columns.len() {
            return Err(DatabaseError::TypeMismatch(
                "Foreign key column count mismatch".to_string(),
            ));
        }

        let metadata = self
            .current_metadata
            .as_ref()
            .ok_or(DatabaseError::NoDatabaseSelected)?;

        let mut column_indices = Vec::with_capacity(fk.columns.len());
        for col_name in &fk.columns {
            let idx = table_meta
                .columns
                .iter()
                .position(|c| &c.name == col_name)
                .ok_or_else(|| {
                    DatabaseError::ColumnNotFound(col_name.clone(), table_meta.name.clone())
                })?;
            column_indices.push(idx);
        }

        let ref_table_meta = metadata.get_table(&fk.ref_table)?.clone();
        let mut ref_column_indices = Vec::with_capacity(fk.ref_columns.len());
        for col_name in &fk.ref_columns {
            let idx = ref_table_meta
                .columns
                .iter()
                .position(|c| &c.name == col_name)
                .ok_or_else(|| {
                    DatabaseError::ColumnNotFound(col_name.clone(), fk.ref_table.clone())
                })?;
            ref_column_indices.push(idx);
        }

        for (pos, (col_idx, ref_idx)) in column_indices
            .iter()
            .zip(ref_column_indices.iter())
            .enumerate()
        {
            let col_type = table_meta.columns[*col_idx].to_data_type();
            let ref_type = ref_table_meta.columns[*ref_idx].to_data_type();
            if col_type != ref_type {
                return Err(DatabaseError::TypeMismatch(format!(
                    "Foreign key column type mismatch: {}.{} vs {}.{}",
                    table_meta.name,
                    fk.columns[pos],
                    fk.ref_table,
                    fk.ref_columns[pos]
                )));
            }
        }

        Ok(ForeignKeyCheck {
            table_name: table_meta.name.clone(),
            column_names: fk.columns.clone(),
            column_indices,
            ref_table: fk.ref_table.clone(),
            ref_column_names: fk.ref_columns.clone(),
            ref_column_indices,
            ref_schema: self.metadata_to_schema(&ref_table_meta),
        })
    }

    fn build_foreign_key_checks(
        &self,
        table_meta: &TableMetadata,
    ) -> DatabaseResult<Vec<ForeignKeyCheck>> {
        if table_meta.foreign_keys.is_empty() {
            return Ok(Vec::new());
        }

        let mut checks = Vec::with_capacity(table_meta.foreign_keys.len());
        for fk in &table_meta.foreign_keys {
            checks.push(self.build_foreign_key_check(table_meta, fk)?);
        }
        Ok(checks)
    }

    fn foreign_key_error(&self, fk: &ForeignKeyCheck, values: &[RecordValue]) -> DatabaseError {
        let value_desc = values
            .iter()
            .map(|value| format!("{:?}", value))
            .collect::<Vec<_>>()
            .join(", ");
        DatabaseError::ForeignKeyViolation(format!(
            "{}({}) references {}({}): value {} not found",
            fk.table_name,
            fk.column_names.join(", "),
            fk.ref_table,
            fk.ref_column_names.join(", "),
            value_desc
        ))
    }

    fn foreign_key_delete_error(
        &self,
        fk: &ReferencingForeignKeyCheck,
        values: &[RecordValue],
    ) -> DatabaseError {
        let value_desc = values
            .iter()
            .map(|value| format!("{:?}", value))
            .collect::<Vec<_>>()
            .join(", ");
        DatabaseError::ForeignKeyViolation(format!(
            "delete violates {}: {}({}) referenced by {}({}) with value {}",
            fk.fk_name,
            fk.parent_table,
            fk.parent_column_names.join(", "),
            fk.child_table,
            fk.child_column_names.join(", "),
            value_desc
        ))
    }

    fn validate_foreign_keys_for_records(
        &mut self,
        table_meta: &TableMetadata,
        records: &[Record],
    ) -> DatabaseResult<()> {
        if table_meta.foreign_keys.is_empty() || records.is_empty() {
            return Ok(());
        }

        let fk_checks = self.build_foreign_key_checks(table_meta)?;
        if fk_checks.is_empty() {
            return Ok(());
        }

        let db_name = self
            .current_db
            .clone()
            .ok_or(DatabaseError::NoDatabaseSelected)?;
        for record in records {
            self.validate_foreign_keys_for_record(&db_name, &fk_checks, record, None)?;
        }

        Ok(())
    }

    fn validate_foreign_keys_for_record(
        &mut self,
        db_name: &str,
        fk_checks: &[ForeignKeyCheck],
        record: &Record,
        update_indices: Option<&HashSet<usize>>,
    ) -> DatabaseResult<()> {
        if fk_checks.is_empty() {
            return Ok(());
        }

        let db_path = self.data_dir.join(db_name);
        let db_path_str = db_path.to_string_lossy().to_string();

        for fk in fk_checks {
            if let Some(indices) = update_indices {
                if !fk
                    .column_indices
                    .iter()
                    .any(|idx| indices.contains(idx))
                {
                    continue;
                }
            }

            let mut values = Vec::with_capacity(fk.column_indices.len());
            let mut has_null = false;
            for col_idx in &fk.column_indices {
                let value = record.get(*col_idx).unwrap();
                if value.is_null() {
                    has_null = true;
                    break;
                }
                values.push(value.clone());
            }

            if has_null {
                continue;
            }

            if fk.column_indices.len() == 1 && fk.ref_column_indices.len() == 1 {
                let fk_value = match values[0] {
                    RecordValue::Int(v) => v as i64,
                    _ => {
                        return Err(DatabaseError::TypeMismatch(
                            "Foreign key column must be INT".to_string(),
                        ))
                    }
                };

                let mut used_index = false;
                let mut index_found = false;
                match self.index_manager.open_index(
                    &db_path_str,
                    &fk.ref_table,
                    &fk.ref_column_names[0],
                ) {
                    Ok(()) => {
                        used_index = true;
                        index_found = self
                            .index_manager
                            .search(&fk.ref_table, &fk.ref_column_names[0], fk_value)
                            .is_some();
                    }
                    Err(IndexError::IndexNotFound(_)) => {}
                    Err(err) => return Err(DatabaseError::IndexError(err)),
                };

                if used_index && index_found {
                    continue;
                }
            }

            let table_path = self.table_path(db_name, &fk.ref_table);
            let _ = self
                .record_manager
                .open_table(&table_path.to_string_lossy(), fk.ref_schema.clone());
            let scan_iter = self.record_manager.scan_iter(&fk.ref_table)?;
            let mut found = false;
            for item in scan_iter {
                let (_rid, ref_record) = item?;
                let mut matches = true;
                for (idx, value) in fk.ref_column_indices.iter().zip(values.iter()) {
                    if ref_record.get(*idx).unwrap() != value {
                        matches = false;
                        break;
                    }
                }
                if matches {
                    found = true;
                    break;
                }
            }

            if !found {
                return Err(self.foreign_key_error(fk, &values));
            }
        }

        Ok(())
    }

    fn build_referencing_fk_checks(
        &self,
        parent_meta: &TableMetadata,
    ) -> DatabaseResult<Vec<ReferencingForeignKeyCheck>> {
        let metadata = self
            .current_metadata
            .as_ref()
            .ok_or(DatabaseError::NoDatabaseSelected)?;

        let mut checks = Vec::new();
        for child_meta in metadata.tables.values() {
            for fk in &child_meta.foreign_keys {
                if fk.ref_table != parent_meta.name {
                    continue;
                }

                if fk.columns.len() != fk.ref_columns.len() {
                    return Err(DatabaseError::TypeMismatch(
                        "Foreign key column count mismatch".to_string(),
                    ));
                }

                let mut child_column_indices = Vec::with_capacity(fk.columns.len());
                for col_name in &fk.columns {
                    let idx = child_meta
                        .columns
                        .iter()
                        .position(|c| &c.name == col_name)
                        .ok_or_else(|| {
                            DatabaseError::ColumnNotFound(
                                col_name.clone(),
                                child_meta.name.clone(),
                            )
                        })?;
                    child_column_indices.push(idx);
                }

                let mut parent_column_indices = Vec::with_capacity(fk.ref_columns.len());
                for col_name in &fk.ref_columns {
                    let idx = parent_meta
                        .columns
                        .iter()
                        .position(|c| &c.name == col_name)
                        .ok_or_else(|| {
                            DatabaseError::ColumnNotFound(
                                col_name.clone(),
                                parent_meta.name.clone(),
                            )
                        })?;
                    parent_column_indices.push(idx);
                }

                for (pos, (child_idx, parent_idx)) in child_column_indices
                    .iter()
                    .zip(parent_column_indices.iter())
                    .enumerate()
                {
                    let child_type = child_meta.columns[*child_idx].to_data_type();
                    let parent_type = parent_meta.columns[*parent_idx].to_data_type();
                    if child_type != parent_type {
                        return Err(DatabaseError::TypeMismatch(format!(
                            "Foreign key column type mismatch: {}.{} vs {}.{}",
                            child_meta.name,
                            fk.columns[pos],
                            parent_meta.name,
                            fk.ref_columns[pos]
                        )));
                    }
                }

                checks.push(ReferencingForeignKeyCheck {
                    child_table: child_meta.name.clone(),
                    child_column_names: fk.columns.clone(),
                    child_column_indices,
                    child_schema: self.metadata_to_schema(child_meta),
                    parent_table: parent_meta.name.clone(),
                    parent_column_names: fk.ref_columns.clone(),
                    parent_column_indices,
                    fk_name: fk.name.clone(),
                });
            }
        }

        Ok(checks)
    }

    fn validate_foreign_keys_on_delete_record(
        &mut self,
        db_name: &str,
        db_path_str: &str,
        referencing_checks: &[ReferencingForeignKeyCheck],
        record: &Record,
    ) -> DatabaseResult<()> {
        for fk in referencing_checks {
            let mut values = Vec::with_capacity(fk.parent_column_indices.len());
            let mut has_null = false;
            for col_idx in &fk.parent_column_indices {
                let value = record.get(*col_idx).unwrap();
                if value.is_null() {
                    has_null = true;
                    break;
                }
                values.push(value.clone());
            }

            if has_null {
                continue;
            }

            if fk.child_column_indices.len() == 1 && fk.parent_column_indices.len() == 1 {
                let fk_value = match values[0] {
                    RecordValue::Int(v) => v as i64,
                    _ => {
                        return Err(DatabaseError::TypeMismatch(
                            "Foreign key column must be INT".to_string(),
                        ))
                    }
                };

                let mut used_index = false;
                let mut index_found = false;
                match self.index_manager.open_index(
                    db_path_str,
                    &fk.child_table,
                    &fk.child_column_names[0],
                ) {
                    Ok(()) => {
                        used_index = true;
                        index_found = self
                            .index_manager
                            .search(&fk.child_table, &fk.child_column_names[0], fk_value)
                            .is_some();
                    }
                    Err(IndexError::IndexNotFound(_)) => {}
                    Err(err) => return Err(DatabaseError::IndexError(err)),
                };

                if used_index && index_found {
                    return Err(self.foreign_key_delete_error(fk, &values));
                }
            }

            let table_path = self.table_path(db_name, &fk.child_table);
            let _ = self
                .record_manager
                .open_table(&table_path.to_string_lossy(), fk.child_schema.clone());
            let scan_iter = self.record_manager.scan_iter(&fk.child_table)?;
            let mut found = false;
            for item in scan_iter {
                let (_rid, child_record) = item?;
                let mut matches = true;
                for (idx, value) in fk.child_column_indices.iter().zip(values.iter()) {
                    let child_value = child_record.get(*idx).unwrap();
                    if child_value.is_null() || child_value != value {
                        matches = false;
                        break;
                    }
                }
                if matches {
                    found = true;
                    break;
                }
            }

            if found {
                return Err(self.foreign_key_delete_error(fk, &values));
            }
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

    fn open_indexed_columns(
        &mut self,
        db_path: &str,
        table_meta: &TableMetadata,
    ) -> DatabaseResult<Vec<(String, usize)>> {
        let mut columns = Vec::new();
        let mut seen = HashSet::new();
        let mut candidates = Vec::new();

        if let Some(pk_cols) = &table_meta.primary_key {
            candidates.extend(pk_cols.iter().cloned());
        }
        for index_meta in &table_meta.indexes {
            candidates.extend(index_meta.columns.iter().cloned());
        }

        for col_name in candidates {
            if !seen.insert(col_name.clone()) {
                continue;
            }
            let col_idx = table_meta
                .columns
                .iter()
                .position(|c| c.name == col_name)
                .ok_or_else(|| {
                    DatabaseError::ColumnNotFound(col_name.clone(), table_meta.name.clone())
                })?;
            if table_meta.columns[col_idx].to_data_type() != DataType::Int {
                continue;
            }
            match self
                .index_manager
                .open_index(db_path, &table_meta.name, &col_name)
            {
                Ok(()) => columns.push((col_name, col_idx)),
                Err(IndexError::IndexNotFound(_)) => {}
                Err(err) => return Err(DatabaseError::IndexError(err)),
            }
        }

        Ok(columns)
    }

    fn resolve_single_column_index(
        &self,
        schema: &TableSchema,
        column: &TableColumn,
    ) -> DatabaseResult<usize> {
        if let Some(table) = &column.table {
            if table != &schema.table_name {
                return Err(DatabaseError::ColumnNotFound(
                    column.column.clone(),
                    table.clone(),
                ));
            }
        }

        schema
            .columns
            .iter()
            .position(|c| c.name == column.column)
            .ok_or_else(|| {
                DatabaseError::ColumnNotFound(column.column.clone(), schema.table_name.clone())
            })
    }

    fn resolve_join_column_ref(
        &self,
        column: &TableColumn,
        left_name: &str,
        left_schema: &TableSchema,
        right_name: &str,
        right_schema: &TableSchema,
    ) -> DatabaseResult<JoinColumnRef> {
        let left_idx = left_schema
            .columns
            .iter()
            .position(|c| c.name == column.column);
        let right_idx = right_schema
            .columns
            .iter()
            .position(|c| c.name == column.column);

        if let Some(table) = &column.table {
            if table == left_name {
                return left_idx
                    .map(|index| JoinColumnRef {
                        side: JoinSide::Left,
                        index,
                    })
                    .ok_or_else(|| {
                        DatabaseError::ColumnNotFound(column.column.clone(), left_name.to_string())
                    });
            }
            if table == right_name {
                return right_idx
                    .map(|index| JoinColumnRef {
                        side: JoinSide::Right,
                        index,
                    })
                    .ok_or_else(|| {
                        DatabaseError::ColumnNotFound(column.column.clone(), right_name.to_string())
                    });
            }
            return Err(DatabaseError::ColumnNotFound(
                column.column.clone(),
                table.clone(),
            ));
        }

        match (left_idx, right_idx) {
            (Some(index), None) => Ok(JoinColumnRef {
                side: JoinSide::Left,
                index,
            }),
            (None, Some(index)) => Ok(JoinColumnRef {
                side: JoinSide::Right,
                index,
            }),
            (Some(_), Some(_)) => Err(DatabaseError::TypeMismatch(format!(
                "Ambiguous column {}",
                column.column
            ))),
            (None, None) => Err(DatabaseError::ColumnNotFound(
                column.column.clone(),
                left_name.to_string(),
            )),
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
                    let col_idx = self.resolve_single_column_index(schema, col)?;

                    let left_val = record.get(col_idx).unwrap();

                    let right_val = match expr {
                        Expression::Value(v) => {
                            let data_type = &schema.columns[col_idx].data_type;
                            self.parser_value_to_record_value(v, data_type)
                        }
                        Expression::Column(_) => {
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
                    let col_idx = self.resolve_single_column_index(schema, col)?;
                    if !record.get(col_idx).unwrap().is_null() {
                        return Ok(false);
                    }
                }
                WhereClause::NotNull(col) => {
                    let col_idx = self.resolve_single_column_index(schema, col)?;
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

    fn join_value_and_type<'a>(
        &self,
        column: &TableColumn,
        left_record: &'a Record,
        left_schema: &'a TableSchema,
        left_name: &str,
        right_record: &'a Record,
        right_schema: &'a TableSchema,
        right_name: &str,
    ) -> DatabaseResult<(&'a RecordValue, &'a DataType)> {
        let col_ref = self.resolve_join_column_ref(
            column,
            left_name,
            left_schema,
            right_name,
            right_schema,
        )?;

        let (record, schema) = match col_ref.side {
            JoinSide::Left => (left_record, left_schema),
            JoinSide::Right => (right_record, right_schema),
        };

        let value = record.get(col_ref.index).unwrap();
        let data_type = &schema.columns[col_ref.index].data_type;
        Ok((value, data_type))
    }

    fn evaluate_join_where(
        &self,
        left_record: &Record,
        left_schema: &TableSchema,
        left_name: &str,
        right_record: &Record,
        right_schema: &TableSchema,
        right_name: &str,
        where_clauses: &[WhereClause],
    ) -> DatabaseResult<bool> {
        for clause in where_clauses {
            match clause {
                WhereClause::Op(col, op, expr) => {
                    let (left_val, data_type) = self.join_value_and_type(
                        col,
                        left_record,
                        left_schema,
                        left_name,
                        right_record,
                        right_schema,
                        right_name,
                    )?;

                    let right_val = match expr {
                        Expression::Value(v) => self.parser_value_to_record_value(v, data_type),
                        Expression::Column(tc) => {
                            let (value, _) = self.join_value_and_type(
                                tc,
                                left_record,
                                left_schema,
                                left_name,
                                right_record,
                                right_schema,
                                right_name,
                            )?;
                            value.clone()
                        }
                    };

                    if !self.compare_values(left_val, op, &right_val) {
                        return Ok(false);
                    }
                }
                WhereClause::Null(col) => {
                    let (value, _) = self.join_value_and_type(
                        col,
                        left_record,
                        left_schema,
                        left_name,
                        right_record,
                        right_schema,
                        right_name,
                    )?;
                    if !value.is_null() {
                        return Ok(false);
                    }
                }
                WhereClause::NotNull(col) => {
                    let (value, _) = self.join_value_and_type(
                        col,
                        left_record,
                        left_schema,
                        left_name,
                        right_record,
                        right_schema,
                        right_name,
                    )?;
                    if value.is_null() {
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
        stmt: AlterStatement,
    ) -> DatabaseResult<QueryResult> {
        match stmt {
            AlterStatement::AddIndex(table_name, index_name, columns) => {
                if columns.len() != 1 {
                    return Err(DatabaseError::TypeMismatch(
                        "Only single-column indexes are supported".to_string(),
                    ));
                }

                let column_name = columns[0].clone();
                let index_name = index_name.unwrap_or_else(|| format!("idx_{}", column_name));

                let (schema, col_idx) = {
                    let metadata = self
                        .current_metadata
                        .as_ref()
                        .ok_or(DatabaseError::NoDatabaseSelected)?;
                    let table_meta = metadata.get_table(&table_name)?.clone();

                    if table_meta
                        .indexes
                        .iter()
                        .any(|idx| idx.name == index_name || idx.columns == columns)
                    {
                        return Err(DatabaseError::TypeMismatch(format!(
                            "Index {} already exists",
                            index_name
                        )));
                    }

                    let col_idx = table_meta
                        .columns
                        .iter()
                        .position(|c| c.name == column_name)
                        .ok_or_else(|| {
                            DatabaseError::ColumnNotFound(column_name.clone(), table_name.clone())
                        })?;

                    if table_meta.columns[col_idx].to_data_type() != DataType::Int {
                        return Err(DatabaseError::TypeMismatch(
                            "Only INT columns can be indexed".to_string(),
                        ));
                    }

                    let schema = self.metadata_to_schema(&table_meta);
                    (schema, col_idx)
                };

                let db_name = self
                    .current_db
                    .as_ref()
                    .ok_or(DatabaseError::NoDatabaseSelected)?;
                let db_path = self.data_dir.join(db_name);
                let db_path_str = db_path.to_string_lossy().to_string();

                let table_path = self.table_path(db_name, &table_name);
                self.record_manager.open_table(
                    &table_path.to_string_lossy().to_string(),
                    schema.clone(),
                )?;

                let scan_iter = self.record_manager.scan_iter(&table_name)?;
                let table_iter = TableIntColumnIter::new(scan_iter, col_idx);
                self.index_manager.create_index_from_table(
                    &db_path_str,
                    &table_name,
                    &column_name,
                    table_iter,
                )?;

                let metadata = self.current_metadata.as_mut().unwrap();
                let table_meta_mut = metadata.get_table_mut(&table_name)?;
                table_meta_mut.indexes.push(IndexMetadata {
                    name: index_name,
                    columns,
                });
                self.save_current_metadata()?;

                Ok(QueryResult::Empty)
            }
            AlterStatement::DropIndex(table_name, index_name) => {
                let db_name = self
                    .current_db
                    .as_ref()
                    .ok_or(DatabaseError::NoDatabaseSelected)?;
                let db_path = self.data_dir.join(db_name);
                let db_path_str = db_path.to_string_lossy().to_string();

                let column_name = {
                    let metadata = self
                        .current_metadata
                        .as_mut()
                        .ok_or(DatabaseError::NoDatabaseSelected)?;
                    let table_meta = metadata.get_table_mut(&table_name)?;

                    let pos = table_meta
                        .indexes
                        .iter()
                        .position(|idx| idx.name == index_name)
                        .ok_or_else(|| {
                            DatabaseError::TypeMismatch(format!(
                                "Index {} not found",
                                index_name
                            ))
                        })?;

                    let index_meta = table_meta.indexes.remove(pos);
                    if index_meta.columns.len() != 1 {
                        return Err(DatabaseError::TypeMismatch(
                            "Only single-column indexes are supported".to_string(),
                        ));
                    }
                    index_meta.columns[0].clone()
                };

                if let Err(err) =
                    self.index_manager
                        .drop_index(&db_path_str, &table_name, &column_name)
                {
                    if !matches!(err, crate::index::IndexError::IndexNotFound(_)) {
                        return Err(DatabaseError::IndexError(err));
                    }
                }

                self.save_current_metadata()?;
                Ok(QueryResult::Empty)
            }
            AlterStatement::AddPKey(table_name, columns) => {
                let (schema, pk_indices) = {
                    let metadata = self
                        .current_metadata
                        .as_ref()
                        .ok_or(DatabaseError::NoDatabaseSelected)?;
                    let table_meta = metadata.get_table(&table_name)?.clone();

                    if table_meta.primary_key.is_some() {
                        return Err(DatabaseError::PrimaryKeyError);
                    }

                    let mut indices = Vec::with_capacity(columns.len());
                    for col_name in &columns {
                        let idx = table_meta
                            .columns
                            .iter()
                            .position(|c| &c.name == col_name)
                            .ok_or_else(|| {
                                DatabaseError::ColumnNotFound(
                                    col_name.clone(),
                                    table_name.clone(),
                                )
                            })?;
                        indices.push(idx);
                    }

                    let schema = self.metadata_to_schema(&table_meta);
                    (schema, indices)
                };

                let db_name = self
                    .current_db
                    .as_ref()
                    .ok_or(DatabaseError::NoDatabaseSelected)?;
                let table_path = self.table_path(db_name, &table_name);
                let _ = self
                    .record_manager
                    .open_table(&table_path.to_string_lossy().to_string(), schema);

                let scan_iter = self.record_manager.scan_iter(&table_name)?;
                let mut pk_set = HashSet::new();
                for item in scan_iter {
                    let (_rid, record) = item?;
                    let mut key_parts = Vec::with_capacity(pk_indices.len());
                    for &idx in &pk_indices {
                        let value = record.get(idx).unwrap();
                        key_parts.push(format!("{:?}", value));
                    }

                    if !pk_set.insert(key_parts.join("|")) {
                        return Err(DatabaseError::PrimaryKeyViolation);
                    }
                }

                let metadata = self.current_metadata.as_mut().unwrap();
                let table_meta_mut = metadata.get_table_mut(&table_name)?;
                table_meta_mut.primary_key = Some(columns.clone());
                for col in &mut table_meta_mut.columns {
                    if columns.contains(&col.name) {
                        col.not_null = true;
                    }
                }
                self.save_current_metadata()?;

                Ok(QueryResult::Empty)
            }
            AlterStatement::DropPKey(table_name, _pkey_name) => {
                let metadata = self
                    .current_metadata
                    .as_mut()
                    .ok_or(DatabaseError::NoDatabaseSelected)?;
                let table_meta = metadata.get_table_mut(&table_name)?;

                if table_meta.primary_key.is_none() {
                    return Err(DatabaseError::PrimaryKeyError);
                }

                table_meta.primary_key = None;
                self.save_current_metadata()?;
                Ok(QueryResult::Empty)
            }
            AlterStatement::AddFKey(table_name, fk_name, fk_cols, ref_table, ref_cols) => {
                if fk_cols.len() != ref_cols.len() {
                    return Err(DatabaseError::TypeMismatch(
                        "Foreign key column count mismatch".to_string(),
                    ));
                }

                let fk_name = fk_name.unwrap_or_else(|| format!("fk_{}", table_name));
                let fk_meta = ForeignKeyMetadata {
                    name: fk_name.clone(),
                    columns: fk_cols.clone(),
                    ref_table: ref_table.clone(),
                    ref_columns: ref_cols.clone(),
                };

                let (table_meta, schema) = {
                    let metadata = self
                        .current_metadata
                        .as_ref()
                        .ok_or(DatabaseError::NoDatabaseSelected)?;
                    let table_meta = metadata.get_table(&table_name)?.clone();

                    if table_meta
                        .foreign_keys
                        .iter()
                        .any(|fk| fk.name == fk_name)
                    {
                        return Err(DatabaseError::TypeMismatch(format!(
                            "Foreign key {} already exists",
                            fk_name
                        )));
                    }

                    let schema = self.metadata_to_schema(&table_meta);
                    (table_meta, schema)
                };

                let fk_check = self.build_foreign_key_check(&table_meta, &fk_meta)?;
                let db_name = self
                    .current_db
                    .clone()
                    .ok_or(DatabaseError::NoDatabaseSelected)?;
                let table_path = self.table_path(&db_name, &table_name);
                let _ = self
                    .record_manager
                    .open_table(&table_path.to_string_lossy(), schema);

                let scan_iter = self.record_manager.scan_iter(&table_name)?;
                for item in scan_iter {
                    let (_rid, record) = item?;
                    self.validate_foreign_keys_for_record(
                        &db_name,
                        std::slice::from_ref(&fk_check),
                        &record,
                        None,
                    )?;
                }

                let metadata = self.current_metadata.as_mut().unwrap();
                let table_meta_mut = metadata.get_table_mut(&table_name)?;
                table_meta_mut.foreign_keys.push(fk_meta);
                self.save_current_metadata()?;

                Ok(QueryResult::Empty)
            }
            AlterStatement::DropFKey(table_name, fk_name) => {
                let metadata = self
                    .current_metadata
                    .as_mut()
                    .ok_or(DatabaseError::NoDatabaseSelected)?;
                let table_meta = metadata.get_table_mut(&table_name)?;

                let pos = table_meta
                    .foreign_keys
                    .iter()
                    .position(|fk| fk.name == fk_name)
                    .ok_or_else(|| {
                        DatabaseError::TypeMismatch(format!(
                            "Foreign key {} not found",
                            fk_name
                        ))
                    })?;
                table_meta.foreign_keys.remove(pos);
                self.save_current_metadata()?;
                Ok(QueryResult::Empty)
            }
        }
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
