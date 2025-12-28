use csv::ReaderBuilder;
use regex::Regex;
use std::cmp::Ordering;
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
    use_indexes: bool,
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

struct TableCompositeIntColumnIter {
    scan_iter: TableScanIter,
    col_idx_left: usize,
    col_idx_right: usize,
}

impl TableCompositeIntColumnIter {
    fn new(scan_iter: TableScanIter, col_idx_left: usize, col_idx_right: usize) -> Self {
        Self {
            scan_iter,
            col_idx_left,
            col_idx_right,
        }
    }

    fn composite_key(left: i32, right: i32) -> i64 {
        let left = left as u32 as u64;
        let right = right as u32 as u64;
        ((left << 32) | right) as i64
    }
}

impl Iterator for TableCompositeIntColumnIter {
    type Item = crate::index::IndexResult<(RecordId, i64)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let item = self.scan_iter.next()?;
            match item {
                Ok((rid, record)) => {
                    let left = match record.get(self.col_idx_left) {
                        Some(RecordValue::Int(val)) => *val,
                        _ => continue,
                    };
                    let right = match record.get(self.col_idx_right) {
                        Some(RecordValue::Int(val)) => *val,
                        _ => continue,
                    };
                    return Some(Ok((rid, Self::composite_key(left, right))));
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

struct IndexDef {
    columns: Vec<String>,
    indices: Vec<usize>,
    storage_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum GroupKey {
    Int(i32),
    Float(u64),
    String(String),
    Null,
}

#[derive(Debug, Clone, Copy)]
enum NumericType {
    Int,
    Float,
}

#[derive(Debug, Clone)]
enum AggSpec {
    CountAll,
    Count {
        col_idx: usize,
    },
    Sum {
        col_idx: usize,
        numeric: NumericType,
    },
    Avg {
        col_idx: usize,
    },
    Min {
        col_idx: usize,
    },
    Max {
        col_idx: usize,
    },
}

#[derive(Debug, Clone)]
enum AggState {
    Count(i64),
    SumInt { sum: i64, has_value: bool },
    SumFloat { sum: f64, has_value: bool },
    Avg { sum: f64, count: i64 },
    Min(Option<RecordValue>),
    Max(Option<RecordValue>),
}

#[derive(Debug, Clone, Copy)]
enum OutputSelector {
    GroupKey,
    Agg(usize),
}

#[derive(Debug, Clone)]
struct GroupState {
    key: RecordValue,
    aggs: Vec<AggState>,
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

enum PreparedWhereClause {
    Op(TableColumn, Operator, Expression),
    Null(TableColumn),
    NotNull(TableColumn),
    Like(TableColumn, Regex),
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
            use_indexes: true,
        })
    }

    pub fn set_use_indexes(&mut self, use_indexes: bool) {
        self.use_indexes = use_indexes;
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
        let updated = self.ensure_foreign_key_indexes()?;
        if updated {
            self.save_current_metadata()?;
        }

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
        let db_name = self.current_db.as_ref().unwrap().clone();
        let table_path = self.table_path(&db_name, name);
        let schema = self.metadata_to_schema(&table_metadata);
        self.record_manager
            .create_table(&table_path.to_string_lossy(), schema)?;

        // Add to metadata
        let metadata = self.current_metadata.as_mut().unwrap();
        metadata.add_table(table_metadata);
        let _ = self.ensure_foreign_key_indexes()?;
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

                let mut used_index = false;
                if self.use_indexes {
                    let has_index = self
                        .index_manager
                        .open_index(&db_path.to_string_lossy(), table, pk_col_name)
                        .is_ok();
                    if has_index {
                        used_index = true;
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
                    }
                }

                if !used_index {
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
            } else if pk_cols.len() == 2 {
                let pk_indices = pk_indices.as_ref().unwrap();
                let storage_name = match Self::index_storage_name(pk_cols) {
                    Some(name) => name,
                    None => String::new(),
                };
                let mut used_index = false;
                if self.use_indexes && !storage_name.is_empty() {
                    let has_index = self
                        .index_manager
                        .open_index(&db_path.to_string_lossy(), table, &storage_name)
                        .is_ok();
                    if has_index {
                        used_index = true;
                        for record in &records {
                            let left = match record.get(pk_indices[0]).unwrap() {
                                RecordValue::Int(val) => *val,
                                _ => continue,
                            };
                            let right = match record.get(pk_indices[1]).unwrap() {
                                RecordValue::Int(val) => *val,
                                _ => continue,
                            };
                            let key =
                                TableCompositeIntColumnIter::composite_key(left, right);
                            if self
                                .index_manager
                                .search(table, &storage_name, key)
                                .is_some()
                            {
                                return Err(DatabaseError::PrimaryKeyViolation);
                            }
                        }
                    }
                }

                if !used_index {
                    // Multi-column PK: use table scan (but only once for the whole batch)
                    let existing_records = self.record_manager.scan(table)?;
                    for record in &records {
                        for (_, existing_record) in &existing_records {
                            let mut is_duplicate = true;
                            for &pk_idx in pk_indices {
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

        let indexed_defs = if skip_index_update {
            Vec::new()
        } else {
            self.open_indexed_defs(&db_path_str, &table_meta)?
        };
        let mut index_keys: Vec<Vec<Option<i64>>> = Vec::new();
        if !indexed_defs.is_empty() {
            index_keys.reserve(records.len());
            for record in &records {
                let mut row_keys = Vec::with_capacity(indexed_defs.len());
                for def in &indexed_defs {
                    let key = match def.indices.as_slice() {
                        [col_idx] => match record.get(*col_idx) {
                            Some(RecordValue::Int(val)) => Some(*val as i64),
                            _ => None,
                        },
                        [left_idx, right_idx] => {
                            let left = match record.get(*left_idx) {
                                Some(RecordValue::Int(val)) => *val,
                                _ => {
                                    row_keys.push(None);
                                    continue;
                                }
                            };
                            let right = match record.get(*right_idx) {
                                Some(RecordValue::Int(val)) => *val,
                                _ => {
                                    row_keys.push(None);
                                    continue;
                                }
                            };
                            Some(TableCompositeIntColumnIter::composite_key(left, right))
                        }
                        _ => None,
                    };
                    row_keys.push(key);
                }
                index_keys.push(row_keys);
            }
        }

        // Insert all records in one batch - much faster as it holds the lock only once
        let record_ids = self.record_manager.bulk_insert(table, records)?;

        if !indexed_defs.is_empty() {
            for (row_idx, rid) in record_ids.iter().enumerate() {
                let row_keys = &index_keys[row_idx];
                for (def_idx, def) in indexed_defs.iter().enumerate() {
                    if let Some(key) = row_keys[def_idx] {
                        self.index_manager
                            .insert(table, &def.storage_name, key, *rid)?;
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
        let where_slice: &[WhereClause] = match &where_clauses {
            Some(clauses) => clauses,
            None => &[],
        };
        let prepared_where = match &where_clauses {
            Some(clauses) => Some(self.prepare_where_clauses(clauses)?),
            None => None,
        };

        let indexed_defs = self.open_indexed_defs(&db_path_str, &table_meta)?;
        let mut deleted = 0;
        let mut targets = Vec::new();
        let index_candidates =
            self.index_candidates_for_where(&db_path_str, &table_meta, &schema, where_slice)?;
        if let Some(rids) = index_candidates {
            for rid in rids {
                let record = self.record_manager.get(table, rid)?;
                let should_delete = match &prepared_where {
                    None => true,
                    Some(clauses) => self.evaluate_prepared_where(&record, &schema, clauses)?,
                };
                if should_delete {
                    targets.push((rid, record));
                }
            }
        } else {
            let scan_iter = self.record_manager.scan_iter(table)?;
            for item in scan_iter {
                let (rid, record) = item?;
                let should_delete = match &prepared_where {
                    None => true,
                    Some(clauses) => self.evaluate_prepared_where(&record, &schema, clauses)?,
                };
                if should_delete {
                    targets.push((rid, record));
                }
            }
        }

        if !referencing_checks.is_empty() {
            let db_name = self
                .current_db
                .clone()
                .ok_or(DatabaseError::NoDatabaseSelected)?;
            for (_rid, record) in &targets {
                self.validate_foreign_keys_on_delete_record(
                    &db_name,
                    &db_path_str,
                    &referencing_checks,
                    record,
                )?;
            }
        }

        for (rid, record) in targets {
            self.record_manager.delete(table, rid)?;
            if !indexed_defs.is_empty() {
                for def in &indexed_defs {
                    let key = match def.indices.as_slice() {
                        [col_idx] => match record.get(*col_idx) {
                            Some(RecordValue::Int(val)) => Some(*val as i64),
                            _ => None,
                        },
                        [left_idx, right_idx] => {
                            let left = match record.get(*left_idx) {
                                Some(RecordValue::Int(val)) => *val,
                                _ => continue,
                            };
                            let right = match record.get(*right_idx) {
                                Some(RecordValue::Int(val)) => *val,
                                _ => continue,
                            };
                            Some(TableCompositeIntColumnIter::composite_key(left, right))
                        }
                        _ => None,
                    };
                    if let Some(key) = key {
                        let _ = self
                            .index_manager
                            .delete_entry(table, &def.storage_name, key, rid)?;
                    }
                }
            }
            deleted += 1;
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

        let indexed_defs = if update_indices.is_empty() {
            Vec::new()
        } else {
            self.open_indexed_defs(&db_path_str, &table_meta)?
        };
        let where_slice: &[WhereClause] = match &where_clauses {
            Some(clauses) => clauses,
            None => &[],
        };
        let prepared_where = match &where_clauses {
            Some(clauses) => Some(self.prepare_where_clauses(clauses)?),
            None => None,
        };
        let mut updated = 0;
        let mut targets = Vec::new();
        let index_candidates =
            self.index_candidates_for_where(&db_path_str, &table_meta, &schema, where_slice)?;
        if let Some(rids) = index_candidates {
            for rid in rids {
                let record = self.record_manager.get(table, rid)?;
                let should_update = match &prepared_where {
                    None => true,
                    Some(clauses) => self.evaluate_prepared_where(&record, &schema, clauses)?,
                };
                if should_update {
                    targets.push((rid, record));
                }
            }
        } else {
            let scan_iter = self.record_manager.scan_iter(table)?;
            for item in scan_iter {
                let (rid, record) = item?;
                let should_update = match &prepared_where {
                    None => true,
                    Some(clauses) => self.evaluate_prepared_where(&record, &schema, clauses)?,
                };
                if should_update {
                    targets.push((rid, record));
                }
            }
        }

        for (rid, mut record) in targets {
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
            if !indexed_defs.is_empty() {
                for def in &indexed_defs {
                    let mut uses_update = false;
                    for idx in &def.indices {
                        if update_indices.contains(idx) {
                            uses_update = true;
                            break;
                        }
                    }
                    if !uses_update {
                        continue;
                    }

                    let old_key = match def.indices.as_slice() {
                        [col_idx] => match original.get(*col_idx).unwrap() {
                            RecordValue::Int(val) => Some(*val as i64),
                            _ => None,
                        },
                        [left_idx, right_idx] => match (
                            original.get(*left_idx).unwrap(),
                            original.get(*right_idx).unwrap(),
                        ) {
                            (RecordValue::Int(left), RecordValue::Int(right)) => Some(
                                TableCompositeIntColumnIter::composite_key(*left, *right),
                            ),
                            _ => None,
                        },
                        _ => None,
                    };
                    let new_key = match def.indices.as_slice() {
                        [col_idx] => match record.get(*col_idx).unwrap() {
                            RecordValue::Int(val) => Some(*val as i64),
                            _ => None,
                        },
                        [left_idx, right_idx] => match (
                            record.get(*left_idx).unwrap(),
                            record.get(*right_idx).unwrap(),
                        ) {
                            (RecordValue::Int(left), RecordValue::Int(right)) => Some(
                                TableCompositeIntColumnIter::composite_key(*left, *right),
                            ),
                            _ => None,
                        },
                        _ => None,
                    };

                    if old_key == new_key {
                        continue;
                    }

                    if let Some(key) = old_key {
                        let _ = self
                            .index_manager
                            .delete_entry(table, &def.storage_name, key, rid)?;
                    }
                    if let Some(key) = new_key {
                        self.index_manager
                            .insert(table, &def.storage_name, key, rid)?;
                    }
                }
            }
            updated += 1;
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
        let table_name = &clause.table[0];
        let (table_meta, schema) = {
            let metadata = self
                .current_metadata
                .as_ref()
                .ok_or(DatabaseError::NoDatabaseSelected)?;
            let table_meta = metadata.get_table(table_name)?.clone();
            let schema = self.metadata_to_schema(&table_meta);
            (table_meta, schema)
        };

        let db_name = self.current_db.as_ref().unwrap();
        let db_path = self.data_dir.join(db_name);
        let db_path_str = db_path.to_string_lossy();
        let table_path = self.table_path(db_name, table_name);
        let table_path_str = table_path.to_string_lossy().to_string();

        // Open table if not already open
        self.record_manager
            .open_table(&table_path_str, schema.clone())?;

        if self.select_has_aggregate(&clause.selectors) || clause.group_by.is_some() {
            return self.select_single_table_aggregate(&clause, &schema, table_name);
        }

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

        let order_by_idx = clause
            .order_by
            .as_ref()
            .map(|(col, _)| self.resolve_single_column_index(&schema, col))
            .transpose()?;
        let prepared_where = if clause.where_clauses.is_empty() {
            None
        } else {
            Some(self.prepare_where_clauses(&clause.where_clauses)?)
        };

        let mut result_rows = Vec::new();
        let mut order_rows = Vec::new();
        let index_candidates = self.index_candidates_for_where(
            db_path_str.as_ref(),
            &table_meta,
            &schema,
            &clause.where_clauses,
        )?;
        if let Some(rids) = index_candidates {
            for rid in rids {
                let record = self.record_manager.get(table_name, rid)?;
                let matches = match &prepared_where {
                    None => true,
                    Some(clauses) => self.evaluate_prepared_where(&record, &schema, clauses)?,
                };

                if matches {
                    let mut row = Vec::new();
                    for &idx in &col_indices {
                        let value = record.get(idx).unwrap();
                        row.push(self.format_value(value));
                    }

                    if let Some(order_idx) = order_by_idx {
                        let key = record.get(order_idx).unwrap().clone();
                        order_rows.push((key, row));
                    } else {
                        result_rows.push(row);
                    }
                }
            }
        } else {
            // Scan table using streaming iterator
            let scan_iter = self.record_manager.scan_iter(table_name)?;
            for item in scan_iter {
                let (_rid, record) = item?;
                // Evaluate WHERE clause
                let matches = match &prepared_where {
                    None => true,
                    Some(clauses) => self.evaluate_prepared_where(&record, &schema, clauses)?,
                };

                if matches {
                    // Project selected columns
                    let mut row = Vec::new();
                    for &idx in &col_indices {
                        let value = record.get(idx).unwrap();
                        row.push(self.format_value(value));
                    }

                    if let Some(order_idx) = order_by_idx {
                        let key = record.get(order_idx).unwrap().clone();
                        order_rows.push((key, row));
                    } else {
                        result_rows.push(row);
                    }
                }
            }
        }

        let mut result_rows = if let Some((_, asc)) = clause.order_by {
            let mut ordering_error = None;
            order_rows.sort_by(|(left_key, _), (right_key, _)| {
                match self.compare_order_values(left_key, right_key) {
                    Ok(ordering) => {
                        if asc {
                            ordering
                        } else {
                            ordering.reverse()
                        }
                    }
                    Err(err) => {
                        if ordering_error.is_none() {
                            ordering_error = Some(err);
                        }
                        Ordering::Equal
                    }
                }
            });
            if let Some(err) = ordering_error {
                return Err(err);
            }
            order_rows.into_iter().map(|(_, row)| row).collect()
        } else {
            result_rows
        };

        result_rows = self.apply_limit_offset(result_rows, clause.limit, clause.offset);

        Ok((selected_columns, result_rows))
    }

    fn select_two_table_join(
        &mut self,
        clause: SelectClause,
    ) -> DatabaseResult<(Vec<String>, Vec<Vec<String>>)> {
        if self.select_has_aggregate(&clause.selectors) || clause.group_by.is_some() {
            return Err(DatabaseError::TypeMismatch(
                "Aggregates are not supported with joins".to_string(),
            ));
        }

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

        let order_by_ref = clause
            .order_by
            .as_ref()
            .map(|(col, _)| {
                self.resolve_join_column_ref(
                    col,
                    left_name,
                    &left_schema,
                    right_name,
                    &right_schema,
                )
            })
            .transpose()?;
        let prepared_where = if clause.where_clauses.is_empty() {
            None
        } else {
            Some(self.prepare_where_clauses(&clause.where_clauses)?)
        };

        let mut result_rows = Vec::new();
        let mut order_rows = Vec::new();
        let index_candidates = self.index_candidates_for_where(
            &self.data_dir.join(db_name).to_string_lossy(),
            &left_meta,
            &left_schema,
            &clause.where_clauses,
        )?;
        if let Some(rids) = index_candidates {
            for rid in rids {
                let left_record = self.record_manager.get(left_name, rid)?;
                for right_record in &right_records {
                    let matches = match &prepared_where {
                        None => true,
                        Some(clauses) => self.evaluate_prepared_join_where(
                            &left_record,
                            &left_schema,
                            left_name,
                            right_record,
                            &right_schema,
                            right_name,
                            clauses,
                        )?,
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

                        if let Some(order_ref) = &order_by_ref {
                            let key = match order_ref.side {
                                JoinSide::Left => left_record.get(order_ref.index).unwrap(),
                                JoinSide::Right => right_record.get(order_ref.index).unwrap(),
                            }
                            .clone();
                            order_rows.push((key, row));
                        } else {
                            result_rows.push(row);
                        }
                    }
                }
            }
        } else {
            let scan_iter = self.record_manager.scan_iter(left_name)?;
            for item in scan_iter {
                let (_rid, left_record) = item?;
                for right_record in &right_records {
                    let matches = match &prepared_where {
                        None => true,
                        Some(clauses) => self.evaluate_prepared_join_where(
                            &left_record,
                            &left_schema,
                            left_name,
                            right_record,
                            &right_schema,
                            right_name,
                            clauses,
                        )?,
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

                        if let Some(order_ref) = &order_by_ref {
                            let key = match order_ref.side {
                                JoinSide::Left => left_record.get(order_ref.index).unwrap(),
                                JoinSide::Right => right_record.get(order_ref.index).unwrap(),
                            }
                            .clone();
                            order_rows.push((key, row));
                        } else {
                            result_rows.push(row);
                        }
                    }
                }
            }
        }

        let mut result_rows = if let Some((_, asc)) = clause.order_by {
            let mut ordering_error = None;
            order_rows.sort_by(|(left_key, _), (right_key, _)| {
                match self.compare_order_values(left_key, right_key) {
                    Ok(ordering) => {
                        if asc {
                            ordering
                        } else {
                            ordering.reverse()
                        }
                    }
                    Err(err) => {
                        if ordering_error.is_none() {
                            ordering_error = Some(err);
                        }
                        Ordering::Equal
                    }
                }
            });
            if let Some(err) = ordering_error {
                return Err(err);
            }
            order_rows.into_iter().map(|(_, row)| row).collect()
        } else {
            result_rows
        };

        result_rows = self.apply_limit_offset(result_rows, clause.limit, clause.offset);

        Ok((selected_columns, result_rows))
    }

    fn select_has_aggregate(&self, selectors: &Selectors) -> bool {
        match selectors {
            Selectors::All => false,
            Selectors::List(list) => list
                .iter()
                .any(|selector| !matches!(selector, Selector::Column(_))),
        }
    }

    fn select_single_table_aggregate(
        &mut self,
        clause: &SelectClause,
        schema: &TableSchema,
        table_name: &str,
    ) -> DatabaseResult<(Vec<String>, Vec<Vec<String>>)> {
        let selectors = match &clause.selectors {
            Selectors::All => {
                return Err(DatabaseError::TypeMismatch(
                    "SELECT * is not supported with aggregates".to_string(),
                ));
            }
            Selectors::List(list) => list,
        };

        let group_by_idx = match &clause.group_by {
            Some(tc) => Some(self.resolve_single_column_index(schema, tc)?),
            None => None,
        };

        let mut headers = Vec::new();
        let mut output_selectors = Vec::new();
        let mut agg_specs = Vec::new();

        for selector in selectors {
            match selector {
                Selector::Column(tc) => {
                    let col_idx = self.resolve_single_column_index(schema, tc)?;
                    match group_by_idx {
                        Some(group_idx) if group_idx == col_idx => {
                            headers.push(tc.column.clone());
                            output_selectors.push(OutputSelector::GroupKey);
                        }
                        Some(_) => {
                            return Err(DatabaseError::TypeMismatch(
                                "Non-aggregate column must match GROUP BY".to_string(),
                            ));
                        }
                        None => {
                            return Err(DatabaseError::TypeMismatch(
                                "Non-aggregate column requires GROUP BY".to_string(),
                            ));
                        }
                    }
                }
                Selector::CountAll => {
                    headers.push("COUNT(*)".to_string());
                    agg_specs.push(AggSpec::CountAll);
                    output_selectors.push(OutputSelector::Agg(agg_specs.len() - 1));
                }
                Selector::Count(tc) => {
                    let col_idx = self.resolve_single_column_index(schema, tc)?;
                    headers.push(format!("COUNT({})", self.format_table_column_name(tc)));
                    agg_specs.push(AggSpec::Count { col_idx });
                    output_selectors.push(OutputSelector::Agg(agg_specs.len() - 1));
                }
                Selector::Average(tc) => {
                    let col_idx = self.resolve_single_column_index(schema, tc)?;
                    self.ensure_numeric_column(schema, col_idx)?;
                    headers.push(format!("AVG({})", self.format_table_column_name(tc)));
                    agg_specs.push(AggSpec::Avg { col_idx });
                    output_selectors.push(OutputSelector::Agg(agg_specs.len() - 1));
                }
                Selector::Max(tc) => {
                    let col_idx = self.resolve_single_column_index(schema, tc)?;
                    headers.push(format!("MAX({})", self.format_table_column_name(tc)));
                    agg_specs.push(AggSpec::Max { col_idx });
                    output_selectors.push(OutputSelector::Agg(agg_specs.len() - 1));
                }
                Selector::Min(tc) => {
                    let col_idx = self.resolve_single_column_index(schema, tc)?;
                    headers.push(format!("MIN({})", self.format_table_column_name(tc)));
                    agg_specs.push(AggSpec::Min { col_idx });
                    output_selectors.push(OutputSelector::Agg(agg_specs.len() - 1));
                }
                Selector::Sum(tc) => {
                    let col_idx = self.resolve_single_column_index(schema, tc)?;
                    let numeric = self.numeric_type_for_column(schema, col_idx)?;
                    headers.push(format!("SUM({})", self.format_table_column_name(tc)));
                    agg_specs.push(AggSpec::Sum { col_idx, numeric });
                    output_selectors.push(OutputSelector::Agg(agg_specs.len() - 1));
                }
            }
        }

        let scan_iter = self.record_manager.scan_iter(table_name)?;
        let prepared_where = if clause.where_clauses.is_empty() {
            None
        } else {
            Some(self.prepare_where_clauses(&clause.where_clauses)?)
        };

        let mut group_states = Vec::new();
        let mut group_index: HashMap<GroupKey, usize> = HashMap::new();
        let mut agg_state = if group_by_idx.is_none() {
            Some(self.init_agg_states(&agg_specs))
        } else {
            None
        };

        for item in scan_iter {
            let (_rid, record) = item?;
            let matches = match &prepared_where {
                None => true,
                Some(clauses) => self.evaluate_prepared_where(&record, schema, clauses)?,
            };

            if !matches {
                continue;
            }

            if let Some(group_idx) = group_by_idx {
                let value = record
                    .get(group_idx)
                    .ok_or_else(|| {
                        DatabaseError::TypeMismatch("Invalid GROUP BY column".to_string())
                    })?
                    .clone();
                let key = self.group_key_from_value(&value);
                let entry_idx = match group_index.get(&key) {
                    Some(idx) => *idx,
                    None => {
                        let idx = group_states.len();
                        group_states.push(GroupState {
                            key: value,
                            aggs: self.init_agg_states(&agg_specs),
                        });
                        group_index.insert(key, idx);
                        idx
                    }
                };
                let state = &mut group_states[entry_idx].aggs;
                self.update_agg_states(state, &agg_specs, &record)?;
            } else if let Some(state) = agg_state.as_mut() {
                self.update_agg_states(state, &agg_specs, &record)?;
            }
        }

        let mut rows = Vec::new();
        if let Some(state) = agg_state {
            rows.push(self.build_aggregate_row(None, &output_selectors, &agg_specs, &state)?);
        } else {
            for group in &group_states {
                rows.push(self.build_aggregate_row(
                    Some(&group.key),
                    &output_selectors,
                    &agg_specs,
                    &group.aggs,
                )?);
            }
        }

        let rows = self.apply_limit_offset(rows, clause.limit, clause.offset);

        Ok((headers, rows))
    }

    fn format_table_column_name(&self, column: &TableColumn) -> String {
        match &column.table {
            Some(table) => format!("{}.{}", table, column.column),
            None => column.column.clone(),
        }
    }

    fn table_column_matches(&self, table_name: &str, column: &TableColumn) -> bool {
        match &column.table {
            Some(table) => table == table_name,
            None => true,
        }
    }

    fn composite_key_from_i64(left: i64, right: i64) -> i64 {
        let left = left as i32;
        let right = right as i32;
        TableCompositeIntColumnIter::composite_key(left, right)
    }

    fn index_storage_name(columns: &[String]) -> Option<String> {
        match columns.len() {
            1 => Some(columns[0].clone()),
            2 => Some(format!("{}__{}", columns[0], columns[1])),
            _ => None,
        }
    }

    fn build_index_defs(&self, table_meta: &TableMetadata) -> DatabaseResult<Vec<IndexDef>> {
        let mut defs = Vec::new();
        let mut seen = HashSet::new();

        let mut index_sets: Vec<Vec<String>> = Vec::new();
        if let Some(pk_cols) = &table_meta.primary_key {
            index_sets.push(pk_cols.clone());
        }
        for index_meta in &table_meta.indexes {
            index_sets.push(index_meta.columns.clone());
        }

        for columns in index_sets {
            let storage_name = match Self::index_storage_name(&columns) {
                Some(name) => name,
                None => continue,
            };
            if !seen.insert(storage_name.clone()) {
                continue;
            }
            let mut indices = Vec::with_capacity(columns.len());
            let mut valid = true;
            for col_name in &columns {
                let idx = match table_meta
                    .columns
                    .iter()
                    .position(|c| &c.name == col_name)
                {
                    Some(idx) => idx,
                    None => {
                        return Err(DatabaseError::ColumnNotFound(
                            col_name.clone(),
                            table_meta.name.clone(),
                        ))
                    }
                };
                if table_meta.columns[idx].to_data_type() != DataType::Int {
                    valid = false;
                    break;
                }
                indices.push(idx);
            }
            if !valid {
                continue;
            }
            defs.push(IndexDef {
                columns,
                indices,
                storage_name,
            });
        }

        Ok(defs)
    }

    fn rebuild_index_for_columns(
        &mut self,
        db_path: &str,
        table_meta: &TableMetadata,
        schema: &TableSchema,
        columns: &[String],
    ) -> DatabaseResult<()> {
        let storage_name = match Self::index_storage_name(columns) {
            Some(name) => name,
            None => {
                return Err(DatabaseError::TypeMismatch(
                    "Composite index requires exactly two columns".to_string(),
                ))
            }
        };
        if columns.len() != 1 && columns.len() != 2 {
            return Err(DatabaseError::TypeMismatch(
                "Only one- or two-column indexes are supported".to_string(),
            ));
        }

        let mut col_indices = Vec::with_capacity(columns.len());
        for col_name in columns {
            let col_idx = table_meta
                .columns
                .iter()
                .position(|c| &c.name == col_name)
                .ok_or_else(|| {
                    DatabaseError::ColumnNotFound(col_name.clone(), table_meta.name.clone())
                })?;
            if table_meta.columns[col_idx].to_data_type() != DataType::Int {
                return Err(DatabaseError::TypeMismatch(
                    "Only INT columns can be indexed".to_string(),
                ));
            }
            col_indices.push(col_idx);
        }

        let table_path = PathBuf::from(db_path).join(format!("{}.tbl", table_meta.name));
        let _ = self
            .record_manager
            .open_table(table_path.to_string_lossy().as_ref(), schema.clone());

        let _ = self
            .index_manager
            .drop_index(db_path, &table_meta.name, &storage_name);

        let scan_iter = self.record_manager.scan_iter(&table_meta.name)?;
        if col_indices.len() == 1 {
            let table_iter = TableIntColumnIter::new(scan_iter, col_indices[0]);
            self.index_manager.create_index_from_table(
                db_path,
                &table_meta.name,
                &storage_name,
                table_iter,
            )?;
        } else {
            let table_iter =
                TableCompositeIntColumnIter::new(scan_iter, col_indices[0], col_indices[1]);
            self.index_manager.create_index_from_table(
                db_path,
                &table_meta.name,
                &storage_name,
                table_iter,
            )?;
        }

        Ok(())
    }

    fn ensure_index_open_for_columns(
        &mut self,
        db_path: &str,
        table_meta: &TableMetadata,
        schema: &TableSchema,
        columns: &[String],
    ) -> DatabaseResult<bool> {
        let storage_name = match Self::index_storage_name(columns) {
            Some(name) => name,
            None => return Ok(false),
        };
        match self
            .index_manager
            .open_index(db_path, &table_meta.name, &storage_name)
        {
            Ok(()) => Ok(true),
            Err(IndexError::IndexNotFound(_))
            | Err(IndexError::InvalidMagic)
            | Err(IndexError::UnsupportedVersion(_))
            | Err(IndexError::CorruptedNode(_)) => {
                self.rebuild_index_for_columns(db_path, table_meta, schema, columns)?;
                Ok(true)
            }
            Err(err) => Err(DatabaseError::IndexError(err)),
        }
    }

    fn index_candidates_for_where(
        &mut self,
        db_path: &str,
        table_meta: &TableMetadata,
        schema: &TableSchema,
        where_clauses: &[WhereClause],
    ) -> DatabaseResult<Option<Vec<RecordId>>> {
        if !self.use_indexes || where_clauses.is_empty() {
            return Ok(None);
        }

        let table_name = &table_meta.name;
        let mut eq_values: HashMap<String, i64> = HashMap::new();
        for clause in where_clauses {
            if let WhereClause::Op(col, Operator::Eq, Expression::Value(ParserValue::Integer(value))) =
                clause
            {
                if !self.table_column_matches(table_name, col) {
                    continue;
                }
                eq_values.insert(col.column.clone(), *value);
            }
        }

        let mut composite_defs = Vec::new();
        if let Some(pk_cols) = &table_meta.primary_key {
            if pk_cols.len() == 2 {
                composite_defs.push(pk_cols.clone());
            }
        }
        for index_meta in &table_meta.indexes {
            if index_meta.columns.len() == 2 {
                composite_defs.push(index_meta.columns.clone());
            }
        }

        for columns in &composite_defs {
            let left_val = match eq_values.get(&columns[0]) {
                Some(val) => *val,
                None => continue,
            };
            let right_val = match eq_values.get(&columns[1]) {
                Some(val) => *val,
                None => continue,
            };
            if !self.ensure_index_open_for_columns(db_path, table_meta, schema, columns)? {
                continue;
            }
            let storage_name = match Self::index_storage_name(columns) {
                Some(name) => name,
                None => continue,
            };
            let key = Self::composite_key_from_i64(left_val, right_val);
            let mut rids = self
                .index_manager
                .search_all(table_name, &storage_name, key);
            rids.sort_by_key(|rid| (rid.page_id, rid.slot_id));
            return Ok(Some(rids));
        }

        for columns in &composite_defs {
            let mut lower = None;
            let mut upper = None;
            for clause in where_clauses {
                if let WhereClause::Op(
                    col,
                    op,
                    Expression::Value(ParserValue::Integer(value)),
                ) = clause
                {
                    if col.column != columns[0] || !self.table_column_matches(table_name, col) {
                        continue;
                    }
                    match op {
                        Operator::Eq => {
                            lower = Some(*value);
                            upper = Some(*value);
                        }
                        Operator::Gt => {
                            let bound = value.saturating_add(1);
                            lower = Some(lower.map_or(bound, |v| v.max(bound)));
                        }
                        Operator::Ge => {
                            lower = Some(lower.map_or(*value, |v| v.max(*value)));
                        }
                        Operator::Lt => {
                            let bound = value.saturating_sub(1);
                            upper = Some(upper.map_or(bound, |v| v.min(bound)));
                        }
                        Operator::Le => {
                            upper = Some(upper.map_or(*value, |v| v.min(*value)));
                        }
                        Operator::Ne => {}
                    }
                }
            }

            if lower.is_none() && upper.is_none() {
                continue;
            }

            if !self.ensure_index_open_for_columns(db_path, table_meta, schema, columns)? {
                continue;
            }
            let storage_name = match Self::index_storage_name(columns) {
                Some(name) => name,
                None => continue,
            };

            let left_min = lower.unwrap_or(0).clamp(i64::from(i32::MIN), i64::from(i32::MAX));
            let left_max = upper
                .unwrap_or(i64::from(i32::MAX))
                .clamp(i64::from(i32::MIN), i64::from(i32::MAX));
            if left_min > left_max {
                return Ok(Some(Vec::new()));
            }
            let lower_key = TableCompositeIntColumnIter::composite_key(left_min as i32, 0);
            let upper_key = TableCompositeIntColumnIter::composite_key(left_max as i32, -1);
            let mut rids = self
                .index_manager
                .range_search(table_name, &storage_name, lower_key, upper_key)
                .into_iter()
                .map(|(_key, rid)| rid)
                .collect::<Vec<_>>();
            rids.sort_by_key(|rid| (rid.page_id, rid.slot_id));
            return Ok(Some(rids));
        }

        for clause in where_clauses {
            match clause {
                WhereClause::Op(col, op, Expression::Value(ParserValue::Integer(value))) => {
                    if !self.table_column_matches(table_name, col) {
                        continue;
                    }
                    let col_idx = self.resolve_single_column_index(schema, col)?;
                    if schema.columns[col_idx].data_type != DataType::Int {
                        continue;
                    }
                    if !self.ensure_index_open_for_columns(
                        db_path,
                        table_meta,
                        schema,
                        &vec![col.column.clone()],
                    )? {
                        continue;
                    }

                    let mut rids = match op {
                        Operator::Eq => self
                            .index_manager
                            .search_all(table_name, &col.column, *value),
                        Operator::Gt | Operator::Ge | Operator::Lt | Operator::Le => {
                            let (lower, upper) = match op {
                                Operator::Gt => (value.saturating_add(1), i64::MAX),
                                Operator::Ge => (*value, i64::MAX),
                                Operator::Lt => (i64::MIN, value.saturating_sub(1)),
                                Operator::Le => (i64::MIN, *value),
                                _ => (i64::MIN, i64::MAX),
                            };
                            self.index_manager
                                .range_search(table_name, &col.column, lower, upper)
                                .into_iter()
                                .map(|(_key, rid)| rid)
                                .collect()
                        }
                        _ => continue,
                    };

                    rids.sort_by_key(|rid| (rid.page_id, rid.slot_id));
                    return Ok(Some(rids));
                }
                WhereClause::In(col, values) => {
                    if !self.table_column_matches(table_name, col) {
                        continue;
                    }
                    let col_idx = self.resolve_single_column_index(schema, col)?;
                    if schema.columns[col_idx].data_type != DataType::Int {
                        continue;
                    }
                    if !self.ensure_index_open_for_columns(
                        db_path,
                        table_meta,
                        schema,
                        &vec![col.column.clone()],
                    )? {
                        continue;
                    }

                    let mut rids = Vec::new();
                    for value in values {
                        if let ParserValue::Integer(int_val) = value {
                            rids.extend(
                                self.index_manager
                                    .search_all(table_name, &col.column, *int_val),
                            );
                        }
                    }
                    if rids.is_empty() {
                        return Ok(Some(Vec::new()));
                    }
                    rids.sort_by_key(|rid| (rid.page_id, rid.slot_id));
                    return Ok(Some(rids));
                }
                _ => continue,
            }
        }

        Ok(None)
    }

    fn ensure_numeric_column(&self, schema: &TableSchema, col_idx: usize) -> DatabaseResult<()> {
        match schema.columns[col_idx].data_type {
            DataType::Int | DataType::Float => Ok(()),
            _ => Err(DatabaseError::TypeMismatch(
                "Aggregate requires numeric column".to_string(),
            )),
        }
    }

    fn numeric_type_for_column(
        &self,
        schema: &TableSchema,
        col_idx: usize,
    ) -> DatabaseResult<NumericType> {
        match schema.columns[col_idx].data_type {
            DataType::Int => Ok(NumericType::Int),
            DataType::Float => Ok(NumericType::Float),
            _ => Err(DatabaseError::TypeMismatch(
                "Aggregate requires numeric column".to_string(),
            )),
        }
    }

    fn init_agg_states(&self, specs: &[AggSpec]) -> Vec<AggState> {
        specs
            .iter()
            .map(|spec| match spec {
                AggSpec::CountAll | AggSpec::Count { .. } => AggState::Count(0),
                AggSpec::Sum {
                    numeric: NumericType::Int,
                    ..
                } => AggState::SumInt {
                    sum: 0,
                    has_value: false,
                },
                AggSpec::Sum {
                    numeric: NumericType::Float,
                    ..
                } => AggState::SumFloat {
                    sum: 0.0,
                    has_value: false,
                },
                AggSpec::Avg { .. } => AggState::Avg { sum: 0.0, count: 0 },
                AggSpec::Min { .. } => AggState::Min(None),
                AggSpec::Max { .. } => AggState::Max(None),
            })
            .collect()
    }

    fn update_agg_states(
        &self,
        states: &mut [AggState],
        specs: &[AggSpec],
        record: &Record,
    ) -> DatabaseResult<()> {
        for (state, spec) in states.iter_mut().zip(specs.iter()) {
            self.update_agg_state(state, spec, record)?;
        }
        Ok(())
    }

    fn update_agg_state(
        &self,
        state: &mut AggState,
        spec: &AggSpec,
        record: &Record,
    ) -> DatabaseResult<()> {
        match spec {
            AggSpec::CountAll => {
                if let AggState::Count(count) = state {
                    *count += 1;
                }
            }
            AggSpec::Count { col_idx } => {
                let value = record.get(*col_idx).ok_or_else(|| {
                    DatabaseError::TypeMismatch("Invalid COUNT column".to_string())
                })?;
                if !matches!(value, RecordValue::Null)
                    && let AggState::Count(count) = state
                {
                    *count += 1;
                }
            }
            AggSpec::Sum { col_idx, numeric } => {
                let value = record
                    .get(*col_idx)
                    .ok_or_else(|| DatabaseError::TypeMismatch("Invalid SUM column".to_string()))?;
                match (numeric, value, state) {
                    (
                        NumericType::Int,
                        RecordValue::Int(v),
                        AggState::SumInt { sum, has_value },
                    ) => {
                        *sum += *v as i64;
                        *has_value = true;
                    }
                    (
                        NumericType::Float,
                        RecordValue::Float(v),
                        AggState::SumFloat { sum, has_value },
                    ) => {
                        *sum += *v;
                        *has_value = true;
                    }
                    (
                        NumericType::Float,
                        RecordValue::Int(v),
                        AggState::SumFloat { sum, has_value },
                    ) => {
                        *sum += *v as f64;
                        *has_value = true;
                    }
                    (_, RecordValue::Null, _) => {}
                    _ => {
                        return Err(DatabaseError::TypeMismatch(
                            "SUM requires numeric column".to_string(),
                        ));
                    }
                }
            }
            AggSpec::Avg { col_idx } => {
                let value = record
                    .get(*col_idx)
                    .ok_or_else(|| DatabaseError::TypeMismatch("Invalid AVG column".to_string()))?;
                if let AggState::Avg { sum, count } = state {
                    match value {
                        RecordValue::Int(v) => {
                            *sum += *v as f64;
                            *count += 1;
                        }
                        RecordValue::Float(v) => {
                            *sum += *v;
                            *count += 1;
                        }
                        RecordValue::Null => {}
                        _ => {
                            return Err(DatabaseError::TypeMismatch(
                                "AVG requires numeric column".to_string(),
                            ));
                        }
                    }
                }
            }
            AggSpec::Min { col_idx } => {
                let value = record
                    .get(*col_idx)
                    .ok_or_else(|| DatabaseError::TypeMismatch("Invalid MIN column".to_string()))?;
                if matches!(value, RecordValue::Null) {
                    return Ok(());
                }
                if let AggState::Min(current) = state {
                    match current {
                        None => *current = Some(value.clone()),
                        Some(existing) => {
                            if self.compare_record_values(value, existing)? == Ordering::Less {
                                *current = Some(value.clone());
                            }
                        }
                    }
                }
            }
            AggSpec::Max { col_idx } => {
                let value = record
                    .get(*col_idx)
                    .ok_or_else(|| DatabaseError::TypeMismatch("Invalid MAX column".to_string()))?;
                if matches!(value, RecordValue::Null) {
                    return Ok(());
                }
                if let AggState::Max(current) = state {
                    match current {
                        None => *current = Some(value.clone()),
                        Some(existing) => {
                            if self.compare_record_values(value, existing)? == Ordering::Greater {
                                *current = Some(value.clone());
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn build_aggregate_row(
        &self,
        group_key: Option<&RecordValue>,
        output_selectors: &[OutputSelector],
        agg_specs: &[AggSpec],
        aggs: &[AggState],
    ) -> DatabaseResult<Vec<String>> {
        let mut row = Vec::with_capacity(output_selectors.len());
        for selector in output_selectors {
            match selector {
                OutputSelector::GroupKey => {
                    let value = group_key.ok_or_else(|| {
                        DatabaseError::TypeMismatch("Missing GROUP BY value".to_string())
                    })?;
                    row.push(self.format_value(value));
                }
                OutputSelector::Agg(idx) => {
                    let state = aggs.get(*idx).ok_or_else(|| {
                        DatabaseError::TypeMismatch("Invalid aggregate selector".to_string())
                    })?;
                    let spec = agg_specs.get(*idx).ok_or_else(|| {
                        DatabaseError::TypeMismatch("Invalid aggregate selector".to_string())
                    })?;
                    row.push(self.format_aggregate_value(state, spec));
                }
            }
        }
        Ok(row)
    }

    fn format_aggregate_value(&self, state: &AggState, spec: &AggSpec) -> String {
        match (spec, state) {
            (AggSpec::CountAll, AggState::Count(count))
            | (AggSpec::Count { .. }, AggState::Count(count)) => count.to_string(),
            (
                AggSpec::Sum {
                    numeric: NumericType::Int,
                    ..
                },
                AggState::SumInt { sum, has_value },
            ) => {
                if *has_value {
                    sum.to_string()
                } else {
                    "NULL".to_string()
                }
            }
            (
                AggSpec::Sum {
                    numeric: NumericType::Float,
                    ..
                },
                AggState::SumFloat { sum, has_value },
            ) => {
                if *has_value {
                    format!("{:.2}", sum)
                } else {
                    "NULL".to_string()
                }
            }
            (AggSpec::Avg { .. }, AggState::Avg { sum, count }) => {
                if *count > 0 {
                    format!("{:.2}", sum / *count as f64)
                } else {
                    "NULL".to_string()
                }
            }
            (AggSpec::Min { .. }, AggState::Min(value))
            | (AggSpec::Max { .. }, AggState::Max(value)) => value
                .as_ref()
                .map_or_else(|| "NULL".to_string(), |v| self.format_value(v)),
            _ => "NULL".to_string(),
        }
    }

    fn group_key_from_value(&self, value: &RecordValue) -> GroupKey {
        match value {
            RecordValue::Int(v) => GroupKey::Int(*v),
            RecordValue::Float(v) => GroupKey::Float(v.to_bits()),
            RecordValue::String(s) => GroupKey::String(s.clone()),
            RecordValue::Null => GroupKey::Null,
        }
    }

    fn compare_record_values(
        &self,
        left: &RecordValue,
        right: &RecordValue,
    ) -> DatabaseResult<Ordering> {
        match (left, right) {
            (RecordValue::Int(l), RecordValue::Int(r)) => Ok(l.cmp(r)),
            (RecordValue::Float(l), RecordValue::Float(r)) => l
                .partial_cmp(r)
                .ok_or_else(|| DatabaseError::TypeMismatch("Invalid float comparison".to_string())),
            (RecordValue::String(l), RecordValue::String(r)) => Ok(l.cmp(r)),
            _ => Err(DatabaseError::TypeMismatch(
                "Aggregate comparison type mismatch".to_string(),
            )),
        }
    }

    fn compare_order_values(
        &self,
        left: &RecordValue,
        right: &RecordValue,
    ) -> DatabaseResult<Ordering> {
        match (left, right) {
            (RecordValue::Null, RecordValue::Null) => Ok(Ordering::Equal),
            (RecordValue::Null, _) => Ok(Ordering::Less),
            (_, RecordValue::Null) => Ok(Ordering::Greater),
            (RecordValue::Int(l), RecordValue::Int(r)) => Ok(l.cmp(r)),
            (RecordValue::Float(l), RecordValue::Float(r)) => l
                .partial_cmp(r)
                .ok_or_else(|| DatabaseError::TypeMismatch("Invalid float comparison".to_string())),
            (RecordValue::String(l), RecordValue::String(r)) => Ok(l.cmp(r)),
            _ => Err(DatabaseError::TypeMismatch(
                "ORDER BY comparison type mismatch".to_string(),
            )),
        }
    }

    fn apply_limit_offset(
        &self,
        rows: Vec<Vec<String>>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Vec<Vec<String>> {
        let start = offset.unwrap_or(0);
        if start >= rows.len() {
            return Vec::new();
        }

        let iter = rows.into_iter().skip(start);
        match limit {
            Some(count) => iter.take(count).collect(),
            None => iter.collect(),
        }
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

        // Step 1: Collect all index definitions before dropping
        let index_defs = self.build_index_defs(&table_meta)?;

        // Step 2: Drop all indexes (including primary key indexes)
        for def in &index_defs {
            let _ = self
                .index_manager
                .drop_index(&db_path_str, table, &def.storage_name);
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
                        self.bulk_insert(table, std::mem::take(&mut batch_rows), true, true, true)?;
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
        for def in &index_defs {
            let table_file = {
                let mut buffer_manager = self.buffer_manager.lock().unwrap();
                TableFile::open(&mut buffer_manager, &table_path_str, schema.clone())?
            };
            let scan_iter = table_file.scan_iter(self.buffer_manager.clone());

            match def.indices.as_slice() {
                [col_idx] => {
                    let table_data = TableIntColumnIter::new(scan_iter, *col_idx);
                    self.index_manager.create_index_from_table(
                        &db_path_str,
                        table,
                        &def.storage_name,
                        table_data,
                    )?;
                }
                [left_idx, right_idx] => {
                    let table_data =
                        TableCompositeIntColumnIter::new(scan_iter, *left_idx, *right_idx);
                    self.index_manager.create_index_from_table(
                        &db_path_str,
                        table,
                        &def.storage_name,
                        table_data,
                    )?;
                }
                _ => continue,
            }

            // Flush and close the index immediately to free memory before building the next one.
            self.index_manager.close_index(table, &def.storage_name)?;

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

    fn implicit_fk_index_name(&self, table: &str, columns: &[String]) -> String {
        match columns.len() {
            1 => format!("__fk_idx_{}_{}", table, columns[0]),
            2 => format!("__fk_idx_{}_{}__{}", table, columns[0], columns[1]),
            _ => format!("__fk_idx_{}_multi", table),
        }
    }

    fn ensure_foreign_key_indexes(&mut self) -> DatabaseResult<bool> {
        let db_name = self
            .current_db
            .as_ref()
            .ok_or(DatabaseError::NoDatabaseSelected)?
            .clone();
        let metadata = self
            .current_metadata
            .as_ref()
            .ok_or(DatabaseError::NoDatabaseSelected)?;

        let mut required_single: HashMap<String, HashSet<String>> = HashMap::new();
        let mut required_composite: HashMap<String, Vec<Vec<String>>> = HashMap::new();
        let mut required_composite_seen: HashMap<String, HashSet<String>> = HashMap::new();
        for (table_name, table_meta) in &metadata.tables {
            for fk in &table_meta.foreign_keys {
                if fk.columns.len() == 1 {
                    required_single
                        .entry(table_name.clone())
                        .or_default()
                        .insert(fk.columns[0].clone());
                }
                if fk.ref_columns.len() == 1 {
                    required_single
                        .entry(fk.ref_table.clone())
                        .or_default()
                        .insert(fk.ref_columns[0].clone());
                }
                if fk.columns.len() == 2 {
                    if let Some(name) = Self::index_storage_name(&fk.columns) {
                        let seen = required_composite_seen
                            .entry(table_name.clone())
                            .or_default();
                        if seen.insert(name) {
                            required_composite
                                .entry(table_name.clone())
                                .or_default()
                                .push(fk.columns.clone());
                        }
                    }
                }
                if fk.ref_columns.len() == 2 {
                    if let Some(name) = Self::index_storage_name(&fk.ref_columns) {
                        let seen = required_composite_seen
                            .entry(fk.ref_table.clone())
                            .or_default();
                        if seen.insert(name) {
                            required_composite
                                .entry(fk.ref_table.clone())
                                .or_default()
                                .push(fk.ref_columns.clone());
                        }
                    }
                }
            }
        }
        let _ = metadata;

        let table_names: Vec<String> = {
            let metadata = self
                .current_metadata
                .as_ref()
                .ok_or(DatabaseError::NoDatabaseSelected)?;
            metadata.tables.keys().cloned().collect()
        };

        let mut updated = false;
        for table_name in table_names {
            let mut table_meta = {
                let metadata = self
                    .current_metadata
                    .as_mut()
                    .ok_or(DatabaseError::NoDatabaseSelected)?;
                metadata
                    .tables
                    .remove(&table_name)
                    .ok_or_else(|| DatabaseError::TableNotFound(table_name.clone()))?
            };

            let required_cols = required_single
                .get(&table_name)
                .cloned()
                .unwrap_or_default();
            let required_comp = required_composite
                .get(&table_name)
                .cloned()
                .unwrap_or_default();
            let required_comp_names: HashSet<String> = required_comp
                .iter()
                .filter_map(|cols| Self::index_storage_name(cols))
                .collect();

            let mut columns_to_drop = Vec::new();
            table_meta.indexes.retain(|idx| {
                if !idx.implicit {
                    return true;
                }
                match idx.columns.len() {
                    1 => {
                        if !required_cols.contains(&idx.columns[0]) {
                            columns_to_drop.push(idx.columns.clone());
                            updated = true;
                            false
                        } else {
                            true
                        }
                    }
                    2 => {
                        let name = match Self::index_storage_name(&idx.columns) {
                            Some(name) => name,
                            None => return true,
                        };
                        if !required_comp_names.contains(&name) {
                            columns_to_drop.push(idx.columns.clone());
                            updated = true;
                            false
                        } else {
                            true
                        }
                    }
                    _ => true,
                }
            });

            let mut to_create = Vec::new();
            let mut to_create_composite = Vec::new();
            for col_name in &required_cols {
                if table_meta
                    .indexes
                    .iter()
                    .any(|idx| idx.columns.len() == 1 && idx.columns[0] == *col_name)
                {
                    continue;
                }
                let col_idx = table_meta
                    .columns
                    .iter()
                    .position(|c| &c.name == col_name)
                    .ok_or_else(|| {
                        DatabaseError::ColumnNotFound(col_name.clone(), table_meta.name.clone())
                    })?;
                if table_meta.columns[col_idx].to_data_type() != DataType::Int {
                    continue;
                }
                to_create.push((col_name.clone(), col_idx));
            }
            for columns in &required_comp {
                if table_meta
                    .indexes
                    .iter()
                    .any(|idx| idx.columns.len() == 2 && idx.columns == *columns)
                {
                    continue;
                }
                let left_idx = table_meta
                    .columns
                    .iter()
                    .position(|c| &c.name == &columns[0])
                    .ok_or_else(|| {
                        DatabaseError::ColumnNotFound(
                            columns[0].clone(),
                            table_meta.name.clone(),
                        )
                    })?;
                let right_idx = table_meta
                    .columns
                    .iter()
                    .position(|c| &c.name == &columns[1])
                    .ok_or_else(|| {
                        DatabaseError::ColumnNotFound(
                            columns[1].clone(),
                            table_meta.name.clone(),
                        )
                    })?;
                if table_meta.columns[left_idx].to_data_type() != DataType::Int
                    || table_meta.columns[right_idx].to_data_type() != DataType::Int
                {
                    continue;
                }
                to_create_composite.push((columns.clone(), left_idx, right_idx));
            }

            let db_path = self.data_dir.join(&db_name);
            let db_path_str = db_path.to_string_lossy().to_string();
            let schema = self.metadata_to_schema(&table_meta);

            if !to_create.is_empty() || !to_create_composite.is_empty() {
                let table_path = self.table_path(&db_name, &table_meta.name);
                self.record_manager
                    .open_table(&table_path.to_string_lossy(), schema)?;
            }

            for (col_name, col_idx) in to_create {
                match self
                    .index_manager
                    .open_index(&db_path_str, &table_meta.name, &col_name)
                {
                    Ok(()) => {}
                    Err(IndexError::IndexNotFound(_)) => {
                        let scan_iter = self.record_manager.scan_iter(&table_meta.name)?;
                        let table_iter = TableIntColumnIter::new(scan_iter, col_idx);
                        self.index_manager.create_index_from_table(
                            &db_path_str,
                            &table_meta.name,
                            &col_name,
                            table_iter,
                        )?;
                    }
                    Err(err) => return Err(DatabaseError::IndexError(err)),
                }

                table_meta.indexes.push(IndexMetadata {
                    name: self.implicit_fk_index_name(&table_meta.name, &vec![col_name.clone()]),
                    columns: vec![col_name],
                    implicit: true,
                });
                updated = true;
            }

            for (columns, left_idx, right_idx) in to_create_composite {
                let storage_name = match Self::index_storage_name(&columns) {
                    Some(name) => name,
                    None => continue,
                };
                match self
                    .index_manager
                    .open_index(&db_path_str, &table_meta.name, &storage_name)
                {
                    Ok(()) => {}
                    Err(IndexError::IndexNotFound(_)) => {
                        let scan_iter = self.record_manager.scan_iter(&table_meta.name)?;
                        let table_iter =
                            TableCompositeIntColumnIter::new(scan_iter, left_idx, right_idx);
                        self.index_manager.create_index_from_table(
                            &db_path_str,
                            &table_meta.name,
                            &storage_name,
                            table_iter,
                        )?;
                    }
                    Err(err) => return Err(DatabaseError::IndexError(err)),
                }

                table_meta.indexes.push(IndexMetadata {
                    name: self.implicit_fk_index_name(&table_meta.name, &columns),
                    columns,
                    implicit: true,
                });
                updated = true;
            }

            for columns in columns_to_drop {
                let storage_name = match Self::index_storage_name(&columns) {
                    Some(name) => name,
                    None => continue,
                };
                if let Err(err) = self.index_manager.drop_index(
                    &db_path_str,
                    &table_meta.name,
                    &storage_name,
                ) {
                    if !matches!(err, crate::index::IndexError::IndexNotFound(_)) {
                        return Err(DatabaseError::IndexError(err));
                    }
                }
            }

            {
                let metadata = self
                    .current_metadata
                    .as_mut()
                    .ok_or(DatabaseError::NoDatabaseSelected)?;
                metadata.tables.insert(table_name, table_meta);
            }
        }

        Ok(updated)
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
                    table_meta.name, fk.columns[pos], fk.ref_table, fk.ref_columns[pos]
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
            if let Some(indices) = update_indices
                && !fk.column_indices.iter().any(|idx| indices.contains(idx))
            {
                continue;
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

            if self.use_indexes && fk.column_indices.len() == 1 && fk.ref_column_indices.len() == 1
            {
                let fk_value = match values[0] {
                    RecordValue::Int(v) => v as i64,
                    _ => {
                        return Err(DatabaseError::TypeMismatch(
                            "Foreign key column must be INT".to_string(),
                        ));
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
            if self.use_indexes && fk.column_indices.len() == 2 && fk.ref_column_indices.len() == 2
            {
                let (left, right) = match (&values[0], &values[1]) {
                    (RecordValue::Int(l), RecordValue::Int(r)) => (*l, *r),
                    _ => {
                        return Err(DatabaseError::TypeMismatch(
                            "Foreign key column must be INT".to_string(),
                        ));
                    }
                };
                let storage_name = match Self::index_storage_name(&fk.ref_column_names) {
                    Some(name) => name,
                    None => {
                        return Err(DatabaseError::TypeMismatch(
                            "Composite index requires exactly two columns".to_string(),
                        ));
                    }
                };
                let mut used_index = false;
                let mut index_found = false;
                match self.index_manager.open_index(
                    &db_path_str,
                    &fk.ref_table,
                    &storage_name,
                ) {
                    Ok(()) => {
                        used_index = true;
                        let key = TableCompositeIntColumnIter::composite_key(left, right);
                        index_found = self
                            .index_manager
                            .search(&fk.ref_table, &storage_name, key)
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
                            DatabaseError::ColumnNotFound(col_name.clone(), child_meta.name.clone())
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
                            child_meta.name, fk.columns[pos], parent_meta.name, fk.ref_columns[pos]
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

            if self.use_indexes
                && fk.child_column_indices.len() == 1
                && fk.parent_column_indices.len() == 1
            {
                let fk_value = match values[0] {
                    RecordValue::Int(v) => v as i64,
                    _ => {
                        return Err(DatabaseError::TypeMismatch(
                            "Foreign key column must be INT".to_string(),
                        ));
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
            if self.use_indexes
                && fk.child_column_indices.len() == 2
                && fk.parent_column_indices.len() == 2
            {
                let (left, right) = match (&values[0], &values[1]) {
                    (RecordValue::Int(l), RecordValue::Int(r)) => (*l, *r),
                    _ => {
                        return Err(DatabaseError::TypeMismatch(
                            "Foreign key column must be INT".to_string(),
                        ));
                    }
                };
                let storage_name = match Self::index_storage_name(&fk.child_column_names) {
                    Some(name) => name,
                    None => {
                        return Err(DatabaseError::TypeMismatch(
                            "Composite index requires exactly two columns".to_string(),
                        ));
                    }
                };

                let mut used_index = false;
                let mut index_found = false;
                match self.index_manager.open_index(
                    db_path_str,
                    &fk.child_table,
                    &storage_name,
                ) {
                    Ok(()) => {
                        used_index = true;
                        let key = TableCompositeIntColumnIter::composite_key(left, right);
                        index_found = self
                            .index_manager
                            .search(&fk.child_table, &storage_name, key)
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

    fn open_indexed_defs(
        &mut self,
        db_path: &str,
        table_meta: &TableMetadata,
    ) -> DatabaseResult<Vec<IndexDef>> {
        let defs = self.build_index_defs(table_meta)?;
        let mut opened = Vec::new();
        for def in defs {
            match self
                .index_manager
                .open_index(db_path, &table_meta.name, &def.storage_name)
            {
                Ok(()) => opened.push(def),
                Err(IndexError::IndexNotFound(_)) => {}
                Err(err) => return Err(DatabaseError::IndexError(err)),
            }
        }
        Ok(opened)
    }

    fn resolve_single_column_index(
        &self,
        schema: &TableSchema,
        column: &TableColumn,
    ) -> DatabaseResult<usize> {
        if let Some(table) = &column.table
            && table != &schema.table_name
        {
            return Err(DatabaseError::ColumnNotFound(
                column.column.clone(),
                table.clone(),
            ));
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

    fn prepare_where_clauses(
        &self,
        where_clauses: &[WhereClause],
    ) -> DatabaseResult<Vec<PreparedWhereClause>> {
        let mut prepared = Vec::with_capacity(where_clauses.len());
        for clause in where_clauses {
            match clause {
                WhereClause::Op(col, op, expr) => {
                    prepared.push(PreparedWhereClause::Op(
                        col.clone(),
                        op.clone(),
                        expr.clone(),
                    ));
                }
                WhereClause::Null(col) => {
                    prepared.push(PreparedWhereClause::Null(col.clone()));
                }
                WhereClause::NotNull(col) => {
                    prepared.push(PreparedWhereClause::NotNull(col.clone()));
                }
                WhereClause::Like(col, pattern) => {
                    let regex =
                        Regex::new(&self.like_pattern_to_regex(pattern)).map_err(|err| {
                            DatabaseError::TypeMismatch(format!("Invalid LIKE pattern: {}", err))
                        })?;
                    prepared.push(PreparedWhereClause::Like(col.clone(), regex));
                }
                _ => {
                    return Err(DatabaseError::TypeMismatch(
                        "WHERE clause type not yet supported".to_string(),
                    ));
                }
            }
        }
        Ok(prepared)
    }

    fn evaluate_prepared_where(
        &self,
        record: &Record,
        schema: &TableSchema,
        where_clauses: &[PreparedWhereClause],
    ) -> DatabaseResult<bool> {
        // All clauses must be true (AND logic)
        for clause in where_clauses {
            match clause {
                PreparedWhereClause::Op(col, op, expr) => {
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
                PreparedWhereClause::Null(col) => {
                    let col_idx = self.resolve_single_column_index(schema, col)?;
                    if !record.get(col_idx).unwrap().is_null() {
                        return Ok(false);
                    }
                }
                PreparedWhereClause::NotNull(col) => {
                    let col_idx = self.resolve_single_column_index(schema, col)?;
                    if record.get(col_idx).unwrap().is_null() {
                        return Ok(false);
                    }
                }
                PreparedWhereClause::Like(col, regex) => {
                    let col_idx = self.resolve_single_column_index(schema, col)?;
                    let value = record.get(col_idx).unwrap();
                    let matches = match value {
                        RecordValue::String(s) => regex.is_match(s),
                        _ => false,
                    };
                    if !matches {
                        return Ok(false);
                    }
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
        let col_ref =
            self.resolve_join_column_ref(column, left_name, left_schema, right_name, right_schema)?;

        let (record, schema) = match col_ref.side {
            JoinSide::Left => (left_record, left_schema),
            JoinSide::Right => (right_record, right_schema),
        };

        let value = record.get(col_ref.index).unwrap();
        let data_type = &schema.columns[col_ref.index].data_type;
        Ok((value, data_type))
    }

    fn evaluate_prepared_join_where(
        &self,
        left_record: &Record,
        left_schema: &TableSchema,
        left_name: &str,
        right_record: &Record,
        right_schema: &TableSchema,
        right_name: &str,
        where_clauses: &[PreparedWhereClause],
    ) -> DatabaseResult<bool> {
        for clause in where_clauses {
            match clause {
                PreparedWhereClause::Op(col, op, expr) => {
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
                PreparedWhereClause::Null(col) => {
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
                PreparedWhereClause::NotNull(col) => {
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
                PreparedWhereClause::Like(col, regex) => {
                    let (value, _) = self.join_value_and_type(
                        col,
                        left_record,
                        left_schema,
                        left_name,
                        right_record,
                        right_schema,
                        right_name,
                    )?;
                    let matches = match value {
                        RecordValue::String(s) => regex.is_match(s),
                        _ => false,
                    };
                    if !matches {
                        return Ok(false);
                    }
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

    fn like_pattern_to_regex(&self, pattern: &str) -> String {
        let mut regex = String::with_capacity(pattern.len() * 2 + 2);
        regex.push('^');
        for ch in pattern.chars() {
            match ch {
                '%' => regex.push_str(".*"),
                '_' => regex.push('.'),
                '.' | '+' | '*' | '?' | '(' | ')' | '|' | '[' | ']' | '{' | '}' | '^' | '$'
                | '\\' => {
                    regex.push('\\');
                    regex.push(ch);
                }
                _ => regex.push(ch),
            }
        }
        regex.push('$');
        regex
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

    pub fn execute_alter_statement(&mut self, stmt: AlterStatement) -> DatabaseResult<QueryResult> {
        match stmt {
            AlterStatement::AddIndex(table_name, index_name, columns) => {
                if columns.is_empty() || columns.len() > 2 {
                    return Err(DatabaseError::TypeMismatch(
                        "Only one- or two-column indexes are supported".to_string(),
                    ));
                }

                let index_name = if let Some(name) = index_name {
                    name
                } else if columns.len() == 1 {
                    format!("idx_{}", columns[0])
                } else {
                    format!("idx_{}_{}", columns[0], columns[1])
                };
                let storage_name = Self::index_storage_name(&columns).ok_or_else(|| {
                    DatabaseError::TypeMismatch(
                        "Only one- or two-column indexes are supported".to_string(),
                    )
                })?;

                let (schema, col_indices, reuse_implicit) = {
                    let metadata = self
                        .current_metadata
                        .as_ref()
                        .ok_or(DatabaseError::NoDatabaseSelected)?;
                    let table_meta = metadata.get_table(&table_name)?.clone();

                    let mut implicit_on_column = false;
                    for idx in &table_meta.indexes {
                        if idx.columns == columns {
                            if idx.implicit {
                                implicit_on_column = true;
                            } else {
                                return Err(DatabaseError::TypeMismatch(format!(
                                    "Index {} already exists",
                                    index_name
                                )));
                            }
                        }
                    }
                    if table_meta.indexes.iter().any(|idx| {
                        idx.name == index_name && !(idx.implicit && idx.columns == columns)
                    }) {
                        return Err(DatabaseError::TypeMismatch(format!(
                            "Index {} already exists",
                            index_name
                        )));
                    }

                    let mut col_indices = Vec::with_capacity(columns.len());
                    for col_name in &columns {
                        let col_idx = table_meta
                            .columns
                            .iter()
                            .position(|c| c.name == *col_name)
                            .ok_or_else(|| {
                                DatabaseError::ColumnNotFound(
                                    col_name.clone(),
                                    table_name.clone(),
                                )
                            })?;
                        if table_meta.columns[col_idx].to_data_type() != DataType::Int {
                            return Err(DatabaseError::TypeMismatch(
                                "Only INT columns can be indexed".to_string(),
                            ));
                        }
                        col_indices.push(col_idx);
                    }

                    let schema = self.metadata_to_schema(&table_meta);
                    (schema, col_indices, implicit_on_column)
                };

                let db_name = self
                    .current_db
                    .as_ref()
                    .ok_or(DatabaseError::NoDatabaseSelected)?;
                let db_path = self.data_dir.join(db_name);
                let db_path_str = db_path.to_string_lossy().to_string();

                if reuse_implicit {
                    match self
                        .index_manager
                        .open_index(&db_path_str, &table_name, &storage_name)
                    {
                        Ok(()) => {}
                        Err(IndexError::IndexNotFound(_)) => {
                            let table_path = self.table_path(db_name, &table_name);
                            self.record_manager.open_table(
                                table_path.to_string_lossy().as_ref(),
                                schema.clone(),
                            )?;
                            let scan_iter = self.record_manager.scan_iter(&table_name)?;
                            if col_indices.len() == 1 {
                                let table_iter = TableIntColumnIter::new(scan_iter, col_indices[0]);
                                self.index_manager.create_index_from_table(
                                    &db_path_str,
                                    &table_name,
                                    &storage_name,
                                    table_iter,
                                )?;
                            } else {
                                let table_iter = TableCompositeIntColumnIter::new(
                                    scan_iter,
                                    col_indices[0],
                                    col_indices[1],
                                );
                                self.index_manager.create_index_from_table(
                                    &db_path_str,
                                    &table_name,
                                    &storage_name,
                                    table_iter,
                                )?;
                            }
                        }
                        Err(err) => return Err(DatabaseError::IndexError(err)),
                    }

                    let metadata = self.current_metadata.as_mut().unwrap();
                    let table_meta_mut = metadata.get_table_mut(&table_name)?;
                    if let Some(idx_meta) = table_meta_mut
                        .indexes
                        .iter_mut()
                        .find(|idx| idx.columns == columns && idx.implicit)
                    {
                        idx_meta.name = index_name;
                        idx_meta.implicit = false;
                    } else {
                        table_meta_mut.indexes.push(IndexMetadata {
                            name: index_name,
                            columns: columns.clone(),
                            implicit: false,
                        });
                    }
                    self.save_current_metadata()?;
                    return Ok(QueryResult::Empty);
                }

                let table_path = self.table_path(db_name, &table_name);
                self.record_manager
                    .open_table(table_path.to_string_lossy().as_ref(), schema.clone())?;

                let scan_iter = self.record_manager.scan_iter(&table_name)?;
                if col_indices.len() == 1 {
                    let table_iter = TableIntColumnIter::new(scan_iter, col_indices[0]);
                    self.index_manager.create_index_from_table(
                        &db_path_str,
                        &table_name,
                        &storage_name,
                        table_iter,
                    )?;
                } else {
                    let table_iter = TableCompositeIntColumnIter::new(
                        scan_iter,
                        col_indices[0],
                        col_indices[1],
                    );
                    self.index_manager.create_index_from_table(
                        &db_path_str,
                        &table_name,
                        &storage_name,
                        table_iter,
                    )?;
                }

                let metadata = self.current_metadata.as_mut().unwrap();
                let table_meta_mut = metadata.get_table_mut(&table_name)?;
                table_meta_mut.indexes.push(IndexMetadata {
                    name: index_name,
                    columns,
                    implicit: false,
                });
                self.save_current_metadata()?;

                Ok(QueryResult::Empty)
            }
            AlterStatement::DropIndex(table_name, index_name) => {
                let db_name = self
                    .current_db
                    .as_ref()
                    .ok_or(DatabaseError::NoDatabaseSelected)?;
                let db_name = db_name.clone();
                let db_path = self.data_dir.join(&db_name);
                let db_path_str = db_path.to_string_lossy().to_string();

                let mut table_meta = {
                    let metadata = self
                        .current_metadata
                        .as_mut()
                        .ok_or(DatabaseError::NoDatabaseSelected)?;
                    metadata
                        .tables
                        .remove(&table_name)
                        .ok_or_else(|| DatabaseError::TableNotFound(table_name.clone()))?
                };

                let pos = table_meta
                    .indexes
                    .iter()
                    .position(|idx| idx.name == index_name)
                    .ok_or_else(|| {
                        DatabaseError::TypeMismatch(format!("Index {} not found", index_name))
                    })?;

                let index_meta = table_meta.indexes.remove(pos);
                if index_meta.columns.is_empty() || index_meta.columns.len() > 2 {
                    return Err(DatabaseError::TypeMismatch(
                        "Only one- or two-column indexes are supported".to_string(),
                    ));
                }
                let storage_name = Self::index_storage_name(&index_meta.columns).ok_or_else(|| {
                    DatabaseError::TypeMismatch(
                        "Only one- or two-column indexes are supported".to_string(),
                    )
                })?;

                if let Err(err) =
                    self.index_manager
                        .drop_index(&db_path_str, &table_name, &storage_name)
                    && !matches!(err, crate::index::IndexError::IndexNotFound(_))
                {
                    return Err(DatabaseError::IndexError(err));
                }

                {
                    let metadata = self
                        .current_metadata
                        .as_mut()
                        .ok_or(DatabaseError::NoDatabaseSelected)?;
                    metadata.tables.insert(table_name.clone(), table_meta);
                }
                let _ = self.ensure_foreign_key_indexes()?;
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
                                DatabaseError::ColumnNotFound(col_name.clone(), table_name.clone())
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
                    .open_table(table_path.to_string_lossy().as_ref(), schema);

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

                    if table_meta.foreign_keys.iter().any(|fk| fk.name == fk_name) {
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
                    .as_ref()
                    .ok_or(DatabaseError::NoDatabaseSelected)?
                    .clone();
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

                let mut table_meta = {
                    let metadata = self.current_metadata.as_mut().unwrap();
                    metadata
                        .tables
                        .remove(&table_name)
                        .ok_or_else(|| DatabaseError::TableNotFound(table_name.clone()))?
                };
                table_meta.foreign_keys.push(fk_meta);
                {
                    let metadata = self.current_metadata.as_mut().unwrap();
                    metadata.tables.insert(table_name.clone(), table_meta);
                }
                let _ = self.ensure_foreign_key_indexes()?;
                self.save_current_metadata()?;

                Ok(QueryResult::Empty)
            }
            AlterStatement::DropFKey(table_name, fk_name) => {
                let mut table_meta = {
                    let metadata = self
                        .current_metadata
                        .as_mut()
                        .ok_or(DatabaseError::NoDatabaseSelected)?;
                    metadata
                        .tables
                        .remove(&table_name)
                        .ok_or_else(|| DatabaseError::TableNotFound(table_name.clone()))?
                };

                let pos = table_meta
                    .foreign_keys
                    .iter()
                    .position(|fk| fk.name == fk_name)
                    .ok_or_else(|| {
                        DatabaseError::TypeMismatch(format!("Foreign key {} not found", fk_name))
                    })?;
                table_meta.foreign_keys.remove(pos);

                {
                    let metadata = self
                        .current_metadata
                        .as_mut()
                        .ok_or(DatabaseError::NoDatabaseSelected)?;
                    metadata.tables.insert(table_name.clone(), table_meta);
                }
                let _ = self.ensure_foreign_key_indexes()?;
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
