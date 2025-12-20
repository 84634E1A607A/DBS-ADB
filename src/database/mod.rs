use std::collections::HashMap;
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
use crate::record::{ColumnDef, Record, RecordManager, TableSchema, Value as RecordValue};

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

    #[error("Primary key violation")]
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
            if entry.file_type()?.is_dir() {
                if let Some(name) = entry.file_name().to_str() {
                    databases.push(name.to_string());
                }
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

        let mut inserted = 0;
        for row in rows {
            // Convert parser values to record values
            let mut record_values = Vec::new();
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
            self.record_manager.insert(table, record)?;
            inserted += 1;
        }

        Ok(inserted)
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

        // Scan and find matching records
        let records = self.record_manager.scan(table)?;

        let mut deleted = 0;
        for (rid, record) in records {
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

        // Scan and update matching records
        let records = self.record_manager.scan(table)?;

        let mut updated = 0;
        for (rid, mut record) in records {
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

        // Try to open table if not already open (ignore error if already open)
        if self.record_manager.scan(table_name).is_err() {
            self.record_manager
                .open_table(&table_path_str, schema.clone())?;
        }

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

        // Ensure table is open
        let table_path_str = table_path.to_string_lossy().to_string();
        if self.record_manager.scan(&table_name).is_err() {
            // Table not open, open it
            self.record_manager
                .open_table(&table_path_str, schema.clone())?;
        }

        // Scan table
        let records = self.record_manager.scan(table_name)?;

        let mut result_rows = Vec::new();
        for (_rid, record) in records {
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
        let content = fs::read_to_string(file_path)?;
        let mut rows = Vec::new();

        for line in content.lines() {
            let fields: Vec<&str> = line.split(delimiter).collect();
            let mut values = Vec::new();

            for field in fields {
                let field = field.trim();
                // Try to parse as different types
                if let Ok(i) = field.parse::<i64>() {
                    values.push(ParserValue::Integer(i));
                } else if let Ok(f) = field.parse::<f64>() {
                    values.push(ParserValue::Float(f));
                } else if field.eq_ignore_ascii_case("null") {
                    values.push(ParserValue::Null);
                } else {
                    values.push(ParserValue::String(field.to_string()));
                }
            }

            if !values.is_empty() {
                rows.push(values);
            }
        }

        self.insert(table, rows)
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
