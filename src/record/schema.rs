use super::error::{RecordError, RecordResult};
use super::value::{DataType, Value};

/// Column definition with metadata
#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub not_null: bool,
    pub default_value: Value,
}

impl ColumnDef {
    /// Create a new column definition
    pub fn new(name: String, data_type: DataType, not_null: bool, default_value: Value) -> Self {
        Self {
            name,
            data_type,
            not_null,
            default_value,
        }
    }

    /// Get the size of this column in bytes
    pub fn size(&self) -> usize {
        self.data_type.size()
    }
}

/// Table schema with all column definitions
#[derive(Debug, Clone)]
pub struct TableSchema {
    table_name: String,
    columns: Vec<ColumnDef>,
    null_bitmap_size: usize,
    record_size: usize,
}

impl TableSchema {
    /// Create a new table schema
    pub fn new(table_name: String, columns: Vec<ColumnDef>) -> Self {
        let null_bitmap_size = columns.len().div_ceil(8); // ⌈n/8⌉
        let record_size = null_bitmap_size + columns.iter().map(|c| c.size()).sum::<usize>();

        Self {
            table_name,
            columns,
            null_bitmap_size,
            record_size,
        }
    }

    /// Get table name
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Get all columns
    pub fn columns(&self) -> &[ColumnDef] {
        &self.columns
    }

    /// Get column count
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Get a specific column
    pub fn column(&self, idx: usize) -> Option<&ColumnDef> {
        self.columns.get(idx)
    }

    /// Find column index by name
    pub fn find_column(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    /// Get NULL bitmap size in bytes
    pub fn null_bitmap_size(&self) -> usize {
        self.null_bitmap_size
    }

    /// Get total record size in bytes (including NULL bitmap)
    pub fn record_size(&self) -> usize {
        self.record_size
    }

    /// Get the byte offset of a column within a record (after NULL bitmap)
    pub fn column_offset(&self, col_idx: usize) -> usize {
        self.null_bitmap_size
            + self.columns[..col_idx]
                .iter()
                .map(|c| c.size())
                .sum::<usize>()
    }

    /// Validate a record against this schema
    pub fn validate_record(&self, values: &[Value]) -> RecordResult<()> {
        if values.len() != self.columns.len() {
            return Err(RecordError::SchemaMismatch(format!(
                "Expected {} columns, got {}",
                self.columns.len(),
                values.len()
            )));
        }

        for (value, col) in values.iter().zip(&self.columns) {
            // Check NOT NULL constraint
            if col.not_null && value.is_null() {
                return Err(RecordError::NullConstraintViolation(format!(
                    "Column '{}' cannot be NULL",
                    col.name
                )));
            }

            // Check type compatibility (if not NULL)
            if !value.is_null() {
                match (&col.data_type, value) {
                    (DataType::Int, Value::Int(_)) => {}
                    (DataType::Float, Value::Float(_)) => {}
                    (DataType::Char(_), Value::String(_)) => {}
                    (dt, val) => {
                        return Err(RecordError::TypeMismatch {
                            expected: format!("{:?}", dt),
                            actual: format!("{:?}", val),
                        });
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_schema() -> TableSchema {
        TableSchema::new(
            "test_table".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DataType::Int, true, Value::Null),
                ColumnDef::new(
                    "name".to_string(),
                    DataType::Char(20),
                    false,
                    Value::String("".to_string()),
                ),
                ColumnDef::new("score".to_string(), DataType::Float, false, Value::Null),
            ],
        )
    }

    #[test]
    fn test_schema_creation() {
        let schema = create_test_schema();
        assert_eq!(schema.table_name(), "test_table");
        assert_eq!(schema.column_count(), 3);
        assert_eq!(schema.null_bitmap_size(), 1); // ⌈3/8⌉ = 1
        assert_eq!(schema.record_size(), 1 + 4 + 20 + 8); // bitmap + int + char(20) + float
    }

    #[test]
    fn test_column_offset() {
        let schema = create_test_schema();
        assert_eq!(schema.column_offset(0), 1); // After 1-byte bitmap
        assert_eq!(schema.column_offset(1), 1 + 4); // After bitmap + int
        assert_eq!(schema.column_offset(2), 1 + 4 + 20); // After bitmap + int + char(20)
    }

    #[test]
    fn test_find_column() {
        let schema = create_test_schema();
        assert_eq!(schema.find_column("id"), Some(0));
        assert_eq!(schema.find_column("name"), Some(1));
        assert_eq!(schema.find_column("score"), Some(2));
        assert_eq!(schema.find_column("nonexistent"), None);
    }

    #[test]
    fn test_validate_record_success() {
        let schema = create_test_schema();
        let values = vec![
            Value::Int(1),
            Value::String("Alice".to_string()),
            Value::Float(95.5),
        ];
        assert!(schema.validate_record(&values).is_ok());
    }

    #[test]
    fn test_validate_record_null_allowed() {
        let schema = create_test_schema();
        let values = vec![
            Value::Int(1),
            Value::Null, // NULL allowed for name
            Value::Null, // NULL allowed for score
        ];
        assert!(schema.validate_record(&values).is_ok());
    }

    #[test]
    fn test_validate_record_not_null_violation() {
        let schema = create_test_schema();
        let values = vec![
            Value::Null, // id is NOT NULL
            Value::String("Alice".to_string()),
            Value::Float(95.5),
        ];
        assert!(schema.validate_record(&values).is_err());
    }

    #[test]
    fn test_validate_record_type_mismatch() {
        let schema = create_test_schema();
        let values = vec![
            Value::String("not_an_int".to_string()), // Wrong type for id
            Value::String("Alice".to_string()),
            Value::Float(95.5),
        ];
        assert!(schema.validate_record(&values).is_err());
    }

    #[test]
    fn test_validate_record_column_count_mismatch() {
        let schema = create_test_schema();
        let values = vec![
            Value::Int(1),
            Value::String("Alice".to_string()),
            // Missing score column
        ];
        assert!(schema.validate_record(&values).is_err());
    }
}
