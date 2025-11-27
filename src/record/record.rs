use super::error::{RecordError, RecordResult};
use super::schema::TableSchema;
use super::value::Value;
use crate::file::PageId;

/// Slot identifier within a page
pub type SlotId = usize;

/// Physical identifier for a record (page + slot)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RecordId {
    pub page_id: PageId,
    pub slot_id: SlotId,
}

impl RecordId {
    pub fn new(page_id: PageId, slot_id: SlotId) -> Self {
        Self { page_id, slot_id }
    }
}

/// A single record (row) with typed values
#[derive(Debug, Clone, PartialEq)]
pub struct Record {
    values: Vec<Value>,
}

impl Record {
    /// Create a new record
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
    }

    /// Get the number of values
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Check if record is empty
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Get a value by index
    pub fn get(&self, idx: usize) -> Option<&Value> {
        self.values.get(idx)
    }

    /// Get all values
    pub fn values(&self) -> &[Value] {
        &self.values
    }

    /// Set a value by index
    pub fn set(&mut self, idx: usize, value: Value) {
        if idx < self.values.len() {
            self.values[idx] = value;
        }
    }

    /// Serialize record to bytes according to schema
    /// Format: [NULL bitmap] [col0 data] [col1 data] ...
    pub fn serialize(&self, schema: &TableSchema) -> RecordResult<Vec<u8>> {
        // Validate record against schema
        schema.validate_record(&self.values)?;

        let mut result = Vec::with_capacity(schema.record_size());

        // 1. Create NULL bitmap
        let bitmap_size = schema.null_bitmap_size();
        let mut bitmap = vec![0u8; bitmap_size];

        for (i, value) in self.values.iter().enumerate() {
            if value.is_null() {
                let byte_idx = i / 8;
                let bit_idx = i % 8;
                bitmap[byte_idx] |= 1 << bit_idx;
            }
        }
        result.extend_from_slice(&bitmap);

        // 2. Serialize each column value
        for (value, col) in self.values.iter().zip(schema.columns()) {
            let bytes = value.serialize(&col.data_type)?;
            result.extend_from_slice(&bytes);
        }

        Ok(result)
    }

    /// Deserialize record from bytes according to schema
    pub fn deserialize(data: &[u8], schema: &TableSchema) -> RecordResult<Self> {
        if data.len() != schema.record_size() {
            return Err(RecordError::Deserialization(format!(
                "Expected {} bytes, got {}",
                schema.record_size(),
                data.len()
            )));
        }

        let bitmap_size = schema.null_bitmap_size();
        let bitmap = &data[..bitmap_size];
        let mut offset = bitmap_size;

        let mut values = Vec::with_capacity(schema.column_count());

        for (i, col) in schema.columns().iter().enumerate() {
            // Check NULL bitmap
            let byte_idx = i / 8;
            let bit_idx = i % 8;
            let is_null = (bitmap[byte_idx] & (1 << bit_idx)) != 0;

            // Deserialize value
            let col_size = col.size();
            let col_data = &data[offset..offset + col_size];
            let value = Value::deserialize(col_data, &col.data_type, is_null)?;
            values.push(value);

            offset += col_size;
        }

        Ok(Record { values })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::{ColumnDef, DataType};

    fn create_test_schema() -> TableSchema {
        TableSchema::new(
            "test".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DataType::Int, true, Value::Null),
                ColumnDef::new("name".to_string(), DataType::Char(10), false, Value::Null),
                ColumnDef::new("score".to_string(), DataType::Float, false, Value::Null),
            ],
        )
    }

    #[test]
    fn test_record_creation() {
        let record = Record::new(vec![
            Value::Int(1),
            Value::String("Alice".to_string()),
            Value::Float(95.5),
        ]);
        assert_eq!(record.len(), 3);
        assert_eq!(record.get(0), Some(&Value::Int(1)));
        assert_eq!(record.get(1), Some(&Value::String("Alice".to_string())));
        assert_eq!(record.get(2), Some(&Value::Float(95.5)));
    }

    #[test]
    fn test_record_serialization() {
        let schema = create_test_schema();
        let record = Record::new(vec![
            Value::Int(42),
            Value::String("test".to_string()),
            Value::Float(3.14),
        ]);

        let bytes = record.serialize(&schema).unwrap();
        assert_eq!(bytes.len(), schema.record_size());

        // Check NULL bitmap (all zeros - no NULLs)
        assert_eq!(bytes[0], 0);

        // Deserialize and check
        let deserialized = Record::deserialize(&bytes, &schema).unwrap();
        assert_eq!(record, deserialized);
    }

    #[test]
    fn test_record_serialization_with_nulls() {
        let schema = create_test_schema();
        let record = Record::new(vec![
            Value::Int(42),
            Value::Null, // NULL name
            Value::Null, // NULL score
        ]);

        let bytes = record.serialize(&schema).unwrap();

        // Check NULL bitmap: bits 1 and 2 should be set (0b00000110 = 6)
        assert_eq!(bytes[0], 0b00000110);

        // Deserialize and check
        let deserialized = Record::deserialize(&bytes, &schema).unwrap();
        assert_eq!(record, deserialized);
    }

    #[test]
    fn test_record_round_trip() {
        let schema = create_test_schema();
        let original = Record::new(vec![
            Value::Int(123),
            Value::String("hello".to_string()),
            Value::Float(99.9),
        ]);

        let bytes = original.serialize(&schema).unwrap();
        let restored = Record::deserialize(&bytes, &schema).unwrap();

        assert_eq!(original, restored);
    }

    #[test]
    fn test_record_validation_error() {
        let schema = create_test_schema();
        let record = Record::new(vec![
            Value::Null, // id is NOT NULL - should fail
            Value::String("test".to_string()),
            Value::Float(1.0),
        ]);

        assert!(record.serialize(&schema).is_err());
    }
}
