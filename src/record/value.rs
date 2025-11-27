use super::error::{RecordError, RecordResult};

/// Represents a column data type
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataType {
    Int,         // 4 bytes
    Float,       // 8 bytes
    Char(usize), // n bytes (fixed length)
}

impl DataType {
    /// Get the size in bytes for this data type
    pub fn size(&self) -> usize {
        match self {
            DataType::Int => 4,
            DataType::Float => 8,
            DataType::Char(n) => *n,
        }
    }

    /// Convert from parser's ColumnType
    pub fn from_column_type(ct: &crate::lexer_parser::ColumnType) -> Self {
        match ct {
            crate::lexer_parser::ColumnType::Int => DataType::Int,
            crate::lexer_parser::ColumnType::Float => DataType::Float,
            crate::lexer_parser::ColumnType::Char(n) => DataType::Char(*n),
        }
    }
}

/// Represents a single column value
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Int(i32),
    Float(f64),
    String(String),
    Null,
}

impl Value {
    /// Check if this value is NULL
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Get the data type of this value
    pub fn data_type(&self) -> Option<DataType> {
        match self {
            Value::Int(_) => Some(DataType::Int),
            Value::Float(_) => Some(DataType::Float),
            Value::String(_) => None, // Need max_len from schema
            Value::Null => None,
        }
    }

    /// Serialize value to bytes
    /// For String, max_len must be provided and value is padded to that length
    pub fn serialize(&self, data_type: &DataType) -> RecordResult<Vec<u8>> {
        match (self, data_type) {
            (Value::Int(i), DataType::Int) => Ok(i.to_le_bytes().to_vec()),
            (Value::Float(f), DataType::Float) => Ok(f.to_le_bytes().to_vec()),
            (Value::String(s), DataType::Char(max_len)) => {
                let bytes = s.as_bytes();
                if bytes.len() > *max_len {
                    return Err(RecordError::Serialization(format!(
                        "String length {} exceeds max length {}",
                        bytes.len(),
                        max_len
                    )));
                }
                let mut result = vec![0u8; *max_len];
                result[..bytes.len()].copy_from_slice(bytes);
                Ok(result)
            }
            (Value::Null, _) => {
                // NULL values are represented by zeros
                Ok(vec![0u8; data_type.size()])
            }
            _ => Err(RecordError::TypeMismatch {
                expected: format!("{:?}", data_type),
                actual: format!("{:?}", self),
            }),
        }
    }

    /// Deserialize value from bytes
    pub fn deserialize(bytes: &[u8], data_type: &DataType, is_null: bool) -> RecordResult<Self> {
        if is_null {
            return Ok(Value::Null);
        }

        match data_type {
            DataType::Int => {
                if bytes.len() != 4 {
                    return Err(RecordError::Deserialization(format!(
                        "Expected 4 bytes for INT, got {}",
                        bytes.len()
                    )));
                }
                let mut buf = [0u8; 4];
                buf.copy_from_slice(bytes);
                Ok(Value::Int(i32::from_le_bytes(buf)))
            }
            DataType::Float => {
                if bytes.len() != 8 {
                    return Err(RecordError::Deserialization(format!(
                        "Expected 8 bytes for FLOAT, got {}",
                        bytes.len()
                    )));
                }
                let mut buf = [0u8; 8];
                buf.copy_from_slice(bytes);
                Ok(Value::Float(f64::from_le_bytes(buf)))
            }
            DataType::Char(max_len) => {
                if bytes.len() != *max_len {
                    return Err(RecordError::Deserialization(format!(
                        "Expected {} bytes for CHAR({}), got {}",
                        max_len,
                        max_len,
                        bytes.len()
                    )));
                }
                // Find the first null byte (string terminator)
                let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
                let s = String::from_utf8(bytes[..end].to_vec())
                    .map_err(|e| RecordError::Deserialization(format!("Invalid UTF-8: {}", e)))?;
                Ok(Value::String(s))
            }
        }
    }

    /// Convert from parser's Value
    pub fn from_parser_value(pv: &crate::lexer_parser::Value) -> Self {
        match pv {
            crate::lexer_parser::Value::Integer(i) => Value::Int(*i as i32),
            crate::lexer_parser::Value::Float(f) => Value::Float(*f),
            crate::lexer_parser::Value::String(s) => Value::String(s.clone()),
            crate::lexer_parser::Value::Null => Value::Null,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_type_size() {
        assert_eq!(DataType::Int.size(), 4);
        assert_eq!(DataType::Float.size(), 8);
        assert_eq!(DataType::Char(10).size(), 10);
        assert_eq!(DataType::Char(255).size(), 255);
    }

    #[test]
    fn test_value_is_null() {
        assert!(Value::Null.is_null());
        assert!(!Value::Int(42).is_null());
        assert!(!Value::Float(3.14).is_null());
        assert!(!Value::String("hello".to_string()).is_null());
    }

    #[test]
    fn test_int_serialization() {
        let val = Value::Int(42);
        let dt = DataType::Int;
        let bytes = val.serialize(&dt).unwrap();
        assert_eq!(bytes.len(), 4);

        let deserialized = Value::deserialize(&bytes, &dt, false).unwrap();
        assert_eq!(val, deserialized);
    }

    #[test]
    fn test_float_serialization() {
        let val = Value::Float(3.14159);
        let dt = DataType::Float;
        let bytes = val.serialize(&dt).unwrap();
        assert_eq!(bytes.len(), 8);

        let deserialized = Value::deserialize(&bytes, &dt, false).unwrap();
        assert_eq!(val, deserialized);
    }

    #[test]
    fn test_string_serialization() {
        let val = Value::String("hello".to_string());
        let dt = DataType::Char(10);
        let bytes = val.serialize(&dt).unwrap();
        assert_eq!(bytes.len(), 10);
        assert_eq!(&bytes[..5], b"hello");
        assert_eq!(&bytes[5..], &[0u8; 5]);

        let deserialized = Value::deserialize(&bytes, &dt, false).unwrap();
        assert_eq!(val, deserialized);
    }

    #[test]
    fn test_string_too_long() {
        let val = Value::String("hello world".to_string());
        let dt = DataType::Char(5);
        let result = val.serialize(&dt);
        assert!(result.is_err());
    }

    #[test]
    fn test_null_serialization() {
        let val = Value::Null;
        let dt = DataType::Int;
        let bytes = val.serialize(&dt).unwrap();
        assert_eq!(bytes, vec![0u8; 4]);

        let deserialized = Value::deserialize(&bytes, &dt, true).unwrap();
        assert_eq!(deserialized, Value::Null);
    }

    #[test]
    fn test_type_mismatch() {
        let val = Value::Int(42);
        let dt = DataType::Float;
        let result = val.serialize(&dt);
        assert!(result.is_err());
    }
}
