use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use thiserror::Error;

use crate::lexer_parser::{ColumnType, Value as ParserValue};
use crate::record::{DataType, Value as RecordValue};

#[derive(Debug, Error)]
pub enum CatalogError {
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("Table {0} not found")]
    TableNotFound(String),

    #[error("Column {0} not found")]
    ColumnNotFound(String),
}

pub type CatalogResult<T> = Result<T, CatalogError>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMetadata {
    pub name: String,
    #[serde(rename = "type")]
    pub column_type: String, // Store as string for JSON: "INT", "FLOAT", "VARCHAR(n)"
    pub not_null: bool,
    pub default_value: Option<String>, // Store as string for JSON
}

impl ColumnMetadata {
    pub fn from_parser(name: String, ct: ColumnType, not_null: bool, default: ParserValue) -> Self {
        let column_type = match ct {
            ColumnType::Int => "INT".to_string(),
            ColumnType::Float => "FLOAT".to_string(),
            ColumnType::Char(n) => format!("VARCHAR({})", n),
        };

        let default_value = match default {
            ParserValue::Null => None,
            ParserValue::Integer(i) => Some(i.to_string()),
            ParserValue::Float(f) => Some(f.to_string()),
            ParserValue::String(s) => Some(s),
        };

        Self {
            name,
            column_type,
            not_null,
            default_value,
        }
    }

    pub fn to_data_type(&self) -> DataType {
        if self.column_type == "INT" {
            DataType::Int
        } else if self.column_type == "FLOAT" {
            DataType::Float
        } else if self.column_type.starts_with("VARCHAR(") {
            let size: usize = self.column_type[8..self.column_type.len() - 1]
                .parse()
                .unwrap();
            DataType::Char(size)
        } else if self.column_type.starts_with("CHAR(") {
            // Backward compatibility for existing metadata
            let size: usize = self.column_type[5..self.column_type.len() - 1]
                .parse()
                .unwrap();
            DataType::Char(size)
        } else {
            panic!("Unknown column type: {}", self.column_type);
        }
    }

    pub fn parse_default_value(&self) -> RecordValue {
        match &self.default_value {
            None => RecordValue::Null,
            Some(s) => {
                if self.column_type == "INT" {
                    RecordValue::Int(s.parse().unwrap())
                } else if self.column_type == "FLOAT" {
                    RecordValue::Float(s.parse().unwrap())
                } else {
                    RecordValue::String(s.clone())
                }
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexMetadata {
    pub name: String,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKeyMetadata {
    pub name: String,
    pub columns: Vec<String>,
    pub ref_table: String,
    pub ref_columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadata {
    pub name: String,
    pub columns: Vec<ColumnMetadata>,
    pub primary_key: Option<Vec<String>>,
    pub foreign_keys: Vec<ForeignKeyMetadata>,
    pub indexes: Vec<IndexMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseMetadata {
    pub name: String,
    pub tables: HashMap<String, TableMetadata>,
}

impl DatabaseMetadata {
    pub fn new(name: String) -> Self {
        Self {
            name,
            tables: HashMap::new(),
        }
    }

    pub fn load(db_path: &Path) -> CatalogResult<Self> {
        let metadata_path = db_path.join("metadata.json");
        let content = fs::read_to_string(&metadata_path)?;
        let metadata = serde_json::from_str(&content)?;
        Ok(metadata)
    }

    pub fn save(&self, db_path: &Path) -> CatalogResult<()> {
        let metadata_path = db_path.join("metadata.json");
        let content = serde_json::to_string_pretty(&self)?;
        fs::write(&metadata_path, content)?;
        Ok(())
    }

    pub fn add_table(&mut self, metadata: TableMetadata) {
        self.tables.insert(metadata.name.clone(), metadata);
    }

    pub fn remove_table(&mut self, name: &str) -> CatalogResult<()> {
        self.tables
            .remove(name)
            .ok_or_else(|| CatalogError::TableNotFound(name.to_string()))?;
        Ok(())
    }

    pub fn get_table(&self, name: &str) -> CatalogResult<&TableMetadata> {
        self.tables
            .get(name)
            .ok_or_else(|| CatalogError::TableNotFound(name.to_string()))
    }

    pub fn get_table_mut(&mut self, name: &str) -> CatalogResult<&mut TableMetadata> {
        self.tables
            .get_mut(name)
            .ok_or_else(|| CatalogError::TableNotFound(name.to_string()))
    }
}
