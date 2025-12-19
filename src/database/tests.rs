use super::*;
use crate::lexer_parser::{ColumnType, CreateTableField, Selectors, WhereClause};
use tempfile::TempDir;

fn setup_test_db() -> (TempDir, DatabaseManager) {
    let temp_dir = TempDir::new().unwrap();
    let db_manager = DatabaseManager::new(temp_dir.path().to_str().unwrap()).unwrap();
    (temp_dir, db_manager)
}

#[test]
fn test_create_and_show_databases() {
    let (_temp, mut db_manager) = setup_test_db();

    // Initially no databases
    let dbs = db_manager.show_databases().unwrap();
    assert_eq!(dbs.len(), 0);

    // Create databases
    db_manager.create_database("db1").unwrap();
    db_manager.create_database("db2").unwrap();
    db_manager.create_database("db3").unwrap();

    // Show databases
    let dbs = db_manager.show_databases().unwrap();
    assert_eq!(dbs.len(), 3);
    assert!(dbs.contains(&"db1".to_string()));
    assert!(dbs.contains(&"db2".to_string()));
    assert!(dbs.contains(&"db3".to_string()));

    // Try to create duplicate
    let result = db_manager.create_database("db1");
    assert!(result.is_err());
}

#[test]
fn test_use_database() {
    let (_temp, mut db_manager) = setup_test_db();

    // Create database
    db_manager.create_database("testdb").unwrap();

    // Use non-existent database
    let result = db_manager.use_database("nonexistent");
    assert!(result.is_err());

    // Use existing database
    db_manager.use_database("testdb").unwrap();
    assert_eq!(db_manager.current_db, Some("testdb".to_string()));
}

#[test]
fn test_drop_database() {
    let (_temp, mut db_manager) = setup_test_db();

    // Create databases
    db_manager.create_database("db1").unwrap();
    db_manager.create_database("db2").unwrap();

    // Drop database
    db_manager.drop_database("db1").unwrap();

    let dbs = db_manager.show_databases().unwrap();
    assert_eq!(dbs.len(), 1);
    assert!(!dbs.contains(&"db1".to_string()));
    assert!(dbs.contains(&"db2".to_string()));

    // Drop non-existent database
    let result = db_manager.drop_database("nonexistent");
    assert!(result.is_err());
}

#[test]
fn test_create_table() {
    let (_temp, mut db_manager) = setup_test_db();

    // Create database and use it
    db_manager.create_database("testdb").unwrap();
    db_manager.use_database("testdb").unwrap();

    // Create table
    let fields = vec![
        CreateTableField::Col(
            "id".to_string(),
            ColumnType::Int,
            true,
            ParserValue::Null,
        ),
        CreateTableField::Col(
            "name".to_string(),
            ColumnType::Char(20),
            true,
            ParserValue::Null,
        ),
        CreateTableField::Col(
            "score".to_string(),
            ColumnType::Float,
            false,
            ParserValue::Float(0.0),
        ),
    ];

    db_manager.create_table("students", fields).unwrap();

    // Show tables
    let tables = db_manager.show_tables().unwrap();
    assert_eq!(tables.len(), 1);
    assert!(tables.contains(&"students".to_string()));

    // Describe table
    let meta = db_manager.describe_table("students").unwrap();
    assert_eq!(meta.columns.len(), 3);
    assert_eq!(meta.columns[0].name, "id");
    assert_eq!(meta.columns[1].name, "name");
    assert_eq!(meta.columns[2].name, "score");
}

#[test]
fn test_create_table_without_database() {
    let (_temp, mut db_manager) = setup_test_db();

    let fields = vec![CreateTableField::Col(
        "id".to_string(),
        ColumnType::Int,
        true,
        ParserValue::Null,
    )];

    let result = db_manager.create_table("test", fields);
    assert!(result.is_err());
}

#[test]
fn test_drop_table() {
    let (_temp, mut db_manager) = setup_test_db();

    db_manager.create_database("testdb").unwrap();
    db_manager.use_database("testdb").unwrap();

    let fields = vec![CreateTableField::Col(
        "id".to_string(),
        ColumnType::Int,
        true,
        ParserValue::Null,
    )];

    db_manager.create_table("table1", fields.clone()).unwrap();
    db_manager.create_table("table2", fields).unwrap();

    // Drop table
    db_manager.drop_table("table1").unwrap();

    let tables = db_manager.show_tables().unwrap();
    assert_eq!(tables.len(), 1);
    assert!(tables.contains(&"table2".to_string()));
}

#[test]
fn test_insert_and_select() {
    let (_temp, mut db_manager) = setup_test_db();

    db_manager.create_database("testdb").unwrap();
    db_manager.use_database("testdb").unwrap();

    let fields = vec![
        CreateTableField::Col(
            "a".to_string(),
            ColumnType::Int,
            true,
            ParserValue::Null,
        ),
        CreateTableField::Col(
            "b".to_string(),
            ColumnType::Char(10),
            true,
            ParserValue::Null,
        ),
        CreateTableField::Col(
            "c".to_string(),
            ColumnType::Float,
            true,
            ParserValue::Null,
        ),
    ];

    db_manager.create_table("test", fields).unwrap();

    // Insert single row
    let rows = vec![vec![
        ParserValue::Integer(1),
        ParserValue::String("hello".to_string()),
        ParserValue::Float(1.5),
    ]];

    let count = db_manager.insert("test", rows).unwrap();
    assert_eq!(count, 1);

    // Insert multiple rows
    let rows = vec![
        vec![
            ParserValue::Integer(2),
            ParserValue::String("world".to_string()),
            ParserValue::Float(2.5),
        ],
        vec![
            ParserValue::Integer(3),
            ParserValue::String("rust".to_string()),
            ParserValue::Float(3.5),
        ],
    ];

    let count = db_manager.insert("test", rows).unwrap();
    assert_eq!(count, 2);

    // Select all
    let clause = SelectClause {
        selectors: Selectors::All,
        table: vec!["test".to_string()],
        where_clauses: vec![],
        group_by: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let (headers, rows) = db_manager.select(clause).unwrap();
    assert_eq!(headers, vec!["a", "b", "c"]);
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], vec!["1", "hello", "1.50"]);
    assert_eq!(rows[1], vec!["2", "world", "2.50"]);
    assert_eq!(rows[2], vec!["3", "rust", "3.50"]);
}

#[test]
fn test_select_specific_columns() {
    let (_temp, mut db_manager) = setup_test_db();

    db_manager.create_database("testdb").unwrap();
    db_manager.use_database("testdb").unwrap();

    let fields = vec![
        CreateTableField::Col(
            "a".to_string(),
            ColumnType::Int,
            true,
            ParserValue::Null,
        ),
        CreateTableField::Col(
            "b".to_string(),
            ColumnType::Char(10),
            true,
            ParserValue::Null,
        ),
        CreateTableField::Col(
            "c".to_string(),
            ColumnType::Float,
            true,
            ParserValue::Null,
        ),
    ];

    db_manager.create_table("test", fields).unwrap();

    let rows = vec![vec![
        ParserValue::Integer(1),
        ParserValue::String("hello".to_string()),
        ParserValue::Float(1.5),
    ]];
    db_manager.insert("test", rows).unwrap();

    // Select specific columns
    let clause = SelectClause {
        selectors: Selectors::List(vec![
            crate::lexer_parser::Selector::Column(crate::lexer_parser::TableColumn {
                table: None,
                column: "b".to_string(),
            }),
            crate::lexer_parser::Selector::Column(crate::lexer_parser::TableColumn {
                table: None,
                column: "a".to_string(),
            }),
        ]),
        table: vec!["test".to_string()],
        where_clauses: vec![],
        group_by: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let (headers, rows) = db_manager.select(clause).unwrap();
    assert_eq!(headers, vec!["b", "a"]);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], vec!["hello", "1"]);
}

#[test]
fn test_select_with_where() {
    let (_temp, mut db_manager) = setup_test_db();

    db_manager.create_database("testdb").unwrap();
    db_manager.use_database("testdb").unwrap();

    let fields = vec![
        CreateTableField::Col(
            "a".to_string(),
            ColumnType::Int,
            true,
            ParserValue::Null,
        ),
        CreateTableField::Col(
            "b".to_string(),
            ColumnType::Char(10),
            true,
            ParserValue::Null,
        ),
    ];

    db_manager.create_table("test", fields).unwrap();

    // Insert test data
    let rows = vec![
        vec![ParserValue::Integer(1), ParserValue::String("a".to_string())],
        vec![ParserValue::Integer(2), ParserValue::String("b".to_string())],
        vec![ParserValue::Integer(3), ParserValue::String("c".to_string())],
        vec![ParserValue::Integer(4), ParserValue::String("d".to_string())],
        vec![ParserValue::Integer(5), ParserValue::String("e".to_string())],
    ];
    db_manager.insert("test", rows).unwrap();

    // WHERE a > 2
    let clause = SelectClause {
        selectors: Selectors::All,
        table: vec!["test".to_string()],
        where_clauses: vec![WhereClause::Op(
            crate::lexer_parser::TableColumn {
                table: None,
                column: "a".to_string(),
            },
            Operator::Gt,
            crate::lexer_parser::Expression::Value(ParserValue::Integer(2)),
        )],
        group_by: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let (_, rows) = db_manager.select(clause).unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0], "3");
    assert_eq!(rows[1][0], "4");
    assert_eq!(rows[2][0], "5");
}

#[test]
fn test_select_with_multiple_where() {
    let (_temp, mut db_manager) = setup_test_db();

    db_manager.create_database("testdb").unwrap();
    db_manager.use_database("testdb").unwrap();

    let fields = vec![
        CreateTableField::Col(
            "a".to_string(),
            ColumnType::Int,
            true,
            ParserValue::Null,
        ),
        CreateTableField::Col(
            "b".to_string(),
            ColumnType::Char(10),
            true,
            ParserValue::Null,
        ),
    ];

    db_manager.create_table("test", fields).unwrap();

    let rows = vec![
        vec![ParserValue::Integer(1), ParserValue::String("a".to_string())],
        vec![ParserValue::Integer(2), ParserValue::String("b".to_string())],
        vec![ParserValue::Integer(3), ParserValue::String("c".to_string())],
        vec![ParserValue::Integer(4), ParserValue::String("d".to_string())],
        vec![ParserValue::Integer(5), ParserValue::String("e".to_string())],
    ];
    db_manager.insert("test", rows).unwrap();

    // WHERE a >= 2 AND a <= 4
    let clause = SelectClause {
        selectors: Selectors::All,
        table: vec!["test".to_string()],
        where_clauses: vec![
            WhereClause::Op(
                crate::lexer_parser::TableColumn {
                    table: None,
                    column: "a".to_string(),
                },
                Operator::Ge,
                crate::lexer_parser::Expression::Value(ParserValue::Integer(2)),
            ),
            WhereClause::Op(
                crate::lexer_parser::TableColumn {
                    table: None,
                    column: "a".to_string(),
                },
                Operator::Le,
                crate::lexer_parser::Expression::Value(ParserValue::Integer(4)),
            ),
        ],
        group_by: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let (_, rows) = db_manager.select(clause).unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0], "2");
    assert_eq!(rows[1][0], "3");
    assert_eq!(rows[2][0], "4");
}

#[test]
fn test_update() {
    let (_temp, mut db_manager) = setup_test_db();

    db_manager.create_database("testdb").unwrap();
    db_manager.use_database("testdb").unwrap();

    let fields = vec![
        CreateTableField::Col(
            "a".to_string(),
            ColumnType::Int,
            true,
            ParserValue::Null,
        ),
        CreateTableField::Col(
            "b".to_string(),
            ColumnType::Char(10),
            true,
            ParserValue::Null,
        ),
    ];

    db_manager.create_table("test", fields).unwrap();

    let rows = vec![
        vec![ParserValue::Integer(1), ParserValue::String("a".to_string())],
        vec![ParserValue::Integer(2), ParserValue::String("b".to_string())],
        vec![ParserValue::Integer(3), ParserValue::String("c".to_string())],
    ];
    db_manager.insert("test", rows).unwrap();

    // Update WHERE a = 2
    let updates = vec![("b".to_string(), ParserValue::String("updated".to_string()))];
    let where_clauses = Some(vec![WhereClause::Op(
        crate::lexer_parser::TableColumn {
            table: None,
            column: "a".to_string(),
        },
        Operator::Eq,
        crate::lexer_parser::Expression::Value(ParserValue::Integer(2)),
    )]);

    let count = db_manager.update("test", updates, where_clauses).unwrap();
    assert_eq!(count, 1);

    // Verify update
    let clause = SelectClause {
        selectors: Selectors::All,
        table: vec!["test".to_string()],
        where_clauses: vec![],
        group_by: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let (_, rows) = db_manager.select(clause).unwrap();
    assert_eq!(rows[1][1], "updated");
}

#[test]
fn test_delete() {
    let (_temp, mut db_manager) = setup_test_db();

    db_manager.create_database("testdb").unwrap();
    db_manager.use_database("testdb").unwrap();

    let fields = vec![CreateTableField::Col(
        "a".to_string(),
        ColumnType::Int,
        true,
        ParserValue::Null,
    )];

    db_manager.create_table("test", fields).unwrap();

    let rows = vec![
        vec![ParserValue::Integer(1)],
        vec![ParserValue::Integer(2)],
        vec![ParserValue::Integer(3)],
        vec![ParserValue::Integer(4)],
        vec![ParserValue::Integer(5)],
    ];
    db_manager.insert("test", rows).unwrap();

    // Delete WHERE a > 3
    let where_clauses = Some(vec![WhereClause::Op(
        crate::lexer_parser::TableColumn {
            table: None,
            column: "a".to_string(),
        },
        Operator::Gt,
        crate::lexer_parser::Expression::Value(ParserValue::Integer(3)),
    )]);

    let count = db_manager.delete("test", where_clauses).unwrap();
    assert_eq!(count, 2);

    // Verify deletion
    let clause = SelectClause {
        selectors: Selectors::All,
        table: vec!["test".to_string()],
        where_clauses: vec![],
        group_by: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let (_, rows) = db_manager.select(clause).unwrap();
    assert_eq!(rows.len(), 3);
}

#[test]
fn test_delete_all() {
    let (_temp, mut db_manager) = setup_test_db();

    db_manager.create_database("testdb").unwrap();
    db_manager.use_database("testdb").unwrap();

    let fields = vec![CreateTableField::Col(
        "a".to_string(),
        ColumnType::Int,
        true,
        ParserValue::Null,
    )];

    db_manager.create_table("test", fields).unwrap();

    let rows = vec![
        vec![ParserValue::Integer(1)],
        vec![ParserValue::Integer(2)],
        vec![ParserValue::Integer(3)],
    ];
    db_manager.insert("test", rows).unwrap();

    // Delete all
    let count = db_manager.delete("test", None).unwrap();
    assert_eq!(count, 3);

    // Verify deletion
    let clause = SelectClause {
        selectors: Selectors::All,
        table: vec!["test".to_string()],
        where_clauses: vec![],
        group_by: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let (_, rows) = db_manager.select(clause).unwrap();
    assert_eq!(rows.len(), 0);
}

#[test]
fn test_not_null_constraint() {
    let (_temp, mut db_manager) = setup_test_db();

    db_manager.create_database("testdb").unwrap();
    db_manager.use_database("testdb").unwrap();

    let fields = vec![CreateTableField::Col(
        "a".to_string(),
        ColumnType::Int,
        true, // NOT NULL
        ParserValue::Null,
    )];

    db_manager.create_table("test", fields).unwrap();

    // Try to insert NULL into NOT NULL column
    let rows = vec![vec![ParserValue::Null]];
    let result = db_manager.insert("test", rows);
    assert!(result.is_err());
}

#[test]
fn test_load_data_infile() {
    let (_temp, mut db_manager) = setup_test_db();

    db_manager.create_database("testdb").unwrap();
    db_manager.use_database("testdb").unwrap();

    let fields = vec![
        CreateTableField::Col(
            "a".to_string(),
            ColumnType::Int,
            true,
            ParserValue::Null,
        ),
        CreateTableField::Col(
            "b".to_string(),
            ColumnType::Char(10),
            true,
            ParserValue::Null,
        ),
        CreateTableField::Col(
            "c".to_string(),
            ColumnType::Float,
            true,
            ParserValue::Null,
        ),
    ];

    db_manager.create_table("test", fields).unwrap();

    // Create test CSV file
    let csv_content = "1,hello,1.5\n2,world,2.5\n3,rust,3.5\n";
    let csv_file = _temp.path().join("test.csv");
    std::fs::write(&csv_file, csv_content).unwrap();

    // Load data
    let count = db_manager
        .load_data_infile(csv_file.to_str().unwrap(), "test", ',')
        .unwrap();
    assert_eq!(count, 3);

    // Verify loaded data
    let clause = SelectClause {
        selectors: Selectors::All,
        table: vec!["test".to_string()],
        where_clauses: vec![],
        group_by: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let (_, rows) = db_manager.select(clause).unwrap();
    assert_eq!(rows.len(), 3);
}

#[test]
fn test_varchar_as_fixed_char() {
    let (_temp, mut db_manager) = setup_test_db();

    db_manager.create_database("testdb").unwrap();
    db_manager.use_database("testdb").unwrap();

    let fields = vec![CreateTableField::Col(
        "name".to_string(),
        ColumnType::Char(5), // VARCHAR(5) treated as CHAR(5)
        true,
        ParserValue::Null,
    )];

    db_manager.create_table("test", fields).unwrap();

    // Insert short string
    let rows = vec![vec![ParserValue::String("hi".to_string())]];
    db_manager.insert("test", rows).unwrap();

    // Verify it's stored and retrieved correctly
    let clause = SelectClause {
        selectors: Selectors::All,
        table: vec!["test".to_string()],
        where_clauses: vec![],
        group_by: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let (_, rows) = db_manager.select(clause).unwrap();
    assert_eq!(rows.len(), 1);
    // String should be trimmed when displayed
    assert_eq!(rows[0][0], "hi");
}

#[test]
fn test_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().to_str().unwrap();

    // Create database and table in first session
    {
        let mut db_manager = DatabaseManager::new(data_path).unwrap();
        db_manager.create_database("testdb").unwrap();
        db_manager.use_database("testdb").unwrap();

        let fields = vec![CreateTableField::Col(
            "a".to_string(),
            ColumnType::Int,
            true,
            ParserValue::Null,
        )];
        db_manager.create_table("test", fields).unwrap();

        let rows = vec![vec![ParserValue::Integer(42)]];
        db_manager.insert("test", rows).unwrap();
    }

    // Reopen and verify data persists
    {
        let mut db_manager = DatabaseManager::new(data_path).unwrap();
        db_manager.use_database("testdb").unwrap();

        let clause = SelectClause {
            selectors: Selectors::All,
            table: vec!["test".to_string()],
            where_clauses: vec![],
            group_by: None,
            order_by: None,
            limit: None,
            offset: None,
        };

        let (_, rows) = db_manager.select(clause).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], "42");
    }
}
