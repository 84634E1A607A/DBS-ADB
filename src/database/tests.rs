use super::*;
use crate::lexer_parser::{
    AlterStatement, ColumnType, CreateTableField, Expression, Operator, SelectClause, Selector,
    Selectors, TableColumn, WhereClause,
};
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
        CreateTableField::Col("id".to_string(), ColumnType::Int, true, ParserValue::Null),
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
        CreateTableField::Col("a".to_string(), ColumnType::Int, true, ParserValue::Null),
        CreateTableField::Col(
            "b".to_string(),
            ColumnType::Char(10),
            true,
            ParserValue::Null,
        ),
        CreateTableField::Col("c".to_string(), ColumnType::Float, true, ParserValue::Null),
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
fn test_select_aggregate_no_group() {
    let (_temp, mut db_manager) = setup_test_db();

    db_manager.create_database("testdb").unwrap();
    db_manager.use_database("testdb").unwrap();

    let fields = vec![
        CreateTableField::Col("a".to_string(), ColumnType::Int, true, ParserValue::Null),
        CreateTableField::Col("b".to_string(), ColumnType::Float, false, ParserValue::Null),
        CreateTableField::Col(
            "c".to_string(),
            ColumnType::Char(10),
            false,
            ParserValue::Null,
        ),
    ];
    db_manager.create_table("t", fields).unwrap();

    let rows = vec![
        vec![
            ParserValue::Integer(1),
            ParserValue::Float(1.0),
            ParserValue::String("x".to_string()),
        ],
        vec![
            ParserValue::Integer(2),
            ParserValue::Float(3.5),
            ParserValue::String("y".to_string()),
        ],
        vec![
            ParserValue::Integer(3),
            ParserValue::Null,
            ParserValue::String("z".to_string()),
        ],
    ];
    db_manager.insert("t", rows).unwrap();

    let clause = SelectClause {
        selectors: Selectors::List(vec![
            Selector::Min(TableColumn {
                table: None,
                column: "a".to_string(),
            }),
            Selector::Max(TableColumn {
                table: None,
                column: "b".to_string(),
            }),
            Selector::Sum(TableColumn {
                table: None,
                column: "a".to_string(),
            }),
            Selector::CountAll,
        ]),
        table: vec!["t".to_string()],
        where_clauses: vec![],
        group_by: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let (headers, rows) = db_manager.select(clause).unwrap();
    assert_eq!(headers, vec!["MIN(a)", "MAX(b)", "SUM(a)", "COUNT(*)"]);
    assert_eq!(rows, vec![vec!["1", "3.50", "6", "3"]]);
}

#[test]
fn test_select_aggregate_group_by() {
    let (_temp, mut db_manager) = setup_test_db();

    db_manager.create_database("testdb").unwrap();
    db_manager.use_database("testdb").unwrap();

    let fields = vec![
        CreateTableField::Col("a".to_string(), ColumnType::Int, true, ParserValue::Null),
        CreateTableField::Col("b".to_string(), ColumnType::Float, false, ParserValue::Null),
        CreateTableField::Col(
            "c".to_string(),
            ColumnType::Char(10),
            false,
            ParserValue::Null,
        ),
    ];
    db_manager.create_table("t", fields).unwrap();

    let rows = vec![
        vec![
            ParserValue::Integer(1),
            ParserValue::Float(1.0),
            ParserValue::String("x".to_string()),
        ],
        vec![
            ParserValue::Integer(2),
            ParserValue::Float(3.0),
            ParserValue::String("x".to_string()),
        ],
        vec![
            ParserValue::Integer(3),
            ParserValue::Float(5.0),
            ParserValue::String("y".to_string()),
        ],
        vec![
            ParserValue::Integer(4),
            ParserValue::Null,
            ParserValue::String("y".to_string()),
        ],
    ];
    db_manager.insert("t", rows).unwrap();

    let clause = SelectClause {
        selectors: Selectors::List(vec![
            Selector::Column(TableColumn {
                table: None,
                column: "c".to_string(),
            }),
            Selector::CountAll,
            Selector::Average(TableColumn {
                table: None,
                column: "b".to_string(),
            }),
        ]),
        table: vec!["t".to_string()],
        where_clauses: vec![],
        group_by: Some(TableColumn {
            table: None,
            column: "c".to_string(),
        }),
        order_by: None,
        limit: None,
        offset: None,
    };

    let (headers, rows) = db_manager.select(clause).unwrap();
    assert_eq!(headers, vec!["c", "COUNT(*)", "AVG(b)"]);
    assert_eq!(
        rows,
        vec![
            vec!["x".to_string(), "2".to_string(), "2.00".to_string()],
            vec!["y".to_string(), "2".to_string(), "5.00".to_string()],
        ]
    );
}

#[test]
fn test_add_and_drop_index() {
    let (temp_dir, mut db_manager) = setup_test_db();

    db_manager.create_database("testdb").unwrap();
    db_manager.use_database("testdb").unwrap();

    let fields = vec![
        CreateTableField::Col("a".to_string(), ColumnType::Int, true, ParserValue::Null),
        CreateTableField::Col(
            "b".to_string(),
            ColumnType::Char(12),
            true,
            ParserValue::Null,
        ),
        CreateTableField::Col("c".to_string(), ColumnType::Float, true, ParserValue::Null),
    ];

    db_manager.create_table("tbl9", fields).unwrap();

    let rows = vec![
        vec![
            ParserValue::Integer(1),
            ParserValue::String("1".to_string()),
            ParserValue::Float(1.0),
        ],
        vec![
            ParserValue::Integer(2),
            ParserValue::String("2".to_string()),
            ParserValue::Float(2.0),
        ],
    ];
    db_manager.insert("tbl9", rows).unwrap();

    db_manager
        .execute_alter_statement(AlterStatement::AddIndex(
            "tbl9".to_string(),
            Some("idx_a".to_string()),
            vec!["a".to_string()],
        ))
        .unwrap();

    let meta = db_manager.describe_table("tbl9").unwrap();
    assert_eq!(meta.indexes.len(), 1);
    assert_eq!(meta.indexes[0].name, "idx_a");
    assert_eq!(meta.indexes[0].columns, vec!["a".to_string()]);

    let index_path = temp_dir.path().join("testdb").join("tbl9_a.idx");
    assert!(index_path.exists());

    db_manager
        .execute_alter_statement(AlterStatement::DropIndex(
            "tbl9".to_string(),
            "idx_a".to_string(),
        ))
        .unwrap();

    let meta = db_manager.describe_table("tbl9").unwrap();
    assert!(meta.indexes.is_empty());
    assert!(!index_path.exists());
}

#[test]
fn test_select_specific_columns() {
    let (_temp, mut db_manager) = setup_test_db();

    db_manager.create_database("testdb").unwrap();
    db_manager.use_database("testdb").unwrap();

    let fields = vec![
        CreateTableField::Col("a".to_string(), ColumnType::Int, true, ParserValue::Null),
        CreateTableField::Col(
            "b".to_string(),
            ColumnType::Char(10),
            true,
            ParserValue::Null,
        ),
        CreateTableField::Col("c".to_string(), ColumnType::Float, true, ParserValue::Null),
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
        CreateTableField::Col("a".to_string(), ColumnType::Int, true, ParserValue::Null),
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
        vec![
            ParserValue::Integer(1),
            ParserValue::String("a".to_string()),
        ],
        vec![
            ParserValue::Integer(2),
            ParserValue::String("b".to_string()),
        ],
        vec![
            ParserValue::Integer(3),
            ParserValue::String("c".to_string()),
        ],
        vec![
            ParserValue::Integer(4),
            ParserValue::String("d".to_string()),
        ],
        vec![
            ParserValue::Integer(5),
            ParserValue::String("e".to_string()),
        ],
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
        CreateTableField::Col("a".to_string(), ColumnType::Int, true, ParserValue::Null),
        CreateTableField::Col(
            "b".to_string(),
            ColumnType::Char(10),
            true,
            ParserValue::Null,
        ),
    ];

    db_manager.create_table("test", fields).unwrap();

    let rows = vec![
        vec![
            ParserValue::Integer(1),
            ParserValue::String("a".to_string()),
        ],
        vec![
            ParserValue::Integer(2),
            ParserValue::String("b".to_string()),
        ],
        vec![
            ParserValue::Integer(3),
            ParserValue::String("c".to_string()),
        ],
        vec![
            ParserValue::Integer(4),
            ParserValue::String("d".to_string()),
        ],
        vec![
            ParserValue::Integer(5),
            ParserValue::String("e".to_string()),
        ],
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
fn test_select_order_by_limit_offset() {
    let (_temp, mut db_manager) = setup_test_db();

    db_manager.create_database("testdb").unwrap();
    db_manager.use_database("testdb").unwrap();

    let fields = vec![
        CreateTableField::Col("a".to_string(), ColumnType::Int, true, ParserValue::Null),
        CreateTableField::Col(
            "b".to_string(),
            ColumnType::Char(10),
            true,
            ParserValue::Null,
        ),
    ];

    db_manager.create_table("test", fields).unwrap();

    let rows = vec![
        vec![
            ParserValue::Integer(1),
            ParserValue::String("one".to_string()),
        ],
        vec![
            ParserValue::Integer(2),
            ParserValue::String("two".to_string()),
        ],
        vec![
            ParserValue::Integer(3),
            ParserValue::String("three".to_string()),
        ],
        vec![
            ParserValue::Integer(4),
            ParserValue::String("four".to_string()),
        ],
        vec![
            ParserValue::Integer(5),
            ParserValue::String("five".to_string()),
        ],
    ];
    db_manager.insert("test", rows).unwrap();

    let clause = SelectClause {
        selectors: Selectors::List(vec![Selector::Column(TableColumn {
            table: None,
            column: "b".to_string(),
        })]),
        table: vec!["test".to_string()],
        where_clauses: vec![],
        group_by: None,
        order_by: Some((
            TableColumn {
                table: None,
                column: "a".to_string(),
            },
            false,
        )),
        limit: Some(2),
        offset: Some(1),
    };

    let (headers, rows) = db_manager.select(clause).unwrap();
    assert_eq!(headers, vec!["b"]);
    assert_eq!(
        rows,
        vec![vec!["four".to_string()], vec!["three".to_string()]]
    );
}

#[test]
fn test_select_order_by_offset_past_end() {
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
    db_manager
        .insert(
            "test",
            vec![
                vec![ParserValue::Integer(1)],
                vec![ParserValue::Integer(2)],
            ],
        )
        .unwrap();

    let clause = SelectClause {
        selectors: Selectors::All,
        table: vec!["test".to_string()],
        where_clauses: vec![],
        group_by: None,
        order_by: Some((
            TableColumn {
                table: None,
                column: "a".to_string(),
            },
            true,
        )),
        limit: None,
        offset: Some(5),
    };

    let (_, rows) = db_manager.select(clause).unwrap();
    assert!(rows.is_empty());
}

#[test]
fn test_update() {
    let (_temp, mut db_manager) = setup_test_db();

    db_manager.create_database("testdb").unwrap();
    db_manager.use_database("testdb").unwrap();

    let fields = vec![
        CreateTableField::Col("a".to_string(), ColumnType::Int, true, ParserValue::Null),
        CreateTableField::Col(
            "b".to_string(),
            ColumnType::Char(10),
            true,
            ParserValue::Null,
        ),
    ];

    db_manager.create_table("test", fields).unwrap();

    let rows = vec![
        vec![
            ParserValue::Integer(1),
            ParserValue::String("a".to_string()),
        ],
        vec![
            ParserValue::Integer(2),
            ParserValue::String("b".to_string()),
        ],
        vec![
            ParserValue::Integer(3),
            ParserValue::String("c".to_string()),
        ],
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
        CreateTableField::Col("a".to_string(), ColumnType::Int, true, ParserValue::Null),
        CreateTableField::Col(
            "b".to_string(),
            ColumnType::Char(10),
            true,
            ParserValue::Null,
        ),
        CreateTableField::Col("c".to_string(), ColumnType::Float, true, ParserValue::Null),
    ];

    db_manager.create_table("test", fields).unwrap();

    // Create test CSV file
    let csv_content = "1,hello,1.5\n2, world,2.5\n3,rust,3.5\n";
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
    assert_eq!(rows[1][1], " world");
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
fn test_two_table_join_basic() {
    let (_temp, mut db_manager) = setup_test_db();

    db_manager.create_database("testdb").unwrap();
    db_manager.use_database("testdb").unwrap();

    let fields1 = vec![
        CreateTableField::Col("a".to_string(), ColumnType::Int, true, ParserValue::Null),
        CreateTableField::Col(
            "b".to_string(),
            ColumnType::Char(10),
            true,
            ParserValue::Null,
        ),
    ];
    let fields2 = vec![
        CreateTableField::Col("a".to_string(), ColumnType::Int, true, ParserValue::Null),
        CreateTableField::Col(
            "c".to_string(),
            ColumnType::Char(10),
            true,
            ParserValue::Null,
        ),
    ];

    db_manager.create_table("t1", fields1).unwrap();
    db_manager.create_table("t2", fields2).unwrap();

    let rows_t1 = vec![
        vec![
            ParserValue::Integer(1),
            ParserValue::String("x".to_string()),
        ],
        vec![
            ParserValue::Integer(2),
            ParserValue::String("y".to_string()),
        ],
        vec![
            ParserValue::Integer(3),
            ParserValue::String("z".to_string()),
        ],
    ];
    let rows_t2 = vec![
        vec![
            ParserValue::Integer(2),
            ParserValue::String("p".to_string()),
        ],
        vec![
            ParserValue::Integer(3),
            ParserValue::String("q".to_string()),
        ],
        vec![
            ParserValue::Integer(4),
            ParserValue::String("r".to_string()),
        ],
    ];

    db_manager.insert("t1", rows_t1).unwrap();
    db_manager.insert("t2", rows_t2).unwrap();

    let clause = SelectClause {
        selectors: Selectors::List(vec![
            Selector::Column(TableColumn {
                table: Some("t1".to_string()),
                column: "a".to_string(),
            }),
            Selector::Column(TableColumn {
                table: Some("t1".to_string()),
                column: "b".to_string(),
            }),
            Selector::Column(TableColumn {
                table: Some("t2".to_string()),
                column: "a".to_string(),
            }),
            Selector::Column(TableColumn {
                table: Some("t2".to_string()),
                column: "c".to_string(),
            }),
        ]),
        table: vec!["t1".to_string(), "t2".to_string()],
        where_clauses: vec![WhereClause::Op(
            TableColumn {
                table: Some("t1".to_string()),
                column: "a".to_string(),
            },
            Operator::Eq,
            Expression::Column(TableColumn {
                table: Some("t2".to_string()),
                column: "a".to_string(),
            }),
        )],
        group_by: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let (headers, rows) = db_manager.select(clause).unwrap();
    assert_eq!(headers, vec!["a", "b", "a", "c"]);
    assert_eq!(
        rows,
        vec![
            vec!["2".to_string(), "y".to_string(), "2".to_string(), "p".to_string()],
            vec!["3".to_string(), "z".to_string(), "3".to_string(), "q".to_string()],
        ]
    );
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

#[test]
fn test_foreign_key_insert_update_validation() {
    let (_temp, mut db_manager) = setup_test_db();

    db_manager.create_database("testdb").unwrap();
    db_manager.use_database("testdb").unwrap();

    let parent_fields = vec![
        CreateTableField::Col("id".to_string(), ColumnType::Int, true, ParserValue::Null),
        CreateTableField::Pkey(Box::new(AlterStatement::AddPKey(
            "parent".to_string(),
            vec!["id".to_string()],
        ))),
    ];
    db_manager.create_table("parent", parent_fields).unwrap();
    db_manager
        .execute_alter_statement(AlterStatement::AddIndex(
            "parent".to_string(),
            None,
            vec!["id".to_string()],
        ))
        .unwrap();

    let child_fields = vec![
        CreateTableField::Col("id".to_string(), ColumnType::Int, true, ParserValue::Null),
        CreateTableField::Col(
            "parent_id".to_string(),
            ColumnType::Int,
            false,
            ParserValue::Null,
        ),
        CreateTableField::Fkey(Box::new(AlterStatement::AddFKey(
            "child".to_string(),
            Some("fk_parent".to_string()),
            vec!["parent_id".to_string()],
            "parent".to_string(),
            vec!["id".to_string()],
        ))),
    ];
    db_manager.create_table("child", child_fields).unwrap();

    db_manager
        .insert("parent", vec![vec![ParserValue::Integer(1)]])
        .unwrap();
    db_manager
        .insert(
            "child",
            vec![vec![ParserValue::Integer(1), ParserValue::Integer(1)]],
        )
        .unwrap();
    db_manager
        .insert(
            "child",
            vec![vec![ParserValue::Integer(2), ParserValue::Null]],
        )
        .unwrap();

    let bad_insert = db_manager.insert(
        "child",
        vec![vec![ParserValue::Integer(3), ParserValue::Integer(99)]],
    );
    assert!(matches!(
        bad_insert,
        Err(DatabaseError::ForeignKeyViolation(_))
    ));

    let updates = vec![("parent_id".to_string(), ParserValue::Integer(100))];
    let bad_update = db_manager.update("child", updates, None);
    assert!(matches!(
        bad_update,
        Err(DatabaseError::ForeignKeyViolation(_))
    ));
}

#[test]
fn test_foreign_key_add_constraint_validation() {
    let (_temp, mut db_manager) = setup_test_db();

    db_manager.create_database("testdb").unwrap();
    db_manager.use_database("testdb").unwrap();

    let parent_fields = vec![
        CreateTableField::Col("id".to_string(), ColumnType::Int, true, ParserValue::Null),
        CreateTableField::Pkey(Box::new(AlterStatement::AddPKey(
            "parent".to_string(),
            vec!["id".to_string()],
        ))),
    ];
    db_manager.create_table("parent", parent_fields).unwrap();
    db_manager
        .execute_alter_statement(AlterStatement::AddIndex(
            "parent".to_string(),
            None,
            vec!["id".to_string()],
        ))
        .unwrap();

    let child_fields = vec![
        CreateTableField::Col("id".to_string(), ColumnType::Int, true, ParserValue::Null),
        CreateTableField::Col(
            "parent_id".to_string(),
            ColumnType::Int,
            false,
            ParserValue::Null,
        ),
    ];
    db_manager.create_table("child", child_fields).unwrap();

    db_manager
        .insert("parent", vec![vec![ParserValue::Integer(1)]])
        .unwrap();
    db_manager
        .insert(
            "child",
            vec![vec![ParserValue::Integer(1), ParserValue::Integer(1)]],
        )
        .unwrap();
    db_manager
        .insert(
            "child",
            vec![vec![ParserValue::Integer(2), ParserValue::Integer(99)]],
        )
        .unwrap();

    let add_fk = db_manager.execute_alter_statement(AlterStatement::AddFKey(
        "child".to_string(),
        Some("fk_parent".to_string()),
        vec!["parent_id".to_string()],
        "parent".to_string(),
        vec!["id".to_string()],
    ));
    assert!(matches!(
        add_fk,
        Err(DatabaseError::ForeignKeyViolation(_))
    ));

    let where_clauses = vec![WhereClause::Op(
        TableColumn {
            table: None,
            column: "parent_id".to_string(),
        },
        Operator::Eq,
        Expression::Value(ParserValue::Integer(99)),
    )];
    db_manager.delete("child", Some(where_clauses)).unwrap();

    db_manager
        .execute_alter_statement(AlterStatement::AddFKey(
            "child".to_string(),
            Some("fk_parent".to_string()),
            vec!["parent_id".to_string()],
            "parent".to_string(),
            vec!["id".to_string()],
        ))
        .unwrap();
}

#[test]
fn test_foreign_key_delete_restriction() {
    let (_temp, mut db_manager) = setup_test_db();

    db_manager.create_database("testdb").unwrap();
    db_manager.use_database("testdb").unwrap();

    let parent_fields = vec![
        CreateTableField::Col("id".to_string(), ColumnType::Int, true, ParserValue::Null),
        CreateTableField::Pkey(Box::new(AlterStatement::AddPKey(
            "parent".to_string(),
            vec!["id".to_string()],
        ))),
    ];
    db_manager.create_table("parent", parent_fields).unwrap();
    db_manager
        .execute_alter_statement(AlterStatement::AddIndex(
            "parent".to_string(),
            None,
            vec!["id".to_string()],
        ))
        .unwrap();

    let child_fields = vec![
        CreateTableField::Col("id".to_string(), ColumnType::Int, true, ParserValue::Null),
        CreateTableField::Col(
            "parent_id".to_string(),
            ColumnType::Int,
            false,
            ParserValue::Null,
        ),
        CreateTableField::Fkey(Box::new(AlterStatement::AddFKey(
            "child".to_string(),
            Some("fk_parent".to_string()),
            vec!["parent_id".to_string()],
            "parent".to_string(),
            vec!["id".to_string()],
        ))),
    ];
    db_manager.create_table("child", child_fields).unwrap();

    db_manager
        .insert("parent", vec![vec![ParserValue::Integer(1)]])
        .unwrap();
    db_manager
        .insert(
            "child",
            vec![vec![ParserValue::Integer(1), ParserValue::Integer(1)]],
        )
        .unwrap();

    let delete_parent = db_manager.delete("parent", None);
    assert!(matches!(
        delete_parent,
        Err(DatabaseError::ForeignKeyViolation(_))
    ));

    let where_child = vec![WhereClause::Op(
        TableColumn {
            table: None,
            column: "id".to_string(),
        },
        Operator::Eq,
        Expression::Value(ParserValue::Integer(1)),
    )];
    db_manager.delete("child", Some(where_child)).unwrap();
    db_manager.delete("parent", None).unwrap();
}
