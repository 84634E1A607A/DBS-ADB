mod lexer;
mod parser;

use lexer::{KeywordEnum, SQLToken, lexer};
pub use parser::{
    AlterStatement, ColumnType, CreateTableField, DBStatement, Expression, Operator, Query,
    SelectClause, Selector, Selectors, TableColumn, TableStatement, Value, WhereClause, parser,
};

use chumsky::Parser;

pub fn parse(input: &str) -> Result<Vec<Query>, String> {
    // Run lexer
    let tokens = match lexer().parse(input).into_result() {
        Ok(t) => t,
        Err(errs) => {
            return Err(format!("Lexer errors: {:?}", errs));
        }
    };

    let ast = parser().parse(tokens.as_slice()).into_result();

    if let Err(errs) = ast {
        return Err(format!("Parser errors: {:?}", errs));
    }

    Ok(ast.unwrap())
}

#[cfg(test)]
mod tests {
    use std::vec;

    use crate::lexer_parser::parser::{SelectClause, TableColumn, WhereClause};

    use super::*;
    use chumsky::Parser;

    #[test]
    fn test_lexer_keyword_bound() {
        let query = "CREATEDATABASE";
        let result = lexer().parse(query);
        assert!(!result.has_errors());
        let tokens = result.unwrap();
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0], SQLToken::Identifier("CREATEDATABASE"));
    }

    #[test]
    fn test_lexer_number() {
        let query = "-123 45.67";
        let result = lexer().parse(query);
        assert!(!result.has_errors());
        let tokens = result.unwrap();
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[0], SQLToken::Integer(-123));
        assert_eq!(tokens[1], SQLToken::Float(45.67));

        let query = "1s";
        let result = lexer().parse(query);
        assert!(!result.has_errors());
        let tokens = result.unwrap();
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[0], SQLToken::Integer(1));
        assert_eq!(tokens[1], SQLToken::Identifier("s"));
    }

    #[test]
    fn test_lexer_annotation() {
        let query = "-- This is a comment;\nSELECT * FROM table;";
        let result = lexer().parse(query);
        assert!(!result.has_errors());
        let tokens = result.unwrap();
        assert_eq!(
            tokens,
            vec![
                SQLToken::Symbol(';'),
                SQLToken::Keyword(KeywordEnum::Select),
                SQLToken::Symbol('*'),
                SQLToken::Keyword(KeywordEnum::From),
                SQLToken::Identifier("table"),
                SQLToken::Symbol(';')
            ]
        );
    }

    #[test]
    fn test_db_stmt() {
        let query = "CREATE DATABASE test_db; DROP DATABASE test_db; SHOW DATABASES; USE test_db; SHOW TABLES; SHOW INDEXES;";
        let result = parse(query);
        assert!(result.is_ok());
        let queries = result.unwrap();
        assert_eq!(
            queries,
            vec![
                Query::DBStmt(DBStatement::CreateDatabase("test_db".into())),
                Query::DBStmt(DBStatement::DropDatabase("test_db".into())),
                Query::DBStmt(DBStatement::ShowDatabases),
                Query::DBStmt(DBStatement::UseDatabase("test_db".into())),
                Query::DBStmt(DBStatement::ShowTables),
                Query::DBStmt(DBStatement::ShowIndexes),
            ]
        );
    }

    #[test]
    fn test_db_stmt_errors() {
        let query = "CREATE DATABASE ;";
        let result = parse(query);
        assert!(result.is_err());
    }

    #[test]
    fn test_alter_stmt() {
        let query = "
            ALTER TABLE my_table ADD INDEX my_index (col1, col2);
            ALTER TABLE my_table ADD INDEX (col3);
            ALTER TABLE my_table DROP INDEX my_index;
            ALTER TABLE my_table DROP PRIMARY KEY;
            ALTER TABLE my_table DROP FOREIGN KEY fk_my_table;
            ALTER TABLE my_table ADD PRIMARY KEY (col1, col2);
            ALTER TABLE my_table ADD FOREIGN KEY fk_my_fkey (col1, col2, col3) REFERENCES ref_table (ref_col1, ref_col2, ref_col3);
            ALTER TABLE my_table ADD CONSTRAINT fk_named FOREIGN KEY (col4) REFERENCES ref_table (ref_col4);
            ";

        let result = parse(query);
        assert!(result.is_ok());
        let queries = result.unwrap();
        assert_eq!(
            queries,
            vec![
                Query::AlterStmt(AlterStatement::AddIndex(
                    "my_table".into(),
                    Some("my_index".into()),
                    vec!["col1".into(), "col2".into()]
                )),
                Query::AlterStmt(AlterStatement::AddIndex(
                    "my_table".into(),
                    None,
                    vec!["col3".into()]
                )),
                Query::AlterStmt(AlterStatement::DropIndex(
                    "my_table".into(),
                    "my_index".into()
                )),
                Query::AlterStmt(AlterStatement::DropPKey("my_table".into(), None)),
                Query::AlterStmt(AlterStatement::DropFKey(
                    "my_table".into(),
                    "fk_my_table".into()
                )),
                Query::AlterStmt(AlterStatement::AddPKey(
                    "my_table".into(),
                    vec!["col1".into(), "col2".into()]
                )),
                Query::AlterStmt(AlterStatement::AddFKey(
                    "my_table".into(),
                    Some("fk_my_fkey".into()),
                    vec!["col1".into(), "col2".into(), "col3".into()],
                    "ref_table".into(),
                    vec!["ref_col1".into(), "ref_col2".into(), "ref_col3".into()]
                )),
                Query::AlterStmt(AlterStatement::AddFKey(
                    "my_table".into(),
                    Some("fk_named".into()),
                    vec!["col4".into()],
                    "ref_table".into(),
                    vec!["ref_col4".into()]
                )),
            ]
        );
    }

    #[test]
    fn test_alter_stmt_errors() {
        let query = "ALTER TABLE my_table ADD FOREIGN KEY fk_my_fkey (col1, col2, col3) REFERENCES ref_table (ref_col1, ref_col2);";

        let result = parse(query);
        assert!(result.is_err());
    }

    #[test]
    fn test_annotation_null() {
        let query = "-- Leading Annotation;
CREATE DATABASE test_db; -- Trailing Annotation
-- Annotation ends here; DROP DATABASE test_db;;";

        let result = parse(query);
        dbg!(&result);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            vec![
                Query::Null,
                Query::DBStmt(DBStatement::CreateDatabase("test_db".into())),
                Query::Null,
                Query::DBStmt(DBStatement::DropDatabase("test_db".into())),
                Query::Null,
            ]
        );
    }

    #[test]
    fn test_table_stmt_misc() {
        let query = "
        DROP TABLE my_table;
        DESC my_table;
        INSERT INTO my_table VALUES (1, 'value'), ('other', 4.2);
        LOAD DATA INFILE 'data.txt' INTO TABLE my_table FIELDS TERMINATED BY ',';
        ";

        let result = parse(query);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            vec![
                Query::TableStmt(parser::TableStatement::DropTable("my_table".into())),
                Query::TableStmt(parser::TableStatement::DescribeTable("my_table".into())),
                Query::TableStmt(parser::TableStatement::InsertInto(
                    "my_table".into(),
                    vec![
                        vec![
                            parser::Value::Integer(1),
                            parser::Value::String("value".into())
                        ],
                        vec![
                            parser::Value::String("other".into()),
                            parser::Value::Float(4.2)
                        ]
                    ]
                )),
                Query::TableStmt(parser::TableStatement::LoadDataInfile(
                    "data.txt".into(),
                    "my_table".into(),
                    ','
                ))
            ]
        );
    }

    #[test]
    fn test_table_stmt_where() {
        let query = "
        DELETE FROM my_table WHERE col1 = 1 AND col2 = 'value';
        UPDATE my_table SET col1 = 2, col2 = 'new_value' WHERE col2 = col3;
        ";

        let result = parse(query);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            vec![
                Query::TableStmt(parser::TableStatement::DeleteFrom(
                    "my_table".into(),
                    Some(vec![
                        WhereClause::Op(
                            TableColumn {
                                table: None,
                                column: "col1".into()
                            },
                            parser::Operator::Eq,
                            parser::Expression::Value(parser::Value::Integer(1))
                        ),
                        WhereClause::Op(
                            TableColumn {
                                table: None,
                                column: "col2".into()
                            },
                            parser::Operator::Eq,
                            parser::Expression::Value(parser::Value::String("value".into()))
                        )
                    ])
                )),
                Query::TableStmt(parser::TableStatement::Update(
                    "my_table".into(),
                    vec![
                        ("col1".into(), parser::Value::Integer(2)),
                        ("col2".into(), parser::Value::String("new_value".into())),
                    ],
                    Some(vec![WhereClause::Op(
                        TableColumn {
                            table: None,
                            column: "col2".into()
                        },
                        parser::Operator::Eq,
                        parser::Expression::Column(TableColumn {
                            table: None,
                            column: "col3".into()
                        })
                    )])
                ))
            ]
        )
    }

    #[test]
    fn test_table_stmt_select() {
        let query = "
        SELECT * FROM my_table;
        SELECT col1, col2 FROM my_table WHERE col3 IN (10, 20, 30) ORDER BY col1 DESC LIMIT 5 OFFSET 10;
        ";

        let result = parse(query);
        dbg!(&result);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            vec![
                Query::TableStmt(parser::TableStatement::Select(SelectClause {
                    table: vec!["my_table".into()],
                    where_clauses: vec![],
                    selectors: parser::Selectors::All,
                    limit: None,
                    offset: None,
                    order_by: None,
                    group_by: None
                })),
                Query::TableStmt(parser::TableStatement::Select(SelectClause {
                    table: vec!["my_table".into()],
                    where_clauses: vec![WhereClause::In(
                        TableColumn {
                            table: None,
                            column: "col3".into()
                        },
                        vec![
                            parser::Value::Integer(10),
                            parser::Value::Integer(20),
                            parser::Value::Integer(30)
                        ]
                    )],
                    selectors: parser::Selectors::List(vec![
                        parser::Selector::Column(TableColumn {
                            table: None,
                            column: "col1".into()
                        }),
                        parser::Selector::Column(TableColumn {
                            table: None,
                            column: "col2".into()
                        })
                    ]),
                    limit: Some(5),
                    offset: Some(10),
                    order_by: Some((
                        TableColumn {
                            table: None,
                            column: "col1".into()
                        },
                        false
                    )),
                    group_by: None
                }))
            ]
        )
    }

    #[test]
    fn test_load_table_delimiter() {
        let query = "LOAD DATA INFILE 'data.txt' INTO TABLE my_table FIELDS TERMINATED BY 'abc';";
        let result = parse(query);
        assert!(result.is_err());
    }

    #[test]
    fn test_table_stmt_create() {
        let query = "CREATE TABLE my_table (
            id INT NOT NULL DEFAULT 0,
            name VARCHAR(100) DEFAULT 'unknown',
            score FLOAT,
            PRIMARY KEY pkey (id),
            FOREIGN KEY fk_name (name) REFERENCES ref_table (ref_name)
        );";

        let result = parse(query);
        dbg!(&result);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            vec![Query::TableStmt(parser::TableStatement::CreateTable(
                "my_table".into(),
                vec![
                    parser::CreateTableField::Col(
                        "id".into(),
                        parser::ColumnType::Int,
                        true,
                        parser::Value::Integer(0)
                    ),
                    parser::CreateTableField::Col(
                        "name".into(),
                        parser::ColumnType::Char(100),
                        false,
                        parser::Value::String("unknown".into())
                    ),
                    parser::CreateTableField::Col(
                        "score".into(),
                        parser::ColumnType::Float,
                        false,
                        parser::Value::Null
                    ),
                    parser::CreateTableField::Pkey(Box::new(AlterStatement::AddPKey(
                        String::default(),
                        vec!["id".into()]
                    ))),
                    parser::CreateTableField::Fkey(Box::new(AlterStatement::AddFKey(
                        String::default(),
                        Some("fk_name".into()),
                        vec!["name".into()],
                        "ref_table".into(),
                        vec!["ref_name".into()]
                    ))),
                ]
            ))]
        );
    }
}
