mod lexer;
mod parser;

use lexer::{KeywordEnum, SQLToken, lexer};
pub use parser::{AlterStatement, DBStatement, Query, parser};

use chumsky::Parser;

pub fn parse<'a>(input: &'a str) -> Result<Vec<Query>, String> {
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
                SQLToken::Comment("-- This is a comment"),
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
-- Annotation ends here; DROP DATABASE test_db;;;;";

        let result = parse(query);
        dbg!(&result);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec![
            Query::Annotation("-- Leading Annotation".into()),
            Query::DBStmt(DBStatement::CreateDatabase("test_db".into())),
            Query::Annotation("-- Trailing Annotation\n-- Annotation ends here".into()),
            Query::DBStmt(DBStatement::DropDatabase("test_db".into())),
            Query::Null,
            Query::Null,
            Query::Null,
        ]);
    }
}
