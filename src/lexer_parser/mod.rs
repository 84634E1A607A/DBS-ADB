mod lexer;
mod parser;

pub use lexer::{lexer, SQLToken};
pub use parser::{parser, Query, DBStatement, AlterStatement};

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
    fn test_db_stmt() {
        let query = "CREATE DATABASE test_db; DROP DATABASE test_db; SHOW DATABASES; USE test_db; SHOW TABLES; SHOW INDEXES;";
        let result = parser().parse(query);
        assert!(!result.has_errors());
        let queries = result.unwrap();
        assert_eq!(queries.len(), 6);
        assert_eq!(
            queries[0],
            Query::DBStmt(DBStatement::CreateDatabase("test_db".into()))
        );
        assert_eq!(
            queries[1],
            Query::DBStmt(DBStatement::DropDatabase("test_db".into()))
        );
        assert_eq!(queries[2], Query::DBStmt(DBStatement::ShowDatabases));
        assert_eq!(
            queries[3],
            Query::DBStmt(DBStatement::UseDatabase("test_db".into()))
        );
        assert_eq!(queries[4], Query::DBStmt(DBStatement::ShowTables));
        assert_eq!(queries[5], Query::DBStmt(DBStatement::ShowIndexes));
    }

    #[test]
    fn test_db_stmt_errors() {
        let query = "CREATE DATABASE ;";
        let result = parser().parse(query);
        assert!(result.has_errors());
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

        let result = parser().parse(query);
        assert!(!result.has_errors());
        let queries = result.unwrap();
        assert_eq!(queries.len(), 7);
        assert_eq!(
            queries[0],
            Query::AlterStmt(AlterStatement::AddIndex(
                "my_table".into(),
                Some("my_index".into()),
                vec!["col1".into(), "col2".into()]
            ))
        );
        assert_eq!(
            queries[1],
            Query::AlterStmt(AlterStatement::AddIndex(
                "my_table".into(),
                None,
                vec!["col3".into()]
            ))
        );
        assert_eq!(
            queries[2],
            Query::AlterStmt(AlterStatement::DropIndex(
                "my_table".into(),
                "my_index".into()
            ))
        );
        assert_eq!(
            queries[3],
            Query::AlterStmt(AlterStatement::DropPKey("my_table".into(), None))
        );
        assert_eq!(
            queries[4],
            Query::AlterStmt(AlterStatement::DropFKey(
                "my_table".into(),
                "fk_my_table".into()
            ))
        );
        assert_eq!(
            queries[5],
            Query::AlterStmt(AlterStatement::AddPKey(
                "my_table".into(),
                vec!["col1".into(), "col2".into()]
            ))
        );
        assert_eq!(
            queries[6],
            Query::AlterStmt(AlterStatement::AddFKey(
                "my_table".into(),
                Some("fk_my_fkey".into()),
                vec!["col1".into(), "col2".into(), "col3".into()],
                "ref_table".into(),
                vec!["ref_col1".into(), "ref_col2".into(), "ref_col3".into()]
            ))
        );
    }

    #[test]
    fn test_alter_stmt_errors() {
        let query = "ALTER TABLE my_table ADD FOREIGN KEY fk_my_fkey (col1, col2, col3) REFERENCES ref_table (ref_col1, ref_col2);";

        let result = parser().parse(query);
        assert!(result.has_errors());
    }

    #[test]
    fn test_annotation() {
        let query = "-- Leading Annotation;
CREATE DATABASE test_db; -- Trailing Annotation
-- Annotation ends here; DROP DATABASE test_db;;;;
        ";

        let result = parser().parse(query);

        result.errors().map(|e| println!("Error: {:?}", e)).count();

        assert!(!result.has_errors());
        let queries = result.unwrap();
        assert_eq!(queries.len(), 7);
        assert_eq!(
            queries[0],
            Query::Annotation("-- Leading Annotation".into())
        );
        assert_eq!(
            queries[1],
            Query::DBStmt(DBStatement::CreateDatabase("test_db".into()))
        );
        assert_eq!(
            queries[2],
            Query::Annotation("-- Trailing Annotation\n-- Annotation ends here".into())
        );
        assert_eq!(
            queries[3],
            Query::DBStmt(DBStatement::DropDatabase("test_db".into()))
        );
    }
}
