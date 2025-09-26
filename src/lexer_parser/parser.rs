use chumsky::{input::Emitter, prelude::*, text::{ascii::ident}};

use crate::lexer_parser::{lexer, SQLToken};

#[derive(Debug, Clone, PartialEq)]
pub enum DBStatement {
    // CREATE DATABASE Identifier
    CreateDatabase(String),

    // DROP DATABASE Identifier
    DropDatabase(String),

    // SHOW DATABASES
    ShowDatabases,

    // USE Identifier
    UseDatabase(String),

    // SHOW TABLES
    ShowTables,

    // SHOW INDEXES
    ShowIndexes,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Number(f64),
    Integer(i64),
    String(String),
    Null,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Operator {
    Eq,
    Ne,
    Gt,
    Lt,
    Ge,
    Le,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ValueList {
    pub values: Vec<Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableColumn {
    pub table: Option<String>,
    pub column: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    Value(Value),
    Column(TableColumn),
}

#[derive(Debug, Clone, PartialEq)]
pub enum WhereClause {
    Op(TableColumn, Operator, Expression),
    OpSubClause(TableColumn, Operator, Box<SelectClause>),
    Null(TableColumn),
    NotNull(TableColumn),
    In(TableColumn, Vec<Value>),
    InSubClause(TableColumn, Box<SelectClause>),
    Like(TableColumn, String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Selector {
    Column(TableColumn),
    Count(TableColumn),
    CountAll,
    Average(TableColumn),
    Max(TableColumn),
    Min(TableColumn),
    Sum(TableColumn),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Selectors {
    All,
    List(Vec<Selector>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectClause {
    selectors: Selectors,
    table: String,
    where_clauses: Vec<WhereClause>,
    group_by: Option<TableColumn>,
    order_by: Option<(TableColumn, bool)>, // bool: true for ASC, false for DESC
    limit: Option<usize>,
    offset: Option<usize>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableStatement {
    // CREATE TABLE Identifier ( field_list )
    CreateTable(String, Vec<String>),

    // DROP TABLE Identifier
    DropTable(String),

    // DESC Identifier
    DescribeTable(String),

    // LOAD DATA INFILE 'file_path' INTO TABLE Identifier FIELDS TERMINATED BY 'delimiter'
    LoadDataInfile(String, String, char),

    // INSERT INTO Identifier VALUES value_lists
    InsertInto(String, Vec<ValueList>),

    // DELETE FROM Identifier
    DeleteFrom(String, Option<WhereClause>),

    // UPDATE Identifier SET set_clause where_clause?
    Update(String, Vec<(String, Value)>, Option<WhereClause>),

    // select_clause
    Select(SelectClause),
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterStatement {
    // ALTER TABLE Identifier ADD INDEX Identifier? ( field_list )
    AddIndex(String, Option<String>, Vec<String>),

    // ALTER TABLE Identifier DROP INDEX Identifier
    DropIndex(String, String),

    // ALTER TABLE Identifier DROP PRIMARY KEY Identifier?
    DropPKey(String, Option<String>),

    // ALTER TABLE Identifier DROP FOREIGN KEY Identifier
    DropFKey(String, String),

    // ALTER TABLE Identifier ADD PRIMARY KEY ( field_list )
    AddPKey(String, Vec<String>),

    // ALTER TABLE Identifier ADD FOREIGN KEY Identifier? ( field_list ) REFERENCES Identifier ( field_list )
    AddFKey(String, Option<String>, Vec<String>, String, Vec<String>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Query {
    DBStmt(DBStatement),
    TableStmt(TableStatement),
    AlterStmt(AlterStatement),
    Annotation(String),
    Null,
}

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

pub fn parser<'a>() -> impl Parser<'a, &'a [SQLToken<'a>], Vec<Query>, extra::Err<Rich<'a, SQLToken<'a>>>> {
    // fn db_statement<'a>() -> impl Parser<'a, &'a str, DBStatement, extra::Err<Rich<'a, char>>> {
    //     let create_db = just("CREATE")
    //         .padded()
    //         .ignore_then(just("DATABASE").padded())
    //         .ignore_then(
    //             ident()
    //                 .map(|s: &str| DBStatement::CreateDatabase(s.into()))
    //                 .padded(),
    //         );

    //     let drop_db = just("DROP")
    //         .padded()
    //         .ignore_then(just("DATABASE").padded())
    //         .ignore_then(
    //             ident()
    //                 .map(|s: &str| DBStatement::DropDatabase(s.into()))
    //                 .padded(),
    //         );

    //     let show_dbs = just("SHOW")
    //         .padded()
    //         .ignore_then(just("DATABASES"))
    //         .to(DBStatement::ShowDatabases)
    //         .padded();

    //     let use_db = just("USE").ignore_then(
    //         ident()
    //             .map(|s: &str| DBStatement::UseDatabase(s.into()))
    //             .padded(),
    //     );

    //     let show_tables = just("SHOW")
    //         .padded()
    //         .ignore_then(just("TABLES"))
    //         .to(DBStatement::ShowTables)
    //         .padded();

    //     let show_indexes = just("SHOW")
    //         .padded()
    //         .ignore_then(just("INDEXES"))
    //         .to(DBStatement::ShowIndexes)
    //         .padded();

    //     choice((
    //         create_db,
    //         drop_db,
    //         show_dbs,
    //         use_db,
    //         show_tables,
    //         show_indexes,
    //     ))
    // }

    // fn alter_statement<'a>() -> impl Parser<'a, &'a str, AlterStatement, extra::Err<Rich<'a, char>>>
    // {
    //     // ALTER TABLE Identifier
    //     let alter_table = just("ALTER")
    //         .padded()
    //         .ignore_then(just("TABLE").padded())
    //         .ignore_then(ident().padded());

    //     let add_index = alter_table
    //         // ADD INDEX Identifier?
    //         .then(
    //             just("ADD")
    //                 .padded()
    //                 .ignore_then(just("INDEX").padded())
    //                 .ignore_then(ident().padded().or_not()),
    //         )
    //         // ( field_list )
    //         .then(
    //             ident()
    //                 .padded()
    //                 .separated_by(just(',').padded())
    //                 .collect()
    //                 .delimited_by(just('(').padded(), just(')').padded()),
    //         )
    //         .map(
    //             |((table_ident, index_name), fields): ((&str, Option<&str>), Vec<&str>)| {
    //                 AlterStatement::AddIndex(
    //                     table_ident.into(),
    //                     index_name.map(|s| s.into()),
    //                     fields.into_iter().map(|s| s.into()).collect(),
    //                 )
    //             },
    //         );

    //     let drop_index = alter_table
    //         // DROP INDEX Identifier
    //         .then(
    //             just("DROP")
    //                 .padded()
    //                 .ignore_then(just("INDEX").padded())
    //                 .ignore_then(ident().padded()),
    //         )
    //         .map(|(table_ident, index_name): (&str, &str)| {
    //             AlterStatement::DropIndex(table_ident.into(), index_name.into())
    //         });

    //     let drop_pkey = alter_table
    //         // DROP PRIMARY KEY Identifier?
    //         .then(
    //             just("DROP")
    //                 .padded()
    //                 .ignore_then(just("PRIMARY").padded())
    //                 .ignore_then(just("KEY").padded())
    //                 .ignore_then(ident().padded().or_not()),
    //         )
    //         .map(|(table_ident, pkey_name): (&str, Option<&str>)| {
    //             AlterStatement::DropPKey(table_ident.into(), pkey_name.map(|s| s.into()))
    //         });

    //     let drop_fkey = alter_table
    //         // DROP FOREIGN KEY Identifier
    //         .then(
    //             just("DROP")
    //                 .padded()
    //                 .ignore_then(just("FOREIGN").padded())
    //                 .ignore_then(just("KEY").padded())
    //                 .ignore_then(ident().padded()),
    //         )
    //         .map(|(table_ident, fkey_name): (&str, &str)| {
    //             AlterStatement::DropFKey(table_ident.into(), fkey_name.into())
    //         });

    //     let add_pkey = alter_table
    //         // ADD PRIMARY KEY
    //         .then(
    //             just("ADD")
    //                 .padded()
    //                 .ignore_then(just("PRIMARY").padded())
    //                 .ignore_then(just("KEY").padded())
    //                 // ( field_list )
    //                 .ignore_then(
    //                     ident()
    //                         .padded()
    //                         .separated_by(just(',').padded())
    //                         .collect()
    //                         .delimited_by(just('(').padded(), just(')').padded()),
    //                 ),
    //         )
    //         .map(|(table_ident, fields): (&str, Vec<&str>)| {
    //             AlterStatement::AddPKey(
    //                 table_ident.into(),
    //                 fields.into_iter().map(|s| s.into()).collect(),
    //             )
    //         });

    //     let add_fkey = alter_table
    //         // ADD FOREIGN KEY Identifier?
    //         .then(just("ADD").padded().ignore_then(just("FOREIGN").padded()).ignore_then(just("KEY").padded()).ignore_then(ident().padded().or_not()))
    //         // ( field_list )
    //         .then(ident().padded().separated_by(just(',').padded()).collect().delimited_by(just('(').padded(), just(')').padded()))
    //         // REFERENCES Identifier
    //         .then(just("REFERENCES").padded().ignore_then(ident().padded()))
    //         // ( field_list )
    //         .then(ident().padded().separated_by(just(',').padded()).collect().delimited_by(just('(').padded(), just(')').padded()))
    //         .validate(|((((table_ident, fkey_name), fields), ref_table), ref_fields): ((((&str, Option<&str>), Vec<&str>), &str), Vec<&str>),
    //             _map, emitter: &mut Emitter<Rich<char>>| {
    //             // Check that the number of fields matches the number of reference fields
    //             if fields.len() != ref_fields.len() {
    //                 // Return a chumsky parse error instead of panicking
    //                 emitter.emit(Rich::custom(
    //                     _map.span(),
    //                     format!(
    //                         "number of fields ({}) does not match number of reference fields ({})",
    //                         fields.len(),
    //                         ref_fields.len()
    //                     ),
    //                 ))
    //             }

    //             AlterStatement::AddFKey(
    //                 table_ident.into(),
    //                 fkey_name.map(|s| s.into()),
    //                 fields.into_iter().map(|s| s.into()).collect(),
    //                 ref_table.into(),
    //                 ref_fields.into_iter().map(|s| s.into()).collect(),
    //             )
    //         });

    //     choice((
    //         add_index, drop_index, drop_pkey, drop_fkey, add_pkey, add_fkey,
    //     ))
    // }

    // fn annotation<'a>() -> impl Parser<'a, &'a str, Query, extra::Err<Rich<'a, char>>> {
    //     just("--").ignore_then(none_of(";").repeated()).to_slice().map(|s: &str| {
    //         Query::Annotation(s.trim().to_string())
    //     })
    // }

    // fn null_statement<'a>() -> impl Parser<'a, &'a str, Query, extra::Err<Rich<'a, char>>> {
    //     just("").to(Query::Null)
    // }

    // fn operator<'a>() -> impl Parser<'a, &'a str, Operator, extra::Err<Rich<'a, char>>> {
    //     choice((
    //         just("=").to(Operator::Eq),
    //         just("<>").to(Operator::Ne),
    //         just(">").to(Operator::Gt),
    //         just("<").to(Operator::Lt),
    //         just(">=").to(Operator::Ge),
    //         just("<=").to(Operator::Le),
    //     ))
    //     .padded()
    // }

    // // fn table_column<'a>() -> impl Parser<'a, &'a str, TableColumn, extra::Err<Rich<'a, char>>> {
    //     // ident()
    //     //     .then(just('.').padded().ignore_then(ident()).or_not())
    //     //     .map(|(first, second): (&str, Option<&str>)| {
    //     //         if let Some(col) = second {
    //     //             TableColumn {
    //     //                 table: Some(first.into()),
    //     //                 column: col.into(),
    //     //             }
    //     //         } else {
    //     //             TableColumn {
    //     //                 table: None,
    //     //                 column: first.into(),
    //     //             }
    //     //         }
    //     //     })
    //     //     .padded()
    // // }

    // choice((
    //     db_statement().map(Query::DBStmt),
    //     alter_statement().map(Query::AlterStmt),
    //     annotation(),
    //     null_statement(),
    // )).then_ignore(just(';')).padded()
    //     .repeated().collect()
}
