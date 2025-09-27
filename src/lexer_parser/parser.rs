use chumsky::{input::Emitter, prelude::*, text::ascii::ident};

use crate::lexer_parser::{SQLToken as T, KeywordEnum as K};

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

pub fn parser<'a>() -> impl Parser<'a, &'a [T<'a>], Vec<Query>, extra::Err<Rich<'a, T<'a>>>> {
    fn identifier<'a>() -> impl Parser<'a, &'a [T<'a>], &'a str, extra::Err<Rich<'a, T<'a>>>> {
        select! { T::Identifier(name) => name }
    }

    fn db_statement<'a>() -> impl Parser<'a, &'a [T<'a>], DBStatement, extra::Err<Rich<'a, T<'a>>>>
    {
        let create_db = just(T::Keyword(K::Create))
            .ignore_then(just(T::Keyword(K::Database)))
            .ignore_then(identifier())
            .map(|db_name| DBStatement::CreateDatabase(db_name.into()));

        let drop_db = just(T::Keyword(K::Drop))
            .ignore_then(just(T::Keyword(K::Database)))
            .ignore_then(identifier())
            .map(|db_name| DBStatement::DropDatabase(db_name.into()));

        let show_dbs = just(T::Keyword(K::Show))
            .ignore_then(just(T::Keyword(K::Databases)))
            .to(DBStatement::ShowDatabases);

        let use_db = just(T::Keyword(K::Use))
            .ignore_then(identifier())
            .map(|db_name| DBStatement::UseDatabase(db_name.into()));

        let show_tables = just(T::Keyword(K::Show))
            .ignore_then(just(T::Keyword(K::Tables)))
            .to(DBStatement::ShowTables);

        let show_indexes = just(T::Keyword(K::Show))
            .ignore_then(just(T::Keyword(K::Indexes)))
            .to(DBStatement::ShowIndexes);

        choice((
            create_db,
            drop_db,
            show_dbs,
            use_db,
            show_tables,
            show_indexes,
        ))
    }

    fn alter_statement<'a>()
    -> impl Parser<'a, &'a [T<'a>], AlterStatement, extra::Err<Rich<'a, T<'a>>>> {
        // ALTER TABLE Identifier
        let alter_table = just(T::Keyword(K::Alter))
            .ignore_then(just(T::Keyword(K::Table)))
            .ignore_then(identifier()).boxed();

        let add_index = alter_table.clone()
            // ADD INDEX Identifier?
            .then(
                just(T::Keyword(K::Add))
                    .ignore_then(just(T::Keyword(K::Indexes)))
                    .ignore_then(identifier().or_not()),
            )
            // ( field_list )
            .then(
                identifier()
                    .separated_by(just(T::Symbol(',')))
                    .collect()
                    .delimited_by(just(T::Symbol('(')), just(T::Symbol(')')))
                    .boxed(),
            )
            .map(
                |((table_ident, index_name), fields): ((&str, Option<&str>), Vec<&str>)| {
                    AlterStatement::AddIndex(
                        table_ident.into(),
                        index_name.map(|s| s.into()),
                        fields.into_iter().map(|s| s.into()).collect(),
                    )
                },
            );

        let drop_index = alter_table.clone()
            // DROP INDEX Identifier
            .then(
                just(T::Keyword(K::Drop))
                    .ignore_then(just(T::Keyword(K::Indexes)))
                    .ignore_then(identifier()),
            )
            .map(|(table_ident, index_name): (&str, &str)| {
                AlterStatement::DropIndex(table_ident.into(), index_name.into())
            });

        let drop_pkey = alter_table.clone()
            // DROP PRIMARY KEY
            .then(
                just(T::Keyword(K::Drop))
                    .ignore_then(just(T::Keyword(K::Primary)))
                    .ignore_then(just(T::Keyword(K::Key)))
                    .ignore_then(identifier()).or_not(),
            )
            .map(|(table_ident, pkey_name): (&str, Option<&str>)| {
                AlterStatement::DropPKey(table_ident.into(), pkey_name.map(|s| s.into()))
            });

        let drop_fkey = alter_table.clone()
            // DROP FOREIGN KEY Identifier
            .then(
                just(T::Keyword(K::Drop))
                    .ignore_then(just(T::Keyword(K::Foreign)))
                    .ignore_then(just(T::Keyword(K::Key)))
                    .ignore_then(identifier()),
            )
            .map(|(table_ident, fkey_name): (&str, &str)| {
                AlterStatement::DropFKey(table_ident.into(), fkey_name.into())
            });

        let add_pkey = alter_table.clone()
            // ADD PRIMARY KEY
            .then(
                just(T::Keyword(K::Add))
                    .ignore_then(just(T::Keyword(K::Primary)))
                    .ignore_then(just(T::Keyword(K::Key)))
                    // ( field_list )
                    .ignore_then(
                        identifier()
                            .separated_by(just(T::Symbol(',')))
                            .collect()
                            .delimited_by(just(T::Symbol('(')), just(T::Symbol(')'))),
                    ).boxed(),
            )
            .map(|(table_ident, fields): (&str, Vec<&str>)| {
                AlterStatement::AddPKey(
                    table_ident.into(),
                    fields.into_iter().map(|s| s.into()).collect(),
                )
            });

        let add_fkey = alter_table
            // ADD FOREIGN KEY Identifier?
            .then(
                just(T::Keyword(K::Add))
                    .ignore_then(just(T::Keyword(K::Foreign)))
                    .ignore_then(just(T::Keyword(K::Key)))
                    .ignore_then(identifier().or_not()),
            )
            // ( field_list )
            .then(
                identifier()
                    .separated_by(just(T::Symbol(',')))
                    .collect()
                    .delimited_by(just(T::Symbol('(')), just(T::Symbol(')'))),
            )
            // REFERENCES Identifier
            .then(
                just(T::Keyword(K::References))
                    .ignore_then(identifier()),
            )
            // ( field_list )
            .then(
                identifier()
                    .separated_by(just(T::Symbol(',')))
                    .collect()
                    .delimited_by(just(T::Symbol('(')), just(T::Symbol(')'))),
            )
            .validate(|((((table_ident, fkey_name), fields), ref_table), ref_fields): ((((&str, Option<&str>), Vec<&str>), &str), Vec<&str>),
                _map, emitter: &mut Emitter<Rich<T<'a>>>| {
                // Check that the number of fields matches the number of reference fields
                if fields.len() != ref_fields.len() {
                    // Return a chumsky parse error instead of panicking
                    emitter.emit(Rich::custom(
                        _map.span(),
                        format!(
                            "number of fields ({}) does not match number of reference fields ({})",
                            fields.len(),
                            ref_fields.len()
                        ),
                    ))
                }

                AlterStatement::AddFKey(
                    table_ident.into(),
                    fkey_name.map(|s| s.into()),
                    fields.into_iter().map(|s| s.into()).collect(),
                    ref_table.into(),
                    ref_fields.into_iter().map(|s| s.into()).collect(),
                )
            });

        choice((
            add_index, drop_index, drop_pkey, drop_fkey, add_pkey, add_fkey,
        ))
    }

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

    choice((
        db_statement().map(Query::DBStmt),
        alter_statement().map(Query::AlterStmt),
        //     annotation(),
        //     null_statement(),
    ))
    .then_ignore(just(T::Symbol(';')))
    .repeated()
    .collect()
}
