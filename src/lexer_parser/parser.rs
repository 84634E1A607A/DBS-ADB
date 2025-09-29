use chumsky::{input::Emitter, prelude::*};

use crate::lexer_parser::{KeywordEnum as K, SQLToken as T};

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
    Float(f64),
    Integer(i64),
    String(String),
    Null,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnType {
    Int,
    Float,
    Char(usize),
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
    // OpSubClause(TableColumn, Operator, Box<SelectClause>),
    Null(TableColumn),
    NotNull(TableColumn),
    In(TableColumn, Vec<Value>),
    // InSubClause(TableColumn, Box<SelectClause>),
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
    pub selectors: Selectors,
    pub table: Vec<String>,
    pub where_clauses: Vec<WhereClause>,
    pub group_by: Option<TableColumn>,
    pub order_by: Option<(TableColumn, bool)>, // bool: true for ASC, false for DESC
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CreateTableField {
    Col(String, ColumnType, bool, Value),
    Pkey(Box<AlterStatement>),
    Fkey(Box<AlterStatement>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableStatement {
    // CREATE TABLE Identifier ( field_list )
    CreateTable(String, Vec<CreateTableField>),

    // DROP TABLE Identifier
    DropTable(String),

    // DESC Identifier
    DescribeTable(String),

    // LOAD DATA INFILE 'file_path' INTO TABLE Identifier FIELDS TERMINATED BY 'delimiter'
    LoadDataInfile(String, String, char),

    // INSERT INTO Identifier VALUES value_lists
    InsertInto(String, Vec<Vec<Value>>),

    // DELETE FROM Identifier
    DeleteFrom(String, Option<Vec<WhereClause>>),

    // UPDATE Identifier SET set_clause where_clause?
    Update(String, Vec<(String, Value)>, Option<Vec<WhereClause>>),

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
    Null,
}

#[allow(clippy::type_complexity)]
pub fn parser<'a>() -> impl Parser<'a, &'a [T<'a>], Vec<Query>, extra::Err<Rich<'a, T<'a>>>> {
    fn identifier<'a>() -> impl Parser<'a, &'a [T<'a>], &'a str, extra::Err<Rich<'a, T<'a>>>> {
        select! { T::Identifier(name) => name }
    }

    fn db_statement<'a>() -> impl Parser<'a, &'a [T<'a>], DBStatement, extra::Err<Rich<'a, T<'a>>>>
    {
        let create_db = just([T::Keyword(K::Create), T::Keyword(K::Database)])
            .ignore_then(identifier())
            .map(|db_name| DBStatement::CreateDatabase(db_name.into()));

        let drop_db = just([T::Keyword(K::Drop), T::Keyword(K::Database)])
            .ignore_then(identifier())
            .map(|db_name| DBStatement::DropDatabase(db_name.into()));

        let show_dbs =
            just([T::Keyword(K::Show), T::Keyword(K::Databases)]).to(DBStatement::ShowDatabases);

        let use_db = just(T::Keyword(K::Use))
            .ignore_then(identifier())
            .map(|db_name| DBStatement::UseDatabase(db_name.into()));

        let show_tables =
            just([T::Keyword(K::Show), T::Keyword(K::Tables)]).to(DBStatement::ShowTables);

        let show_indexes =
            just([T::Keyword(K::Show), T::Keyword(K::Indexes)]).to(DBStatement::ShowIndexes);

        choice((
            create_db,
            drop_db,
            show_dbs,
            use_db,
            show_tables,
            show_indexes,
        ))
        .boxed()
    }

    fn alter_statement<'a>()
    -> impl Parser<'a, &'a [T<'a>], AlterStatement, extra::Err<Rich<'a, T<'a>>>> {
        // ALTER TABLE Identifier
        let alter_table = just([T::Keyword(K::Alter), T::Keyword(K::Table)])
            .ignore_then(identifier())
            .boxed();

        let add_index = alter_table
            .clone()
            // ADD INDEX Identifier?
            .then(
                just([T::Keyword(K::Add), T::Keyword(K::Index)]).ignore_then(identifier().or_not()),
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

        let drop_index = alter_table
            .clone()
            // DROP INDEX Identifier
            .then(just([T::Keyword(K::Drop), T::Keyword(K::Index)]).ignore_then(identifier()))
            .map(|(table_ident, index_name): (&str, &str)| {
                AlterStatement::DropIndex(table_ident.into(), index_name.into())
            });

        let drop_pkey = alter_table
            .clone()
            // DROP PRIMARY KEY
            .then(
                just([
                    T::Keyword(K::Drop),
                    T::Keyword(K::Primary),
                    T::Keyword(K::Key),
                ])
                .ignore_then(identifier().or_not()),
            )
            .map(|(table_ident, pkey_name): (&str, Option<&str>)| {
                AlterStatement::DropPKey(table_ident.into(), pkey_name.map(|s| s.into()))
            });

        let drop_fkey = alter_table
            .clone()
            // DROP FOREIGN KEY Identifier
            .then(
                just([
                    T::Keyword(K::Drop),
                    T::Keyword(K::Foreign),
                    T::Keyword(K::Key),
                ])
                .ignore_then(identifier()),
            )
            .map(|(table_ident, fkey_name): (&str, &str)| {
                AlterStatement::DropFKey(table_ident.into(), fkey_name.into())
            });

        let add_pkey = alter_table
            .clone()
            // ADD PRIMARY KEY
            .then(
                just([
                    T::Keyword(K::Add),
                    T::Keyword(K::Primary),
                    T::Keyword(K::Key),
                ])
                // ( field_list )
                .ignore_then(
                    identifier()
                        .separated_by(just(T::Symbol(',')))
                        .collect()
                        .delimited_by(just(T::Symbol('(')), just(T::Symbol(')'))),
                )
                .boxed(),
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
                just([
                    T::Keyword(K::Add),
                    T::Keyword(K::Foreign),
                    T::Keyword(K::Key),
                ])
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
            // REFERENCES Identifier
            .then(just(T::Keyword(K::References)).ignore_then(identifier()))
            // ( field_list )
            .then(
                identifier()
                    .separated_by(just(T::Symbol(',')))
                    .collect()
                    .delimited_by(just(T::Symbol('(')), just(T::Symbol(')')))
                    .boxed(),
            )
            .validate(
                |((((table_ident, fkey_name), fields), ref_table), ref_fields): (
                    (((&str, Option<&str>), Vec<&str>), &str),
                    Vec<&str>,
                ),
                 _map,
                 emitter: &mut Emitter<Rich<T<'a>>>| {
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
                },
            );

        choice((
            add_index, drop_index, drop_pkey, drop_fkey, add_pkey, add_fkey,
        ))
        .boxed()
    }

    fn null_statement<'a>() -> impl Parser<'a, &'a [T<'a>], Query, extra::Err<Rich<'a, T<'a>>>> {
        just([]).or_not().to(Query::Null)
    }

    fn table_statement<'a>()
    -> impl Parser<'a, &'a [T<'a>], TableStatement, extra::Err<Rich<'a, T<'a>>>> {
        let operator = choice((
            just(T::Symbol('=')).to(Operator::Eq),
            just([T::Symbol('<'), T::Symbol('>')]).to(Operator::Ne),
            just(T::Symbol('>')).to(Operator::Gt),
            just(T::Symbol('<')).to(Operator::Lt),
            just([T::Symbol('>'), T::Symbol('=')]).to(Operator::Ge),
            just([T::Symbol('<'), T::Symbol('=')]).to(Operator::Le),
        ))
        .boxed();

        let table_column = identifier()
            .then_ignore(just(T::Symbol('.')))
            .or_not()
            .then(identifier())
            .map(|(table, col)| TableColumn {
                table: table.map(|s| s.into()),
                column: col.into(),
            })
            .boxed();

        let value = select! {
            T::Integer(i) => Value::Integer(i),
            T::Float(f) => Value::Float(f),
            T::String(s) => Value::String(s.into()),
            T::Keyword(K::Null) => Value::Null,
        };

        let value_list = value
            .separated_by(just(T::Symbol(',')))
            .collect::<Vec<Value>>()
            .delimited_by(just(T::Symbol('(')), just(T::Symbol(')')))
            .boxed();

        let value_lists = value_list
            .clone()
            .separated_by(just(T::Symbol(',')))
            .collect();

        // let type_ = choice((
        //     just(T::Keyword(K::Int)).to(ColumnType::Int),
        //     just(T::Keyword(K::Float)).to(ColumnType::Float),
        //     just(T::Keyword(K::Varchar))
        //         .ignore_then(
        //             select! { T::Integer(i) => i as usize }
        //                 .delimited_by(just(T::Symbol('(')), just(T::Symbol(')'))),
        //         )
        //         .map(ColumnType::Char),
        // ))
        // .boxed();

        let order = choice((
            just(T::Keyword(K::Asc)).to(true),
            just(T::Keyword(K::Desc)).to(false),
        ));

        let expression = choice((
            value.map(Expression::Value),
            table_column.clone().map(Expression::Column),
        ))
        .boxed();

        let where_and_clause = {
            // column operator expression
            let op_expr = table_column
                .clone()
                .then(operator.clone())
                .then(expression.clone())
                .map(|((col, op), expr)| WhereClause::Op(col, op, expr));

            // column IS NULL
            let is_null = table_column
                .clone()
                .then_ignore(just([T::Keyword(K::Is), T::Keyword(K::Null)]))
                .map(WhereClause::Null);

            // column IS NOT NULL
            let not_null = table_column
                .clone()
                .then_ignore(just([
                    T::Keyword(K::Is),
                    T::Keyword(K::Not),
                    T::Keyword(K::Null),
                ]))
                .map(WhereClause::NotNull);

            // column IN ( value_list )
            let in_clause = table_column
                .clone()
                .then_ignore(just(T::Keyword(K::In)))
                .then(value_list.clone())
                .map(|(col, vals)| WhereClause::In(col, vals));

            // column LIKE 'pattern'
            let like_clause = table_column
                .clone()
                .then_ignore(just(T::Keyword(K::Like)))
                .then(select! { T::String(s) => s.into() })
                .map(|(col, s)| WhereClause::Like(col, s));

            just(T::Keyword(K::Where))
                .ignore_then(
                    choice((op_expr, is_null, not_null, in_clause, like_clause))
                        .separated_by(just(T::Keyword(K::And)))
                        .collect(),
                )
                .boxed()
        };

        // DROP TABLE Identifier
        let drop_table = just([T::Keyword(K::Drop), T::Keyword(K::Table)])
            .ignore_then(identifier())
            .map(|table_name| TableStatement::DropTable(table_name.into()));

        // DESC Identifier
        let describe_table = just(T::Keyword(K::Desc))
            .ignore_then(identifier())
            .map(|table_name| TableStatement::DescribeTable(table_name.into()));

        // INSERT INTO Identifier VALUES value_lists
        let insert_into_table = just([T::Keyword(K::Insert), T::Keyword(K::Into)])
            .ignore_then(identifier())
            .then(just(T::Keyword(K::Values)).ignore_then(value_lists))
            .map(|(table_name, vals)| TableStatement::InsertInto(table_name.into(), vals))
            .boxed();

        // LOAD DATA INFILE 'file_path' INTO TABLE Identifier FIELDS TERMINATED BY 'delimiter'
        let load_data_infile = just([
            T::Keyword(K::Load),
            T::Keyword(K::Data),
            T::Keyword(K::Infile),
        ])
        .ignore_then(select! { T::String(s) => s.into() })
        .then_ignore(just([T::Keyword(K::Into), T::Keyword(K::Table)]))
        .then(identifier())
        .then_ignore(just([
            T::Keyword(K::Fields),
            T::Keyword(K::Terminated),
            T::Keyword(K::By),
        ]))
        .then(select! { T::String(s) => s })
        .validate(
            |((file_path, table_name), delimiter): ((String, &str), &str),
             _map,
             emitter: &mut Emitter<Rich<T<'a>>>| {
                let delim_chars: Vec<char> = delimiter.chars().collect();
                if delim_chars.len() != 1 {
                    emitter.emit(Rich::custom(
                        _map.span(),
                        "delimiter must be a single character".to_string(),
                    ));
                }
                TableStatement::LoadDataInfile(file_path, table_name.into(), delim_chars[0])
            },
        )
        .boxed();

        // DELETE FROM Identifier ('WHERE' where_and_clause)?
        let delete_from_table = just([T::Keyword(K::Delete), T::Keyword(K::From)])
            .ignore_then(identifier())
            .then(where_and_clause.clone().or_not())
            .map(|(table_name, where_clause)| {
                TableStatement::DeleteFrom(table_name.into(), where_clause)
            })
            .boxed();

        let set_clause = identifier()
            .then_ignore(just(T::Symbol('=')))
            .then(value)
            .map(|(name, value)| (name.into(), value))
            .separated_by(just(T::Symbol(',')))
            .collect()
            .boxed();

        // UPDATE table SET set_clause WHERE where_and_clause
        let update_table = just(T::Keyword(K::Update))
            .ignore_then(identifier())
            .then(just(T::Keyword(K::Set)).ignore_then(set_clause))
            .then(where_and_clause.clone().or_not())
            .map(
                |((table_name, set_clause), where_clause): (
                    (&str, Vec<(String, Value)>),
                    Option<Vec<WhereClause>>,
                )| {
                    TableStatement::Update(table_name.into(), set_clause, where_clause)
                },
            )
            .boxed();

        let selector = choice((
            table_column.clone().map(Selector::Column),
            just([
                T::Keyword(K::Count),
                T::Symbol('('),
                T::Symbol('*'),
                T::Symbol(')'),
            ])
            .to(Selector::CountAll),
            select! {
                T::Keyword(K::Count) => K::Count,
                T::Keyword(K::Average) => K::Average,
                T::Keyword(K::Max) => K::Max,
                T::Keyword(K::Min) => K::Min,
                T::Keyword(K::Sum) => K::Sum,
            }
            .then(
                table_column
                    .clone()
                    .delimited_by(just(T::Symbol('(')), just(T::Symbol(')'))),
            )
            .map(|(func, col)| match func {
                K::Count => Selector::Count(col),
                K::Average => Selector::Average(col),
                K::Max => Selector::Max(col),
                K::Min => Selector::Min(col),
                K::Sum => Selector::Sum(col),
                _ => unreachable!(),
            }),
        ))
        .boxed();

        let selectors = choice((
            just(T::Symbol('*')).to(Selectors::All),
            selector
                .separated_by(just(T::Symbol(',')))
                .collect()
                .map(Selectors::List),
        ))
        .boxed();

        // SELECT selectors
        let select_table = just(T::Keyword(K::Select))
            .ignore_then(selectors.clone())
            // FROM identifiers
            .then_ignore(just(T::Keyword(K::From)))
            .then(
                identifier()
                    .map(|s| s.to_string())
                    .separated_by(just(T::Symbol(',')))
                    .collect::<Vec<String>>(),
            )
            // where_and_clause?
            .then(where_and_clause.clone().or_not())
            // ('GROUP' 'BY' column)?
            .then(
                just([T::Keyword(K::Group), T::Keyword(K::By)])
                    .ignore_then(table_column.clone())
                    .or_not(),
            )
            .boxed()
            // ('ORDER' 'BY' column (order)?)?
            .then(
                just([T::Keyword(K::Order), T::Keyword(K::By)])
                    .ignore_then(table_column.clone())
                    .then(order.or_not())
                    .or_not(),
            )
            // ('LIMIT' Integer ('OFFSET' Integer)?)?
            .then(
                just(T::Keyword(K::Limit))
                    .ignore_then(select! { T::Integer(i) => i })
                    .then(
                        just(T::Keyword(K::Offset))
                            .ignore_then(select! { T::Integer(i) => i })
                            .or_not(),
                    )
                    .or_not()
                    .boxed(),
            )
            .boxed()
            .map(
                |(((((selectors, tables), where_clauses), group_by), order_by), limit_offset): (
                    (
                        (
                            ((Selectors, Vec<String>), Option<Vec<WhereClause>>),
                            Option<TableColumn>,
                        ),
                        Option<(TableColumn, Option<bool>)>,
                    ),
                    Option<(i64, Option<i64>)>,
                )| {
                    let (limit, offset) = match limit_offset {
                        Some((l, o)) => (Some(l), o),
                        None => (None, None),
                    };
                    TableStatement::Select(SelectClause {
                        selectors,
                        table: tables,
                        where_clauses: where_clauses.unwrap_or_default(),
                        group_by,
                        order_by: order_by.map(|(col, asc)| (col, asc.unwrap_or(true))),
                        limit: limit.map(|l| l as usize),
                        offset: offset.map(|o| o as usize),
                    })
                },
            )
            .boxed();

        let column_type = choice((
            just(T::Keyword(K::Int)).to(ColumnType::Int),
            just(T::Keyword(K::Float)).to(ColumnType::Float),
            just(T::Keyword(K::Varchar))
                .ignore_then(
                    select! { T::Integer(i) => i as usize }
                        .delimited_by(just(T::Symbol('(')), just(T::Symbol(')'))),
                )
                .map(ColumnType::Char),
        ))
        .boxed();

        let create_table_field = choice((
            // Identifier type (NOT NULL)? (DEFAULT value)?
            identifier()
                .then(column_type)
                .then(
                    just([T::Keyword(K::Not), T::Keyword(K::Null)])
                        .ignored()
                        .or_not(),
                )
                .then(just(T::Keyword(K::Default)).ignore_then(value).or_not())
                .map(
                    |(((name, ctype), notnull), default_value): (
                        ((&str, ColumnType), Option<()>),
                        Option<Value>,
                    )| {
                        let notnull = notnull.is_some();
                        let default_value = default_value.unwrap_or(Value::Null);

                        CreateTableField::Col(name.into(), ctype, notnull, default_value)
                    },
                ),
            // PRIMARY KEY (Identifier)? ( identifiers )
            just([T::Keyword(K::Primary), T::Keyword(K::Key)])
                .ignore_then(identifier().or_not())
                .then(
                    identifier()
                        .separated_by(just(T::Symbol(',')))
                        .collect()
                        .delimited_by(just(T::Symbol('(')), just(T::Symbol(')'))),
                )
                .map(|(_pkey_name, fields): (Option<&str>, Vec<&str>)| {
                    CreateTableField::Pkey(Box::new(AlterStatement::AddPKey(
                        String::default(), // Table name will be filled later
                        fields.into_iter().map(|s| s.into()).collect(),
                    )))
                }),
            // FOREIGN KEY (Identifier)? ( identifiers ) REFERENCES Identifier ( identifiers )
            just([T::Keyword(K::Foreign), T::Keyword(K::Key)])
                .ignore_then(identifier().or_not())
                .then(
                    identifier()
                        .separated_by(just(T::Symbol(',')))
                        .collect()
                        .delimited_by(just(T::Symbol('(')), just(T::Symbol(')'))),
                )
                .then(just(T::Keyword(K::References)).ignore_then(identifier()))
                .then(
                    identifier()
                        .separated_by(just(T::Symbol(',')))
                        .collect()
                        .delimited_by(just(T::Symbol('(')), just(T::Symbol(')'))),
                )
                .validate(
                    |(((fkey_name, fields), ref_table), ref_fields): (
                        ((Option<&str>, Vec<&str>), &str),
                        Vec<&str>,
                    ), m, e| {
                        if fields.len() != ref_fields.len() {
                            e.emit(Rich::custom(
                                m.span(),
                                format!(
                                    "number of fields ({}) does not match number of reference fields ({})",
                                    fields.len(),
                                    ref_fields.len()
                                ),
                            ));
                        }

                        CreateTableField::Fkey(Box::new(AlterStatement::AddFKey(
                            String::default(), // Table name will be filled later
                            fkey_name.map(|s| s.into()),
                            fields.into_iter().map(|s| s.into()).collect(),
                            ref_table.into(),
                            ref_fields.into_iter().map(|s| s.into()).collect(),
                        )))
                    },
                ),
        ))
        .boxed();

        // CREATE TABLE Identifier
        let create_table = just([T::Keyword(K::Create), T::Keyword(K::Table)])
            .ignore_then(identifier())
            .then(
                // ( field_list )
                create_table_field
                    .separated_by(just(T::Symbol(',')))
                    .collect()
                    .delimited_by(just(T::Symbol('(')), just(T::Symbol(')'))),
            )
            .map(|(table_name, fields)| TableStatement::CreateTable(table_name.into(), fields))
            .boxed();

        choice((
            drop_table,
            describe_table,
            insert_into_table,
            load_data_infile,
            delete_from_table,
            update_table,
            select_table,
            create_table,
        ))
        .boxed()
    }

    choice((
        db_statement().map(Query::DBStmt),
        alter_statement().map(Query::AlterStmt),
        table_statement().map(Query::TableStmt),
        null_statement(),
    ))
    .then_ignore(just(T::Symbol(';')))
    .repeated()
    .collect()
}
