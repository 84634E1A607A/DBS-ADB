use chumsky::{prelude::*, regex::regex, text::ascii::ident};

#[derive(Debug, Clone, PartialEq)]
pub enum KeywordEnum {
    Create,
    Database,
    Databases,
    Drop,
    Show,
    Use,
    Tables,
    Index,
    Indexes,
    Table,
    Alter,
    Add,
    Primary,
    Key,
    Foreign,
    References,
    Infile,
    Into,
    Fields,
    Terminated,
    By,
    Values,
    Insert,
    Delete,
    From,
    Update,
    Set,
    Where,
    Select,
    Count,
    Average,
    Max,
    Min,
    Sum,
    Group,
    Order,
    Limit,
    Offset,
    Asc,
    Desc,
    Null,
    Not,
    Like,
    Int,
    Varchar,
    Float,
    Is,
    In,
    And,
    Load,
    Data,
    Default,
    Constraint,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SQLToken<'a> {
    Keyword(KeywordEnum),
    Identifier(&'a str),
    Symbol(char),
    Integer(i64),
    Float(f64),
    String(&'a str),
}

pub fn lexer<'a>() -> impl Parser<'a, &'a str, Vec<SQLToken<'a>>, extra::Err<Rich<'a, char>>> {
    lexer_with_keyword_case(false)
}

pub fn lexer_with_keyword_case<'a>(
    case_insensitive: bool,
) -> impl Parser<'a, &'a str, Vec<SQLToken<'a>>, extra::Err<Rich<'a, char>>> {
    let comment = just("--")
        .ignore_then(none_of([';', '\n']).repeated())
        .padded()
        .ignored();

    let number = regex(r"-?\d+\.\d*")
        .try_map(|s: &str, span| {
            s.parse::<f64>()
                .map(|val| SQLToken::Float(val))
                .map_err(|err| Rich::custom(span, err.to_string()))
        })
        .padded();

    let integer = regex(r"-?\d+")
        .try_map(|s: &str, span| {
            s.parse::<i64>()
                .map(|val| SQLToken::Integer(val))
                .map_err(|err| Rich::custom(span, err.to_string()))
        })
        .padded();

    let string = regex(r#"'([^'\\]|\\.)*'"#)
        .map(|s: &str| SQLToken::String(&s[1..s.len() - 1]))
        .padded();

    let identifier = ident().map(|s: &str| SQLToken::Identifier(s)).padded();

    let keyword = {
        // Use a regex with word boundaries so keywords aren't matched as prefixes of identifiers
        let pattern = if case_insensitive {
            r"(?i)\b(?:CREATE|DATABASE|DATABASES|DROP|SHOW|USE|TABLES|INDEX|INDEXES|TABLE|ALTER|ADD|PRIMARY|KEY|FOREIGN|REFERENCES|INFILE|INTO|FIELDS|TERMINATED|BY|VALUES|INSERT|DELETE|FROM|UPDATE|SET|WHERE|SELECT|COUNT|AVERAGE|AVG|MAX|MIN|SUM|GROUP|ORDER|LIMIT|OFFSET|ASC|DESC|NULL|NOT|LIKE|INT|VARCHAR|FLOAT|IS|IN|AND|LOAD|DATA|DEFAULT|CONSTRAINT)\b"
        } else {
            r"\b(?:CREATE|DATABASE|DATABASES|DROP|SHOW|USE|TABLES|INDEX|INDEXES|TABLE|ALTER|ADD|PRIMARY|KEY|FOREIGN|REFERENCES|INFILE|INTO|FIELDS|TERMINATED|BY|VALUES|INSERT|DELETE|FROM|UPDATE|SET|WHERE|SELECT|COUNT|AVERAGE|AVG|MAX|MIN|SUM|GROUP|ORDER|LIMIT|OFFSET|ASC|DESC|NULL|NOT|LIKE|INT|VARCHAR|FLOAT|IS|IN|AND|LOAD|DATA|DEFAULT|CONSTRAINT)\b"
        };
        regex(pattern)
            .map(move |s: &str| {
                SQLToken::Keyword(if case_insensitive {
                    match s.to_ascii_uppercase().as_str() {
                        "CREATE" => KeywordEnum::Create,
                        "DATABASE" => KeywordEnum::Database,
                        "DATABASES" => KeywordEnum::Databases,
                        "DROP" => KeywordEnum::Drop,
                        "SHOW" => KeywordEnum::Show,
                        "USE" => KeywordEnum::Use,
                        "TABLES" => KeywordEnum::Tables,
                        "INDEX" => KeywordEnum::Index,
                        "INDEXES" => KeywordEnum::Indexes,
                        "TABLE" => KeywordEnum::Table,
                        "ALTER" => KeywordEnum::Alter,
                        "ADD" => KeywordEnum::Add,
                        "PRIMARY" => KeywordEnum::Primary,
                        "KEY" => KeywordEnum::Key,
                        "FOREIGN" => KeywordEnum::Foreign,
                        "REFERENCES" => KeywordEnum::References,
                        "INFILE" => KeywordEnum::Infile,
                        "INTO" => KeywordEnum::Into,
                        "FIELDS" => KeywordEnum::Fields,
                        "TERMINATED" => KeywordEnum::Terminated,
                        "BY" => KeywordEnum::By,
                        "VALUES" => KeywordEnum::Values,
                        "INSERT" => KeywordEnum::Insert,
                        "DELETE" => KeywordEnum::Delete,
                        "FROM" => KeywordEnum::From,
                        "UPDATE" => KeywordEnum::Update,
                        "SET" => KeywordEnum::Set,
                        "WHERE" => KeywordEnum::Where,
                        "SELECT" => KeywordEnum::Select,
                        "COUNT" => KeywordEnum::Count,
                        "AVERAGE" => KeywordEnum::Average,
                        "AVG" => KeywordEnum::Average,
                        "MAX" => KeywordEnum::Max,
                        "MIN" => KeywordEnum::Min,
                        "SUM" => KeywordEnum::Sum,
                        "GROUP" => KeywordEnum::Group,
                        "ORDER" => KeywordEnum::Order,
                        "LIMIT" => KeywordEnum::Limit,
                        "OFFSET" => KeywordEnum::Offset,
                        "ASC" => KeywordEnum::Asc,
                        "DESC" => KeywordEnum::Desc,
                        "NULL" => KeywordEnum::Null,
                        "NOT" => KeywordEnum::Not,
                        "LIKE" => KeywordEnum::Like,
                        "INT" => KeywordEnum::Int,
                        "VARCHAR" => KeywordEnum::Varchar,
                        "FLOAT" => KeywordEnum::Float,
                        "IS" => KeywordEnum::Is,
                        "IN" => KeywordEnum::In,
                        "AND" => KeywordEnum::And,
                        "LOAD" => KeywordEnum::Load,
                        "DATA" => KeywordEnum::Data,
                        "DEFAULT" => KeywordEnum::Default,
                        "CONSTRAINT" => KeywordEnum::Constraint,
                        _ => unreachable!(),
                    }
                } else {
                    match s {
                        "CREATE" => KeywordEnum::Create,
                        "DATABASE" => KeywordEnum::Database,
                        "DATABASES" => KeywordEnum::Databases,
                        "DROP" => KeywordEnum::Drop,
                        "SHOW" => KeywordEnum::Show,
                        "USE" => KeywordEnum::Use,
                        "TABLES" => KeywordEnum::Tables,
                        "INDEX" => KeywordEnum::Index,
                        "INDEXES" => KeywordEnum::Indexes,
                        "TABLE" => KeywordEnum::Table,
                        "ALTER" => KeywordEnum::Alter,
                        "ADD" => KeywordEnum::Add,
                        "PRIMARY" => KeywordEnum::Primary,
                        "KEY" => KeywordEnum::Key,
                        "FOREIGN" => KeywordEnum::Foreign,
                        "REFERENCES" => KeywordEnum::References,
                        "INFILE" => KeywordEnum::Infile,
                        "INTO" => KeywordEnum::Into,
                        "FIELDS" => KeywordEnum::Fields,
                        "TERMINATED" => KeywordEnum::Terminated,
                        "BY" => KeywordEnum::By,
                        "VALUES" => KeywordEnum::Values,
                        "INSERT" => KeywordEnum::Insert,
                        "DELETE" => KeywordEnum::Delete,
                        "FROM" => KeywordEnum::From,
                        "UPDATE" => KeywordEnum::Update,
                        "SET" => KeywordEnum::Set,
                        "WHERE" => KeywordEnum::Where,
                        "SELECT" => KeywordEnum::Select,
                        "COUNT" => KeywordEnum::Count,
                        "AVERAGE" => KeywordEnum::Average,
                        "AVG" => KeywordEnum::Average,
                        "MAX" => KeywordEnum::Max,
                        "MIN" => KeywordEnum::Min,
                        "SUM" => KeywordEnum::Sum,
                        "GROUP" => KeywordEnum::Group,
                        "ORDER" => KeywordEnum::Order,
                        "LIMIT" => KeywordEnum::Limit,
                        "OFFSET" => KeywordEnum::Offset,
                        "ASC" => KeywordEnum::Asc,
                        "DESC" => KeywordEnum::Desc,
                        "NULL" => KeywordEnum::Null,
                        "NOT" => KeywordEnum::Not,
                        "LIKE" => KeywordEnum::Like,
                        "INT" => KeywordEnum::Int,
                        "VARCHAR" => KeywordEnum::Varchar,
                        "FLOAT" => KeywordEnum::Float,
                        "IS" => KeywordEnum::Is,
                        "IN" => KeywordEnum::In,
                        "AND" => KeywordEnum::And,
                        "LOAD" => KeywordEnum::Load,
                        "DATA" => KeywordEnum::Data,
                        "DEFAULT" => KeywordEnum::Default,
                        "CONSTRAINT" => KeywordEnum::Constraint,
                        _ => unreachable!(),
                    }
                })
            })
            .padded()
    };
    let symbol = one_of("(),;=*<>.").map(SQLToken::Symbol).padded();

    choice((number, integer, string, keyword, identifier, symbol))
        .separated_by(comment.repeated().or_not())
        .collect()
        .delimited_by(comment.repeated().or_not(), comment.repeated().or_not())
}
