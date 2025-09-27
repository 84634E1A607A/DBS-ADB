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
    Avg,
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
}

#[derive(Debug, Clone, PartialEq)]
pub enum SQLToken<'a> {
    Keyword(KeywordEnum),
    Identifier(&'a str),
    Symbol(char),
    Integer(i64),
    Float(f64),
    String(&'a str),
    Comment(&'a str),
}

pub fn lexer<'a>() -> impl Parser<'a, &'a str, Vec<SQLToken<'a>>, extra::Err<Rich<'a, char>>> {
    let comment = just("--")
        .ignore_then(none_of(";").repeated())
        .to_slice()
        .map(|s: &str| SQLToken::Comment(s))
        .padded();

    let number = regex(r"-?\d+\.\d*")
        .map(|s: &str| SQLToken::Float(s.parse().unwrap()))
        .padded();

    let integer = regex(r"-?\d+")
        .map(|s: &str| SQLToken::Integer(s.parse().unwrap()))
        .padded();

    let string = regex(r#"'([^'\\]|\\.)*'"#)
        .map(|s: &str| SQLToken::String(&s[1..s.len() - 1]))
        .padded();

    let identifier = ident().map(|s: &str| SQLToken::Identifier(s)).padded();

    let keyword = {
        // Use a regex with word boundaries so keywords aren't matched as prefixes of identifiers
        let pattern = r"\b(?:CREATE|DATABASE|DATABASES|DROP|SHOW|USE|TABLES|INDEXES|TABLE|ALTER|ADD|PRIMARY|KEY|FOREIGN|REFERENCES|INFILE|INTO|FIELDS|TERMINATED|BY|VALUES|INSERT|DELETE|FROM|UPDATE|SET|WHERE|SELECT|COUNT|AVG|MAX|MIN|SUM|GROUP|ORDER|LIMIT|OFFSET|ASC|DESC|NULL|NOT|LIKE)\b";
        regex(pattern).map(|s: &str| SQLToken::Keyword(match s {
            "CREATE" => KeywordEnum::Create,
            "DATABASE" => KeywordEnum::Database,
            "DATABASES" => KeywordEnum::Databases,
            "DROP" => KeywordEnum::Drop,
            "SHOW" => KeywordEnum::Show,
            "USE" => KeywordEnum::Use,
            "TABLES" => KeywordEnum::Tables,
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
            "AVG" => KeywordEnum::Avg,
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
            _ => unreachable!(),
        })).padded()
    };
    let symbol = one_of("(),;=*<>").map(SQLToken::Symbol).padded();

    choice((
        comment, number, integer, string, keyword, identifier, symbol,
    ))
    .repeated()
    .collect()
}
