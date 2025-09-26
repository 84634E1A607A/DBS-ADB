use chumsky::{prelude::*, regex::regex, text::ascii::ident};

#[derive(Debug, Clone, PartialEq)]
pub enum SQLToken<'a> {
    Keyword(&'a str),
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
        let pattern = r"\b(?:CREATE|DATABASE|DROP|SHOW|USE|TABLES|INDEXES|TABLE|ALTER|ADD|PRIMARY|KEY|FOREIGN|REFERENCES|INFILE|INTO|FIELDS|TERMINATED|BY|VALUES|INSERT|DELETE|FROM|UPDATE|SET|WHERE|SELECT|COUNT|AVG|MAX|MIN|SUM|GROUP|ORDER|LIMIT|OFFSET|ASC|DESC|NULL|NOT|LIKE)\b";
        regex(pattern).map(|s: &str| SQLToken::Keyword(s)).padded()
    };
    let symbol = one_of("(),;=*<>").map(SQLToken::Symbol).padded();

    choice((
        comment, number, integer, string, keyword, identifier, symbol,
    ))
    .repeated()
    .collect()
}
