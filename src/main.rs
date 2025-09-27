use chumsky::Parser;

mod lexer_parser;

fn main() {
    let query = "CREATE DATABASE ;";
    let result = lexer_parser::parse(query);
    assert!(result.is_ok());
}
