use chumsky::Parser;

mod lexer_parser;

fn main() {
    let query = "CREATE DATABASE ;";
    let result = lexer_parser::parser().parse(query);
    assert!(result.has_errors());
    let errors = result.errors();
    errors.for_each(|e| println!("{}", e));
}
