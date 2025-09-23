use chumsky::Parser;

mod lexer;

fn main() {
    let query = "CREATE DATABASE ;";
    let result = lexer::parser().parse(query);
    assert!(result.has_errors());
    let errors = result.errors();
    errors.for_each(|e| println!("{}", e));
}
