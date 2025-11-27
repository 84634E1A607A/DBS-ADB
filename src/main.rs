use adb::lexer_parser;

fn main() {
    println!("ADB - A Simple Database Management System");
    println!("File management layer initialized successfully.");

    // Test the parser
    let query = "CREATE DATABASE test_db;";
    let result = lexer_parser::parse(query);
    match result {
        Ok(queries) => println!("Parsed {} queries successfully", queries.len()),
        Err(e) => println!("Parse error: {}", e),
    }
}
