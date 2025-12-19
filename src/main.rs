use adb::database::{DatabaseManager, QueryResult};
use adb::lexer_parser::{self, Query};
use std::io::{self, BufRead, Write};

fn main() {
    let data_dir = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "./data".to_string());

    let mut db_manager = match DatabaseManager::new(&data_dir) {
        Ok(manager) => manager,
        Err(e) => {
            eprintln!("Failed to initialize database manager: {}", e);
            std::process::exit(1);
        }
    };

    let stdin = io::stdin();
    let mut stdout = io::stdout();

    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break,
        };

        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        // Parse SQL
        let queries = match lexer_parser::parse(line) {
            Ok(q) => q,
            Err(e) => {
                eprintln!("Parse error: {}", e);
                continue;
            }
        };

        // Execute each query
        for query in queries {
            // Echo the query type
            print_query_echo(line, &query);

            let result = execute_query(&mut db_manager, query);
            match result {
                Ok(res) => print_result(&res),
                Err(e) => eprintln!("Error: {}", e),
            }

            stdout.flush().unwrap();
        }
    }
}

fn print_query_echo(original: &str, query: &Query) {
    // Print original query as comment
    println!("@{}", original);
}

fn execute_query(
    db: &mut DatabaseManager,
    query: Query,
) -> Result<QueryResult, adb::database::DatabaseError> {
    match query {
        Query::DBStmt(stmt) => db.execute_db_statement(stmt),
        Query::TableStmt(stmt) => db.execute_table_statement(stmt),
        Query::AlterStmt(stmt) => db.execute_alter_statement(stmt),
        Query::Null => Ok(QueryResult::Empty),
    }
}

fn print_result(result: &QueryResult) {
    match result {
        QueryResult::Empty => {
            // No output for empty results
        }
        QueryResult::RowsAffected(count) => {
            println!("rows");
            println!("{}", count);
        }
        QueryResult::ResultSet(headers, rows) => {
            // Print headers
            println!("{}", headers.join(","));

            // Print rows
            for row in rows {
                println!("{}", row.join(","));
            }
        }
        QueryResult::List(items) => {
            for item in items {
                println!("{}", item);
            }
        }
        QueryResult::TableDescription(meta) => {
            println!("Field,Type,Null,Default");
            for col in &meta.columns {
                let null_str = if col.not_null { "NO" } else { "YES" };
                let default_str = col.default_value.as_deref().unwrap_or("NULL");
                println!(
                    "{},{},{},{}",
                    col.name, col.column_type, null_str, default_str
                );
            }
        }
    }
}
