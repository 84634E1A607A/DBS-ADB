use adb::database::{DatabaseManager, QueryResult};
use adb::lexer_parser::{self, Query};
use clap::Parser;
use std::fs;
use std::io::{self, BufRead, BufReader, Write};
use std::path::Path;

#[derive(Parser, Debug)]
#[command(name = "adb")]
#[command(about = "A simple database management system", long_about = None)]
struct Args {
    /// Initialize database (delete existing data and exit with code 0)
    #[arg(long)]
    init: bool,

    /// Batch processing mode
    #[arg(short, long)]
    batch: bool,

    /// Import data from file (must be used with -t/--table)
    #[arg(short, long, value_name = "PATH")]
    file: Option<String>,

    /// Target table for data import (must be used with -f/--file)
    #[arg(short, long, value_name = "TABLE")]
    table: Option<String>,

    /// Database to use at startup (executes USE <db>)
    #[arg(short, long, value_name = "DB")]
    database: Option<String>,

    /// Data directory path
    #[arg(long, default_value = "./data")]
    data_dir: String,
}

fn main() {
    let args = Args::parse();

    // Validate argument combinations
    if args.file.is_some() && args.table.is_none() {
        eprintln!("Error: -f/--file requires -t/--table to be specified");
        std::process::exit(1);
    }
    if args.table.is_some() && args.file.is_none() {
        eprintln!("Error: -t/--table requires -f/--file to be specified");
        std::process::exit(1);
    }

    // Handle --init: delete existing data and initialize
    if args.init {
        if Path::new(&args.data_dir).exists() {
            if let Err(e) = fs::remove_dir_all(&args.data_dir) {
                eprintln!("Failed to remove existing data directory: {}", e);
                std::process::exit(1);
            }
        }

        match DatabaseManager::new(&args.data_dir) {
            Ok(_) => {
                println!("Database initialized at: {}", args.data_dir);
                std::process::exit(0);
            }
            Err(e) => {
                eprintln!("Failed to initialize database manager: {}", e);
                std::process::exit(1);
            }
        }
    }

    // Initialize database manager
    let mut db_manager = match DatabaseManager::new(&args.data_dir) {
        Ok(manager) => manager,
        Err(e) => {
            eprintln!("Failed to initialize database manager: {}", e);
            std::process::exit(1);
        }
    };

    // If database is specified, execute USE command
    if let Some(db_name) = args.database {
        let use_query = format!("USE {};", db_name);
        if let Err(e) = execute_sql_line(&mut db_manager, &use_query, args.batch) {
            eprintln!("Failed to use database {}: {}", db_name, e);
            std::process::exit(1);
        }
    }

    // Handle file import mode
    if let (Some(file_path), Some(table_name)) = (args.file, args.table) {
        if let Err(e) = import_data_from_file(&mut db_manager, &file_path, &table_name, args.batch)
        {
            eprintln!("Failed to import data: {}", e);
            std::process::exit(1);
        }
        std::process::exit(0);
    }

    // Interactive or batch mode
    run_interactive_mode(&mut db_manager, args.batch);
}

fn import_data_from_file(
    db_manager: &mut DatabaseManager,
    file_path: &str,
    _table_name: &str,
    batch_mode: bool,
) -> Result<(), String> {
    let file = fs::File::open(file_path).map_err(|e| format!("Cannot open file: {}", e))?;
    let reader = BufReader::new(file);

    for line in reader.lines() {
        let line = line.map_err(|e| format!("Failed to read line: {}", e))?;
        let line = line.trim();

        if line.is_empty() {
            continue;
        }

        // Parse and execute the SQL statement
        if let Err(e) = execute_sql_line(db_manager, line, batch_mode) {
            return Err(format!("Error executing '{}': {}", line, e));
        }
    }

    Ok(())
}

fn execute_sql_line(
    db_manager: &mut DatabaseManager,
    line: &str,
    batch_mode: bool,
) -> Result<(), String> {
    let queries = lexer_parser::parse(line).map_err(|e| format!("Parse error: {}", e))?;

    for query in queries {
        if !batch_mode {
            print_query_echo(line);
        }

        let result = execute_query(db_manager, query).map_err(|e| format!("{}", e))?;

        print_result(&result);
    }

    Ok(())
}

fn run_interactive_mode(db_manager: &mut DatabaseManager, batch_mode: bool) {
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
            // Echo the query (unless in batch mode)
            if !batch_mode {
                print_query_echo(line);
            }

            let result = execute_query(db_manager, query);
            match result {
                Ok(res) => print_result(&res),
                Err(e) => eprintln!("Error: {}", e),
            }

            if batch_mode {
                print_query_echo(line);
            }

            stdout.flush().unwrap();
        }
    }
}

fn print_query_echo(_original: &str) {
    // Print original query as comment
    println!("@{}", _original);
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
