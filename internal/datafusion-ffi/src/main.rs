use prql_compiler::{compile, Options};

fn main() {
    let prql = "from table | select [col1, col2]";
    match compile(prql, &Options::default()) {
        Ok(sql) => println!("SQL: {}", sql),
        Err(e) => println!("Error: {}", e),
    }
}
