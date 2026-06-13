use arrow::array::{make_array, RecordBatch};
use arrow::datatypes::{Field, Schema};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi, to_ffi};
use datafusion::prelude::*;
use prql_compiler::{compile, Options};
use std::ffi::{c_char, CStr, CString};
use std::ptr;
use std::sync::Arc;

#[repr(C)]
pub struct FFIColumn {
    pub name: *const c_char,
    pub schema: *mut FFI_ArrowSchema,
    pub array: *mut FFI_ArrowArray,
}

#[repr(C)]
pub struct FFIColumnList {
    pub columns: *mut FFIColumn,
    pub count: usize,
    pub error: *mut c_char,
}

#[unsafe(no_mangle)]
pub extern "C" fn execute_prql_query(
    query_ptr: *const c_char,
    input_columns: *const FFIColumn,
    input_count: usize,
) -> FFIColumnList {
    let result = std::panic::catch_unwind(|| {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(async {
                execute_prql_internal(query_ptr, input_columns, input_count).await
            })
    });

    match result {
        Ok(Ok(list)) => list,
        Ok(Err(e)) => error_list(&e.to_string()),
        Err(_) => error_list("Rust panic during execute_prql_query"),
    }
}

async fn execute_prql_internal(
    query_ptr: *const c_char,
    input_columns: *const FFIColumn,
    input_count: usize,
) -> Result<FFIColumnList, Box<dyn std::error::Error>> {
    if query_ptr.is_null() {
        return Err("Query string is null".into());
    }
    
    let query_c = unsafe { CStr::from_ptr(query_ptr) };
    let prql_query = query_c.to_str()?;

    // Compile PRQL to SQL
    let sql_query = compile(prql_query, &Options::default().no_format().no_signature())?;

    // Parse SQL using sqlparser to extract query tables
    let dialect = sqlparser::dialect::GenericDialect;
    let statements = sqlparser::parser::Parser::parse_sql(&dialect, &sql_query)?;
    let mut query_tables = std::collections::HashSet::new();
    let mut ctes = std::collections::HashSet::new();
    for stmt in &statements {
        if let sqlparser::ast::Statement::Query(query) = stmt {
            let mut tables = Vec::new();
            let mut aliases = Vec::new();
            extract_tables_and_aliases_from_query(query, &mut tables, &mut aliases, &mut ctes);
            for t in tables {
                query_tables.insert(t);
            }
        }
    }
    // Remove CTEs
    for cte in ctes {
        query_tables.remove(&cte);
    }

    // Read and group input columns by table name
    let mut table_columns: std::collections::HashMap<String, Vec<(String, arrow::array::ArrayRef)>> = std::collections::HashMap::new();

    let input_slice = unsafe { std::slice::from_raw_parts(input_columns, input_count) };
    for col in input_slice {
        if col.name.is_null() || col.schema.is_null() || col.array.is_null() {
            return Err("Input column name, schema or array pointer is null".into());
        }
        let name_c = unsafe { CStr::from_ptr(col.name) };
        let name_str = name_c.to_str()?;

        let ffi_schema = unsafe { std::ptr::read(col.schema) };
        let ffi_array = unsafe { std::ptr::read(col.array) };
        let array_data = unsafe { from_ffi(ffi_array, &ffi_schema)? };
        let arrow_array = make_array(array_data);

        // Find table grouping prefix
        let mut table_name = "input".to_string();
        let mut col_name = name_str.to_string();
        for q_table in &query_tables {
            if q_table != "input" {
                let prefix = format!("{}_", q_table);
                if name_str.starts_with(&prefix) {
                    table_name = q_table.clone();
                    col_name = name_str[prefix.len()..].to_string();
                    break;
                }
            }
        }

        table_columns.entry(table_name).or_default().push((col_name, arrow_array));
    }

    // Execute via DataFusion
    let ctx = SessionContext::new();

    // Build and register a RecordBatch for each table group
    for (tbl_name, cols) in table_columns {
        let mut fields = Vec::new();
        let mut arrays = Vec::new();
        for (col_name, arr) in cols {
            let field = Field::new(&col_name, arr.data_type().clone(), true);
            fields.push(Arc::new(field));
            arrays.push(arr);
        }
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema, arrays)?;
        ctx.register_batch(&tbl_name, batch)?;
    }

    let df_result = ctx.sql(&sql_query).await?;
    let batches = df_result.collect().await?;
    
    if batches.is_empty() {
        return Ok(FFIColumnList {
            columns: ptr::null_mut(),
            count: 0,
            error: ptr::null_mut(),
        });
    }

    let final_batch = arrow::compute::concat_batches(&batches[0].schema(), &batches)?;
    
    let mut out_cols = Vec::new();
    for (i, field) in final_batch.schema().fields().iter().enumerate() {
        let name_cstr = CString::new(field.name().as_str())?;
        
        let (out_array, out_schema) = to_ffi(&final_batch.column(i).to_data())?;
        
        let out_array_ptr = Box::into_raw(Box::new(out_array));
        let out_schema_ptr = Box::into_raw(Box::new(out_schema));

        out_cols.push(FFIColumn {
            name: name_cstr.into_raw(),
            schema: out_schema_ptr,
            array: out_array_ptr,
        });
    }

    let count = out_cols.len();
    let out_cols_ptr = {
        let mut b = out_cols.into_boxed_slice();
        let ptr = b.as_mut_ptr();
        std::mem::forget(b);
        ptr
    };

    Ok(FFIColumnList {
        columns: out_cols_ptr,
        count,
        error: ptr::null_mut(),
    })
}

fn error_list(msg: &str) -> FFIColumnList {
    let err_cstr = CString::new(msg).unwrap_or_else(|_| CString::new("Unknown error").unwrap());
    FFIColumnList {
        columns: ptr::null_mut(),
        count: 0,
        error: err_cstr.into_raw(),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn free_ffi_column_list(list: FFIColumnList) {
    unsafe {
        if !list.error.is_null() {
            let _ = CString::from_raw(list.error);
        }
        if !list.columns.is_null() && list.count > 0 {
            let cols = Box::from_raw(std::slice::from_raw_parts_mut(list.columns, list.count));
            for col in cols.iter() {
                if !col.name.is_null() {
                    let _ = CString::from_raw(col.name as *mut c_char);
                }
                if !col.schema.is_null() {
                    let _ = Box::from_raw(col.schema);
                }
                if !col.array.is_null() {
                    let _ = Box::from_raw(col.array);
                }
            }
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn free_string(ptr: *mut c_char) {
    if !ptr.is_null() {
        unsafe {
            let _ = CString::from_raw(ptr);
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn validate_prql(
    query_ptr: *const c_char,
    error_out: *mut *mut c_char,
) -> *mut c_char {
    let result = std::panic::catch_unwind(|| {
        validate_prql_internal(query_ptr, error_out)
    });

    match result {
        Ok(ptr) => ptr,
        Err(_) => {
            unsafe {
                if !error_out.is_null() {
                    let err_msg = CString::new("Rust panic during validate_prql").unwrap();
                    *error_out = err_msg.into_raw();
                }
            }
            std::ptr::null_mut()
        }
    }
}

fn validate_prql_internal(
    query_ptr: *const c_char,
    error_out: *mut *mut c_char,
) -> *mut c_char {
    if query_ptr.is_null() {
        unsafe {
            if !error_out.is_null() {
                let err_msg = CString::new("Query pointer is null").unwrap();
                *error_out = err_msg.into_raw();
            }
        }
        return std::ptr::null_mut();
    }

    let query_c = unsafe { CStr::from_ptr(query_ptr) };
    let prql_query = match query_c.to_str() {
        Ok(s) => s,
        Err(e) => {
            unsafe {
                if !error_out.is_null() {
                    let err_msg = CString::new(format!("Invalid UTF-8 query string: {}", e)).unwrap();
                    *error_out = err_msg.into_raw();
                }
            }
            return std::ptr::null_mut();
        }
    };

    // Compile PRQL to SQL
    let sql_query = match compile(prql_query, &Options::default().no_format().no_signature()) {
        Ok(sql) => sql,
        Err(e) => {
            unsafe {
                if !error_out.is_null() {
                    let err_msg = CString::new(format!("PRQL compilation error: {}", e)).unwrap();
                    *error_out = err_msg.into_raw();
                }
            }
            return std::ptr::null_mut();
        }
    };

    // Parse SQL using sqlparser
    let dialect = sqlparser::dialect::GenericDialect;
    let statements = match sqlparser::parser::Parser::parse_sql(&dialect, &sql_query) {
        Ok(stmts) => stmts,
        Err(e) => {
            unsafe {
                if !error_out.is_null() {
                    let err_msg = CString::new(format!("SQL parsing error: {}", e)).unwrap();
                    *error_out = err_msg.into_raw();
                }
            }
            return std::ptr::null_mut();
        }
    };

    // Extract table names and aliases
    let mut tables = Vec::new();
    let mut aliases = Vec::new();
    let mut ctes = std::collections::HashSet::new();
    for stmt in &statements {
        match stmt {
            sqlparser::ast::Statement::Query(query) => {
                extract_tables_and_aliases_from_query(query, &mut tables, &mut aliases, &mut ctes);
            }
            _ => {}
        }
    }

    // Filter out CTEs
    let unique_tables: std::collections::HashSet<String> = tables
        .into_iter()
        .filter(|t| !ctes.contains(t))
        .collect();

    let unique_aliases: std::collections::HashSet<String> = aliases
        .into_iter()
        .filter(|a| !ctes.contains(a))
        .collect();

    // Convert to sorted vector for deterministic output in test assertions
    let mut unique_tables_vec: Vec<String> = unique_tables.into_iter().collect();
    unique_tables_vec.sort();

    let mut unique_aliases_vec: Vec<String> = unique_aliases.into_iter().collect();
    unique_aliases_vec.sort();

    let tables_str = unique_tables_vec.join(",");
    let aliases_str = unique_aliases_vec.join(",");
    let full_res = format!("{};{}", tables_str, aliases_str);
    let res_cstr = match CString::new(full_res) {
        Ok(cstr) => cstr,
        Err(e) => {
            unsafe {
                if !error_out.is_null() {
                    let err_msg = CString::new(format!("Failed to create table names CString: {}", e)).unwrap();
                    *error_out = err_msg.into_raw();
                }
            }
            return std::ptr::null_mut();
        }
    };

    unsafe {
        if !error_out.is_null() {
            *error_out = std::ptr::null_mut();
        }
    }

    res_cstr.into_raw()
}

fn extract_tables_and_aliases_from_query(
    query: &sqlparser::ast::Query,
    tables: &mut Vec<String>,
    aliases: &mut Vec<String>,
    ctes: &mut std::collections::HashSet<String>,
) {
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            let cte_name = cte.alias.name.value.clone();
            ctes.insert(cte_name);
            extract_tables_and_aliases_from_query(&cte.query, tables, aliases, ctes);
        }
    }
    extract_tables_and_aliases_from_set_expr(&query.body, tables, aliases, ctes);
}

fn extract_tables_and_aliases_from_set_expr(
    expr: &sqlparser::ast::SetExpr,
    tables: &mut Vec<String>,
    aliases: &mut Vec<String>,
    ctes: &mut std::collections::HashSet<String>,
) {
    match expr {
        sqlparser::ast::SetExpr::Select(select) => {
            extract_tables_and_aliases_from_select(select, tables, aliases, ctes);
        }
        sqlparser::ast::SetExpr::Query(query) => {
            extract_tables_and_aliases_from_query(query, tables, aliases, ctes);
        }
        sqlparser::ast::SetExpr::SetOperation { left, right, .. } => {
            extract_tables_and_aliases_from_set_expr(left, tables, aliases, ctes);
            extract_tables_and_aliases_from_set_expr(right, tables, aliases, ctes);
        }
        _ => {}
    }
}

fn extract_tables_and_aliases_from_select(
    select: &sqlparser::ast::Select,
    tables: &mut Vec<String>,
    aliases: &mut Vec<String>,
    ctes: &mut std::collections::HashSet<String>,
) {
    for table_with_joins in &select.from {
        extract_tables_and_aliases_from_table_factor(&table_with_joins.relation, tables, aliases, ctes);
        for join in &table_with_joins.joins {
            extract_tables_and_aliases_from_table_factor(&join.relation, tables, aliases, ctes);
        }
    }
    if let Some(expr) = &select.selection {
        extract_tables_and_aliases_from_expr(expr, tables, aliases, ctes);
    }
    for proj in &select.projection {
        match proj {
            sqlparser::ast::SelectItem::UnnamedExpr(expr) |
            sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => {
                extract_tables_and_aliases_from_expr(expr, tables, aliases, ctes);
            }
            _ => {}
        }
    }
}

fn extract_tables_and_aliases_from_table_factor(
    factor: &sqlparser::ast::TableFactor,
    tables: &mut Vec<String>,
    aliases: &mut Vec<String>,
    ctes: &mut std::collections::HashSet<String>,
) {
    match factor {
        sqlparser::ast::TableFactor::Table { name, alias, .. } => {
            let table_name = name.to_string();
            tables.push(table_name);
            if let Some(table_alias) = alias {
                aliases.push(table_alias.name.value.clone());
            }
        }
        sqlparser::ast::TableFactor::Derived { subquery, alias, .. } => {
            extract_tables_and_aliases_from_query(subquery, tables, aliases, ctes);
            if let Some(table_alias) = alias {
                aliases.push(table_alias.name.value.clone());
            }
        }
        sqlparser::ast::TableFactor::NestedJoin { table_with_joins, alias, .. } => {
            extract_tables_and_aliases_from_table_factor(&table_with_joins.relation, tables, aliases, ctes);
            for join in &table_with_joins.joins {
                extract_tables_and_aliases_from_table_factor(&join.relation, tables, aliases, ctes);
            }
            if let Some(table_alias) = alias {
                aliases.push(table_alias.name.value.clone());
            }
        }
        _ => {}
    }
}

fn extract_tables_and_aliases_from_expr(
    expr: &sqlparser::ast::Expr,
    tables: &mut Vec<String>,
    aliases: &mut Vec<String>,
    ctes: &mut std::collections::HashSet<String>,
) {
    match expr {
        sqlparser::ast::Expr::Subquery(query) => {
            extract_tables_and_aliases_from_query(query, tables, aliases, ctes);
        }
        sqlparser::ast::Expr::InSubquery { subquery, expr, .. } => {
            extract_tables_and_aliases_from_expr(expr, tables, aliases, ctes);
            extract_tables_and_aliases_from_query(subquery, tables, aliases, ctes);
        }
        sqlparser::ast::Expr::Exists { subquery, .. } => {
            extract_tables_and_aliases_from_query(subquery, tables, aliases, ctes);
        }
        sqlparser::ast::Expr::BinaryOp { left, right, .. } => {
            extract_tables_and_aliases_from_expr(left, tables, aliases, ctes);
            extract_tables_and_aliases_from_expr(right, tables, aliases, ctes);
        }
        sqlparser::ast::Expr::UnaryOp { expr, .. } => {
            extract_tables_and_aliases_from_expr(expr, tables, aliases, ctes);
        }
        sqlparser::ast::Expr::Nested(expr) => {
            extract_tables_and_aliases_from_expr(expr, tables, aliases, ctes);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prql_compile() {
        let prql = "from input | select {col1, col2}";
        let sql = compile(prql, &Options::default().no_format().no_signature()).unwrap();
        assert!(sql.contains("SELECT"));
    }

    #[test]
    fn test_validate_prql_ok() {
        let prql = "from table_a | join table_b (==col_a) | select {table_a.col_a, table_b.col_b}";
        let mut err_out = std::ptr::null_mut();
        let query_cstr = CString::new(prql).unwrap();
        let res_ptr = validate_prql(query_cstr.as_ptr(), &mut err_out);
        assert!(err_out.is_null(), "Expected no error, got: {:?}", unsafe { CString::from_raw(err_out) });
        assert!(!res_ptr.is_null());
        let res_cstr = unsafe { CString::from_raw(res_ptr) };
        let res_str = res_cstr.to_str().unwrap();
        assert_eq!(res_str, "table_a,table_b;");
    }

    #[test]
    fn test_validate_prql_with_alias() {
        let prql = "from table_a | join t=table_b (==col_a) | select {table_a.col_a, t.col_b}";
        let mut err_out = std::ptr::null_mut();
        let query_cstr = CString::new(prql).unwrap();
        let res_ptr = validate_prql(query_cstr.as_ptr(), &mut err_out);
        assert!(err_out.is_null(), "Expected no error, got: {:?}", unsafe { CString::from_raw(err_out) });
        assert!(!res_ptr.is_null());
        let res_cstr = unsafe { CString::from_raw(res_ptr) };
        let res_str = res_cstr.to_str().unwrap();
        assert_eq!(res_str, "table_a,table_b;t");
    }

    #[test]
    fn test_validate_prql_err() {
        let prql = "from table_a | select invalid_syntax {";
        let mut err_out = std::ptr::null_mut();
        let query_cstr = CString::new(prql).unwrap();
        let res_ptr = validate_prql(query_cstr.as_ptr(), &mut err_out);
        assert!(res_ptr.is_null());
        assert!(!err_out.is_null());
        let err_cstr = unsafe { CString::from_raw(err_out) };
        let err_str = err_cstr.to_str().unwrap();
        assert!(err_str.contains("PRQL compilation error") || err_str.contains("Syntax Error"));
    }
}
