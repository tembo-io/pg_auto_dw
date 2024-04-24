use pgrx::prelude::*;
// use serde::{Deserialize, Serialize};
// use pgrx::{info, spi, IntoDatum};

pgrx::pg_module_magic!();

// Creating Extension TABLES ** Purely ** as a Hello World + Example.

// TABLE_STATUS
// Used pulls table in from the information schema for processing, starting status new.
extension_sql!( r#"

CREATE TABLE table_status (
    id SERIAL PRIMARY KEY,
    schema_name VARCHAR(255) NOT NULL DEFAULT 'public',
    table_name VARCHAR(255) NOT NULL,
    status_text TEXT NOT NULL DEFAULT 'new' CHECK (status_text IN ('new', 'changed', 'ready', 'processed')),
    usage_indicator BOOLEAN NOT NULL DEFAULT TRUE
);
"#,
    name = "create_table_status_table",
);

// TABLE_COLUMN_CAT
// Used to categorize attribute either manually or by LLM.
// If overridden then confidence_level = na and is_overridden = true.
extension_sql!( r#"
CREATE TABLE table_column_cat (
    id SERIAL PRIMARY KEY,
    schema_name VARCHAR(255) NOT NULL DEFAULT 'public',
    table_name VARCHAR(255) NOT NULL,
    column_name VARCHAR(255) NOT NULL,
    column_cat VARCHAR(255) NOT NULL DEFAULT 'na',
    confidence_level VARCHAR(255) NOT NULL DEFAULT 'na' CHECK (confidence_level IN ('high', 'medium', 'low', 'na')),
    is_overridden BOOLEAN NOT NULL DEFAULT FALSE
);
"#,
    name = "create_table_column_cat_stats_table",
);
// - METADATA
extension_sql!( r#"

CREATE TABLE metadata (
    id SERIAL PRIMARY KEY
);
"#,
    name = "create_metadata_table",
);

// Creating Sample TABLES
// - CUSTOMER
extension_sql!( r#"
CREATE TABLE public.customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    phone_number VARCHAR(15),
    date_of_birth DATE,
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
"#,
    name = "create_example_tables",
);

#[pg_extern]
fn hello_pg_auto_dw() -> &'static str {
    "Hello, pg_auto_dw"
}

#[pg_extern]
fn list_days_of_week() -> &'static str {
    "Mon, Tue, Wed, Thur, Fri, Sat, Sun"
}

#[pg_extern]
fn sum_vec(input: Vec<Option<i32>>) -> i32 {

    let mut sum: i32 = 0;
    for i in input {
        if let Some(a) = i {
            sum += a;
        };    
    }

    sum
}

#[pg_extern]
fn employees() -> TableIterator<'static,
        (
            name!(id, i64),
            name!(dept_code, String),
            name!(full_text, &'static str)
        )
> {
    info!("Employee Table");
    TableIterator::new(vec![
        (42, "ARQ".into(), "John Hammond"),
        (87, "EGA".into(), "Mary Kinson"),
        (3,  "BLA".into(), "Perry Johnson"),
    ])
}


#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_hello_pg_auto_dw() {
        assert_eq!("Hello, pg_auto_dw", crate::hello_pg_auto_dw());
    }

}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
