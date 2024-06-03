pub use pgrx::prelude::*;

pgrx::pg_module_magic!();

mod setup;
mod queries;
mod bgworker;

#[pg_extern]
fn hello_pg_auto_dw() -> &'static str {
    "Hello, pg_auto_dw"
}

#[pg_extern(name="go")]
fn go(flag: &str, status: &str) -> &'static str {
    let _ = flag;
    let _ = status;
    _ = Spi::run(queries::SELLER_DV);
    queries::GO_OUTPUT
}

#[pg_extern(name="go")]
fn go_no() -> &'static str {
    _ = Spi::run(queries::SELLER_DV);
    queries::GO_OUTPUT
}

#[pg_extern]
fn source_push() -> &'static str {
    _ = Spi::run(queries::SOURCE_OBJECTS_INIT);
    "Pushed"
}

#[pg_extern]
fn source_update() -> &'static str {
    _ = Spi::run(queries::source_object_dw( 
        "a^", 
        "a^", 
        "a^", 
        "a^", 
        "a^", 
        "a^")
        .as_str());
    "soure_objects_updated"
}

#[pg_extern]
fn source_include(  schema_pattern_include: &str, 
                    table_pattern_include: default!(Option<&str>, "NULL"), 
                    column_pattern_include: default!(Option<&str>, "NULL")) -> &'static str {
    // Include Patterns
    let schema_pattern_include: &str = schema_pattern_include;
    let table_pattern_include: &str = table_pattern_include.unwrap_or(".*");
    let column_pattern_include: &str = column_pattern_include.unwrap_or(".*");
    // Exclude Patterns
    let schema_pattern_exclude: &str = "a^";
    let table_pattern_exclude: &str = "a^";
    let column_pattern_exclude: &str = "a^";
    _ = Spi::run(queries::source_object_dw( schema_pattern_include, 
                                            table_pattern_include, 
                                            column_pattern_include, 
                                            schema_pattern_exclude, 
                                            table_pattern_exclude, 
                                            column_pattern_exclude)
                                            .as_str());
    "Pattern Excluded"
}

#[pg_extern]
fn source_exlude(   schema_pattern_exclude: &str, 
                    table_pattern_exclude: default!(Option<&str>, "NULL"), 
                    column_pattern_exclude: default!(Option<&str>, "NULL")) -> &'static str {
    let schema_pattern_include: &str = "a^";
    let table_pattern_include: &str = "a^";
    let column_pattern_include: &str = "a^";
    let schema_pattern_exclude: &str = schema_pattern_exclude;
    let table_pattern_exclude: &str = table_pattern_exclude.unwrap_or(".*");
    let column_pattern_exclude: &str = column_pattern_exclude.unwrap_or(".*");
    _ = Spi::run(queries::source_object_dw( schema_pattern_include, 
                                            table_pattern_include, 
                                            column_pattern_include, 
                                            schema_pattern_exclude, 
                                            table_pattern_exclude, 
                                            column_pattern_exclude)
                                            .as_str());
    "Pattern Excluded"
}

#[pg_extern]
fn source_table() -> Result<
    TableIterator<
        'static,
        (
            name!(schema, Result<Option<String>, pgrx::spi::Error>),
            name!(table, Result<Option<String>, pgrx::spi::Error>),
            name!(status, Result<Option<String>, pgrx::spi::Error>),
            name!(status_code, Result<Option<String>, pgrx::spi::Error>),
            name!(status_response, Result<Option<String>, pgrx::spi::Error>)
        )
    >,
    spi::Error,
> {
    let query: &str = queries::SOURCE_TABLE_SAMPLE;

    info!("Evaluation of TABLE customer");
    Spi::connect(|client| {
        Ok(client
            .select(query, None, None)?
            .map(|row| (
                row["schema"].value(), 
                row["table"].value(), 
                row["status"].value(),
                row["status_code"].value(),
                row["status_response"].value())
            )
            .collect::<Vec<_>>())
    })
    .map(TableIterator::new)
}

#[pg_extern]
fn source_column() -> Result<
    TableIterator<
        'static,
        (
            name!(schema, Result<Option<String>, pgrx::spi::Error>),
            name!(table, Result<Option<String>, pgrx::spi::Error>),
            name!(column, Result<Option<String>, pgrx::spi::Error>),
            name!(status, Result<Option<String>, pgrx::spi::Error>),
            name!(confidence_level, Result<Option<String>, pgrx::spi::Error>),
            name!(status_response, Result<Option<String>, pgrx::spi::Error>)
        )
    >,
    spi::Error,
> {
    let query: &str = queries::SOURCE_COLUMN_SAMPLE;

    info!("Evaluation of TABLE customer");
    Spi::connect(|client| {
        Ok(client
            .select(query, None, None)?
            .map(|row| (
                row["schema"].value(), 
                row["table"].value(), 
                row["column"].value(), 
                row["status"].value(),
                row["confidence_level"].value(),
                row["status_response"].value())
            )
            .collect::<Vec<_>>())
    })
    .map(TableIterator::new)
}

#[pg_extern]
fn evaluate_table(table: &str) -> Result<
    TableIterator<
        'static,
        (
            name!(schema_name, Result<Option<String>, pgrx::spi::Error>),
            name!(table_name, Result<Option<String>, pgrx::spi::Error>),
            name!(column_name, Result<Option<String>, pgrx::spi::Error>),
            name!(column_cat, Result<Option<String>, pgrx::spi::Error>),
            name!(confidence_level, Result<Option<String>, pgrx::spi::Error>),
            name!(is_overridden, Result<Option<bool>, pgrx::spi::Error>)
        )
    >,
    spi::Error,
> {
    let schema = "public";
    // let table = "customer";

    let query_string = format!(r#"
        SELECT schema_name, 
            table_name, 
            column_name, 
            column_cat, 
            confidence_level, 
            is_overridden 
        FROM auto_dw.table_column_cat
        WHERE 
            schema_name = '{}' AND 
            table_name = '{}'
        "#, schema, table);
    
    let query: &str = query_string.as_str();

    info!("Evaluation of TABLE customer");
    Spi::connect(|client| {
        Ok(client
            .select(query, None, None)?
            .map(|row| (
                row["schema_name"].value(), 
                row["table_name"].value(), 
                row["column_name"].value(),
                row["column_cat"].value(),
                row["confidence_level"].value(),
                row["is_overridden"].value())
            )
            .collect::<Vec<_>>())
    })
    .map(TableIterator::new)
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
