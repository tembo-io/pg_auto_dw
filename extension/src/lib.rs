mod controller; // Coordinates application logic and model-service interactions.
mod model;      // Defines data structures and data-related methods.
mod utility;    // Initialization, Configuration Management, and External Services

pub use pgrx::prelude::*;
use uuid::Uuid;

pgrx::pg_module_magic!();

use model::queries;

#[pg_extern(name="go")]
fn go_default() -> String {
    let build_id = Uuid::new_v4();
    let message = format!("Build ID: {} | Data warehouse tables are currently being built.", build_id);
    info!("{}", message);
    let build_id = build_id.to_string();
    let build_flag = "Build";
    let build_status = "RTD";
    let status = "Ready to Deploy";
    let query_insert = &queries::insert_into_build_call(&build_id, &build_flag, &build_status, &status);
    _ = Spi::run(query_insert);
    let query_build_pull = &queries::build_object_pull(&build_id);
    controller::dv_builder::build_dv(&build_id, query_build_pull);

    message
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
    "Pattern Included"
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
    let query: &str = queries::SOURCE_COLUMN;

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

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    // TODO: Unit Testing
    #[pg_test]
    fn go_default() {
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
