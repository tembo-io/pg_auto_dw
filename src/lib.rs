pub use pgrx::prelude::*;
// use serde::{Deserialize, Serialize};
// use pgrx::{info, spi, IntoDatum};

pgrx::pg_module_magic!();

mod setup;

#[pg_extern]
fn hello_pg_auto_dw() -> &'static str {
    "Hello, pg_auto_dw"
}

#[pg_extern]
fn series_hello_table() -> Result<
    TableIterator<
        'static,
        (
            name!(id, Result<Option<i32>, pgrx::spi::Error>),
            name!(greeting, Result<Option<String>, pgrx::spi::Error>)
        )
    >,
    spi::Error,
> {
    let query = "SELECT 1 as id, 'hello' as greeting FROM generate_series(1, 10)";
    info!("Employee Table");
    Spi::connect(|client| {
        Ok(client
            .select(query, None, None)?
            .map(|row| (row["id"].value(), row["greeting"].value()))
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
