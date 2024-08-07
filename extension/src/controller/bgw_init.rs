use pgrx::bgworkers::*;
use pgrx::prelude::*;

use crate::utility::guc;

#[pg_guard]
pub extern "C" fn _PG_init() {

    guc::init_guc();

    let database_name_o = guc::get_guc(guc::PgAutoDWGuc::DatabaseName);

    match database_name_o {
        Some(_database_name) => {

            BackgroundWorkerBuilder::new("Background Worker Source Object Update")
            .set_function("background_worker_source_objects")
            .set_library("pg_auto_dw")
            .enable_spi_access()
            .load();

           BackgroundWorkerBuilder::new("Background Worker Transformer Client")
            .set_function("background_worker_transformer_client")
            .set_library("pg_auto_dw")
            .enable_spi_access()
            .load();
        }
        None => {
            log!("Database Name for this extension has not been set.");
        }
    }
}






