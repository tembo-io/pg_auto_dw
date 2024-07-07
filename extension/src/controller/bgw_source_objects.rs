use pgrx::bgworkers::*;
use pgrx::prelude::*;

use std::time::Duration;

use crate::queries;
use crate::utility::guc;

#[pg_guard]
#[no_mangle]
pub extern "C" fn background_worker_source_objects(_arg: pg_sys::Datum) {

    let optional_database_name = guc::get_guc(guc::PgAutoDWGuc::DatabaseName);

    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    BackgroundWorker::connect_worker_to_spi(optional_database_name.as_deref(), None);

    while BackgroundWorker::wait_latch(Some(Duration::from_secs(10))) {
        let result: Result<(), pgrx::spi::Error> = BackgroundWorker::transaction(|| {
            Spi::connect(|mut client| {

                let table_check_results: Result<spi::SpiTupleTable, spi::SpiError> = 
                    client.select("SELECT table_name FROM information_schema.tables WHERE table_schema = 'auto_dw' AND table_name = 'source_objects'", None, None);
                match table_check_results {
                    Ok(table_check) => {
                        if table_check.len() > 0 {
                            log!("TABLE AUTO_DW.SOURCE_OJBECTS exists. Proceeding with update.");
                            client.update(
                                queries::source_object_dw(
                                    "a^", 
                                    "a^", 
                                    "a^", 
                                    "a^", 
                                    "a^", 
                                    "a^"
                                ).as_str(),
                                None,
                                None,
                            )?;
                        } else {
                            panic!("TABLE AUTO_DW.SOURCE_OJBECTS not found. PG_AUTO_DW Extension may need to be installed.");
                        }
                    },
                    Err(e) => {
                        log!("Error checking TABLE AUTO_DW.SOURCE_OJBECTS: {:?}", e);
                    }
                }
                Ok(())
            })
        });
        result.unwrap_or_else(|e| panic!("got an error: {}", e));
    }
}