use pgrx::bgworkers::*;
use pgrx::prelude::*;
use std::time::Duration;
use tokio::runtime::Runtime;


use crate::queries;
use crate::ollama_client;

// TODO: Create initial pattern for injection of public schema.
// TODO: Break after X tries w/out Schema.

#[pg_guard]
pub extern "C" fn _PG_init() {
    BackgroundWorkerBuilder::new("Background Worker Source Object Update")
        .set_function("background_worker_main")
        .set_library("pg_auto_dw")
        .enable_spi_access()
        .load();

    BackgroundWorkerBuilder::new("Background Worker Ollama Client")
    .set_function("background_worker_ollama_client_main")
    .set_library("pg_auto_dw")
    .enable_spi_access()
    .load();
}

#[pg_guard]
#[no_mangle]
pub extern "C" fn background_worker_main(_arg: pg_sys::Datum) {

    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    BackgroundWorker::connect_worker_to_spi(Some("pg_auto_dw"), None);

    while BackgroundWorker::wait_latch(Some(Duration::from_secs(10))) {
        let result: Result<(), pgrx::spi::Error> = BackgroundWorker::transaction(|| {
            Spi::connect(|mut client| {
                log!("Client BG Worker - Source Objects to update.");
                log!("Checking if TABLE AUTO_DW.SOURCE_OJBECTS exists.");
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
                            log!("Client BG Worker - Source Objects updated.");
                        } else {
                            log!("TABLE AUTO_DW.SOURCE_OJBECTS does not exist. Skipping update.");
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
log!("Goodbye from inside the {} BGWorker! ", BackgroundWorker::get_name());
}

#[pg_guard]
#[no_mangle]
pub extern "C" fn background_worker_ollama_client_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    BackgroundWorker::connect_worker_to_spi(Some("pg_auto_dw"), None);

    // Initialize Tokio runtime
    let runtime = Runtime::new().expect("Failed to create Tokio runtime");
    while BackgroundWorker::wait_latch(Some(Duration::from_secs(90))) {
        runtime.block_on(async {
            let new_json = r#"
            {
              "Schema Name": "public",
              "Table Name": "seller",
              "Column Details": [
                "Column No: 2 Named: city of type: character varying(255)",
                "Column No: 3 Named: state of type: character(2)",
                "Column No: 4 Named: zip_5 of type: character varying(10)",
                "Column No: 1 Named: seller_id of type: uuid And is a primary key."
              ]
            }
            "#;
            match ollama_client::send_request(new_json).await {
                Ok(_) => log!("Ollama client request successful."),
                Err(e) => log!("Error in Ollama client request: {}", e),
            }
        });
    }
    log!("Goodbye from inside the {} BGWorker! ", BackgroundWorker::get_name());
}