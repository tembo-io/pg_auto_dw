use pgrx::bgworkers::*;
use pgrx::prelude::*;

use std::time::Duration;
use tokio::runtime::Runtime;

use crate::queries;

use crate::service::ollama_client;
use pgrx::Json;

use crate::model::source_objects;


// TODO: Create initial pattern for injection of public schema.

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
            let result: Result<Vec<source_objects::SourceTablePrompt>, pgrx::spi::Error> = BackgroundWorker::transaction(|| {
                Spi::connect(|client| {
                    log!("Client BG Worker - Source Objects JSON Pulling.");
                    let source_objects_json = client.select(queries::SOURCE_OBJECTS_JSON, None, None)?;
                    let mut v_source_table_prompts: Vec<source_objects::SourceTablePrompt> = Vec::new();
                    for source_object_json in source_objects_json {

                        let table_oid = source_object_json.get_datum_by_ordinal(1)?.value::<u32>()?.unwrap();
                        let table_column_links = source_object_json.get_datum_by_ordinal(2)?.value::<pgrx::Json>()?.unwrap();
                        let table_details = source_object_json.get_datum_by_ordinal(3)?.value::<pgrx::Json>()?.unwrap();

                        let source_table_prompt = source_objects::SourceTablePrompt{
                                                                                        key: table_oid, 
                                                                                        table_column_links: table_column_links, 
                                                                                        table_details: table_details
                                                                                    };
                        
                        v_source_table_prompts.push(source_table_prompt)
                    }
                    Ok(v_source_table_prompts)
                })
            });

            let v_source_table_prompts = result.unwrap_or_else(|e| panic!("got an error: {}", e));

            for source_table_prompt in v_source_table_prompts {
                let json_prompt_pretty = serde_json::to_string_pretty(&source_table_prompt.table_details).expect("Failed to convert JSON to pretty string");
                log!("JSON pretty {}", json_prompt_pretty);

                let response_json: Option<serde_json::Value> = match ollama_client::send_request(json_prompt_pretty.as_str()).await {
                    Ok(response_json) => {
                        log!("Ollama client request successful.");
                        Some(response_json)
                    },
                    Err(e) => {
                        log!("Error in Ollama client request: {}", e);
                        None
                    }
                };

                
                // // Pushing to Table
                // BackgroundWorker::transaction(|| {
                //     Spi::connect(|client| {
                //         log!("About to Push this to PG: Json {}", serde_json::to_string_pretty(&response_json).unwrap());
                        
                //         // Deserialize JSON string to TableDetails struct
                //         let table_details: TableDetails = serde_json::from_str(json_str)?;
                //         response_json.
                //         // client.update(query, limit, args)

                //         // Check if the Option is Some and then deserialize
                //         if let Some(json_value) = json_value {
                //             let table_details: TableDetails = serde_json::from_value(json_value)?;
                //             println!("{:?}", table_details);
                //         } else {
                //             println!("No JSON value provided");
                //         }
                //     })
                // });
                
            }
        });
    }
    log!("Goodbye from inside the {} BGWorker! ", BackgroundWorker::get_name());
}