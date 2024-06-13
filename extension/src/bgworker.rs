use pgrx::bgworkers::*;
use pgrx::prelude::*;

use std::time::Duration;
use tokio::runtime::Runtime;

use crate::queries;

use crate::service::ollama_client;

use crate::model::source_objects;

use serde::de::DeserializeOwned;
use serde_json::from_value;



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
        

            // Load Prompts into Results
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

            // Get Prompts for Processing
            let v_source_table_prompts = result.unwrap_or_else(|e| panic!("got an error: {}", e));

            // Process Each Prompt
            for source_table_prompt in v_source_table_prompts {
                
                let table_details_json_str = serde_json::to_string_pretty(&source_table_prompt.table_details).expect("Failed to convert JSON Table Detailsto pretty string");
                log!("JSON pretty Table Details {}", table_details_json_str);

                let table_column_link_json_str = serde_json::to_string_pretty(&source_table_prompt.table_column_links).expect("Failed to convert JSON Column Links to pretty string");
                log!("JSON pretty Table Column Links{}", table_column_link_json_str);
                let table_column_links_o: Option<source_objects::TableLinks> = serde_json::from_str(&table_column_link_json_str).ok();
                log!("Test 456 {:?}", table_column_links_o);

                // let table_column_links_o: Option<source_objects::TableLinks> = serde_json::from_value(source_table_prompt.table_column_links).ok();
                
                // Define generation_json_o outside the runtime.block_on block
                let mut generation_json_o: Option<serde_json::Value> = None;

                // Run the async block
                runtime.block_on(async {
                    // Get Generation
                    generation_json_o = match ollama_client::send_request(table_details_json_str.as_str()).await {
                        Ok(response_json) => {
                            log!("Ollama client request successful.");
                            Some(response_json)
                        },
                        Err(e) => {
                            log!("Error in Ollama client request: {}", e);
                            None
                        }
                    };
                });

                log!("About to Push this to PG: Json {}", serde_json::to_string_pretty(&generation_json_o).unwrap());

                let generation_table_detail_o: Option<source_objects::GenerationTableDetail> = deserialize_option(generation_json_o);
                
                let table_column_links = table_column_links_o.unwrap();
                let generation_table_detail = generation_table_detail_o.unwrap();

                // Build the SQL INSERT statement
                let mut insert_sql = String::from("INSERT INTO auto_dw.transformer_responses (fk_source_objects, model_name, category, confidence_score, reason) VALUES ");

                for (index, column_link) in table_column_links.column_links.iter().enumerate() {
                    log!("{}: Table Column Link Key: {} Ordinal Position {}", index, column_link.pk_source_objects, column_link.column_ordinal_position);

                    let not_last = index != table_column_links.column_links.len() - 1;

                    let index_o = generation_table_detail.response_column_details.iter().position(|r| r.column_no == column_link.column_ordinal_position);
                    match index_o {
                        Some(index) => {
                            let column_detail = &generation_table_detail.response_column_details[index];

                            let column_no = &column_detail.column_no;
                            let category = &column_detail.category.replace("'", "''");
                            let confidence_score = &column_detail.confidence;
                            let reason = &column_detail.reason.replace("'", "''");
                            let pk_source_objects = column_link.pk_source_objects;
                            
                            let model_name = "Mixtral";

                            log!("Key {} has values Column No: {} Category: {} Confidence: {} Reason: {}", pk_source_objects, column_no, category, confidence_score, reason);
                            
                            //(11, 'Mistral', 'Business Key', 0.99, 'The column ''seller_id'' is a primary key, which is a strong indicator of a Business Key.',
                            if not_last {
                                // This is the last iteration
                                insert_sql.push_str(&format!("({}, '{}', '{}', {}, '{}'),", pk_source_objects, model_name, category, confidence_score, reason));
                            } else {
                                insert_sql.push_str(&format!("({}, '{}', '{}', {}, '{}');", pk_source_objects, model_name, category, confidence_score, reason));
                            }
                        }
                        None => {break;}
                    }
                }
                
                log!("Insert SQL: {}", insert_sql);
                
                // Push Generation to TABLE TRANSFORMER_RESPONSES 
                BackgroundWorker::transaction(|| {
                    Spi::connect(|mut client| {
                        _ = client.update(insert_sql.as_str(), None, None);
                        log!("TABLE TRANSFORMER_RESPONSES UPDATTED!");
                    })
                });
                
            }
        
    }
    log!("Goodbye from inside the {} BGWorker! ", BackgroundWorker::get_name());
}

fn deserialize_option<T>(json_option: Option<serde_json::Value>) -> Option<T>
where
    T: DeserializeOwned
{
    json_option.and_then(|json| {
        from_value::<T>(json).ok()
    })
}