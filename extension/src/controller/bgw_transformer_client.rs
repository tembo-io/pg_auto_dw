use pgrx::bgworkers::*;
use pgrx::prelude::*;

use std::time::Duration;
use std::collections::HashMap;
use tokio::runtime::Runtime;
use serde::Deserialize;

use crate::model::*;
use crate::utility::transformer_client;
use crate::utility::guc;
use regex::Regex;

const MAX_TRANSFORMER_RETRIES: u8 = 3; // TODO: Set in GUC

#[pg_guard]
#[no_mangle]
pub extern "C" fn background_worker_transformer_client(_arg: pg_sys::Datum) {

    let database_name_string = guc::get_guc(guc::PgAutoDWGuc::DatabaseName);
    let database_name_o: Option<&str> = database_name_string.as_deref();

    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    BackgroundWorker::connect_worker_to_spi(database_name_o, None);

    // Initialize Tokio runtime
    let runtime = Runtime::new().expect("Failed to create Tokio runtime");

    while BackgroundWorker::wait_latch(Some(Duration::from_secs(10))) {

            extension_log("BGWorker: Transformer Client", "INFO", "Beginning Transformer Background Process.");
        
            // Load Prompts into Results
            let result: Result<Vec<source_objects::SourceTablePrompt>, pgrx::spi::Error> = BackgroundWorker::transaction(|| {
                Spi::connect(|client| {
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
                log!("Starting Loop for Table Processing.");
                let table_details_json_str = serde_json::to_string_pretty(&source_table_prompt.table_details).expect("Failed to convert JSON Table Details to pretty string");

                let table_column_link_json_str = serde_json::to_string_pretty(&source_table_prompt.table_column_links).expect("Failed to convert JSON Column Links to pretty string");
                let table_column_links_o: Option<source_objects::TableLinks> = serde_json::from_str(&table_column_link_json_str).ok();

                let columns = extract_column_numbers(&table_details_json_str);

                // Table Business Key Component Identification
                let mut generation_json_business_key_component_identification: Option<serde_json::Value> = None;
                let mut generation_json_business_key_name: Option<serde_json::Value> = None;
                let mut business_key_component_identification: HashMap<&u32, BusinessKeyComponentIdentification> = HashMap::new();
                let mut business_key_name: HashMap<&u32, BusinessKeyName> = HashMap::new();

                // Evaluate Attributes
                for column in &columns {
                    let mut retries = 0;
                    let mut hints = String::new();

                    while retries < MAX_TRANSFORMER_RETRIES {
                        runtime.block_on(async {
                            generation_json_business_key_component_identification = 
                                match transformer_client::send_request(
                                    table_details_json_str.as_str(), 
                                    prompt_template::PromptTemplate::BKComponentIdentification, 
                                    column, 
                                    &hints).await {
                                Ok(response_json) => {
                                    Some(response_json)
                                },
                                Err(e) => {
                                    log!("Error in transformer request, malformed or timed out: {}", e);
                                    hints = format!("Hint: Please ensure you provide a JSON response only.  This is your {} attempt.", retries + 1);
                                    None
                                }
                            };
                        });

                        if generation_json_business_key_component_identification.is_none() {
                            retries += 1;
                            continue; // Skip to the next iteration
                        }

                        match serde_json::from_value::<BusinessKeyComponentIdentification>(generation_json_business_key_component_identification.clone().unwrap()) {
                            Ok(bki) => {
                                business_key_component_identification.insert(column, bki);
                                break; // Successfully Decoded
                            }
                            Err(e) => {
                                log!("Error JSON JSON Structure not of type DescriptorSensitive: {}", e);
                            }
                        }
                        retries += 1;
                        log!("Transformer Retry No: {retries}");
                    }
                }

                // Generate Name if Identified as BK
                for column in &columns {
                    let mut retries = 0;
                    let mut hints = String::new();

                    match business_key_component_identification.get(column) {
                        Some(bkci) => {
                            if bkci.business_key_component_identification.is_business_key_component {
                                // Identify BK Name
                                while retries < MAX_TRANSFORMER_RETRIES {
                                    runtime.block_on(async {
                                        generation_json_business_key_name = 
                                          match transformer_client::send_request(table_details_json_str.as_str(), prompt_template::PromptTemplate::BKName, &column, &hints).await {
                                            Ok(response_json) => {
                                                Some(response_json)
                                            },
                                            Err(e) => {
                                                log!("Error in transformer request, malformed or timed out: {}", e);
                                                hints = format!("Hint: Please ensure you provide a JSON response only.  This is your {} attempt.", retries + 1);
                                                None
                                            }
                                          };
                                    });

                                    if generation_json_business_key_name.is_none() {
                                        retries += 1;
                                        continue; // Skip to the next iteration
                                    }

                                    match serde_json::from_value::<BusinessKeyName>(generation_json_business_key_name.clone().unwrap()) {
                                        Ok(bkn) => {
                                            business_key_name.insert(column, bkn);
                                            break; // Successfully Decoded
                                        }
                                        Err(e) => {
                                            log!("Error JSON JSON Structure not of type BusinessKeyName: {}", e);
                                        }
                                    }

                                    retries += 1;
                                }
                            } else {
                                continue; // Go do next column
                            }
                        }
                        None => panic!("All columns should have been checked for business keys.  No BusinessKeyComponetIdentification Struct Found."),
                    }
                }

                // Identity Descriptor - Sensitive
                // let mut generation_json_descriptors_sensitive: HashMap<&u32, Option<serde_json::Value>> = HashMap::new();
                let mut descriptors_sensitive: HashMap<&u32, DescriptorSensitive> = HashMap::new();
                let mut generation_json_descriptor_sensitive: Option<serde_json::Value> = None;
                for column in &columns {
                    let mut retries = 0;
                    let mut hints = String::new();
                    while retries < MAX_TRANSFORMER_RETRIES {   
                    // Run the async block
                        runtime.block_on(async {
                            // Get Generation
                            generation_json_descriptor_sensitive = 
                                match transformer_client::send_request(
                                    table_details_json_str.as_str(), 
                                    prompt_template::PromptTemplate::DescriptorSensitive, 
                                    column, 
                                    &hints).await {
                                Ok(response_json) => {
                                    Some(response_json)
                                },
                                Err(e) => {
                                    log!("Error in transformer request, malformed or timed out: {}", e);
                                    hints = format!("Hint: Please ensure you provide a JSON response only.  This is your {} attempt.", retries + 1);
                                    None
                                }
                            };
                            // generation_json_descriptors_sensitive.insert(column, generation_json_descriptor_sensitive);
                        });

                        if generation_json_descriptor_sensitive.is_none() {
                            retries += 1;
                            continue; // Skip to the next iteration
                        }

                        match serde_json::from_value::<DescriptorSensitive>(generation_json_descriptor_sensitive.clone().unwrap()) {
                            Ok(des) => {
                                // business_key_name_opt = Some(des);
                                descriptors_sensitive.insert(column, des);
                                break; // Successfully Decoded
                            }
                            Err(e) => {
                                log!("Error JSON JSON Structure not of type DescriptorSensitive: {}", e);
                            }
                        }

                        retries += 1;
                    }
                }
                
                let table_column_links = table_column_links_o.unwrap();

               // Build the SQL INSERT statement
                let mut insert_sql = String::from("INSERT INTO auto_dw.transformer_responses (fk_source_objects, model_name, category, business_key_name, confidence_score, reason) VALUES ");

                for (index, column) in columns.iter().enumerate() {

                    let last = {index == table_column_links.column_links.len() - 1};

                    match (business_key_component_identification.get(column), business_key_name.get(column)) {
                        (Some(business_key_component_identification), Some(business_key_name)) => {
                            let category = "Business Key Part";
                            // Calculate the overall confidence score by taking the minimum of the confidence values
                            // for the identified business key and the business key name. This approach is chosen to 
                            // ensure that the overall confidence reflects the weakest link, avoiding inflation of 
                            // the confidence score when one value is significantly lower than the other.
                            let confidence_score = 
                                business_key_component_identification.business_key_component_identification.confidence_value.min(
                                    business_key_name.business_key_name_values.confidence_value);
                            let bk_name = &business_key_name.business_key_name_values.name;
                            let bk_identified_reason = &business_key_component_identification.business_key_component_identification.reason;
                            let bk_name_reason = &business_key_name.business_key_name_values.reason;
                            let reason = format!("BK Identified Reason: {}, BK Naming Reason: {}", bk_identified_reason, bk_name_reason);
                            let model_name_owned = guc::get_guc(guc::PgAutoDWGuc::Model).expect("MODEL GUC is not set.");
                            let model_name = model_name_owned.as_str();

                            let pk_source_objects: i32;

                            if let Some(pk_source_objects_temp) = table_column_links.find_pk_source_objects(column.clone() as i32) {
                                pk_source_objects = pk_source_objects_temp;
                            } else {
                                println!("No match found for column_ordinal_position: {}", column);
                                panic!()
                            }
    
                            if !last {
                                insert_sql.push_str(&format!("({}, '{}', '{}', '{}', {}, '{}'),", pk_source_objects, model_name, category, bk_name.replace(" ", "_"), confidence_score, reason.replace("'", "''")));
                            } else {
                                insert_sql.push_str(&format!("({}, '{}', '{}', '{}', {}, '{}');", pk_source_objects, model_name, category, bk_name.replace(" ", "_"), confidence_score, reason.replace("'", "''")));
                            }
        
                        }
                        _ => { // Not Identified as BKs
                            let pk_source_objects: i32; 
                            let mut category = "Descriptor";
                            let mut confidence_score: f64 = 1.0;
                            let bk_name = "NA";
                            let mut reason = "Defaulted of category 'Descriptor' maintained.".to_string();
                            let model_name_owned = guc::get_guc(guc::PgAutoDWGuc::Model).expect("MODEL GUC is not set.");
                            let model_name = model_name_owned.as_str();
                            
                            if let Some(pk_source_objects_temp) = table_column_links.find_pk_source_objects(column.clone() as i32) {
                                pk_source_objects = pk_source_objects_temp;
                            } else {
                                println!("No match found for column_ordinal_position: {}", column);
                                panic!()
                            }
                            
                            if let Some(descriptor_sensitive) = descriptors_sensitive.get(&column) {
                                if descriptor_sensitive.descriptor_sensitive_values.is_pii && (descriptor_sensitive.descriptor_sensitive_values.confidence_value > 0.5) {
                                    category = "Descriptor - Sensitive";
                                    confidence_score = descriptor_sensitive.descriptor_sensitive_values.confidence_value;
                                    reason = descriptor_sensitive.descriptor_sensitive_values.reason.clone();
                                }
                            } else {
                                log!("Teseting Can't find a response for {} in Descriptors Sensitive Hashmap.", column);
                            }
    
                            if !last {
                                insert_sql.push_str(&format!("({}, '{}', '{}', '{}', {}, '{}'),", pk_source_objects, model_name, category, bk_name.replace(" ", "_"), confidence_score, reason.replace("'", "''")));
                            } else {
                                insert_sql.push_str(&format!("({}, '{}', '{}', '{}', {}, '{}');", pk_source_objects, model_name, category, bk_name.replace(" ", "_"), confidence_score, reason.replace("'", "''")));
                            }
                        }
                    }
                }
                
                // Push Generation to TABLE TRANSFORMER_RESPONSES 
                BackgroundWorker::transaction(|| {
                    Spi::connect(|mut client| {
                        _ = client.update(insert_sql.as_str(), None, None);
                    })
                });
        }
        
    }
}

fn extension_log(process: &str, level: &str, message: &str) {

    let insert_statement = format!(r#"
                                            INSERT INTO auto_dw.log (process, level, message)
                                            VALUES ('{}', '{}', '{}');
                                        "#, process, level, message);

    BackgroundWorker::transaction(|| {
        Spi::connect(|mut client| {
            _ = client.update(insert_statement.as_str(), None, None);
        })
    });
}

fn extract_column_numbers(json_str: &str) -> Vec<u32> {
    // Define a regex to capture the column numbers
    let re = Regex::new(r"Column No: (\d+)").expect("Invalid regex");

    // Find all matches and collect the column numbers
    re.captures_iter(json_str)
        .filter_map(|caps| caps.get(1).map(|m| m.as_str().parse::<u32>().unwrap()))
        .collect()
}

#[derive(Deserialize, Debug)]
enum TableClassificationType {
    Hub,
    Link,
}

#[derive(Deserialize, Debug)]
struct BusinessKeyComponentIdentification {
    #[serde(rename = "Business Key Component Identification")]
    business_key_component_identification: BusinessKeyComponentIdentificationValues,
}

#[derive(Deserialize, Debug)]
struct BusinessKeyComponentIdentificationValues {
    #[serde(rename = "Is Business Key Component")]
    is_business_key_component: bool,
    #[serde(rename = "Confidence Value")]
    confidence_value: f64,
    #[serde(rename = "Reason")]
    reason: String,
}

#[derive(Deserialize, Debug)]
struct BusinessKeyName {
    #[serde(rename = "Business Key Name")]
    business_key_name_values: BusinessKeyNameValues,
}

#[derive(Deserialize, Debug)]
struct BusinessKeyNameValues {
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "Confidence Value")]
    confidence_value: f64,
    #[serde(rename = "Reason")]
    reason: String,
}

#[derive(Deserialize, Debug)]
struct DescriptorSensitive {
    #[serde(rename = "Descriptor - Sensitive")]
    descriptor_sensitive_values: DescriptorSensitiveValues,
}

#[derive(Deserialize, Debug)]
struct DescriptorSensitiveValues {
    #[serde(rename = "Is PII")]
    is_pii: bool,
    #[serde(rename = "Confidence Value")]
    confidence_value: f64,
    #[serde(rename = "Reason")]
    reason: String,
}

