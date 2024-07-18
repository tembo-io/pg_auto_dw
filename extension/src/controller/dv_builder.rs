use pgrx::prelude::*;
use uuid::Uuid;
use std::collections::HashMap;
use chrono::Utc;

use crate::model::queries;
use crate::utility::guc;
use crate::model::dv_transformer_schema::{self, BusinessKey};

pub fn build_dv(build_id: &String, dv_objects_query: &str) {

    let mut dv_transformer_objects_hm: HashMap<u32, Vec<TransformerObject>> = HashMap::new();

    Spi::connect(|client| 
        {
            let dv_transformer_objects_result = client.select(dv_objects_query, None, None);

            match dv_transformer_objects_result {

                Ok(dv_transformer_objects) => {

                    for dv_transformer_object in dv_transformer_objects {

                        let schema_name = dv_transformer_object.get_datum_by_ordinal(1).unwrap().value::<String>().unwrap().unwrap();
                        let table_name = dv_transformer_object.get_datum_by_ordinal(2).unwrap().value::<String>().unwrap().unwrap();
                        let column_category = dv_transformer_object.get_datum_by_ordinal(3).unwrap().value::<String>().unwrap().unwrap();
                        let business_key_name = dv_transformer_object.get_datum_by_ordinal(4).unwrap().value::<String>().unwrap().unwrap();
                        let column_name = dv_transformer_object.get_datum_by_ordinal(5).unwrap().value::<String>().unwrap().unwrap();
                        let column_type_name = dv_transformer_object.get_datum_by_ordinal(6).unwrap().value::<String>().unwrap().unwrap();
                        let system_id = dv_transformer_object.get_datum_by_ordinal(7).unwrap().value::<i64>().unwrap().unwrap();
                        let table_oid: u32 = dv_transformer_object.get_datum_by_ordinal(8).unwrap().value::<u32>().unwrap().unwrap();
                        let column_ordinal_position = dv_transformer_object.get_datum_by_ordinal(9).unwrap().value::<i16>().unwrap().unwrap();
                        
                        // log!("dv_transformer_object PrintOut: {:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}", 
                        // schema_name, table_name, column_category, column_name, column_type_name, system_id, table_oid, column_ordinal_position);

                        let column_category = ColumnCategory::from_str(&column_category);

                        let transformer_object: TransformerObject = 
                            TransformerObject { 
                                schema_name, 
                                table_name,
                                business_key_name,
                                column_name, 
                                column_type_name, 
                                system_id, 
                                table_oid, 
                                column_ordinal_position, 
                                column_category, 
                            };

                        // Bucket TransformerObject by table
                        dv_transformer_objects_hm
                            .entry(table_oid)
                            .or_insert_with(Vec::new)
                            .push(transformer_object);

                    }
                }

                Err(e) => {
                    log!("Error getting DV Transformer Objects Result: {:?}", e);
                }
            }
        }
    );

    // Build a Vector of BusinessKey's
    let mut business_keys: Vec<dv_transformer_schema::BusinessKey> = Vec::new();
    for dv_transformer_objects_v in dv_transformer_objects_hm {

        let mut descriptors: Vec<dv_transformer_schema::Descriptor> = Vec::new();
        let mut business_key_part_links: Vec<dv_transformer_schema::BusinessKeyPartLink> = Vec::new();

        // Build Descriptors
        for dv_transformer_object in &dv_transformer_objects_v.1 {

            let column_data_id = Uuid::new_v4();

            let column_data = dv_transformer_schema::ColumnData {
                id: column_data_id,
                system_id: dv_transformer_object.system_id,
                table_oid: dv_transformer_object.table_oid,
                column_ordinal_position: dv_transformer_object.column_ordinal_position,
                column_type_name: dv_transformer_object.column_type_name.clone(),
            };
            let orbit = dv_transformer_object.table_name.clone();
            // let orbit = dv_transformer_object.business_key_name.clone();

            if dv_transformer_object.column_category == ColumnCategory::Descriptor {
                let descriptor = get_descriptor(dv_transformer_object.column_name.clone(), column_data, orbit, false);
                descriptors.push(descriptor);
            } else if dv_transformer_object.column_category == ColumnCategory::DescriptorSensitive {
                let descriptor = get_descriptor(dv_transformer_object.column_name.clone(), column_data, orbit, true);
                descriptors.push(descriptor);
            }
        }

        // Build Business Key Part Links
        for dv_transformer_object in &dv_transformer_objects_v.1 {

            let column_data_id = Uuid::new_v4();

            let column_data = dv_transformer_schema::ColumnData {
                id: column_data_id,
                system_id: dv_transformer_object.system_id,
                table_oid: dv_transformer_object.table_oid,
                column_ordinal_position: dv_transformer_object.column_ordinal_position,
                column_type_name: dv_transformer_object.column_type_name.clone(),
            };

            if dv_transformer_object.column_category == ColumnCategory::BusinessKeyPart {
                let business_key_part_link = get_business_key_part_link(dv_transformer_object.column_name.clone(), column_data);
                business_key_part_links.push(business_key_part_link);
            }
        }

        // TODO: Handle multiple business keys for link tables. Ensure appropriate error handling!
        let business_key_name: String = {
            let mut business_key_name = String::new();
            for dv_transformer_object in &dv_transformer_objects_v.1 {
                if dv_transformer_object.business_key_name.to_lowercase() != "na" {
                    business_key_name = dv_transformer_object.business_key_name.to_lowercase().clone();
                }
            }
            business_key_name
        };

        let business_key_id = Uuid::new_v4();
        let business_key = dv_transformer_schema::BusinessKey {
            id: business_key_id,
            name: business_key_name,
            business_key_part_links,
            descriptors 
        };

        // log!("Business Key for DV Generation: {:?}", business_key);
        business_keys.push(business_key);
    }

    let dw_schema = guc::get_guc(guc::PgAutoDWGuc::DwSchema).expect("DW SCHEMA GUC is not set.");

    // Build DV
    // Push DV Function
    let mut dv_ddl_sql = String::new();

    for business_key in &business_keys {
        let dv_business_key_ddl_sql = build_sql_from_business_key(&dw_schema, business_key);
        dv_ddl_sql.push_str(&dv_business_key_ddl_sql);
    }

    log!("DDL Full: {}", &dv_ddl_sql);

    // Build Tables using DDL
    Spi::connect( |mut client| {
            log!("Building DV Tables");
            // client.select(dv_objects_query, None, None);
            _ = client.update(&dv_ddl_sql, None, None);
            log!("DV Tables Built");
        }
    );


    // Build DVTransformerSchema

    // Get the current time in GMT
    let now_gmt = Utc::now().naive_utc();

    let mut dv_transformer_schema = dv_transformer_schema::DVTransformerSchema {
        id: Uuid::new_v4(),
        dw_schema,
        create_timestamp_gmt: now_gmt,
        modified_timestamp_gmt: now_gmt,
        business_keys,
    };

    // Add Target Columns to dv_transformer_schema links.

    dv_transformer_schema_add_target_columns(&mut dv_transformer_schema);
    log!("DV Transformer Schema JSON: {:?}", &dv_transformer_schema);

    dv_transformer_schema_push_to_repo(&build_id, &mut dv_transformer_schema);

    match dv_transformer_load_schema_from_build_id(&build_id) {
        Some(schema) => {
            dv_transformer_schema = schema;
        }
        None => {}
    };

    log!("DV Transformer Schema Reloaded: {:?}", &dv_transformer_schema);
    
}

fn dv_transformer_load_schema_from_build_id(build_id: &String) -> Option<dv_transformer_schema::DVTransformerSchema> {
    let get_schema_query: &str = r#"
        SELECT schema
        FROM auto_dw.dv_transformer_repo
        WHERE build_id = $1
    "#;

    // Variable to store the result
    let mut schema_result: Option<dv_transformer_schema::DVTransformerSchema> = None;

    // Load Schema w/ Build ID
    Spi::connect( |client| {
        log!("DV Schema: Pulling from Repo: {}", build_id);
        let results = client.select(get_schema_query, None, 
            Some(vec![
                (PgOid::from(pg_sys::TEXTOID), build_id.into_datum()),
            ]));
        log!("DV Schema: Pushed to REPO TABLE");

        match results {
            Ok(results) => {
                if let Some(result) = results.into_iter().next() {
                    let schema_json = result.get_datum_by_ordinal(1).unwrap().value::<pgrx::Json>().unwrap().unwrap();
                    let deserialized_schema: Result<dv_transformer_schema::DVTransformerSchema, serde_json::Error> = serde_json::from_value(schema_json.0);
                    match deserialized_schema {
                        Ok(deserialized_schema) => {
                            log!("Schema deserialized correctly: JSON{:?}", &deserialized_schema);
                            schema_result = Some(deserialized_schema);
                        },
                        Err(_) => {
                            log!("Schema could not deserialized");
                        },
                    }
                }
            },
            Err(_) => {
                log!("Schema could not deserialized");
            },
        }

    });
    return schema_result;
}


fn dv_transformer_schema_push_to_repo(build_id: &String, dv_transformer_schema: &mut dv_transformer_schema::DVTransformerSchema) {

    let now_gmt = Utc::now().naive_utc();

    dv_transformer_schema.modified_timestamp_gmt = now_gmt;

    let insert_schema_query: &str = r#"
        INSERT INTO auto_dw.dv_transformer_repo (build_id, schema)
        VALUES ($1, $2)
        "#; 

    let repo_json_string = serde_json::to_string(dv_transformer_schema).unwrap();

    // Build Tables using DDL
    Spi::connect( |mut client| {
        log!("DV Schema: Pushing to REPO TABLE");
        log!("Schema JSON: {}", &repo_json_string);
        _ = client.update(insert_schema_query, None, 
            Some(vec![
                (PgOid::from(pg_sys::TEXTOID), build_id.into_datum()),
                (PgOid::from(pg_sys::JSONOID), repo_json_string.into_datum()),
            ]));
        log!("DV Schema: Pushed to REPO TABLE");
        }
    );

}

fn dv_transformer_schema_add_target_columns(dv_transformer_schema: &mut dv_transformer_schema::DVTransformerSchema) {

    for business_key in &mut dv_transformer_schema.business_keys {

        // For Descriptors in Business Keys
        for descriptor in &mut business_key.descriptors {
            let schema_name = &dv_transformer_schema.dw_schema;
            let table_name = &{"sat_".to_string() + &descriptor.orbit + {if descriptor.is_sensitive { "_sensitive" } else {""}}};
            let column_name = &descriptor.descriptor_link.alias;
            
            let get_column_data = queries::get_column_data(schema_name, table_name, column_name);

            let column_data: Option<dv_transformer_schema::ColumnData> = Spi::connect( |client| {

                match client.select(&get_column_data, None, None) {
                    Ok(column_data) => {
                        // Only 0 or 1 record should be returned.
                        if let Some(column_data_record) = column_data.into_iter().next() {
                            let system_id =  column_data_record.get_datum_by_ordinal(1).unwrap().value::<i64>().unwrap().unwrap();
                            let _schema_oid =  column_data_record.get_datum_by_ordinal(2).unwrap().value::<u32>().unwrap().unwrap();
                            let _schema_name =  column_data_record.get_datum_by_ordinal(3).unwrap().value::<String>().unwrap().unwrap();
                            let _table_name =  column_data_record.get_datum_by_ordinal(4).unwrap().value::<String>().unwrap().unwrap();
                            let table_oid =  column_data_record.get_datum_by_ordinal(5).unwrap().value::<u32>().unwrap().unwrap();
                            let _column_name =  column_data_record.get_datum_by_ordinal(6).unwrap().value::<String>().unwrap().unwrap();
                            let column_ordinal_position =  column_data_record.get_datum_by_ordinal(7).unwrap().value::<i16>().unwrap().unwrap(); 
                            let column_type_name =  column_data_record.get_datum_by_ordinal(8).unwrap().value::<String>().unwrap().unwrap();


                            log!("Column Data PrintOut: {:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}", 
                                system_id, _schema_oid, _schema_name, _table_name, table_oid, _column_name, column_ordinal_position, column_type_name);

                            let column_id = Uuid::new_v4();


                            let column_data = dv_transformer_schema::ColumnData {
                                id: column_id,
                                system_id,
                                table_oid,
                                column_ordinal_position,
                                column_type_name,
                            };

                            // descriptor.descriptor_link.target_column_entiy = Some(enity);
                            return Some(column_data)
                            
                        } else {
                            log!("Column Data Not available.");
                            
                        }
                        return None
                    }
                    Err(e) => {
                        log!("Target Column Data Error: {:?}", e);
                        return None
                    }
                }
            });

            descriptor.descriptor_link.target_column_entiy = column_data;
        }

        // For Business Key Parts in Business Keys
        for business_key_part_link in &mut business_key.business_key_part_links {
            let schema_name = &dv_transformer_schema.dw_schema;
            let table_name = &{"hub_".to_string() + &business_key.name};
            let column_name = &(business_key_part_link.alias.clone() + "_bk");

            let get_column_data= queries::get_column_data(schema_name, table_name, column_name);

            let column_data: Option<dv_transformer_schema::ColumnData> = Spi::connect( |client| {

                match client.select(&get_column_data, None, None) {
                    Ok(column_data) => {
                        // Only 0 or 1 record should be returned.
                        if let Some(column_data_record) = column_data.into_iter().next() {
                            let system_id =  column_data_record.get_datum_by_ordinal(1).unwrap().value::<i64>().unwrap().unwrap();
                            let _schema_oid =  column_data_record.get_datum_by_ordinal(2).unwrap().value::<u32>().unwrap().unwrap();
                            let _schema_name =  column_data_record.get_datum_by_ordinal(3).unwrap().value::<String>().unwrap().unwrap();
                            let _table_name =  column_data_record.get_datum_by_ordinal(4).unwrap().value::<String>().unwrap().unwrap();
                            let table_oid =  column_data_record.get_datum_by_ordinal(5).unwrap().value::<u32>().unwrap().unwrap();
                            let _column_name =  column_data_record.get_datum_by_ordinal(6).unwrap().value::<String>().unwrap().unwrap();
                            let column_ordinal_position =  column_data_record.get_datum_by_ordinal(7).unwrap().value::<i16>().unwrap().unwrap(); 
                            let column_type_name =  column_data_record.get_datum_by_ordinal(8).unwrap().value::<String>().unwrap().unwrap();


                            log!("Column Data PrintOut: {:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}", 
                                system_id, _schema_oid, _schema_name, _table_name, table_oid, _column_name, column_ordinal_position, column_type_name);

                            let column_id = Uuid::new_v4();


                            let column_data = dv_transformer_schema::ColumnData {
                                id: column_id,
                                system_id,
                                table_oid,
                                column_ordinal_position,
                                column_type_name,
                            };

                            // descriptor.descriptor_link.target_column_entiy = Some(enity);
                            return Some(column_data)
                            
                        } else {
                            log!("Column Data Not available.");
                            
                        }
                        return None
                    }
                    Err(e) => {
                        log!("Target Column Data Error: {:?}", e);
                        return None
                    }
                }
            });

            business_key_part_link.target_column_id = column_data;
        }
        
    }
}

fn get_descriptor(column_name: String, column_data: dv_transformer_schema::ColumnData, orbit: String, is_sensitive: bool) -> dv_transformer_schema::Descriptor {
    let descriptor_link_id = Uuid::new_v4();
    let descriptor_link = dv_transformer_schema::DescriptorLink {
        id: descriptor_link_id,
        alias: column_name, // TODO: Give the user an option to change name in the future - modality TBD.
        source_column_entity: Some(column_data),
        target_column_entiy: None,
    };
    let descriptor_id = Uuid::new_v4();
    let descriptor = dv_transformer_schema::Descriptor {
        id: descriptor_id,
        descriptor_link,
        orbit,
        is_sensitive,
    };
    // log!("dv Enity Object {:?}", &descriptor);
    descriptor
}

fn get_business_key_part_link(alias: String, column_data: dv_transformer_schema::ColumnData) -> dv_transformer_schema::BusinessKeyPartLink {
    let business_key_part_link_id = Uuid::new_v4();
    let mut sources_column_data: Vec<dv_transformer_schema::ColumnData> = Vec::new(); 
    sources_column_data.push(column_data);

    let business_key_link = dv_transformer_schema::BusinessKeyPartLink {
        id: business_key_part_link_id,
        alias,
        source_column_entities: sources_column_data,
        target_column_id: None,
    };

    business_key_link
}

fn build_sql_from_business_key(dw_schema: &String, business_key: &BusinessKey) -> String {
    let mut dv_business_key_ddl_sql = String::new();

    // Hub Buildout
    let mut hub_bks = String::new();

    for part_link in &business_key.business_key_part_links {
        let r = format!(r#",
            {}_bk VARCHAR"#, part_link.alias);
        hub_bks.push_str(&r);
    }

    let hub_sql = 
    format!(r#"
        CREATE TABLE {}.hub_{} (
            hub_{}_hk VARCHAR NOT NULL,
            load_ts TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            record_source VARCHAR NOT NULL{}
        );
    "#, dw_schema, business_key.name, business_key.name, hub_bks);

    // log!("Hub SQL: {}", hub);
    dv_business_key_ddl_sql.push_str(&format!(
        r#"
        {}"#, hub_sql));

    // Sat Buildout

    let mut satellite_sqls: HashMap<String, String> = HashMap::new(); 

    for descriptor in &business_key.descriptors {

        let sensitive_string = {
            if descriptor.is_sensitive == true {
                "_sensitive".to_string()
            } else {
                "".to_string()
            }
        };

        let satellite_sql_key = descriptor.orbit.clone() + &sensitive_string;
        let desc_column_name = &descriptor.descriptor_link.alias;
        let desc_column_type = &descriptor.descriptor_link.source_column_entity.as_ref().unwrap().column_type_name;
        let sat_descriptor_sql_part: String = format!(",\n    {} {}", desc_column_name, desc_column_type);

        if let Some(existing_sat_sql) = satellite_sqls.get_mut(&satellite_sql_key) {
            if let Some(pos) = existing_sat_sql.find(");") {
                existing_sat_sql.insert_str(pos, &sat_descriptor_sql_part);
            } else {
                println!("The substring \");\" was not found in the original string.");
            }
        } else {
            let begin_sat_sql = 
                format!(r#"
                    CREATE TABLE {}.sat_{} (
                        hub_{}_hk VARCHAR NOT NULL,
                        load_ts TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                        record_source VARCHAR NOT NULL,
                        sat_{}_hd VARCHAR NOT NULL{});
                "#, dw_schema, satellite_sql_key, business_key.name, descriptor.orbit, sat_descriptor_sql_part);
            satellite_sqls.insert(satellite_sql_key, begin_sat_sql);
        }

    }

    // TODO: Map sats to dv_business_key_ddl_sql
    for satellite_sql in satellite_sqls {
        dv_business_key_ddl_sql.push_str(&satellite_sql.1);
    }
    // dv_business_key_ddl_sql.push_str(string)

    dv_business_key_ddl_sql
}

#[derive(Debug, PartialEq)]
enum ColumnCategory {
    BusinessKeyPart,
    Descriptor,
    DescriptorSensitive,
}

impl ColumnCategory {
    fn from_str(input: &str) -> ColumnCategory {
        match input {
            "Business Key Part" => ColumnCategory::BusinessKeyPart,
            "Descriptor" => ColumnCategory::Descriptor,
            "Descriptor - Sensitive" => ColumnCategory::DescriptorSensitive,
            _ => panic!("'{}' is not a valid ColumnCategory", input),
        }
    }
}

#[derive(Debug)]
struct TransformerObject {
    #[allow(dead_code)]
    schema_name: String,
    table_name: String,
    business_key_name: String,
    column_name: String,
    column_type_name: String,
    system_id: i64,
    table_oid: u32,
    column_ordinal_position: i16,
    column_category: ColumnCategory,
}
