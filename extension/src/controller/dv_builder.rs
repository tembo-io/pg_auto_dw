use pgrx::prelude::*;
use uuid::Uuid;
use std::collections::HashMap;
use chrono::Utc;

use crate::model::queries;
use crate::utility::guc;
use crate::model::dv_schema::{
                                DVSchema, 
                                BusinessKey, 
                                BusinessKeyPartLink, 
                                Descriptor, 
                                DescriptorLink, 
                                ColumnData
                            };

use super::dv_loader::*;

pub fn build_dv(build_id: &String, dv_objects_query: &str) {

    let mut dv_objects_hm: HashMap<u32, Vec<TransformerObject>> = HashMap::new();

    Spi::connect(|client| 
        {
            let dv_objects_result = client.select(dv_objects_query, None, None);

            match dv_objects_result {

                Ok(dv_objects) => {

                    for dv_object in dv_objects {

                        let schema_name = dv_object.get_datum_by_ordinal(1).unwrap().value::<String>().unwrap().unwrap();
                        let table_name = dv_object.get_datum_by_ordinal(2).unwrap().value::<String>().unwrap().unwrap();
                        let column_category = dv_object.get_datum_by_ordinal(3).unwrap().value::<String>().unwrap().unwrap();
                        let business_key_name = dv_object.get_datum_by_ordinal(4).unwrap().value::<String>().unwrap().unwrap();
                        let column_name = dv_object.get_datum_by_ordinal(5).unwrap().value::<String>().unwrap().unwrap();
                        let column_type_name = dv_object.get_datum_by_ordinal(6).unwrap().value::<String>().unwrap().unwrap();
                        let system_id = dv_object.get_datum_by_ordinal(7).unwrap().value::<i64>().unwrap().unwrap();
                        let table_oid: u32 = dv_object.get_datum_by_ordinal(8).unwrap().value::<u32>().unwrap().unwrap();
                        let column_ordinal_position = dv_object.get_datum_by_ordinal(9).unwrap().value::<i16>().unwrap().unwrap();
                        
                        // log!("dv_object PrintOut: {:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}", 
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
                        dv_objects_hm
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
    let mut business_keys: Vec<BusinessKey> = Vec::new();
    for dv_objects_v in dv_objects_hm {

        let mut descriptors: Vec<Descriptor> = Vec::new();
        let mut business_key_part_links: Vec<BusinessKeyPartLink> = Vec::new();

        // Build Descriptors
        for dv_object in &dv_objects_v.1 {

            let column_data_id = Uuid::new_v4();

            let column_data = ColumnData {
                id: column_data_id,
                system_id: dv_object.system_id,
                schema_name: dv_object.schema_name.clone(),
                table_oid: dv_object.table_oid,
                table_name: dv_object.table_name.clone(),
                column_name: dv_object.column_name.clone(),
                column_ordinal_position: dv_object.column_ordinal_position,
                column_type_name: dv_object.column_type_name.clone(),
            };
            let orbit = dv_object.table_name.clone();
            // let orbit = dv_object.business_key_name.clone();

            if dv_object.column_category == ColumnCategory::Descriptor {
                let descriptor = get_descriptor(dv_object.column_name.clone(), column_data, orbit, false);
                descriptors.push(descriptor);
            } else if dv_object.column_category == ColumnCategory::DescriptorSensitive {
                let descriptor = get_descriptor(dv_object.column_name.clone(), column_data, orbit, true);
                descriptors.push(descriptor);
            }
        }

        // Build Business Key Part Links
        for dv_object in &dv_objects_v.1 {

            let column_data_id = Uuid::new_v4();

            let column_data = ColumnData {
                id: column_data_id,
                system_id: dv_object.system_id,
                schema_name: dv_object.schema_name.clone(),
                table_oid: dv_object.table_oid,
                table_name: dv_object.table_name.clone(),
                column_name: dv_object.column_name.clone(),
                column_ordinal_position: dv_object.column_ordinal_position,
                column_type_name: dv_object.column_type_name.clone(),
            };

            if dv_object.column_category == ColumnCategory::BusinessKeyPart {
                let business_key_part_link = get_business_key_part_link(dv_object.column_name.clone(), column_data);
                business_key_part_links.push(business_key_part_link);
            }
        }

        // TODO: Handle multiple business keys for link tables. Ensure appropriate error handling!
        let business_key_name: String = {
            let mut business_key_name = String::new();
            for dv_object in &dv_objects_v.1 {
                if dv_object.business_key_name.to_lowercase() != "na" {
                    business_key_name = dv_object.business_key_name.to_lowercase().clone();
                }
            }
            business_key_name
        };

        let business_key_id = Uuid::new_v4();
        let business_key = BusinessKey {
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

    let mut dv_schema = DVSchema {
        id: Uuid::new_v4(),
        dw_schema,
        create_timestamp_gmt: now_gmt,
        modified_timestamp_gmt: now_gmt,
        business_keys,
    };

    // Add Target Columns to dv_schema links.

    dv_schema_add_target_columns(&mut dv_schema);

    dv_schema_push_to_repo(&build_id, &mut dv_schema);

    // ToDo: Remove as this is redundant and for testing purposes.  However, this function will be integral for future data refreshes.
    match dv_load_schema_from_build_id(&build_id) {
        Some(schema) => {
            dv_schema = schema;
        }
        None => {
            panic!("Repo Error")
        }
    };

    // dv_loader::dv_data_load(&dv_schema);
    dv_data_loader(&dv_schema);
}



fn dv_schema_push_to_repo(build_id: &String, dv_schema: &mut DVSchema) {

    let now_gmt = Utc::now().naive_utc();

    dv_schema.modified_timestamp_gmt = now_gmt;

    let insert_schema_query: &str = r#"
        INSERT INTO auto_dw.dv_repo (build_id, schema)
        VALUES ($1, $2)
        "#; 

    let repo_json_string = serde_json::to_string(dv_schema).unwrap();

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

fn dv_schema_add_target_columns(dv_schema: &mut DVSchema) {

    for business_key in &mut dv_schema.business_keys {

        // For Descriptors in Business Keys
        for descriptor in &mut business_key.descriptors {
            let schema_name = &dv_schema.dw_schema;
            let table_name = &{"sat_".to_string() + &descriptor.orbit + {if descriptor.is_sensitive { "_sensitive" } else {""}}};
            let column_name = &descriptor.descriptor_link.alias;
            
            let get_column_data = queries::get_column_data(schema_name, table_name, column_name);

            let column_data: Option<ColumnData> = Spi::connect( |client| {

                match client.select(&get_column_data, None, None) {
                    Ok(column_data) => {
                        // Only 0 or 1 record should be returned.
                        if let Some(column_data_record) = column_data.into_iter().next() {
                            let system_id =  column_data_record.get_datum_by_ordinal(1).unwrap().value::<i64>().unwrap().unwrap();
                            let _schema_oid =  column_data_record.get_datum_by_ordinal(2).unwrap().value::<u32>().unwrap().unwrap();
                            let schema_name =  column_data_record.get_datum_by_ordinal(3).unwrap().value::<String>().unwrap().unwrap();
                            let table_name =  column_data_record.get_datum_by_ordinal(4).unwrap().value::<String>().unwrap().unwrap();
                            let table_oid =  column_data_record.get_datum_by_ordinal(5).unwrap().value::<u32>().unwrap().unwrap();
                            let column_name =  column_data_record.get_datum_by_ordinal(6).unwrap().value::<String>().unwrap().unwrap();
                            let column_ordinal_position =  column_data_record.get_datum_by_ordinal(7).unwrap().value::<i16>().unwrap().unwrap(); 
                            let column_type_name =  column_data_record.get_datum_by_ordinal(8).unwrap().value::<String>().unwrap().unwrap();


                            log!("Column Data PrintOut: {:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}", 
                                system_id, _schema_oid, schema_name, table_name, table_oid, column_name, column_ordinal_position, column_type_name);

                            let column_id = Uuid::new_v4();


                            let column_data = ColumnData {
                                id: column_id,
                                system_id,
                                schema_name,
                                table_oid,
                                table_name,
                                column_name,
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

            descriptor.descriptor_link.target_column = column_data;
        }

        // For Business Key Parts in Business Keys
        for business_key_part_link in &mut business_key.business_key_part_links {
            let schema_name = &dv_schema.dw_schema;
            let table_name = &{"hub_".to_string() + &business_key.name};
            let column_name = &(business_key_part_link.alias.clone() + "_bk");

            let get_column_data= queries::get_column_data(schema_name, table_name, column_name);

            let column_data: Option<ColumnData> = Spi::connect( |client| {

                match client.select(&get_column_data, None, None) {
                    Ok(column_data) => {
                        // Only 0 or 1 record should be returned.
                        if let Some(column_data_record) = column_data.into_iter().next() {
                            let system_id =  column_data_record.get_datum_by_ordinal(1).unwrap().value::<i64>().unwrap().unwrap();
                            let _schema_oid =  column_data_record.get_datum_by_ordinal(2).unwrap().value::<u32>().unwrap().unwrap();
                            let schema_name =  column_data_record.get_datum_by_ordinal(3).unwrap().value::<String>().unwrap().unwrap();
                            let table_name =  column_data_record.get_datum_by_ordinal(4).unwrap().value::<String>().unwrap().unwrap();
                            let table_oid =  column_data_record.get_datum_by_ordinal(5).unwrap().value::<u32>().unwrap().unwrap();
                            let column_name =  column_data_record.get_datum_by_ordinal(6).unwrap().value::<String>().unwrap().unwrap();
                            let column_ordinal_position =  column_data_record.get_datum_by_ordinal(7).unwrap().value::<i16>().unwrap().unwrap(); 
                            let column_type_name =  column_data_record.get_datum_by_ordinal(8).unwrap().value::<String>().unwrap().unwrap();


                            log!("Column Data PrintOut: {:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}", 
                                system_id, _schema_oid, schema_name, table_name, table_oid, column_name, column_ordinal_position, column_type_name);

                            let column_id = Uuid::new_v4();


                            let column_data = ColumnData {
                                id: column_id,
                                system_id,
                                schema_name,
                                table_oid,
                                table_name,
                                column_name,
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

            business_key_part_link.target_column = column_data;
        }
        
    }
}

fn get_descriptor(column_name: String, column_data: ColumnData, orbit: String, is_sensitive: bool) -> Descriptor {
    let descriptor_link_id = Uuid::new_v4();
    let descriptor_link = DescriptorLink {
        id: descriptor_link_id,
        alias: column_name, // TODO: Give the user an option to change name in the future - modality TBD.
        source_column: Some(column_data),
        target_column: None,
    };
    let descriptor_id = Uuid::new_v4();
    let descriptor = Descriptor {
        id: descriptor_id,
        descriptor_link,
        orbit,
        is_sensitive,
    };
    // log!("dv Enity Object {:?}", &descriptor);
    descriptor
}

fn get_business_key_part_link(alias: String, column_data: ColumnData) -> BusinessKeyPartLink {
    let business_key_part_link_id = Uuid::new_v4();
    let mut sources_column_data: Vec<ColumnData> = Vec::new(); 
    sources_column_data.push(column_data);

    let business_key_link = BusinessKeyPartLink {
        id: business_key_part_link_id,
        alias,
        source_columns: sources_column_data,
        target_column: None,
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
        let desc_column_type = &descriptor.descriptor_link.source_column.as_ref().unwrap().column_type_name;
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
                "#, dw_schema, satellite_sql_key, business_key.name, satellite_sql_key, sat_descriptor_sql_part);
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
