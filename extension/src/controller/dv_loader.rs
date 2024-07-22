use pgrx::prelude::*;
use std::collections::HashMap;
use crate::model::dv_schema::*;

pub fn dv_load_schema_from_build_id(build_id: &String) -> Option<DVSchema> {
    let get_schema_query: &str = r#"
        SELECT schema
        FROM auto_dw.dv_repo
        WHERE build_id = $1
    "#;

    // Variable to store the result
    let mut schema_result: Option<DVSchema> = None;

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
                    let deserialized_schema: Result<DVSchema, serde_json::Error> = serde_json::from_value(schema_json.0);
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

// Refreshes based on dv_schema
pub fn dv_data_loader(dv_schema: &DVSchema) {

    // TODO: Create SQL For Hubs
    let hub_dml = dv_data_loader_hub_dml(dv_schema);

    // TODO: Create SQL For Satellites 
    let sat_dml = dv_data_loader_sat_dml(dv_schema);

    // Run SQL
    let dv_dml = hub_dml + &sat_dml;
    log!("DV DML: {}", &dv_dml);
    // Build Tables using DDL
    Spi::connect( |mut client| {
        // client.select(dv_objects_query, None, None);
        _ = client.update(&dv_dml, None, None);
        log!("Data Pushed to DV tables.");
    }
    );

}

fn dv_data_loader_hub_dml (dv_schema: &DVSchema) -> String {

    let mut hub_insert_dmls = String::new();

    for business_key in &dv_schema.business_keys {

        // Hub Buildout
        let dw_schema_name = &dv_schema.dw_schema;
        let busines_key_name = &business_key.name;

        // Business Key Part(s)
        let mut hub_bk_parts_sql = String::new();
        for part_link in &business_key.business_key_part_links {
            let r = format!(r#",
                            {}_bk"#, part_link.alias);
            hub_bk_parts_sql.push_str(&r);
        }

        // INSERT INTO Header
        let hub_insert_into_header_part_sql = format!(r#"
            INSERT INTO {}.hub_{} (
                hub_{}_hk,
                load_ts,
                record_source
                {}
            )
            "#, 
            dw_schema_name, busines_key_name, busines_key_name, hub_bk_parts_sql);


        // Business Key Part(s) Init SQL
        let mut hub_bk_neg_1_init_parts_sql = String::new();
        let mut hub_bk_neg_2_init_parts_sql = String::new();
        for part_link in &business_key.business_key_part_links {
            let neg_1: String = format!(r#",
                '-1'::TEXT AS {}_bk"#, part_link.alias);
            hub_bk_neg_1_init_parts_sql.push_str(&neg_1);
            let neg_2: String = format!(r#",
                '-2'::TEXT AS {}_bk"#, part_link.alias);
            hub_bk_neg_2_init_parts_sql.push_str(&neg_2);
        }
                
        let hub_insert_into_init_part_sql = format!(r#"
            WITH initialized AS (
            SELECT
            CASE
                WHEN COUNT(*) > 0 THEN TRUE
                ELSE FALSE
            END is_initialized
            FROM {}.hub_{}
            )
            SELECT
                ENCODE(PUBLIC.DIGEST(ARRAY_TO_STRING(ARRAY[-1], ',')::TEXT, 'sha256'), 'hex') AS hub_{}_hk,
                '0001-01-01'::TIMESTAMP WITHOUT TIME ZONE AS load_ts, 
                'SYSTEM'::TEXT AS record_source
                {}
                FROM initialized WHERE NOT initialized.is_initialized
            UNION
            SELECT
                ENCODE(PUBLIC.DIGEST(ARRAY_TO_STRING(ARRAY[-2], ',')::TEXT, 'sha256'), 'hex') AS hub_{}_hk,
                '0001-01-01'::TIMESTAMP WITHOUT TIME ZONE AS load_ts,
                'SYSTEM'::TEXT AS record_source
                {}
                FROM initialized WHERE NOT initialized.is_initialized
            ;
            "#, dw_schema_name, busines_key_name, 
            busines_key_name, hub_bk_neg_1_init_parts_sql, 
            busines_key_name, hub_bk_neg_2_init_parts_sql);

        let hub_insert_init = hub_insert_into_header_part_sql.clone() + &hub_insert_into_init_part_sql;
        hub_insert_dmls.push_str(&hub_insert_init);

        // Insert Main

        // Arrary Parts
        let mut hub_bk_parts_sql_stg_array = String ::new();
        for part_link in &business_key.business_key_part_links {
            // TODO: Need acount for more than once source.  However, Vec data structure isn't ideal - refactor. 
            let e = format!(r#"stg.{}::TEXT,"#, part_link.source_columns[0].column_name);
            hub_bk_parts_sql_stg_array.push_str(&e);
        } 
        hub_bk_parts_sql_stg_array.pop(); // Removing the last ","

        // Source Schema
        // TODO: Schema Needs to be pushed up to the link.
        let mut source_schema = String::new();
        // TODO: Table Needs to be pushed up to the link.
        let mut source_table = String::new();

        // Business Key Part(s)
        let mut hub_bk_parts_stg_names = String::new();
        for part_link in &business_key.business_key_part_links {
            let source_column_name = &part_link.source_columns[0].column_name;
            let e = format!(r#",
                            stg.{}::TEXT AS {}_bk"#, source_column_name, source_column_name);
            hub_bk_parts_stg_names.push_str(&e);
            source_schema = part_link.source_columns[0].schema_name.clone();
            source_table = part_link.source_columns[0].table_name.clone();
        }

        let hub_insert_into_main_part_sql = format!(r#"
            WITH
            stg_data AS (
            SELECT
                ENCODE(
                    public.DIGEST(
                        ARRAY_TO_STRING(
                            ARRAY[{}], ','), 'sha256'), 'hex') AS hub_{}_hk,
                (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::TIMESTAMP(6) AS load_ts,
                '{}' AS record_source{}
            FROM {}.{} AS stg
            ),
            new_stg_data AS (
            SELECT stg_data.* FROM stg_data
            LEFT JOIN {}.hub_{} ON stg_data.hub_{}_hk = hub_{}.hub_{}_hk
            WHERE hub_{}.hub_{}_hk IS NULL
            )
            SELECT
            hub_{}_hk,
            load_ts,
            record_source{}
            FROM new_stg_data
            ;
            "#, 
            hub_bk_parts_sql_stg_array, busines_key_name,
            source_schema, hub_bk_parts_stg_names,
            source_schema, source_table,
            dw_schema_name, busines_key_name, busines_key_name, busines_key_name, busines_key_name,
            busines_key_name, busines_key_name,
            busines_key_name,
            hub_bk_parts_sql
        );

        log!("MAIN HUB INSERT SQL: 
            {}", hub_insert_into_main_part_sql);

        let hub_insert_main = hub_insert_into_header_part_sql + &hub_insert_into_main_part_sql;
        hub_insert_dmls.push_str(&hub_insert_main);
    }

    hub_insert_dmls
}

fn dv_data_loader_sat_dml (dv_schema: &DVSchema) -> String {

    let mut sat_insert_dmls = String::new();
    let dw_schema = dv_schema.dw_schema.clone();

    for business_key in &dv_schema.business_keys {

        // Arrary Parts
        let mut hub_bk_parts_sql_stg_array = String ::new();
        for part_link in &business_key.business_key_part_links {
            // TODO: Need acount for more than once source.  However, Vec data structure isn't ideal - refactor. 
            let e = format!(r#"stg.{}::TEXT,"#, part_link.source_columns[0].column_name);
            hub_bk_parts_sql_stg_array.push_str(&e);
        } 
        hub_bk_parts_sql_stg_array.pop(); // Removing the last ","

          // Sat Buildout
        let mut sat_insert_sql_header_parts: HashMap<String, String> = HashMap::new();
        let mut descriptors_for_sats: HashMap<String, Vec<&Descriptor>> = HashMap::new();

        for descriptor in &business_key.descriptors {

            let sensitive_string = {
                if descriptor.is_sensitive == true {
                    "_sensitive".to_string()
                } else {
                    "".to_string()
                }
            };

            let satellite_sql_key = descriptor.orbit.clone() + &sensitive_string;

            descriptors_for_sats
                .entry(satellite_sql_key.clone())
                .or_insert_with(Vec::new)
                .push(&descriptor);

            let desc_column_name = &descriptor.descriptor_link.alias;

            // SAT INSERT Header 
            let sat_descriptor_sql_part: String = format!(",\n    {}", desc_column_name);
            if let Some(existing_sat_sql) = sat_insert_sql_header_parts.get_mut(&satellite_sql_key) {
                if let Some(pos) = existing_sat_sql.find(")") {
                    existing_sat_sql.insert_str(pos, &sat_descriptor_sql_part);
                } else {
                    println!("The substring \")\" was not found in the original string.");
                }
            } else {
                let begin_sat_sql = 
                    format!(r#"
                            INSERT INTO {}.sat_{} (
                                hub_{}_hk,
                                load_ts,
                                record_source,
                                sat_{}_hd{})
                            "#, 
                            dw_schema, &satellite_sql_key, 
                            business_key.name, 
                            &satellite_sql_key, sat_descriptor_sql_part);

                sat_insert_sql_header_parts.insert(satellite_sql_key.clone(), begin_sat_sql);
            }
        }

        log!("Hub Array Part {}", hub_bk_parts_sql_stg_array);

        for sat_insert_sql_header_part in &sat_insert_sql_header_parts {
            log!("SAT INSERT SQL HEADER {} :: {}", sat_insert_sql_header_part.0, sat_insert_sql_header_part.1);
        }

        // Array SQL
        let mut sats_source_sql_array: HashMap<String, String> = HashMap::new();
        for (key, descriptors) in descriptors_for_sats.clone() {
            let array_part_str = sats_source_sql_array.entry(key.clone()).or_insert_with(String::new);
        
            for descriptor in descriptors {
                if let Some(column) = descriptor.descriptor_link.source_column.as_ref() {
                    let array_part = if array_part_str.is_empty() {
                        format!("stg.{}::TEXT", column.column_name)
                    } else {
                        format!(", stg.{}::TEXT", column.column_name)
                    };
                    array_part_str.push_str(&array_part);
                }
            }
        }

        for sat_source_sql_array in &sats_source_sql_array {
            log!("Sat Source SQL Key: {} ARRAY: {}", sat_source_sql_array.0, sat_source_sql_array.1);
        }

        // Column SQL
        let mut sats_source_sql_cols: HashMap<String, String> = HashMap::new();
        for (key, descriptors) in descriptors_for_sats.clone() {
            let col_part_str = sats_source_sql_cols.entry(key.clone()).or_insert_with(String::new);
        
            for descriptor in descriptors {
                if let Some(column) = descriptor.descriptor_link.source_column.as_ref() {
                    let col_part = format!(r#",
                                                    {}"#, 
                                                    column.column_name);
                    col_part_str.push_str(&col_part);
                }
            }
        }

        for sat_source_sql_cols in &sats_source_sql_cols {
            log!("Sat Source SQL Key: {} Cols: {}", sat_source_sql_cols.0, sat_source_sql_cols.1);
        }

        // Main Insert

        for (key, insert_header) in sat_insert_sql_header_parts {
            
            let sat_source_sql_array = sats_source_sql_array.get(&key).map(|v| v.as_str()).unwrap_or("NA");
            let sat_source_sql_cols = sats_source_sql_cols.get(&key).map(|v| v.as_str()).unwrap_or("NA");

            // TODO: Change data structure to support multiple source schemas.
            let source_schema_name = descriptors_for_sats
                .get(&key)
                .and_then(|v| v.get(0))  // Safely get the first element
                .and_then(|descriptor| descriptor.descriptor_link.source_column.as_ref())  // Safely access target_column
                .map(|source_column| source_column.schema_name.clone())  // Safely get schema_name and clone it
                .unwrap_or_default();  // Provide a default value in case of None
            
            let source_table_name = descriptors_for_sats
                .get(&key)
                .and_then(|v| v.get(0))  // Safely get the first element
                .and_then(|descriptor| descriptor.descriptor_link.source_column.as_ref())  // Safely access target_column
                .map(|source_column| source_column.table_name.clone())  // Safely get schema_name and clone it
                .unwrap_or_default();  // Provide a default value in case of None

            let business_key_name = &business_key.name;

            let insert_sql =  format!(r#"
                -- SAT INSERT SQL
                {insert_header}
                WITH stg AS (
                SELECT 
                    *,
                    ENCODE(
                        {source_schema_name}.DIGEST(
                            ARRAY_TO_STRING(
                                ARRAY[{hub_bk_parts_sql_stg_array}], ','), 'sha256'), 'hex') AS hub_{business_key_name}_hk,
                    ENCODE(
                            {source_schema_name}.DIGEST(
                                ARRAY_TO_STRING(
                                    ARRAY[{sat_source_sql_array}], ','), 'sha256'), 'hex') AS sat_{key}_hd
                    FROM {source_schema_name}.{source_table_name} AS stg
                ),
                new_stg_data AS (  
                SELECT stg.*
                    FROM stg
                LEFT JOIN {dw_schema}.sat_{key} ON 
                    stg.hub_{business_key_name}_hk = sat_{key}.hub_{business_key_name}_hk AND
                    stg.sat_{key}_hd = sat_{key}.sat_{key}_hd
                WHERE sat_{key}.hub_{business_key_name}_hk IS NULL
                )
                SELECT   
                hub_{business_key_name}_hk,
                (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::TIMESTAMP WITHOUT TIME ZONE AS load_ts ,
                '{source_schema_name}' AS record_source ,
                sat_{key}_hd
                {sat_source_sql_cols}
                FROM new_stg_data
                ; 
                "#);

            log!("{insert_sql}");
            sat_insert_dmls.push_str(&insert_sql);
        }
    }

    sat_insert_dmls
}