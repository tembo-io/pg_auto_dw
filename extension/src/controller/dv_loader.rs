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
            let e = format!(r#"stg.{},"#, part_link.source_columns[0].column_name);
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

          // Sat Buildout
        let mut sat_insert_into_header_part_sql: HashMap<String, String> = HashMap::new();

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

            // SAT INSERT Header 
            let sat_descriptor_sql_part: String = format!(",\n    {}", desc_column_name);
            if let Some(existing_sat_sql) = sat_insert_into_header_part_sql.get_mut(&satellite_sql_key) {
                if let Some(pos) = existing_sat_sql.find(");") {
                    existing_sat_sql.insert_str(pos, &sat_descriptor_sql_part);
                } else {
                    println!("The substring \");\" was not found in the original string.");
                }
            } else {
                let begin_sat_sql = 
                    format!(r#"
                            INSERT INTO {}.sat_{} (
                                hub_{}_hk,
                                load_ts,
                                record_source,
                                sat_{}_hd{});
                            "#, 
                            dw_schema, satellite_sql_key, 
                            business_key.name, 
                            satellite_sql_key, sat_descriptor_sql_part);

                sat_insert_into_header_part_sql.insert(satellite_sql_key, begin_sat_sql);
                
            }
        }

    }


    sat_insert_dmls
}


// INSERT INTO public.sat_seller (
//     hub_seller_hk,
//     load_ts,
//     record_source,
//     sat_seller_hd,
//     city,
//     state,
//     zip_5
// )
// WITH stg AS (
//     SELECT 
//         *,
//         ENCODE(
//             public.DIGEST(
//                 ARRAY_TO_STRING(
//                     ARRAY[stg.seller_id], ','), 'sha256'), 'hex') AS hub_seller_hk,
//         ENCODE(
//             public.DIGEST(
//                 ARRAY_TO_STRING(
//                     ARRAY[stg.city, stg.state, stg.zip_5], ','), 'sha256'), 'hex') AS sat_seller_hd
//     FROM public.seller AS stg
// ),
// new_stg_data AS (  
// SELECT stg.*
//     FROM stg
// LEFT JOIN public.sat_seller ON 
//     stg.hub_seller_hk = sat_seller.hub_seller_hk AND
//     stg.sat_seller_hd = sat_seller.sat_seller_hd
// WHERE sat_seller.hub_seller_hk IS NULL
// )
// SELECT   
// hub_seller_hk,
// (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::TIMESTAMP WITHOUT TIME ZONE AS load_ts ,
// 'PUBLIC SCHEMA' AS record_source ,
// sat_seller_hd,
// city,
// state,
// zip_5
// FROM new_stg_data
// ; 
// "#;