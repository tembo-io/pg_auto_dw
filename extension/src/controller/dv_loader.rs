use pgrx::prelude::*;
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
fn dv_data_load(dv_schema: &DVSchema) {

}
