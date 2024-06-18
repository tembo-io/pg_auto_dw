use pgrx::prelude::*;

pub fn build_dv(dv_objects_query: &str) {
    log!("In build_dv function.");
    let l = Spi::connect(|client| 
        {
            log!("In build_dv function - Spi::connect.");
            let dv_transformer_objects_result = client.select(dv_objects_query, None, None);
            
            match dv_transformer_objects_result {
                Ok(dv_transformer_objects) => {
                    log!("DV Transforer Object Length {}", dv_transformer_objects.len());
                    for dv_transformer_object in dv_transformer_objects {
                        log!("In Tuple Table Loop");
                        let schema_name = dv_transformer_object.get_datum_by_ordinal(1).unwrap().value::<String>().unwrap();
                        let table_name = dv_transformer_object.get_datum_by_ordinal(1).unwrap().value::<String>().unwrap();
                        let column_category = dv_transformer_object.get_datum_by_ordinal(1).unwrap().value::<String>().unwrap();
                        let column_name = dv_transformer_object.get_datum_by_ordinal(1).unwrap().value::<String>().unwrap();
                        let column_type_name = dv_transformer_object.get_datum_by_ordinal(1).unwrap().value::<String>().unwrap();
        
                        log!("dv_transformer_object PrintOut: {:?}, {:?}, {:?}, {:?}, {:?}", schema_name, table_name, column_category, column_name, column_type_name);

                    }
                }
                Err(e) => {
                    log!("Error getting DV Transformer Objects Result: {:?}", e);
                }
            }
        }
    );
}