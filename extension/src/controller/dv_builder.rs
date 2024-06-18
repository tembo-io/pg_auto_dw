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
                        let table_name = dv_transformer_object.get_datum_by_ordinal(2).unwrap().value::<String>().unwrap();
                        let column_category = dv_transformer_object.get_datum_by_ordinal(3).unwrap().value::<String>().unwrap();
                        let column_name = dv_transformer_object.get_datum_by_ordinal(4).unwrap().value::<String>().unwrap();
                        let column_type_name = dv_transformer_object.get_datum_by_ordinal(5).unwrap().value::<String>().unwrap();
                        let system_id = dv_transformer_object.get_datum_by_ordinal(6).unwrap().value::<i64>().unwrap();
                        let table_oid = dv_transformer_object.get_datum_by_ordinal(7).unwrap().value::<u32>().unwrap();
                        let column_ordinal_position = dv_transformer_object.get_datum_by_ordinal(8).unwrap().value::<i16>().unwrap();
                        
                        log!("dv_transformer_object PrintOut: {:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}", schema_name, table_name, column_category, column_name, column_type_name, system_id, table_oid, column_ordinal_position);
                    }
                }
                Err(e) => {
                    log!("Error getting DV Transformer Objects Result: {:?}", e);
                }
            }
        }
    );
}