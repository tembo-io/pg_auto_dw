use pgrx::prelude::*;
use uuid::Uuid;
use serde_json;
use crate::model::dv_transformer_schema;

pub fn build_dv(dv_objects_query: &str) {
    log!("In build_dv function.");
    let mut dv_transformer_objects_v: Vec<TransformerObject> = Vec::new();

    Spi::connect(|client| 
        {
            log!("In build_dv function - Spi::connect.");
            let dv_transformer_objects_result = client.select(dv_objects_query, None, None);

            match dv_transformer_objects_result {
                Ok(dv_transformer_objects) => {
                    log!("DV Transforer Object Length {}", dv_transformer_objects.len());
                    for dv_transformer_object in dv_transformer_objects {
                        log!("In Tuple Table Loop");

                        let schema_name = dv_transformer_object.get_datum_by_ordinal(1).unwrap().value::<String>().unwrap().unwrap();
                        let table_name = dv_transformer_object.get_datum_by_ordinal(2).unwrap().value::<String>().unwrap().unwrap();
                        let column_category = dv_transformer_object.get_datum_by_ordinal(3).unwrap().value::<String>().unwrap().unwrap();
                        let column_name = dv_transformer_object.get_datum_by_ordinal(4).unwrap().value::<String>().unwrap().unwrap();
                        let column_type_name = dv_transformer_object.get_datum_by_ordinal(5).unwrap().value::<String>().unwrap().unwrap();
                        let system_id = dv_transformer_object.get_datum_by_ordinal(6).unwrap().value::<i64>().unwrap().unwrap();
                        let table_oid = dv_transformer_object.get_datum_by_ordinal(7).unwrap().value::<u32>().unwrap().unwrap();
                        let column_ordinal_position = dv_transformer_object.get_datum_by_ordinal(8).unwrap().value::<i16>().unwrap().unwrap();
                        
                        log!("dv_transformer_object PrintOut: {:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}", 
                        schema_name, table_name, column_category, column_name, column_type_name, system_id, table_oid, column_ordinal_position);

                        let column_category = ColumnCategory::from_str(&column_category);

                        let transformer_object: TransformerObject = 
                            TransformerObject { 
                                schema_name, 
                                table_name, 
                                column_name, 
                                column_type_name, 
                                system_id, 
                                table_oid, 
                                column_ordinal_position, 
                                column_category, 
                            };

                        dv_transformer_objects_v.push(transformer_object);

                    }
                }

                Err(e) => {
                    log!("Error getting DV Transformer Objects Result: {:?}", e);
                }
            }
        }
    );

    let mut descriptors: Vec<dv_transformer_schema::Descriptor> = Vec::new();
    // Build Descriptors
    for dv_transformer_object in dv_transformer_objects_v {

        let entity_id = Uuid::new_v4();

        let entity = dv_transformer_schema::Entity {
            id: entity_id,
            system_id: dv_transformer_object.system_id,
            table_oid: dv_transformer_object.table_oid,
            column_ordinal_position: dv_transformer_object.column_ordinal_position,
            column_type_name: dv_transformer_object.column_type_name,
        };

        if dv_transformer_object.column_category == ColumnCategory::Descriptor {
            let descriptor = get_descriptor(dv_transformer_object.column_name, entity, false);
            descriptors.push(descriptor);
        } else if dv_transformer_object.column_category == ColumnCategory::DescriptorSensitive {
            let descriptor = get_descriptor(dv_transformer_object.column_name, entity, true);
            descriptors.push(descriptor);
        }
    }

    // Build Business Key Part Links
}

fn get_descriptor(column_name: String, entity: dv_transformer_schema::Entity, is_sensitive: bool) -> dv_transformer_schema::Descriptor {
    let descriptor_link_id = Uuid::new_v4();
    let descriptor_link = dv_transformer_schema::DescriptorLink {
        id: descriptor_link_id,
        alias: column_name, // TODO: Give the user an option to change name in the future - modality TBD.
        source_column_id: Some(entity),
        target_column_id: None,
    };
    let descriptor_id = Uuid::new_v4();
    let descriptor = dv_transformer_schema::Descriptor {
        id: descriptor_id,
        descriptor_link,
        is_sensitive: is_sensitive,
    };
    log!("dv Enity Object {:?}", &descriptor);
    descriptor
}

#[derive(Debug, PartialEq)]
enum ColumnCategory {
    BusinessKey,
    Descriptor,
    DescriptorSensitive,
}

impl ColumnCategory {
    fn from_str(input: &str) -> ColumnCategory {
        match input {
            "Business Key" => ColumnCategory::BusinessKey,
            "Descriptor" => ColumnCategory::Descriptor,
            "Descriptor - Sensitive" => ColumnCategory::DescriptorSensitive,
            _ => panic!("'{}' is not a valid ColumnCategory", input),
        }
    }
}

#[derive(Debug)]
struct TransformerObject {
    schema_name: String,
    table_name: String,
    column_name: String,
    column_type_name: String,
    system_id: i64,
    table_oid: u32,
    column_ordinal_position: i16,
    column_category: ColumnCategory,
}


// let entity_id = Uuid::new_v4();

// let entity = dv_transformer_schema::Entity {
//     id: entity_id,
//     system_id,
//     table_oid,
//     column_ordinal_position,
//     column_type_name,
// };



// match column_category {
//     ColumnCategory::BusinessKey => {
//         // Code for BusinessKey
//         println!("This is the Business Key variant.");
//     }
//     ColumnCategory::Descriptor => {
//         // Code for Descriptor
//         println!("This is the Descriptor variant.");
//         let descriptor = get_descriptor(column_name, entity, false);
//     }
//     ColumnCategory::DescriptorSensitive => {
//         // Code for DescriptorSensitive
//         println!("This is the Descriptor Sensitive variant."); 
//         let descriptor = get_descriptor(column_name, entity, true);
//     }
// }