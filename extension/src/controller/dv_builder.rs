use pgrx::prelude::*;
use uuid::Uuid;
use std::collections::HashMap;

use crate::utility::guc;
use crate::model::dv_transformer_schema;

pub fn build_dv(dv_objects_query: &str) {

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
                        let column_name = dv_transformer_object.get_datum_by_ordinal(4).unwrap().value::<String>().unwrap().unwrap();
                        let column_type_name = dv_transformer_object.get_datum_by_ordinal(5).unwrap().value::<String>().unwrap().unwrap();
                        let system_id = dv_transformer_object.get_datum_by_ordinal(6).unwrap().value::<i64>().unwrap().unwrap();
                        let table_oid: u32 = dv_transformer_object.get_datum_by_ordinal(7).unwrap().value::<u32>().unwrap().unwrap();
                        let column_ordinal_position = dv_transformer_object.get_datum_by_ordinal(8).unwrap().value::<i16>().unwrap().unwrap();
                        
                        // log!("dv_transformer_object PrintOut: {:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}", 
                        // schema_name, table_name, column_category, column_name, column_type_name, system_id, table_oid, column_ordinal_position);

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
    let mut business_key_v: Vec<dv_transformer_schema::BusinessKey> = Vec::new();
    for dv_transformer_objects_v in dv_transformer_objects_hm {

        let mut descriptors: Vec<dv_transformer_schema::Descriptor> = Vec::new();
        let mut business_key_part_links: Vec<dv_transformer_schema::BusinessKeyPartLink> = Vec::new();

        // Build Descriptors
        for dv_transformer_object in &dv_transformer_objects_v.1 {

            let entity_id = Uuid::new_v4();

            let entity = dv_transformer_schema::Entity {
                id: entity_id,
                system_id: dv_transformer_object.system_id,
                table_oid: dv_transformer_object.table_oid,
                column_ordinal_position: dv_transformer_object.column_ordinal_position,
                column_type_name: dv_transformer_object.column_type_name.clone(),
            };

            if dv_transformer_object.column_category == ColumnCategory::Descriptor {
                let descriptor = get_descriptor(dv_transformer_object.column_name.clone(), entity, false);
                descriptors.push(descriptor);
            } else if dv_transformer_object.column_category == ColumnCategory::DescriptorSensitive {
                let descriptor = get_descriptor(dv_transformer_object.column_name.clone(), entity, true);
                descriptors.push(descriptor);
            }
        }

        // Build Business Key Part Links
        for dv_transformer_object in &dv_transformer_objects_v.1 {

            let entity_id = Uuid::new_v4();

            let entity = dv_transformer_schema::Entity {
                id: entity_id,
                system_id: dv_transformer_object.system_id,
                table_oid: dv_transformer_object.table_oid,
                column_ordinal_position: dv_transformer_object.column_ordinal_position,
                column_type_name: dv_transformer_object.column_type_name.clone(),
            };

            
            if dv_transformer_object.column_category == ColumnCategory::BusinessKey {
                let business_key_part_link = get_business_key_part_link(dv_transformer_object.column_name.clone(), entity);
                business_key_part_links.push(business_key_part_link);
            }
        }

        let business_key_id = Uuid::new_v4();
        let business_key = dv_transformer_schema::BusinessKey {
            id: business_key_id,
            name: dv_transformer_objects_v.1[0].table_name.clone(),
            business_key_part_links,
            descriptors 
        };

        // log!("Business Key for DV Generation: {:?}", business_key);
        business_key_v.push(business_key);
    }

    let dw_schema = guc::get_guc(guc::PgAutoDWGuc::DwSchema).expect("DW SCHEMA GUC is not set.");

    // Build DV
    // Push DV Function
    let mut dv_ddl_sql = String::new();

    for business_key in business_key_v {

        let mut hub_bks = String::new();

        for part_link in business_key.business_key_part_links {
            let r = format!(r#",
                {}_bk VARCHAR"#, part_link.alias);
            hub_bks.push_str(&r);
        }

        let hub = 
            format!(r#"
                CREATE TABLE {}.hub_{} (
                    hub_{}_hk VARCHAR NOT NULL,
                    load_ts TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                    record_source VARCHAR NOT NULL{}
                );
            "#, dw_schema, business_key.name, business_key.name, hub_bks);

        // log!("Hub SQL: {}", hub);
        dv_ddl_sql.push_str(&format!(
            r#"
            {}"#, hub));

        // TODO: Have an unlimited number of satellites "orbits."
        let mut sat_descriptors = String::new();
        let mut sat_descriptors_sensitive = String::new();

        for descriptor in &business_key.descriptors {
            let desc_column_name = &descriptor.descriptor_link.alias;
            let desc_column_type = &descriptor.descriptor_link.source_column_entity.as_ref().unwrap().column_type_name;
            let r = format!(r#",
                                    {} {}"#, desc_column_name, desc_column_type);

            if !descriptor.is_sensitive {
                sat_descriptors.push_str(&r);
            } else {
                sat_descriptors_sensitive.push_str(&r);
            }
        }

        let sat = 
            format!(r#"
                CREATE TABLE {}.sat_{} (
                    hub_{}_hk VARCHAR NOT NULL,
                    load_ts TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                    record_source VARCHAR NOT NULL,
                    sat_{}_hd VARCHAR NOT NULL{}
                );
            "#, dw_schema, business_key.name, business_key.name, business_key.name, sat_descriptors); // TODO: Should be the name of source table unless specified.

        let sat_sensitive = 
            format!(r#"
                CREATE TABLE {}.sat_{}_sensitive_data (
                    hub_{}_hk VARCHAR NOT NULL,
                    load_ts TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                    record_source VARCHAR NOT NULL,
                    sat_{}_hd VARCHAR NOT NULL{}
                );
            "#, dw_schema, business_key.name, business_key.name, business_key.name, sat_descriptors_sensitive); // TODO: Should be the name of source table unless specified.
        
        if sat_descriptors.len() > 0 {
            // log!("Sat SQL: {} \n Length {}", sat, sat_descriptors.len());
            dv_ddl_sql.push_str(&format!(
                r#"
                {}"#, sat));
        } else {
            log!("No Sat Fields");
        }
        if sat_descriptors_sensitive.len() > 0 {
            // log!("Sat Sensitive SQL: {} \n Length {}", sat_sensitive, sat_descriptors_sensitive.len());
            dv_ddl_sql.push_str(&format!(
                r#"
                {}"#, sat_sensitive));
        } else {
            log!("No Sensitive Sat Fields");
        }
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

}

fn get_descriptor(column_name: String, entity: dv_transformer_schema::Entity, is_sensitive: bool) -> dv_transformer_schema::Descriptor {
    let descriptor_link_id = Uuid::new_v4();
    let descriptor_link = dv_transformer_schema::DescriptorLink {
        id: descriptor_link_id,
        alias: column_name, // TODO: Give the user an option to change name in the future - modality TBD.
        source_column_entity: Some(entity),
        target_column_entiy: None,
    };
    let descriptor_id = Uuid::new_v4();
    let descriptor = dv_transformer_schema::Descriptor {
        id: descriptor_id,
        descriptor_link,
        is_sensitive: is_sensitive,
    };
    // log!("dv Enity Object {:?}", &descriptor);
    descriptor
}

fn get_business_key_part_link(alias: String, entity: dv_transformer_schema::Entity) -> dv_transformer_schema::BusinessKeyPartLink {
    let business_key_part_link_id = Uuid::new_v4();
    let mut source_column_entities: Vec<dv_transformer_schema::Entity> = Vec::new(); 
    source_column_entities.push(entity);

    let business_key_link = dv_transformer_schema::BusinessKeyPartLink {
        id: business_key_part_link_id,
        alias,
        source_column_entities: source_column_entities,
        target_column_id: None,
    };

    business_key_link
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
    #[allow(dead_code)]
    schema_name: String,
    table_name: String,
    column_name: String,
    column_type_name: String,
    system_id: i64,
    table_oid: u32,
    column_ordinal_position: i16,
    column_category: ColumnCategory,
}
