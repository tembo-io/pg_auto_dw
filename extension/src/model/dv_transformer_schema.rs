use uuid::Uuid;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct DVTransformerSchema {
    #[serde(rename = "ID")]
    id: Uuid,
    #[serde(rename = "Create Date")]
    create_date: String,
    #[serde(rename = "Modified Date")]
    modified_date: String,
    #[serde(rename = "Business Keys")]
    business_keys: Vec<BusinessKey>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BusinessKey {
    #[serde(rename = "ID")]
    pub id: Uuid,
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "Business Key Part Links")]
    pub business_key_part_links: Vec<BusinessKeyPartLink>,
    #[serde(rename = "Descriptors")]
    pub descriptors: Vec<Descriptor>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BusinessKeyPartLink {
    #[serde(rename = "ID")]
    pub id: Uuid,
    #[serde(rename = "Alias")]
    pub alias: String,
    #[serde(rename = "Source Column IDs")]
    pub source_column_entities: Vec<Entity>,
    #[serde(rename = "Target Column ID")]
    pub target_column_id: Option<Entity>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Descriptor {
    #[serde(rename = "ID")]
    pub id: Uuid,
    #[serde(rename = "Descriptor Link")]
    pub descriptor_link: DescriptorLink,
    #[serde(rename = "Is Sensitive")]
    pub is_sensitive: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DescriptorLink {
    #[serde(rename = "ID")]
    pub id: Uuid,
    #[serde(rename = "Alias")]
    pub alias: String,
    #[serde(rename = "Source Column ID")]
    pub source_column_entity: Option<Entity>,
    #[serde(rename = "Target Column ID")]
    pub target_column_entiy: Option<Entity>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Entity {
    #[serde(rename = "ID")]
    pub id: Uuid,
    #[serde(rename = "System ID")]
    pub system_id: i64,
    #[serde(rename = "Table OID")]
    pub table_oid: u32,
    #[serde(rename = "Column Ordinal Position")]
    pub column_ordinal_position: i16,
    #[serde(rename = "Column Type")]
    pub column_type_name: String,
}