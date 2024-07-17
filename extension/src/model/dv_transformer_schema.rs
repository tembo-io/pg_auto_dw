use uuid::Uuid;
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct DVTransformerSchema {
    #[serde(rename = "ID")]
    pub id: Uuid,
    #[serde(rename = "DW Schema")]
    pub dw_schema: String,
    #[serde(rename = "Create Date")]
    pub create_timestamp_gmt: NaiveDateTime,
    #[serde(rename = "Modified Date")]
    pub modified_timestamp_gmt: NaiveDateTime,
    #[serde(rename = "Business Keys")]
    pub business_keys: Vec<BusinessKey>,
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
    #[serde(rename = "Source Column Data")]
    pub source_column_entities: Vec<ColumnData>,
    #[serde(rename = "Target Column Data")]
    pub target_column_id: Option<ColumnData>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Descriptor {
    #[serde(rename = "ID")]
    pub id: Uuid,
    #[serde(rename = "Descriptor Link")]
    pub descriptor_link: DescriptorLink,
    #[serde(rename = "Orbit")]
    pub orbit: String,
    #[serde(rename = "Is Sensitive")]
    pub is_sensitive: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DescriptorLink {
    #[serde(rename = "ID")]
    pub id: Uuid,
    #[serde(rename = "Alias")]
    pub alias: String,
    #[serde(rename = "Source Column Data")]
    pub source_column_entity: Option<ColumnData>,
    #[serde(rename = "Target Column Data")]
    pub target_column_entiy: Option<ColumnData>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ColumnData {
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