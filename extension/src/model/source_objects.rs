use pgrx::Json as JsonValue;
use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub struct SourceTablePrompt {
    #[allow(dead_code)]
    pub key: u32,
    pub table_column_links: JsonValue, // For linking columns to foreign keys
    pub table_details: JsonValue,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    #[serde(rename = "Table ID")]
    pub table_id: u32,
    #[serde(rename = "Generation")]
    pub generation: GenerationTableDetail,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GenerationColumnDetail {
    #[serde(rename = "Category")]
    pub category: String,
    #[serde(rename = "Business Key Name")]
    pub business_key_name: String,
    #[serde(rename = "Column No")]
    pub column_no: i32,
    #[serde(rename = "Confidence")]
    pub confidence: f64,
    #[serde(rename = "Reason")]
    pub reason: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GenerationTableDetail {
    #[serde(rename = "Schema Name")]
    pub schema_name: String,
    #[serde(rename = "Table Name")]
    pub table_name: String,
    #[serde(rename = "Column Details")]
    pub response_column_details: Vec<GenerationColumnDetail>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ColumnLink {
    #[serde(rename = "Column Ordinal Position")]
    pub column_ordinal_position: i32,
    #[serde(rename = "PK Source Objects")]
    pub pk_source_objects: i32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TableLinks {
    #[serde(rename = "Column Links")]
    pub column_links: Vec<ColumnLink>,
}