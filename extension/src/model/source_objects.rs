use pgrx::Json as JsonValue;
use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub struct SourceTablePrompt {
    pub key: u32,
    pub table_column_links: JsonValue, // For linking columns to foreign keys
    pub table_details: JsonValue,
}


#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    #[serde(rename = "Table ID")]
    table_id: u32,
    #[serde(rename = "Generation")]
    generation: GenerationTableDetail,
    // #[serde(rename = "Generation")]
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GenerationColumnDetail {
    #[serde(rename = "Category")]
    category: String,
    #[serde(rename = "Column No")]
    column_no: i32,
    #[serde(rename = "Confidence")]
    confidence: f64,
    #[serde(rename = "Reason")]
    reason: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GenerationTableDetail {
    #[serde(rename = "Schema Name")]
    schema_name: String,
    #[serde(rename = "Table Name")]
    table_name: String,
    #[serde(rename = "Column Details")]
    response_column_details: Vec<GenerationColumnDetail>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ColumnLink {
    #[serde(rename = "Column Ordinal Position")]
    column_ordinal_position: i32,
    #[serde(rename = "PK Source Objects")]
    pk_source_objects: i32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TableLinks {
    #[serde(rename = "Column Links")]
    column_links: Vec<ColumnLink>,
}