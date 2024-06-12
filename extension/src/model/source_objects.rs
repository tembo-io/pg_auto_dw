use pgrx::Json as JsonValue;
use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub struct SourceTablePrompt {
    pub key: u32,
    pub table_column_links: JsonValue, // For linking columns to foreign keys
    pub table_details: JsonValue,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ColumnDetail {
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
pub struct TableDetails {
    #[serde(rename = "Schema Name")]
    schema_name: String,
    #[serde(rename = "Table Name")]
    table_name: String,
    #[serde(rename = "Column Details")]
    column_details: Vec<ColumnDetail>,
}