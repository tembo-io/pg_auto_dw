use pgrx::Json as JsonValue;

#[derive(Debug)]
pub struct SourceTablePrompt {
    pub key: u32,
    pub table_column_links: JsonValue, // For linking columns to foreign keys
    pub table_details: JsonValue,
}

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct ColumnDetail {
    Category: String,
    Column_No: i32,
    Confidence: f64,
    Reason: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct TableDetails {
    Schema_Name: String,
    Table_Name: String,
    Column_Details: Vec<ColumnDetail>,
}