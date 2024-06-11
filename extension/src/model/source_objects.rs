use pgrx::Json as JsonValue;

#[derive(Debug)]
pub struct SourceTablePrompt {
    pub key: u32,
    pub table_column_links: JsonValue, // For linking columns to foreign keys
    pub table_details: JsonValue,
}