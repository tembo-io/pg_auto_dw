use pgrx::Timestamp as Timestamp;
use pgrx::Json as JsonValue;

#[derive(Debug)]
pub struct SourceObjectRecord {
    pub key: i64,
    pub timestamp: Timestamp,
    pub data: JsonValue,
}