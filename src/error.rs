// src/error.rs
// Unified error type for the rustino-sstable crate

use thiserror::Error;

#[derive(Error, Debug)]
pub enum SstError {
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("Avro error: {0}")]
    Avro(#[from] apache_avro::Error),

    #[error("Schema mismatch: expected {expected}, got {actual}")]
    SchemaMismatch { expected: String, actual: String },

    #[error("Segment not found: {0}")]
    SegmentNotFound(String),

    #[error("Empty batch — nothing to write")]
    EmptyBatch,

    #[error("Sort key column '{0}' not found in schema")]
    SortKeyNotFound(String),

    #[error("Compaction error: {0}")]
    Compaction(String),
}

pub type Result<T> = std::result::Result<T, SstError>;
