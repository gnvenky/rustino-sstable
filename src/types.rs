// src/types.rs
// Core types shared across the SSTable implementation

use arrow::datatypes::SchemaRef;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Metadata about a single immutable segment on disk.
/// This is persisted alongside the Parquet file as a .meta sidecar.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentMeta {
    /// Unique ID for this segment (used as filename base)
    pub id: String,

    /// Number of rows stored in this segment
    pub row_count: u64,

    /// Minimum value of the sort key column (as string for simplicity)
    pub min_key: Option<String>,

    /// Maximum value of the sort key column
    pub max_key: Option<String>,

    /// Size in bytes of the Parquet file
    pub file_size_bytes: u64,

    /// Which compaction level this segment lives at (0 = fresh, 1 = compacted)
    pub level: u8,

    /// Timestamp when this segment was written (Unix millis)
    pub created_at_ms: u64,
}

/// A reference to a segment: its metadata + where it lives on disk
#[derive(Debug, Clone)]
pub struct SegmentRef {
    pub meta: SegmentMeta,
    pub parquet_path: PathBuf,
    pub meta_path: PathBuf,
}

/// The SSTable manifest — a JSON file describing all live segments
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub name: String,
    pub segments: Vec<SegmentMeta>,
    pub version: u64,
}

/// Column statistics collected during a segment write
#[derive(Debug, Clone)]
pub struct ColumnStats {
    pub name: String,
    pub schema: SchemaRef,
    pub row_count: u64,
    pub min_key: Option<String>,
    pub max_key: Option<String>,
}
