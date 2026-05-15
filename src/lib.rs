// src/lib.rs
//
// rustino-sstable
// ===============
// A columnar SSTable implementation built on Apache Arrow + Parquet.
//
// Module layout:
//   types   — shared structs (SegmentMeta, Manifest, SegmentRef)
//   error   — unified SstError type
//   writer  — SegmentWriter: sort + write Parquet + .meta sidecar
//   reader  — SegmentReader: read with projection + range/bloom pruning
//   merge   — MergeIterator + Compactor: k-way merge of sorted segments
//   sstable — SSTable: top-level table with manifest + append/scan/compact

pub mod error;
pub mod iceberg;
pub mod merge;
pub mod reader;
pub mod sstable;
pub mod types;
pub mod writer;

// Convenient re-exports for library users
pub use error::{Result, SstError};
pub use iceberg::IcebergTableMetadata;
pub use sstable::SSTable;
pub use types::{Manifest, SegmentMeta, SegmentRef};
