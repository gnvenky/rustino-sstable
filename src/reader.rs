// src/reader.rs
//
// SegmentReader
// =============
// Reads a Parquet segment back as a stream of RecordBatches.
// Supports:
//   - Column projection (only read the columns you need)
//   - Key range pruning (skip segments whose min/max range can't contain the key)
//   - Bloom filter skip (Phase 2 addition)
//
// This is the read path for Phase 1 + Phase 2.

use crate::error::Result;
use crate::types::SegmentRef;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use bloomfilter::Bloom;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;

/// A reader for a single immutable segment.
pub struct SegmentReader {
    segment: SegmentRef,

    /// Optional bloom filter loaded from the sidecar
    bloom: Option<Bloom<String>>,
}

impl SegmentReader {
    pub fn new(segment: SegmentRef) -> Self {
        Self {
            segment,
            bloom: None,
        }
    }

    /// Attach a bloom filter (loaded from the .bloom sidecar file if it exists)
    pub fn with_bloom(mut self, bloom: Bloom<String>) -> Self {
        self.bloom = Some(bloom);
        self
    }

    /// Quick check: can this segment possibly contain the given key?
    ///
    /// Uses two layers:
    ///   1. Key range (min/max) — zero cost, no I/O
    ///   2. Bloom filter — probabilistic, no false negatives
    pub fn might_contain(&self, key: &str) -> bool {
        // Layer 1: range check
        if let (Some(min), Some(max)) = (&self.segment.meta.min_key, &self.segment.meta.max_key) {
            if key < min.as_str() || key > max.as_str() {
                return false; // Definitely not here
            }
        }

        // Layer 2: bloom filter check
        if let Some(bloom) = &self.bloom {
            if !bloom.check(&key.to_string()) {
                return false; // Bloom says definitely not
            }
        }

        true // Could be here — need to actually read
    }

    /// Read all rows from this segment.
    /// Optionally project to only the specified columns.
    pub fn read_all(&self, projection: Option<Vec<String>>) -> Result<Vec<RecordBatch>> {
        let file = File::open(&self.segment.parquet_path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

        // Apply column projection if requested
        let builder = if let Some(cols) = projection {
            let schema = builder.schema().clone();

            let indices: Vec<usize> = cols
                .iter()
                .filter_map(|name| schema.index_of(name).ok())
                .collect();

            // IMPORTANT:
            // Build the projection mask inside a separate scope
            // so the borrow on builder.parquet_schema() ends
            // before builder is moved into with_projection().
            let projection_mask = {
                let parquet_schema = builder.parquet_schema();

                parquet::arrow::ProjectionMask::roots(
                    parquet_schema,
                    indices,
                )
            };

            builder.with_projection(projection_mask)
        } else {
            builder
        };

        let reader = builder
            .with_batch_size(8192) // Read in 8k row batches — tunable
            .build()?;

        let batches: std::result::Result<Vec<_>, _> = reader.collect();

        Ok(batches?)
    }

    /// Return the schema of this segment without reading any row data
    pub fn schema(&self) -> Result<SchemaRef> {
        let file = File::open(&self.segment.parquet_path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

        Ok(builder.schema().clone())
    }

    pub fn meta(&self) -> &crate::types::SegmentMeta {
        &self.segment.meta
    }
}

/// BloomFilter builder — called during SegmentWriter to produce
/// the bloom sidecar. Kept here so reader and writer share the same params.
pub struct BloomBuilder {
    bloom: Bloom<String>,
}

impl BloomBuilder {
    /// Create a bloom filter sized for `expected_items` with a 1% false positive rate
    pub fn new(expected_items: usize) -> Self {
        Self {
            bloom: Bloom::new_for_fp_rate(expected_items, 0.01),
        }
    }

    pub fn add(&mut self, key: &str) {
        self.bloom.set(&key.to_string());
    }

    pub fn finish(self) -> Bloom<String> {
        self.bloom
    }
}
