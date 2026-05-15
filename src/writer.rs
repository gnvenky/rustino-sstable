// src/writer.rs
//
// SegmentWriter
// =============
// Takes a RecordBatch, sorts it by a configurable sort key column,
// writes it to a Parquet file, and produces a .meta sidecar.
//
// This is Phase 1 of the columnar SSTable build.

use crate::error::{Result, SstError};
use crate::types::{SegmentMeta, SegmentRef};

use arrow::array::{Array, StringArray, Int64Array, RecordBatch};
use arrow::compute::sort_to_indices;
use arrow::compute::take;
use arrow::datatypes::{DataType, SchemaRef};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

pub struct SegmentWriter {
    /// Directory where segment files are written
    base_dir: PathBuf,

    /// Name of the column to sort by
    sort_key: String,

    /// Arrow schema — validated on first write
    schema: SchemaRef,
}

impl SegmentWriter {
    pub fn new(base_dir: impl Into<PathBuf>, sort_key: impl Into<String>, schema: SchemaRef) -> Self {
        Self {
            base_dir: base_dir.into(),
            sort_key: sort_key.into(),
            schema,
        }
    }

    /// Write a RecordBatch as a new immutable segment.
    ///
    /// Steps:
    ///   1. Validate the batch schema matches what we expect
    ///   2. Sort rows by the sort key column
    ///   3. Write sorted rows to Parquet
    ///   4. Write a .meta sidecar with stats
    ///   5. Return a SegmentRef describing the result
    pub fn write(&self, batch: &RecordBatch) -> Result<SegmentRef> {
        if batch.num_rows() == 0 {
            return Err(SstError::EmptyBatch);
        }

        // Validate schema
        self.validate_schema(batch.schema())?;

        // Sort the batch by the sort key
        let sorted = self.sort_batch(batch)?;

        // Generate a unique segment ID using current time + random suffix
        let id = Self::new_segment_id();
        let parquet_path = self.base_dir.join(format!("{}.parquet", id));
        let meta_path = self.base_dir.join(format!("{}.meta", id));

        // Write Parquet file
        let file_size = self.write_parquet(&sorted, &parquet_path)?;

        // Extract min/max key values for the sparse index
        let (min_key, max_key) = self.extract_key_range(&sorted)?;

        // Build and persist metadata
        let meta = SegmentMeta {
            id: id.clone(),
            row_count: sorted.num_rows() as u64,
            min_key,
            max_key,
            file_size_bytes: file_size,
            level: 0, // Fresh segments start at L0
            created_at_ms: Self::now_ms(),
        };

        self.write_meta(&meta, &meta_path)?;

        Ok(SegmentRef {
            meta,
            parquet_path,
            meta_path,
        })
    }

    /// Sort all rows in the batch by the sort key column.
    /// Returns a new RecordBatch with rows in ascending order.
    fn sort_batch(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let key_col_idx = batch
            .schema()
            .index_of(&self.sort_key)
            .map_err(|_| SstError::SortKeyNotFound(self.sort_key.clone()))?;

        let key_col = batch.column(key_col_idx);

        // sort_to_indices returns an Int32Array of row indices in sorted order
        let sort_options = arrow::compute::SortOptions {
            descending: false,
            nulls_first: false,
        };
        let indices = sort_to_indices(key_col.as_ref(), Some(sort_options), None)?;

        // Reorder all columns according to the sorted indices
        let sorted_columns: Vec<arrow::array::ArrayRef> = batch
            .columns()
            .iter()
            .map(|col| take(col.as_ref(), &indices, None).map_err(SstError::Arrow))
            .collect::<Result<Vec<_>>>()?;

        Ok(RecordBatch::try_new(batch.schema(), sorted_columns)?)
    }

    /// Write a sorted RecordBatch to a Parquet file.
    /// Returns the number of bytes written.
    fn write_parquet(&self, batch: &RecordBatch, path: &Path) -> Result<u64> {
        std::fs::create_dir_all(&self.base_dir)?;

        let file = File::create(path)?;
        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::SNAPPY)
            .build();

        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
        writer.write(batch)?;
        writer.close()?;

        let size = std::fs::metadata(path)?.len();
        Ok(size)
    }

    /// Write the SegmentMeta to a JSON sidecar file.
    fn write_meta(&self, meta: &SegmentMeta, path: &Path) -> Result<()> {
        let json = serde_json::to_string_pretty(meta)?;
        std::fs::write(path, json)?;
        Ok(())
    }

    /// Extract the min and max values of the sort key column.
    /// The batch must already be sorted for this to give correct results.
    fn extract_key_range(&self, sorted_batch: &RecordBatch) -> Result<(Option<String>, Option<String>)> {
        let idx = sorted_batch
            .schema()
            .index_of(&self.sort_key)
            .map_err(|_| SstError::SortKeyNotFound(self.sort_key.clone()))?;

        let col = sorted_batch.column(idx);
        let n = col.len();

        if n == 0 {
            return Ok((None, None));
        }

        // We handle String and Int64 sort keys for now
        match col.data_type() {
            DataType::Utf8 => {
                let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
                let min = arr.value(0).to_string();
                let max = arr.value(n - 1).to_string();
                Ok((Some(min), Some(max)))
            }
            DataType::Int64 => {
                let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
                let min = arr.value(0).to_string();
                let max = arr.value(n - 1).to_string();
                Ok((Some(min), Some(max)))
            }
            _other => {
                // For other types just skip range tracking for now
                Ok((None, None))
            }
        }
    }

    fn validate_schema(&self, incoming: SchemaRef) -> Result<()> {
        if incoming.fields() != self.schema.fields() {
            return Err(SstError::SchemaMismatch {
                expected: format!("{:?}", self.schema.fields()),
                actual: format!("{:?}", incoming.fields()),
            });
        }
        Ok(())
    }

    fn new_segment_id() -> String {
        // Simple ID: timestamp_ms + small random suffix
        let ts = Self::now_ms();
        let rng: u32 = rand_u32();
        format!("seg_{:016x}_{:08x}", ts, rng)
    }

    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }
}

/// Minimal xorshift random — avoids pulling in `rand` crate just for IDs
fn rand_u32() -> u32 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    SystemTime::now().hash(&mut h);
    std::thread::current().id().hash(&mut h);
    h.finish() as u32
}
