// src/merge.rs
//
// MergeIterator + Compactor
// =========================
// Merges N sorted segments into one without materializing everything.
//
// Strategy:
//   - Load all segments into memory as sorted RecordBatch vecs (Phase 3 simplification)
//   - Use Arrow's concat + sort to merge (clean, vectorized)
//   - In a future phase this becomes a true k-way streaming merge heap
//
// The Compactor drives this: reads N L0 segments, merges them, writes one L1 segment.

use crate::error::{Result, SstError};
use crate::reader::SegmentReader;
use crate::types::SegmentRef;
use crate::writer::SegmentWriter;

use arrow::array::RecordBatch;
use arrow::compute::{concat_batches, sort_to_indices, take};

/// Merge multiple sorted RecordBatches into a single sorted RecordBatch.
///
/// This is the core of the compaction step.
/// Uses Arrow's vectorized sort — no row-by-row iteration.
pub fn merge_sorted_batches(
    batches: Vec<RecordBatch>,
    sort_key: &str,
) -> Result<RecordBatch> {
    if batches.is_empty() {
        return Err(SstError::Compaction("No batches to merge".into()));
    }

    let schema = batches[0].schema();

    // Step 1: Concatenate all batches into one large batch
    // This is fine for compaction — we have bounded memory per compaction job
    let combined = concat_batches(&schema, &batches)?;

    // Step 2: Sort the combined batch by the sort key
    let key_idx = combined
        .schema()
        .index_of(sort_key)
        .map_err(|_| SstError::SortKeyNotFound(sort_key.into()))?;

    let key_col = combined.column(key_idx);
    let sort_opts = arrow::compute::SortOptions {
        descending: false,
        nulls_first: false,
    };
    let indices = sort_to_indices(key_col.as_ref(), Some(sort_opts), None)?;

    // Step 3: Reorder all columns by sorted indices
    let sorted_cols: Vec<arrow::array::ArrayRef> = combined
        .columns()
        .iter()
        .map(|col| take(col.as_ref(), &indices, None).map_err(SstError::Arrow))
        .collect::<Result<_>>()?;

    Ok(RecordBatch::try_new(schema, sorted_cols)?)
}

/// Compactor — takes a list of segments, merges them, writes one new segment.
///
/// Typical usage:
///   let compactor = Compactor::new(writer);
///   let new_seg = compactor.compact(l0_segments, "user_id")?;
///   // Then delete the old segments
pub struct Compactor {
    writer: SegmentWriter,
}

impl Compactor {
    pub fn new(writer: SegmentWriter) -> Self {
        Self { writer }
    }

    /// Compact N segments into one.
    ///
    /// Steps:
    ///   1. Read all rows from all input segments
    ///   2. Merge-sort them
    ///   3. Write as a new L1 segment
    ///   4. Return the new SegmentRef (caller is responsible for deleting inputs)
    pub fn compact(
        &self,
        segments: Vec<SegmentRef>,
        sort_key: &str,
    ) -> Result<SegmentRef> {
        if segments.is_empty() {
            return Err(SstError::Compaction("Nothing to compact".into()));
        }

        println!(
            "  [compactor] merging {} segments...",
            segments.len()
        );

        // Collect all batches from all segments
        let mut all_batches: Vec<RecordBatch> = Vec::new();
        for seg in &segments {
            let reader = SegmentReader::new(seg.clone());
            let batches = reader.read_all(None)?;
            let rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
            println!(
                "    read segment {} — {} rows",
                seg.meta.id, rows
            );
            all_batches.extend(batches);
        }

        // Merge all into one sorted batch
        let merged = merge_sorted_batches(all_batches, sort_key)?;
        println!(
            "  [compactor] merged into {} rows, writing L1 segment...",
            merged.num_rows()
        );

        // Write as a new segment (writer will sort again — harmless since already sorted)
        let mut new_seg = self.writer.write(&merged)?;

        // Mark as L1
        new_seg.meta.level = 1;

        Ok(new_seg)
    }
}
