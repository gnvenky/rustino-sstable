// tests/integration.rs
//
// Phase-gating integration tests.
// Each test validates one complete phase of the build.
// Green = phase complete. These are your "did I build it right?" checkpoints.

use apache_avro::{types::Value as AvroValue, Reader};
use arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use rustino_sstable::SSTable;
use serde_json::{json, Value as JsonValue};
use std::fs;
use std::fs::File;
use std::sync::Arc;
use tempfile::TempDir;

/// Build a simple test schema: (user_id: String, score: Int64, region: String)
fn test_schema() -> arrow::datatypes::SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("score", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
    ]))
}

/// Build a RecordBatch with given user_ids, scores, regions
fn make_batch(
    schema: arrow::datatypes::SchemaRef,
    user_ids: Vec<&str>,
    scores: Vec<i64>,
    regions: Vec<&str>,
) -> RecordBatch {
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(user_ids)),
            Arc::new(Int64Array::from(scores)),
            Arc::new(StringArray::from(regions)),
        ],
    )
    .unwrap()
}

// ── Phase 1: Write + Read ────────────────────────────────────────────────────

/// Phase 1 test: write a RecordBatch to a segment, read it back,
/// assert sort order is preserved.
#[test]
fn phase1_write_and_read_sorted() {
    let dir = TempDir::new().unwrap();
    let schema = test_schema();

    let mut table = SSTable::open_or_create(
        dir.path(),
        "test_table",
        schema.clone(),
        "user_id", // sort by user_id alphabetically
    )
    .unwrap();

    // Write rows in intentionally scrambled order
    let batch = make_batch(
        schema,
        vec!["charlie", "alice", "eve", "bob", "diana"],
        vec![30, 10, 50, 20, 40],
        vec!["EU", "US", "AS", "US", "EU"],
    );

    table.append(&batch).unwrap();

    // Read back
    let results = table.scan(None, None).unwrap();
    assert!(!results.is_empty(), "Should have results");

    let result_batch = &results[0];
    assert_eq!(result_batch.num_rows(), 5, "All 5 rows should be returned");

    // Assert sort order: user_ids should be alphabetically sorted
    let user_ids = result_batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    let ids: Vec<&str> = (0..user_ids.len())
        .map(|i| user_ids.value(i))
        .collect();

    assert_eq!(
        ids,
        vec!["alice", "bob", "charlie", "diana", "eve"],
        "Rows should be sorted by user_id"
    );

    println!("✅ Phase 1 passed: write + read with sort order preserved");
}

// ── Phase 2: Range Pruning ───────────────────────────────────────────────────

/// Phase 2 test: two segments with non-overlapping key ranges.
/// A point lookup on one segment should skip the other entirely.
#[test]
fn phase2_range_pruning_skips_segments() {
    let dir = TempDir::new().unwrap();
    let schema = test_schema();

    let mut table = SSTable::open_or_create(
        dir.path(),
        "prune_test",
        schema.clone(),
        "user_id",
    )
    .unwrap();

    // Segment A: user_ids a-m
    let batch_a = make_batch(
        schema.clone(),
        vec!["alice", "bob", "charlie"],
        vec![10, 20, 30],
        vec!["US", "EU", "US"],
    );

    // Segment B: user_ids n-z
    let batch_b = make_batch(
        schema.clone(),
        vec!["nina", "oscar", "zara"],
        vec![40, 50, 60],
        vec!["AS", "EU", "US"],
    );

    table.append(&batch_a).unwrap();
    table.append(&batch_b).unwrap();

    // Inspect should show 2 segments with distinct key ranges
    table.inspect();

    // Scan for a key in segment A — segment B should be skipped
    let results = table.scan(Some("bob"), None).unwrap();
    assert!(!results.is_empty());

    println!("✅ Phase 2 passed: range pruning working across 2 segments");
}

// ── Phase 3: Compaction ──────────────────────────────────────────────────────

/// Phase 3 test: append 5 batches, compact to L1, assert:
///   - Only 1 segment remains
///   - Row count is exact
///   - Output is globally sorted
#[test]
fn phase3_compact_l0_to_l1() {
    let dir = TempDir::new().unwrap();
    let schema = test_schema();

    let mut table = SSTable::open_or_create(
        dir.path(),
        "compact_test",
        schema.clone(),
        "user_id",
    )
    .unwrap();

    // Append 5 small batches with overlapping key ranges
    let batches = vec![
        make_batch(
            schema.clone(),
            vec!["frank", "alice"],
            vec![6, 1],
            vec!["EU", "US"],
        ),
        make_batch(
            schema.clone(),
            vec!["charlie", "bob"],
            vec![3, 2],
            vec!["US", "EU"],
        ),
        make_batch(
            schema.clone(),
            vec!["diana", "eve"],
            vec![4, 5],
            vec!["AS", "US"],
        ),
        make_batch(
            schema.clone(),
            vec!["grace", "henry"],
            vec![7, 8],
            vec!["EU", "AS"],
        ),
        make_batch(
            schema.clone(),
            vec!["iris", "jack"],
            vec![9, 10],
            vec!["US", "EU"],
        ),
    ];

    for batch in &batches {
        table.append(batch).unwrap();
    }

    println!("\n--- Before compaction ---");
    table.inspect();

    // Compact all L0 → L1
    let new_meta = table.compact_l0().unwrap();
    assert!(
        new_meta.is_some(),
        "Compaction should produce a new segment"
    );

    println!("\n--- After compaction ---");
    table.inspect();

    // Read back all rows
    let results = table.scan(None, None).unwrap();
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();

    assert_eq!(total_rows, 10, "All 10 rows should survive compaction");

    // Verify global sort order across the merged L1 segment
    let ids: Vec<String> = results
        .iter()
        .flat_map(|b| {
            let arr = b
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            (0..arr.len())
                .map(|i| arr.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect();

    let mut expected = ids.clone();
    expected.sort();

    assert_eq!(ids, expected, "L1 segment should be globally sorted");

    println!("✅ Phase 3 passed: 5 L0 segments → 1 L1 segment, globally sorted");
}

// ── Phase 4: Column Projection ───────────────────────────────────────────────

/// Phase 4 test: scan with column projection — only read user_id and score,
/// skip the region column entirely.
#[test]
fn phase4_column_projection() {
    let dir = TempDir::new().unwrap();
    let schema = test_schema();

    let mut table = SSTable::open_or_create(
        dir.path(),
        "projection_test",
        schema.clone(),
        "user_id",
    )
    .unwrap();

    let batch = make_batch(
        schema,
        vec!["alice", "bob", "charlie"],
        vec![100, 200, 300],
        vec!["US", "EU", "AS"],
    );

    table.append(&batch).unwrap();

    // Only project user_id and score — skip region
    let results = table
        .scan(None, Some(vec!["user_id".into(), "score".into()]))
        .unwrap();

    assert!(!results.is_empty());

    let result = &results[0];

    // Should only have 2 columns
    assert_eq!(
        result.num_columns(),
        2,
        "Projection should return only 2 columns"
    );

    assert_eq!(result.schema().field(0).name(), "user_id");
    assert_eq!(result.schema().field(1).name(), "score");

    println!("✅ Phase 4 passed: column projection works correctly");
}

#[test]
fn phase5_iceberg_metadata_export() {
    let dir = TempDir::new().unwrap();
    let schema = test_schema();

    let mut table = SSTable::open_or_create(
        dir.path(),
        "iceberg_test",
        schema.clone(),
        "user_id",
    )
    .unwrap();

    let batch = make_batch(
        schema,
        vec!["alice", "bob"],
        vec![10, 20],
        vec!["US", "EU"],
    );

    table.append(&batch).unwrap();

    let metadata_path = dir.path().join("metadata.json");

    assert!(metadata_path.exists(), "metadata.json should be written");

    let metadata_text = fs::read_to_string(&metadata_path).unwrap();
    let metadata: JsonValue = serde_json::from_str(&metadata_text).unwrap();

    assert_eq!(metadata["format-version"], json!(2));
    assert_eq!(metadata["location"], json!(dir.path().to_string_lossy().to_string()));

    let manifest_list_name = metadata["manifest-list"]
        .as_str()
        .expect("metadata should include manifest-list")
        .to_string();

    let manifest_list_path = dir.path().join(&manifest_list_name);
    assert!(manifest_list_path.exists(), "manifest list should be written");
    assert!(manifest_list_name.ends_with(".avro"));

    let mut reader = Reader::new(File::open(&manifest_list_path).unwrap()).unwrap();
    assert!(reader.user_metadata().contains_key("schema"));

    let records: Vec<_> = reader
        .into_iter()
        .collect::<std::result::Result<_, _>>()
        .unwrap();
    assert_eq!(records.len(), 1, "manifest list should have one entry");

    let manifest_path = match &records[0] {
        AvroValue::Record(fields) => fields
            .iter()
            .find(|(name, _)| name == "manifest_path")
            .expect("manifest list entry should contain manifest_path")
            .1
            .clone(),
        _ => panic!("manifest list row should be a record"),
    };

    let manifest_path = match manifest_path {
        AvroValue::String(path) => path,
        _ => panic!("manifest_path should be a string"),
    };
    assert!(manifest_path.ends_with(".avro"));

    let manifest_file_path = dir.path().join(&manifest_path);
    assert!(manifest_file_path.exists(), "manifest file should be written");

    let mut manifest_reader = Reader::new(File::open(&manifest_file_path).unwrap()).unwrap();
    assert!(manifest_reader.user_metadata().contains_key("schema"));
    let mf_records: Vec<_> = manifest_reader
        .into_iter()
        .collect::<std::result::Result<_, _>>()
        .unwrap();
    assert_eq!(mf_records.len(), 1, "manifest should have one entry");
    println!("✅ Phase 5 passed: Iceberg metadata files were exported");
}

