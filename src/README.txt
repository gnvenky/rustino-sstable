File by File
types.rs — The Data Structures
Three structs that everything else is built around.
SegmentMeta is the most important one. It's a cheap, JSON-serializable summary of one Parquet file — row count, min/max key, level, file size. The whole point is that you can answer "is my data in this file?" without opening the Parquet file at all. It lives as a .meta sidecar next to each .parquet file.
SegmentRef is the runtime version — it adds the actual file paths to a SegmentMeta. It's never serialized, just used in memory while the process is running.
Manifest is the table of contents — a JSON file (manifest.json) that lists every live segment. Every append and compaction rewrites this file atomically.

error.rs — Unified Error Type
Uses the thiserror crate. The #[from] attributes are the key thing — they let you use ? everywhere in the codebase without manual .map_err() calls. For example when Parquet returns an error, ? automatically converts it to SstError::Parquet.

writer.rs — The Write Path
SegmentWriter::write() is the entry point. Four things happen in order:
1. Schema validation — checks the incoming RecordBatch has the same fields as the writer was configured with. Fails fast with a clear error if not.
2. Sorting — this is the core Arrow move. sort_to_indices() looks at the sort key column and returns an Int32Array of row indices in sorted order — it doesn't move any data yet. Then take() uses those indices to reorder every column simultaneously. This is vectorized and allocation-efficient.
3. Parquet write — uses ArrowWriter with Snappy compression. Returns the file size in bytes.
4. .meta sidecar — extracts the first and last key value from the sorted batch (the min/max range), builds a SegmentMeta, and writes it as pretty-printed JSON. This is what makes skipping fast later.
New segments are always written at level 0 (L0 = fresh, uncompacted).

reader.rs — The Read Path + Skip Logic
SegmentReader::might_contain() is the most important method here. It's a two-layer filter that runs before any Parquet I/O:
Layer 1 — range check: compares the query key against min_key and max_key from the .meta. Zero I/O, just string comparison. If the key is outside the range, the segment is skipped entirely.
Layer 2 — bloom filter: a probabilistic check. Bloom filters have no false negatives — if it says "not here", the key is definitely not in this segment. The false positive rate is set to 1% so occasionally you read a segment unnecessarily, but almost never.
Only after both layers pass does read_all() actually open the Parquet file.
read_all() supports column projection — you pass a list of column names and the Parquet reader only deserializes those columns off disk. Everything else is skipped at the I/O level, not just filtered in memory.
BloomBuilder at the bottom of the file is a small helper used by the writer to populate the bloom filter during a write. It lives in reader.rs so both sides share the same configuration (1% false positive rate, same hash functions).

merge.rs — Compaction
Two things here: the merge function and the compactor that drives it.
merge_sorted_batches() is deliberately simple. It does two Arrow operations:

concat_batches() — combines all input batches into one large batch. No row copying, just re-pointing buffer references.
sort_to_indices() + take() — same pattern as the writer, now operating over the combined batch.

This isn't a streaming k-way heap merge (that's a future phase). It works fine for L0→L1 compaction because you have bounded input — you're only merging a handful of L0 segments at a time, not the entire table.
Compactor::compact() orchestrates it: reads all input segments, calls merge_sorted_batches(), writes the result as a new segment with level = 1, returns the new SegmentRef. It doesn't delete the old segments — that's the SSTable's job, so the SSTable can update the manifest atomically first.

sstable.rs — The Public API
This is the layer most application code touches. It manages three things: the manifest, the append path, and the scan path.
open_or_create() — either loads an existing manifest from disk or creates a fresh one. The table is identified by its directory.
append() — creates a SegmentWriter, writes the batch, pushes the new SegmentMeta into the manifest, increments the manifest version, and flushes to disk before returning. Always durable.
scan() — iterates the manifest, runs might_contain() on each segment, reads survivors, logs how many were skipped. The projection parameter threads all the way down to Parquet I/O.
compact_l0() — filters manifest for L0 segments, hands them to Compactor, then does the manifest surgery: removes old L0 entries, inserts the new L1 entry, flushes, then deletes the old Parquet files. Order matters here — manifest is updated before files are deleted, so a crash between the two leaves orphaned files (harmless) not a corrupted manifest.
inspect() — prints a box-drawing ASCII summary of every segment: level, ID, row count, file size, key range. No arguments, no setup. This is the "low bar" diagnostic entry point — you can call it at any point in a test or a binary and immediately see the state of the table on disk.

tests/integration.rs — The Four Checkpoints
Each test is a complete end-to-end exercise of one phase. They use TempDir from the tempfile crate so every test gets a fresh isolated directory that's deleted when the test ends.

Phase 1 writes rows in scrambled order, reads them back, asserts alphabetical sort.
Phase 2 creates two segments with non-overlapping key ranges, does a point lookup, relies on the log output showing one segment was skipped.
Phase 3 appends 5 overlapping batches, runs compact_l0(), asserts the row count is exact and the output is globally sorted.
Phase 4 scans with a projection list, asserts only 2 columns come back.

For a runnable, step-by-step tutorial, see `README.md` at the repository root and run `cargo run --example tutorial`.

Iceberg demo
------------
The table now exports lightweight Iceberg-style metadata after each append or compaction:
- `metadata.json`
- `manifest-list.json`

Example:

    let mut table = SSTable::open_or_create(dir, "demo_table", schema, "user_id")?;
    table.append(&batch)?;
    assert!(dir.join("metadata.json").exists());
    assert!(dir.join("manifest-list.json").exists());

This keeps the current Parquet data file layout while also generating metadata that can be inspected or extended later.