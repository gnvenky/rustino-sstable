# rustino-sstable

A lightweight SSTable-style Rust project using Apache Arrow and Parquet, with a simple Iceberg-style metadata export path.
appends create sorted L0 Parquet segments, scans use the manifest plus key-range metadata to skip irrelevant files, and compaction merges all L0 files into one globally sorted L1 segment while exporting lightweight Iceberg-style metadata alongside the table.

## Step-by-step tutorial

This tutorial shows you how to run the project and inspect the generated files.

### 1) Build the project

```bash
cd /Users/venky/rustino-sstable
cargo check
```

### 2) Run the integration checks

```bash
cargo test --test integration
```

### 3) Run the tutorial demo example

```bash
cargo run --example tutorial
```

This creates a `tutorial-demo/` directory inside the repository with:

- `*.parquet` data files
- `*.meta` segment sidecar files
- `manifest.json`
- `metadata.json`
- `manifest-list.json`

### 4) Inspect the tutorial output

Open `tutorial-demo/metadata.json` and `tutorial-demo/manifest-list.json` to see the generated Iceberg-style metadata.

### What the tutorial does

The example:

- builds an Arrow schema
- opens or creates an SSTable
- appends sample rows
- scans the table
- prints results
- compacts L0 segments into a single L1 segment
- exports Iceberg metadata files
- prints the output directory

## L0 and L1 segments

This project stores data in immutable segment files. Each segment is a Parquet
file with a `.meta` sidecar that records its row count, file size, compaction
level, and sort-key range.

`L0` segments are fresh writes. Every call to `SSTable::append()` writes a new
Parquet segment and records it in `manifest.json` as level `0`. Each L0 segment
is sorted internally by the table's sort key, but different L0 segments can have
overlapping key ranges.

For example, after several appends the table might contain:

```text
L0 seg_a: alice..frank
L0 seg_b: bob..charlie
L0 seg_c: diana..eve
```

`L1` segments are compacted output. When `compact_l0()` runs, it reads all live
L0 segments, merges their rows, globally sorts the combined data by the sort key,
writes one new segment, and marks that segment as level `1`.

After compaction, the manifest changes to something like:

```text
L1 seg_x: alice..frank
```

The old L0 entries are removed from the manifest and their files are deleted
after the new L1 segment is written. This reduces read fanout because scans have
fewer segment files to check and open. Scans can still use each segment's
`min_key` and `max_key` from the `.meta` sidecar to skip files whose range cannot
contain the requested key.

The current implementation is intentionally small: it supports a simple
`L0 -> L1` compaction path. It does not yet implement deeper levels such as L2 or
L3, size-tiered compaction policies, tombstones, or deduplication semantics.

### Run the demo repeatedly

If you want to repeat the demo, remove or rename `tutorial-demo/` and run the example again.

## Docker validation with Spark

A Docker Compose file is included so you can verify the generated table files with Spark.

1. Build and run the demo:

```bash
cd /Users/venky/rustino-sstable
cargo run --example tutorial
```

2. Start the Spark container:

```bash
docker compose up
```

3. Attach to the running container and verify the Parquet files:

```bash
docker attach rustino-iceberg-spark
```

In the Spark REPL, run:

```python
df = spark.read.parquet("/data/*.parquet")
df.show()
df.printSchema()
```

4. If you want an interactive shell instead of attaching, use:

```bash
docker compose exec spark bash
/opt/spark/bin/pyspark
```

## Notes

- The current implementation uses Parquet files for columnar storage.
- The metadata export is not a full Iceberg implementation, but it writes `metadata.json` and `manifest-list.json` alongside the table.
- `src/README.txt` contains internal developer notes; use `README.md` as the user-facing tutorial.

  ```mermaid
flowchart TD
    A["Application / Example Code"] --> B["SSTable API"]

    B --> C["append(RecordBatch)"]
    B --> D["scan(key_hint, projection)"]
    B --> E["compact_l0()"]
    B --> F["inspect()"]

    C --> G["SegmentWriter"]
    G --> H["Validate Arrow Schema"]
    H --> I["Sort Rows by Sort Key"]
    I --> J["Write Parquet Segment"]
    I --> K["Extract min_key / max_key"]
    J --> L["*.parquet"]
    K --> M["*.meta Sidecar"]
    L --> N["manifest.json"]
    M --> N

    D --> O["Read manifest.json"]
    O --> P["For Each Segment"]
    P --> Q["Range Pruning using min_key / max_key"]
    Q -->|Cannot contain key| R["Skip Segment"]
    Q -->|Might contain key| S["SegmentReader"]
    S --> T["Read Parquet"]
    T --> U["Optional Column Projection"]
    U --> V["RecordBatch Results"]

    E --> W["Find L0 Segments"]
    W --> X["Read L0 Parquet Files"]
    X --> Y["Merge + Globally Sort Rows"]
    Y --> Z["Write New L1 Segment"]
    Z --> AA["Update manifest.json"]
    AA --> AB["Delete Old L0 Files"]

    C --> AC["Iceberg Metadata Export"]
    E --> AC
    AC --> AD["manifest-*.avro"]
    AC --> AE["manifest-list-*.avro"]
    AC --> AF["metadata.json"]

    F --> N
    F --> AG["Print Table Summary"]

    subgraph Storage["On-Disk Table Directory"]
        L
        M
        N
        AD
        AE
        AF
    end

    subgraph Levels["Segment Levels"]
        L0["L0: Fresh append segments"]
        L1["L1: Compacted segment"]
    end

    W --> L0
    Z --> L1

```
