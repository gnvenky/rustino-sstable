use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use rustino_sstable::SSTable;
use std::fs;
use std::sync::Arc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("score", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
    ]));

    let root_dir = std::env::current_dir()?;
    let table_dir = root_dir.join("tutorial-demo");
    fs::create_dir_all(&table_dir)?;

    let mut table = SSTable::open_or_create(&table_dir, "tutorial_table", schema.clone(), "user_id")?;

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["charlie", "alice", "bob"])),
            Arc::new(Int64Array::from(vec![30, 10, 20])),
            Arc::new(StringArray::from(vec!["EU", "US", "US"])),
        ],
    )?;

    println!("1) Appending sample rows...");
    table.append(&batch)?;

    println!("2) Scanning the table...");
    let results = table.scan(None, None)?;
    println!("   scan returned {} batch(es)", results.len());

    if let Some(batch) = results.get(0) {
        println!("   first batch rows: {}", batch.num_rows());
    }

    table.inspect();

    println!("3) Running compaction...");
    table.compact_l0()?;
    table.inspect();

    println!("4) Iceberg-style metadata files exported to: {}", table_dir.display());
    println!("   - metadata.json");
    println!("   - manifest-list.json");

    Ok(())
}
