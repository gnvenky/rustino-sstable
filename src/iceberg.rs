// src/iceberg.rs
// Iceberg-compatible metadata layer for rustino-sstable.

use crate::error::Result;
use crate::types::SegmentMeta;
use apache_avro::{Schema, Writer};
use arrow::datatypes::{DataType, SchemaRef};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::path::Path;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct IcebergTableMetadata {
    pub format_version: u32,
    pub table_uuid: String,
    pub location: String,
    pub last_updated_ms: u64,
    pub last_sequence_number: u64,
    pub last_assigned_field_id: u32,
    pub last_assigned_partition_id: u32,
    pub current_schema_id: u32,
    pub default_spec_id: u32,
    pub current_spec_id: u32,
    pub schemas: Vec<IcebergSchema>,
    pub partition_specs: Vec<IcebergPartitionSpec>,
    pub properties: HashMap<String, String>,
    pub manifest_list: String,
    pub snapshots: Vec<IcebergSnapshot>,
    pub snapshot_log: Vec<IcebergSnapshotLog>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub manifests: Vec<IcebergManifest>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct IcebergSchema {
    pub schema_id: u32,
    #[serde(rename = "type")]
    pub kind: String,
    pub fields: Vec<IcebergSchemaField>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct IcebergSchemaField {
    pub id: u32,
    pub name: String,
    pub required: bool,
    #[serde(rename = "type")]
    pub kind: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct IcebergPartitionSpec {
    pub spec_id: u32,
    pub fields: Vec<IcebergPartitionField>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct IcebergPartitionField {
    pub field_id: u32,
    pub name: String,
    pub transform: String,
    pub source_id: u32,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct IcebergSnapshot {
    pub snapshot_id: u64,
    pub parent_snapshot_id: Option<u64>,
    pub sequence_number: u64,
    pub timestamp_ms: u64,
    pub manifest_list: String,
    pub summary: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct IcebergSnapshotLog {
    pub timestamp_ms: u64,
    pub snapshot_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct IcebergManifest {
    pub manifest_path: String,
    pub manifest_length: u64,
    pub partition_spec_id: u32,
    pub content: String,
    pub sequence_number: u64,
    pub snapshot_id: u64,
    pub added_snapshot_id: u64,
    pub added_files_count: u32,
    pub existing_files_count: u32,
    pub deleted_files_count: u32,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct IcebergDataFile {
    pub file_path: String,
    pub file_format: String,
    pub partition: HashMap<String, Option<String>>,
    pub record_count: u64,
    pub file_size_in_bytes: u64,
    pub lower_bounds: HashMap<String, String>,
    pub upper_bounds: HashMap<String, String>,
}

impl IcebergTableMetadata {
    pub fn build_metadata(
        name: &str,
        location: &str,
        schema: SchemaRef,
        segments: &[SegmentMeta],
        manifest_version: u64,
        manifest_list_path: &str,
        manifests: Vec<IcebergManifest>,
    ) -> Self {
        let schema = IcebergSchema::from_arrow(&schema);
        let partition_spec = IcebergPartitionSpec {
            spec_id: 0,
            fields: vec![],
        };

        let last_assigned_field_id = schema.fields.len() as u32;

        let snapshots = if segments.is_empty() {
            vec![]
        } else {
            vec![IcebergSnapshot {
                snapshot_id: manifest_version,
                parent_snapshot_id: None,
                sequence_number: manifest_version,
                timestamp_ms: now_ms(),
                manifest_list: manifest_list_path.to_string(),
                summary: build_summary(segments.len() as u64),
            }]
        };

        Self {
            format_version: 2,
            table_uuid: generate_uuid(&format!("{}:{}", name, location)),
            location: location.to_string(),
            last_updated_ms: now_ms(),
            last_sequence_number: manifest_version,
            last_assigned_field_id,
            last_assigned_partition_id: 0,
            current_schema_id: 0,
            default_spec_id: 0,
            current_spec_id: 0,
            schemas: vec![schema],
            partition_specs: vec![partition_spec],
            properties: HashMap::new(),
            manifest_list: manifest_list_path.to_string(),
            snapshots,
            snapshot_log: vec![],
            manifests,
        }
    }

    pub fn build_manifest_file(segments: &[SegmentMeta], sort_key: &str) -> Vec<IcebergDataFile> {
        segments
            .iter()
            .map(|segment| IcebergDataFile::from_segment(segment, sort_key))
            .collect()
    }

    pub fn write_manifest_avro(
        dir: &Path,
        manifest_version: u64,
        segments: &[SegmentMeta],
        _sort_key: &str,
        table_schema: SchemaRef,
    ) -> Result<(String, u64)> {
        let schema = Self::manifest_entry_avro_schema()?;
        let manifest_file_name = format!("manifest-{}.avro", manifest_version);
        let manifest_path = dir.join(&manifest_file_name);
        let file = File::create(&manifest_path)?;
        let mut writer = Writer::new(&schema, file);

        let manifest_schema_json = serde_json::to_string(&IcebergSchema::from_arrow(&table_schema))?;
        let partition_spec_json = serde_json::to_string(&IcebergPartitionSpec {
            spec_id: 0,
            fields: vec![],
        })?;

        writer.add_user_metadata("schema".to_string(), manifest_schema_json)?;
        writer.add_user_metadata("schema-id".to_string(), "0")?;
        writer.add_user_metadata("partition-spec".to_string(), partition_spec_json)?;
        writer.add_user_metadata("partition-spec-id".to_string(), "0")?;
        writer.add_user_metadata("format-version".to_string(), "2")?;
        writer.add_user_metadata("content".to_string(), "data")?;

        let entries: Vec<ManifestEntry> = segments
            .iter()
            .map(ManifestEntry::from_segment)
            .collect();

        writer.extend_ser(entries)?;
        writer.flush()?;

        let manifest_length = manifest_path.metadata()?.len();
        Ok((manifest_file_name, manifest_length))
    }

    pub fn write_manifest_list_avro(
        dir: &Path,
        manifest_version: u64,
        manifest_file_name: &str,
        manifest_file_length: u64,
        added_files_count: u32,
        added_rows_count: u64,
    ) -> Result<(String, u64)> {
        let schema = Self::manifest_list_avro_schema()?;
        let manifest_list_name = format!("manifest-list-{}.avro", manifest_version);
        let manifest_list_path = dir.join(&manifest_list_name);
        let file = File::create(&manifest_list_path)?;
        let mut writer = Writer::new(&schema, file);

        let manifest_list_schema_json = serde_json::to_string(&schema)?;
        let partition_spec_json = serde_json::to_string(&Vec::<IcebergPartitionField>::new())?;

        writer.add_user_metadata("schema".to_string(), manifest_list_schema_json)?;
        writer.add_user_metadata("schema-id".to_string(), "0")?;
        writer.add_user_metadata("partition-spec".to_string(), partition_spec_json)?;
        writer.add_user_metadata("partition-spec-id".to_string(), "0")?;
        writer.add_user_metadata("format-version".to_string(), "2")?;
        writer.add_user_metadata("content".to_string(), "data")?;

        let entry = ManifestListEntry {
            manifest_path: manifest_file_name.to_string(),
            manifest_length: manifest_file_length as i64,
            partition_spec_id: 0,
            content: 0,
            sequence_number: manifest_version as i64,
            min_sequence_number: 0,
            added_snapshot_id: manifest_version as i64,
            added_files_count: added_files_count as i32,
            existing_files_count: 0,
            deleted_files_count: 0,
            added_rows_count: added_rows_count as i64,
            existing_rows_count: 0,
            deleted_rows_count: 0,
            partitions: None,
        };

        writer.append_ser(entry)?;
        writer.flush()?;

        let list_length = manifest_list_path.metadata()?.len();
        Ok((manifest_list_name, list_length))
    }

    fn manifest_entry_avro_schema() -> Result<Schema> {
        let schema = r#"
        {
          "type": "record",
          "name": "manifest_entry",
          "fields": [
            {"name": "status", "type": "int", "field-id": 0},
            {"name": "snapshot_id", "type": ["null", "long"], "default": null, "field-id": 1},
            {"name": "sequence_number", "type": ["null", "long"], "default": null, "field-id": 3},
            {"name": "file_sequence_number", "type": ["null", "long"], "default": null, "field-id": 4},
            {"name": "data_file", "type": {
              "type": "record",
              "name": "data_file",
              "fields": [
                {"name": "content", "type": "int", "field-id": 134},
                {"name": "file_path", "type": "string", "field-id": 100},
                {"name": "file_format", "type": "string", "field-id": 101},
                {"name": "partition", "type": {"type": "record", "name": "partition", "fields": []}, "field-id": 102},
                {"name": "record_count", "type": "long", "field-id": 103},
                {"name": "file_size_in_bytes", "type": "long", "field-id": 104},
                {"name": "lower_bounds", "type": ["null", {"type": "map", "values": "bytes"}], "default": null, "field-id": 125},
                {"name": "upper_bounds", "type": ["null", {"type": "map", "values": "bytes"}], "default": null, "field-id": 128},
                {"name": "key_metadata", "type": ["null", "bytes"], "default": null, "field-id": 131},
                {"name": "split_offsets", "type": ["null", {"type": "array", "items": "long"}], "default": null, "field-id": 132},
                {"name": "equality_ids", "type": ["null", {"type": "array", "items": "int"}], "default": null, "field-id": 135},
                {"name": "sort_order_id", "type": ["null", "int"], "default": null, "field-id": 140},
                {"name": "first_row_id", "type": ["null", "long"], "default": null, "field-id": 142},
                {"name": "referenced_data_file", "type": ["null", "string"], "default": null, "field-id": 143},
                {"name": "content_offset", "type": ["null", "long"], "default": null, "field-id": 144},
                {"name": "content_size_in_bytes", "type": ["null", "long"], "default": null, "field-id": 145}
              ]
            }, "field-id": 2}
          ]
        }
        "#;
        Schema::parse_str(schema).map_err(Into::into)
    }

    fn manifest_list_avro_schema() -> Result<Schema> {
        let schema = r#"
        {
          "type": "record",
          "name": "manifest_file",
          "fields": [
            {"name": "manifest_path", "type": "string", "field-id": 500},
            {"name": "manifest_length", "type": "long", "field-id": 501},
            {"name": "partition_spec_id", "type": "int", "field-id": 502},
            {"name": "content", "type": "int", "field-id": 517},
            {"name": "sequence_number", "type": "long", "field-id": 515},
            {"name": "min_sequence_number", "type": "long", "field-id": 516},
            {"name": "added_snapshot_id", "type": "long", "field-id": 503},
            {"name": "added_files_count", "type": "int", "field-id": 504},
            {"name": "existing_files_count", "type": "int", "field-id": 505},
            {"name": "deleted_files_count", "type": "int", "field-id": 506},
            {"name": "added_rows_count", "type": "long", "field-id": 512},
            {"name": "existing_rows_count", "type": "long", "field-id": 513},
            {"name": "deleted_rows_count", "type": "long", "field-id": 514},
            {"name": "partitions", "type": ["null", {"type": "array", "items": {
              "type": "record",
              "name": "field_summary",
              "fields": [
                {"name": "contains_null", "type": "boolean", "field-id": 509},
                {"name": "contains_nan", "type": ["null", "boolean"], "default": null, "field-id": 518},
                {"name": "lower_bound", "type": ["null", "bytes"], "default": null, "field-id": 510},
                {"name": "upper_bound", "type": ["null", "bytes"], "default": null, "field-id": 511}
              ]
            }}], "default": null}
          ]
        }
        "#;
        Schema::parse_str(schema).map_err(Into::into)
    }
}

impl IcebergSchema {
    fn from_arrow(schema: &SchemaRef) -> Self {
        let fields = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, field)| IcebergSchemaField {
                id: (idx + 1) as u32,
                name: field.name().clone(),
                required: !field.is_nullable(),
                kind: map_arrow_type(field.data_type()),
            })
            .collect();

        Self {
            schema_id: 0,
            kind: "struct".to_string(),
            fields,
        }
    }
}

impl IcebergDataFile {
    fn from_segment(segment: &SegmentMeta, sort_key: &str) -> Self {
        let mut lower_bounds = HashMap::new();
        let mut upper_bounds = HashMap::new();

        if let Some(min_key) = &segment.min_key {
            lower_bounds.insert(sort_key.to_string(), min_key.clone());
        }
        if let Some(max_key) = &segment.max_key {
            upper_bounds.insert(sort_key.to_string(), max_key.clone());
        }

        Self {
            file_path: format!("{}.parquet", segment.id),
            file_format: "parquet".to_string(),
            partition: HashMap::new(),
            record_count: segment.row_count,
            file_size_in_bytes: segment.file_size_bytes,
            lower_bounds,
            upper_bounds,
        }
    }
}

impl IcebergManifest {
    pub fn new(
        manifest_path: &str,
        manifest_length: u64,
        partition_spec_id: u32,
        snapshot_id: u64,
        added_files_count: u32,
    ) -> Self {
        Self {
            manifest_path: manifest_path.to_string(),
            manifest_length,
            partition_spec_id,
            content: "data".to_string(),
            sequence_number: snapshot_id,
            snapshot_id,
            added_snapshot_id: snapshot_id,
            added_files_count,
            existing_files_count: 0,
            deleted_files_count: 0,
        }
    }
}

fn build_summary(file_count: u64) -> HashMap<String, String> {
    let mut summary = HashMap::new();
    summary.insert("operation".to_string(), "append".to_string());
    summary.insert("added-data-files".to_string(), file_count.to_string());
    summary.insert("total-records".to_string(), file_count.to_string());
    summary
}

fn map_arrow_type(data_type: &DataType) -> String {
    match data_type {
        DataType::Utf8 => "string".to_string(),
        DataType::Int64 => "long".to_string(),
        _ => "string".to_string(),
    }
}

#[derive(Serialize)]
struct ManifestEntry {
    status: i32,
    snapshot_id: Option<i64>,
    sequence_number: Option<i64>,
    file_sequence_number: Option<i64>,
    data_file: ManifestDataFile,
}

#[derive(Serialize)]
struct ManifestDataFile {
    content: i32,
    file_path: String,
    file_format: String,
    partition: ManifestPartition,
    record_count: i64,
    file_size_in_bytes: i64,
    lower_bounds: Option<HashMap<String, Vec<u8>>>,
    upper_bounds: Option<HashMap<String, Vec<u8>>>,
    key_metadata: Option<Vec<u8>>,
    split_offsets: Option<Vec<i64>>,
    equality_ids: Option<Vec<i32>>,
    sort_order_id: Option<i32>,
    first_row_id: Option<i64>,
    referenced_data_file: Option<String>,
    content_offset: Option<i64>,
    content_size_in_bytes: Option<i64>,
}

#[derive(Serialize)]
struct ManifestPartition {}

#[derive(Serialize)]
struct ManifestListEntry {
    manifest_path: String,
    manifest_length: i64,
    partition_spec_id: i32,
    content: i32,
    sequence_number: i64,
    min_sequence_number: i64,
    added_snapshot_id: i64,
    added_files_count: i32,
    existing_files_count: i32,
    deleted_files_count: i32,
    added_rows_count: i64,
    existing_rows_count: i64,
    deleted_rows_count: i64,
    partitions: Option<Vec<ManifestFieldSummary>>,
}

#[derive(Serialize)]
struct ManifestFieldSummary {
    contains_null: bool,
    contains_nan: Option<bool>,
    lower_bound: Option<Vec<u8>>,
    upper_bound: Option<Vec<u8>>,
}

impl ManifestEntry {
    fn from_segment(segment: &SegmentMeta) -> Self {
        ManifestEntry {
            status: 1,
            snapshot_id: None,
            sequence_number: None,
            file_sequence_number: None,
            data_file: ManifestDataFile {
                content: 0,
                file_path: format!("{}.parquet", segment.id),
                file_format: "parquet".to_string(),
                partition: ManifestPartition {},
                record_count: segment.row_count as i64,
                file_size_in_bytes: segment.file_size_bytes as i64,
                lower_bounds: None,
                upper_bounds: None,
                key_metadata: None,
                split_offsets: None,
                equality_ids: None,
                sort_order_id: None,
                first_row_id: None,
                referenced_data_file: None,
                content_offset: None,
                content_size_in_bytes: None,
            },
        }
    }
}

fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn generate_uuid(source: &str) -> String {
    let mut hasher = DefaultHasher::new();
    source.hash(&mut hasher);
    let hash = hasher.finish();
    format!("00000000-0000-0000-0000-{:012x}", hash & 0x0000_ffffffff_ffff)
}
