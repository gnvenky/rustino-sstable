// src/sstable.rs
//
// SSTable
// =======
// The top-level abstraction: a named, versioned collection of segments.
//
// Manages:
//   - A JSON manifest listing all live segments
//   - Append (write new L0 segment)
//   - Scan (fan out across segments, prune via bloom/range)
//   - Compact (merge L0 segments into L1)
//   - Inspect (diagnostic output — the "low bar" CLI entry point)

use crate::error::{Result, SstError};
use crate::iceberg::{IcebergManifest, IcebergTableMetadata};
use crate::merge::Compactor;
use crate::reader::SegmentReader;
use crate::types::{Manifest, SegmentMeta, SegmentRef};
use crate::writer::SegmentWriter;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use std::path::PathBuf;

const MANIFEST_FILE: &str = "manifest.json";

pub struct SSTable {
    /// Root directory for this table
    dir: PathBuf,

    /// Table name
    pub name: String,

    /// Arrow schema for all segments
    schema: SchemaRef,

    /// Which column to sort by
    sort_key: String,

    /// Current manifest (in-memory, flushed to disk on every change)
    manifest: Manifest,
}

impl SSTable {
    /// Open an existing SSTable or create a new one.
    pub fn open_or_create(
        dir: impl Into<PathBuf>,
        name: impl Into<String>,
        schema: SchemaRef,
        sort_key: impl Into<String>,
    ) -> Result<Self> {
        let dir = dir.into();
        let name = name.into();
        let sort_key = sort_key.into();

        std::fs::create_dir_all(&dir)?;

        let manifest_path = dir.join(MANIFEST_FILE);
        let manifest = if manifest_path.exists() {
            let json = std::fs::read_to_string(&manifest_path)?;
            serde_json::from_str(&json)?
        } else {
            Manifest {
                name: name.clone(),
                segments: vec![],
                version: 0,
            }
        };

        Ok(Self {
            dir,
            name,
            schema,
            sort_key,
            manifest,
        })
    }

    /// Append a RecordBatch — creates a new L0 segment.
    pub fn append(&mut self, batch: &RecordBatch) -> Result<SegmentMeta> {
        let writer = SegmentWriter::new(&self.dir, &self.sort_key, self.schema.clone());
        let seg_ref = writer.write(batch)?;

        println!(
            "[sstable] appended segment {} — {} rows, L{}",
            seg_ref.meta.id, seg_ref.meta.row_count, seg_ref.meta.level
        );

        self.manifest.segments.push(seg_ref.meta.clone());
        self.manifest.version += 1;
        self.flush_manifest()?;
        self.export_to_iceberg()?;

        Ok(seg_ref.meta)
    }

    /// Scan all segments, returning all matching RecordBatches.
    ///
    /// Pass a key hint to enable range + bloom pruning.
    /// Pass None to do a full table scan.
    pub fn scan(&self, key_hint: Option<&str>, projection: Option<Vec<String>>) -> Result<Vec<RecordBatch>> {
        let mut results = Vec::new();
        let mut segments_scanned = 0;
        let mut segments_skipped = 0;

        for meta in &self.manifest.segments {
            let seg_ref = self.resolve_segment(meta)?;
            let reader = SegmentReader::new(seg_ref);

            // Prune using key range if a hint is provided
            if let Some(key) = key_hint {
                if !reader.might_contain(key) {
                    segments_skipped += 1;
                    continue;
                }
            }

            segments_scanned += 1;
            let batches = reader.read_all(projection.clone())?;
            results.extend(batches);
        }

        println!(
            "[sstable] scan complete — {} segments read, {} skipped by pruning",
            segments_scanned, segments_skipped
        );

        Ok(results)
    }

    /// Compact all L0 segments into a single L1 segment.
    ///
    /// After compaction, L0 segments are deleted and the manifest is updated.
    pub fn compact_l0(&mut self) -> Result<Option<SegmentMeta>> {
        let l0_metas: Vec<SegmentMeta> = self
            .manifest
            .segments
            .iter()
            .filter(|s| s.level == 0)
            .cloned()
            .collect();

        if l0_metas.is_empty() {
            println!("[sstable] nothing to compact");
            return Ok(None);
        }

        println!(
            "[sstable] compacting {} L0 segments...",
            l0_metas.len()
        );

        let l0_refs: Vec<SegmentRef> = l0_metas
            .iter()
            .map(|m| self.resolve_segment(m))
            .collect::<Result<_>>()?;

        let writer = SegmentWriter::new(&self.dir, &self.sort_key, self.schema.clone());
        let compactor = Compactor::new(writer);
        let new_seg = compactor.compact(l0_refs.clone(), &self.sort_key)?;

        // Update manifest: remove L0 entries, add L1
        let l0_ids: std::collections::HashSet<String> =
            l0_metas.iter().map(|m| m.id.clone()).collect();

        self.manifest.segments.retain(|s| !l0_ids.contains(&s.id));
        self.manifest.segments.push(new_seg.meta.clone());
        self.manifest.version += 1;
        self.flush_manifest()?;
        self.export_to_iceberg()?;

        // Delete old L0 files
        for seg_ref in &l0_refs {
            let _ = std::fs::remove_file(&seg_ref.parquet_path);
            let _ = std::fs::remove_file(&seg_ref.meta_path);
        }

        println!(
            "[sstable] compaction done — new L1 segment: {} ({} rows)",
            new_seg.meta.id, new_seg.meta.row_count
        );

        Ok(Some(new_seg.meta))
    }

    /// Print a human-readable diagnostic summary of this table.
    /// This is the "iceberg diagnostics" equivalent — low bar entry point.
    pub fn inspect(&self) {
        println!("╔══════════════════════════════════════════════╗");
        println!("║  SSTable: {:<35} ║", self.name);
        println!("╠══════════════════════════════════════════════╣");
        println!("║  Sort key : {:<33} ║", self.sort_key);
        println!("║  Manifest version: {:<25} ║", self.manifest.version);
        println!("║  Segments: {:<34} ║", self.manifest.segments.len());
        println!("╠══════════════════════════════════════════════╣");

        let total_rows: u64 = self.manifest.segments.iter().map(|s| s.row_count).sum();
        let total_bytes: u64 = self.manifest.segments.iter().map(|s| s.file_size_bytes).sum();

        for seg in &self.manifest.segments {
            let display_id = seg.id.get(..32).unwrap_or(&seg.id);
            println!("║                                              ║");
            println!("║  [L{}] {:<32} ║", seg.level, display_id);
            println!(
                "║      rows: {:>10}  size: {:>8} KB     ║",
                seg.row_count,
                seg.file_size_bytes / 1024
            );
            println!(
                "║      key range: {:>10} → {:<10}     ║",
                seg.min_key.as_deref().unwrap_or("?"),
                seg.max_key.as_deref().unwrap_or("?")
            );
        }

        println!("╠══════════════════════════════════════════════╣");
        println!(
            "║  TOTAL  rows: {:>10}  size: {:>8} KB  ║",
            total_rows,
            total_bytes / 1024
        );
        println!("╚══════════════════════════════════════════════╝");
    }

    // ── Private helpers ─────────────────────────────────────────────────

    fn flush_manifest(&self) -> Result<()> {
        let path = self.dir.join(MANIFEST_FILE);
        let json = serde_json::to_string_pretty(&self.manifest)?;
        std::fs::write(path, json)?;
        Ok(())
    }

    pub fn export_to_iceberg(&self) -> Result<()> {
        let (manifest_file_name, manifest_file_length) =
            IcebergTableMetadata::write_manifest_avro(
                &self.dir,
                self.manifest.version,
                &self.manifest.segments,
                &self.sort_key,
                self.schema.clone(),
            )?;

        let added_rows_count: u64 = self.manifest.segments.iter().map(|s| s.row_count).sum();
        let (manifest_list_name, _manifest_list_length) =
            IcebergTableMetadata::write_manifest_list_avro(
                &self.dir,
                self.manifest.version,
                &manifest_file_name,
                manifest_file_length,
                self.manifest.segments.len() as u32,
                added_rows_count,
            )?;

        let metadata = IcebergTableMetadata::build_metadata(
            &self.name,
            &self.dir.to_string_lossy(),
            self.schema.clone(),
            &self.manifest.segments,
            self.manifest.version,
            &manifest_list_name,
            vec![],
        );

        std::fs::write(
            self.dir.join("metadata.json"),
            serde_json::to_string_pretty(&metadata)?,
        )?;

        Ok(())
    }

    fn resolve_segment(&self, meta: &SegmentMeta) -> Result<SegmentRef> {
        let parquet_path = self.dir.join(format!("{}.parquet", meta.id));
        let meta_path = self.dir.join(format!("{}.meta", meta.id));

        if !parquet_path.exists() {
            return Err(SstError::SegmentNotFound(meta.id.clone()));
        }

        Ok(SegmentRef {
            meta: meta.clone(),
            parquet_path,
            meta_path,
        })
    }
}
