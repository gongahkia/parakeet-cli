use bytes::Bytes;
use memmap2::Mmap;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet_lens_common::{ParquetLensError, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParquetFileInfo {
    pub path: PathBuf,
    pub file_size: u64,
    pub row_count: i64,
    pub row_group_count: usize,
    pub created_by: Option<String>,
    pub parquet_version: i32,
    pub key_value_metadata: Vec<(String, Option<String>)>,
    pub schema_fields: Vec<SchemaFieldInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaFieldInfo {
    pub name: String,
    pub physical_type: String,
    pub logical_type: Option<String>,
    pub repetition: String,
    pub max_def_level: i16,
    pub max_rep_level: i16,
}

pub fn open_parquet_file(path: &Path) -> Result<(ParquetFileInfo, ParquetMetaData)> {
    let file = std::fs::File::open(path)?;
    let file_size = file.metadata()?.len();
    // memory-map the file for zero-copy footer access
    let mmap: Mmap = unsafe { Mmap::map(&file)? };
    let bytes = Bytes::copy_from_slice(&mmap);
    let reader = SerializedFileReader::new(bytes).map_err(ParquetLensError::Parquet)?;
    let meta = reader.metadata().clone();
    let file_meta = meta.file_metadata();
    let created_by = file_meta.created_by().map(|s| s.to_owned());
    let parquet_version = file_meta.version();
    let kv_meta = file_meta
        .key_value_metadata()
        .map(|kv| {
            kv.iter()
                .map(|k| (k.key.clone(), k.value.clone()))
                .collect()
        })
        .unwrap_or_default();
    let schema = file_meta.schema_descr();
    let schema_fields: Vec<SchemaFieldInfo> = (0..schema.num_columns())
        .map(|i| {
            let col = schema.column(i);
            let basic = col.self_type().get_basic_info();
            SchemaFieldInfo {
                name: col.name().to_owned(),
                physical_type: format!("{:?}", col.physical_type()),
                logical_type: col.logical_type().map(|lt| format!("{lt:?}")),
                repetition: if basic.has_repetition() {
                    format!("{:?}", basic.repetition())
                } else {
                    "REQUIRED".into()
                },
                max_def_level: col.max_def_level(),
                max_rep_level: col.max_rep_level(),
            }
        })
        .collect();
    let row_count: i64 = (0..meta.num_row_groups())
        .map(|i| meta.row_group(i).num_rows())
        .sum();
    let info = ParquetFileInfo {
        path: path.to_path_buf(),
        file_size,
        row_count,
        row_group_count: meta.num_row_groups(),
        created_by,
        parquet_version,
        key_value_metadata: kv_meta,
        schema_fields,
    };
    Ok((info, meta))
}

/// unified async opener: dispatches to S3, GCS, or local reader based on URI prefix
pub async fn open_parquet_auto(
    path: &str,
    s3_endpoint: Option<&str>,
) -> Result<(ParquetFileInfo, ParquetMetaData)> {
    if crate::s3_reader::is_s3_uri(path) {
        let meta = crate::s3_reader::read_s3_parquet_metadata(path, s3_endpoint).await?;
        let fi = ParquetFileInfo {
            path: PathBuf::from(path),
            file_size: 0,
            row_count: meta.file_metadata().num_rows(),
            row_group_count: meta.num_row_groups(),
            created_by: meta.file_metadata().created_by().map(|s| s.to_owned()),
            parquet_version: meta.file_metadata().version(),
            key_value_metadata: Vec::new(),
            schema_fields: Vec::new(),
        };
        Ok((fi, meta))
    } else if crate::gcs_reader::is_gcs_uri(path) {
        let meta = crate::gcs_reader::read_gcs_parquet_metadata(path).await?;
        let fi = ParquetFileInfo {
            path: PathBuf::from(path),
            file_size: 0,
            row_count: meta.file_metadata().num_rows(),
            row_group_count: meta.num_row_groups(),
            created_by: meta.file_metadata().created_by().map(|s| s.to_owned()),
            parquet_version: meta.file_metadata().version(),
            key_value_metadata: Vec::new(),
            schema_fields: Vec::new(),
        };
        Ok((fi, meta))
    } else {
        open_parquet_file(Path::new(path))
    }
}
