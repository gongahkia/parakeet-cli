use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use parquet_lens_common::{ParquetLensError, Result};
use crate::reader::{open_parquet_file, SchemaFieldInfo};
use crate::scanner::ParquetFilePath;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetProfile {
    pub file_count: usize,
    pub total_rows: i64,
    pub total_bytes: u64,
    pub files: Vec<FileProfile>,
    pub combined_schema: Vec<SchemaFieldInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileProfile {
    pub path: PathBuf,
    pub row_count: i64,
    pub row_group_count: usize,
    pub file_size: u64,
    pub created_by: Option<String>,
}

pub fn read_metadata_parallel(paths: &[ParquetFilePath]) -> Result<DatasetProfile> {
    let results: Vec<Result<FileProfile>> = paths
        .par_iter()
        .map(|pf| {
            let (info, _meta) = open_parquet_file(&pf.path)?;
            Ok(FileProfile {
                path: info.path,
                row_count: info.row_count,
                row_group_count: info.row_group_count,
                file_size: info.file_size,
                created_by: info.created_by,
            })
        })
        .collect();

    let mut files = Vec::with_capacity(results.len());
    let mut errors = Vec::new();
    for r in results {
        match r {
            Ok(fp) => files.push(fp),
            Err(e) => errors.push(e),
        }
    }
    if files.is_empty() && !errors.is_empty() {
        return Err(errors.remove(0));
    }

    let total_rows = files.iter().map(|f| f.row_count).sum();
    let total_bytes = files.iter().map(|f| f.file_size).sum();

    // build combined schema from first file
    let combined_schema = if !paths.is_empty() {
        open_parquet_file(&paths[0].path).map(|(info, _)| info.schema_fields).unwrap_or_default()
    } else {
        Vec::new()
    };

    Ok(DatasetProfile {
        file_count: files.len(),
        total_rows,
        total_bytes,
        files,
        combined_schema,
    })
}
