use crate::reader::open_parquet_file;
use crate::scanner::ParquetFilePath;
use crate::schema::{extract_schema, ColumnSchema};
use parquet_lens_common::Result;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetProfile {
    pub file_count: usize,
    pub total_rows: i64,
    pub total_bytes: u64,
    pub files: Vec<FileProfile>,
    pub combined_schema: Vec<ColumnSchema>,
    pub schema_inconsistencies: Vec<String>, // per-file schema mismatches vs first file
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

    let combined_schema = if !paths.is_empty() {
        extract_schema(&paths[0].path).unwrap_or_default()
    } else {
        Vec::new()
    };

    // check schema consistency across all files vs first file
    let mut schema_inconsistencies = Vec::new();
    if paths.len() > 1 {
        let ref_col_names: std::collections::HashSet<&str> =
            combined_schema.iter().map(|c| c.name.as_str()).collect(); // O(1) lookup
        for pf in &paths[1..] {
            if let Ok(other_schema) = extract_schema(&pf.path) {
                let other_names: std::collections::HashSet<&str> =
                    other_schema.iter().map(|c| c.name.as_str()).collect(); // O(1) lookup
                for &name in &ref_col_names {
                    if !other_names.contains(name) {
                        schema_inconsistencies.push(format!(
                            "{}: missing column '{}'",
                            pf.path.display(),
                            name
                        ));
                    }
                }
                for &name in &other_names {
                    if !ref_col_names.contains(name) {
                        schema_inconsistencies.push(format!(
                            "{}: extra column '{}'",
                            pf.path.display(),
                            name
                        ));
                    }
                }
                // type mismatches
                let other_type_map: std::collections::HashMap<&str, &str> =
                    other_schema.iter().map(|c| (c.name.as_str(), c.physical_type.as_str())).collect();
                for col in &combined_schema {
                    if let Some(&other_type) = other_type_map.get(col.name.as_str()) {
                        if other_type != col.physical_type.as_str() {
                            schema_inconsistencies.push(format!(
                                "{}: column '{}' type {} vs {}",
                                pf.path.display(),
                                col.name,
                                col.physical_type,
                                other_type
                            ));
                        }
                    }
                }
            }
        }
    }

    Ok(DatasetProfile {
        file_count: files.len(),
        total_rows,
        total_bytes,
        files,
        combined_schema,
        schema_inconsistencies,
    })
}
