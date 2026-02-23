use parquet_lens_common::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParquetFilePath {
    pub path: PathBuf,
    pub partitions: HashMap<String, String>, // col=val pairs from Hive path segments
}

/// parse Hive-style partition segments from path components (e.g. "year=2024/month=01")
fn parse_hive_partitions(path: &Path, base: &Path) -> HashMap<String, String> {
    let mut map = HashMap::new();
    if let Ok(rel) = path.strip_prefix(base) {
        for component in rel.components() {
            let s = component.as_os_str().to_string_lossy();
            if let Some(eq) = s.find('=') {
                let key = s[..eq].to_string();
                let val = s[eq + 1..].to_string();
                if !key.is_empty() && !val.is_empty() {
                    map.insert(key, val);
                }
            }
        }
    }
    map
}

pub fn scan_directory(base: &Path) -> Result<Vec<ParquetFilePath>> {
    let mut results = Vec::new();
    scan_recursive(base, base, &mut results)?;
    Ok(results)
}

fn scan_recursive(base: &Path, dir: &Path, out: &mut Vec<ParquetFilePath>) -> Result<()> {
    let entries = std::fs::read_dir(dir)?;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            scan_recursive(base, &path, out)?;
        } else if path.extension().and_then(|e| e.to_str()) == Some("parquet") {
            let partitions = parse_hive_partitions(&path, base);
            out.push(ParquetFilePath { path, partitions });
        }
    }
    Ok(())
}

/// resolve a path string: single file, directory, glob pattern, or S3/GCS URI (async)
pub async fn resolve_paths(input: &str) -> Result<Vec<ParquetFilePath>> {
    use crate::gcs_reader::{is_gcs_uri, list_gcs_parquet};
    use crate::s3_reader::{is_s3_uri, list_s3_parquet};
    // S3 URI detection
    if is_s3_uri(input) {
        let keys = list_s3_parquet(input).await?;
        return Ok(keys
            .into_iter()
            .map(|k| ParquetFilePath {
                path: PathBuf::from(k),
                partitions: HashMap::new(),
            })
            .collect());
    }
    // GCS URI detection
    if is_gcs_uri(input) {
        let keys = list_gcs_parquet(input).await?;
        return Ok(keys
            .into_iter()
            .map(|k| ParquetFilePath {
                path: PathBuf::from(k),
                partitions: HashMap::new(),
            })
            .collect());
    }
    // local path resolution (sync ops are fine in async context)
    let path = Path::new(input);
    if path.is_file() {
        return Ok(vec![ParquetFilePath {
            path: path.to_path_buf(),
            partitions: HashMap::new(),
        }]);
    }
    if path.is_dir() {
        return scan_directory(path);
    }
    // glob pattern
    let mut results = Vec::new();
    if let Ok(entries) = glob::glob(input) {
        for entry in entries.flatten() {
            if entry.is_file() && entry.extension().and_then(|e| e.to_str()) == Some("parquet") {
                results.push(ParquetFilePath {
                    path: entry,
                    partitions: HashMap::new(),
                });
            }
        }
    }
    Ok(results)
}
