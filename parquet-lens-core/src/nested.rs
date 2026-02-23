use bytes::Bytes;
use memmap2::Mmap;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet_lens_common::{ParquetLensError, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Serialize, Deserialize)]
pub struct NestedColumnProfile {
    pub column_name: String,
    pub nesting_depth: usize,
    pub physical_type: String,
    pub max_def_level: i16,
    pub max_rep_level: i16,
    pub is_list: bool,
    pub is_map: bool,
    pub is_struct: bool,
}

pub fn profile_nested_columns(path: &Path) -> Result<Vec<NestedColumnProfile>> {
    let file = std::fs::File::open(path)?;
    let mmap: Mmap = unsafe { Mmap::map(&file)? };
    let bytes = Bytes::copy_from_slice(&mmap);
    let reader = SerializedFileReader::new(bytes).map_err(ParquetLensError::Parquet)?;
    let meta = reader.metadata();
    let schema = meta.file_metadata().schema_descr();
    let mut profiles = Vec::new();
    for i in 0..schema.num_columns() {
        let col = schema.column(i);
        let path_str = col.path().string(); // dot-separated path
        let depth = path_str.chars().filter(|&c| c == '.').count(); // dots = depth
        if depth == 0 {
            continue;
        } // flat column, skip
        let path_lower = path_str.to_lowercase();
        let is_list = path_lower.contains(".list.")
            || path_lower.ends_with(".list")
            || path_lower.contains(".element");
        let is_map = path_lower.contains("key_value")
            || path_lower.contains(".key")
            || path_lower.contains(".value") && !is_list;
        let is_struct = !is_list && !is_map;
        profiles.push(NestedColumnProfile {
            column_name: col.name().to_owned(),
            nesting_depth: depth,
            physical_type: format!("{:?}", col.physical_type()),
            max_def_level: col.max_def_level(),
            max_rep_level: col.max_rep_level(),
            is_list,
            is_map,
            is_struct,
        });
    }
    Ok(profiles)
}
