use bytes::Bytes;
use memmap2::Mmap;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet_lens_common::{ParquetLensError, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ColumnSchema {
    pub name: String,
    pub physical_type: String,
    pub logical_type: Option<String>,
    pub repetition: String,
    pub max_def_level: i16,
    pub max_rep_level: i16,
}

pub fn extract_schema(path: &Path) -> Result<Vec<ColumnSchema>> {
    let file = std::fs::File::open(path)?;
    let mmap: Mmap = unsafe { Mmap::map(&file)? };
    let bytes = Bytes::copy_from_slice(&mmap);
    let reader = SerializedFileReader::new(bytes).map_err(ParquetLensError::Parquet)?;
    let meta = reader.metadata();
    let schema = meta.file_metadata().schema_descr();
    let columns = (0..schema.num_columns())
        .map(|i| {
            let col = schema.column(i);
            let basic = col.self_type().get_basic_info();
            ColumnSchema {
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
    Ok(columns)
}
