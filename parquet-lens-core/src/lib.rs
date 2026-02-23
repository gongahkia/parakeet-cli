pub mod reader;
pub mod scanner;
pub mod parallel_reader;
pub mod schema;
pub use parquet_lens_common::{ParquetLensError, Result};
pub use reader::{ParquetFileInfo, SchemaFieldInfo, open_parquet_file};
pub use scanner::{ParquetFilePath, resolve_paths, scan_directory};
pub use parallel_reader::{DatasetProfile, FileProfile, read_metadata_parallel};
pub use schema::{ColumnSchema, extract_schema};
