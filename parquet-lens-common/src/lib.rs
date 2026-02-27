pub mod config;
pub use config::{Config, GcsConfig};

use thiserror::Error;

#[derive(Error, Debug)]
pub enum ParquetLensError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("Authentication error: {0}")]
    Auth(String),
    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, ParquetLensError>;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FileInfo {
    pub path: String,
    pub row_count: i64,
    pub row_group_count: usize,
}
