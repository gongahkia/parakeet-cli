use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DisplayConfig {
    #[serde(default = "default_theme")]
    pub theme: String,
    #[serde(default = "default_max_rows")]
    pub max_rows_preview: usize,
    #[serde(default)]
    pub sidebar_width: Option<u16>, // falls back to 30 when None
}

fn default_theme() -> String {
    "dark".into()
}
fn default_max_rows() -> usize {
    100
}

impl Default for DisplayConfig {
    fn default() -> Self {
        Self {
            theme: default_theme(),
            max_rows_preview: default_max_rows(),
            sidebar_width: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfilingConfig {
    #[serde(default = "default_mode")]
    pub default_mode: String,
    #[serde(default = "default_sample")]
    pub sample_size: usize,
    #[serde(default = "default_bins")]
    pub histogram_bins: usize,
    #[serde(default = "default_large_file_threshold")]
    pub large_file_threshold_bytes: u64,
    #[serde(default)]
    pub full_scan_timeout_secs: Option<u64>,
}

fn default_mode() -> String {
    "metadata".into()
}
fn default_sample() -> usize {
    10000
}
fn default_bins() -> usize {
    30
}
fn default_large_file_threshold() -> u64 {
    1073741824 // 1GiB
}

impl Default for ProfilingConfig {
    fn default() -> Self {
        Self {
            default_mode: default_mode(),
            sample_size: default_sample(),
            histogram_bins: default_bins(),
            large_file_threshold_bytes: default_large_file_threshold(),
            full_scan_timeout_secs: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct S3Config {
    pub region: Option<String>,
    pub profile: Option<String>,
    pub endpoint_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportConfig {
    #[serde(default = "default_format")]
    pub format: String,
    #[serde(default = "default_output_dir")]
    pub output_dir: String,
}

fn default_format() -> String {
    "json".into()
}
fn default_output_dir() -> String {
    ".".into()
}

impl Default for ExportConfig {
    fn default() -> Self {
        Self {
            format: default_format(),
            output_dir: default_output_dir(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GcsConfig {
    pub project_id: Option<String>,
    pub credentials_file: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Config {
    #[serde(default)]
    pub display: DisplayConfig,
    #[serde(default)]
    pub profiling: ProfilingConfig,
    #[serde(default)]
    pub s3: S3Config,
    #[serde(default)]
    pub export: ExportConfig,
    #[serde(default)]
    pub gcs: GcsConfig,
}

impl Config {
    pub fn config_path() -> PathBuf {
        dirs::config_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("parquet-lens")
            .join("config.toml")
    }

    pub fn load() -> crate::Result<Self> {
        let path = if let Ok(env_path) = std::env::var("PARQUET_LENS_CONFIG") {
            PathBuf::from(env_path) // $PARQUET_LENS_CONFIG overrides default config path
        } else {
            Self::config_path()
        };
        if !path.exists() {
            return Ok(Self::default());
        }
        let content = std::fs::read_to_string(&path)?;
        let cfg: Self =
            toml::from_str(&content).map_err(|e| crate::ParquetLensError::Other(e.to_string()))?;
        Ok(cfg)
    }

    pub fn save(&self) -> crate::Result<()> {
        let path = Self::config_path();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let content = toml::to_string_pretty(self)
            .map_err(|e| crate::ParquetLensError::Other(e.to_string()))?;
        std::fs::write(&path, content)?;
        Ok(())
    }
}
