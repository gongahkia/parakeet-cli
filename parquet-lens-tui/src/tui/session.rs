use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Serialize, Deserialize)]
pub struct Session {
    pub input_path: String,
    pub sidebar_selected: usize,
    pub view: String,        // serialized view name
    pub profiling_mode: String,
    #[serde(default)]
    pub bookmarks: Vec<String>,
    #[serde(default)]
    pub sidebar_sort: String,
    #[serde(default = "default_true")]
    pub sidebar_sort_asc: bool,
    #[serde(default)]
    pub show_bookmarks_only: bool,
}

fn default_true() -> bool { true }

impl Session {
    pub fn cache_path() -> PathBuf {
        dirs::cache_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("parquet-lens")
            .join("session.json")
    }
    pub fn save(&self) -> anyhow::Result<()> {
        let path = Self::cache_path();
        if let Some(parent) = path.parent() { std::fs::create_dir_all(parent)?; }
        std::fs::write(&path, serde_json::to_string_pretty(self)?)?;
        Ok(())
    }
    pub fn load() -> Option<Self> {
        let path = Self::cache_path();
        serde_json::from_str(&std::fs::read_to_string(&path).ok()?).ok()
    }
}
