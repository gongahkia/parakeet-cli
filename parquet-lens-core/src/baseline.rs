use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use crate::stats::AggregatedColumnStats;
use crate::quality::QualityScore;
use crate::schema::ColumnSchema;

#[derive(Debug, Serialize, Deserialize)]
pub struct BaselineProfile {
    pub file_path: String,
    pub captured_at: u64,       // unix timestamp secs
    pub schema: Vec<ColumnSchema>,
    pub agg_stats: Vec<AggregatedColumnStats>,
    pub quality_scores: Vec<QualityScore>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BaselineRegression {
    pub column: String,
    pub kind: String,
    pub detail: String,
}

impl BaselineProfile {
    fn cache_path(file_path: &str) -> PathBuf {
        let hash = simple_hash(file_path);
        dirs::cache_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("parquet-lens")
            .join(format!("baseline_{hash:016x}.json"))
    }

    pub fn save(&self) -> anyhow::Result<()> {
        let path = Self::cache_path(&self.file_path);
        if let Some(parent) = path.parent() { std::fs::create_dir_all(parent)?; }
        std::fs::write(&path, serde_json::to_string_pretty(self)?)?;
        Ok(())
    }

    pub fn load(file_path: &str) -> Option<Self> {
        let path = Self::cache_path(file_path);
        serde_json::from_str(&std::fs::read_to_string(&path).ok()?).ok()
    }

    pub fn new(
        file_path: &str,
        schema: Vec<ColumnSchema>,
        agg_stats: Vec<AggregatedColumnStats>,
        quality_scores: Vec<QualityScore>,
    ) -> Self {
        let captured_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        Self { file_path: file_path.into(), captured_at, schema, agg_stats, quality_scores }
    }

    pub fn diff(&self, current_agg: &[AggregatedColumnStats], current_quality: &[QualityScore], current_schema: &[ColumnSchema]) -> Vec<BaselineRegression> {
        let mut regressions = Vec::new();
        // schema changes
        for col in current_schema {
            if !self.schema.iter().any(|s| s.name == col.name) {
                regressions.push(BaselineRegression { column: col.name.clone(), kind: "schema_added".into(), detail: format!("new column {} ({})", col.name, col.physical_type) });
            }
        }
        for col in &self.schema {
            if !current_schema.iter().any(|s| s.name == col.name) {
                regressions.push(BaselineRegression { column: col.name.clone(), kind: "schema_removed".into(), detail: format!("column {} removed", col.name) });
            }
        }
        // quality regressions
        for qs in current_quality {
            if let Some(base_qs) = self.quality_scores.iter().find(|b| b.column_name == qs.column_name) {
                if base_qs.score > qs.score + 5 {
                    regressions.push(BaselineRegression { column: qs.column_name.clone(), kind: "quality_drop".into(), detail: format!("score {} → {} (Δ{})", base_qs.score, qs.score, qs.score as i32 - base_qs.score as i32) });
                }
            }
        }
        // null rate increases
        for agg in current_agg {
            if let Some(base_agg) = self.agg_stats.iter().find(|b| b.column_name == agg.column_name) {
                let delta = agg.null_percentage - base_agg.null_percentage;
                if delta > 5.0 {
                    regressions.push(BaselineRegression { column: agg.column_name.clone(), kind: "null_increase".into(), detail: format!("null rate {:.1}% → {:.1}% (+{:.1}%)", base_agg.null_percentage, agg.null_percentage, delta) });
                }
            }
        }
        regressions
    }
}

fn simple_hash(s: &str) -> u64 {
    xxhash_rust::xxh3::xxh3_64(s.as_bytes())
}

/// wrapper to load baseline and produce regressions for a given file path
pub fn load_baseline_regressions(
    file_path: &Path,
    current_agg: &[AggregatedColumnStats],
    current_quality: &[QualityScore],
    current_schema: &[ColumnSchema],
) -> (Option<BaselineProfile>, Vec<BaselineRegression>) {
    let key = file_path.to_string_lossy().to_string();
    let base = BaselineProfile::load(&key);
    let regressions = base.as_ref().map(|b| b.diff(current_agg, current_quality, current_schema)).unwrap_or_default();
    (base, regressions)
}
