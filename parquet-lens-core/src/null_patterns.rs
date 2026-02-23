use serde::{Deserialize, Serialize};
use crate::stats::AggregatedColumnStats;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NullPatternGroup {
    pub columns: Vec<String>,
    pub null_percentage: f64,   // shared approximate null rate
    pub pattern_type: String,   // "always_null", "never_null", "correlated_nulls"
}

/// group columns by similar null rate (within 2% tolerance)
pub fn analyze_null_patterns(agg_stats: &[AggregatedColumnStats]) -> Vec<NullPatternGroup> {
    let mut groups: Vec<NullPatternGroup> = Vec::new();
    // always-null columns (>99%)
    let always_null: Vec<String> = agg_stats.iter()
        .filter(|s| s.null_percentage > 99.0)
        .map(|s| s.column_name.clone())
        .collect();
    if !always_null.is_empty() {
        groups.push(NullPatternGroup { columns: always_null, null_percentage: 100.0, pattern_type: "always_null".into() });
    }
    // never-null columns (0%)
    let never_null: Vec<String> = agg_stats.iter()
        .filter(|s| s.null_percentage == 0.0)
        .map(|s| s.column_name.clone())
        .collect();
    if never_null.len() > 1 {
        groups.push(NullPatternGroup { columns: never_null, null_percentage: 0.0, pattern_type: "never_null".into() });
    }
    // correlated-null groups: same null rate within 2%, >1% null, not in always_null
    let candidates: Vec<&AggregatedColumnStats> = agg_stats.iter()
        .filter(|s| s.null_percentage > 1.0 && s.null_percentage <= 99.0)
        .collect();
    let mut used = vec![false; candidates.len()];
    for i in 0..candidates.len() {
        if used[i] { continue; }
        let mut group_cols = vec![candidates[i].column_name.clone()];
        used[i] = true;
        for j in (i+1)..candidates.len() {
            if used[j] { continue; }
            if (candidates[i].null_percentage - candidates[j].null_percentage).abs() < 2.0 {
                group_cols.push(candidates[j].column_name.clone());
                used[j] = true;
            }
        }
        if group_cols.len() > 1 {
            groups.push(NullPatternGroup {
                columns: group_cols,
                null_percentage: candidates[i].null_percentage,
                pattern_type: "correlated_nulls".into(),
            });
        }
    }
    groups
}
