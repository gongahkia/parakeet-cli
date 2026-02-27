use crate::stats::{AggregatedColumnStats, EncodingAnalysis, RowGroupProfile};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct RepairSuggestion {
    pub issue: String,
    pub severity: String, // "high", "medium", "low"
    pub recommendation: String,
}

pub fn detect_repair_suggestions(
    row_groups: &[RowGroupProfile],
    agg_stats: &[AggregatedColumnStats],
    encodings: &[EncodingAnalysis],
) -> Vec<RepairSuggestion> {
    let rg_count = row_groups.len();
    if rg_count == 0 {
        return Vec::new(); // guard: no row groups, avoid integer division
    }
    let mut suggestions = Vec::new();
    if rg_count > 100 {
        let avg_bytes = row_groups
            .iter()
            .map(|r| r.total_byte_size as f64)
            .sum::<f64>()
            / rg_count as f64;
        if avg_bytes < 64.0 * 1024.0 * 1024.0 {
            suggestions.push(RepairSuggestion {
                issue: format!(
                    "{rg_count} row groups, avg {:.1}MB — too fragmented",
                    avg_bytes / 1048576.0
                ),
                severity: "high".into(),
                recommendation: "Compact into fewer, larger row groups (target 128-256MB each)"
                    .into(),
            });
        }
    }
    if rg_count > 0 {
        for agg in agg_stats {
            let dict_used = encodings
                .iter()
                .find(|e| e.column_name == agg.column_name)
                .map(|e| {
                    e.encodings.iter().any(|enc| {
                        enc.contains("RLE_DICTIONARY") || enc.contains("PLAIN_DICTIONARY")
                    })
                })
                .unwrap_or(false);
            let avg_page = agg.total_data_page_size / rg_count as i64;
            if avg_page > 1024 * 1024 && dict_used {
                suggestions.push(RepairSuggestion {
                    issue: format!(
                        "column '{}' dict page avg {:.1}MB",
                        agg.column_name,
                        avg_page as f64 / 1048576.0
                    ),
                    severity: "medium".into(),
                    recommendation: format!(
                        "Disable dictionary encoding for '{}' — dict page too large",
                        agg.column_name
                    ),
                });
            }
        }
    }
    for agg in agg_stats {
        if agg.null_percentage > 50.0 {
            suggestions.push(RepairSuggestion {
                issue: format!(
                    "column '{}' is {:.1}% null",
                    agg.column_name, agg.null_percentage
                ),
                severity: "low".into(),
                recommendation: format!(
                    "Consider dropping column '{}' or replacing with sparse representation",
                    agg.column_name
                ),
            });
        }
    }
    suggestions
}

#[cfg(test)]
mod tests_detect_repair_suggestions {
    use super::*;
    fn rg(byte_size: i64) -> RowGroupProfile {
        RowGroupProfile { index: 0, num_rows: 1000, total_byte_size: byte_size, compressed_size: byte_size, compression_ratio: 1.0, column_offsets: vec![], column_sizes: vec![] }
    }
    fn agg(name: &str, null_pct: f64, page_size: i64) -> AggregatedColumnStats {
        AggregatedColumnStats { column_name: name.into(), total_null_count: 0, null_percentage: null_pct, total_distinct_count_estimate: None, total_data_page_size: page_size, total_compressed_size: page_size, compression_ratio: 1.0, min_bytes: None, max_bytes: None }
    }
    fn enc(name: &str, encodings: Vec<&str>) -> EncodingAnalysis {
        EncodingAnalysis { column_name: name.into(), encodings: encodings.iter().map(|s| s.to_string()).collect(), is_plain_only: false }
    }
    #[test] fn zero_row_groups_returns_empty() {
        assert!(detect_repair_suggestions(&[], &[], &[]).is_empty());
    }
    #[test] fn fragmentation_trigger() {
        let rgs: Vec<RowGroupProfile> = (0..101).map(|_| rg(1024 * 1024)).collect(); // 1MB each, avg < 64MB
        let result = detect_repair_suggestions(&rgs, &[], &[]);
        assert!(result.iter().any(|s| s.severity == "high" && s.issue.contains("row groups")));
    }
    #[test] fn no_fragmentation_below_threshold() {
        let rgs: Vec<RowGroupProfile> = (0..50).map(|_| rg(128 * 1024 * 1024)).collect(); // 50 rgs, avg 128MB
        assert!(detect_repair_suggestions(&rgs, &[], &[]).is_empty());
    }
    #[test] fn high_null_column_suggestion() {
        let result = detect_repair_suggestions(&[rg(1)], &[agg("col_a", 75.0, 0)], &[]);
        assert!(result.iter().any(|s| s.severity == "low" && s.issue.contains("col_a")));
    }
    #[test] fn large_dict_page_suggestion() {
        let a = agg("col_b", 0.0, 2 * 1024 * 1024); // 2MB page size for 1 row group => avg > 1MB
        let e = enc("col_b", vec!["RLE_DICTIONARY"]);
        let result = detect_repair_suggestions(&[rg(1)], &[a], &[e]);
        assert!(result.iter().any(|s| s.severity == "medium" && s.issue.contains("col_b")));
    }
}
