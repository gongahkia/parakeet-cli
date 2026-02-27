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
