use crate::parallel_reader::DatasetProfile;
use crate::schema::ColumnSchema;
use crate::stats::AggregatedColumnStats;
use serde::{Deserialize, Serialize};

// --- Task 47: schema diff ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnSchemaDiff {
    pub name: String,
    pub status: DiffStatus,
    pub left_type: Option<String>,
    pub right_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DiffStatus {
    Added,
    Removed,
    TypeChanged,
    Matching,
}

pub fn diff_schemas(left: &[ColumnSchema], right: &[ColumnSchema]) -> Vec<ColumnSchemaDiff> {
    use std::collections::HashMap;
    let lmap: HashMap<&str, &ColumnSchema> = left.iter().map(|c| (c.name.as_str(), c)).collect();
    let rmap: HashMap<&str, &ColumnSchema> = right.iter().map(|c| (c.name.as_str(), c)).collect();
    let mut diffs = Vec::new();
    for (name, lc) in &lmap {
        if let Some(rc) = rmap.get(name) {
            let status =
                if lc.physical_type != rc.physical_type || lc.logical_type != rc.logical_type {
                    DiffStatus::TypeChanged
                } else {
                    DiffStatus::Matching
                };
            diffs.push(ColumnSchemaDiff {
                name: name.to_string(),
                status,
                left_type: Some(format!(
                    "{}/{}",
                    lc.physical_type,
                    lc.logical_type.as_deref().unwrap_or("-")
                )),
                right_type: Some(format!(
                    "{}/{}",
                    rc.physical_type,
                    rc.logical_type.as_deref().unwrap_or("-")
                )),
            });
        } else {
            diffs.push(ColumnSchemaDiff {
                name: name.to_string(),
                status: DiffStatus::Removed,
                left_type: Some(lc.physical_type.clone()),
                right_type: None,
            });
        }
    }
    for (name, rc) in &rmap {
        if !lmap.contains_key(name) {
            diffs.push(ColumnSchemaDiff {
                name: name.to_string(),
                status: DiffStatus::Added,
                left_type: None,
                right_type: Some(rc.physical_type.clone()),
            });
        }
    }
    diffs.sort_by(|a, b| a.name.cmp(&b.name));
    diffs
}

// --- Task 48: stats diff ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStatsDiff {
    pub name: String,
    pub null_rate_delta: f64,
    pub null_rate_significant: bool,
    pub cardinality_delta: Option<i64>,
    pub size_delta_bytes: i64,
}

pub fn diff_stats(
    left: &[AggregatedColumnStats],
    right: &[AggregatedColumnStats],
) -> Vec<ColumnStatsDiff> {
    use std::collections::HashMap;
    let rmap: HashMap<&str, &AggregatedColumnStats> =
        right.iter().map(|s| (s.column_name.as_str(), s)).collect();
    let mut diffs = Vec::new();
    for ls in left {
        if let Some(rs) = rmap.get(ls.column_name.as_str()) {
            let null_delta = rs.null_percentage - ls.null_percentage;
            let null_sig = null_delta.abs() > (ls.null_percentage * 0.10).max(1.0); // >10% relative
            let card_delta = match (
                ls.total_distinct_count_estimate,
                rs.total_distinct_count_estimate,
            ) {
                (Some(l), Some(r)) => Some(r as i64 - l as i64),
                _ => None,
            };
            diffs.push(ColumnStatsDiff {
                name: ls.column_name.clone(),
                null_rate_delta: null_delta,
                null_rate_significant: null_sig,
                cardinality_delta: card_delta,
                size_delta_bytes: rs.total_data_page_size - ls.total_data_page_size,
            });
        }
    }
    diffs
}

// --- Task 49: row count and size comparison summary ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetComparison {
    pub left_rows: i64,
    pub right_rows: i64,
    pub row_delta: i64,
    pub row_delta_pct: f64,
    pub left_files: usize,
    pub right_files: usize,
    pub left_bytes: u64,
    pub right_bytes: u64,
    pub size_delta_bytes: i64,
    pub left_columns: usize,
    pub right_columns: usize,
    pub schema_diffs: Vec<ColumnSchemaDiff>,
    pub stats_diffs: Vec<ColumnStatsDiff>,
}

pub fn compare_datasets(
    left: &DatasetProfile,
    right: &DatasetProfile,
    left_stats: &[AggregatedColumnStats],
    right_stats: &[AggregatedColumnStats],
) -> DatasetComparison {
    let row_delta = right.total_rows - left.total_rows;
    let row_delta_pct = if left.total_rows > 0 {
        row_delta as f64 / left.total_rows as f64 * 100.0
    } else {
        0.0
    };
    let schema_diffs = diff_schemas(&left.combined_schema, &right.combined_schema);
    let stats_diffs = diff_stats(left_stats, right_stats);
    DatasetComparison {
        left_rows: left.total_rows,
        right_rows: right.total_rows,
        row_delta,
        row_delta_pct,
        left_files: left.file_count,
        right_files: right.file_count,
        left_bytes: left.total_bytes,
        right_bytes: right.total_bytes,
        size_delta_bytes: right.total_bytes as i64 - left.total_bytes as i64,
        left_columns: left.combined_schema.len(),
        right_columns: right.combined_schema.len(),
        schema_diffs,
        stats_diffs,
    }
}
