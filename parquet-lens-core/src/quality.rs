use crate::stats::AggregatedColumnStats;
use arrow::array::Array;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet_lens_common::{ParquetLensError, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;

// task 23: per-column quality score
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualityScore {
    pub column_name: String,
    pub score: u8, // 0-100
    pub null_penalty: f64,
    pub is_constant: bool,
    pub cardinality_flag: bool,
    pub is_plain_only_encoding: bool,
    pub breakdown: String,
}

pub fn score_column(
    column_name: &str,
    null_percentage: f64,
    distinct_count: Option<u64>,
    total_rows: i64,
    is_plain_only: bool,
) -> QualityScore {
    let mut score: f64 = 100.0;
    let mut notes = Vec::new();
    // null penalty: each 1% above 5% costs 2 points
    let null_penalty = if null_percentage > 5.0 {
        (null_percentage - 5.0) * 2.0
    } else {
        0.0
    };
    score -= null_penalty.min(60.0);
    if null_penalty > 0.0 {
        notes.push(format!("null_rate={null_percentage:.1}%"));
    }
    // constant column
    let is_constant = distinct_count.is_some_and(|d| d <= 1);
    if is_constant {
        score -= 20.0;
        notes.push("constant_column".into());
    }
    // high cardinality (= row count, likely an ID or raw event column)
    let cardinality_flag = distinct_count.is_some_and(|d| total_rows > 0 && d as i64 == total_rows);
    if cardinality_flag {
        score -= 5.0;
        notes.push("cardinality=row_count".into());
    }
    // plain-only encoding
    if is_plain_only {
        score -= 5.0;
        notes.push("plain_only_encoding".into());
    }
    QualityScore {
        column_name: column_name.to_owned(),
        score: score.max(0.0).round() as u8,
        null_penalty,
        is_constant,
        cardinality_flag,
        is_plain_only_encoding: is_plain_only,
        breakdown: notes.join(", "),
    }
}

// task 24: dataset-level quality summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetQuality {
    pub overall_score: u8,
    pub total_null_cell_pct: f64,
    pub worst_columns: Vec<String>,
    pub schema_consistent: bool,
    pub column_scores: Vec<QualityScore>,
}

pub fn summarize_quality(
    scores: Vec<QualityScore>,
    total_cells: i64,
    total_nulls: u64,
    schema_consistent: bool,
    agg_stats: &[AggregatedColumnStats],
) -> DatasetQuality {
    let overall_score = if scores.is_empty() {
        100
    } else {
        // weighted mean by total_data_page_size; fall back to arithmetic mean if all weights zero
        let weight_sum: i64 = scores
            .iter()
            .filter_map(|s| agg_stats.iter().find(|a| a.column_name == s.column_name))
            .map(|a| a.total_data_page_size.max(0))
            .sum();
        if weight_sum > 0 {
            let weighted: f64 = scores
                .iter()
                .map(|s| {
                    let w = agg_stats
                        .iter()
                        .find(|a| a.column_name == s.column_name)
                        .map(|a| a.total_data_page_size.max(0))
                        .unwrap_or(0);
                    s.score as f64 * w as f64
                })
                .sum::<f64>();
            (weighted / weight_sum as f64).round() as u8
        } else {
            (scores.iter().map(|s| s.score as u32).sum::<u32>() / scores.len() as u32) as u8
        }
    };
    let total_null_cell_pct = if total_cells > 0 {
        total_nulls as f64 / total_cells as f64 * 100.0
    } else {
        0.0
    };
    let mut sorted = scores.clone();
    sorted.sort_by(|a, b| a.score.cmp(&b.score));
    let worst_columns = sorted
        .iter()
        .filter(|s| s.score < 80) // only genuinely poor columns
        .take(5)
        .map(|s| s.column_name.clone())
        .collect();
    DatasetQuality {
        overall_score,
        total_null_cell_pct,
        worst_columns,
        schema_consistent,
        column_scores: scores,
    }
}

// task 25: duplicate row detection with bloom filter + xxhash
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DuplicateReport {
    pub total_rows: u64,
    pub estimated_duplicates: u64,
    pub estimated_duplicate_pct: f64,
}

/// Hash a single row across all columns into a u64 fingerprint.
fn hash_row(batch: &arrow::record_batch::RecordBatch, row: usize) -> u64 {
    use xxhash_rust::xxh3::xxh3_64;
    let mut row_bytes = Vec::new();
    for col in batch.columns() {
        if !col.is_null(row) {
            match col.data_type() {
                arrow::datatypes::DataType::Int32 => {
                    if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Int32Array>() {
                        row_bytes.extend_from_slice(&arr.value(row).to_le_bytes());
                    }
                }
                arrow::datatypes::DataType::Int64 => {
                    if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Int64Array>() {
                        row_bytes.extend_from_slice(&arr.value(row).to_le_bytes());
                    }
                }
                arrow::datatypes::DataType::Float32 => {
                    if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Float32Array>() {
                        row_bytes.extend_from_slice(&arr.value(row).to_le_bytes());
                    }
                }
                arrow::datatypes::DataType::Float64 => {
                    if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Float64Array>() {
                        row_bytes.extend_from_slice(&arr.value(row).to_le_bytes());
                    }
                }
                arrow::datatypes::DataType::Boolean => {
                    if let Some(arr) = col.as_any().downcast_ref::<arrow::array::BooleanArray>() {
                        row_bytes.push(arr.value(row) as u8);
                    }
                }
                arrow::datatypes::DataType::Utf8 => {
                    if let Some(arr) = col.as_any().downcast_ref::<arrow::array::StringArray>() {
                        row_bytes.extend_from_slice(arr.value(row).as_bytes());
                    }
                }
                arrow::datatypes::DataType::LargeUtf8 => {
                    if let Some(arr) =
                        col.as_any().downcast_ref::<arrow::array::LargeStringArray>()
                    {
                        row_bytes.extend_from_slice(arr.value(row).as_bytes());
                    }
                }
                _ => row_bytes.push(0u8),
            }
        } else {
            row_bytes.push(0xFF);
        }
    }
    xxh3_64(&row_bytes)
}

/// Detect duplicate rows. For files with <= 5_000_000 rows (or when exact=true),
/// uses a HashSet<u64> for authoritative counts. Otherwise uses a bloom filter
/// (~1% false-positive rate) to keep memory bounded.
pub fn detect_duplicates(path: &Path, exact: bool) -> Result<DuplicateReport> {
    use bloomfilter::Bloom;

    let file = std::fs::File::open(path)?;
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(file).map_err(ParquetLensError::Parquet)?;
    // estimate row count from metadata for bloom sizing / exact threshold
    let total_rows_estimate = builder.metadata().file_metadata().num_rows().max(1) as usize;
    let reader = builder
        .with_batch_size(65536)
        .build()
        .map_err(ParquetLensError::Parquet)?;

    let use_exact = exact || total_rows_estimate <= 5_000_000; // exact threshold: 5M rows
    let mut total_rows = 0u64;
    let mut dups = 0u64;

    if use_exact {
        let mut seen: std::collections::HashSet<u64> =
            std::collections::HashSet::with_capacity(total_rows_estimate.min(5_000_000));
        for batch_result in reader {
            let batch = batch_result.map_err(ParquetLensError::Arrow)?;
            for row in 0..batch.num_rows() {
                let hash = hash_row(&batch, row);
                if !seen.insert(hash) {
                    dups += 1;
                }
                total_rows += 1;
            }
        }
    } else {
        // bloom filter: 1% false positive rate, capped at 50M to prevent OOM
        if total_rows_estimate > 10_000_000 {
            eprintln!(
                "warning: bloom filter for {} rows may use significant memory; consider --exact for authoritative results",
                total_rows_estimate
            );
        }
        let bloom_size = total_rows_estimate.clamp(1000, 50_000_000);
        let mut bloom: Bloom<u64> = Bloom::new_for_fp_rate(bloom_size, 0.01);
        for batch_result in reader {
            let batch = batch_result.map_err(ParquetLensError::Arrow)?;
            for row in 0..batch.num_rows() {
                let hash = hash_row(&batch, row);
                if bloom.check(&hash) {
                    dups += 1;
                } else {
                    bloom.set(&hash);
                }
                total_rows += 1;
            }
        }
    }

    let estimated_duplicate_pct = if total_rows > 0 {
        dups as f64 / total_rows as f64 * 100.0
    } else {
        0.0
    };
    Ok(DuplicateReport {
        total_rows,
        estimated_duplicates: dups,
        estimated_duplicate_pct,
    })
}

#[cfg(test)]
mod tests_score_column {
    use super::*;

    fn sc(null_pct: f64, distinct: Option<u64>, total: i64) -> QualityScore {
        score_column("col", null_pct, distinct, total, false)
    }

    #[test] fn null_0pct() { let s = sc(0.0, None, 100); assert_eq!(s.null_penalty, 0.0); assert_eq!(s.score, 100); }
    #[test] fn null_5pct() { let s = sc(5.0, None, 100); assert_eq!(s.null_penalty, 0.0); assert_eq!(s.score, 100); }
    #[test] fn null_50pct() { let s = sc(50.0, None, 100); assert!((s.null_penalty - 90.0).abs() < 0.01); assert_eq!(s.score, 40); }
    #[test] fn null_100pct() { let s = sc(100.0, None, 100); assert!(s.null_penalty >= 60.0); assert_eq!(s.score, 40); } // capped
    #[test] fn constant_distinct_0() { let s = sc(0.0, Some(0), 100); assert!(s.is_constant); assert_eq!(s.score, 80); }
    #[test] fn constant_distinct_1() { let s = sc(0.0, Some(1), 100); assert!(s.is_constant); assert_eq!(s.score, 80); }
    #[test] fn cardinality_flag() { let s = sc(0.0, Some(100), 100); assert!(s.cardinality_flag); assert_eq!(s.score, 95); }
    #[test] fn no_cardinality_flag() { let s = sc(0.0, Some(50), 100); assert!(!s.cardinality_flag); }
}
