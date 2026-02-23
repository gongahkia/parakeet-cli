use crate::reader::open_parquet_file;
use crate::scanner::ParquetFilePath;
use parquet::file::metadata::ParquetMetaData;
use parquet_lens_common::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

// --- Task 50: partition key analysis ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionInfo {
    pub key: String,
    pub distinct_values: Vec<String>,
    pub partition_row_counts: HashMap<String, i64>,
    pub partition_byte_sizes: HashMap<String, u64>,
    pub skewed_partitions: Vec<String>,
}

pub fn analyze_partitions(paths: &[ParquetFilePath]) -> Vec<PartitionInfo> {
    let mut key_values: HashMap<String, HashMap<String, (i64, u64)>> = HashMap::new();
    for pf in paths {
        let file_size = pf.path.metadata().map(|m| m.len()).unwrap_or(0);
        let row_count = open_parquet_file(&pf.path)
            .map(|(info, _)| info.row_count)
            .unwrap_or(0);
        for (k, v) in &pf.partitions {
            let entry = key_values
                .entry(k.clone())
                .or_default()
                .entry(v.clone())
                .or_insert((0, 0));
            entry.0 += row_count;
            entry.1 += file_size;
        }
    }
    key_values
        .into_iter()
        .map(|(key, value_map)| {
            let mut distinct_values: Vec<String> = value_map.keys().cloned().collect();
            distinct_values.sort();
            let partition_row_counts: HashMap<String, i64> = value_map
                .iter()
                .map(|(v, (r, _))| (v.clone(), *r))
                .collect();
            let partition_byte_sizes: HashMap<String, u64> = value_map
                .iter()
                .map(|(v, (_, b))| (v.clone(), *b))
                .collect();
            let row_counts: Vec<i64> = partition_row_counts.values().cloned().collect();
            let median_rows = {
                let mut sorted = row_counts.clone();
                sorted.sort();
                if sorted.is_empty() {
                    1
                } else {
                    sorted[sorted.len() / 2].max(1)
                }
            };
            let skewed_partitions = partition_row_counts
                .iter()
                .filter(|(_, &r)| r > median_rows * 3)
                .map(|(v, _)| v.clone())
                .collect();
            PartitionInfo {
                key,
                distinct_values,
                partition_row_counts,
                partition_byte_sizes,
                skewed_partitions,
            }
        })
        .collect()
}

// --- Task 51: column correlation matrix ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorrelationMatrix {
    pub columns: Vec<String>,
    pub values: Vec<Vec<f64>>, // [col_i][col_j] = pearson r
}

pub fn compute_correlation(_meta: &ParquetMetaData, path: &Path) -> Result<CorrelationMatrix> {
    use arrow::array::*;
    use arrow::datatypes::DataType;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet_lens_common::ParquetLensError;

    let file = std::fs::File::open(path)?;
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(file).map_err(ParquetLensError::Parquet)?;
    let schema = builder.schema().clone();
    let numeric_cols: Vec<usize> = schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(i, f)| match f.data_type() {
            DataType::Int32 | DataType::Int64 | DataType::Float32 | DataType::Float64 => Some(i),
            _ => None,
        })
        .collect();
    let col_names: Vec<String> = numeric_cols
        .iter()
        .map(|&i| schema.field(i).name().clone())
        .collect();
    let n = numeric_cols.len();
    if n == 0 {
        return Ok(CorrelationMatrix {
            columns: Vec::new(),
            values: Vec::new(),
        });
    }
    // accumulators: sum, sum_sq, sum_cross, count
    let mut sums = vec![0.0f64; n];
    let mut sums_sq = vec![0.0f64; n];
    let mut cross = vec![vec![0.0f64; n]; n];
    let mut count = 0u64;
    let mask =
        parquet::arrow::ProjectionMask::roots(builder.parquet_schema(), numeric_cols.clone());
    let reader = builder
        .with_projection(mask)
        .with_batch_size(65536)
        .build()
        .map_err(ParquetLensError::Parquet)?;
    for batch_result in reader {
        let batch = batch_result.map_err(ParquetLensError::Arrow)?;
        let batch_n = batch.num_rows();
        let vals: Vec<Vec<f64>> = (0..n)
            .map(|ci| {
                let col = batch.column(ci);
                (0..batch_n)
                    .map(|row| {
                        if col.is_null(row) {
                            f64::NAN
                        } else {
                            match col.data_type() {
                                DataType::Int32 => {
                                    col.as_any()
                                        .downcast_ref::<Int32Array>()
                                        .unwrap()
                                        .value(row) as f64
                                }
                                DataType::Int64 => {
                                    col.as_any()
                                        .downcast_ref::<Int64Array>()
                                        .unwrap()
                                        .value(row) as f64
                                }
                                DataType::Float32 => {
                                    col.as_any()
                                        .downcast_ref::<Float32Array>()
                                        .unwrap()
                                        .value(row) as f64
                                }
                                DataType::Float64 => col
                                    .as_any()
                                    .downcast_ref::<Float64Array>()
                                    .unwrap()
                                    .value(row),
                                _ => f64::NAN,
                            }
                        }
                    })
                    .collect()
            })
            .collect();
        #[allow(clippy::needless_range_loop)]
        for row in 0..batch_n {
            count += 1;
            for i in 0..n {
                let v = vals[i][row];
                if !v.is_nan() {
                    sums[i] += v;
                    sums_sq[i] += v * v;
                    for j in i..n {
                        if !vals[j][row].is_nan() {
                            cross[i][j] += v * vals[j][row];
                        }
                    }
                }
            }
        }
    }
    let cnt = count as f64;
    let mut matrix = vec![vec![0.0f64; n]; n];
    for i in 0..n {
        for j in 0..n {
            let (ci, cj) = if i <= j { (i, j) } else { (j, i) };
            let cov = cross[ci][cj] / cnt - (sums[ci] / cnt) * (sums[cj] / cnt);
            let si = ((sums_sq[i] / cnt) - (sums[i] / cnt).powi(2)).sqrt();
            let sj = ((sums_sq[j] / cnt) - (sums[j] / cnt).powi(2)).sqrt();
            matrix[i][j] = if si > 0.0 && sj > 0.0 {
                cov / (si * sj)
            } else {
                0.0
            };
        }
    }
    Ok(CorrelationMatrix {
        columns: col_names,
        values: matrix,
    })
}

// --- Task 52: string length histogram ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StringLengthHist {
    pub column_name: String,
    pub bins: Vec<(usize, usize, u64)>, // (start_len, end_len, count)
}

pub fn string_length_histogram(path: &Path, column: &str, bins: usize) -> Result<StringLengthHist> {
    use arrow::array::{LargeStringArray, StringArray};
    use arrow::datatypes::DataType;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet_lens_common::ParquetLensError;
    let file = std::fs::File::open(path)?;
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(file).map_err(ParquetLensError::Parquet)?;
    let reader = builder
        .with_batch_size(65536)
        .build()
        .map_err(ParquetLensError::Parquet)?;
    let mut lengths = Vec::new();
    for batch in reader {
        let batch = batch.map_err(ParquetLensError::Arrow)?;
        let col_idx = batch
            .schema()
            .fields()
            .iter()
            .position(|f| f.name() == column);
        if let Some(idx) = col_idx {
            let col = batch.column(idx);
            for row in 0..batch.num_rows() {
                if !col.is_null(row) {
                    let len = match col.data_type() {
                        DataType::Utf8 => col
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .map(|a| a.value(row).len()),
                        DataType::LargeUtf8 => col
                            .as_any()
                            .downcast_ref::<LargeStringArray>()
                            .map(|a| a.value(row).len()),
                        _ => None,
                    };
                    if let Some(l) = len {
                        lengths.push(l);
                    }
                }
            }
        }
    }
    if lengths.is_empty() {
        return Ok(StringLengthHist {
            column_name: column.to_owned(),
            bins: Vec::new(),
        });
    }
    let min = *lengths.iter().min().unwrap();
    let max = *lengths.iter().max().unwrap();
    let width = ((max - min) / bins).max(1);
    let mut counts = vec![0u64; bins];
    for &l in &lengths {
        let idx = ((l - min) / width).min(bins - 1);
        counts[idx] += 1;
    }
    let result = counts
        .into_iter()
        .enumerate()
        .map(|(i, c)| (min + i * width, min + (i + 1) * width, c))
        .collect();
    Ok(StringLengthHist {
        column_name: column.to_owned(),
        bins: result,
    })
}

// --- Task 53: sorted order detection ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortedOrderInfo {
    pub column_name: String,
    pub appears_ascending: bool,
    pub appears_descending: bool,
    pub confidence: f64, // ratio of in-order adjacent row-group pairs (0.0–1.0)
}

pub fn detect_sort_order(meta: &ParquetMetaData) -> Vec<SortedOrderInfo> {
    let schema = meta.file_metadata().schema_descr();
    (0..schema.num_columns())
        .map(|col_idx| {
            let col_name = schema.column(col_idx).name().to_owned();
            let mut last_max: Option<Vec<u8>> = None;
            let mut asc = true;
            let mut desc = true;
            let mut total_pairs = 0usize;
            let mut asc_pairs = 0usize;
            for rg_idx in 0..meta.num_row_groups() {
                let rg = meta.row_group(rg_idx);
                if col_idx >= rg.num_columns() {
                    break;
                }
                let chunk = rg.column(col_idx);
                if let Some(stats) = chunk.statistics() {
                    let min = stats.min_bytes_opt().map(|b| b.to_vec());
                    let max = stats.max_bytes_opt().map(|b| b.to_vec());
                    if let (Some(ref prev_max), Some(ref cur_min)) = (&last_max, &min) {
                        total_pairs += 1;
                        if cur_min >= prev_max {
                            asc_pairs += 1;
                        } else {
                            asc = false;
                        }
                        if cur_min > prev_max {
                            desc = false;
                        }
                    }
                    last_max = max;
                }
            }
            let confidence = if total_pairs == 0 {
                1.0
            } else {
                asc_pairs as f64 / total_pairs as f64
            };
            SortedOrderInfo {
                column_name: col_name,
                appears_ascending: asc,
                appears_descending: desc,
                confidence,
            }
        })
        .collect()
}

// --- Task 54: page index analysis ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageIndexInfo {
    pub has_column_index: bool,
    pub has_offset_index: bool,
    pub column_index_coverage_pct: f64,
}

pub fn analyze_page_index(meta: &ParquetMetaData) -> PageIndexInfo {
    let rg_count = meta.num_row_groups();
    let mut col_idx_count = 0usize;
    let mut off_idx_count = 0usize;
    let mut total = 0usize;
    for rg_idx in 0..rg_count {
        let rg = meta.row_group(rg_idx);
        for col_idx in 0..rg.num_columns() {
            total += 1;
            let chunk = rg.column(col_idx);
            if chunk.column_index_offset().is_some() {
                col_idx_count += 1;
            }
            if chunk.offset_index_offset().is_some() {
                off_idx_count += 1;
            }
        }
    }
    PageIndexInfo {
        has_column_index: col_idx_count > 0,
        has_offset_index: off_idx_count > 0,
        column_index_coverage_pct: if total > 0 {
            col_idx_count as f64 / total as f64 * 100.0
        } else {
            0.0
        },
    }
}

// --- Task 55: bloom filter presence detection ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BloomFilterInfo {
    pub column_name: String,
    pub has_bloom_filter: bool,
}

pub fn detect_bloom_filters(meta: &ParquetMetaData) -> Vec<BloomFilterInfo> {
    let schema = meta.file_metadata().schema_descr();
    (0..schema.num_columns())
        .map(|col_idx| {
            let col_name = schema.column(col_idx).name().to_owned();
            let has_bloom = (0..meta.num_row_groups()).any(|rg_idx| {
                let rg = meta.row_group(rg_idx);
                col_idx < rg.num_columns() && rg.column(col_idx).bloom_filter_offset().is_some()
            });
            BloomFilterInfo {
                column_name: col_name,
                has_bloom_filter: has_bloom,
            }
        })
        .collect()
}
