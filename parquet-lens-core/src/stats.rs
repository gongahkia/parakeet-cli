use parquet::file::metadata::ParquetMetaData;
use parquet::file::statistics::Statistics;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// --- Task 9: per-column, per-row-group stats from metadata ---

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ColumnStats {
    pub column_name: String,
    pub row_group_index: usize,
    pub null_count: Option<u64>,
    pub distinct_count: Option<u64>,
    pub min_bytes: Option<Vec<u8>>,
    pub max_bytes: Option<Vec<u8>>,
    pub data_page_size: i64,
    pub compressed_size: i64,
}

pub fn read_column_stats(meta: &ParquetMetaData) -> Vec<ColumnStats> {
    let mut out = Vec::new();
    for rg_idx in 0..meta.num_row_groups() {
        let rg = meta.row_group(rg_idx);
        for col_idx in 0..rg.num_columns() {
            let col = rg.column(col_idx);
            let name = col.column_descr().name().to_owned();
            let (null_count, distinct_count, min_bytes, max_bytes) = match col.statistics() {
                Some(stats) => (
                    stats.null_count_opt(),
                    stats.distinct_count_opt(),
                    stats.min_bytes_opt().map(|b| b.to_vec()),
                    stats.max_bytes_opt().map(|b| b.to_vec()),
                ),
                None => (None, None, None, None),
            };
            out.push(ColumnStats {
                column_name: name,
                row_group_index: rg_idx,
                null_count,
                distinct_count,
                min_bytes,
                max_bytes,
                data_page_size: col.uncompressed_size(),
                compressed_size: col.compressed_size(),
            });
        }
    }
    out
}

// --- Task 10: aggregated file-level column stats ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregatedColumnStats {
    pub column_name: String,
    pub total_null_count: u64,
    pub null_percentage: f64,
    pub total_distinct_count_estimate: Option<u64>,
    pub total_data_page_size: i64,
    pub total_compressed_size: i64,
    pub compression_ratio: f64,
    pub min_bytes: Option<Vec<u8>>,
    pub max_bytes: Option<Vec<u8>>,
}

pub fn aggregate_column_stats(
    per_rg: &[ColumnStats],
    total_rows: i64,
) -> Vec<AggregatedColumnStats> {
    let mut map: HashMap<String, Vec<&ColumnStats>> = HashMap::new();
    for cs in per_rg {
        map.entry(cs.column_name.clone()).or_default().push(cs);
    }
    let mut out = Vec::new();
    for (name, cols) in map {
        let total_null_count: u64 = cols.iter().filter_map(|c| c.null_count).sum();
        let null_percentage = if total_rows > 0 {
            total_null_count as f64 / total_rows as f64 * 100.0
        } else {
            0.0
        };
        let total_distinct_count_estimate = if cols.iter().all(|c| c.distinct_count.is_some()) {
            Some(cols.iter().filter_map(|c| c.distinct_count).sum())
        } else {
            None
        };
        let total_data_page_size: i64 = cols.iter().map(|c| c.data_page_size).sum();
        let total_compressed_size: i64 = cols.iter().map(|c| c.compressed_size).sum();
        let compression_ratio = if total_compressed_size > 0 {
            total_data_page_size as f64 / total_compressed_size as f64
        } else {
            1.0
        };
        // global min/max = just use first non-None (raw bytes, not type-aware)
        let min_bytes = cols.iter().find_map(|c| c.min_bytes.clone());
        let max_bytes = cols.iter().find_map(|c| c.max_bytes.clone());
        out.push(AggregatedColumnStats {
            column_name: name,
            total_null_count,
            null_percentage,
            total_distinct_count_estimate,
            total_data_page_size,
            total_compressed_size,
            compression_ratio,
            min_bytes,
            max_bytes,
        });
    }
    out
}

// --- Task 11: row group profiler ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowGroupProfile {
    pub index: usize,
    pub num_rows: i64,
    pub total_byte_size: i64,
    pub compressed_size: i64,
    pub compression_ratio: f64,
    pub column_offsets: Vec<i64>,
    pub column_sizes: Vec<i64>,
}

pub fn profile_row_groups(meta: &ParquetMetaData) -> Vec<RowGroupProfile> {
    (0..meta.num_row_groups())
        .map(|i| {
            let rg = meta.row_group(i);
            let compressed_size = rg.compressed_size();
            let total_byte_size = rg.total_byte_size();
            let compression_ratio = if compressed_size > 0 {
                total_byte_size as f64 / compressed_size as f64
            } else {
                1.0
            };
            let column_offsets = (0..rg.num_columns())
                .map(|j| rg.column(j).file_offset())
                .collect();
            let column_sizes = (0..rg.num_columns())
                .map(|j| rg.column(j).compressed_size())
                .collect();
            RowGroupProfile {
                index: i,
                num_rows: rg.num_rows(),
                total_byte_size,
                compressed_size,
                compression_ratio,
                column_offsets,
                column_sizes,
            }
        })
        .collect()
}

// --- Task 12: row group uniformity analysis ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UniformityReport {
    pub count: usize,
    pub mean_rows: f64,
    pub median_rows: f64,
    pub stddev_rows: f64,
    pub min_rows: i64,
    pub max_rows: i64,
    pub mean_bytes: f64,
    pub median_bytes: f64,
    pub stddev_bytes: f64,
    pub min_bytes: i64,
    pub max_bytes: i64,
    pub outlier_indices: Vec<usize>, // >2 stddev from mean (bytes)
}

fn median_f64(sorted: &[f64]) -> f64 {
    let n = sorted.len();
    if n == 0 {
        return 0.0;
    }
    if n % 2 == 0 {
        (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0
    } else {
        sorted[n / 2]
    }
}

pub fn analyze_uniformity(profiles: &[RowGroupProfile]) -> UniformityReport {
    let n = profiles.len();
    if n == 0 {
        return UniformityReport {
            count: 0,
            mean_rows: 0.0,
            median_rows: 0.0,
            stddev_rows: 0.0,
            min_rows: 0,
            max_rows: 0,
            mean_bytes: 0.0,
            median_bytes: 0.0,
            stddev_bytes: 0.0,
            min_bytes: 0,
            max_bytes: 0,
            outlier_indices: Vec::new(),
        };
    }
    let rows: Vec<f64> = profiles.iter().map(|p| p.num_rows as f64).collect();
    let bytes: Vec<f64> = profiles.iter().map(|p| p.total_byte_size as f64).collect();
    let mean_rows = rows.iter().sum::<f64>() / n as f64;
    let mean_bytes = bytes.iter().sum::<f64>() / n as f64;
    let stddev_rows = (rows.iter().map(|r| (r - mean_rows).powi(2)).sum::<f64>() / n as f64).sqrt();
    let stddev_bytes =
        (bytes.iter().map(|b| (b - mean_bytes).powi(2)).sum::<f64>() / n as f64).sqrt();
    let mut sorted_rows = rows.clone();
    sorted_rows.sort_by(f64::total_cmp);
    let mut sorted_bytes = bytes.clone();
    sorted_bytes.sort_by(f64::total_cmp);
    let outlier_indices = profiles
        .iter()
        .enumerate()
        .filter(|(_, p)| (p.total_byte_size as f64 - mean_bytes).abs() > 2.0 * stddev_bytes)
        .map(|(i, _)| i)
        .collect();
    UniformityReport {
        count: n,
        mean_rows,
        median_rows: median_f64(&sorted_rows),
        stddev_rows,
        min_rows: profiles.iter().map(|p| p.num_rows).min().unwrap_or(0),
        max_rows: profiles.iter().map(|p| p.num_rows).max().unwrap_or(0),
        mean_bytes,
        median_bytes: median_f64(&sorted_bytes),
        stddev_bytes,
        min_bytes: profiles
            .iter()
            .map(|p| p.total_byte_size)
            .min()
            .unwrap_or(0),
        max_bytes: profiles
            .iter()
            .map(|p| p.total_byte_size)
            .max()
            .unwrap_or(0),
        outlier_indices,
    }
}

// --- Task 13: encoding analysis per column ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncodingAnalysis {
    pub column_name: String,
    pub encodings: Vec<String>,
    pub is_plain_only: bool,
}

pub fn analyze_encodings(meta: &ParquetMetaData) -> Vec<EncodingAnalysis> {
    let mut map: HashMap<String, Vec<String>> = HashMap::new();
    for rg_idx in 0..meta.num_row_groups() {
        let rg = meta.row_group(rg_idx);
        for col_idx in 0..rg.num_columns() {
            let col = rg.column(col_idx);
            let name = col.column_descr().name().to_owned();
            let encodings: Vec<String> = col.encodings().iter().map(|e| format!("{e:?}")).collect();
            map.entry(name).or_default().extend(encodings);
        }
    }
    map.into_iter()
        .map(|(name, mut encs)| {
            encs.sort();
            encs.dedup();
            let is_plain_only = encs
                .iter()
                .all(|e| e == "PLAIN" || e == "RLE_DICTIONARY" || e == "BIT_PACKED");
            let is_plain_only = encs == vec!["PLAIN".to_string()];
            EncodingAnalysis {
                column_name: name,
                encodings: encs,
                is_plain_only,
            }
        })
        .collect()
}

// --- Task 14: compression analysis per column ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionAnalysis {
    pub column_name: String,
    pub codec: String,
    pub uncompressed_size: i64,
    pub compressed_size: i64,
    pub compression_ratio: f64,
    pub is_uncompressed: bool,
}

pub fn analyze_compression(meta: &ParquetMetaData) -> Vec<CompressionAnalysis> {
    let mut map: HashMap<String, (String, i64, i64)> = HashMap::new();
    for rg_idx in 0..meta.num_row_groups() {
        let rg = meta.row_group(rg_idx);
        for col_idx in 0..rg.num_columns() {
            let col = rg.column(col_idx);
            let name = col.column_descr().name().to_owned();
            let codec = format!("{:?}", col.compression());
            let (_, uncomp, comp) = map.entry(name).or_insert((codec.clone(), 0, 0));
            *uncomp += col.uncompressed_size();
            *comp += col.compressed_size();
        }
    }
    map.into_iter()
        .map(|(name, (codec, uncomp, comp))| {
            let compression_ratio = if comp > 0 {
                uncomp as f64 / comp as f64
            } else {
                1.0
            };
            let is_uncompressed = codec == "UNCOMPRESSED";
            CompressionAnalysis {
                column_name: name,
                codec,
                uncompressed_size: uncomp,
                compressed_size: comp,
                compression_ratio,
                is_uncompressed,
            }
        })
        .collect()
}
