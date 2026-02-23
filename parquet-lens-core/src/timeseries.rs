use serde::{Serialize, Deserialize};
use std::path::Path;
use bytes::Bytes;
use memmap2::Mmap;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet_lens_common::{ParquetLensError, Result};

#[derive(Debug, Serialize, Deserialize)]
pub struct TimeSeriesProfile {
    pub column_name: String,
    pub min_timestamp: Option<i64>, // epoch millis
    pub max_timestamp: Option<i64>,
    pub total_duration_ms: Option<i64>,
    pub mean_gap_ms: Option<f64>,
    pub max_gap_ms: Option<i64>,
    pub is_monotonic: bool,
    pub missing_interval_hint: Option<String>,
}

pub fn profile_timeseries(path: &Path, timestamp_columns: &[String]) -> Result<Vec<TimeSeriesProfile>> {
    let file = std::fs::File::open(path)?;
    let mmap: Mmap = unsafe { Mmap::map(&file)? };
    let bytes = Bytes::copy_from_slice(&mmap);
    let reader = SerializedFileReader::new(bytes).map_err(ParquetLensError::Parquet)?;
    let meta = reader.metadata();
    let schema = meta.file_metadata().schema_descr();
    let num_rgs = meta.num_row_groups();
    let mut profiles = Vec::new();
    for col_name in timestamp_columns {
        // find column index by name
        let col_idx = (0..schema.num_columns()).find(|&i| schema.column(i).name() == col_name.as_str());
        let Some(col_idx) = col_idx else { continue; };
        let mut rg_mins: Vec<i64> = Vec::new();
        let mut rg_maxs: Vec<i64> = Vec::new();
        for rg_i in 0..num_rgs {
            let rg = meta.row_group(rg_i);
            if col_idx >= rg.num_columns() { continue; }
            let col_meta = rg.column(col_idx);
            if let Some(stats) = col_meta.statistics() {
                use parquet::data_type::AsBytes;
                let min_bytes = stats.min_bytes_opt();
                let max_bytes = stats.max_bytes_opt();
                if let (Some(mn), Some(mx)) = (min_bytes, max_bytes) {
                    if mn.len() >= 8 && mx.len() >= 8 {
                        let min_v = i64::from_le_bytes(mn[..8].try_into().unwrap_or([0;8]));
                        let max_v = i64::from_le_bytes(mx[..8].try_into().unwrap_or([0;8]));
                        rg_mins.push(min_v);
                        rg_maxs.push(max_v);
                    }
                }
            }
        }
        if rg_mins.is_empty() {
            profiles.push(TimeSeriesProfile {
                column_name: col_name.clone(),
                min_timestamp: None, max_timestamp: None,
                total_duration_ms: None, mean_gap_ms: None, max_gap_ms: None,
                is_monotonic: true, missing_interval_hint: None,
            });
            continue;
        }
        let overall_min = *rg_mins.iter().min().unwrap();
        let overall_max = *rg_maxs.iter().max().unwrap();
        let total_duration_ms = Some(overall_max - overall_min);
        // gaps between consecutive row groups: gap[i] = rg_min[i+1] - rg_max[i]
        let mut gaps: Vec<i64> = Vec::new();
        let mut is_monotonic = true;
        for i in 1..rg_mins.len() {
            if rg_mins[i] < rg_maxs[i - 1] { is_monotonic = false; }
            let gap = rg_mins[i] - rg_maxs[i - 1];
            gaps.push(gap);
        }
        let mean_gap_ms = if gaps.is_empty() { None } else {
            Some(gaps.iter().sum::<i64>() as f64 / gaps.len() as f64)
        };
        let max_gap_ms = gaps.iter().copied().max();
        let missing_interval_hint = if let (Some(mean), Some(max_g)) = (mean_gap_ms, max_gap_ms) {
            if mean > 0.0 && max_g as f64 > 10.0 * mean {
                Some(format!("gap detected: max_gap {}ms >> mean_gap {:.0}ms", max_g, mean))
            } else { None }
        } else { None };
        profiles.push(TimeSeriesProfile {
            column_name: col_name.clone(),
            min_timestamp: Some(overall_min),
            max_timestamp: Some(overall_max),
            total_duration_ms,
            mean_gap_ms,
            max_gap_ms,
            is_monotonic,
            missing_interval_hint,
        });
    }
    Ok(profiles)
}
