use crate::profile::ColumnProfileResult;
use crate::stats::{AggregatedColumnStats, RowGroupProfile};
use crate::{aggregate_column_stats, profile_row_groups, read_column_stats};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet_lens_common::{ParquetLensError, Result};
use std::path::Path;

pub struct SampleConfig {
    pub percentage: f64,        // 0.0–100.0
    pub no_extrapolation: bool, // when true, skip confidence extrapolation
    pub seed: Option<u64>,      // deterministic rg selection seed; None uses default (seed=0)
}

pub struct SampledProfile {
    pub agg_stats: Vec<AggregatedColumnStats>,
    pub row_groups: Vec<RowGroupProfile>,
    pub profile_results: Vec<ColumnProfileResult>,
    pub sampled_rg_count: usize,
    pub total_rg_count: usize,
    pub confidence_note: String,
}

pub fn sample_row_groups(
    path: &Path,
    config: &SampleConfig,
    histogram_bins: usize,
) -> Result<SampledProfile> {
    let file = std::fs::File::open(path)?;
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(file).map_err(ParquetLensError::Parquet)?;
    let meta = builder.metadata().clone();
    let total = meta.num_row_groups();
    if total == 0 {
        return Err(ParquetLensError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "no row groups in file",
        )));
    }
    let n = ((config.percentage / 100.0) * total as f64).ceil() as usize;
    let n = n.clamp(1, total);
    // deterministic pseudo-random selection: XOR index with seed then knuth hash
    let seed = config.seed.unwrap_or(0);
    let mut indices: Vec<usize> = (0..total).collect();
    indices.sort_by_key(|&i| (i as u64 ^ seed).wrapping_mul(2654435761)); // knuth multiplicative hash
    let selected: Vec<usize> = indices[..n].iter().cloned().collect();

    // profile only selected row groups
    let rg_all = profile_row_groups(&meta);
    let row_groups: Vec<RowGroupProfile> = selected.iter().map(|&i| rg_all[i].clone()).collect();

    // read column stats for selected rgs
    let col_stats_all = read_column_stats(&meta);
    let sampled_row_count: i64 = selected.iter().map(|&i| meta.row_group(i).num_rows()).sum();
    let mut agg_stats = aggregate_column_stats(&col_stats_all, sampled_row_count);

    // extrapolate: scale null counts + sizes by total/sampled ratio (skipped if no_extrapolation)
    if !config.no_extrapolation {
        let scale = total as f64 / n as f64;
        let total_rows_est: i64 = (sampled_row_count as f64 * scale).round() as i64;
        for s in &mut agg_stats {
            s.total_null_count = (s.total_null_count as f64 * scale).round() as u64;
            s.total_data_page_size = (s.total_data_page_size as f64 * scale).round() as i64;
            s.total_compressed_size = (s.total_compressed_size as f64 * scale).round() as i64;
            s.null_percentage = if total_rows_est > 0 {
                s.total_null_count as f64 / total_rows_est as f64 * 100.0
            } else {
                0.0
            };
        }
    }

    // profile columns (full data read on selected rgs only via row group filter)
    // build a temp file reader restricted to selected row groups
    let profile_results = profile_columns_sampled(path, &selected, histogram_bins)?;

    // 95% CI margin: p=0.5, n=sampled rg count → ±1.96*sqrt(0.25/n)*100
    let margin = if n > 0 {
        1.96 * (0.25_f64 / n as f64).sqrt() * 100.0
    } else {
        100.0
    };
    let confidence_note = format!(
        "~{:.0}% sample ({} of {} row groups); stats extrapolated; ±{:.1}% CI",
        config.percentage, n, total, margin
    );

    Ok(SampledProfile {
        agg_stats,
        row_groups,
        profile_results,
        sampled_rg_count: n,
        total_rg_count: total,
        confidence_note,
    })
}

fn profile_columns_sampled(
    path: &Path,
    rg_indices: &[usize],
    histogram_bins: usize,
) -> Result<Vec<ColumnProfileResult>> {
    use arrow::array::*;
    use arrow::datatypes::{DataType, TimeUnit};
    // accumulator types imported below
    use crate::profile::full_scan::ColumnProfileResult as CPR;
    // re-use profile_columns but with row group restriction
    // build reader filtered to selected row groups only
    let file = std::fs::File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(ParquetLensError::Parquet)?
        .with_row_groups(rg_indices.to_vec());
    let reader = builder.build().map_err(ParquetLensError::Parquet)?;
    // delegate to inner accumulation logic — reuse profile_columns internals via direct call
    // simpler: just call profile_columns after writing a filtered parquet to /tmp — too heavy
    // instead, inline the accumulation using the restricted reader
    drop(reader); // drop, rebuild below for actual processing
                  // re-open and read with restriction, delegating to existing profile_columns logic
                  // profile_columns doesn't accept rg filter — call it via temp path workaround is too heavy
                  // instead read all data from selected RGs directly here
    let file2 = std::fs::File::open(path)?;
    let builder2 = ParquetRecordBatchReaderBuilder::try_new(file2)
        .map_err(ParquetLensError::Parquet)?
        .with_row_groups(rg_indices.to_vec());
    let schema = builder2.schema().clone();
    let reader2 = builder2
        .with_batch_size(8192)
        .build()
        .map_err(ParquetLensError::Parquet)?;

    let field_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    let ncols = field_names.len();

    // import accumulator types from profile submodules
    use super::profile::boolean::BooleanAccumulator;
    use super::profile::cardinality::HllEstimator;
    use super::profile::frequency::FrequencyCounter;
    use super::profile::histogram::build_histogram as bh;
    use super::profile::numeric::NumericAccumulator;
    use super::profile::string_profiler::StringAccumulator;
    use super::profile::temporal::TemporalAccumulator;

    let mut hlls: Vec<HllEstimator> = (0..ncols).map(|_| HllEstimator::new()).collect();
    let mut freq_counters: Vec<FrequencyCounter> =
        (0..ncols).map(|_| FrequencyCounter::new()).collect();
    let mut numeric_accs: Vec<Option<NumericAccumulator>> = schema
        .fields()
        .iter()
        .map(|f| match f.data_type() {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64 => Some(NumericAccumulator::new()),
            _ => None,
        })
        .collect();
    let mut str_accs: Vec<Option<StringAccumulator>> = schema
        .fields()
        .iter()
        .map(|f| match f.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 => Some(StringAccumulator::new()),
            _ => None,
        })
        .collect();
    let mut temporal_accs: Vec<Option<TemporalAccumulator>> = schema
        .fields()
        .iter()
        .map(|f| match f.data_type() {
            DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64 => {
                Some(TemporalAccumulator::new())
            }
            _ => None,
        })
        .collect();
    let mut bool_accs: Vec<Option<BooleanAccumulator>> = schema
        .fields()
        .iter()
        .map(|f| match f.data_type() {
            DataType::Boolean => Some(BooleanAccumulator::new()),
            _ => None,
        })
        .collect();
    let mut numeric_vals: Vec<Vec<f64>> = (0..ncols).map(|_| Vec::new()).collect();

    for batch_result in reader2 {
        let batch = batch_result.map_err(ParquetLensError::Arrow)?;
        for (col_idx, col_array) in batch.columns().iter().enumerate() {
            for row in 0..col_array.len() {
                if col_array.is_null(row) {
                    if let Some(acc) = &mut temporal_accs[col_idx] {
                        acc.add_null();
                    }
                    if let Some(acc) = &mut bool_accs[col_idx] {
                        acc.add(None);
                    }
                    continue;
                }
                let val_str = array_val_str(col_array.as_ref(), row);
                hlls[col_idx].add_bytes(val_str.as_bytes());
                freq_counters[col_idx].add(val_str);
                match col_array.data_type() {
                    DataType::Int8 => {
                        let a = col_array.as_any().downcast_ref::<Int8Array>().unwrap();
                        let v = a.value(row) as f64;
                        if let Some(acc) = &mut numeric_accs[col_idx] {
                            acc.add(v);
                        }
                        numeric_vals[col_idx].push(v);
                    }
                    DataType::Int16 => {
                        let a = col_array.as_any().downcast_ref::<Int16Array>().unwrap();
                        let v = a.value(row) as f64;
                        if let Some(acc) = &mut numeric_accs[col_idx] {
                            acc.add(v);
                        }
                        numeric_vals[col_idx].push(v);
                    }
                    DataType::Int32 => {
                        let a = col_array.as_any().downcast_ref::<Int32Array>().unwrap();
                        let v = a.value(row) as f64;
                        if let Some(acc) = &mut numeric_accs[col_idx] {
                            acc.add(v);
                        }
                        numeric_vals[col_idx].push(v);
                    }
                    DataType::Int64 => {
                        let a = col_array.as_any().downcast_ref::<Int64Array>().unwrap();
                        let v = a.value(row) as f64;
                        if let Some(acc) = &mut numeric_accs[col_idx] {
                            acc.add(v);
                        }
                        numeric_vals[col_idx].push(v);
                    }
                    DataType::UInt8 => {
                        let a = col_array.as_any().downcast_ref::<UInt8Array>().unwrap();
                        let v = a.value(row) as f64;
                        if let Some(acc) = &mut numeric_accs[col_idx] {
                            acc.add(v);
                        }
                        numeric_vals[col_idx].push(v);
                    }
                    DataType::UInt16 => {
                        let a = col_array.as_any().downcast_ref::<UInt16Array>().unwrap();
                        let v = a.value(row) as f64;
                        if let Some(acc) = &mut numeric_accs[col_idx] {
                            acc.add(v);
                        }
                        numeric_vals[col_idx].push(v);
                    }
                    DataType::UInt32 => {
                        let a = col_array.as_any().downcast_ref::<UInt32Array>().unwrap();
                        let v = a.value(row) as f64;
                        if let Some(acc) = &mut numeric_accs[col_idx] {
                            acc.add(v);
                        }
                        numeric_vals[col_idx].push(v);
                    }
                    DataType::UInt64 => {
                        let a = col_array.as_any().downcast_ref::<UInt64Array>().unwrap();
                        let v = a.value(row) as f64;
                        if let Some(acc) = &mut numeric_accs[col_idx] {
                            acc.add(v);
                        }
                        numeric_vals[col_idx].push(v);
                    }
                    DataType::Float32 => {
                        let a = col_array.as_any().downcast_ref::<Float32Array>().unwrap();
                        let v = a.value(row) as f64;
                        if let Some(acc) = &mut numeric_accs[col_idx] {
                            acc.add(v);
                        }
                        numeric_vals[col_idx].push(v);
                    }
                    DataType::Float64 => {
                        let a = col_array.as_any().downcast_ref::<Float64Array>().unwrap();
                        let v = a.value(row);
                        if let Some(acc) = &mut numeric_accs[col_idx] {
                            acc.add(v);
                        }
                        numeric_vals[col_idx].push(v);
                    }
                    DataType::Utf8 => {
                        let a = col_array.as_any().downcast_ref::<StringArray>().unwrap();
                        if let Some(acc) = &mut str_accs[col_idx] {
                            acc.add(a.value(row));
                        }
                    }
                    DataType::LargeUtf8 => {
                        let a = col_array
                            .as_any()
                            .downcast_ref::<LargeStringArray>()
                            .unwrap();
                        if let Some(acc) = &mut str_accs[col_idx] {
                            acc.add(a.value(row));
                        }
                    }
                    DataType::Boolean => {
                        let a = col_array.as_any().downcast_ref::<BooleanArray>().unwrap();
                        if let Some(acc) = &mut bool_accs[col_idx] {
                            acc.add(Some(a.value(row)));
                        }
                    }
                    DataType::Timestamp(TimeUnit::Millisecond, _) => {
                        let a = col_array
                            .as_any()
                            .downcast_ref::<TimestampMillisecondArray>()
                            .unwrap();
                        if let Some(acc) = &mut temporal_accs[col_idx] {
                            acc.add_ms(a.value(row));
                        }
                    }
                    DataType::Timestamp(TimeUnit::Second, _) => {
                        let a = col_array
                            .as_any()
                            .downcast_ref::<TimestampSecondArray>()
                            .unwrap();
                        if let Some(acc) = &mut temporal_accs[col_idx] {
                            acc.add_ms(a.value(row) * 1000);
                        }
                    }
                    DataType::Timestamp(TimeUnit::Microsecond, _) => {
                        let a = col_array
                            .as_any()
                            .downcast_ref::<TimestampMicrosecondArray>()
                            .unwrap();
                        if let Some(acc) = &mut temporal_accs[col_idx] {
                            acc.add_ms(a.value(row) / 1000);
                        }
                    }
                    DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                        let a = col_array
                            .as_any()
                            .downcast_ref::<TimestampNanosecondArray>()
                            .unwrap();
                        if let Some(acc) = &mut temporal_accs[col_idx] {
                            acc.add_ms(a.value(row) / 1_000_000);
                        }
                    }
                    DataType::Date32 => {
                        let a = col_array.as_any().downcast_ref::<Date32Array>().unwrap();
                        if let Some(acc) = &mut temporal_accs[col_idx] {
                            acc.add_ms(a.value(row) as i64 * 86400 * 1000);
                        }
                    }
                    DataType::Date64 => {
                        let a = col_array.as_any().downcast_ref::<Date64Array>().unwrap();
                        if let Some(acc) = &mut temporal_accs[col_idx] {
                            acc.add_ms(a.value(row));
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    let results = field_names
        .into_iter()
        .enumerate()
        .map(|(i, name)| {
            let cardinality = hlls.remove(0).estimate();
            let freq_counter = freq_counters.remove(0);
            let frequency = if cardinality.approximate_distinct < 10000 {
                Some(freq_counter.top_n(20))
            } else {
                let _ = freq_counter.top_n(0);
                None
            };
            let numeric = numeric_accs[i].take().map(|acc| acc.finish());
            let histogram = if !numeric_vals[i].is_empty() {
                Some(bh(&numeric_vals[i], histogram_bins))
            } else {
                None
            };
            let string = str_accs[i].take().map(|acc| acc.finish());
            let temporal = temporal_accs[i].take().map(|acc| acc.finish());
            let boolean = bool_accs[i].take().map(|acc| acc.finish());
            CPR {
                column_name: name,
                cardinality,
                frequency,
                numeric,
                histogram,
                string,
                temporal,
                boolean,
                truncated: false,
            }
        })
        .collect();
    Ok(results)
}

fn array_val_str(array: &dyn arrow::array::Array, row: usize) -> String {
    use arrow::array::*;
    use arrow::datatypes::DataType;
    match array.data_type() {
        DataType::Int8 => array
            .as_any()
            .downcast_ref::<Int8Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::Int16 => array
            .as_any()
            .downcast_ref::<Int16Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::Int32 => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::UInt8 => array
            .as_any()
            .downcast_ref::<UInt8Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::UInt16 => array
            .as_any()
            .downcast_ref::<UInt16Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::UInt32 => array
            .as_any()
            .downcast_ref::<UInt32Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::UInt64 => array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::Float32 => array
            .as_any()
            .downcast_ref::<Float32Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::Float64 => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::LargeUtf8 => array
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::Boolean => array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        _ => format!("row_{row}"),
    }
}
