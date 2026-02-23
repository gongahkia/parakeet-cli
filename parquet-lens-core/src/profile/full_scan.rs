use super::boolean::BooleanAccumulator;
use super::cardinality::HllEstimator;
use super::frequency::FrequencyCounter;
use super::histogram::{build_histogram, HistogramBin};
use super::numeric::NumericAccumulator;
use super::string_profiler::StringAccumulator;
use super::temporal::TemporalAccumulator;
use super::{
    BooleanProfile, CardinalityEstimate, FrequencyResult, NumericProfile, StringProfile,
    TemporalProfile,
};
use arrow::array::*;
use arrow::datatypes::{DataType, TimeUnit};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet_lens_common::{ParquetLensError, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnProfileResult {
    pub column_name: String,
    pub cardinality: CardinalityEstimate,
    pub frequency: Option<FrequencyResult>,
    pub numeric: Option<NumericProfile>,
    pub histogram: Option<Vec<HistogramBin>>,
    pub string: Option<StringProfile>,
    pub temporal: Option<TemporalProfile>,
    pub boolean: Option<BooleanProfile>,
    pub truncated: bool, // true if scan was aborted early by timeout
}

pub fn profile_columns(
    path: &Path,
    columns: Option<&[String]>,
    batch_size: usize,
    histogram_bins: usize,
) -> Result<Vec<ColumnProfileResult>> {
    profile_columns_with_timeout(path, columns, batch_size, histogram_bins, None)
}

pub fn profile_columns_with_timeout(
    path: &Path,
    columns: Option<&[String]>,
    batch_size: usize,
    histogram_bins: usize,
    timeout_secs: Option<u64>,
) -> Result<Vec<ColumnProfileResult>> {
    let file = std::fs::File::open(path)?;
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(file).map_err(ParquetLensError::Parquet)?;
    let schema = builder.schema().clone();
    let builder = if let Some(cols) = columns {
        let indices: Vec<usize> = cols
            .iter()
            .filter_map(|c| schema.fields().iter().position(|f| f.name() == c))
            .collect();
        // capture parquet_schema ref before consuming builder
        let mask = parquet::arrow::ProjectionMask::roots(builder.parquet_schema(), indices);
        builder.with_projection(mask)
    } else {
        builder
    };
    let reader = builder
        .with_batch_size(batch_size)
        .build()
        .map_err(ParquetLensError::Parquet)?;

    let field_names: Vec<String> = reader
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let ncols = field_names.len();
    let mut hlls: Vec<HllEstimator> = (0..ncols).map(|_| HllEstimator::new()).collect();
    let mut freq_counters: Vec<FrequencyCounter> =
        (0..ncols).map(|_| FrequencyCounter::new()).collect();
    let mut numeric_accs: Vec<Option<NumericAccumulator>> = reader
        .schema()
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
    let mut str_accs: Vec<Option<StringAccumulator>> = reader
        .schema()
        .fields()
        .iter()
        .map(|f| match f.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 => Some(StringAccumulator::new()),
            _ => None,
        })
        .collect();
    let mut temporal_accs: Vec<Option<TemporalAccumulator>> = reader
        .schema()
        .fields()
        .iter()
        .map(|f| match f.data_type() {
            DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64 => {
                Some(TemporalAccumulator::new())
            }
            _ => None,
        })
        .collect();
    let mut bool_accs: Vec<Option<BooleanAccumulator>> = reader
        .schema()
        .fields()
        .iter()
        .map(|f| match f.data_type() {
            DataType::Boolean => Some(BooleanAccumulator::new()),
            _ => None,
        })
        .collect();
    let mut numeric_vals: Vec<Vec<f64>> = (0..ncols).map(|_| Vec::new()).collect();
    let deadline =
        timeout_secs.map(|s| std::time::Instant::now() + std::time::Duration::from_secs(s));
    let mut timed_out = false;
    let mut reader = reader.peekable();

    while let Some(batch_result) = reader.next() {
        if let Some(dl) = deadline {
            if std::time::Instant::now() >= dl {
                timed_out = true;
                break;
            }
        }
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
                let val_str = array_value_to_str(col_array.as_ref(), row);
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
    } // end while

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
                Some(build_histogram(&numeric_vals[i], histogram_bins))
            } else {
                None
            };
            let string = str_accs[i].take().map(|acc| acc.finish());
            let temporal = temporal_accs[i].take().map(|acc| acc.finish());
            let boolean = bool_accs[i].take().map(|acc| acc.finish());
            ColumnProfileResult {
                column_name: name,
                cardinality,
                frequency,
                numeric,
                histogram,
                string,
                temporal,
                boolean,
                truncated: timed_out,
            }
        })
        .collect();
    Ok(results)
}

fn array_value_to_str(array: &dyn arrow::array::Array, row: usize) -> String {
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
