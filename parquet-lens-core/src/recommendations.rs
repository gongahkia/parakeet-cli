use serde::{Deserialize, Serialize};
use crate::stats::{EncodingAnalysis, CompressionAnalysis, AggregatedColumnStats, RowGroupProfile};
use crate::schema::ColumnSchema;

// --- Task 60: encoding recommendation ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncodingRecommendation {
    pub column_name: String,
    pub current_encodings: Vec<String>,
    pub recommended_encoding: String,
    pub reason: String,
}

pub fn recommend_encodings(
    schema: &[ColumnSchema],
    encodings: &[EncodingAnalysis],
    agg: &[AggregatedColumnStats],
) -> Vec<EncodingRecommendation> {
    schema.iter().filter_map(|col| {
        let enc = encodings.iter().find(|e| e.column_name == col.name)?;
        let stats = agg.iter().find(|s| s.column_name == col.name);
        let distinct = stats.and_then(|s| s.total_distinct_count_estimate).unwrap_or(u64::MAX);
        let (recommended, reason) = match col.physical_type.as_str() {
            "BYTE_ARRAY" | "FIXED_LEN_BYTE_ARRAY" => {
                if distinct < 10000 {
                    ("DICTIONARY".into(), format!("low cardinality ({distinct} distinct values) — dictionary encoding optimal"))
                } else {
                    ("DELTA_LENGTH_BYTE_ARRAY".into(), "high cardinality strings — delta length encoding saves header overhead".into())
                }
            }
            "INT32" | "INT64" => {
                if distinct < 1000 {
                    ("DICTIONARY".into(), format!("low cardinality ({distinct} distinct) integer — dictionary saves space"))
                } else {
                    ("DELTA_BINARY_PACKED".into(), "sorted or monotonic integers — delta encoding highly efficient".into())
                }
            }
            "FLOAT" | "DOUBLE" => ("PLAIN".into(), "floating point — PLAIN is typically optimal".into()),
            "BOOLEAN" => ("RLE".into(), "boolean — RLE encoding is standard and efficient".into()),
            _ => return None,
        };
        if enc.encodings.contains(&recommended) { return None; } // already using recommended
        Some(EncodingRecommendation {
            column_name: col.name.clone(),
            current_encodings: enc.encodings.clone(),
            recommended_encoding: recommended,
            reason,
        })
    }).collect()
}

// --- row group size recommendation ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowGroupSizeRecommendation {
    pub current_avg_bytes: u64,
    pub target_bytes: u64, // 128MB
    pub recommendation: String,
    pub action: String,
}

pub fn recommend_row_group_size(row_groups: &[RowGroupProfile]) -> Option<RowGroupSizeRecommendation> {
    if row_groups.is_empty() { return None; }
    let target_bytes: u64 = 128 * 1024 * 1024; // 128MB
    let avg_bytes = row_groups.iter().map(|rg| rg.total_byte_size as u64).sum::<u64>() / row_groups.len() as u64;
    let ratio = avg_bytes as f64 / target_bytes as f64;
    let (recommendation, action) = if ratio < 0.25 {
        ("Row groups are much smaller than 128MB target — consider increasing row group size for better read efficiency.".into(),
         "Rewrite with row_group_size=128MB (e.g., PyArrow: pq.write_table(t, f, row_group_size=1_000_000))".into())
    } else if ratio < 0.5 {
        ("Row groups are below 128MB target — consider merging small files or increasing row group size.".into(),
         "Set row_group_size to reach ~128MB per group".into())
    } else if ratio > 4.0 {
        ("Row groups are much larger than 128MB — this may reduce parallelism for readers.".into(),
         "Rewrite with row_group_size capped at 128MB".into())
    } else {
        return None; // within acceptable range
    };
    Some(RowGroupSizeRecommendation { current_avg_bytes: avg_bytes, target_bytes, recommendation, action })
}

// --- Task 61: compression recommendation ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionRecommendation {
    pub column_name: String,
    pub current_codec: String,
    pub recommended_codec: String,
    pub estimated_savings_pct: f64,
    pub reason: String,
}

pub fn recommend_compression(compression: &[CompressionAnalysis]) -> Vec<CompressionRecommendation> {
    compression.iter().filter_map(|c| {
        if c.codec == "ZSTD" { return None; } // already optimal
        let (recommended, estimated_savings_pct, reason) = if c.is_uncompressed {
            ("ZSTD".into(), 40.0, "uncompressed column — ZSTD typically achieves 40%+ savings".into())
        } else if c.codec == "SNAPPY" {
            ("ZSTD".into(), 15.0, "ZSTD achieves ~15% better ratio than SNAPPY with comparable speed".into())
        } else if c.codec == "GZIP" {
            ("ZSTD".into(), 5.0, "ZSTD matches GZIP compression with significantly faster decompression".into())
        } else {
            return None;
        };
        if estimated_savings_pct < 20.0 && !c.is_uncompressed { return None; }
        Some(CompressionRecommendation {
            column_name: c.column_name.clone(),
            current_codec: c.codec.clone(),
            recommended_codec: recommended,
            estimated_savings_pct,
            reason,
        })
    }).collect()
}
