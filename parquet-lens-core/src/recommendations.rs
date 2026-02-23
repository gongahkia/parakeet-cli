use serde::{Deserialize, Serialize};
use crate::stats::{EncodingAnalysis, CompressionAnalysis, AggregatedColumnStats};
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
