use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineInfo {
    pub engine_name: String,
    pub version_hint: Option<String>,
    pub hints: Vec<String>,
}

pub fn identify_engine(created_by: &str) -> EngineInfo {
    let lower = created_by.to_lowercase();
    let (name, hints): (&str, Vec<&str>) = if lower.contains("spark") {
        ("Apache Spark", vec![
            "Use ZSTD compression for better ratio than Snappy",
            "Consider enabling bloom filters on high-cardinality join keys",
            "Check spark.sql.parquet.compression.codec setting",
        ])
    } else if lower.contains("pyarrow") || lower.contains("arrow") {
        ("PyArrow / Apache Arrow", vec![
            "Consider row group size: default 128MB is good for most workloads",
            "Use write_statistics=True for better predicate pushdown",
        ])
    } else if lower.contains("duckdb") {
        ("DuckDB", vec![
            "DuckDB writes well-optimized Parquet with column statistics",
            "Consider ZSTD compression: COPY ... TO ... (COMPRESSION ZSTD)",
        ])
    } else if lower.contains("impala") {
        ("Apache Impala", vec![
            "Impala may not write column statistics — verify with parquet-tools",
            "Consider rewriting with Spark/PyArrow for better statistics coverage",
        ])
    } else if lower.contains("hive") {
        ("Apache Hive", vec![
            "Hive-written files may use older Parquet encodings",
            "Consider rewriting with ZSTD for improved compression",
        ])
    } else if lower.contains("trino") || lower.contains("presto") {
        ("Trino / Presto", vec![
            "Trino writes well-structured Parquet with statistics",
            "Bloom filters may not be present — add if using point lookups",
        ])
    } else if lower.contains("flink") {
        ("Apache Flink", vec![
            "Streaming-written files may have many small row groups — consider compaction",
        ])
    } else if lower.contains("pandas") {
        ("Pandas (via fastparquet or PyArrow)", vec![
            "Verify that write_index=False to avoid redundant index columns",
        ])
    } else {
        ("Unknown", vec![])
    };
    let version_hint = extract_version(created_by);
    EngineInfo { engine_name: name.into(), version_hint, hints: hints.iter().map(|s| s.to_string()).collect() }
}

fn extract_version(s: &str) -> Option<String> {
    // try to find "version X.Y.Z" or "vX.Y.Z" pattern
    let re_patterns = ["version ", "v"];
    for prefix in re_patterns {
        if let Some(pos) = s.to_lowercase().find(prefix) {
            let rest = &s[pos + prefix.len()..];
            let ver: String = rest.chars().take_while(|c| c.is_ascii_digit() || *c == '.' || *c == '-').collect();
            if !ver.is_empty() { return Some(ver); }
        }
    }
    None
}
