use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineInfo {
    pub engine_name: String,
    pub version_hint: Option<String>,
    pub hints: Vec<String>,
}

pub fn identify_engine(created_by: &str) -> EngineInfo {
    let lower = created_by.to_lowercase();
    let (name, hints): (&str, &[&'static str]) =
        if lower.contains("spark") {
            (
                "Apache Spark",
                &[
                    "Use ZSTD compression for better ratio than Snappy",
                    "Consider enabling bloom filters on high-cardinality join keys",
                    "Check spark.sql.parquet.compression.codec setting",
                ],
            )
        } else if lower.contains("pyarrow") || lower.contains("arrow") {
            (
                "PyArrow / Apache Arrow",
                &[
                    "Consider row group size: default 128MB is good for most workloads",
                    "Use write_statistics=True for better predicate pushdown",
                ],
            )
        } else if lower.contains("duckdb") {
            (
                "DuckDB",
                &[
                    "DuckDB writes well-optimized Parquet with column statistics",
                    "Consider ZSTD compression: COPY ... TO ... (COMPRESSION ZSTD)",
                ],
            )
        } else if lower.contains("impala") {
            (
                "Apache Impala",
                &[
                    "Impala may not write column statistics — verify with parquet-tools",
                    "Consider rewriting with Spark/PyArrow for better statistics coverage",
                ],
            )
        } else if lower.contains("hive") {
            (
                "Apache Hive",
                &[
                    "Hive-written files may use older Parquet encodings",
                    "Consider rewriting with ZSTD for improved compression",
                ],
            )
        } else if lower.contains("trino") || lower.contains("presto") {
            (
                "Trino / Presto",
                &[
                    "Trino writes well-structured Parquet with statistics",
                    "Bloom filters may not be present — add if using point lookups",
                ],
            )
        } else if lower.contains("flink") {
            (
                "Apache Flink",
                &["Streaming-written files may have many small row groups — consider compaction"],
            )
        } else if lower.contains("pandas") {
            (
                "Pandas (via fastparquet or PyArrow)",
                &["Verify that write_index=False to avoid redundant index columns"],
            )
        } else if lower.contains("parquet-go") || lower.contains("parquet_go") {
            ("parquet-go", &[
            "parquet-go may omit column statistics — verify before using predicate pushdown",
        ])
        } else if lower.contains("parquet4s") {
            ("parquet4s (Scala)", &[
            "parquet4s uses Spark-compatible encodings; consider ZSTD for better compression",
        ])
        } else {
            ("Unknown", &[])
        };
    let version_hint = extract_version(created_by);
    EngineInfo {
        engine_name: name.into(),
        version_hint,
        hints: hints.iter().map(|s| s.to_string()).collect(),
    }
}

#[cfg(test)]
mod tests_identify_engine {
    use super::*;
    #[test]
    fn spark() {
        let e = identify_engine("Apache Spark 3.4.0");
        assert_eq!(e.engine_name, "Apache Spark");
        assert!(!e.hints.is_empty());
    }
    #[test]
    fn spark_lowercase() {
        let e = identify_engine("spark sql");
        assert_eq!(e.engine_name, "Apache Spark");
    }
    #[test]
    fn pyarrow() {
        let e = identify_engine("pyarrow 14.0.1");
        assert_eq!(e.engine_name, "PyArrow / Apache Arrow");
        assert!(!e.hints.is_empty());
    }
    #[test]
    fn arrow() {
        let e = identify_engine("arrow 12.0");
        assert_eq!(e.engine_name, "PyArrow / Apache Arrow");
    }
    #[test]
    fn duckdb() {
        let e = identify_engine("DuckDB 0.10.0");
        assert_eq!(e.engine_name, "DuckDB");
        assert!(!e.hints.is_empty());
    }
    #[test]
    fn impala() {
        let e = identify_engine("impala version 4.0");
        assert_eq!(e.engine_name, "Apache Impala");
        assert!(!e.hints.is_empty());
    }
    #[test]
    fn hive() {
        let e = identify_engine("Apache Hive 3.1.3");
        assert_eq!(e.engine_name, "Apache Hive");
        assert!(!e.hints.is_empty());
    }
    #[test]
    fn trino() {
        let e = identify_engine("Trino 420");
        assert_eq!(e.engine_name, "Trino / Presto");
        assert!(!e.hints.is_empty());
    }
    #[test]
    fn presto() {
        let e = identify_engine("presto-main:0.284");
        assert_eq!(e.engine_name, "Trino / Presto");
    }
    #[test]
    fn flink() {
        let e = identify_engine("Apache Flink 1.18.0");
        assert_eq!(e.engine_name, "Apache Flink");
        assert!(!e.hints.is_empty());
    }
    #[test]
    fn pandas() {
        let e = identify_engine("pandas 2.1.0");
        assert_eq!(e.engine_name, "Pandas (via fastparquet or PyArrow)");
        assert!(!e.hints.is_empty());
    }
    #[test]
    fn parquet_go_dash() {
        let e = identify_engine("parquet-go v1.0");
        assert_eq!(e.engine_name, "parquet-go");
        assert!(!e.hints.is_empty());
    }
    #[test]
    fn parquet_go_underscore() {
        let e = identify_engine("parquet_go 1.0");
        assert_eq!(e.engine_name, "parquet-go");
    }
    #[test]
    fn parquet4s() {
        let e = identify_engine("parquet4s 2.0.0");
        assert_eq!(e.engine_name, "parquet4s (Scala)");
        assert!(!e.hints.is_empty());
    }
    #[test]
    fn unknown_returns_empty_hints() {
        let e = identify_engine("some-unknown-writer 99");
        assert_eq!(e.engine_name, "Unknown");
        assert!(e.hints.is_empty());
    }
}

fn extract_version(s: &str) -> Option<String> {
    // try to find "version X.Y.Z" or "vX.Y.Z" pattern
    let re_patterns = ["version ", "v"];
    for prefix in re_patterns {
        if let Some(pos) = s.to_lowercase().find(prefix) {
            let rest = &s[pos + prefix.len()..];
            let ver: String = rest
                .chars()
                .take_while(|c| c.is_ascii_digit() || *c == '.' || *c == '-')
                .collect();
            if !ver.is_empty() {
                return Some(ver);
            }
        }
    }
    None
}
