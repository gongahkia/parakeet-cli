pub mod parallel_reader;
pub mod profile;
pub mod reader;
pub mod scanner;
pub mod schema;
pub mod schema_diff;
pub mod stats;
pub use parallel_reader::{read_metadata_parallel, DatasetProfile, FileProfile};
pub use parquet_lens_common::{ParquetLensError, Result};
pub use profile::{
    build_histogram, profile_columns, profile_columns_with_timeout, BooleanProfile,
    CardinalityEstimate, ColumnProfileResult, FrequencyResult, HistogramBin, NumericProfile,
    StringProfile, TemporalProfile,
};
pub use reader::{open_parquet_file, ParquetFileInfo, SchemaFieldInfo};
pub use scanner::{resolve_paths, scan_directory, ParquetFilePath};
pub use schema::{extract_schema, ColumnSchema};
pub use schema_diff::{check_schema_consistency, InconsistencyKind, SchemaInconsistency};
pub use stats::{
    aggregate_column_stats, analyze_compression, analyze_encodings, analyze_uniformity,
    profile_row_groups, read_column_stats, AggregatedColumnStats, ColumnStats, CompressionAnalysis,
    EncodingAnalysis, RowGroupProfile, UniformityReport,
};
pub mod compare;
pub mod export;
pub mod gcs_reader;
pub mod quality;
pub mod recommendations;
pub mod s3_reader;
pub mod stats_ext;
pub use compare::{
    compare_datasets, diff_schemas, diff_stats, ColumnSchemaDiff, ColumnStatsDiff,
    DatasetComparison, DiffStatus,
};
pub use export::{export_csv, export_json, print_summary};
pub use gcs_reader::{
    is_gcs_uri, list_gcs_parquet, parse_gcs_uri, read_gcs_parquet_metadata, GcsUri,
};
pub use quality::{
    detect_duplicates, score_column, summarize_quality, DatasetQuality, DuplicateReport,
    QualityScore,
};
pub use recommendations::{
    recommend_compression, recommend_encodings, recommend_row_group_size,
    CompressionRecommendation, EncodingRecommendation, RowGroupSizeRecommendation,
};
pub use s3_reader::{
    is_s3_uri, list_s3_parquet, parse_s3_uri, read_s3_parquet_metadata, read_s3_range, S3Uri,
};
pub use stats_ext::{
    analyze_page_index, analyze_partitions, compute_correlation, detect_bloom_filters,
    detect_sort_order, string_length_histogram, BloomFilterInfo, CorrelationMatrix, PageIndexInfo,
    PartitionInfo, SortedOrderInfo, StringLengthHist,
};
pub mod filter;
pub use filter::{filter_count, parse_predicate, FilterResult, Predicate};
pub mod sample;
pub use sample::{sample_row_groups, SampleConfig, SampledProfile};
pub mod baseline;
pub mod engine;
pub mod nested;
pub mod null_patterns;
pub mod repair;
pub mod timeseries;
pub use baseline::{load_baseline_regressions, BaselineProfile, BaselineRegression};
pub use engine::{identify_engine, EngineInfo};
pub use nested::{profile_nested_columns, NestedColumnProfile};
pub use null_patterns::{analyze_null_patterns, NullPatternGroup};
pub use repair::{detect_repair_suggestions, RepairSuggestion};
pub use timeseries::{profile_timeseries, TimeSeriesProfile};
