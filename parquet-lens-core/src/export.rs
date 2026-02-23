use std::io::Write;
use std::path::Path;
use serde_json;
use parquet_lens_common::Result;
use crate::parallel_reader::DatasetProfile;
use crate::stats::{AggregatedColumnStats, RowGroupProfile};
use crate::quality::{QualityScore, DatasetQuality};
use crate::null_patterns::NullPatternGroup;
use crate::engine::EngineInfo;
use crate::baseline::BaselineRegression;

// --- Task 62: headless summary output ---

pub fn print_summary(dataset: &DatasetProfile, quality: Option<&DatasetQuality>) {
    println!("{:<16} {}", "Files:", dataset.file_count);
    println!("{:<16} {}", "Rows:", dataset.total_rows);
    println!("{:<16} {} bytes", "Size:", dataset.total_bytes);
    println!("{:<16} {}", "Columns:", dataset.combined_schema.len());
    if let Some(q) = quality {
        println!("{:<16} {}/100", "Quality:", q.overall_score);
        println!("{:<16} {:.2}%", "Null cells:", q.total_null_cell_pct);
        if !q.worst_columns.is_empty() {
            println!("{:<16} {}", "Worst cols:", q.worst_columns.join(", "));
        }
    }
}

// --- Task 63: JSON export ---

pub fn export_json(
    output_path: &Path,
    dataset: &DatasetProfile,
    agg_stats: &[AggregatedColumnStats],
    row_groups: &[RowGroupProfile],
    quality_scores: &[QualityScore],
    null_patterns: &[NullPatternGroup],
    engine_info: Option<&EngineInfo>,
    baseline_regressions: &[BaselineRegression],
) -> Result<()> {
    let mut doc = serde_json::json!({
        "dataset": dataset,
        "column_stats": agg_stats,
        "row_groups": row_groups,
        "quality_scores": quality_scores,
        "null_patterns": null_patterns,
        "baseline_regressions": baseline_regressions,
    });
    if let Some(ei) = engine_info {
        doc["engine_info"] = serde_json::to_value(ei)
            .unwrap_or(serde_json::Value::Null);
    }
    let mut file = std::fs::File::create(output_path)?;
    serde_json::to_writer_pretty(&mut file, &doc)
        .map_err(|e| parquet_lens_common::ParquetLensError::Other(e.to_string()))?;
    Ok(())
}

// --- Task 64: CSV export ---

pub fn export_csv(
    output_path: &Path,
    agg_stats: &[AggregatedColumnStats],
    quality_scores: &[QualityScore],
) -> Result<()> {
    let mut file = std::fs::File::create(output_path)?;
    writeln!(file, "column_name,type,null_rate,cardinality,data_size_bytes,compressed_size_bytes,compression_ratio,quality_score,breakdown")?;
    for stat in agg_stats {
        let qs = quality_scores.iter().find(|q| q.column_name == stat.column_name);
        let quality = qs.map(|q| q.score).unwrap_or(100);
        let breakdown_raw = qs.map(|q| q.breakdown.as_str()).unwrap_or("");
        // csv-escape: wrap in quotes if contains comma, quote, or newline
        let breakdown = if breakdown_raw.contains(',') || breakdown_raw.contains('"') || breakdown_raw.contains('\n') {
            format!("\"{}\"", breakdown_raw.replace('"', "\"\""))
        } else {
            breakdown_raw.to_string()
        };
        writeln!(file, "{},{},{:.4},{},{},{},{:.4},{},{}",
            stat.column_name,
            "-",
            stat.null_percentage / 100.0,
            stat.total_distinct_count_estimate.map_or("-".into(), |d| d.to_string()),
            stat.total_data_page_size,
            stat.total_compressed_size,
            stat.compression_ratio,
            quality,
            breakdown,
        )?;
    }
    Ok(())
}
