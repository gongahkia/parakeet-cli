mod tui;

use clap::{Parser, Subcommand};
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use parquet::file::metadata::ParquetMetaData;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet_lens_core::{
    aggregate_column_stats,
    analyze_compression,
    analyze_encodings,
    analyze_null_patterns,
    analyze_partitions,
    compare_datasets,
    detect_duplicates,
    detect_repair_suggestions,
    export_csv,
    export_json,
    identify_engine,
    is_gcs_uri,
    is_s3_uri,
    load_baseline_regressions,
    open_parquet_file, // resolve_paths used in rp() helper
    print_summary,
    profile_columns,
    profile_nested_columns,
    profile_row_groups,
    profile_timeseries,
    read_column_stats,
    read_gcs_parquet_metadata,
    read_metadata_parallel,
    read_s3_parquet_metadata,
    recommend_row_group_size,
    resolve_paths,
    sample_row_groups,
    score_column,
    summarize_quality,
    AggregatedColumnStats,
    DatasetProfile,
    EncodingAnalysis,
    ParquetFileInfo,
    ParquetFilePath,
    QualityScore,
    SampleConfig,
};
use ratatui::{backend::CrosstermBackend, Terminal};
use std::{io, time::Duration};
use tui::app::{App, View};
use tui::events::handle_key;
use tui::session::Session;
use tui::ui::render;

fn parse_sample_pct(s: &str) -> Result<f64, String> {
    // validate sample % at CLI parse time
    let v: f64 = s.parse().map_err(|_| format!("not a float: {s}"))?;
    if v > 0.0 && v <= 100.0 {
        Ok(v)
    } else {
        Err(format!("sample must be in (0.0, 100.0], got {v}"))
    }
}

/// block_in_place wrapper to call async resolve_paths from sync context
fn rp(input: &str) -> anyhow::Result<Vec<ParquetFilePath>> {
    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(resolve_paths(input)))
        .map_err(|e| anyhow::anyhow!("{e}"))
}

fn compute_quality_scores(
    agg_stats: &[AggregatedColumnStats],
    encodings: &[EncodingAnalysis],
    total_rows: i64,
) -> Vec<QualityScore> {
    agg_stats
        .iter()
        .map(|agg| {
            let is_plain = encodings
                .iter()
                .find(|e| e.column_name == agg.column_name)
                .map(|e| e.is_plain_only)
                .unwrap_or(false);
            score_column(
                &agg.column_name,
                agg.null_percentage,
                agg.total_distinct_count_estimate,
                total_rows,
                is_plain,
            )
        })
        .collect()
}

fn load_file_stats(
    paths: &[ParquetFilePath],
) -> anyhow::Result<(DatasetProfile, ParquetFileInfo, ParquetMetaData)> {
    let dataset = read_metadata_parallel(paths).map_err(|e| anyhow::anyhow!("{e}"))?;
    let p0_str = paths[0].path.to_string_lossy().to_string();
    let (file_info, meta) = tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current().block_on(
            parquet_lens_core::open_parquet_auto(&p0_str, None),
        )
    })
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok((dataset, file_info, meta))
}

use parquet_lens_common::Config;

#[derive(Parser)]
#[command(name = "parquet-lens", version, about = "Parquet file inspector")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Inspect {
        path: String,
        #[arg(long, value_parser = parse_sample_pct)]
        sample: Option<f64>,
        #[arg(long)]
        watch: bool,
        #[arg(long)]
        no_sample_extrapolation: bool,
        #[arg(long)]
        save_baseline: bool,
        #[arg(long)]
        sample_seed: Option<u64>,
        #[arg(long)]
        watch_interval: Option<u64>,
        #[arg(long)]
        fail_on_regression: bool,
    },
    Summary {
        path: String,
        #[arg(long)]
        save: bool,
        #[arg(long, default_value = "plain")]
        format: String,
        #[arg(long)]
        json: bool,
        #[arg(long, value_parser = parse_sample_pct)]
        sample: Option<f64>,
        #[arg(long)]
        sample_seed: Option<u64>,
    },
    Compare {
        path1: String,
        path2: String,
    },
    Export {
        path: String,
        #[arg(long, default_value = "json")]
        format: String,
        #[arg(long, value_delimiter = ',')]
        columns: Option<Vec<String>>,
        #[arg(long)]
        output: Option<String>,
        #[arg(long, value_parser = parse_sample_pct)]
        sample: Option<f64>,
        #[arg(long)]
        sample_seed: Option<u64>,
    },
    Duplicates {
        path: String,
        /// Force HashSet-based exact dedup regardless of row count
        #[arg(long)]
        exact: bool,
        #[arg(long)]
        json: bool,
        #[arg(long)]
        threshold: Option<f64>,
    },
    Check {
        path: String,
        #[arg(long, default_value = "plain")]
        format: String,
        #[arg(long)]
        fail_on_regression: bool,
    },
    Filter {
        path: String,
        expr: String,
        #[arg(long)]
        output: Option<String>,
        #[arg(long)]
        limit: Option<usize>,
    },
    Schema {
        path: String,
        #[arg(long)]
        json: bool,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let config = Config::load().unwrap_or_else(|e| {
        eprintln!(
            "warning: config load failed ({}): {e} — using defaults",
            Config::config_path().display()
        );
        Config::default()
    });
    match cli.command {
        Commands::Inspect {
            path,
            sample,
            watch,
            no_sample_extrapolation,
            save_baseline,
            sample_seed,
            watch_interval,
            fail_on_regression,
        } => {
            run_tui(
                path,
                config,
                sample,
                no_sample_extrapolation,
                save_baseline,
                sample_seed,
                watch,
                watch_interval,
                fail_on_regression,
            )?
        }
        Commands::Summary {
            path,
            save,
            format,
            json,
            sample,
            sample_seed,
        } => run_summary(path, save, &format, json, sample, sample_seed, &config)?,
        Commands::Compare { path1, path2 } => run_compare(path1, path2, config)?,
        Commands::Export {
            path,
            format,
            columns,
            output,
            sample,
            sample_seed,
        } => run_export(path, format, columns, output, sample, sample_seed, config)?,
        Commands::Duplicates { path, exact, json, threshold } => run_duplicates(path, exact, json, threshold)?,
        Commands::Check { path, format, fail_on_regression } => run_check(path, &format, fail_on_regression)?,
        Commands::Filter { path, expr, output, limit } => run_filter(path, expr, output, limit)?,
        Commands::Schema { path, json } => run_schema(path, json)?,
    }
    Ok(())
}

fn run_duplicates(input_path: String, exact: bool, json: bool, threshold: Option<f64>) -> anyhow::Result<()> {
    let dup_path = if is_s3_uri(&input_path) || is_gcs_uri(&input_path) {
        // download to tempfile for cloud paths
        let bytes = if is_s3_uri(&input_path) {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current()
                    .block_on(parquet_lens_core::read_s3_range(&input_path, 0, i64::MAX, None))
            })
            .map_err(|e| anyhow::anyhow!("{e}"))?
        } else {
            // GCS: fetch full object
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current()
                    .block_on(parquet_lens_core::read_gcs_parquet_metadata(&input_path))
            })
            .map_err(|_| anyhow::anyhow!("GCS download not fully supported for duplicates"))?;
            anyhow::bail!("GCS duplicate detection requires local file download (not yet implemented)");
        };
        let mut tmp = tempfile::NamedTempFile::new()?;
        std::io::Write::write_all(&mut tmp, &bytes)?;
        tmp.into_temp_path().to_path_buf()
    } else {
        std::path::PathBuf::from(&input_path)
    };
    let report =
        detect_duplicates(&dup_path, exact).map_err(|e| anyhow::anyhow!("{e}"))?;
    if json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!("{:<24} {}", "total_rows:", report.total_rows);
        println!("{:<24} {}", "estimated_duplicates:", report.estimated_duplicates);
        println!("{:<24} {:.2}%", "estimated_duplicate_pct:", report.estimated_duplicate_pct);
    }
    if let Some(thr) = threshold {
        if report.estimated_duplicate_pct > thr {
            eprintln!("FAIL: duplicate rate {:.2}% exceeds threshold {:.2}%", report.estimated_duplicate_pct, thr);
            std::process::exit(1);
        }
    }
    Ok(())
}

fn run_filter(input_path: String, expr: String, output: Option<String>, limit: Option<usize>) -> anyhow::Result<()> {
    let predicate = parquet_lens_core::parse_predicate(&expr).map_err(|e| anyhow::anyhow!("{e}"))?;
    let path = std::path::Path::new(&input_path);
    let result = parquet_lens_core::filter_count(path, &predicate).map_err(|e| anyhow::anyhow!("{e}"))?;
    println!("matched_rows:  {}", result.matched_rows);
    println!("scanned_rows:  {}", result.scanned_rows);
    println!("skipped_rgs:   {}/{}", result.skipped_rgs, result.total_rgs);
    if let Some(out_path) = output {
        let batches = parquet_lens_core::filter_rows(path, &predicate, limit).map_err(|e| anyhow::anyhow!("{e}"))?;
        if batches.is_empty() {
            println!("no matching rows — CSV not written");
            return Ok(());
        }
        let mut file = std::fs::File::create(&out_path)?;
        let schema = batches[0].schema();
        let mut writer = arrow::csv::WriterBuilder::new().with_header(true).build(&mut file);
        for batch in &batches {
            writer.write(batch).map_err(|e| anyhow::anyhow!("{e}"))?;
        }
        drop(writer);
        println!("exported to {out_path}");
        let _ = schema; // suppress unused warning
    }
    Ok(())
}

fn run_schema(input_path: String, json: bool) -> anyhow::Result<()> {
    let path = std::path::Path::new(&input_path);
    let schema = parquet_lens_core::extract_schema(path).map_err(|e| anyhow::anyhow!("{e}"))?;
    if json {
        println!("{}", serde_json::to_string_pretty(&schema)?);
    } else {
        println!("{:<40} {:<12} {:<20} {}", "name", "type", "logical_type", "repetition");
        println!("{}", "-".repeat(80));
        for col in &schema {
            println!("{:<40} {:<12} {:<20} {}", col.name, col.physical_type, col.logical_type.as_deref().unwrap_or("-"), col.repetition);
        }
    }
    Ok(())
}

fn run_check(input_path: String, format: &str, fail_on_regression: bool) -> anyhow::Result<()> {
    let paths = rp(&input_path)?;
    if paths.is_empty() {
        anyhow::bail!("No Parquet files found: {input_path}");
    }
    let (dataset, _, meta) = load_file_stats(&paths)?;
    let col_stats = read_column_stats(&meta);
    let total_rows = dataset.total_rows;
    let agg_stats = aggregate_column_stats(&col_stats, total_rows);
    let encodings = analyze_encodings(&meta);
    let quality_scores = compute_quality_scores(&agg_stats, &encodings, total_rows);
    let schema: Vec<parquet_lens_core::ColumnSchema> = dataset
        .combined_schema
        .iter()
        .map(|c| parquet_lens_core::ColumnSchema {
            name: c.name.clone(),
            physical_type: c.physical_type.clone(),
            logical_type: c.logical_type.clone(),
            repetition: c.repetition.clone(),
            max_def_level: c.max_def_level,
            max_rep_level: c.max_rep_level,
        })
        .collect();
    let (_, regressions) =
        load_baseline_regressions(&paths[0].path, &agg_stats, &quality_scores, &schema);
    if format == "json" {
        println!("{}", serde_json::to_string(&regressions)?);
    } else {
        if regressions.is_empty() {
            eprintln!("check: no regressions detected");
        } else {
            for r in &regressions {
                eprintln!("regression: {} — {}", r.column, r.detail);
            }
        }
    }
    if fail_on_regression && !regressions.is_empty() {
        anyhow::bail!("{} regression(s) detected", regressions.len());
    }
    Ok(())
}

fn run_tui(
    input_path: String,
    config: Config,
    sample_pct: Option<f64>,
    no_sample_extrapolation: bool,
    save_baseline: bool,
    sample_seed: Option<u64>,
    watch: bool,
    watch_interval: Option<u64>,
    fail_on_regression: bool,
) -> anyhow::Result<()> {
    let paths = rp(&input_path)?;
    if paths.is_empty() {
        anyhow::bail!("No Parquet files found: {input_path}");
    }
    let dataset = read_metadata_parallel(&paths).map_err(|e| anyhow::anyhow!("{e}"))?;
    // task 18/19: handle S3/GCS metadata via specialized readers
    let p0_str = paths[0].path.to_string_lossy();
    let (file_info, meta) = if is_s3_uri(&p0_str) {
        let meta = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(read_s3_parquet_metadata(
                &p0_str,
                config.s3.endpoint_url.as_deref(),
            ))
        })
        .map_err(|e| anyhow::anyhow!("{e}"))?;
        let fi = parquet_lens_core::ParquetFileInfo {
            path: paths[0].path.clone(),
            file_size: 0,
            row_count: meta.file_metadata().num_rows(),
            row_group_count: meta.num_row_groups(),
            created_by: meta.file_metadata().created_by().map(|s| s.to_owned()),
            parquet_version: meta.file_metadata().version(),
            key_value_metadata: Vec::new(),
            schema_fields: Vec::new(),
        };
        (fi, meta)
    } else if is_gcs_uri(&p0_str) {
        let meta = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(read_gcs_parquet_metadata(&p0_str))
        })
        .map_err(|e| anyhow::anyhow!("{e}"))?;
        let fi = parquet_lens_core::ParquetFileInfo {
            path: paths[0].path.clone(),
            file_size: 0,
            row_count: meta.file_metadata().num_rows(),
            row_group_count: meta.num_row_groups(),
            created_by: meta.file_metadata().created_by().map(|s| s.to_owned()),
            parquet_version: meta.file_metadata().version(),
            key_value_metadata: Vec::new(),
            schema_fields: Vec::new(),
        };
        (fi, meta)
    } else {
        open_parquet_file(&paths[0].path).map_err(|e| anyhow::anyhow!("{e}"))?
    };
    let row_groups = profile_row_groups(&meta);
    let col_stats = read_column_stats(&meta);
    let total_rows = file_info.row_count;
    let agg_stats = aggregate_column_stats(&col_stats, total_rows);
    let encoding_analysis = analyze_encodings(&meta);
    let compression_analysis = analyze_compression(&meta);
    let quality_scores = compute_quality_scores(&agg_stats, &encoding_analysis, total_rows);

    let mut app = App::new(input_path.clone(), config);
    if let Some(s) = Session::load() {
        app.restore_from_session(&s);
    }
    app.dataset = Some(dataset.clone());
    app.file_info = Some(file_info);
    app.row_groups = row_groups;
    app.agg_stats = agg_stats;
    app.encoding_analysis = encoding_analysis;
    app.compression_analysis = compression_analysis;
    app.quality_scores = quality_scores;

    // data preview: read up to max_rows_preview rows for DataPreview view
    if !is_s3_uri(&p0_str) && !is_gcs_uri(&p0_str) {
        let max_preview = app.config.display.max_rows_preview;
        if let Ok(preview_file) = std::fs::File::open(&paths[0].path) {
            if let Ok(pb) = ParquetRecordBatchReaderBuilder::try_new(preview_file) {
                let schema = pb.schema().clone();
                app.preview_headers = schema.fields().iter().map(|f| f.name().clone()).collect();
                if let Ok(reader) = pb.with_batch_size(max_preview).build() {
                    let mut rows = Vec::new();
                    for batch_result in reader {
                        if rows.len() >= max_preview { break; }
                        if let Ok(batch) = batch_result {
                            for row_idx in 0..batch.num_rows() {
                                if rows.len() >= max_preview { break; }
                                let row: Vec<String> = (0..batch.num_columns())
                                    .map(|c| arrow::util::display::array_value_to_string(batch.column(c), row_idx).unwrap_or_default())
                                    .collect();
                                rows.push(row);
                            }
                        }
                    }
                    app.preview_rows = rows;
                }
            }
        }
    }

    // repair suggestions
    app.repair_suggestions =
        detect_repair_suggestions(&app.row_groups, &app.agg_stats, &app.encoding_analysis);
    app.rg_size_recommendation = recommend_row_group_size(&app.row_groups);

    // time-series profiling — detect timestamp/date/time columns from schema
    let ts_cols: Vec<String> = dataset
        .combined_schema
        .iter()
        .filter(|c| {
            let logical_match = c.logical_type.as_deref().map(|t| {
                t.contains("Timestamp") || t.contains("Date") || t.contains("Time")
            }).unwrap_or(false);
            // fallback: INT96 with no logical type = legacy Spark timestamp
            let int96_fallback = c.physical_type == "INT96" && c.logical_type.is_none();
            logical_match || int96_fallback
        })
        .map(|c| c.name.clone())
        .collect();
    if !ts_cols.is_empty() {
        match profile_timeseries(&paths[0].path, &ts_cols) {
            Ok(ts) => {
                app.timeseries_profiles = ts;
            }
            Err(e) => {
                eprintln!("timeseries warning: {e}");
                app.status_msg = format!("timeseries error: {e}");
            }
        }
    }

    // nested column profiling
    match profile_nested_columns(&paths[0].path) {
        Ok(np) => app.nested_profiles = np,
        Err(e) => eprintln!("nested profile warning: {e}"),
    }

    // engine identification from created_by
    if let Some(created_by) = app
        .file_info
        .as_ref()
        .and_then(|fi| fi.created_by.as_deref())
    {
        app.engine_info = Some(identify_engine(created_by));
    }

    // baseline diff
    {
        let schema = app.columns().to_vec();
        let (base, regressions) =
            load_baseline_regressions(&paths[0].path, &app.agg_stats, &app.quality_scores, &schema);
        app.baseline_captured_at = base.as_ref().map(|b| b.captured_at);
        app.has_baseline = base.is_some();
        app.baseline_regressions = regressions;
        if save_baseline {
            let new_base = parquet_lens_core::BaselineProfile::new(
                &app.input_path,
                schema,
                app.agg_stats.clone(),
                app.quality_scores.clone(),
            );
            match new_base.save() {
                Ok(_) => {
                    app.status_msg = "baseline saved (--save-baseline)".into();
                    app.has_baseline = true;
                }
                Err(e) => {
                    app.status_msg = format!("--save-baseline failed: {e}");
                }
            }
        }
    }

    // --fail-on-regression: exit before TUI if regressions found
    if fail_on_regression && !app.baseline_regressions.is_empty() {
        for r in &app.baseline_regressions {
            eprintln!("regression: {} — {}", r.column, r.detail);
        }
        anyhow::bail!("{} regression(s) detected (--fail-on-regression)", app.baseline_regressions.len());
    }

    // pre-compute null patterns
    app.null_patterns = analyze_null_patterns(&app.agg_stats);

    // partition key analysis (hive-style key=value path segments)
    app.partition_infos = analyze_partitions(&paths);

    if let Some(pct) = sample_pct {
        let cfg = SampleConfig {
            percentage: pct,
            no_extrapolation: no_sample_extrapolation,
            seed: sample_seed,
        };
        match sample_row_groups(&paths[0].path, &cfg, 20) {
            Ok(sp) => {
                app.agg_stats = sp.agg_stats;
                app.row_groups = sp.row_groups;
                app.full_scan_results = sp.profile_results;
                app.sample_note = Some(sp.confidence_note.clone());
                app.status_msg = format!("Sampled — {} | q:quit ?:help", sp.confidence_note);
            }
            Err(e) => {
                app.status_msg = format!("Sample error: {e}");
            }
        }
    } else {
        app.status_msg = "Ready — q:quit ?:help".into();
    }

    // --watch: local filesystem watcher
    let _watcher_guard: Option<notify::RecommendedWatcher> = if watch && !is_s3_uri(&p0_str) && !is_gcs_uri(&p0_str) {
        use notify::{Watcher, RecursiveMode, Config as NotifyConfig};
        let (wtx, wrx) = std::sync::mpsc::channel::<()>();
        let mut watcher = notify::RecommendedWatcher::new(
            move |res: Result<notify::Event, notify::Error>| {
                if let Ok(ev) = res {
                    if ev.kind.is_modify() || ev.kind.is_create() {
                        let _ = wtx.send(());
                    }
                }
            },
            NotifyConfig::default(),
        ).map_err(|e| anyhow::anyhow!("watch init failed: {e}"))?;
        let watch_path = std::path::Path::new(&input_path);
        let watch_target = if watch_path.is_file() {
            watch_path.parent().unwrap_or(watch_path)
        } else {
            watch_path
        };
        watcher.watch(watch_target, RecursiveMode::NonRecursive)
            .map_err(|e| anyhow::anyhow!("watch failed: {e}"))?;
        app.watch_rx = Some(wrx);
        Some(watcher)
    } else if watch && (is_s3_uri(&p0_str) || is_gcs_uri(&p0_str)) {
        let (wtx, wrx) = std::sync::mpsc::channel::<()>();
        let uri = p0_str.to_string();
        let s3_endpoint = app.config.s3.endpoint_url.clone();
        let cloud_interval = watch_interval.unwrap_or(30);
        let is_s3 = is_s3_uri(&uri);
        tokio::spawn(async move {
            let interval = tokio::time::Duration::from_secs(cloud_interval);
            let mut prev_rows: Option<i64> = None;
            loop {
                tokio::time::sleep(interval).await;
                let cur_rows = if is_s3 {
                    read_s3_parquet_metadata(&uri, s3_endpoint.as_deref()).await
                        .ok()
                        .map(|m| m.file_metadata().num_rows())
                } else {
                    read_gcs_parquet_metadata(&uri).await
                        .ok()
                        .map(|m| m.file_metadata().num_rows())
                };
                if let Some(rows) = cur_rows {
                    if prev_rows.map(|p| p != rows).unwrap_or(false) {
                        let _ = wtx.send(());
                    }
                    prev_rows = Some(rows);
                }
            }
        });
        app.watch_rx = Some(wrx);
        None
    } else {
        None
    };

    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // SIGTERM handler: restore terminal before process exits to prevent corruption
    ctrlc::set_handler(move || {
        let _ = crossterm::terminal::disable_raw_mode();
        let _ = crossterm::execute!(
            std::io::stdout(),
            crossterm::terminal::LeaveAlternateScreen
        );
        std::process::exit(0);
    })
    .ok();

    let tick = Duration::from_millis(66); // 15Hz
    loop {
        terminal.draw(|f| render(f, &app))?;
        // poll watch reload events
        if let Some(ref wrx) = app.watch_rx {
            if wrx.try_recv().is_ok() {
                // drain any pending events
                while wrx.try_recv().is_ok() {}
                // reload file stats
                if let Ok(new_paths) = rp(&app.input_path) {
                    if let Ok((ds, fi, mt)) = load_file_stats(&new_paths) {
                        let cs = read_column_stats(&mt);
                        let tr = fi.row_count;
                        app.dataset = Some(ds);
                        app.file_info = Some(fi);
                        app.row_groups = profile_row_groups(&mt);
                        app.agg_stats = aggregate_column_stats(&cs, tr);
                        app.encoding_analysis = analyze_encodings(&mt);
                        app.compression_analysis = analyze_compression(&mt);
                        app.quality_scores = compute_quality_scores(&app.agg_stats, &app.encoding_analysis, tr);
                        app.repair_suggestions = detect_repair_suggestions(&app.row_groups, &app.agg_stats, &app.encoding_analysis);
                        app.rg_size_recommendation = recommend_row_group_size(&app.row_groups);
                        app.null_patterns = analyze_null_patterns(&app.agg_stats);
                        app.status_msg = "Reloaded (file changed) — q:quit ?:help".into();
                    }
                }
            }
        }
        // spawn full-scan when pending flag is set
        if app.pending_full_scan {
            app.pending_full_scan = false;
            let total_rows = app
                .file_info
                .as_ref()
                .map(|f| f.row_count as u64)
                .unwrap_or(0);
            app.progress = tui::app::ProgressState::Running {
                rows_processed: 0,
                total_rows,
            };
            let path = std::path::PathBuf::from(&app.input_path);
            let bins = app.config.profiling.histogram_bins;
            let (tx, rx) = std::sync::mpsc::channel::<(u64, Vec<parquet_lens_core::ColumnProfileResult>)>();
            app.progress_rx = Some(rx);
            tokio::task::spawn_blocking(move || {
                match profile_columns(&path, None, 65536, bins) {
                    Ok(results) => { let _ = tx.send((total_rows, results)); }
                    Err(_) => { let _ = tx.send((total_rows, Vec::new())); }
                }
            });
        }
        // poll async full-scan progress channel
        let scan_done = if let Some(rx) = &app.progress_rx {
            let mut done = false;
            while let Ok((rows_processed, results)) = rx.try_recv() {
                if let tui::app::ProgressState::Running { total_rows, .. } = app.progress {
                    if rows_processed >= total_rows {
                        app.progress = tui::app::ProgressState::Done;
                        app.full_scan_results = results;
                        done = true;
                    } else {
                        app.progress = tui::app::ProgressState::Running {
                            rows_processed,
                            total_rows,
                        };
                    }
                }
            }
            done
        } else {
            false
        };
        if scan_done {
            app.progress_rx = None;
        }
        // spawn duplicate scan when pending flag is set
        if app.pending_duplicate_scan {
            app.pending_duplicate_scan = false;
            let path = std::path::PathBuf::from(&app.input_path);
            let (tx, rx) = std::sync::mpsc::channel::<Result<parquet_lens_core::DuplicateReport, String>>();
            app.duplicate_rx = Some(rx);
            tokio::task::spawn_blocking(move || {
                let res = detect_duplicates(&path, false).map_err(|e| e.to_string());
                let _ = tx.send(res);
            });
        }
        // poll async duplicate scan channel
        if let Some(rx) = &app.duplicate_rx {
            if let Ok(res) = rx.try_recv() {
                match res {
                    Ok(report) => {
                        app.duplicate_report = Some(report);
                        app.view = tui::app::View::Duplicates;
                    }
                    Err(e) => {
                        app.status_msg = format!("dup detect error: {e}");
                    }
                }
                app.duplicate_rx = None;
            }
        }
        if event::poll(tick)? {
            match event::read()? {
                Event::Key(key) => {
                    handle_key(&mut app, key);
                }
                Event::Mouse(mouse) => {
                    use crossterm::event::MouseEventKind;
                    match mouse.kind {
                        MouseEventKind::ScrollDown => {
                            if app.focus == tui::app::Focus::Sidebar {
                                app.sidebar_down();
                            } else if app.preview_scroll_y + 1 < app.preview_rows.len() {
                                app.preview_scroll_y += 1;
                            }
                        }
                        MouseEventKind::ScrollUp => {
                            if app.focus == tui::app::Focus::Sidebar {
                                app.sidebar_up();
                            } else if app.preview_scroll_y > 0 {
                                app.preview_scroll_y -= 1;
                            }
                        }
                        _ => {}
                    }
                }
                _ => {}
            }
        }
        if app.should_quit {
            break;
        }
    }
    let _ = app.to_session().save();

    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;
    Ok(())
}

fn run_compare(path1: String, path2: String, config: Config) -> anyhow::Result<()> {
    if path1.is_empty() {
        anyhow::bail!("path1 is empty");
    }
    if path2.is_empty() {
        anyhow::bail!("path2 is empty");
    }
    if !is_s3_uri(&path1) && !is_gcs_uri(&path1) && !std::path::Path::new(&path1).exists() {
        anyhow::bail!("path1 not found: {path1}");
    }
    if !is_s3_uri(&path2) && !is_gcs_uri(&path2) && !std::path::Path::new(&path2).exists() {
        anyhow::bail!("path2 not found: {path2}");
    }
    let paths1 = rp(&path1)?;
    let paths2 = rp(&path2)?;
    if paths1.is_empty() {
        anyhow::bail!("No Parquet files found: {path1}");
    }
    if paths2.is_empty() {
        anyhow::bail!("No Parquet files found: {path2}");
    }
    let dataset1 = read_metadata_parallel(&paths1).map_err(|e| anyhow::anyhow!("{e}"))?;
    let dataset2 = read_metadata_parallel(&paths2).map_err(|e| anyhow::anyhow!("{e}"))?;
    let p1_str = paths1[0].path.to_string_lossy().to_string();
    let (file_info, meta) = tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current().block_on(parquet_lens_core::open_parquet_auto(&p1_str, None))
    }).map_err(|e| anyhow::anyhow!("{e}"))?;
    let row_groups = profile_row_groups(&meta);
    let col_stats = read_column_stats(&meta);
    let total_rows = file_info.row_count;
    let agg_stats = aggregate_column_stats(&col_stats, total_rows);
    let encoding_analysis = analyze_encodings(&meta);
    let p2_str = paths2[0].path.to_string_lossy().to_string();
    let (_, meta2) = tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current().block_on(parquet_lens_core::open_parquet_auto(&p2_str, None))
    }).map_err(|e| anyhow::anyhow!("{e}"))?;
    let col_stats2 = read_column_stats(&meta2);
    let agg_stats2 = aggregate_column_stats(&col_stats2, dataset2.total_rows);
    let comparison = compare_datasets(&dataset1, &dataset2, &agg_stats, &agg_stats2);
    let quality_scores = compute_quality_scores(&agg_stats, &encoding_analysis, total_rows);
    let mut app = App::new(path1, config);
    app.dataset = Some(dataset1);
    app.file_info = Some(file_info);
    app.row_groups = row_groups;
    app.agg_stats = agg_stats;
    app.encoding_analysis = encoding_analysis;
    app.quality_scores = quality_scores;
    app.comparison = Some(comparison);
    app.view = View::Compare;
    app.status_msg = "Compare — q:quit ?:help".into();
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    let tick = Duration::from_millis(66);
    loop {
        terminal.draw(|f| render(f, &app))?;
        if event::poll(tick)? {
            if let Event::Key(key) = event::read()? {
                handle_key(&mut app, key);
            }
        }
        if app.should_quit {
            break;
        }
    }
    let _ = app.to_session().save();
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;
    Ok(())
}

fn run_summary(
    input_path: String,
    save: bool,
    format: &str,
    json_out: bool,
    sample_pct: Option<f64>,
    sample_seed: Option<u64>,
    config: &Config,
) -> anyhow::Result<()> {
    let paths = rp(&input_path)?;
    if paths.is_empty() {
        anyhow::bail!("No Parquet files found: {input_path}");
    }
    let (dataset, _, meta) = load_file_stats(&paths)?;
    let total_rows = dataset.total_rows;
    let col_stats = if let Some(pct) = sample_pct {
        let cfg = SampleConfig { percentage: pct, no_extrapolation: false, seed: sample_seed };
        match sample_row_groups(&paths[0].path, &cfg, 20) {
            Ok(sp) => sp.agg_stats,
            Err(e) => {
                eprintln!("sample error: {e}");
                let cs = read_column_stats(&meta);
                aggregate_column_stats(&cs, total_rows)
            }
        }
    } else {
        let cs = read_column_stats(&meta);
        aggregate_column_stats(&cs, total_rows)
    };
    let agg_stats = col_stats;
    let encodings = analyze_encodings(&meta);
    let quality_scores = compute_quality_scores(&agg_stats, &encodings, total_rows);
    let total_cells = total_rows * dataset.combined_schema.len() as i64;
    let total_nulls: u64 = agg_stats.iter().map(|s| s.total_null_count).sum();
    let quality = summarize_quality(quality_scores, total_cells, total_nulls, dataset.schema_inconsistencies.is_empty(), &agg_stats);
    if json_out {
        println!("{}", serde_json::to_string(&quality)?);
        return Ok(());
    }
    if format == "pretty" {
        const BOLD: &str = "\x1b[1m";
        const RESET: &str = "\x1b[0m";
        const GREEN: &str = "\x1b[32m";
        const YELLOW: &str = "\x1b[33m";
        const RED: &str = "\x1b[31m";
        println!("{}Files:{}           {}", BOLD, RESET, dataset.file_count);
        println!("{}Rows:{}            {}", BOLD, RESET, dataset.total_rows);
        println!(
            "{}Size:{}            {} bytes",
            BOLD, RESET, dataset.total_bytes
        );
        println!(
            "{}Columns:{}         {}",
            BOLD,
            RESET,
            dataset.combined_schema.len()
        );
        let qcolor = if quality.overall_score >= 80 {
            GREEN
        } else if quality.overall_score >= 50 {
            YELLOW
        } else {
            RED
        };
        println!(
            "{}Quality:{}         {}{}/100{}",
            BOLD, RESET, qcolor, quality.overall_score, RESET
        );
        println!(
            "{}Null cells:{}      {}{:.2}%{}",
            BOLD,
            RESET,
            if quality.total_null_cell_pct > 10.0 {
                RED
            } else {
                GREEN
            },
            quality.total_null_cell_pct,
            RESET
        );
        if !quality.worst_columns.is_empty() {
            println!(
                "{}Worst cols:{}      {}{}{}",
                BOLD,
                RESET,
                RED,
                quality.worst_columns.join(", "),
                RESET
            );
        }
    } else {
        print_summary(&dataset, Some(&quality));
    }
    if save {
        let out_dir = std::path::Path::new(&config.export.output_dir);
        std::fs::create_dir_all(out_dir)?;
        let out_path = out_dir.join("summary.json");
        let doc = serde_json::json!({ "dataset": dataset, "quality": quality });
        std::fs::write(&out_path, serde_json::to_string_pretty(&doc)?)?;
        println!("Summary saved to {}", out_path.display());
    }
    Ok(())
}

fn run_export(
    input_path: String,
    format: String,
    columns: Option<Vec<String>>,
    output: Option<String>,
    sample_pct: Option<f64>,
    sample_seed: Option<u64>,
    config: Config,
) -> anyhow::Result<()> {
    let paths = rp(&input_path)?;
    if paths.is_empty() {
        anyhow::bail!("No Parquet files found: {input_path}");
    }
    let (dataset, _, meta) = load_file_stats(&paths)?;
    let row_groups = profile_row_groups(&meta);
    let mut agg_stats = if let Some(pct) = sample_pct {
        let cfg = SampleConfig { percentage: pct, no_extrapolation: false, seed: sample_seed };
        match sample_row_groups(&paths[0].path, &cfg, 20) {
            Ok(sp) => sp.agg_stats,
            Err(e) => {
                eprintln!("sample error: {e}");
                let cs = read_column_stats(&meta);
                aggregate_column_stats(&cs, dataset.total_rows)
            }
        }
    } else {
        let cs = read_column_stats(&meta);
        aggregate_column_stats(&cs, dataset.total_rows)
    };
    let encodings = analyze_encodings(&meta);
    let mut quality_scores = compute_quality_scores(&agg_stats, &encodings, dataset.total_rows);
    // column filtering
    if let Some(ref cols) = columns {
        let col_set: std::collections::HashSet<&str> = cols.iter().map(|s| s.as_str()).collect();
        agg_stats.retain(|s| col_set.contains(s.column_name.as_str()));
        quality_scores.retain(|q| col_set.contains(q.column_name.as_str()));
    }
    let default_name = format!("profile.{format}");
    let out_path: std::path::PathBuf = if let Some(ref o) = output {
        std::path::PathBuf::from(o)
    } else {
        std::path::Path::new(&config.export.output_dir).join(&default_name)
    };
    if let Some(parent) = out_path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    let null_patterns = analyze_null_patterns(&agg_stats);
    let engine_info = dataset
        .files
        .first()
        .and_then(|f| f.created_by.as_deref())
        .map(identify_engine);
    let schema = dataset
        .combined_schema
        .iter()
        .map(|c| parquet_lens_core::ColumnSchema {
            name: c.name.clone(),
            physical_type: c.physical_type.clone(),
            logical_type: c.logical_type.clone(),
            repetition: c.repetition.clone(),
            max_def_level: c.max_def_level,
            max_rep_level: c.max_rep_level,
        })
        .collect::<Vec<_>>();
    let (_, baseline_regressions) =
        load_baseline_regressions(&paths[0].path, &agg_stats, &quality_scores, &schema);
    match format.as_str() {
        "json" => {
            export_json(
                &out_path,
                &dataset,
                &agg_stats,
                &row_groups,
                &quality_scores,
                &null_patterns,
                engine_info.as_ref(),
                &baseline_regressions,
            )
            .map_err(|e| anyhow::anyhow!("{e}"))?;
            println!("Exported to {}", out_path.display());
        }
        "csv" => {
            export_csv(&out_path, &agg_stats, &quality_scores)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            println!("Exported to {}", out_path.display());
        }
        _ => anyhow::bail!("Unknown format: {format} (use json or csv)"),
    }
    Ok(())
}
