mod tui;

use clap::{Parser, Subcommand};
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{backend::CrosstermBackend, Terminal};
use std::{io, time::Duration};
use tui::app::{App, View};
use tui::events::handle_key;
use tui::session::Session;
use tui::ui::render;
use parquet_lens_core::{
    resolve_paths, read_metadata_parallel, open_parquet_file, // resolve_paths used in rp() helper
    profile_row_groups, read_column_stats, aggregate_column_stats,
    analyze_encodings, analyze_compression, score_column,
    summarize_quality, print_summary, export_json, export_csv,
    sample_row_groups, SampleConfig,
    detect_repair_suggestions, profile_timeseries, profile_nested_columns,
    identify_engine, load_baseline_regressions, analyze_null_patterns,
    compare_datasets, profile_columns, detect_duplicates,
    is_s3_uri, read_s3_parquet_metadata,
    is_gcs_uri, read_gcs_parquet_metadata,
    recommend_row_group_size,
    ParquetFilePath, AggregatedColumnStats, EncodingAnalysis, QualityScore,
    DatasetProfile, ParquetFileInfo,
};
use parquet::file::metadata::ParquetMetaData;

fn parse_sample_pct(s: &str) -> Result<f64, String> { // validate sample % at CLI parse time
    let v: f64 = s.parse().map_err(|_| format!("not a float: {s}"))?;
    if v > 0.0 && v <= 100.0 { Ok(v) } else { Err(format!("sample must be in (0.0, 100.0], got {v}")) }
}

/// block_in_place wrapper to call async resolve_paths from sync context
fn rp(input: &str) -> anyhow::Result<Vec<ParquetFilePath>> {
    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(resolve_paths(input)))
        .map_err(|e| anyhow::anyhow!("{e}"))
}

fn compute_quality_scores(agg_stats: &[AggregatedColumnStats], encodings: &[EncodingAnalysis], total_rows: i64) -> Vec<QualityScore> {
    agg_stats.iter().map(|agg| {
        let is_plain = encodings.iter().find(|e| e.column_name == agg.column_name).map(|e| e.is_plain_only).unwrap_or(false);
        score_column(&agg.column_name, agg.null_percentage, agg.total_distinct_count_estimate, total_rows, is_plain)
    }).collect()
}

fn load_file_stats(paths: &[ParquetFilePath]) -> anyhow::Result<(DatasetProfile, ParquetFileInfo, ParquetMetaData)> {
    let dataset = read_metadata_parallel(paths).map_err(|e| anyhow::anyhow!("{e}"))?;
    let (file_info, meta) = open_parquet_file(&paths[0].path).map_err(|e| anyhow::anyhow!("{e}"))?;
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
        #[arg(long)] watch: bool,
        #[arg(long)] no_sample_extrapolation: bool,
        #[arg(long)] save_baseline: bool,
    },
    Summary { path: String, #[arg(long)] save: bool },
    Compare { path1: String, path2: String },
    Export {
        path: String,
        #[arg(long, default_value = "json")] format: String,
        #[arg(long, value_delimiter = ',')] columns: Option<Vec<String>>,
        #[arg(long)] output: Option<String>,
    },
    Duplicates { path: String },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let config = Config::load().unwrap_or_default();
    match cli.command {
        Commands::Inspect { path, sample, watch, no_sample_extrapolation, save_baseline } => {
            if watch { eprintln!("--watch: not yet implemented"); }
            run_tui(path, config, sample, no_sample_extrapolation, save_baseline)?
        }
        Commands::Summary { path, save } => run_summary(path, save, &config)?,
        Commands::Compare { path1, path2 } => run_compare(path1, path2, config)?,
        Commands::Export { path, format, columns, output } => run_export(path, format, columns, output, config)?,
        Commands::Duplicates { path } => run_duplicates(path)?,
    }
    Ok(())
}

fn run_duplicates(input_path: String) -> anyhow::Result<()> {
    let report = detect_duplicates(std::path::Path::new(&input_path))
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    println!("{:<24} {}", "total_rows:", report.total_rows);
    println!("{:<24} {}", "estimated_duplicates:", report.estimated_duplicates);
    println!("{:<24} {:.2}%", "estimated_duplicate_pct:", report.estimated_duplicate_pct);
    Ok(())
}

fn run_tui(input_path: String, config: Config, sample_pct: Option<f64>, no_sample_extrapolation: bool, save_baseline: bool) -> anyhow::Result<()> {
    let paths = rp(&input_path)?;
    if paths.is_empty() { anyhow::bail!("No Parquet files found: {input_path}"); }
    let dataset = read_metadata_parallel(&paths).map_err(|e| anyhow::anyhow!("{e}"))?;
    // task 18/19: handle S3/GCS metadata via specialized readers
    let p0_str = paths[0].path.to_string_lossy();
    let (file_info, meta) = if is_s3_uri(&p0_str) {
        let meta = tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(
            read_s3_parquet_metadata(&p0_str, config.s3.endpoint_url.as_deref())
        )).map_err(|e| anyhow::anyhow!("{e}"))?;
        let fi = parquet_lens_core::ParquetFileInfo {
            path: paths[0].path.clone(), file_size: 0, row_count: meta.file_metadata().num_rows(),
            row_group_count: meta.num_row_groups(), created_by: meta.file_metadata().created_by().map(|s| s.to_owned()),
            parquet_version: meta.file_metadata().version(), key_value_metadata: Vec::new(), schema_fields: Vec::new(),
        };
        (fi, meta)
    } else if is_gcs_uri(&p0_str) {
        let meta = tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(
            read_gcs_parquet_metadata(&p0_str)
        )).map_err(|e| anyhow::anyhow!("{e}"))?;
        let fi = parquet_lens_core::ParquetFileInfo {
            path: paths[0].path.clone(), file_size: 0, row_count: meta.file_metadata().num_rows(),
            row_group_count: meta.num_row_groups(), created_by: meta.file_metadata().created_by().map(|s| s.to_owned()),
            parquet_version: meta.file_metadata().version(), key_value_metadata: Vec::new(), schema_fields: Vec::new(),
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

    let mut app = App::new(input_path, config);
    if let Some(s) = Session::load() { app.restore_from_session(&s); }
    app.dataset = Some(dataset.clone());
    app.file_info = Some(file_info);
    app.row_groups = row_groups;
    app.agg_stats = agg_stats;
    app.encoding_analysis = encoding_analysis;
    app.compression_analysis = compression_analysis;
    app.quality_scores = quality_scores;

    // repair suggestions
    app.repair_suggestions = detect_repair_suggestions(&app.row_groups, &app.agg_stats, &app.encoding_analysis);
    app.rg_size_recommendation = recommend_row_group_size(&app.row_groups);

    // time-series profiling — detect timestamp/date columns from schema
    let ts_cols: Vec<String> = dataset.combined_schema.iter()
        .filter(|c| c.logical_type.as_deref().map(|t| t.contains("Timestamp") || t.contains("Date")).unwrap_or(false))
        .map(|c| c.name.clone())
        .collect();
    if !ts_cols.is_empty() {
        match profile_timeseries(&paths[0].path, &ts_cols) {
            Ok(ts) => { app.timeseries_profiles = ts; }
            Err(e) => { app.status_msg = format!("timeseries error: {e}"); }
        }
    }

    // nested column profiling
    if let Ok(np) = profile_nested_columns(&paths[0].path) {
        app.nested_profiles = np;
    }

    // engine identification from created_by
    if let Some(created_by) = app.file_info.as_ref().and_then(|fi| fi.created_by.as_deref()) {
        app.engine_info = Some(identify_engine(created_by));
    }

    // baseline diff
    {
        let schema = app.columns().to_vec();
        let (base, regressions) = load_baseline_regressions(&paths[0].path, &app.agg_stats, &app.quality_scores, &schema);
        app.baseline_captured_at = base.as_ref().map(|b| b.captured_at);
        app.has_baseline = base.is_some();
        app.baseline_regressions = regressions;
        if save_baseline {
            let new_base = parquet_lens_core::BaselineProfile::new(&app.input_path, schema, app.agg_stats.clone(), app.quality_scores.clone());
            match new_base.save() {
                Ok(_) => { app.status_msg = "baseline saved (--save-baseline)".into(); app.has_baseline = true; }
                Err(e) => { app.status_msg = format!("--save-baseline failed: {e}"); }
            }
        }
    }

    // pre-compute null patterns
    app.null_patterns = analyze_null_patterns(&app.agg_stats);

    if let Some(pct) = sample_pct {
        let cfg = SampleConfig { percentage: pct, no_extrapolation: no_sample_extrapolation };
        match sample_row_groups(&paths[0].path, &cfg, 20) {
            Ok(sp) => {
                app.agg_stats = sp.agg_stats;
                app.row_groups = sp.row_groups;
                app.full_scan_results = sp.profile_results;
                app.sample_note = Some(sp.confidence_note.clone());
                app.status_msg = format!("Sampled — {} | q:quit ?:help", sp.confidence_note);
            }
            Err(e) => { app.status_msg = format!("Sample error: {e}"); }
        }
    } else {
        app.status_msg = "Ready — q:quit ?:help".into();
    }

    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let tick = Duration::from_millis(66); // 15Hz
    loop {
        terminal.draw(|f| render(f, &app))?;
        // spawn full-scan when pending flag is set
        if app.pending_full_scan {
            app.pending_full_scan = false;
            let total_rows = app.file_info.as_ref().map(|f| f.row_count as u64).unwrap_or(0);
            app.progress = tui::app::ProgressState::Running { rows_processed: 0, total_rows };
            let path = std::path::PathBuf::from(&app.input_path);
            let bins = app.config.profiling.histogram_bins;
            let (tx, rx) = std::sync::mpsc::channel::<u64>();
            app.progress_rx = Some(rx);
            tokio::task::spawn_blocking(move || {
                let _ = profile_columns(&path, None, 65536, bins);
                let _ = tx.send(total_rows); // signal completion
            });
        }
        // poll async full-scan progress channel
        let scan_done = if let Some(rx) = &app.progress_rx {
            let mut done = false;
            while let Ok(rows_processed) = rx.try_recv() {
                if let tui::app::ProgressState::Running { total_rows, .. } = app.progress {
                    if rows_processed >= total_rows {
                        app.progress = tui::app::ProgressState::Done;
                        done = true;
                    } else {
                        app.progress = tui::app::ProgressState::Running { rows_processed, total_rows };
                    }
                }
            }
            done
        } else { false };
        if scan_done { app.progress_rx = None; }
        if event::poll(tick)? {
            match event::read()? {
                Event::Key(key) => { handle_key(&mut app, key); }
                Event::Mouse(mouse) => {
                    use crossterm::event::{MouseEvent, MouseEventKind};
                    match mouse.kind {
                        MouseEventKind::ScrollDown => {
                            if app.focus == tui::app::Focus::Sidebar { app.sidebar_down(); }
                            else if app.preview_scroll_y + 1 < app.preview_rows.len() { app.preview_scroll_y += 1; }
                        }
                        MouseEventKind::ScrollUp => {
                            if app.focus == tui::app::Focus::Sidebar { app.sidebar_up(); }
                            else if app.preview_scroll_y > 0 { app.preview_scroll_y -= 1; }
                        }
                        _ => {}
                    }
                }
                _ => {}
            }
        }
        if app.should_quit { break; }
    }
    let _ = app.to_session().save();

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen, DisableMouseCapture)?;
    terminal.show_cursor()?;
    Ok(())
}

fn run_compare(path1: String, path2: String, config: Config) -> anyhow::Result<()> {
    if path1.is_empty() { anyhow::bail!("path1 is empty"); }
    if path2.is_empty() { anyhow::bail!("path2 is empty"); }
    if !std::path::Path::new(&path1).exists() { anyhow::bail!("path1 not found: {path1}"); }
    if !std::path::Path::new(&path2).exists() { anyhow::bail!("path2 not found: {path2}"); }
    let paths1 = rp(&path1)?;
    let paths2 = rp(&path2)?;
    if paths1.is_empty() { anyhow::bail!("No Parquet files found: {path1}"); }
    if paths2.is_empty() { anyhow::bail!("No Parquet files found: {path2}"); }
    let dataset1 = read_metadata_parallel(&paths1).map_err(|e| anyhow::anyhow!("{e}"))?;
    let dataset2 = read_metadata_parallel(&paths2).map_err(|e| anyhow::anyhow!("{e}"))?;
    let (file_info, meta) = open_parquet_file(&paths1[0].path).map_err(|e| anyhow::anyhow!("{e}"))?;
    let row_groups = profile_row_groups(&meta);
    let col_stats = read_column_stats(&meta);
    let total_rows = file_info.row_count;
    let agg_stats = aggregate_column_stats(&col_stats, total_rows);
    let encoding_analysis = analyze_encodings(&meta);
    let (_, meta2) = open_parquet_file(&paths2[0].path).map_err(|e| anyhow::anyhow!("{e}"))?;
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
            if let Event::Key(key) = event::read()? { handle_key(&mut app, key); }
        }
        if app.should_quit { break; }
    }
    let _ = app.to_session().save();
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen, DisableMouseCapture)?;
    terminal.show_cursor()?;
    Ok(())
}

fn run_summary(input_path: String, save: bool, config: &Config) -> anyhow::Result<()> {
    let paths = rp(&input_path)?;
    if paths.is_empty() { anyhow::bail!("No Parquet files found: {input_path}"); }
    let (dataset, _, meta) = load_file_stats(&paths)?;
    let col_stats = read_column_stats(&meta);
    let total_rows = dataset.total_rows;
    let agg_stats = aggregate_column_stats(&col_stats, total_rows);
    let encodings = analyze_encodings(&meta);
    let quality_scores = compute_quality_scores(&agg_stats, &encodings, total_rows);
    let total_cells = total_rows * dataset.combined_schema.len() as i64;
    let total_nulls: u64 = agg_stats.iter().map(|s| s.total_null_count).sum();
    let quality = summarize_quality(quality_scores, total_cells, total_nulls, true);
    print_summary(&dataset, Some(&quality));
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

fn run_export(input_path: String, format: String, columns: Option<Vec<String>>, output: Option<String>, config: Config) -> anyhow::Result<()> {
    let paths = rp(&input_path)?;
    if paths.is_empty() { anyhow::bail!("No Parquet files found: {input_path}"); }
    let (dataset, _, meta) = load_file_stats(&paths)?;
    let col_stats = read_column_stats(&meta);
    let row_groups = profile_row_groups(&meta);
    let mut agg_stats = aggregate_column_stats(&col_stats, dataset.total_rows);
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
        if !parent.as_os_str().is_empty() { std::fs::create_dir_all(parent)?; }
    }
    let null_patterns = analyze_null_patterns(&agg_stats);
    let engine_info = dataset.files.first()
        .and_then(|f| f.created_by.as_deref())
        .map(identify_engine);
    let schema = dataset.combined_schema.iter().map(|c| parquet_lens_core::ColumnSchema {
        name: c.name.clone(), physical_type: c.physical_type.clone(),
        logical_type: c.logical_type.clone(), repetition: c.repetition.clone(),
        max_def_level: c.max_def_level, max_rep_level: c.max_rep_level,
    }).collect::<Vec<_>>();
    let (_, baseline_regressions) = load_baseline_regressions(&paths[0].path, &agg_stats, &quality_scores, &schema);
    match format.as_str() {
        "json" => {
            export_json(&out_path, &dataset, &agg_stats, &row_groups, &quality_scores, &null_patterns, engine_info.as_ref(), &baseline_regressions).map_err(|e| anyhow::anyhow!("{e}"))?;
            println!("Exported to {}", out_path.display());
        }
        "csv" => {
            export_csv(&out_path, &agg_stats, &quality_scores).map_err(|e| anyhow::anyhow!("{e}"))?;
            println!("Exported to {}", out_path.display());
        }
        _ => anyhow::bail!("Unknown format: {format} (use json or csv)"),
    }
    Ok(())
}
