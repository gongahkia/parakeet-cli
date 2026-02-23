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
    resolve_paths, read_metadata_parallel, open_parquet_file,
    profile_row_groups, read_column_stats, aggregate_column_stats,
    analyze_encodings, analyze_compression, score_column,
    summarize_quality, print_summary, export_json, export_csv,
    sample_row_groups, SampleConfig,
    detect_repair_suggestions, profile_timeseries, profile_nested_columns,
    identify_engine, load_baseline_regressions,
    compare_datasets,
};
use parquet_lens_common::Config;

#[derive(Parser)]
#[command(name = "parquet-lens", version, about = "Parquet file inspector")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Inspect { path: String, #[arg(long)] sample: Option<f64> },
    Summary { path: String },
    Compare { path1: String, path2: String },
    Export {
        path: String,
        #[arg(long, default_value = "json")] format: String,
        #[arg(long, value_delimiter = ',')] columns: Option<Vec<String>>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let config = Config::load().unwrap_or_default();
    match cli.command {
        Commands::Inspect { path, sample } => run_tui(path, config, sample)?,
        Commands::Summary { path } => run_summary(path)?,
        Commands::Compare { path1, path2 } => run_compare(path1, path2, config)?,
        Commands::Export { path, format, columns } => run_export(path, format, columns)?,
    }
    Ok(())
}

fn run_tui(input_path: String, config: Config, sample_pct: Option<f64>) -> anyhow::Result<()> {
    let paths = resolve_paths(&input_path).map_err(|e| anyhow::anyhow!("{e}"))?;
    if paths.is_empty() { anyhow::bail!("No Parquet files found: {input_path}"); }
    let dataset = read_metadata_parallel(&paths).map_err(|e| anyhow::anyhow!("{e}"))?;
    let (file_info, meta) = open_parquet_file(&paths[0].path).map_err(|e| anyhow::anyhow!("{e}"))?;
    let row_groups = profile_row_groups(&meta);
    let col_stats = read_column_stats(&meta);
    let total_rows = file_info.row_count;
    let agg_stats = aggregate_column_stats(&col_stats, total_rows);
    let encoding_analysis = analyze_encodings(&meta);
    let compression_analysis = analyze_compression(&meta);
    let quality_scores: Vec<_> = agg_stats.iter().map(|agg| {
        let is_plain = encoding_analysis.iter().find(|e| e.column_name == agg.column_name).map(|e| e.is_plain_only).unwrap_or(false);
        score_column(&agg.column_name, agg.null_percentage, agg.total_distinct_count_estimate, total_rows, is_plain)
    }).collect();

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

    // time-series profiling — detect timestamp/date columns from schema
    let ts_cols: Vec<String> = dataset.combined_schema.iter()
        .filter(|c| c.logical_type.as_deref().map(|t| t.contains("Timestamp") || t.contains("Date")).unwrap_or(false))
        .map(|c| c.name.clone())
        .collect();
    if !ts_cols.is_empty() {
        if let Ok(ts) = profile_timeseries(&paths[0].path, &ts_cols) {
            app.timeseries_profiles = ts;
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
        app.has_baseline = base.is_some();
        app.baseline_regressions = regressions;
    }

    if let Some(pct) = sample_pct {
        let pct = pct.clamp(1.0, 100.0);
        let sp_path = paths[0].path.to_string_lossy().to_string();
        let cfg = SampleConfig { percentage: pct };
        match sample_row_groups(std::path::Path::new(&sp_path), &cfg, 20) {
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
        if event::poll(tick)? {
            if let Event::Key(key) = event::read()? {
                handle_key(&mut app, key);
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
    let paths1 = resolve_paths(&path1).map_err(|e| anyhow::anyhow!("{e}"))?;
    let paths2 = resolve_paths(&path2).map_err(|e| anyhow::anyhow!("{e}"))?;
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
    let quality_scores: Vec<_> = agg_stats.iter().map(|agg| {
        let is_plain = encoding_analysis.iter().find(|e| e.column_name == agg.column_name).map(|e| e.is_plain_only).unwrap_or(false);
        score_column(&agg.column_name, agg.null_percentage, agg.total_distinct_count_estimate, total_rows, is_plain)
    }).collect();
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

fn run_summary(input_path: String) -> anyhow::Result<()> {
    let paths = resolve_paths(&input_path).map_err(|e| anyhow::anyhow!("{e}"))?;
    if paths.is_empty() { anyhow::bail!("No Parquet files found: {input_path}"); }
    let dataset = read_metadata_parallel(&paths).map_err(|e| anyhow::anyhow!("{e}"))?;
    let (_, meta) = open_parquet_file(&paths[0].path).map_err(|e| anyhow::anyhow!("{e}"))?;
    let col_stats = read_column_stats(&meta);
    let total_rows = dataset.total_rows;
    let agg_stats = aggregate_column_stats(&col_stats, total_rows);
    let encodings = analyze_encodings(&meta);
    let quality_scores: Vec<_> = agg_stats.iter().map(|agg| {
        let is_plain = encodings.iter().find(|e| e.column_name == agg.column_name).map(|e| e.is_plain_only).unwrap_or(false);
        score_column(&agg.column_name, agg.null_percentage, agg.total_distinct_count_estimate, total_rows, is_plain)
    }).collect();
    let total_cells = total_rows * dataset.combined_schema.len() as i64;
    let total_nulls: u64 = agg_stats.iter().map(|s| s.total_null_count).sum();
    let quality = summarize_quality(quality_scores, total_cells, total_nulls, true);
    print_summary(&dataset, Some(&quality));
    Ok(())
}

fn run_export(input_path: String, format: String, _columns: Option<Vec<String>>) -> anyhow::Result<()> {
    let paths = resolve_paths(&input_path).map_err(|e| anyhow::anyhow!("{e}"))?;
    if paths.is_empty() { anyhow::bail!("No Parquet files found: {input_path}"); }
    let dataset = read_metadata_parallel(&paths).map_err(|e| anyhow::anyhow!("{e}"))?;
    let (_, meta) = open_parquet_file(&paths[0].path).map_err(|e| anyhow::anyhow!("{e}"))?;
    let col_stats = read_column_stats(&meta);
    let row_groups = profile_row_groups(&meta);
    let agg_stats = aggregate_column_stats(&col_stats, dataset.total_rows);
    let encodings = analyze_encodings(&meta);
    let quality_scores: Vec<_> = agg_stats.iter().map(|agg| {
        let is_plain = encodings.iter().find(|e| e.column_name == agg.column_name).map(|e| e.is_plain_only).unwrap_or(false);
        score_column(&agg.column_name, agg.null_percentage, agg.total_distinct_count_estimate, dataset.total_rows, is_plain)
    }).collect();
    match format.as_str() {
        "json" => {
            let out = std::path::Path::new("profile.json");
            export_json(out, &dataset, &agg_stats, &row_groups, &quality_scores).map_err(|e| anyhow::anyhow!("{e}"))?;
            println!("Exported to profile.json");
        }
        "csv" => {
            let out = std::path::Path::new("profile.csv");
            export_csv(out, &agg_stats, &quality_scores).map_err(|e| anyhow::anyhow!("{e}"))?;
            println!("Exported to profile.csv");
        }
        _ => anyhow::bail!("Unknown format: {format} (use json or csv)"),
    }
    Ok(())
}
