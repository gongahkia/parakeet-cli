mod tui;

use clap::{Parser, Subcommand};
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{backend::CrosstermBackend, Terminal};
use std::{io, time::Duration};
use tui::app::App;
use tui::events::handle_key;
use tui::ui::render;
use parquet_lens_core::{
    resolve_paths, read_metadata_parallel, open_parquet_file,
    profile_row_groups, read_column_stats, aggregate_column_stats,
    analyze_encodings, analyze_compression, score_column,
    summarize_quality, print_summary, export_json, export_csv,
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
    Inspect { path: String },
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
        Commands::Inspect { path } => run_tui(path, config)?,
        Commands::Summary { path } => run_summary(path)?,
        Commands::Compare { path1, path2 } => println!("compare: {path1} vs {path2}"),
        Commands::Export { path, format, columns } => run_export(path, format, columns)?,
    }
    Ok(())
}

fn run_tui(input_path: String, config: Config) -> anyhow::Result<()> {
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
    app.dataset = Some(dataset);
    app.file_info = Some(file_info);
    app.row_groups = row_groups;
    app.agg_stats = agg_stats;
    app.encoding_analysis = encoding_analysis;
    app.compression_analysis = compression_analysis;
    app.quality_scores = quality_scores;
    app.status_msg = "Ready — q:quit ?:help".into();

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
