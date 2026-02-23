use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "parquet-lens", version, about = "Parquet file inspector")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Launch interactive TUI dashboard
    Inspect {
        /// File path, directory glob, or s3:// URI
        path: String,
    },
    /// Print headless stats to stdout
    Summary {
        /// File path, directory glob, or s3:// URI
        path: String,
    },
    /// Compare two datasets side-by-side
    Compare {
        path1: String,
        path2: String,
    },
    /// Export profile to JSON or CSV
    Export {
        /// File path, directory glob, or s3:// URI
        path: String,
        #[arg(long, default_value = "json")]
        format: String,
        #[arg(long, value_delimiter = ',')]
        columns: Option<Vec<String>>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Inspect { path } => {
            println!("inspect: {path}");
        }
        Commands::Summary { path } => {
            println!("summary: {path}");
        }
        Commands::Compare { path1, path2 } => {
            println!("compare: {path1} vs {path2}");
        }
        Commands::Export { path, format, columns } => {
            println!("export: {path} as {format}");
            if let Some(cols) = columns {
                println!("columns: {}", cols.join(", "));
            }
        }
    }
    Ok(())
}
