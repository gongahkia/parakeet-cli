use parquet_lens_core::{
    DatasetProfile, ColumnSchema, ParquetFileInfo, RowGroupProfile,
    AggregatedColumnStats, EncodingAnalysis, CompressionAnalysis,
    QualityScore, ColumnProfileResult,
};
use parquet_lens_common::Config;

#[derive(Debug, Clone, PartialEq)]
pub enum View { FileOverview, Schema, ColumnDetail(usize), RowGroups, NullHeatmap, DataPreview, Help, ConfirmFullScan }

#[derive(Debug, Clone, PartialEq)]
pub enum ProfilingMode { Metadata, FullScan }

#[derive(Debug, Clone, PartialEq)]
pub enum Focus { Sidebar, Main, Overlay }

#[derive(Debug, Clone, PartialEq)]
pub enum ProgressState { Idle, Running { rows_processed: u64, total_rows: u64 }, Done, Cancelled }

pub struct App {
    pub input_path: String,
    pub dataset: Option<DatasetProfile>,
    pub file_info: Option<ParquetFileInfo>,
    pub row_groups: Vec<RowGroupProfile>,
    pub agg_stats: Vec<AggregatedColumnStats>,
    pub encoding_analysis: Vec<EncodingAnalysis>,
    pub compression_analysis: Vec<CompressionAnalysis>,
    pub quality_scores: Vec<QualityScore>,
    pub full_scan_results: Vec<ColumnProfileResult>,
    pub preview_rows: Vec<Vec<String>>,
    pub preview_headers: Vec<String>,
    pub view: View,
    pub focus: Focus,
    pub profiling_mode: ProfilingMode,
    pub sidebar_selected: usize,
    pub rg_sort_col: usize,
    pub rg_sort_asc: bool,
    pub preview_scroll_x: usize,
    pub preview_scroll_y: usize,
    pub progress: ProgressState,
    pub status_msg: String,
    pub should_quit: bool,
    pub config: Config,
}

impl App {
    pub fn new(input_path: String, config: Config) -> Self {
        Self {
            input_path, dataset: None, file_info: None,
            row_groups: Vec::new(), agg_stats: Vec::new(),
            encoding_analysis: Vec::new(), compression_analysis: Vec::new(),
            quality_scores: Vec::new(), full_scan_results: Vec::new(),
            preview_rows: Vec::new(), preview_headers: Vec::new(),
            view: View::FileOverview, focus: Focus::Sidebar,
            profiling_mode: ProfilingMode::Metadata,
            sidebar_selected: 0, rg_sort_col: 0, rg_sort_asc: true,
            preview_scroll_x: 0, preview_scroll_y: 0,
            progress: ProgressState::Idle,
            status_msg: String::from("Loading..."),
            should_quit: false, config,
        }
    }
    pub fn columns(&self) -> &[ColumnSchema] {
        self.dataset.as_ref().map(|d| d.combined_schema.as_slice()).unwrap_or(&[])
    }
    pub fn column_count(&self) -> usize {
        self.dataset.as_ref().map(|d| d.combined_schema.len()).unwrap_or(0)
    }
    pub fn sidebar_down(&mut self) {
        let max = self.column_count().saturating_sub(1);
        if self.sidebar_selected < max { self.sidebar_selected += 1; }
    }
    pub fn sidebar_up(&mut self) {
        if self.sidebar_selected > 0 { self.sidebar_selected -= 1; }
    }
    pub fn cycle_focus(&mut self) {
        self.focus = match self.focus {
            Focus::Sidebar => Focus::Main,
            Focus::Main => Focus::Sidebar,
            Focus::Overlay => Focus::Sidebar,
        };
    }
    pub fn cycle_profiling_mode(&mut self) {
        match self.profiling_mode {
            ProfilingMode::Metadata => {
                let large = self.file_info.as_ref().map(|f| f.file_size > 1024*1024*1024).unwrap_or(false);
                if large { self.view = View::ConfirmFullScan; self.focus = Focus::Overlay; }
                else { self.profiling_mode = ProfilingMode::FullScan; }
            }
            ProfilingMode::FullScan => { self.profiling_mode = ProfilingMode::Metadata; }
        }
    }
}
