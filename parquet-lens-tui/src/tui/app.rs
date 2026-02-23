use parquet_lens_core::{
    DatasetProfile, ColumnSchema, ParquetFileInfo, RowGroupProfile,
    AggregatedColumnStats, EncodingAnalysis, CompressionAnalysis,
    QualityScore, ColumnProfileResult, DatasetComparison,
};
use parquet_lens_common::Config;

#[derive(Debug, Clone, PartialEq)]
pub enum SidebarSort { Name, NullRate, Cardinality, Size, Quality }

#[derive(Debug, Clone, PartialEq)]
pub enum View { FileOverview, Schema, ColumnDetail(usize), RowGroups, NullHeatmap, DataPreview, Help, ConfirmFullScan, Compare, ColumnSizeBreakdown, FileList }

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
    pub comparison: Option<DatasetComparison>,
    pub compare_sidebar_col: usize,
    pub sidebar_search: String,
    pub sidebar_searching: bool,
    pub sidebar_sort: SidebarSort,
    pub sidebar_sort_asc: bool,
    pub bookmarks: Vec<String>,
    pub show_bookmarks_only: bool,
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
            comparison: None, compare_sidebar_col: 0,
            sidebar_search: String::new(), sidebar_searching: false,
            sidebar_sort: SidebarSort::Name, sidebar_sort_asc: true,
            bookmarks: Vec::new(), show_bookmarks_only: false,
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
    /// returns column indices after applying search filter, bookmark filter, and sort
    pub fn filtered_column_indices(&self) -> Vec<usize> {
        let cols = self.columns();
        let mut indices: Vec<usize> = (0..cols.len()).filter(|&i| {
            let col = &cols[i];
            let search_match = self.sidebar_search.is_empty() || col.name.to_lowercase().contains(&self.sidebar_search.to_lowercase());
            let bookmark_match = !self.show_bookmarks_only || self.bookmarks.contains(&col.name);
            search_match && bookmark_match
        }).collect();
        indices.sort_by(|&a, &b| {
            let ca = &cols[a]; let cb = &cols[b];
            let ord = match self.sidebar_sort {
                SidebarSort::Name => ca.name.cmp(&cb.name),
                SidebarSort::NullRate => {
                    let na = self.agg_stats.iter().find(|s| s.column_name == ca.name).map(|s| (s.null_percentage * 1000.0) as i64).unwrap_or(0);
                    let nb = self.agg_stats.iter().find(|s| s.column_name == cb.name).map(|s| (s.null_percentage * 1000.0) as i64).unwrap_or(0);
                    na.cmp(&nb)
                }
                SidebarSort::Quality => {
                    let qa = self.quality_scores.iter().find(|s| s.column_name == ca.name).map(|s| s.score).unwrap_or(100);
                    let qb = self.quality_scores.iter().find(|s| s.column_name == cb.name).map(|s| s.score).unwrap_or(100);
                    qa.cmp(&qb)
                }
                SidebarSort::Size => {
                    let sa = self.agg_stats.iter().find(|s| s.column_name == ca.name).map(|s| s.total_data_page_size).unwrap_or(0);
                    let sb = self.agg_stats.iter().find(|s| s.column_name == cb.name).map(|s| s.total_data_page_size).unwrap_or(0);
                    sa.cmp(&sb)
                }
                SidebarSort::Cardinality => {
                    let ca2 = self.agg_stats.iter().find(|s| s.column_name == ca.name).and_then(|s| s.total_distinct_count_estimate).unwrap_or(0);
                    let cb2 = self.agg_stats.iter().find(|s| s.column_name == cb.name).and_then(|s| s.total_distinct_count_estimate).unwrap_or(0);
                    ca2.cmp(&cb2)
                }
            };
            if self.sidebar_sort_asc { ord } else { ord.reverse() }
        });
        indices
    }

    pub fn toggle_bookmark(&mut self) {
        if let Some(&col_idx) = self.filtered_column_indices().get(self.sidebar_selected) {
            let name = self.columns()[col_idx].name.clone();
            if let Some(pos) = self.bookmarks.iter().position(|b| *b == name) {
                self.bookmarks.remove(pos);
            } else {
                self.bookmarks.push(name);
            }
        }
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
