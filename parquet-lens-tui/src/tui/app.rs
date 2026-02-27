use crate::tui::session::Session;
use crate::tui::theme::Theme;
use parquet_lens_common::Config;
use parquet_lens_core::{
    AggregatedColumnStats, BaselineRegression, ColumnProfileResult, ColumnSchema,
    CompressionAnalysis, DatasetComparison, DatasetProfile, DuplicateReport, EncodingAnalysis,
    EngineInfo, FilterResult, NestedColumnProfile, NullPatternGroup, ParquetFileInfo,
    PartitionInfo, QualityScore, RepairSuggestion, RowGroupProfile, RowGroupSizeRecommendation,
    TimeSeriesProfile,
};

#[derive(Debug, Clone, PartialEq)]
pub enum SidebarSort {
    Name,
    NullRate,
    Cardinality,
    Size,
    Quality,
}

#[derive(Debug, Clone, PartialEq)]
pub enum View {
    FileOverview,
    Schema,
    ColumnDetail(usize),
    RowGroups,
    NullHeatmap,
    DataPreview,
    Help,
    ConfirmFullScan,
    Compare,
    ColumnSizeBreakdown,
    FileList,
    FilterInput,
    Repair,
    TimeSeries,
    Nested,
    NullPatterns,
    Baseline,
    Duplicates,
    Partitions,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProfilingMode {
    Metadata,
    FullScan,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Focus {
    Sidebar,
    Main,
    Overlay,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProgressState {
    Idle,
    Running {
        rows_processed: u64,
        total_rows: u64,
    },
    Done,
    #[allow(dead_code)]
    Cancelled,
}

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
    pub progress_rx: Option<std::sync::mpsc::Receiver<(u64, Vec<ColumnProfileResult>)>>, // async full-scan progress
    pub pending_full_scan: bool, // triggers spawn_blocking for full-scan
    pub status_msg: String,
    pub should_quit: bool,
    pub config: Config,
    pub comparison: Option<DatasetComparison>,
    #[allow(dead_code)]
    pub compare_sidebar_col: usize,
    pub sidebar_search: String,
    pub sidebar_searching: bool,
    pub sidebar_sort: SidebarSort,
    pub sidebar_sort_asc: bool,
    pub bookmarks: Vec<String>,
    pub show_bookmarks_only: bool,
    pub show_null_hotspot_only: bool, // 'I' keybind: filter columns with null_rate > 5%
    pub filter_input: String,
    pub filter_active: bool,
    pub filter_result: Option<FilterResult>,
    pub sample_note: Option<String>,
    pub repair_suggestions: Vec<RepairSuggestion>,
    pub rg_size_recommendation: Option<RowGroupSizeRecommendation>,
    pub timeseries_profiles: Vec<TimeSeriesProfile>,
    pub nested_profiles: Vec<NestedColumnProfile>,
    pub engine_info: Option<EngineInfo>,
    pub null_patterns: Vec<NullPatternGroup>,
    pub baseline_regressions: Vec<BaselineRegression>,
    pub has_baseline: bool,
    pub baseline_captured_at: Option<u64>, // unix secs
    pub duplicate_report: Option<DuplicateReport>,
    pub partition_infos: Vec<PartitionInfo>,
    pub theme: Theme,
    pub help_scroll: usize, // scroll offset for help keybind table
    pub watch_rx: Option<std::sync::mpsc::Receiver<()>>, // reload events from filesystem watcher
    pub sidebar_width: u16, // runtime-adjustable sidebar width, clamped 15..=60
    pub sidebar_visible: bool, // backtick toggle; also auto-hidden when terminal < 80 cols
    pub pending_duplicate_scan: bool,
    pub duplicate_rx: Option<std::sync::mpsc::Receiver<Result<parquet_lens_core::DuplicateReport, String>>>,
}

impl App {
    pub fn new(input_path: String, config: Config) -> Self {
        let sidebar_width = config.display.sidebar_width.unwrap_or(30);
        Self {
            input_path,
            dataset: None,
            file_info: None,
            row_groups: Vec::new(),
            agg_stats: Vec::new(),
            encoding_analysis: Vec::new(),
            compression_analysis: Vec::new(),
            quality_scores: Vec::new(),
            full_scan_results: Vec::new(),
            preview_rows: Vec::new(),
            preview_headers: Vec::new(),
            view: View::FileOverview,
            focus: Focus::Sidebar,
            profiling_mode: ProfilingMode::Metadata,
            sidebar_selected: 0,
            rg_sort_col: 0,
            rg_sort_asc: true,
            preview_scroll_x: 0,
            preview_scroll_y: 0,
            progress: ProgressState::Idle,
            progress_rx: None,
            pending_full_scan: false,
            status_msg: String::from("Loading..."),
            should_quit: false,
            theme: Theme::from_name(&config.display.theme),
            config,
            comparison: None,
            compare_sidebar_col: 0,
            sidebar_search: String::new(),
            sidebar_searching: false,
            sidebar_sort: SidebarSort::Name,
            sidebar_sort_asc: true,
            bookmarks: Vec::new(),
            show_bookmarks_only: false,
            show_null_hotspot_only: false,
            filter_input: String::new(),
            filter_active: false,
            filter_result: None,
            sample_note: None,
            repair_suggestions: Vec::new(),
            rg_size_recommendation: None,
            timeseries_profiles: Vec::new(),
            nested_profiles: Vec::new(),
            engine_info: None,
            null_patterns: Vec::new(),
            baseline_regressions: Vec::new(),
            has_baseline: false,
            baseline_captured_at: None,
            duplicate_report: None,
            partition_infos: Vec::new(),
            help_scroll: 0,
            watch_rx: None,
            sidebar_width,
            sidebar_visible: true,
            pending_duplicate_scan: false,
            duplicate_rx: None,
        }
    }
    pub fn columns(&self) -> &[ColumnSchema] {
        self.dataset
            .as_ref()
            .map(|d| d.combined_schema.as_slice())
            .unwrap_or(&[])
    }
    pub fn column_count(&self) -> usize {
        self.dataset
            .as_ref()
            .map(|d| d.combined_schema.len())
            .unwrap_or(0)
    }
    pub fn sidebar_down(&mut self) {
        let max = self.column_count().saturating_sub(1);
        if self.sidebar_selected < max {
            self.sidebar_selected += 1;
        }
    }
    pub fn sidebar_up(&mut self) {
        if self.sidebar_selected > 0 {
            self.sidebar_selected -= 1;
        }
    }
    pub fn cycle_focus(&mut self) {
        self.focus = match self.focus {
            Focus::Sidebar => Focus::Main,
            Focus::Main => Focus::Sidebar,
            Focus::Overlay => Focus::Sidebar,
        };
    }
    pub fn filtered_column_indices(&self) -> Vec<usize> {
        let cols = self.columns();
        let mut indices: Vec<usize> = (0..cols.len())
            .filter(|&i| {
                let col = &cols[i];
                let search_match = self.sidebar_search.is_empty()
                    || col
                        .name
                        .to_lowercase()
                        .contains(&self.sidebar_search.to_lowercase());
                let bookmark_match =
                    !self.show_bookmarks_only || self.bookmarks.contains(&col.name);
                let hotspot_match = !self.show_null_hotspot_only
                    || self
                        .agg_stats
                        .iter()
                        .find(|s| s.column_name == col.name)
                        .map(|s| s.null_percentage > 5.0)
                        .unwrap_or(false);
                search_match && bookmark_match && hotspot_match
            })
            .collect();
        // precompute lookup maps to avoid O(n) scan per sort comparison
        let agg_map: std::collections::HashMap<&str, &AggregatedColumnStats> = self
            .agg_stats
            .iter()
            .map(|s| (s.column_name.as_str(), s))
            .collect();
        let qual_map: std::collections::HashMap<&str, &QualityScore> = self
            .quality_scores
            .iter()
            .map(|s| (s.column_name.as_str(), s))
            .collect();
        indices.sort_by(|&a, &b| {
            let ca = &cols[a];
            let cb = &cols[b];
            let ord = match self.sidebar_sort {
                SidebarSort::Name => ca.name.cmp(&cb.name),
                SidebarSort::NullRate => {
                    let na = agg_map
                        .get(ca.name.as_str())
                        .map(|s| (s.null_percentage * 1000.0) as i64)
                        .unwrap_or(0);
                    let nb = agg_map
                        .get(cb.name.as_str())
                        .map(|s| (s.null_percentage * 1000.0) as i64)
                        .unwrap_or(0);
                    na.cmp(&nb)
                }
                SidebarSort::Quality => {
                    let qa = qual_map
                        .get(ca.name.as_str())
                        .map(|s| s.score)
                        .unwrap_or(100);
                    let qb = qual_map
                        .get(cb.name.as_str())
                        .map(|s| s.score)
                        .unwrap_or(100);
                    qa.cmp(&qb)
                }
                SidebarSort::Size => {
                    let sa = agg_map
                        .get(ca.name.as_str())
                        .map(|s| s.total_data_page_size)
                        .unwrap_or(0);
                    let sb = agg_map
                        .get(cb.name.as_str())
                        .map(|s| s.total_data_page_size)
                        .unwrap_or(0);
                    sa.cmp(&sb)
                }
                SidebarSort::Cardinality => {
                    let ca2 = agg_map
                        .get(ca.name.as_str())
                        .and_then(|s| s.total_distinct_count_estimate)
                        .unwrap_or(0);
                    let cb2 = agg_map
                        .get(cb.name.as_str())
                        .and_then(|s| s.total_distinct_count_estimate)
                        .unwrap_or(0);
                    ca2.cmp(&cb2)
                }
            };
            if self.sidebar_sort_asc {
                ord
            } else {
                ord.reverse()
            }
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

    pub fn to_session(&self) -> Session {
        let view = match &self.view {
            View::FileOverview => "overview",
            View::Schema => "schema",
            View::ColumnDetail(_) => "column_detail",
            View::RowGroups => "row_groups",
            View::NullHeatmap => "null_heatmap",
            View::DataPreview => "data_preview",
            View::Compare => "compare",
            View::ColumnSizeBreakdown => "col_size",
            View::FileList => "file_list",
            View::FilterInput => "filter_input",
            View::Repair => "repair",
            View::TimeSeries => "timeseries",
            View::Nested => "nested",
            View::NullPatterns => "null_patterns",
            View::Baseline => "baseline",
            View::Duplicates => "duplicates",
            View::Partitions => "partitions",
            _ => "overview",
        };
        let mode = match self.profiling_mode {
            ProfilingMode::Metadata => "metadata",
            ProfilingMode::FullScan => "full_scan",
        };
        let sort_str = match self.sidebar_sort {
            SidebarSort::Name => "name",
            SidebarSort::NullRate => "null_rate",
            SidebarSort::Cardinality => "cardinality",
            SidebarSort::Size => "size",
            SidebarSort::Quality => "quality",
        };
        Session {
            input_path: self.input_path.clone(),
            sidebar_selected: self.sidebar_selected,
            view: view.into(),
            profiling_mode: mode.into(),
            bookmarks: self.bookmarks.clone(),
            sidebar_sort: sort_str.into(),
            sidebar_sort_asc: self.sidebar_sort_asc,
            show_bookmarks_only: self.show_bookmarks_only,
            sidebar_width: self.sidebar_width,
        }
    }

    pub fn restore_from_session(&mut self, s: &Session) {
        if s.input_path != self.input_path {
            return;
        }
        self.sidebar_selected = s.sidebar_selected;
        self.view = match s.view.as_str() {
            "schema" => View::Schema,
            "column_detail" => View::ColumnDetail(s.sidebar_selected),
            "row_groups" => View::RowGroups,
            "null_heatmap" => View::NullHeatmap,
            "data_preview" => View::DataPreview,
            "compare" => View::Compare,
            "col_size" => View::ColumnSizeBreakdown,
            "file_list" => View::FileList,
            "repair" => View::Repair,
            "timeseries" => View::TimeSeries,
            "nested" => View::Nested,
            "null_patterns" => View::NullPatterns,
            "baseline" => View::Baseline,
            "duplicates" => View::Duplicates,
            "partitions" => View::Partitions,
            _ => View::FileOverview,
        };
        self.profiling_mode = if s.profiling_mode == "full_scan" {
            ProfilingMode::FullScan
        } else {
            ProfilingMode::Metadata
        };
        self.bookmarks = s.bookmarks.clone();
        self.sidebar_sort = match s.sidebar_sort.as_str() {
            "null_rate" => SidebarSort::NullRate,
            "cardinality" => SidebarSort::Cardinality,
            "size" => SidebarSort::Size,
            "quality" => SidebarSort::Quality,
            _ => SidebarSort::Name,
        };
        self.sidebar_sort_asc = s.sidebar_sort_asc;
        self.show_bookmarks_only = s.show_bookmarks_only;
        self.sidebar_width = s.sidebar_width;
    }

    pub fn cycle_profiling_mode(&mut self) {
        match self.profiling_mode {
            ProfilingMode::Metadata => {
                let large = self
                    .file_info
                    .as_ref()
                    .map(|f| f.file_size > 1024 * 1024 * 1024)
                    .unwrap_or(false);
                if large {
                    self.view = View::ConfirmFullScan;
                    self.focus = Focus::Overlay;
                } else {
                    self.profiling_mode = ProfilingMode::FullScan;
                    self.pending_full_scan = true;
                }
            }
            ProfilingMode::FullScan => {
                self.profiling_mode = ProfilingMode::Metadata;
            }
        }
    }
}
