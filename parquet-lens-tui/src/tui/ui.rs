use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Gauge, List, ListItem, ListState, Paragraph, Row, Table, Wrap},
};
use crate::tui::app::{App, Focus, ProfilingMode, ProgressState, View};
use crate::tui::theme::Theme;

pub fn render(frame: &mut Frame, app: &App) {
    let theme = &app.theme;
    let area = frame.area();
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Min(0), Constraint::Length(1)])
        .split(area);
    render_topbar(frame, app, chunks[0], &theme);
    let mid = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Length(30), Constraint::Min(0)])
        .split(chunks[1]);
    render_sidebar(frame, app, mid[0], &theme);
    render_main(frame, app, mid[1], &theme);
    render_bottombar(frame, app, chunks[2], &theme);
    if app.view == View::Help { render_help(frame, area); }
    if app.view == View::ConfirmFullScan { render_confirm(frame, area); }
    if app.filter_active || app.view == View::FilterInput { render_filter_overlay(frame, app, area); }
    if let ProgressState::Running { rows_processed, total_rows } = &app.progress {
        render_progress(frame, area, *rows_processed, *total_rows, &theme);
    }
}

fn render_topbar(frame: &mut Frame, app: &App, area: Rect, theme: &Theme) {
    let badge = match app.profiling_mode {
        ProfilingMode::Metadata => Span::styled("[META]", Style::default().fg(theme.success)),
        ProfilingMode::FullScan => Span::styled("[SCAN]", Style::default().fg(theme.error)),
    };
    let info = if let Some(ds) = &app.dataset {
        format!(" {} | {} files | {} rows | {}", app.input_path, ds.file_count, ds.total_rows, fmt_bytes(ds.total_bytes))
    } else {
        format!(" {}", app.input_path)
    };
    let line = Line::from(vec![badge, Span::raw(info)]);
    frame.render_widget(Paragraph::new(line).style(Style::default().bg(theme.bg).fg(theme.fg)), area);
}

fn render_sidebar(frame: &mut Frame, app: &App, area: Rect, theme: &Theme) {
    let focused = app.focus == Focus::Sidebar;
    let search_suffix = if app.sidebar_searching { format!("/{}_", app.sidebar_search) } else if !app.sidebar_search.is_empty() { format!("/{}", app.sidebar_search) } else { String::new() };
    let bmark_flag = if app.show_bookmarks_only { " [★]" } else { "" };
    let title = format!("Columns{bmark_flag}{search_suffix}");
    let block = Block::default().borders(Borders::ALL).title(title)
        .border_style(if focused { Style::default().fg(theme.highlight) } else { Style::default() });
    let cols = app.columns();
    let indices = app.filtered_column_indices();
    let items: Vec<ListItem> = indices.iter().map(|&i| {
        let col = &cols[i];
        let icon = type_icon(&col.physical_type);
        let quality = app.quality_scores.iter().find(|s| s.column_name == col.name).map(|s| s.score).unwrap_or(100);
        let qcolor = if quality >= 80 { theme.success } else if quality >= 50 { theme.warning } else { theme.error };
        let bmark = if app.bookmarks.contains(&col.name) { "★" } else { " " };
        ListItem::new(Line::from(vec![
            Span::raw(format!("{bmark}{icon} {:<16}", truncate(&col.name, 16))),
            Span::styled(format!("{:3}%", quality), Style::default().fg(qcolor)),
        ]))
    }).collect();
    let mut state = ListState::default();
    if !items.is_empty() { state.select(Some(app.sidebar_selected.min(items.len().saturating_sub(1)))); }
    let list = List::new(items).block(block).highlight_style(Style::default().add_modifier(Modifier::REVERSED));
    frame.render_stateful_widget(list, area, &mut state);
}

fn render_main(frame: &mut Frame, app: &App, area: Rect, theme: &Theme) {
    match &app.view {
        View::FileOverview | View::ConfirmFullScan | View::Help => render_file_overview(frame, app, area),
        View::Schema => render_schema(frame, app, area, theme),
        View::ColumnDetail(idx) => render_column_detail(frame, app, area, *idx, theme),
        View::RowGroups => render_row_groups(frame, app, area, theme),
        View::NullHeatmap => render_null_heatmap(frame, app, area, theme),
        View::DataPreview => render_data_preview(frame, app, area),
        View::Compare => render_compare(frame, app, area, theme),
        View::ColumnSizeBreakdown => render_col_size_breakdown(frame, app, area),
        View::FileList => render_file_list(frame, app, area),
        View::FilterInput => render_file_overview(frame, app, area),
        View::Repair => render_repair(frame, app, area, theme),
        View::TimeSeries => render_timeseries(frame, app, area, theme),
        View::Nested => render_nested(frame, app, area, theme),
        View::NullPatterns => render_null_patterns(frame, app, area, theme),
        View::Baseline => render_baseline(frame, app, area, theme),
        View::Duplicates => render_duplicates(frame, app, area, theme),
    }
}

fn render_duplicates(frame: &mut Frame, app: &App, area: Rect, theme: &Theme) {
    let Some(report) = &app.duplicate_report else {
        frame.render_widget(Paragraph::new("No duplicate report. Press V to analyze.").block(Block::default().borders(Borders::ALL).title("Duplicate Detection (V)")), area);
        return;
    };
    let color = if report.estimated_duplicate_pct > 5.0 { theme.error } else if report.estimated_duplicate_pct > 1.0 { theme.warning } else { theme.success };
    let lines = vec![
        Line::from(format!("Total rows:            {}", report.total_rows)),
        Line::from(vec![
            Span::raw("Estimated duplicates:  "),
            Span::styled(format!("{}", report.estimated_duplicates), Style::default().fg(color)),
        ]),
        Line::from(vec![
            Span::raw("Estimated dup %:       "),
            Span::styled(format!("{:.2}%", report.estimated_duplicate_pct), Style::default().fg(color)),
        ]),
    ];
    frame.render_widget(Paragraph::new(lines).block(Block::default().borders(Borders::ALL).title("Duplicate Detection (V)")).wrap(Wrap { trim: false }), area);
}

fn render_repair(frame: &mut Frame, app: &App, area: Rect, theme: &Theme) {
    if app.repair_suggestions.is_empty() {
        frame.render_widget(Paragraph::new("No repair suggestions — file looks healthy.").block(Block::default().borders(Borders::ALL).title("Repair Suggestions (W)")), area);
        return;
    }
    let header = Row::new(["Severity","Issue","Recommendation"].map(|h| Cell::from(h).style(Style::default().add_modifier(Modifier::BOLD))));
    let rows: Vec<Row> = app.repair_suggestions.iter().map(|s| {
        let color = match s.severity.as_str() { "high" => theme.error, "medium" => theme.warning, _ => theme.fg };
        Row::new([
            Cell::from(s.severity.clone()).style(Style::default().fg(color)),
            Cell::from(s.issue.clone()),
            Cell::from(s.recommendation.clone()),
        ])
    }).collect();
    let table = Table::new(rows, [Constraint::Length(8), Constraint::Min(30), Constraint::Min(40)])
        .header(header).block(Block::default().borders(Borders::ALL).title("Repair Suggestions (W)"));
    frame.render_widget(table, area);
}

fn render_timeseries(frame: &mut Frame, app: &App, area: Rect, theme: &Theme) {
    if app.timeseries_profiles.is_empty() {
        frame.render_widget(Paragraph::new("No timestamp columns detected.").block(Block::default().borders(Borders::ALL).title("Time-Series Profile (T)")), area);
        return;
    }
    let header = Row::new(["Column","Min","Max","Duration","MeanGap","MaxGap","Monotonic","Alert"].map(|h| Cell::from(h).style(Style::default().add_modifier(Modifier::BOLD))));
    let rows: Vec<Row> = app.timeseries_profiles.iter().map(|ts| {
        let mono_color = if ts.is_monotonic { theme.success } else { theme.error };
        Row::new([
            Cell::from(ts.column_name.clone()),
            Cell::from(ts.min_timestamp.map_or("-".into(), |v| v.to_string())),
            Cell::from(ts.max_timestamp.map_or("-".into(), |v| v.to_string())),
            Cell::from(ts.total_duration_ms.map_or("-".into(), |v| fmt_ms(v))),
            Cell::from(ts.mean_gap_ms.map_or("-".into(), |v| fmt_ms(v as i64))),
            Cell::from(ts.max_gap_ms.map_or("-".into(), |v| fmt_ms(v))),
            Cell::from(if ts.is_monotonic { "yes" } else { "NO" }).style(Style::default().fg(mono_color)),
            Cell::from(ts.missing_interval_hint.clone().unwrap_or_default()),
        ])
    }).collect();
    let table = Table::new(rows, [
        Constraint::Min(16), Constraint::Length(14), Constraint::Length(14),
        Constraint::Length(12), Constraint::Length(10), Constraint::Length(10),
        Constraint::Length(9), Constraint::Min(20),
    ]).header(header).block(Block::default().borders(Borders::ALL).title("Time-Series Profile (T)"));
    frame.render_widget(table, area);
}

fn render_nested(frame: &mut Frame, app: &App, area: Rect, theme: &Theme) {
    if app.nested_profiles.is_empty() {
        frame.render_widget(Paragraph::new("No nested columns detected (all flat schema).").block(Block::default().borders(Borders::ALL).title("Nested Type Profile (X)")), area);
        return;
    }
    let header = Row::new(["Column","Type","Depth","DefLvl","RepLvl","List","Map","Struct"].map(|h| Cell::from(h).style(Style::default().add_modifier(Modifier::BOLD))));
    let rows: Vec<Row> = app.nested_profiles.iter().map(|np| {
        let kind_color = if np.is_list { theme.string } else if np.is_map { theme.temporal } else { theme.numeric };
        Row::new([
            Cell::from(np.column_name.clone()),
            Cell::from(np.physical_type.clone()).style(Style::default().fg(kind_color)),
            Cell::from(np.nesting_depth.to_string()),
            Cell::from(np.max_def_level.to_string()),
            Cell::from(np.max_rep_level.to_string()),
            Cell::from(if np.is_list { "yes" } else { "" }),
            Cell::from(if np.is_map { "yes" } else { "" }),
            Cell::from(if np.is_struct { "yes" } else { "" }),
        ])
    }).collect();
    let table = Table::new(rows, [
        Constraint::Min(20), Constraint::Length(14), Constraint::Length(6),
        Constraint::Length(7), Constraint::Length(7),
        Constraint::Length(5), Constraint::Length(5), Constraint::Length(7),
    ]).header(header).block(Block::default().borders(Borders::ALL).title("Nested Type Profile (X)"));
    frame.render_widget(table, area);
}

fn render_null_patterns(frame: &mut Frame, app: &App, area: Rect, theme: &Theme) {
    if app.null_patterns.is_empty() {
        frame.render_widget(Paragraph::new("No null patterns detected. Press C to analyze.").block(Block::default().borders(Borders::ALL).title("Cross-Column Null Patterns (C)")), area);
        return;
    }
    let header = Row::new(["Pattern","Null%","Columns"].map(|h| Cell::from(h).style(Style::default().add_modifier(Modifier::BOLD))));
    let rows: Vec<Row> = app.null_patterns.iter().map(|p| {
        let color = match p.pattern_type.as_str() { "always_null" => theme.error, "correlated_nulls" => theme.warning, _ => theme.success };
        Row::new([
            Cell::from(p.pattern_type.clone()).style(Style::default().fg(color)),
            Cell::from(format!("{:.1}%", p.null_percentage)),
            Cell::from(p.columns.join(", ")),
        ])
    }).collect();
    let table = Table::new(rows, [Constraint::Length(18), Constraint::Length(8), Constraint::Min(40)])
        .header(header).block(Block::default().borders(Borders::ALL).title("Cross-Column Null Patterns (C)"));
    frame.render_widget(table, area);
}

fn render_baseline(frame: &mut Frame, app: &App, area: Rect, theme: &Theme) {
    let mut lines = Vec::new();
    if !app.has_baseline && app.baseline_regressions.is_empty() {
        lines.push(Line::from("No baseline loaded. Press G to save current profile as baseline."));
        frame.render_widget(Paragraph::new(lines).block(Block::default().borders(Borders::ALL).title("Baseline Diff (A)")).wrap(Wrap { trim: false }), area);
        return;
    }
    if app.baseline_regressions.is_empty() {
        lines.push(Line::from(Span::styled("No regressions detected — profile matches baseline.", Style::default().fg(theme.success))));
    } else {
        lines.push(Line::from(Span::styled(format!("{} regression(s) found:", app.baseline_regressions.len()), Style::default().fg(theme.error).add_modifier(Modifier::BOLD))));
        lines.push(Line::from(""));
        for r in &app.baseline_regressions {
            let color = match r.kind.as_str() { "quality_drop" => theme.error, "null_increase" => theme.warning, _ => theme.fg };
            lines.push(Line::from(Span::styled(format!("[{}] {} — {}", r.kind, r.column, r.detail), Style::default().fg(color))));
        }
    }
    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled("G: save current as baseline", Style::default().fg(theme.fg))));
    frame.render_widget(Paragraph::new(lines).block(Block::default().borders(Borders::ALL).title("Baseline Diff (A)")).wrap(Wrap { trim: false }), area);
}

fn render_col_size_breakdown(frame: &mut Frame, app: &App, area: Rect) {
    let mut cols: Vec<(&str, i64)> = app.agg_stats.iter().map(|s| (s.column_name.as_str(), s.total_compressed_size)).collect();
    cols.sort_by(|a, b| b.1.cmp(&a.1));
    let max_size = cols.first().map(|(_, s)| *s).unwrap_or(1).max(1);
    let bar_width = (area.width as usize).saturating_sub(30).max(10);
    let lines: Vec<Line> = cols.iter().map(|(name, size)| {
        let blen = (*size as f64 / max_size as f64 * bar_width as f64) as usize;
        Line::from(format!("{:<20} |{:<bw$}| {}", truncate(name, 20), "█".repeat(blen), fmt_bytes(*size as u64), bw=bar_width))
    }).collect();
    frame.render_widget(Paragraph::new(lines).block(Block::default().borders(Borders::ALL).title("Column Size Breakdown (Z) — sorted by compressed size")), area);
}

fn render_file_list(frame: &mut Frame, app: &App, area: Rect) {
    let Some(ds) = &app.dataset else {
        frame.render_widget(Paragraph::new("No dataset loaded.").block(Block::default().borders(Borders::ALL).title("File List (F)")), area);
        return;
    };
    let header = Row::new(["Path","Rows","Size","RowGroups"].map(|h| Cell::from(h).style(Style::default().add_modifier(Modifier::BOLD))));
    let rows: Vec<Row> = ds.files.iter().map(|f| {
        Row::new([
            truncate(f.path.to_str().unwrap_or(""), 40),
            f.row_count.to_string(),
            fmt_bytes(f.file_size),
            f.row_group_count.to_string(),
        ])
    }).collect();
    let table = Table::new(rows, [Constraint::Min(40), Constraint::Length(10), Constraint::Length(10), Constraint::Length(10)])
        .header(header).block(Block::default().borders(Borders::ALL).title("File List (F)"));
    frame.render_widget(table, area);
}

fn render_compare(frame: &mut Frame, app: &App, area: Rect, theme: &Theme) {
    use parquet_lens_core::compare::DiffStatus;
    let Some(cmp) = &app.comparison else {
        frame.render_widget(Paragraph::new("No comparison loaded.").block(Block::default().borders(Borders::ALL).title("Compare")), area);
        return;
    };
    let panes = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(area);
    let mut left_lines = Vec::new();
    left_lines.push(Line::from(Span::styled("Dataset A", Style::default().add_modifier(Modifier::BOLD))));
    left_lines.push(Line::from(format!("Rows:  {}  Files: {}  Size: {}", cmp.left_rows, cmp.left_files, fmt_bytes(cmp.left_bytes))));
    left_lines.push(Line::from(format!("Cols:  {}", cmp.left_columns)));
    left_lines.push(Line::from(""));
    left_lines.push(Line::from(Span::styled("Schema Diff:", Style::default().add_modifier(Modifier::BOLD))));
    for d in &cmp.schema_diffs {
        let (prefix, color) = match d.status {
            DiffStatus::Added => ("+", theme.success),
            DiffStatus::Removed => ("-", theme.error),
            DiffStatus::TypeChanged => ("~", theme.warning),
            DiffStatus::Matching => (" ", theme.fg),
        };
        left_lines.push(Line::from(Span::styled(format!("{prefix} {:<24} {}", d.name, d.left_type.as_deref().unwrap_or("-")), Style::default().fg(color))));
    }
    frame.render_widget(Paragraph::new(left_lines).block(Block::default().borders(Borders::ALL).title("Left dataset (A)")).wrap(Wrap { trim: false }), panes[0]);
    let mut right_lines = Vec::new();
    right_lines.push(Line::from(Span::styled("Dataset B", Style::default().add_modifier(Modifier::BOLD))));
    right_lines.push(Line::from(format!("Rows:  {}  Files: {}  Size: {}", cmp.right_rows, cmp.right_files, fmt_bytes(cmp.right_bytes))));
    right_lines.push(Line::from(format!("Cols:  {}", cmp.right_columns)));
    right_lines.push(Line::from(format!("Row delta:  {:+}  ({:+.1}%)", cmp.row_delta, cmp.row_delta_pct)));
    right_lines.push(Line::from(format!("Size delta: {:+} bytes", cmp.size_delta_bytes)));
    right_lines.push(Line::from(""));
    right_lines.push(Line::from(Span::styled("Stats Diff:", Style::default().add_modifier(Modifier::BOLD))));
    for d in &cmp.stats_diffs {
        let color = if d.null_rate_significant { theme.error } else { theme.fg };
        right_lines.push(Line::from(Span::styled(
            format!("{:<24} null: {:+.2}%  card: {}", d.name, d.null_rate_delta, d.cardinality_delta.map_or("-".into(), |c| format!("{c:+}"))),
            Style::default().fg(color)
        )));
    }
    frame.render_widget(Paragraph::new(right_lines).block(Block::default().borders(Borders::ALL).title("Right dataset (B)")).wrap(Wrap { trim: false }), panes[1]);
}

fn render_file_overview(frame: &mut Frame, app: &App, area: Rect) {
    let mut lines = Vec::new();
    if let Some(fi) = &app.file_info {
        lines.push(Line::from(vec![Span::styled("File:      ", Style::default().add_modifier(Modifier::BOLD)), Span::raw(fi.path.display().to_string())]));
        lines.push(Line::from(format!("Version:   {}", fi.parquet_version)));
        lines.push(Line::from(format!("Created:   {}", fi.created_by.as_deref().unwrap_or("unknown"))));
        lines.push(Line::from(format!("Row groups:{}", fi.row_group_count)));
        lines.push(Line::from(format!("Rows:      {}", fi.row_count)));
        lines.push(Line::from(format!("Size:      {}", fmt_bytes(fi.file_size))));
        if let Some(eng) = &app.engine_info {
            lines.push(Line::from(format!("Engine:    {} {}", eng.engine_name, eng.version_hint.as_deref().unwrap_or(""))));
            for hint in &eng.hints {
                lines.push(Line::from(format!("  hint: {hint}")));
            }
        }
        if !fi.key_value_metadata.is_empty() {
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled("Key-value metadata:", Style::default().add_modifier(Modifier::BOLD))));
            for (k, v) in &fi.key_value_metadata {
                lines.push(Line::from(format!("  {k} = {}", v.as_deref().unwrap_or(""))));
            }
        }
    } else {
        lines.push(Line::from(app.status_msg.clone()));
    }
    frame.render_widget(Paragraph::new(lines).block(Block::default().borders(Borders::ALL).title("File Overview")).wrap(Wrap { trim: false }), area);
}

fn render_schema(frame: &mut Frame, app: &App, area: Rect, theme: &Theme) {
    let header = Row::new(["Name","Physical","Logical","Repetition","DefLvl","RepLvl"].map(|h| Cell::from(h).style(Style::default().add_modifier(Modifier::BOLD))));
    let rows: Vec<Row> = app.columns().iter().map(|col| {
        let color = type_color(&col.physical_type, col.logical_type.as_deref(), theme);
        Row::new([
            col.name.clone(), col.physical_type.clone(),
            col.logical_type.clone().unwrap_or_else(|| "-".into()),
            col.repetition.clone(),
            col.max_def_level.to_string(), col.max_rep_level.to_string(),
        ].map(|s| Cell::from(s).style(Style::default().fg(color))))
    }).collect();
    let table = Table::new(rows, [Constraint::Min(20), Constraint::Length(12), Constraint::Length(16), Constraint::Length(10), Constraint::Length(7), Constraint::Length(7)])
        .header(header).block(Block::default().borders(Borders::ALL).title("Schema (S)"));
    frame.render_widget(table, area);
}

fn render_column_detail(frame: &mut Frame, app: &App, area: Rect, idx: usize, theme: &Theme) {
    let cols = app.columns();
    if idx >= cols.len() { return; }
    let col = &cols[idx];
    let mut lines = Vec::new();
    lines.push(Line::from(vec![Span::styled("Column: ", Style::default().add_modifier(Modifier::BOLD)), Span::raw(col.name.clone())]));
    lines.push(Line::from(format!("Type:       {} / {}", col.physical_type, col.logical_type.as_deref().unwrap_or("-"))));
    lines.push(Line::from(format!("Repetition: {}", col.repetition)));
    if let Some(agg) = app.agg_stats.iter().find(|s| s.column_name == col.name) {
        lines.push(Line::from(format!("Null rate:  {:.2}%  ({} nulls)", agg.null_percentage, agg.total_null_count)));
        lines.push(Line::from(format!("Cardinality:{}", agg.total_distinct_count_estimate.map_or("-".into(), |d| d.to_string()))));
        lines.push(Line::from(format!("Size:       {} uncomp / {} comp  ({:.2}x)", fmt_bytes(agg.total_data_page_size as u64), fmt_bytes(agg.total_compressed_size as u64), agg.compression_ratio)));
    }
    if let Some(enc) = app.encoding_analysis.iter().find(|e| e.column_name == col.name) {
        lines.push(Line::from(format!("Encodings:  {}", enc.encodings.join(", "))));
    }
    if let Some(comp) = app.compression_analysis.iter().find(|c| c.column_name == col.name) {
        lines.push(Line::from(format!("Codec:      {}  {:.2}x", comp.codec, comp.compression_ratio)));
    }
    if let Some(qs) = app.quality_scores.iter().find(|s| s.column_name == col.name) {
        let color = if qs.score >= 80 { theme.success } else if qs.score >= 50 { theme.warning } else { theme.error };
        lines.push(Line::from(vec![Span::styled("Quality:    ", Style::default().add_modifier(Modifier::BOLD)), Span::styled(format!("{}/100 ", qs.score), Style::default().fg(color)), Span::raw(qs.breakdown.clone())]));
    }
    if let Some(fsr) = app.full_scan_results.iter().find(|r| r.column_name == col.name) {
        if let Some(num) = &fsr.numeric {
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled("Numeric:", Style::default().add_modifier(Modifier::BOLD))));
            lines.push(Line::from(format!("  mean={:.3}  stddev={:.3}  min={:.3}  max={:.3}", num.mean, num.stddev, num.min, num.max)));
            lines.push(Line::from(format!("  p1={:.2} p25={:.2} p50={:.2} p75={:.2} p99={:.2}", num.p1, num.p25, num.p50, num.p75, num.p99)));
            lines.push(Line::from(format!("  skew={:.3}  kurt={:.3}", num.skewness, num.kurtosis)));
        }
        if let Some(hist) = &fsr.histogram {
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled("Histogram:", Style::default().add_modifier(Modifier::BOLD))));
            let max_c = hist.iter().map(|b| b.count).max().unwrap_or(1);
            let bw = (area.width as usize).saturating_sub(30).max(10);
            for bin in hist {
                let blen = (bin.count as f64 / max_c as f64 * bw as f64) as usize;
                lines.push(Line::from(format!("{:8.2}-{:8.2} |{:<bw$}| {}", bin.range_start, bin.range_end, "█".repeat(blen), bin.count, bw=bw)));
            }
        }
        if let Some(freq) = &fsr.frequency {
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled("Top values:", Style::default().add_modifier(Modifier::BOLD))));
            for e in &freq.top_values {
                lines.push(Line::from(format!("  {:<30} {:6}  {:.1}%", truncate(&e.value, 30), e.count, e.percentage)));
            }
        }
        if let Some(s) = &fsr.string {
            lines.push(Line::from(format!("String: len {}-{}  avg={:.1}  empty={}  ws={}", s.min_length, s.max_length, s.mean_length, s.empty_count, s.whitespace_only_count)));
        }
        if let Some(b) = &fsr.boolean {
            lines.push(Line::from(format!("Boolean: true={}  false={}  null={}  {:.1}%", b.true_count, b.false_count, b.null_count, b.true_percentage)));
        }
    }
    frame.render_widget(Paragraph::new(lines).block(Block::default().borders(Borders::ALL).title(format!("Column: {}", col.name))).wrap(Wrap { trim: false }), area);
}

fn render_row_groups(frame: &mut Frame, app: &App, area: Rect, theme: &Theme) {
    let mut rgs = app.row_groups.clone();
    match app.rg_sort_col {
        0 => rgs.sort_by_key(|r| r.index),
        1 => rgs.sort_by_key(|r| r.num_rows),
        2 => rgs.sort_by_key(|r| r.total_byte_size),
        3 => rgs.sort_by_key(|r| r.compressed_size),
        _ => {}
    }
    if !app.rg_sort_asc { rgs.reverse(); }
    let mean_b = if rgs.is_empty() { 0.0 } else { rgs.iter().map(|r| r.total_byte_size as f64).sum::<f64>() / rgs.len() as f64 };
    let std_b = if rgs.len() > 1 { (rgs.iter().map(|r| (r.total_byte_size as f64 - mean_b).powi(2)).sum::<f64>() / rgs.len() as f64).sqrt() } else { 0.0 };
    let rows: Vec<Row> = rgs.iter().map(|rg| {
        let outlier = (rg.total_byte_size as f64 - mean_b).abs() > 2.0 * std_b && std_b > 0.0;
        Row::new([rg.index.to_string(), rg.num_rows.to_string(), fmt_bytes(rg.total_byte_size as u64), fmt_bytes(rg.compressed_size as u64), format!("{:.2}x", rg.compression_ratio)])
            .style(if outlier { Style::default().fg(theme.error) } else { Style::default() })
    }).collect();
    let hdrs: Vec<String> = ["idx","rows","bytes","compressed","ratio"].iter().enumerate().map(|(i, h)| {
        let arrow = if i == app.rg_sort_col { if app.rg_sort_asc { "▲" } else { "▼" } } else { "" };
        format!("{h}{arrow}")
    }).collect();
    let header = Row::new(hdrs.iter().map(|h| Cell::from(h.as_str()).style(Style::default().add_modifier(Modifier::BOLD))));
    let table = Table::new(rows, [Constraint::Length(6), Constraint::Length(10), Constraint::Length(12), Constraint::Length(12), Constraint::Length(8)])
        .header(header).block(Block::default().borders(Borders::ALL).title("Row Groups (R) — </> sort, outliers red"));
    frame.render_widget(table, area);
}

fn render_null_heatmap(frame: &mut Frame, app: &App, area: Rect, theme: &Theme) {
    let mut lines = Vec::new();
    lines.push(Line::from("Null Heatmap — ░<1% ▒<25% ▓<75% █>=75%"));
    lines.push(Line::from(""));
    let max_cols = 15usize;
    let col_header: String = app.columns().iter().take(max_cols).map(|c| format!("{:>6}", truncate(&c.name, 6))).collect::<Vec<_>>().join(" ");
    lines.push(Line::from(format!("      {col_header}")));
    for rg in &app.row_groups {
        let mut row_spans = vec![Span::raw(format!("rg{:>3}  ", rg.index))];
        for col in app.columns().iter().take(max_cols) {
            let null_pct = app.agg_stats.iter().find(|s| s.column_name == col.name).map(|s| s.null_percentage).unwrap_or(0.0);
            let (ch, color) = if null_pct < 1.0 { ("\u{2591}", theme.fg) } else if null_pct < 25.0 { ("\u{2592}", theme.warning) } else if null_pct < 75.0 { ("\u{2593}", theme.error) } else { ("\u{2588}", theme.error) };
            row_spans.push(Span::styled(format!("{:>7}", ch), Style::default().fg(color)));
        }
        lines.push(Line::from(row_spans));
    }
    frame.render_widget(Paragraph::new(lines).block(Block::default().borders(Borders::ALL).title("Null Heatmap (N)")), area);
}

fn render_data_preview(frame: &mut Frame, app: &App, area: Rect) {
    if app.preview_headers.is_empty() {
        frame.render_widget(Paragraph::new("Data preview not loaded.").block(Block::default().borders(Borders::ALL).title("Data Preview (D)")), area);
        return;
    }
    let vis_cols: Vec<&str> = app.preview_headers.iter().skip(app.preview_scroll_x).take(8).map(|h| h.as_str()).collect();
    let header = Row::new(vis_cols.iter().map(|h| Cell::from(*h).style(Style::default().add_modifier(Modifier::BOLD))));
    let rows: Vec<Row> = app.preview_rows.iter().skip(app.preview_scroll_y).take(area.height.saturating_sub(4) as usize).map(|row| {
        Row::new(row.iter().skip(app.preview_scroll_x).take(8).map(|v| Cell::from(truncate(v, 15))))
    }).collect();
    let widths: Vec<Constraint> = vis_cols.iter().map(|_| Constraint::Min(16)).collect();
    frame.render_widget(Table::new(rows, widths).header(header).block(Block::default().borders(Borders::ALL).title("Data Preview (D) — arrows scroll")), area);
}

fn render_filter_overlay(frame: &mut Frame, app: &App, area: Rect) {
    let popup = centered_rect(60, 30, area);
    frame.render_widget(ratatui::widgets::Clear, popup);
    let result_line = if let Some(r) = &app.filter_result {
        format!("matched: {}  scanned: {}  skipped rgs: {}/{}",
            r.matched_rows, r.scanned_rows, r.skipped_rgs, r.total_rgs)
    } else {
        String::from("Enter expression, press Enter to run, Esc to cancel")
    };
    let content = format!("> {}_

{}", app.filter_input, result_line);
    frame.render_widget(
        Paragraph::new(content)
            .block(Block::default().borders(Borders::ALL).title("Filter (P) — WHERE expression"))
            .wrap(Wrap { trim: false }),
        popup,
    );
}

fn render_help(frame: &mut Frame, area: Rect) {
    let text = vec![
        Line::from(Span::styled("Keybindings", Style::default().add_modifier(Modifier::BOLD))),
        Line::from("  q        Quit"),
        Line::from("  ?        Toggle help"),
        Line::from("  Tab      Cycle focus"),
        Line::from("  m        Toggle profiling mode"),
        Line::from("  S        Schema view"),
        Line::from("  R        Row groups"),
        Line::from("  N        Null heatmap"),
        Line::from("  D        Data preview"),
        Line::from("  T        Time-series profile"),
        Line::from("  X        Nested type profile"),
        Line::from("  W        Repair suggestions"),
        Line::from("  j/k      Navigate sidebar"),
        Line::from("  Enter    Column detail"),
        Line::from("  </> Sort row groups"),
        Line::from("  arrows   Scroll data preview"),
        Line::from("  Esc      Back to overview"),
        Line::from("  P        Predicate filter mode"),
    ];
    let popup = centered_rect(50, 70, area);
    frame.render_widget(ratatui::widgets::Clear, popup);
    frame.render_widget(Paragraph::new(text).block(Block::default().borders(Borders::ALL).title("Help (?)")), popup);
}

fn render_confirm(frame: &mut Frame, area: Rect) {
    let popup = centered_rect(50, 20, area);
    frame.render_widget(ratatui::widgets::Clear, popup);
    frame.render_widget(Paragraph::new("File >1GB. Full-scan may be slow.\nEnter: confirm  Esc: cancel").block(Block::default().borders(Borders::ALL).title("Confirm Full Scan")), popup);
}

fn render_progress(frame: &mut Frame, area: Rect, rp: u64, tr: u64, theme: &Theme) {
    let popup = centered_rect(50, 10, area);
    frame.render_widget(ratatui::widgets::Clear, popup);
    let ratio = if tr > 0 { (rp as f64 / tr as f64).min(1.0) } else { 0.0 };
    frame.render_widget(Gauge::default().block(Block::default().borders(Borders::ALL).title("Profiling... (Esc cancel)")).gauge_style(Style::default().fg(theme.numeric)).ratio(ratio).label(format!("{rp}/{tr}")), popup);
}

fn render_bottombar(frame: &mut Frame, app: &App, area: Rect, theme: &Theme) {
    let bar_text = if let Some(r) = &app.filter_result {
        format!(" {} | filter: {} matched / {} scanned ({} rgs skipped) | q:quit ?:help Tab:focus S R N D T X W m P",
            app.status_msg, r.matched_rows, r.scanned_rows, r.skipped_rgs)
    } else {
        format!(" {} | q:quit ?:help Tab:focus S R N D T X W m P", app.status_msg)
    };
    frame.render_widget(Paragraph::new(bar_text).style(Style::default().bg(theme.bg).fg(theme.fg)), area);
}

fn centered_rect(px: u16, py: u16, r: Rect) -> Rect {
    let v = Layout::default().direction(Direction::Vertical).constraints([Constraint::Percentage((100-py)/2), Constraint::Percentage(py), Constraint::Percentage((100-py)/2)]).split(r);
    Layout::default().direction(Direction::Horizontal).constraints([Constraint::Percentage((100-px)/2), Constraint::Percentage(px), Constraint::Percentage((100-px)/2)]).split(v[1])[1]
}

fn type_icon(t: &str) -> &'static str {
    match t { "INT32"|"INT64" => "#", "FLOAT"|"DOUBLE" => "~", "BYTE_ARRAY"|"FIXED_LEN_BYTE_ARRAY" => "\"", "BOOLEAN" => "?", _ => "." }
}

fn type_color(phys: &str, log: Option<&str>, theme: &Theme) -> Color {
    if let Some(lt) = log {
        if lt.contains("String") || lt.contains("Utf8") { return theme.string; }
        if lt.contains("Date") || lt.contains("Timestamp") { return theme.temporal; }
    }
    match phys { "INT32"|"INT64"|"FLOAT"|"DOUBLE" => theme.numeric, "BOOLEAN" => theme.boolean, _ => theme.fg }
}

fn fmt_bytes(b: u64) -> String {
    if b < 1024 { format!("{b}B") } else if b < 1<<20 { format!("{:.1}KB", b as f64/1024.0) } else if b < 1<<30 { format!("{:.1}MB", b as f64/1048576.0) } else { format!("{:.2}GB", b as f64/1073741824.0) }
}

fn fmt_ms(ms: i64) -> String {
    if ms.abs() < 1000 { format!("{ms}ms") } else if ms.abs() < 60_000 { format!("{:.1}s", ms as f64 / 1000.0) } else if ms.abs() < 3_600_000 { format!("{:.1}m", ms as f64 / 60000.0) } else { format!("{:.1}h", ms as f64 / 3_600_000.0) }
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max { s.to_owned() } else { format!("{}\u{2026}", s.chars().take(max.saturating_sub(1)).collect::<String>()) }
}
