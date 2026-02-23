use std::path::Path;
use crossterm::event::{KeyCode, KeyEvent};
use crate::tui::app::{App, Focus, ProfilingMode, SidebarSort, View};
use parquet_lens_core::{parse_predicate, filter_count, BaselineProfile, analyze_null_patterns};

pub fn handle_key(app: &mut App, key: KeyEvent) {
    match key.code {
        KeyCode::Char('q') => { app.should_quit = true; return; }
        KeyCode::Tab => { app.cycle_focus(); return; }
        KeyCode::Char('?') => {
            app.view = if app.view == View::Help { View::FileOverview } else { View::Help };
            return;
        }
        KeyCode::Char('m') => { app.cycle_profiling_mode(); return; }
        _ => {}
    }
    match app.focus {
        Focus::Sidebar => handle_sidebar(app, key),
        Focus::Main => handle_main(app, key),
        Focus::Overlay => handle_overlay(app, key),
    }
}

fn handle_sidebar(app: &mut App, key: KeyEvent) {
    if app.sidebar_searching {
        match key.code {
            KeyCode::Esc | KeyCode::Enter => { app.sidebar_searching = false; }
            KeyCode::Backspace => { app.sidebar_search.pop(); }
            KeyCode::Char(c) => { app.sidebar_search.push(c); app.sidebar_selected = 0; }
            _ => {}
        }
        return;
    }
    match key.code {
        KeyCode::Char('j') | KeyCode::Down => app.sidebar_down(),
        KeyCode::Char('k') | KeyCode::Up => app.sidebar_up(),
        KeyCode::Enter => {
            let indices = app.filtered_column_indices();
            if let Some(&col_idx) = indices.get(app.sidebar_selected) {
                app.view = View::ColumnDetail(col_idx);
                app.focus = Focus::Main;
            }
        }
        KeyCode::Char('S') => app.view = View::Schema,
        KeyCode::Char('R') => app.view = View::RowGroups,
        KeyCode::Char('N') => app.view = View::NullHeatmap,
        KeyCode::Char('D') => app.view = View::DataPreview,
        KeyCode::Char('Z') => app.view = View::ColumnSizeBreakdown,
        KeyCode::Char('F') => app.view = View::FileList,
        KeyCode::Char('T') => app.view = View::TimeSeries, // time-series profile
        KeyCode::Char('X') => app.view = View::Nested,     // nested type profile
        KeyCode::Char('W') => app.view = View::Repair,     // repair suggestions
        KeyCode::Char('/') => { app.sidebar_searching = true; app.sidebar_search.clear(); }
        KeyCode::Char('o') => {
            app.sidebar_sort = match app.sidebar_sort {
                SidebarSort::Name => SidebarSort::NullRate,
                SidebarSort::NullRate => SidebarSort::Cardinality,
                SidebarSort::Cardinality => SidebarSort::Size,
                SidebarSort::Size => SidebarSort::Quality,
                SidebarSort::Quality => { app.sidebar_sort_asc = !app.sidebar_sort_asc; SidebarSort::Name }
            };
        }
        KeyCode::Char('b') => app.toggle_bookmark(),
        KeyCode::Char('B') => { app.show_bookmarks_only = !app.show_bookmarks_only; app.sidebar_selected = 0; }
        KeyCode::Char('P') => { app.filter_active = true; app.view = View::FilterInput; app.focus = Focus::Overlay; }
        KeyCode::Char('C') => {
            app.null_patterns = analyze_null_patterns(&app.agg_stats);
            app.view = View::NullPatterns;
        }
        KeyCode::Char('A') => { app.view = View::Baseline; }
        KeyCode::Char('G') => {
            // save current profile as baseline
            let schema = app.columns().to_vec();
            let base = BaselineProfile::new(&app.input_path, schema, app.agg_stats.clone(), app.quality_scores.clone());
            match base.save() {
                Ok(_) => { app.status_msg = "baseline saved".into(); app.has_baseline = true; }
                Err(e) => { app.status_msg = format!("save baseline failed: {e}"); }
            }
        }
        KeyCode::Esc => { app.view = View::FileOverview; app.sidebar_search.clear(); app.sidebar_searching = false; }
        _ => {}
    }
}

fn handle_main(app: &mut App, key: KeyEvent) {
    match key.code {
        KeyCode::Char('S') => app.view = View::Schema,
        KeyCode::Char('R') => app.view = View::RowGroups,
        KeyCode::Char('N') => app.view = View::NullHeatmap,
        KeyCode::Char('D') => app.view = View::DataPreview,
        KeyCode::Esc => { app.view = View::FileOverview; app.focus = Focus::Sidebar; }
        KeyCode::Char('<') => { if app.rg_sort_col > 0 { app.rg_sort_col -= 1; } else { app.rg_sort_asc = !app.rg_sort_asc; } }
        KeyCode::Char('>') => { app.rg_sort_col = (app.rg_sort_col + 1) % 5; }
        KeyCode::Left => { if app.preview_scroll_x > 0 { app.preview_scroll_x -= 1; } }
        KeyCode::Right => { app.preview_scroll_x += 1; }
        KeyCode::Up => { if app.preview_scroll_y > 0 { app.preview_scroll_y -= 1; } }
        KeyCode::Down => { app.preview_scroll_y += 1; }
        _ => {}
    }
}

fn handle_overlay(app: &mut App, key: KeyEvent) {
    if app.filter_active || app.view == View::FilterInput {
        match key.code {
            KeyCode::Esc => {
                app.filter_active = false;
                app.view = View::FileOverview;
                app.focus = Focus::Sidebar;
            }
            KeyCode::Backspace => { app.filter_input.pop(); }
            KeyCode::Enter => {
                let expr = app.filter_input.trim().to_string();
                if !expr.is_empty() {
                    match parse_predicate(&expr) {
                        Err(e) => { app.status_msg = format!("parse error: {e}"); }
                        Ok(pred) => {
                            let path = Path::new(&app.input_path);
                            match filter_count(path, &pred) {
                                Ok(r) => {
                                    app.status_msg = format!("filter: {} matched / {} scanned ({} rgs skipped)",
                                        r.matched_rows, r.scanned_rows, r.skipped_rgs);
                                    app.filter_result = Some(r);
                                }
                                Err(e) => { app.status_msg = format!("filter error: {e}"); }
                            }
                        }
                    }
                }
                app.filter_active = false;
                app.view = View::FileOverview;
                app.focus = Focus::Sidebar;
            }
            KeyCode::Char(c) => { app.filter_input.push(c); }
            _ => {}
        }
        return;
    }
    match key.code {
        KeyCode::Esc => { app.view = View::FileOverview; app.focus = Focus::Sidebar; }
        KeyCode::Enter => {
            if app.view == View::ConfirmFullScan {
                app.profiling_mode = ProfilingMode::FullScan;
                app.view = View::FileOverview;
                app.focus = Focus::Sidebar;
            }
        }
        _ => {}
    }
}
