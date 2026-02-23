use crossterm::event::{KeyCode, KeyEvent};
use crate::tui::app::{App, Focus, ProfilingMode, View};

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
    match key.code {
        KeyCode::Char('j') | KeyCode::Down => app.sidebar_down(),
        KeyCode::Char('k') | KeyCode::Up => app.sidebar_up(),
        KeyCode::Enter => {
            if app.column_count() > 0 {
                app.view = View::ColumnDetail(app.sidebar_selected);
                app.focus = Focus::Main;
            }
        }
        KeyCode::Char('S') => app.view = View::Schema,
        KeyCode::Char('R') => app.view = View::RowGroups,
        KeyCode::Char('N') => app.view = View::NullHeatmap,
        KeyCode::Char('D') => app.view = View::DataPreview,
        KeyCode::Esc => app.view = View::FileOverview,
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
