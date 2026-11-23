use crate::engine::{DashboardSnapshot, DownloadEngine};
use crate::models::{format_bytes, format_eta, format_speed, InterfaceStatus};
use anyhow::Result;
use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Gauge, Paragraph, Row, Table};
use ratatui::{Frame, Terminal};
use std::io;
use std::sync::Arc;
use std::time::Duration;

fn status_style(status: InterfaceStatus) -> Style {
    match status {
        InterfaceStatus::Active => Style::default().fg(Color::Green).add_modifier(Modifier::BOLD),
        InterfaceStatus::Degraded => Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD),
        InterfaceStatus::Stopped => Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        InterfaceStatus::Probing => Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
    }
}

fn draw_ui(f: &mut Frame, snap: &DashboardSnapshot) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .margin(1)
        .constraints([
            Constraint::Length(5), // Overall progress + stats
            Constraint::Min(7),    // Per-interface table
            Constraint::Length(8), // Activity log
            Constraint::Length(1), // Controls bar
        ])
        .split(f.area());

    // 1. Header & Overall Progress Gauge
    let ratio = match snap.total_size {
        Some(tot) if tot > 0 => (snap.downloaded_bytes as f64 / tot as f64).clamp(0.0, 1.0),
        _ => 0.0,
    };
    let size_str = match snap.total_size {
        Some(tot) => format!(
            "{} / {}",
            format_bytes(snap.downloaded_bytes as f64),
            format_bytes(tot as f64)
        ),
        None => format!("{} / ?", format_bytes(snap.downloaded_bytes as f64)),
    };
    let state_label = if snap.is_paused {
        "PAUSED"
    } else if snap.is_finished {
        "COMPLETED"
    } else {
        snap.status_message.as_str()
    };

    let header_block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan))
        .title(format!(" paralleldl (pldl) — {} ", snap.filename));
    let header_inner = header_block.inner(chunks[0]);
    f.render_widget(header_block, chunks[0]);

    let header_rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Length(1), Constraint::Length(1)])
        .split(header_inner);

    let gauge = Gauge::default()
        .gauge_style(Style::default().fg(Color::Green).bg(Color::DarkGray))
        .ratio(ratio)
        .label(format!("{:.1}%", ratio * 100.0));
    f.render_widget(gauge, header_rows[0]);

    let stats_line = Line::from(vec![
        Span::raw("Status: "),
        Span::styled(
            state_label,
            Style::default()
                .fg(if snap.is_paused {
                    Color::Yellow
                } else if snap.is_finished {
                    Color::Green
                } else {
                    Color::Cyan
                })
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw("   |   Transferred: "),
        Span::styled(size_str, Style::default().add_modifier(Modifier::BOLD)),
        Span::raw("   |   Total Speed: "),
        Span::styled(
            format_speed(snap.total_speed_bps),
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw("   |   ETA: "),
        Span::styled(
            format_eta(snap.eta_seconds),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::raw("   |   Queue: "),
        Span::styled(
            snap.queue_len.to_string(),
            Style::default().add_modifier(Modifier::BOLD),
        ),
    ]);
    f.render_widget(Paragraph::new(stats_line), header_rows[2]);

    // 2. Per-Interface Table (connected interfaces only)
    let header = Row::new(vec![
        "Interface",
        "IP Address",
        "Bind",
        "Status",
        "Workers",
        "Speed",
        "Downloaded",
        "Chunks",
        "Errors",
    ])
    .style(
        Style::default()
            .fg(Color::Gray)
            .add_modifier(Modifier::BOLD),
    );

    let rows: Vec<Row> = snap
        .interfaces
        .iter()
        .map(|i| {
            let status_txt = match i.probe_countdown_secs {
                Some(secs) => format!("{} ({secs}s)", i.status.as_str()),
                None => i.status.as_str().to_string(),
            };
            Row::new(vec![
                Cell::from(i.name.clone()).style(Style::default().add_modifier(Modifier::BOLD)),
                Cell::from(i.ip.clone()).style(Style::default().fg(Color::DarkGray)),
                Cell::from(i.bind_mode).style(Style::default().fg(if i.bind_mode == "SO_BIND" {
                    Color::Green
                } else {
                    Color::Yellow
                })),
                Cell::from(status_txt).style(status_style(i.status)),
                Cell::from(format!("{}/{}", i.active_workers, i.target_workers)),
                Cell::from(format_speed(i.speed_bps)).style(
                    Style::default()
                        .fg(Color::Green)
                        .add_modifier(Modifier::BOLD),
                ),
                Cell::from(format_bytes(i.downloaded_bytes as f64)),
                Cell::from(i.chunks_completed.to_string()),
                Cell::from(i.total_errors.to_string()),
            ])
        })
        .collect();

    let widths = [
        Constraint::Length(14),
        Constraint::Length(16),
        Constraint::Length(9),
        Constraint::Length(16),
        Constraint::Length(9),
        Constraint::Length(14),
        Constraint::Length(13),
        Constraint::Length(8),
        Constraint::Length(8),
    ];

    let table = Table::new(rows, widths).header(header).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Blue))
            .title(" Network Interfaces (Connected) "),
    );
    f.render_widget(table, chunks[1]);

    // 3. Activity Log
    let log_lines: Vec<Line> = if snap.events.is_empty() {
        vec![Line::from("Waiting for events...")]
    } else {
        snap.events
            .iter()
            .rev()
            .take(6)
            .rev()
            .map(|e| Line::from(Span::styled(e.as_str(), Style::default().fg(Color::Gray))))
            .collect()
    };
    let log = Paragraph::new(log_lines).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray))
            .title(" Activity Log "),
    );
    f.render_widget(log, chunks[2]);

    // 4. Controls Footer
    let pause_lbl = if snap.is_paused { "Resume" } else { "Pause" };
    let controls = Line::from(vec![
        Span::raw("Controls:  "),
        Span::styled(
            "[p]",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(format!(" {pause_lbl}   |   ")),
        Span::styled(
            "[r]",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(" Recheck stopped NICs   |   "),
        Span::styled(
            "[l]",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(" Rescan NICs   |   "),
        Span::styled(
            "[s / q]",
            Style::default()
                .fg(Color::Red)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(" Stop & save JSON"),
    ]);
    f.render_widget(Paragraph::new(controls), chunks[3]);
}

pub async fn run_with_tui(engine: Arc<DownloadEngine>) -> Result<bool> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let run_engine = Arc::clone(&engine);
    let engine_handle = tokio::spawn(async move { run_engine.run().await });

    let result = loop {
        let snap = engine.snapshot().await;
        terminal.draw(|f| draw_ui(f, &snap))?;

        if engine_handle.is_finished() {
            break engine_handle.await?;
        }

        if event::poll(Duration::from_millis(120))? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    match key.code {
                        KeyCode::Char('p') | KeyCode::Char('P') => {
                            engine.toggle_pause().await;
                        }
                        KeyCode::Char('r') | KeyCode::Char('R') => {
                            engine.trigger_recheck_stopped().await;
                        }
                        KeyCode::Char('l') | KeyCode::Char('L') => {
                            engine.trigger_rescan().await;
                        }
                        KeyCode::Char('s') | KeyCode::Char('S') | KeyCode::Char('q') | KeyCode::Char('Q') => {
                            engine.request_stop().await;
                        }
                        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                            engine.request_stop().await;
                        }
                        _ => {}
                    }
                }
            }
        }
    };

    // Final render briefly before restoring terminal
    let final_snap = engine.snapshot().await;
    let _ = terminal.draw(|f| draw_ui(f, &final_snap));

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    if let Ok(done) = &result {
        println!(
            "{}: {} ({})",
            if *done { "Completed" } else { "Stopped" },
            final_snap.filename,
            format_bytes(final_snap.downloaded_bytes as f64)
        );
    }

    result
}
