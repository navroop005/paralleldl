mod engine;
mod models;
mod net_linux;
mod probe;
mod state;
mod tui;
mod writer;

use anyhow::Result;
use clap::Parser;
use engine::DownloadEngine;
use models::{format_bytes, parse_size, DownloadConfig};
use net_linux::discover_linux_interfaces;
use std::io::IsTerminal;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Parser)]
#[command(
    name = "pldl",
    version,
    about = "Rootless Linux multi-interface parallel HTTP file downloader with TUI."
)]
struct Cli {
    /// HTTP/HTTPS URL to download
    url: Option<String>,

    /// List available Linux network interfaces and exit without downloading
    #[arg(short = 'L', long)]
    list_interfaces: bool,

    /// Include DOWN interfaces when listing networks
    #[arg(long)]
    include_down: bool,

    /// Include Docker/bridge/virtual interfaces when listing networks
    #[arg(long)]
    include_virtual: bool,

    /// Comma-separated interface names to use (e.g., enp12s0,wlan0)
    #[arg(short = 'i', long)]
    interfaces: Option<String>,

    /// Concurrent connections per network interface (e.g., 4)
    #[arg(short = 'c', long, conflicts_with = "total_connections")]
    per_interface: Option<usize>,

    /// Total concurrent connections split equally across active interfaces
    #[arg(short = 't', long, conflicts_with = "per_interface")]
    total_connections: Option<usize>,

    /// Chunk size for Range splitting (e.g., 100M, 16M)
    #[arg(long, default_value = "16M")]
    chunk_size: String,

    /// Minimum chunk size allowed when splitting tail/gaps
    #[arg(long, default_value = "1M")]
    min_chunk_size: String,

    /// In-memory buffer size per worker before pwrite flush
    #[arg(long, default_value = "1M")]
    flush_size: String,

    /// Consecutive errors before stopping a connected interface and re-routing its chunks
    #[arg(long, default_value_t = 5)]
    max_iface_errors: u32,

    /// Directory to save downloaded file and progress JSON
    #[arg(short = 'd', long, default_value = ".")]
    output_dir: PathBuf,

    /// Override target filename
    #[arg(short = 'o', long)]
    output_name: Option<String>,

    /// Ignore existing .pldl.json progress file and start fresh
    #[arg(long)]
    no_resume: bool,

    /// Disable SO_BINDTODEVICE and bind only by source IP (for kernels < 5.7)
    #[arg(long)]
    no_bind_device: bool,

    /// Run headless without the interactive Ratatui TUI
    #[arg(long)]
    no_tui: bool,
}

fn print_interfaces(include_down: bool, include_virtual: bool) {
    let ifaces = discover_linux_interfaces(include_down, include_virtual);
    if ifaces.is_empty() {
        println!("No IPv4 network interfaces detected.");
        return;
    }
    println!(
        "{:<16} {:>7} {:<8} {:<16} {}",
        "INTERFACE", "IFINDEX", "STATE", "IPV4 ADDRESS", "DEFAULT GATEWAY"
    );
    println!("{}", "-".repeat(68));
    for iface in ifaces {
        println!(
            "{:<16} {:>7} {:<8} {:<16} {}",
            iface.name,
            iface.ifindex,
            iface.operstate,
            iface
                .ip
                .map(|ip| ip.to_string())
                .unwrap_or_else(|| "-".to_string()),
            iface.gateway.unwrap_or_else(|| "None".to_string())
        );
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    if cli.list_interfaces {
        print_interfaces(cli.include_down, cli.include_virtual);
        return Ok(());
    }

    let Some(url) = cli.url else {
        anyhow::bail!("Provide a download URL, or use --list-interfaces.");
    };

    let interfaces = cli.interfaces.map(|raw| {
        raw.split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>()
    });

    let config = DownloadConfig {
        url,
        output_dir: cli.output_dir,
        output_name: cli.output_name,
        interfaces,
        total_connections: cli.total_connections,
        per_interface_connections: cli.per_interface,
        chunk_size: parse_size(&cli.chunk_size)?,
        min_chunk_size: parse_size(&cli.min_chunk_size)?,
        flush_size: parse_size(&cli.flush_size)? as usize,
        max_iface_errors: cli.max_iface_errors,
        connect_timeout: Duration::from_secs(10),
        read_timeout: Duration::from_secs(20),
        resume: !cli.no_resume,
        no_tui: cli.no_tui,
        use_so_bindtodevice: !cli.no_bind_device,
        probe_interval_base: Duration::from_secs(5),
        probe_interval_max: Duration::from_secs(60),
    };

    let engine = DownloadEngine::new(config.clone());

    // Handle Ctrl+C cleanly in headless mode
    let sig_engine = Arc::clone(&engine);
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            sig_engine.request_stop().await;
        }
    });

    let ok = if config.no_tui || !std::io::stdout().is_terminal() {
        let res = engine.run().await?;
        let snap = engine.snapshot().await;
        for ev in snap.events {
            println!("{ev}");
        }
        println!(
            "Done: {} (status: {})",
            format_bytes(snap.downloaded_bytes as f64),
            snap.status_message
        );
        res
    } else {
        tui::run_with_tui(engine).await?
    };

    if !ok {
        std::process::exit(1);
    }
    Ok(())
}
