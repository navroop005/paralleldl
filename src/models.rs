use anyhow::{bail, Result};
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::net::IpAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InterfaceStatus {
    Active,
    Degraded,
    Stopped,
    Probing,
}

impl InterfaceStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "ACTIVE",
            Self::Degraded => "DEGRADED",
            Self::Stopped => "STOPPED",
            Self::Probing => "PROBING",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkInterface {
    pub name: String,
    pub ip: Option<IpAddr>,
    pub gateway: Option<String>,
    pub operstate: String,
    pub ifindex: u32,
}

/// Inclusive byte-range work item `[start, end]`.
/// Ordered as a min-heap by `(priority, start)` inside `BinaryHeap`.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ChunkTask {
    pub priority: u8,
    pub start: u64,
    pub end: u64,
    pub retries: u32,
}

impl Ord for ChunkTask {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reversed so BinaryHeap pops smallest priority, then smallest start offset.
        other
            .priority
            .cmp(&self.priority)
            .then_with(|| other.start.cmp(&self.start))
    }
}

impl PartialOrd for ChunkTask {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone)]
pub struct ProbeResult {
    pub url: String,
    pub final_url: String,
    pub filename: String,
    pub total_size: Option<u64>,
    pub supports_ranges: bool,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DownloadConfig {
    pub url: String,
    pub output_dir: PathBuf,
    pub output_name: Option<String>,
    pub interfaces: Option<Vec<String>>,
    pub total_connections: Option<usize>,
    pub per_interface_connections: Option<usize>,
    pub chunk_size: u64,
    pub min_chunk_size: u64,
    pub flush_size: usize,
    pub max_iface_errors: u32,
    pub connect_timeout: Duration,
    pub read_timeout: Duration,
    pub resume: bool,
    pub no_tui: bool,
    pub use_so_bindtodevice: bool,
    pub probe_interval_base: Duration,
    pub probe_interval_max: Duration,
}

#[derive(Debug)]
pub struct InterfaceRuntime {
    pub interface: NetworkInterface,
    pub status: InterfaceStatus,
    pub target_workers: usize,
    pub active_workers: usize,
    pub downloaded_bytes: u64,
    pub chunks_completed: u64,
    pub consecutive_errors: u32,
    pub total_errors: u32,
    pub stop_count: u32,
    pub last_error: String,
    pub speed_samples: VecDeque<(Instant, u64)>,
    pub next_probe_at: Option<Instant>,
}

impl InterfaceRuntime {
    pub fn new(interface: NetworkInterface) -> Self {
        Self {
            interface,
            status: InterfaceStatus::Active,
            target_workers: 0,
            active_workers: 0,
            downloaded_bytes: 0,
            chunks_completed: 0,
            consecutive_errors: 0,
            total_errors: 0,
            stop_count: 0,
            last_error: String::new(),
            speed_samples: VecDeque::new(),
            next_probe_at: None,
        }
    }

    pub fn record_bytes(&mut self, n: u64) {
        if n == 0 {
            return;
        }
        self.downloaded_bytes += n;
        self.speed_samples.push_back((Instant::now(), n));
    }

    pub fn compute_speed_bps(&mut self, window: Duration) -> f64 {
        let now = Instant::now();
        while let Some((ts, _)) = self.speed_samples.front() {
            if now.duration_since(*ts) > window {
                self.speed_samples.pop_front();
            } else {
                break;
            }
        }
        let Some((first_ts, _)) = self.speed_samples.front() else {
            return 0.0;
        };
        let total: u64 = self.speed_samples.iter().map(|(_, b)| *b).sum();
        let span = now
            .duration_since(*first_ts)
            .as_secs_f64()
            .min(window.as_secs_f64())
            .max(0.25);
        total as f64 / span
    }
}

pub fn parse_size(input: &str) -> Result<u64> {
    let s = input.trim();
    if s.is_empty() {
        bail!("Empty size specification");
    }
    let split_idx = s
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(s.len());
    let num: f64 = s[..split_idx]
        .parse()
        .map_err(|_| anyhow::anyhow!("Invalid size number: {input:?}"))?;
    let unit = s[split_idx..].trim().to_ascii_lowercase();
    let mult: f64 = match unit.as_str() {
        "" | "b" => 1.0,
        "k" | "kb" | "kib" => 1024.0,
        "m" | "mb" | "mib" => 1024.0 * 1024.0,
        "g" | "gb" | "gib" => 1024.0 * 1024.0 * 1024.0,
        "t" | "tb" | "tib" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
        _ => bail!("Unsupported size unit: {unit:?}"),
    };
    if num < 0.0 {
        bail!("Size must be non-negative");
    }
    Ok((num * mult) as u64)
}

pub fn format_bytes(bytes: f64) -> String {
    let mut val = bytes.max(0.0);
    let units = ["B", "KiB", "MiB", "GiB", "TiB"];
    for unit in units {
        if val < 1024.0 || unit == "TiB" {
            return if unit == "B" {
                format!("{} B", val as u64)
            } else {
                format!("{val:.2} {unit}")
            };
        }
        val /= 1024.0;
    }
    format!("{val:.2} TiB")
}

pub fn format_speed(bps: f64) -> String {
    format!("{}/s", format_bytes(bps))
}

pub fn format_eta(seconds: Option<f64>) -> String {
    let Some(s) = seconds else {
        return "--:--".to_string();
    };
    if !s.is_finite() || s < 0.0 || s > 99.0 * 3600.0 {
        return "--:--".to_string();
    }
    let total = s as u64;
    let h = total / 3600;
    let m = (total % 3600) / 60;
    let sec = total % 60;
    if h > 0 {
        format!("{h}:{m:02}:{sec:02}")
    } else {
        format!("{m:02}:{sec:02}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_size() {
        assert_eq!(parse_size("100M").unwrap(), 100 * 1024 * 1024);
        assert_eq!(parse_size("16MB").unwrap(), 16 * 1024 * 1024);
        assert_eq!(parse_size("1G").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_size("512K").unwrap(), 512 * 1024);
    }
}
