use crate::models::{ChunkTask, ProbeResult};
use anyhow::{Context, Result};
use chrono::Local;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

pub fn merge_ranges(ranges: &[[u64; 2]]) -> Vec<[u64; 2]> {
    let mut cleaned: Vec<[u64; 2]> = ranges.iter().copied().filter(|[s, e]| e >= s).collect();
    cleaned.sort_by_key(|[s, e]| (*s, *e));
    if cleaned.is_empty() {
        return Vec::new();
    }
    let mut merged = vec![cleaned[0]];
    for [s, e] in cleaned.into_iter().skip(1) {
        let last = merged.last_mut().unwrap();
        if s <= last[1].saturating_add(1) {
            if e > last[1] {
                last[1] = e;
            }
        } else {
            merged.push([s, e]);
        }
    }
    merged
}

pub fn missing_ranges(total_size: u64, completed: &[[u64; 2]]) -> Vec<(u64, u64)> {
    if total_size == 0 {
        return Vec::new();
    }
    let merged = merge_ranges(completed);
    let mut gaps = Vec::new();
    let mut cursor = 0u64;
    for [s, e] in merged {
        if s > cursor {
            gaps.push((cursor, (s - 1).min(total_size - 1)));
        }
        cursor = cursor.max(e.saturating_add(1));
    }
    if cursor < total_size {
        gaps.push((cursor, total_size - 1));
    }
    gaps
}

pub fn slice_ranges_into_chunks(
    gaps: &[(u64, u64)],
    chunk_size: u64,
    min_chunk_size: u64,
) -> Vec<ChunkTask> {
    let effective = chunk_size.max(min_chunk_size).max(1);
    let mut chunks = Vec::new();
    for &(gap_start, gap_end) in gaps {
        let mut pos = gap_start;
        while pos <= gap_end {
            let remaining = gap_end - pos + 1;
            let end = if remaining <= effective {
                gap_end
            } else {
                let candidate = pos + effective - 1;
                let tail = gap_end - candidate;
                if tail > 0 && tail < min_chunk_size {
                    gap_end
                } else {
                    candidate
                }
            };
            chunks.push(ChunkTask {
                priority: 10,
                start: pos,
                end,
                retries: 0,
            });
            pos = end.saturating_add(1);
        }
    }
    chunks
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgressReport {
    pub version: u32,
    pub url: String,
    pub filename: String,
    pub total_size: Option<u64>,
    pub chunk_size: u64,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub completed_bytes: u64,
    pub completed_ranges: Vec<[u64; 2]>,
    pub interface_bytes: BTreeMap<String, u64>,
    pub updated_at: String,
}

#[derive(Debug)]
pub struct ProgressStateStore {
    pub target_file_path: PathBuf,
    pub state_path: PathBuf,
    pub report: ProgressReport,
}

impl ProgressStateStore {
    pub fn new(target_file_path: PathBuf) -> Self {
        let mut state_os = target_file_path.as_os_str().to_os_string();
        state_os.push(".pldl.json");
        let state_path = PathBuf::from(state_os);
        let filename = target_file_path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("download.bin")
            .to_string();

        Self {
            target_file_path,
            state_path,
            report: ProgressReport {
                version: 1,
                url: String::new(),
                filename,
                total_size: None,
                chunk_size: 0,
                etag: None,
                last_modified: None,
                completed_bytes: 0,
                completed_ranges: Vec::new(),
                interface_bytes: BTreeMap::new(),
                updated_at: String::new(),
            },
        }
    }

    pub fn init_from_probe(&mut self, probe: &ProbeResult, chunk_size: u64) {
        self.report.url = probe.url.clone();
        self.report.filename = probe.filename.clone();
        self.report.total_size = probe.total_size;
        self.report.chunk_size = chunk_size;
        self.report.etag = probe.etag.clone();
        self.report.last_modified = probe.last_modified.clone();
    }

    pub fn completed_bytes(&self) -> u64 {
        self.report.completed_bytes
    }

    pub fn record_range(&mut self, start: u64, end: u64, iface_name: &str) {
        if end < start {
            return;
        }
        self.report.completed_ranges.push([start, end]);
        self.report.completed_ranges = merge_ranges(&self.report.completed_ranges);
        self.report.completed_bytes = self
            .report
            .completed_ranges
            .iter()
            .map(|[s, e]| e - s + 1)
            .sum();
        *self
            .report
            .interface_bytes
            .entry(iface_name.to_string())
            .or_insert(0) += end - start + 1;
    }

    pub fn load_if_compatible(&mut self, probe: &ProbeResult) -> (bool, String) {
        if !self.state_path.exists() {
            return (false, "No existing progress file.".to_string());
        }
        let raw = match fs::read_to_string(&self.state_path) {
            Ok(s) => s,
            Err(e) => return (false, format!("Unreadable progress JSON ({e}); starting fresh.")),
        };
        let saved: ProgressReport = match serde_json::from_str(&raw) {
            Ok(r) => r,
            Err(e) => return (false, format!("Corrupt progress JSON ({e}); starting fresh.")),
        };

        if probe.total_size.is_some() && saved.total_size != probe.total_size {
            return (false, "Remote file size changed; starting fresh.".to_string());
        }
        if let (Some(a), Some(b)) = (&probe.etag, &saved.etag) {
            if a != b {
                return (false, "Remote ETag changed; starting fresh.".to_string());
            }
        } else if probe.etag.is_none() {
            if let (Some(a), Some(b)) = (&probe.last_modified, &saved.last_modified) {
                if a != b {
                    return (
                        false,
                        "Remote Last-Modified changed; starting fresh.".to_string(),
                    );
                }
            }
        }
        if !self.target_file_path.exists() {
            return (false, "Partial target file missing; starting fresh.".to_string());
        }

        self.report = saved;
        self.report.completed_ranges = merge_ranges(&self.report.completed_ranges);
        self.report.completed_bytes = self
            .report
            .completed_ranges
            .iter()
            .map(|[s, e]| e - s + 1)
            .sum();
        let name = self
            .state_path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("progress.json");
        (
            true,
            format!("Resumed {} bytes from {name}.", self.report.completed_bytes),
        )
    }

    pub fn save_atomic(&mut self) -> Result<()> {
        self.report.updated_at = Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, false);
        let json = serde_json::to_string_pretty(&self.report)?;
        let mut tmp_os = self.state_path.as_os_str().to_os_string();
        tmp_os.push(".tmp");
        let tmp_path = PathBuf::from(tmp_os);

        if let Some(parent) = Path::new(&self.state_path).parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }
        {
            let mut fh = File::create(&tmp_path)
                .with_context(|| format!("Failed to create {}", tmp_path.display()))?;
            fh.write_all(json.as_bytes())?;
            fh.sync_all()?;
        }
        fs::rename(&tmp_path, &self.state_path)?;
        Ok(())
    }

    pub fn remove(&self) {
        let _ = fs::remove_file(&self.state_path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_and_gaps() {
        let merged = merge_ranges(&[[0, 99], [100, 199], [300, 399], [150, 249]]);
        assert_eq!(merged, vec![[0, 249], [300, 399]]);
        let gaps = missing_ranges(500, &merged);
        assert_eq!(gaps, vec![(250, 299), (400, 499)]);
    }

    #[test]
    fn test_slice_respects_min_chunk_size() {
        let chunks = slice_ranges_into_chunks(&[(0, 249)], 100, 60);
        let pairs: Vec<(u64, u64)> = chunks.iter().map(|c| (c.start, c.end)).collect();
        assert_eq!(pairs, vec![(0, 99), (100, 249)]);

        let chunks2 = slice_ranges_into_chunks(&[(0, 249)], 100, 20);
        let pairs2: Vec<(u64, u64)> = chunks2.iter().map(|c| (c.start, c.end)).collect();
        assert_eq!(pairs2, vec![(0, 99), (100, 199), (200, 249)]);
    }
}
