use crate::models::{NetworkInterface, ProbeResult};
use anyhow::{bail, Context, Result};
use reqwest::{header, Client};
use std::path::Path;
use std::time::Duration;

pub fn build_interface_client(
    iface: &NetworkInterface,
    connect_timeout: Duration,
    read_timeout: Duration,
    use_so_bindtodevice: bool,
) -> Result<Client> {
    let mut builder = Client::builder()
        .user_agent("paralleldl/0.1 (Linux)")
        .connect_timeout(connect_timeout)
        .read_timeout(read_timeout);

    #[cfg(target_os = "linux")]
    if use_so_bindtodevice && iface.name != "default" && !iface.name.is_empty() {
        builder = builder.interface(&iface.name);
    }

    if let Some(ip) = iface.ip {
        builder = builder.local_address(ip);
    }

    builder
        .build()
        .with_context(|| format!("Failed to build HTTP client for interface {}", iface.name))
}

fn extract_filename(
    final_url: &str,
    headers: &header::HeaderMap,
    override_name: Option<&str>,
) -> String {
    if let Some(name) = override_name {
        if let Some(base) = Path::new(name).file_name().and_then(|s| s.to_str()) {
            if !base.is_empty() {
                return base.to_string();
            }
        }
    }
    if let Some(cd) = headers
        .get(header::CONTENT_DISPOSITION)
        .and_then(|v| v.to_str().ok())
    {
        for part in cd.split(';') {
            let trimmed = part.trim();
            if let Some(rest) = trimmed
                .strip_prefix("filename*=")
                .or_else(|| trimmed.strip_prefix("filename="))
            {
                let cleaned = rest
                    .trim_start_matches("UTF-8''")
                    .trim_start_matches("utf-8''")
                    .trim_matches('"')
                    .trim();
                if let Some(base) = Path::new(cleaned).file_name().and_then(|s| s.to_str()) {
                    if !base.is_empty() {
                        return base.to_string();
                    }
                }
            }
        }
    }
    if let Ok(parsed) = reqwest::Url::parse(final_url) {
        if let Some(mut segs) = parsed.path_segments() {
            if let Some(last) = segs.next_back() {
                if !last.is_empty() {
                    return last.to_string();
                }
            }
        }
    }
    "download.bin".to_string()
}

fn parse_content_range_total(val: &str) -> Option<u64> {
    let (_, total_str) = val.rsplit_once('/')?;
    total_str.trim().parse::<u64>().ok()
}

pub async fn probe_url(
    url: &str,
    iface: &NetworkInterface,
    output_name: Option<&str>,
    connect_timeout: Duration,
    read_timeout: Duration,
    use_so_bindtodevice: bool,
) -> Result<ProbeResult> {
    let client = build_interface_client(iface, connect_timeout, read_timeout, use_so_bindtodevice)?;

    let mut total_size: Option<u64> = None;
    let mut supports_ranges = false;
    let mut etag: Option<String> = None;
    let mut last_modified: Option<String> = None;
    let mut final_url = url.to_string();
    let mut head_headers = header::HeaderMap::new();

    // Step 1: Try HEAD
    if let Ok(resp) = client.head(url).send().await {
        if resp.status().is_success() || resp.status().is_redirection() {
            final_url = resp.url().to_string();
            head_headers = resp.headers().clone();
            if let Some(cl) = head_headers
                .get(header::CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
            {
                total_size = Some(cl);
            }
            if head_headers
                .get(header::ACCEPT_RANGES)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.eq_ignore_ascii_case("bytes"))
                .unwrap_or(false)
            {
                supports_ranges = true;
            }
            etag = head_headers
                .get(header::ETAG)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());
            last_modified = head_headers
                .get(header::LAST_MODIFIED)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());
        }
    }

    // Step 2: Confirm Range support via `Range: bytes=0-0` if ambiguous or HEAD failed
    if !supports_ranges || total_size.is_none() {
        let resp = client
            .get(&final_url)
            .header(header::RANGE, "bytes=0-0")
            .send()
            .await
            .context("Range probe GET request failed")?;

        let status = resp.status();
        final_url = resp.url().to_string();
        let headers = resp.headers().clone();

        if status == reqwest::StatusCode::PARTIAL_CONTENT {
            supports_ranges = true;
            if let Some(cr) = headers
                .get(header::CONTENT_RANGE)
                .and_then(|v| v.to_str().ok())
            {
                if let Some(sz) = parse_content_range_total(cr) {
                    total_size = Some(sz);
                }
            }
        } else if status.is_success() {
            supports_ranges = false;
            if let Some(cl) = headers
                .get(header::CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
            {
                total_size = Some(cl);
            }
        } else {
            bail!("Server returned HTTP {} for {}", status.as_u16(), url);
        }

        if etag.is_none() {
            etag = headers
                .get(header::ETAG)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());
        }
        if last_modified.is_none() {
            last_modified = headers
                .get(header::LAST_MODIFIED)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());
        }

        let filename = extract_filename(&final_url, &headers, output_name);
        return Ok(ProbeResult {
            url: url.to_string(),
            final_url,
            filename,
            total_size,
            supports_ranges: supports_ranges && total_size.unwrap_or(0) > 0,
            etag,
            last_modified,
        });
    }

    let filename = extract_filename(&final_url, &head_headers, output_name);
    Ok(ProbeResult {
        url: url.to_string(),
        final_url,
        filename,
        total_size,
        supports_ranges: supports_ranges && total_size.unwrap_or(0) > 0,
        etag,
        last_modified,
    })
}
