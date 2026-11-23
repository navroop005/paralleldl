use crate::models::NetworkInterface;
use serde::Deserialize;
use std::collections::HashMap;
use std::net::IpAddr;
use std::process::{Command, Stdio};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command as TokioCommand;
use tokio::sync::watch;

const VIRTUAL_PREFIXES: &[&str] = &[
    "lo", "docker", "br-", "veth", "virbr", "cni", "flannel", "kube", "podman", "dummy", "tun",
    "tap",
];

#[derive(Debug, Deserialize)]
struct IpAddrEntry {
    ifindex: Option<u32>,
    ifname: String,
    #[serde(default)]
    flags: Vec<String>,
    operstate: Option<String>,
    #[serde(default)]
    addr_info: Vec<AddrInfo>,
}

#[derive(Debug, Deserialize)]
struct AddrInfo {
    family: Option<String>,
    local: Option<String>,
}

#[derive(Debug, Deserialize)]
struct IpRouteEntry {
    dst: Option<String>,
    gateway: Option<String>,
    dev: Option<String>,
    table: Option<serde_json::Value>,
}

fn run_ip_json(args: &[&str]) -> Option<String> {
    let out = Command::new("ip").arg("-j").args(args).output().ok()?;
    if !out.status.success() {
        return None;
    }
    String::from_utf8(out.stdout).ok()
}

fn is_virtual(name: &str, include_virtual: bool) -> bool {
    if include_virtual {
        return name == "lo";
    }
    VIRTUAL_PREFIXES
        .iter()
        .any(|p| name == *p || name.starts_with(p))
}

/// Discover active Linux IPv4 network interfaces and default gateways using `ip -j`.
pub fn discover_linux_interfaces(
    include_down: bool,
    include_virtual: bool,
) -> Vec<NetworkInterface> {
    let Some(addr_raw) = run_ip_json(&["-4", "addr", "show"]) else {
        return Vec::new();
    };
    let entries: Vec<IpAddrEntry> = serde_json::from_str(&addr_raw).unwrap_or_default();

    let mut gateways: HashMap<String, String> = HashMap::new();
    if let Some(route_raw) = run_ip_json(&["-4", "route", "show", "table", "all"]) {
        let routes: Vec<IpRouteEntry> = serde_json::from_str(&route_raw).unwrap_or_default();
        for r in routes {
            if r.dst.as_deref() == Some("default") {
                if let (Some(dev), Some(gw)) = (r.dev, r.gateway) {
                    let is_main = r
                        .table
                        .as_ref()
                        .and_then(|v| v.as_str())
                        .map(|s| s == "main")
                        .unwrap_or(true);
                    if !gateways.contains_key(&dev) || is_main {
                        gateways.insert(dev, gw);
                    }
                }
            }
        }
    }

    let mut results = Vec::new();
    for entry in entries {
        let is_lo = entry.ifname == "lo" || entry.flags.iter().any(|f| f == "LOOPBACK");
        if is_lo || is_virtual(&entry.ifname, include_virtual) {
            continue;
        }
        let oper = entry
            .operstate
            .unwrap_or_else(|| "UNKNOWN".to_string())
            .to_ascii_uppercase();
        let has_up = entry.flags.iter().any(|f| f == "UP");
        let has_lower = entry.flags.iter().any(|f| f == "LOWER_UP");
        let is_up = oper == "UP" || (has_up && has_lower);
        if !is_up && !include_down {
            continue;
        }

        let ipv4 = entry.addr_info.iter().find_map(|ai| {
            if ai.family.as_deref() == Some("inet") {
                ai.local.as_deref()?.parse::<IpAddr>().ok()
            } else {
                None
            }
        });
        let Some(ip) = ipv4 else {
            continue;
        };

        let gateway = gateways.get(&entry.ifname).cloned();
        results.push(NetworkInterface {
            name: entry.ifname,
            ip: Some(ip),
            gateway,
            operstate: if is_up { "UP".to_string() } else { oper },
            ifindex: entry.ifindex.unwrap_or(0),
        });
    }

    results.sort_by(|a, b| {
        let ga = if a.gateway.is_some() { 0 } else { 1 };
        let gb = if b.gateway.is_some() { 0 } else { 1 };
        ga.cmp(&gb).then_with(|| a.name.cmp(&b.name))
    });
    results
}

/// Spawns a Linux Netlink monitor (`ip monitor link addr route`) + periodic fallback poll
/// that invokes `on_change` whenever connected interfaces, IPs, or gateways change.
pub fn spawn_interface_watcher<F, Fut>(
    mut stop_rx: watch::Receiver<bool>,
    mut rescan_rx: tokio::sync::mpsc::UnboundedReceiver<()>,
    on_change: F,
) -> tokio::task::JoinHandle<()>
where
    F: Fn(Vec<NetworkInterface>) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send,
{
    tokio::spawn(async move {
        let mut child = TokioCommand::new("ip")
            .args(["monitor", "link", "addr", "route"])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .ok();

        let mut reader = child
            .as_mut()
            .and_then(|c| c.stdout.take())
            .map(|out| BufReader::new(out).lines());

        let mut last_snapshot: Vec<(String, Option<IpAddr>, Option<String>)> = Vec::new();
        let mut poll_interval = tokio::time::interval(Duration::from_secs(3));
        poll_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            let force = tokio::select! {
                _ = stop_rx.changed() => {
                    if *stop_rx.borrow() {
                        break;
                    }
                    false
                }
                _ = poll_interval.tick() => false,
                Some(_) = rescan_rx.recv() => true,
                line = async {
                    match &mut reader {
                        Some(r) => r.next_line().await.ok().flatten(),
                        None => std::future::pending::<Option<String>>().await,
                    }
                } => {
                    if line.is_none() {
                        reader = None;
                        false
                    } else {
                        // Brief debounce for bursts of Netlink link/addr events
                        tokio::time::sleep(Duration::from_millis(300)).await;
                        false
                    }
                }
            };

            let ifaces = tokio::task::spawn_blocking(|| discover_linux_interfaces(false, false))
                .await
                .unwrap_or_default();
            let snap: Vec<(String, Option<IpAddr>, Option<String>)> = ifaces
                .iter()
                .map(|i| (i.name.clone(), i.ip, i.gateway.clone()))
                .collect();

            if force || snap != last_snapshot {
                last_snapshot = snap;
                on_change(ifaces).await;
            }
        }

        if let Some(mut c) = child {
            let _ = c.kill().await;
        }
    })
}
