use crate::models::{
    format_bytes, ChunkTask, DownloadConfig, InterfaceRuntime, InterfaceStatus, NetworkInterface,
    ProbeResult,
};
use crate::net_linux::{discover_linux_interfaces, spawn_interface_watcher};
use crate::probe::{build_interface_client, probe_url};
use crate::state::{missing_ranges, slice_ranges_into_chunks, ProgressStateStore};
use crate::writer::PositionalFileWriter;
use anyhow::{bail, Result};
use chrono::Local;
use futures_util::StreamExt;
use reqwest::header;
use std::collections::{BTreeMap, BinaryHeap, HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch, Mutex, Notify};
use tokio::task::JoinHandle;

#[derive(Debug, Clone)]
pub struct InterfaceSnapshot {
    pub name: String,
    pub ip: String,
    pub bind_mode: &'static str,
    pub status: InterfaceStatus,
    pub probe_countdown_secs: Option<u64>,
    pub active_workers: usize,
    pub target_workers: usize,
    pub speed_bps: f64,
    pub downloaded_bytes: u64,
    pub chunks_completed: u64,
    pub total_errors: u32,
}

#[derive(Debug, Clone)]
pub struct DashboardSnapshot {
    pub filename: String,
    pub total_size: Option<u64>,
    pub downloaded_bytes: u64,
    pub total_speed_bps: f64,
    pub eta_seconds: Option<f64>,
    pub queue_len: usize,
    pub is_paused: bool,
    pub is_finished: bool,
    pub status_message: String,
    pub interfaces: Vec<InterfaceSnapshot>,
    pub events: Vec<String>,
}

struct SharedState {
    probe: Option<ProbeResult>,
    state: Option<ProgressStateStore>,
    queue: BinaryHeap<ChunkTask>,
    in_flight_count: usize,
    interfaces: BTreeMap<String, InterfaceRuntime>,
    iface_cancels: HashMap<String, watch::Sender<bool>>,
    worker_handles: HashMap<String, Vec<JoinHandle<()>>>,
    is_paused: bool,
    is_finished: bool,
    is_simple_mode: bool,
    status_message: String,
    events: VecDeque<String>,
}

impl SharedState {
    fn log(&mut self, msg: impl Into<String>) {
        let ts = Local::now().format("%H:%M:%S");
        self.events.push_back(format!("[{ts}] {}", msg.into()));
        while self.events.len() > 14 {
            self.events.pop_front();
        }
    }
}

pub struct DownloadEngine {
    pub config: DownloadConfig,
    shared: Mutex<SharedState>,
    queue_notify: Notify,
    recheck_notify: Notify,
    rebalance_notify: Notify,
    pause_tx: watch::Sender<bool>,
    stop_tx: watch::Sender<bool>,
    rescan_tx: mpsc::UnboundedSender<()>,
    rescan_rx: Mutex<Option<mpsc::UnboundedReceiver<()>>>,
}

impl DownloadEngine {
    pub fn new(config: DownloadConfig) -> Arc<Self> {
        let (pause_tx, _) = watch::channel(false);
        let (stop_tx, _) = watch::channel(false);
        let (rescan_tx, rescan_rx) = mpsc::unbounded_channel();

        Arc::new(Self {
            config,
            shared: Mutex::new(SharedState {
                probe: None,
                state: None,
                queue: BinaryHeap::new(),
                in_flight_count: 0,
                interfaces: BTreeMap::new(),
                iface_cancels: HashMap::new(),
                worker_handles: HashMap::new(),
                is_paused: false,
                is_finished: false,
                is_simple_mode: false,
                status_message: "Initializing...".to_string(),
                events: VecDeque::new(),
            }),
            queue_notify: Notify::new(),
            recheck_notify: Notify::new(),
            rebalance_notify: Notify::new(),
            pause_tx,
            stop_tx,
            rescan_tx,
            rescan_rx: Mutex::new(Some(rescan_rx)),
        })
    }

    // ------------------------------------------------------------------
    // Controls (called from Ratatui keypresses or SIGINT)
    // ------------------------------------------------------------------
    pub async fn toggle_pause(&self) {
        let mut st = self.shared.lock().await;
        if st.is_paused {
            st.is_paused = false;
            st.status_message = "Downloading".to_string();
            st.log("Resumed download.");
            let _ = self.pause_tx.send(false);
        } else {
            st.is_paused = true;
            st.status_message = "Paused by user".to_string();
            st.log("Paused download; flushing active buffers...");
            let _ = self.pause_tx.send(true);
            if let Some(store) = st.state.as_mut() {
                let _ = store.save_atomic();
            }
        }
    }

    pub async fn request_stop(&self) {
        let mut st = self.shared.lock().await;
        if !*self.stop_tx.borrow() {
            st.status_message = "Stopping and saving progress...".to_string();
            st.log("Graceful stop requested; flushing buffers & saving JSON...");
            let _ = self.stop_tx.send(true);
            let _ = self.pause_tx.send(false);
            self.queue_notify.notify_waiters();
            self.recheck_notify.notify_waiters();
        }
    }

    pub async fn trigger_recheck_stopped(&self) {
        let mut st = self.shared.lock().await;
        let mut count = 0usize;
        for rt in st.interfaces.values_mut() {
            if rt.status == InterfaceStatus::Stopped {
                rt.next_probe_at = Some(Instant::now());
                count += 1;
            }
        }
        if count > 0 {
            st.log(format!(
                "Triggered immediate recheck on {count} stopped interface(s)."
            ));
        } else {
            st.log("No stopped interfaces to recheck.");
        }
        self.recheck_notify.notify_waiters();
    }

    pub async fn trigger_rescan(&self) {
        {
            let mut st = self.shared.lock().await;
            st.log("Rescanning Linux network interfaces...");
        }
        let _ = self.rescan_tx.send(());
    }

    pub async fn snapshot(&self) -> DashboardSnapshot {
        let mut st = self.shared.lock().await;
        let now = Instant::now();
        let window = Duration::from_secs(2);

        let filename = st
            .probe
            .as_ref()
            .map(|p| p.filename.clone())
            .unwrap_or_else(|| "Probing...".to_string());
        let total_size = st.probe.as_ref().and_then(|p| p.total_size);
        let downloaded_bytes = st
            .state
            .as_ref()
            .map(|s| s.completed_bytes())
            .unwrap_or(0);

        let mut total_speed = 0.0f64;
        let mut ifaces = Vec::with_capacity(st.interfaces.len());
        for rt in st.interfaces.values_mut() {
            let spd = rt.compute_speed_bps(window);
            total_speed += spd;
            let countdown = if rt.status == InterfaceStatus::Stopped {
                rt.next_probe_at
                    .and_then(|t| t.checked_duration_since(now))
                    .map(|d| d.as_secs())
            } else {
                None
            };
            let bind_mode = if self.config.use_so_bindtodevice && rt.interface.name != "default" {
                "SO_BIND"
            } else {
                "src-ip"
            };
            ifaces.push(InterfaceSnapshot {
                name: rt.interface.name.clone(),
                ip: rt
                    .interface
                    .ip
                    .map(|ip| ip.to_string())
                    .unwrap_or_else(|| "default".to_string()),
                bind_mode,
                status: rt.status,
                probe_countdown_secs: countdown,
                active_workers: rt.active_workers,
                target_workers: rt.target_workers,
                speed_bps: spd,
                downloaded_bytes: rt.downloaded_bytes,
                chunks_completed: rt.chunks_completed,
                total_errors: rt.total_errors,
            });
        }

        let eta_seconds = match (total_size, total_speed > 1.0) {
            (Some(tot), true) => Some(tot.saturating_sub(downloaded_bytes) as f64 / total_speed),
            _ => None,
        };

        DashboardSnapshot {
            filename,
            total_size,
            downloaded_bytes,
            total_speed_bps: total_speed,
            eta_seconds,
            queue_len: st.queue.len(),
            is_paused: st.is_paused,
            is_finished: st.is_finished,
            status_message: st.status_message.clone(),
            interfaces: ifaces,
            events: st.events.iter().cloned().collect(),
        }
    }

    // ------------------------------------------------------------------
    // Interface filtering & worker budget computation
    // ------------------------------------------------------------------
    fn filter_interfaces(&self, discovered: Vec<NetworkInterface>) -> Vec<NetworkInterface> {
        if let Some(ref wanted) = self.config.interfaces {
            return discovered
                .into_iter()
                .filter(|i| wanted.iter().any(|w| w == &i.name))
                .collect();
        }
        let with_gw: Vec<_> = discovered
            .iter()
            .filter(|i| i.gateway.is_some())
            .cloned()
            .collect();
        if !with_gw.is_empty() {
            with_gw
        } else {
            discovered
        }
    }

    fn compute_worker_targets(&self, active_names: &[String]) -> HashMap<String, usize> {
        let mut targets = HashMap::new();
        if active_names.is_empty() {
            return targets;
        }
        if let Some(per) = self.config.per_interface_connections {
            let n = per.max(1);
            for name in active_names {
                targets.insert(name.clone(), n);
            }
            return targets;
        }
        if let Some(total) = self.config.total_connections {
            let t = total.max(1);
            let base = t / active_names.len();
            let rem = t % active_names.len();
            let mut sorted = active_names.to_vec();
            sorted.sort();
            for (idx, name) in sorted.into_iter().enumerate() {
                targets.insert(name, base + usize::from(idx < rem));
            }
            return targets;
        }
        for name in active_names {
            targets.insert(name.clone(), 4);
        }
        targets
    }

    // ------------------------------------------------------------------
    // Main Entrypoint
    // ------------------------------------------------------------------
    pub async fn run(self: &Arc<Self>) -> Result<bool> {
        let discovered = tokio::task::spawn_blocking(|| discover_linux_interfaces(false, false))
            .await
            .unwrap_or_default();
        let mut selected = self.filter_interfaces(discovered);
        if selected.is_empty() {
            selected.push(NetworkInterface {
                name: "default".to_string(),
                ip: None,
                gateway: Some("auto".to_string()),
                operstate: "UP".to_string(),
                ifindex: 0,
            });
            self.shared
                .lock()
                .await
                .log("No explicit NICs found; falling back to OS default route.");
        }

        {
            let mut st = self.shared.lock().await;
            for iface in &selected {
                st.interfaces
                    .insert(iface.name.clone(), InterfaceRuntime::new(iface.clone()));
            }
            st.status_message = format!("Probing {}...", self.config.url);
        }

        // Probe URL with failover across active interfaces
        let mut probe_result = None;
        let mut probe_iface = selected[0].clone();
        let mut last_err = None;
        for cand in &selected {
            {
                let mut st = self.shared.lock().await;
                st.log(format!(
                    "Probing server via {} ({})...",
                    cand.name,
                    cand.ip
                        .map(|ip| ip.to_string())
                        .unwrap_or_else(|| "default".to_string())
                ));
            }
            match probe_url(
                &self.config.url,
                cand,
                self.config.output_name.as_deref(),
                self.config.connect_timeout,
                self.config.read_timeout,
                self.config.use_so_bindtodevice,
            )
            .await
            {
                Ok(p) => {
                    probe_result = Some(p);
                    probe_iface = cand.clone();
                    break;
                }
                Err(e) => {
                    self.shared
                        .lock()
                        .await
                        .log(format!("Probe failed on {}: {e}", cand.name));
                    last_err = Some(e);
                }
            }
        }

        let probe = match probe_result {
            Some(p) => p,
            None => bail!(
                "Failed to probe URL on all interfaces: {}",
                last_err
                    .map(|e| e.to_string())
                    .unwrap_or_else(|| "unknown error".to_string())
            ),
        };

        let target_path = self.config.output_dir.join(&probe.filename);
        let mut store = ProgressStateStore::new(target_path.clone());
        store.init_from_probe(&probe, self.config.chunk_size);

        if !probe.supports_ranges {
            {
                let mut st = self.shared.lock().await;
                st.probe = Some(probe.clone());
                st.state = Some(store);
                st.is_simple_mode = true;
                st.log("Server does not support HTTP Range requests; using single-stream download.");
            }
            return self.run_simple_download(probe, probe_iface, target_path).await;
        }

        let total_size = probe.total_size.unwrap_or(0);
        {
            let mut st = self.shared.lock().await;
            st.log(format!(
                "Range supported: {} ({}).",
                probe.filename,
                format_bytes(total_size as f64)
            ));
        }

        if self.config.resume {
            let (resumed, msg) = store.load_if_compatible(&probe);
            self.shared.lock().await.log(msg);
            if resumed && store.completed_bytes() >= total_size {
                let mut st = self.shared.lock().await;
                st.probe = Some(probe);
                st.state = Some(store);
                st.is_finished = true;
                st.status_message = "Already complete".to_string();
                st.log("Target file is already 100% downloaded.");
                return Ok(true);
            }
        } else {
            store.remove();
        }

        let writer = PositionalFileWriter::open(&target_path, Some(total_size))?;
        let gaps = missing_ranges(total_size, &store.report.completed_ranges);
        let chunks = slice_ranges_into_chunks(
            &gaps,
            self.config.chunk_size,
            self.config.min_chunk_size,
        );
        let chunk_count = chunks.len();
        store.save_atomic()?;

        {
            let mut st = self.shared.lock().await;
            st.probe = Some(probe.clone());
            st.state = Some(store);
            for c in chunks {
                st.queue.push(c);
            }
            let nic_count = st.interfaces.len();
            st.log(format!(
                "Queued {chunk_count} chunk(s) across {nic_count} interface(s)."
            ));
            st.status_message = "Downloading".to_string();
        }

        // Start Linux hot-plug watcher (removes disconnected NICs & adds newly connected NICs)
        let rescan_rx = self.rescan_rx.lock().await.take().unwrap();
        let watcher_engine = Arc::clone(self);
        let watcher_writer = writer.clone();
        let watcher_handle = spawn_interface_watcher(
            self.stop_tx.subscribe(),
            rescan_rx,
            move |ifaces| {
                let eng = Arc::clone(&watcher_engine);
                let wr = watcher_writer.clone();
                async move {
                    eng.on_interfaces_changed(ifaces, wr).await;
                }
            },
        );

        // Start health prober for connected-but-STOPPED interfaces
        let health_engine = Arc::clone(self);
        let health_writer = writer.clone();
        let health_handle = tokio::spawn(async move {
            health_engine.health_probe_loop(health_writer).await;
        });

        // Start periodic JSON checkpoint saver
        let ckpt_engine = Arc::clone(self);
        let ckpt_handle = tokio::spawn(async move {
            ckpt_engine.periodic_checkpoint_loop().await;
        });

        self.rebalance_workers(writer.clone()).await;

        // Wait until download finishes or stop is requested
        loop {
            if *self.stop_tx.borrow() {
                break;
            }
            {
                let mut st = self.shared.lock().await;
                let done = st
                    .state
                    .as_ref()
                    .map(|s| s.completed_bytes())
                    .unwrap_or(0);
                if done >= total_size && st.in_flight_count == 0 && st.queue.is_empty() {
                    st.is_finished = true;
                    st.status_message = "Completed".to_string();
                    st.log("Download complete!");
                    break;
                }
                let active_any = st.interfaces.values().any(|rt| {
                    matches!(rt.status, InterfaceStatus::Active | InterfaceStatus::Degraded)
                });
                if !st.is_paused {
                    if st.interfaces.is_empty() {
                        st.status_message = "Waiting for a network interface to connect...".to_string();
                    } else if !active_any {
                        st.status_message = "All interfaces stopped — probing for recovery...".to_string();
                    } else {
                        st.status_message = "Downloading".to_string();
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
        }

        // Clean shutdown
        let _ = self.stop_tx.send(true);
        let _ = self.pause_tx.send(false);
        self.queue_notify.notify_waiters();
        self.recheck_notify.notify_waiters();

        // Cancel all per-interface channels & await workers
        let handles: Vec<JoinHandle<()>> = {
            let mut st = self.shared.lock().await;
            for tx in st.iface_cancels.values() {
                let _ = tx.send(true);
            }
            st.worker_handles
                .drain()
                .flat_map(|(_, v)| v.into_iter())
                .collect()
        };
        for h in handles {
            let _ = h.await;
        }
        let _ = watcher_handle.await;
        health_handle.abort();
        ckpt_handle.abort();

        let _ = writer.sync_all();
        let finished = {
            let mut st = self.shared.lock().await;
            if let Some(store) = st.state.as_mut() {
                let _ = store.save_atomic();
            }
            st.is_finished
        };

        Ok(finished)
    }

    // ------------------------------------------------------------------
    // Simple Single-Stream Fallback
    // ------------------------------------------------------------------
    async fn run_simple_download(
        self: &Arc<Self>,
        probe: ProbeResult,
        iface: NetworkInterface,
        target_path: std::path::PathBuf,
    ) -> Result<bool> {
        let writer = PositionalFileWriter::open(&target_path, probe.total_size)?;
        let client = build_interface_client(
            &iface,
            self.config.connect_timeout,
            self.config.read_timeout,
            self.config.use_so_bindtodevice,
        )?;
        {
            let mut st = self.shared.lock().await;
            if let Some(rt) = st.interfaces.get_mut(&iface.name) {
                rt.target_workers = 1;
                rt.active_workers = 1;
            }
            st.status_message = "Downloading (single stream)".to_string();
        }

        let mut offset = 0u64;
        let mut stop_rx = self.stop_tx.subscribe();
        let mut pause_rx = self.pause_tx.subscribe();
        let mut ok = false;

        match client.get(&probe.final_url).send().await {
            Ok(resp) if resp.status().is_success() => {
                let mut stream = resp.bytes_stream();
                let mut buf = Vec::with_capacity(self.config.flush_size);
                while let Some(item) = stream.next().await {
                    if *stop_rx.borrow() {
                        break;
                    }
                    while *pause_rx.borrow() && !*stop_rx.borrow() {
                        tokio::select! {
                            _ = pause_rx.changed() => {}
                            _ = stop_rx.changed() => break,
                        }
                    }
                    if *stop_rx.borrow() {
                        break;
                    }
                    match item {
                        Ok(bytes) => {
                            let n = bytes.len() as u64;
                            buf.extend_from_slice(&bytes);
                            if let Some(rt) = self.shared.lock().await.interfaces.get_mut(&iface.name) {
                                rt.record_bytes(n);
                            }
                            if buf.len() >= self.config.flush_size {
                                let chunk = std::mem::take(&mut buf);
                                let len = chunk.len() as u64;
                                writer.pwrite(offset, chunk).await?;
                                if let Some(store) = self.shared.lock().await.state.as_mut() {
                                    store.record_range(offset, offset + len - 1, &iface.name);
                                }
                                offset += len;
                            }
                        }
                        Err(e) => {
                            self.shared
                                .lock()
                                .await
                                .log(format!("Single-stream error: {e}"));
                            break;
                        }
                    }
                }
                if !buf.is_empty() {
                    let len = buf.len() as u64;
                    writer.pwrite(offset, buf).await?;
                    if let Some(store) = self.shared.lock().await.state.as_mut() {
                        store.record_range(offset, offset + len - 1, &iface.name);
                    }
                    offset += len;
                }
                if !*stop_rx.borrow() {
                    ok = true;
                }
            }
            Ok(resp) => {
                self.shared
                    .lock()
                    .await
                    .log(format!("Single-stream HTTP {}", resp.status()));
            }
            Err(e) => {
                self.shared
                    .lock()
                    .await
                    .log(format!("Single-stream connect error: {e}"));
            }
        }

        let _ = writer.sync_all();
        let mut st = self.shared.lock().await;
        if let Some(rt) = st.interfaces.get_mut(&iface.name) {
            rt.active_workers = 0;
        }
        if ok {
            st.is_finished = true;
            st.status_message = "Completed".to_string();
            st.log(format!(
                "Single-stream download complete ({}).",
                format_bytes(offset as f64)
            ));
        }
        if let Some(store) = st.state.as_mut() {
            let _ = store.save_atomic();
        }
        Ok(ok)
    }

    // ------------------------------------------------------------------
    // Dynamic Worker Management & Hot-Plug (Fixes disconnected NIC bug!)
    // ------------------------------------------------------------------
    async fn on_interfaces_changed(
        self: &Arc<Self>,
        discovered: Vec<NetworkInterface>,
        writer: PositionalFileWriter,
    ) {
        if *self.stop_tx.borrow() {
            return;
        }
        let filtered = self.filter_interfaces(discovered);
        let discovered_map: HashMap<String, NetworkInterface> = filtered
            .into_iter()
            .map(|i| (i.name.clone(), i))
            .collect();

        let mut to_cancel_handles: Vec<JoinHandle<()>> = Vec::new();
        {
            let mut st = self.shared.lock().await;
            if st.is_simple_mode {
                return;
            }

            // 1. Remove any interface that physically disconnected (do NOT keep showing or retrying!)
            let disconnected: Vec<String> = st
                .interfaces
                .keys()
                .filter(|name| name.as_str() != "default" && !discovered_map.contains_key(*name))
                .cloned()
                .collect();

            for name in disconnected {
                st.interfaces.remove(&name);
                if let Some(tx) = st.iface_cancels.remove(&name) {
                    let _ = tx.send(true);
                }
                if let Some(handles) = st.worker_handles.remove(&name) {
                    to_cancel_handles.extend(handles);
                }
                st.log(format!(
                    "Interface {name} disconnected; removed and re-routing chunks."
                ));
            }

            // 2. Add newly connected interfaces or restart on IP/gateway change
            for (name, iface) in discovered_map {
                if let Some(rt) = st.interfaces.get_mut(&name) {
                    if rt.interface.ip != iface.ip || rt.interface.gateway != iface.gateway {
                        let old_ip = rt
                            .interface
                            .ip
                            .map(|ip| ip.to_string())
                            .unwrap_or_else(|| "none".to_string());
                        let new_ip = iface
                            .ip
                            .map(|ip| ip.to_string())
                            .unwrap_or_else(|| "none".to_string());
                        rt.interface = iface;
                        rt.status = InterfaceStatus::Active;
                        rt.consecutive_errors = 0;
                        rt.stop_count = 0;
                        rt.next_probe_at = None;
                        if let Some(tx) = st.iface_cancels.remove(&name) {
                            let _ = tx.send(true);
                        }
                        if let Some(handles) = st.worker_handles.remove(&name) {
                            to_cancel_handles.extend(handles);
                        }
                        st.log(format!(
                            "Interface {name} changed ({old_ip} -> {new_ip}); restarting workers."
                        ));
                    } else if rt.status == InterfaceStatus::Stopped {
                        // Still connected & link event fired: schedule immediate probe
                        rt.next_probe_at = Some(Instant::now());
                        self.recheck_notify.notify_waiters();
                    }
                } else {
                    let ip_str = iface
                        .ip
                        .map(|ip| ip.to_string())
                        .unwrap_or_else(|| "default".to_string());
                    st.interfaces.insert(name.clone(), InterfaceRuntime::new(iface));
                    st.log(format!("Hot-plug: connected {name} ({ip_str})."));
                }
            }
        }

        // Wait for disconnected/restarting workers to flush and return their chunks
        for h in to_cancel_handles {
            let _ = h.await;
        }
        self.rebalance_workers(writer).await;
    }

    async fn rebalance_workers(self: &Arc<Self>, writer: PositionalFileWriter) {
        if *self.stop_tx.borrow() {
            return;
        }
        let mut spawn_list: Vec<(NetworkInterface, watch::Receiver<bool>, usize)> = Vec::new();
        {
            let mut st = self.shared.lock().await;
            let active_names: Vec<String> = st
                .interfaces
                .iter()
                .filter(|(_, rt)| {
                    matches!(rt.status, InterfaceStatus::Active | InterfaceStatus::Degraded)
                })
                .map(|(k, _)| k.clone())
                .collect();
            let targets = self.compute_worker_targets(&active_names);

            // Clean finished worker handles first
            for handles in st.worker_handles.values_mut() {
                handles.retain(|h| !h.is_finished());
            }

            let names: Vec<String> = st.interfaces.keys().cloned().collect();
            for name in names {
                let desired = targets.get(&name).copied().unwrap_or(0);
                let iface = {
                    let rt = st.interfaces.get_mut(&name).unwrap();
                    rt.target_workers = desired;
                    rt.interface.clone()
                };

                let cur_len = st.worker_handles.get(&name).map(|v| v.len()).unwrap_or(0);
                if desired == 0 && cur_len > 0 {
                    if let Some(tx) = st.iface_cancels.remove(&name) {
                        let _ = tx.send(true);
                    }
                } else if cur_len < desired {
                    let cancel_tx = st
                        .iface_cancels
                        .entry(name.clone())
                        .or_insert_with(|| watch::channel(false).0);
                    for _ in cur_len..desired {
                        spawn_list.push((iface.clone(), cancel_tx.subscribe(), desired));
                    }
                }
            }
        }

        for (iface, cancel_rx, _) in spawn_list {
            let eng = Arc::clone(self);
            let wr = writer.clone();
            let name = iface.name.clone();
            let handle = tokio::spawn(async move {
                eng.worker_loop(iface, cancel_rx, wr).await;
            });
            self.shared
                .lock()
                .await
                .worker_handles
                .entry(name)
                .or_default()
                .push(handle);
        }
    }

    // ------------------------------------------------------------------
    // Worker Coroutine
    // ------------------------------------------------------------------
    async fn worker_loop(
        self: Arc<Self>,
        iface: NetworkInterface,
        mut cancel_rx: watch::Receiver<bool>,
        writer: PositionalFileWriter,
    ) {
        let iface_name = iface.name.clone();
        let client = match build_interface_client(
            &iface,
            self.config.connect_timeout,
            self.config.read_timeout,
            self.config.use_so_bindtodevice,
        ) {
            Ok(c) => c,
            Err(e) => {
                self.shared
                    .lock()
                    .await
                    .log(format!("Client init failed on {iface_name}: {e}"));
                return;
            }
        };

        {
            let mut st = self.shared.lock().await;
            if let Some(rt) = st.interfaces.get_mut(&iface_name) {
                rt.active_workers += 1;
            } else {
                return;
            }
        }

        let mut stop_rx = self.stop_tx.subscribe();
        let mut pause_rx = self.pause_tx.subscribe();

        loop {
            if *stop_rx.borrow() || *cancel_rx.borrow() {
                break;
            }

            // Wait while paused
            while *pause_rx.borrow() && !*stop_rx.borrow() && !*cancel_rx.borrow() {
                tokio::select! {
                    _ = pause_rx.changed() => {}
                    _ = stop_rx.changed() => break,
                    _ = cancel_rx.changed() => break,
                }
            }
            if *stop_rx.borrow() || *cancel_rx.borrow() {
                break;
            }

            // Pop next chunk from shared priority queue
            let (chunk_opt, final_url, should_exit) = {
                let mut st = self.shared.lock().await;
                let Some(rt) = st.interfaces.get(&iface_name) else {
                    break;
                };
                if rt.status == InterfaceStatus::Stopped {
                    break;
                }
                if rt.active_workers > rt.target_workers && rt.target_workers > 0 {
                    break;
                }
                let url = st
                    .probe
                    .as_ref()
                    .map(|p| p.final_url.clone())
                    .unwrap_or_default();
                let total = st.probe.as_ref().and_then(|p| p.total_size).unwrap_or(0);
                let done = st.state.as_ref().map(|s| s.completed_bytes()).unwrap_or(0);
                if let Some(c) = st.queue.pop() {
                    st.in_flight_count += 1;
                    (Some(c), url, false)
                } else {
                    (None, url, done >= total && st.in_flight_count == 0)
                }
            };

            if should_exit {
                break;
            }

            let Some(chunk) = chunk_opt else {
                tokio::select! {
                    _ = self.queue_notify.notified() => {}
                    _ = tokio::time::sleep(Duration::from_millis(250)) => {}
                    _ = stop_rx.changed() => break,
                    _ = cancel_rx.changed() => break,
                }
                continue;
            };

            let res = self
                .download_chunk(
                    &client,
                    &iface_name,
                    &final_url,
                    &chunk,
                    &writer,
                    &mut stop_rx,
                    &mut pause_rx,
                    &mut cancel_rx,
                )
                .await;

            let mut should_rebalance = false;
            let mut break_worker = false;
            let mut backoff_ms = 0u64;

            {
                let mut st = self.shared.lock().await;
                st.in_flight_count = st.in_flight_count.saturating_sub(1);

                match res {
                    Ok(next_offset) if next_offset > chunk.end => {
                        if let Some(rt) = st.interfaces.get_mut(&iface_name) {
                            rt.chunks_completed += 1;
                            rt.consecutive_errors = 0;
                            rt.stop_count = 0;
                            rt.last_error.clear();
                            rt.status = InterfaceStatus::Active;
                        }
                    }
                    Ok(next_offset) => {
                        // Paused, stopped, or interface cancelled mid-chunk: return remaining range
                        st.queue.push(ChunkTask {
                            priority: 1,
                            start: next_offset,
                            end: chunk.end,
                            retries: chunk.retries,
                        });
                        self.queue_notify.notify_waiters();
                    }
                    Err((next_offset, err_msg)) => {
                        if next_offset <= chunk.end {
                            st.queue.push(ChunkTask {
                                priority: 1,
                                start: next_offset,
                                end: chunk.end,
                                retries: chunk.retries + 1,
                            });
                            self.queue_notify.notify_waiters();
                        }

                        // Only record failure if interface is still connected (not already removed)
                        if let Some(rt) = st.interfaces.get_mut(&iface_name) {
                            rt.consecutive_errors += 1;
                            rt.total_errors += 1;
                            rt.last_error = err_msg.chars().take(120).collect();

                            if rt.consecutive_errors >= self.config.max_iface_errors {
                                rt.stop_count += 1;
                                let mult = 1u32.checked_shl(rt.stop_count.saturating_sub(1)).unwrap_or(16);
                                let delay = self
                                    .config
                                    .probe_interval_base
                                    .saturating_mul(mult)
                                    .min(self.config.probe_interval_max);
                                rt.status = InterfaceStatus::Stopped;
                                rt.next_probe_at = Some(Instant::now() + delay);
                                let errs = rt.consecutive_errors;
                                let last_e = rt.last_error.clone();
                                st.log(format!(
                                    "Interface {iface_name} stopped after {errs} errors ({last_e}); re-routing chunks."
                                ));
                                if let Some(tx) = st.iface_cancels.remove(&iface_name) {
                                    let _ = tx.send(true);
                                }
                                should_rebalance = true;
                                break_worker = true;
                            } else {
                                if rt.status == InterfaceStatus::Active {
                                    rt.status = InterfaceStatus::Degraded;
                                }
                                backoff_ms = (350 * rt.consecutive_errors as u64).min(2000);
                            }
                        } else {
                            break_worker = true;
                        }
                    }
                }
            }

            if should_rebalance {
                self.rebalance_notify.notify_one();
            }
            if break_worker {
                break;
            }
            if backoff_ms > 0 {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(backoff_ms)) => {}
                    _ = stop_rx.changed() => break,
                    _ = cancel_rx.changed() => break,
                }
            }
        }

        let mut st = self.shared.lock().await;
        if let Some(rt) = st.interfaces.get_mut(&iface_name) {
            rt.active_workers = rt.active_workers.saturating_sub(1);
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn download_chunk(
        &self,
        client: &reqwest::Client,
        iface_name: &str,
        url: &str,
        chunk: &ChunkTask,
        writer: &PositionalFileWriter,
        stop_rx: &mut watch::Receiver<bool>,
        pause_rx: &mut watch::Receiver<bool>,
        cancel_rx: &mut watch::Receiver<bool>,
    ) -> std::result::Result<u64, (u64, String)> {
        let range_hdr = format!("bytes={}-{}", chunk.start, chunk.end);
        let resp = tokio::select! {
            r = client.get(url).header(header::RANGE, range_hdr).send() => {
                r.map_err(|e| (chunk.start, e.to_string()))?
            }
            _ = stop_rx.changed() => return Ok(chunk.start),
            _ = cancel_rx.changed() => return Ok(chunk.start),
        };

        let status = resp.status();
        if status != reqwest::StatusCode::PARTIAL_CONTENT && status != reqwest::StatusCode::OK {
            return Err((
                chunk.start,
                format!("HTTP {} for {}-{}", status.as_u16(), chunk.start, chunk.end),
            ));
        }

        let mut stream = resp.bytes_stream();
        let mut cur_offset = chunk.start;
        let mut buf = Vec::with_capacity(self.config.flush_size);

        loop {
            if *stop_rx.borrow() || *pause_rx.borrow() || *cancel_rx.borrow() {
                break;
            }

            let next_item = tokio::select! {
                item = stream.next() => item,
                _ = stop_rx.changed() => None,
                _ = pause_rx.changed() => None,
                _ = cancel_rx.changed() => None,
            };

            let Some(item) = next_item else {
                break;
            };

            let bytes = match item {
                Ok(b) => b,
                Err(e) => {
                    if !buf.is_empty() {
                        let len = buf.len() as u64;
                        if self.flush_buffer(writer, cur_offset, std::mem::take(&mut buf), iface_name).await.is_ok() {
                            cur_offset += len;
                        }
                    }
                    return Err((cur_offset, e.to_string()));
                }
            };

            let in_buf_end = cur_offset + buf.len() as u64;
            if in_buf_end > chunk.end {
                break;
            }
            let allowed = (chunk.end - in_buf_end + 1) as usize;
            let slice = if bytes.len() > allowed {
                &bytes[..allowed]
            } else {
                &bytes[..]
            };
            buf.extend_from_slice(slice);

            {
                let mut st = self.shared.lock().await;
                if let Some(rt) = st.interfaces.get_mut(iface_name) {
                    rt.record_bytes(slice.len() as u64);
                }
            }

            if buf.len() >= self.config.flush_size {
                let to_write = std::mem::take(&mut buf);
                let len = to_write.len() as u64;
                self.flush_buffer(writer, cur_offset, to_write, iface_name)
                    .await
                    .map_err(|e| (cur_offset, e.to_string()))?;
                cur_offset += len;
            }

            if cur_offset > chunk.end {
                break;
            }
        }

        if !buf.is_empty() {
            let len = buf.len() as u64;
            self.flush_buffer(writer, cur_offset, buf, iface_name)
                .await
                .map_err(|e| (cur_offset, e.to_string()))?;
            cur_offset += len;
        }

        Ok(cur_offset)
    }

    async fn flush_buffer(
        &self,
        writer: &PositionalFileWriter,
        offset: u64,
        data: Vec<u8>,
        iface_name: &str,
    ) -> Result<()> {
        let len = data.len() as u64;
        writer.pwrite(offset, data).await?;
        let mut st = self.shared.lock().await;
        if let Some(store) = st.state.as_mut() {
            store.record_range(offset, offset + len - 1, iface_name);
        }
        Ok(())
    }

    // ------------------------------------------------------------------
    // Health Recovery Loop (ONLY probes interfaces still connected)
    // ------------------------------------------------------------------
    async fn health_probe_loop(self: Arc<Self>, writer: PositionalFileWriter) {
        let mut stop_rx = self.stop_tx.subscribe();
        while !*stop_rx.borrow() {
            let due: Vec<(NetworkInterface, String)> = {
                let mut st = self.shared.lock().await;
                let now = Instant::now();
                let url = st
                    .probe
                    .as_ref()
                    .map(|p| p.final_url.clone())
                    .unwrap_or_default();
                let mut list = Vec::new();
                for rt in st.interfaces.values_mut() {
                    if rt.status == InterfaceStatus::Stopped
                        && rt.next_probe_at.map(|t| now >= t).unwrap_or(false)
                    {
                        rt.status = InterfaceStatus::Probing;
                        list.push((rt.interface.clone(), url.clone()));
                    }
                }
                list
            };

            let mut recovered_any = false;
            for (iface, url) in due {
                let ok = match build_interface_client(
                    &iface,
                    self.config.connect_timeout.min(Duration::from_secs(5)),
                    self.config.read_timeout.min(Duration::from_secs(10)),
                    self.config.use_so_bindtodevice,
                ) {
                    Ok(c) => match c.head(&url).send().await {
                        Ok(r) => !r.status().is_server_error(),
                        Err(_) => false,
                    },
                    Err(_) => false,
                };

                let mut st = self.shared.lock().await;
                // Only update if interface wasn't physically disconnected during probe
                if let Some(rt) = st.interfaces.get_mut(&iface.name) {
                    if ok {
                        rt.status = InterfaceStatus::Active;
                        rt.consecutive_errors = 0;
                        rt.stop_count = 0;
                        rt.last_error.clear();
                        rt.next_probe_at = None;
                        st.log(format!("Interface {} recovered; restarting workers.", iface.name));
                        recovered_any = true;
                    } else {
                        rt.stop_count += 1;
                        let mult = 1u32.checked_shl(rt.stop_count.saturating_sub(1)).unwrap_or(16);
                        let delay = self
                            .config
                            .probe_interval_base
                            .saturating_mul(mult)
                            .min(self.config.probe_interval_max);
                        rt.status = InterfaceStatus::Stopped;
                        rt.next_probe_at = Some(Instant::now() + delay);
                    }
                }
            }

            if recovered_any {
                self.rebalance_workers(writer.clone()).await;
            }

            tokio::select! {
                _ = self.rebalance_notify.notified() => {
                    self.rebalance_workers(writer.clone()).await;
                }
                _ = self.recheck_notify.notified() => {}
                _ = tokio::time::sleep(Duration::from_millis(500)) => {}
                _ = stop_rx.changed() => break,
            }
        }
    }

    async fn periodic_checkpoint_loop(self: Arc<Self>) {
        let mut stop_rx = self.stop_tx.subscribe();
        while !*stop_rx.borrow() {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(2)) => {}
                _ = stop_rx.changed() => break,
            }
            let mut st = self.shared.lock().await;
            if let Some(store) = st.state.as_mut() {
                let _ = store.save_atomic();
            }
        }
    }
}
