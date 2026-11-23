# paralleldl (`pldl`)

Rootless Linux multi-interface parallel HTTP file downloader written in Rust. Combines bandwidth across Ethernet, Wi-Fi, and USB tethering using `reqwest` (`SO_BINDTODEVICE`), `tokio`, lock-free `pwrite` (`FileExt::write_all_at`), per-interface circuit breakers, live hot-plug add/remove handling, atomic JSON resume checkpoints, and a `ratatui` TUI.

## Build & Install

```bash
cargo build --release
# Optional: install `pldl` into ~/.cargo/bin
cargo install --path .
```

## Usage

```bash
# List available Linux network interfaces without starting a download
pldl --list-interfaces

# 4 connections per interface, 100 MB chunks, 16 MB minimum chunk size
pldl https://example.com/10gb.iso --per-interface 4 --chunk-size 100M --min-chunk-size 16M

# Split 10 total connections equally across specific interfaces
pldl https://example.com/10gb.iso --total-connections 10 --interfaces enp12s0,wlan0

# Headless mode (no TUI)
pldl https://example.com/10gb.iso --no-tui
```

## TUI Controls

| Key | Action |
| --- | --- |
| `p` | Pause / Resume download (flushes buffers and checkpoints `<file>.pldl.json`) |
| `r` | Trigger immediate connectivity recheck on all `STOPPED` (connected) interfaces |
| `l` | Rescan Linux network interfaces for hot-plugged links |
| `s` / `q` | Graceful stop (flushes buffers, saves `<file>.pldl.json`, exits cleanly) |

## Disconnected vs. Stopped Interfaces

- **Disconnected interfaces** (cable unplugged, Wi-Fi off, lost IP/gateway) immediately cancel their workers, re-route in-flight chunks to healthy links, and are **removed** from the table so they never clutter the UI or keep retrying. Reconnecting them later adds them back automatically via `ip monitor`.
- **Connected-but-failing interfaces** (`STOPPED`) stay visible with a recovery countdown and are probed automatically (or immediately via `r`).

## Tests

```bash
cargo test
```
