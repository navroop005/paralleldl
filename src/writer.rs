use anyhow::{Context, Result};
use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;

/// Lock-free positional writer backed by Linux `pwrite(2)` (`FileExt::write_all_at`).
#[derive(Clone)]
pub struct PositionalFileWriter {
    file: Arc<File>,
}

impl PositionalFileWriter {
    pub fn open(path: &Path, total_size: Option<u64>) -> Result<Self> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)
            .with_context(|| format!("Failed to open target file {}", path.display()))?;

        if let Some(sz) = total_size {
            if sz > 0 && file.metadata()?.len() < sz {
                file.set_len(sz)
                    .with_context(|| format!("Failed to pre-allocate {} bytes", sz))?;
            }
        }

        Ok(Self {
            file: Arc::new(file),
        })
    }

    pub async fn pwrite(&self, offset: u64, data: Vec<u8>) -> Result<()> {
        let file = Arc::clone(&self.file);
        tokio::task::spawn_blocking(move || file.write_all_at(&data, offset))
            .await
            .context("pwrite worker panicked")??;
        Ok(())
    }

    pub fn sync_all(&self) -> Result<()> {
        self.file.sync_all()?;
        Ok(())
    }
}
