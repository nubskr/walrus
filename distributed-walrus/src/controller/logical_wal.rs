use anyhow::Result;

use super::NodeController;

#[derive(Default, Clone, Copy)]
struct OffsetCursor {
    logical: u64,
    physical: u64,
}

impl OffsetCursor {
    fn advance(&mut self, payload_len: u64) {
        self.logical += payload_len;
        self.physical += payload_len + NodeController::ENTRY_OVERHEAD;
    }
}

// LogicalWal keeps the “logical offset = payload bytes” invariant in one place. Walrus stores
// entries with extra overhead bytes; readers supply logical offsets and this maps them back to the
// physical offsets and trims the leading bytes as needed.

pub(super) struct LogicalWal<'a> {
    pub(super) controller: &'a NodeController,
    pub(super) wal_key: &'a str,
}

impl<'a> LogicalWal<'a> {
    pub async fn logical_len(&self) -> u64 {
        let tracked = self.controller.tracked_len(self.wal_key).await;
        match self.scan_logical_bytes().await {
            Ok(scanned) => {
                if scanned != tracked {
                    self.controller
                        .set_tracked_len(self.wal_key, scanned)
                        .await;
                }
                scanned
            }
            Err(_) => tracked,
        }
    }

    pub async fn read(&self, logical_offset: u64, max_bytes: usize) -> Result<Vec<Vec<u8>>> {
        let (physical_offset, trim) = self.map_logical_to_physical_offset(logical_offset).await?;
        let mut entries = self
            .controller
            .bucket
            .read_by_key_from_offset(self.wal_key, physical_offset, max_bytes)
            .await?;

        if trim > 0 {
            if let Some(first) = entries.first_mut() {
                if trim >= first.data.len() {
                    first.data.clear();
                } else {
                    first.data.drain(0..trim);
                }
            }
        }

        Ok(entries.into_iter().map(|e| e.data).collect())
    }

    async fn map_logical_to_physical_offset(
        &self,
        logical_offset: u64,
    ) -> Result<(u64, usize)> {
        if logical_offset == 0 {
            return Ok((0, 0));
        }

        let total_physical = self
            .controller
            .bucket
            .get_topic_size_blocking(self.wal_key);
        let mut cursor = OffsetCursor::default();

        while cursor.physical < total_physical {
            let max_bytes = scan_window(total_physical, cursor.physical);
            let entries = self
                .controller
                .bucket
                .read_by_key_from_offset(self.wal_key, cursor.physical, max_bytes)
                .await?;
            if entries.is_empty() {
                break;
            }

            for entry in entries {
                let payload_len = entry.data.len() as u64;

                if logical_offset < cursor.logical + payload_len {
                    let trim = (logical_offset - cursor.logical) as usize;
                    return Ok((cursor.physical, trim));
                }

                cursor.advance(payload_len);

                if logical_offset == cursor.logical {
                    return Ok((cursor.physical, 0));
                }
            }
        }

        Ok((cursor.physical, 0))
    }

    async fn scan_logical_bytes(&self) -> Result<u64> {
        let total_physical = self
            .controller
            .bucket
            .get_topic_size_blocking(self.wal_key);
        let mut cursor = OffsetCursor::default();

        while cursor.physical < total_physical {
            let max_bytes = scan_window(total_physical, cursor.physical);
            let entries = self
                .controller
                .bucket
                .read_by_key_from_offset(self.wal_key, cursor.physical, max_bytes)
                .await?;
            if entries.is_empty() {
                break;
            }

            for entry in entries {
                cursor.advance(entry.data.len() as u64);
            }
        }

        Ok(cursor.logical)
    }
}

fn scan_window(total_physical: u64, physical_cursor: u64) -> usize {
    let remaining = (total_physical.saturating_sub(physical_cursor)) as usize;
    remaining
        .min(NodeController::SCAN_WINDOW)
        .max(NodeController::ENTRY_OVERHEAD as usize + 1)
}
