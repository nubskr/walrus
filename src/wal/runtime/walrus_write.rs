use super::Walrus;
use super::walrus::Cursor;
use std::sync::Arc;

impl Walrus {
    pub fn append_for_topic(&self, col_name: &str, raw_bytes: &[u8]) -> std::io::Result<()> {
        self.mark_topic_dirty(col_name);
        let writer = self.get_or_create_writer(col_name)?;
        writer.write(raw_bytes)?;
        self.increment_topic_entry_count(col_name, 1);
        Ok(())
    }

    pub fn append_for_topic_with_cursor(
        &self,
        col_name: &str,
        raw_bytes: &[u8],
    ) -> std::io::Result<Cursor> {
        const TAIL_FLAG: u64 = 1u64 << 63;
        self.mark_topic_dirty(col_name);
        let writer = self.get_or_create_writer(col_name)?;
        let (block_id, offset) = writer.write_with_tail_cursor(raw_bytes)?;
        self.increment_topic_entry_count(col_name, 1);
        Ok(Cursor::new(
            Arc::from(col_name),
            block_id | TAIL_FLAG,
            offset,
        ))
    }

    pub fn batch_append_for_topic(&self, col_name: &str, batch: &[&[u8]]) -> std::io::Result<()> {
        self.mark_topic_dirty(col_name);
        let writer = self.get_or_create_writer(col_name)?;
        writer.batch_write(batch)?;
        self.increment_topic_entry_count(col_name, batch.len() as u64);
        Ok(())
    }
}
