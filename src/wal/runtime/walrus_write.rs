use super::Walrus;

impl Walrus {
    pub fn append_for_topic(&self, col_name: &str, raw_bytes: &[u8]) -> std::io::Result<()> {
        self.mark_topic_dirty(col_name);
        let writer = self.get_or_create_writer(col_name)?;
        writer.write(raw_bytes)?;
        self.increment_topic_entry_count(col_name, 1);
        Ok(())
    }

    pub fn batch_append_for_topic(&self, col_name: &str, batch: &[&[u8]]) -> std::io::Result<()> {
        self.mark_topic_dirty(col_name);
        let writer = self.get_or_create_writer(col_name)?;
        writer.batch_write(batch)?;
        self.increment_topic_entry_count(col_name, batch.len() as u64);
        Ok(())
    }
}
