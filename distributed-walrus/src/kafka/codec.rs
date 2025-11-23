use anyhow::{bail, Result};
use bytes::{Buf, BufMut, BytesMut};
use std::io::Cursor;

pub trait KafkaPrimitive: Sized {
    fn decode(buf: &mut Cursor<&[u8]>) -> Result<Self>;
    fn encode(&self, buf: &mut BytesMut);
}

impl KafkaPrimitive for i32 {
    fn decode(buf: &mut Cursor<&[u8]>) -> Result<Self> {
        if buf.remaining() < 4 {
            bail!("not enough bytes for i32");
        }
        Ok(buf.get_i32())
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_i32(*self);
    }
}

impl KafkaPrimitive for i16 {
    fn decode(buf: &mut Cursor<&[u8]>) -> Result<Self> {
        if buf.remaining() < 2 {
            bail!("not enough bytes for i16");
        }
        Ok(buf.get_i16())
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_i16(*self);
    }
}

impl KafkaPrimitive for i64 {
    fn decode(buf: &mut Cursor<&[u8]>) -> Result<Self> {
        if buf.remaining() < 8 {
            bail!("not enough bytes for i64");
        }
        Ok(buf.get_i64())
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_i64(*self);
    }
}

impl KafkaPrimitive for String {
    fn decode(buf: &mut Cursor<&[u8]>) -> Result<Self> {
        let len = i16::decode(buf)?;
        if len == -1 {
            return Ok(String::new());
        }
        let len = len as usize;
        if buf.remaining() < len {
            bail!("string too short");
        }
        let bytes = buf.copy_to_bytes(len);
        Ok(String::from_utf8(bytes.to_vec())?)
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_i16(self.len() as i16);
        buf.put_slice(self.as_bytes());
    }
}

#[derive(Debug)]
pub struct RequestHeader {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: String,
}

impl KafkaPrimitive for RequestHeader {
    fn decode(buf: &mut Cursor<&[u8]>) -> Result<Self> {
        Ok(Self {
            api_key: i16::decode(buf)?,
            api_version: i16::decode(buf)?,
            correlation_id: i32::decode(buf)?,
            client_id: String::decode(buf)?,
        })
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.correlation_id.encode(buf);
    }
}

pub fn decode_string_array(buf: &mut Cursor<&[u8]>) -> Result<Vec<String>> {
    let count = i32::decode(buf)?;
    if count < 0 {
        return Ok(Vec::new());
    }
    let mut out = Vec::with_capacity(count as usize);
    for _ in 0..count {
        out.push(String::decode(buf)?);
    }
    Ok(out)
}
