use crate::TransportError;
use std::io::{Read, Write};

/// Maximum frame payload size (16 MiB).
pub const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

/// Write a length-prefixed frame (u32 big-endian + body).
pub fn write_frame(mut writer: impl Write, body: &[u8]) -> Result<(), TransportError> {
    if body.len() > MAX_FRAME_SIZE {
        return Err(TransportError::Protocol(format!(
            "frame too large: {} bytes",
            body.len()
        )));
    }
    let len = body.len() as u32;
    writer
        .write_all(&len.to_be_bytes())
        .map_err(|e| TransportError::Io(e.to_string()))?;
    writer
        .write_all(body)
        .map_err(|e| TransportError::Io(e.to_string()))?;
    Ok(())
}

/// Read a length-prefixed frame.
pub fn read_frame(mut reader: impl Read) -> Result<Vec<u8>, TransportError> {
    let mut len_buf = [0u8; 4];
    reader
        .read_exact(&mut len_buf)
        .map_err(|e| TransportError::Io(e.to_string()))?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME_SIZE {
        return Err(TransportError::Protocol(format!(
            "frame too large: {len} bytes"
        )));
    }
    let mut body = vec![0u8; len];
    reader
        .read_exact(&mut body)
        .map_err(|e| TransportError::Io(e.to_string()))?;
    Ok(body)
}
