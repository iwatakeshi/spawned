//! Length-framed request/response codec for libp2p cluster streams.

use crate::TransportError;
use async_trait::async_trait;
use futures::{AsyncRead, AsyncWrite};
use libp2p::StreamProtocol;
use std::io;

pub const CLUSTER_PROTOCOL: StreamProtocol = StreamProtocol::new("/spawned/cluster/3");

#[derive(Clone, Default)]
pub struct ClusterCodec;

#[async_trait]
impl libp2p::request_response::Codec for ClusterCodec {
    type Protocol = StreamProtocol;
    type Request = Vec<u8>;
    type Response = Vec<u8>;

    async fn read_request<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        read_frame_async(io)
            .await
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))
    }

    async fn write_request<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        req: Self::Request,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        write_frame_async(io, &req)
            .await
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))
    }

    async fn read_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        read_frame_async(io)
            .await
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))
    }

    async fn write_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        res: Self::Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        write_frame_async(io, &res)
            .await
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))
    }
}

async fn read_frame_async<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Vec<u8>, TransportError> {
    use futures::AsyncReadExt;

    let mut len_buf = [0u8; 4];
    reader
        .read_exact(&mut len_buf)
        .await
        .map_err(|e| TransportError::Io(e.to_string()))?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > crate::frame::MAX_FRAME_SIZE {
        return Err(TransportError::Protocol(format!(
            "frame too large: {len} bytes"
        )));
    }
    let mut body = vec![0u8; len];
    reader
        .read_exact(&mut body)
        .await
        .map_err(|e| TransportError::Io(e.to_string()))?;
    Ok(body)
}

async fn write_frame_async<W: AsyncWrite + Unpin>(
    writer: &mut W,
    body: &[u8],
) -> Result<(), TransportError> {
    use futures::AsyncWriteExt;

    if body.len() > crate::frame::MAX_FRAME_SIZE {
        return Err(TransportError::Protocol(format!(
            "frame too large: {} bytes",
            body.len()
        )));
    }
    let len = body.len() as u32;
    writer
        .write_all(&len.to_be_bytes())
        .await
        .map_err(|e| TransportError::Io(e.to_string()))?;
    writer
        .write_all(body)
        .await
        .map_err(|e| TransportError::Io(e.to_string()))?;
    Ok(())
}
