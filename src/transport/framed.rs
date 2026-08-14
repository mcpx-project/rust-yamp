//! Content-Length framing (HTTP / LSP-style message boundary).
//!
//! `Content-Length: N\r\n\r\n` then exactly N payload bytes. Because the
//! boundary is a byte count, payloads that contain `\r\n` or the text
//! `Content-Length` are carried unharmed.

use std::io;

use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::{trim, MessageRead, MessageWrite};

/// A declared frame larger than this is rejected before any buffer is sized, so
/// a hostile or corrupt Content-Length cannot force a huge allocation. Internal
/// framed messages are small; 64 MiB is generous headroom.
pub const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;

pub fn parse_content_length(header: &[u8]) -> io::Result<usize> {
    for raw in header.split(|&b| b == b'\n') {
        let line = match raw {
            [head @ .., b'\r'] => head,
            _ => raw,
        };
        if let Some(pos) = line.iter().position(|&b| b == b':') {
            let (name, rest) = line.split_at(pos);
            if trim(name).eq_ignore_ascii_case(b"content-length") {
                let value = trim(&rest[1..]);
                let length: usize = std::str::from_utf8(value)
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid Content-Length"))?;
                if length > MAX_FRAME_BYTES {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "Content-Length exceeds maximum"));
                }
                return Ok(length);
            }
        }
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "missing Content-Length header",
    ))
}

pub struct FramedReader<R> {
    inner: R,
}

impl<R: AsyncBufRead + Unpin> FramedReader<R> {
    pub fn new(inner: R) -> Self {
        Self { inner }
    }

    async fn read_header(&mut self) -> io::Result<Option<Vec<u8>>> {
        let mut header = Vec::new();
        loop {
            let mut line = Vec::new();
            let n = self.inner.read_until(b'\n', &mut line).await?;
            if n == 0 {
                return Ok(None); // EOF, possibly mid-header
            }
            if matches!(line.as_slice(), b"\r\n" | b"\n") {
                return Ok(Some(header)); // blank line ends the headers
            }
            header.extend_from_slice(&line);
        }
    }
}

impl<R: AsyncBufRead + Unpin + Send> MessageRead for FramedReader<R> {
    async fn receive(&mut self) -> io::Result<Option<Vec<u8>>> {
        let header = match self.read_header().await? {
            None => return Ok(None),
            Some(header) => header,
        };
        let length = parse_content_length(&header)?;
        let mut body = vec![0u8; length];
        match self.inner.read_exact(&mut body).await {
            Ok(_) => Ok(Some(body)),
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => Ok(None),
            Err(err) => Err(err),
        }
    }
}

pub struct FramedWriter<W> {
    inner: W,
}

impl<W: AsyncWrite + Unpin> FramedWriter<W> {
    pub fn new(inner: W) -> Self {
        Self { inner }
    }
}

impl<W: AsyncWrite + Unpin + Send> MessageWrite for FramedWriter<W> {
    async fn send(&mut self, payload: &[u8]) -> io::Result<()> {
        let header = format!("Content-Length: {}\r\n\r\n", payload.len());
        self.inner.write_all(header.as_bytes()).await?;
        self.inner.write_all(payload).await?;
        self.inner.flush().await
    }

    async fn send_eof(&mut self) -> io::Result<()> {
        self.inner.shutdown().await
    }
}
