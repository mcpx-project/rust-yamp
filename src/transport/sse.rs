//! Server-Sent Events framing (MCP HTTP+SSE transport).
//!
//! Each message is one SSE event: one or more `data:` lines terminated by a
//! blank line. Only the `data` field carries the payload; comment lines
//! (starting with `:`) and other fields are ignored on read. Because a
//! transport's read and write halves are independent, this framing composes
//! with line and content-length framing through the relay and router.

use std::io;

use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt};

use super::{MessageRead, MessageWrite};

pub struct SseReader<R> {
    inner: R,
}

impl<R: AsyncBufRead + Unpin> SseReader<R> {
    pub fn new(inner: R) -> Self {
        Self { inner }
    }
}

impl<R: AsyncBufRead + Unpin + Send> MessageRead for SseReader<R> {
    async fn receive(&mut self) -> io::Result<Option<Vec<u8>>> {
        let mut data: Vec<Vec<u8>> = Vec::new();
        loop {
            let mut line = Vec::new();
            let n = self.inner.read_until(b'\n', &mut line).await?;
            if n == 0 {
                return Ok(if data.is_empty() { None } else { Some(join(&data)) });
            }
            if line.ends_with(b"\n") {
                line.pop();
            }
            if line.ends_with(b"\r") {
                line.pop();
            }
            if line.is_empty() {
                if !data.is_empty() {
                    return Ok(Some(join(&data)));
                }
                continue; // blank line with no data
            }
            if line.first() == Some(&b':') {
                continue; // comment
            }
            if line.starts_with(b"data:") {
                data.push(field_value(&line));
            }
            // other fields (event, id, retry) are ignored
        }
    }
}

pub struct SseWriter<W> {
    inner: W,
}

impl<W: AsyncWrite + Unpin> SseWriter<W> {
    pub fn new(inner: W) -> Self {
        Self { inner }
    }
}

impl<W: AsyncWrite + Unpin + Send> MessageWrite for SseWriter<W> {
    async fn send(&mut self, payload: &[u8]) -> io::Result<()> {
        for part in payload.split(|&b| b == b'\n') {
            self.inner.write_all(b"data: ").await?;
            self.inner.write_all(part).await?;
            self.inner.write_all(b"\n").await?;
        }
        self.inner.write_all(b"\n").await?; // blank line terminates the event
        self.inner.flush().await
    }

    async fn send_eof(&mut self) -> io::Result<()> {
        self.inner.shutdown().await
    }
}

fn field_value(line: &[u8]) -> Vec<u8> {
    let value = &line["data:".len()..];
    let value = if value.first() == Some(&b' ') { &value[1..] } else { value };
    value.to_vec()
}

fn join(parts: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::new();
    for (i, part) in parts.iter().enumerate() {
        if i > 0 {
            out.push(b'\n');
        }
        out.extend_from_slice(part);
    }
    out
}
