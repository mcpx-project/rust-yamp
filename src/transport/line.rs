//! Newline-delimited framing (MCP stdio transport).

use std::io;

use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt};

use super::{MessageRead, MessageWrite};

pub struct LineReader<R> {
    inner: R,
}

impl<R: AsyncBufRead + Unpin> LineReader<R> {
    pub fn new(inner: R) -> Self {
        Self { inner }
    }
}

impl<R: AsyncBufRead + Unpin + Send> MessageRead for LineReader<R> {
    async fn receive(&mut self) -> io::Result<Option<Vec<u8>>> {
        let mut buf = Vec::new();
        let n = self.inner.read_until(b'\n', &mut buf).await?;
        if n == 0 {
            // Clean EOF, or a final unterminated message already consumed.
            return Ok(None);
        }
        if buf.last() == Some(&b'\n') {
            buf.pop();
        }
        Ok(Some(buf))
    }
}

pub struct LineWriter<W> {
    inner: W,
}

impl<W: AsyncWrite + Unpin> LineWriter<W> {
    pub fn new(inner: W) -> Self {
        Self { inner }
    }
}

impl<W: AsyncWrite + Unpin + Send> MessageWrite for LineWriter<W> {
    async fn send(&mut self, payload: &[u8]) -> io::Result<()> {
        self.inner.write_all(payload).await?;
        self.inner.write_all(b"\n").await?;
        self.inner.flush().await
    }

    async fn send_eof(&mut self) -> io::Result<()> {
        self.inner.shutdown().await
    }
}
