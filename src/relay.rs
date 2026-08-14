//! Layer 1 relay (draft §5.1, Relay mode).
//!
//! Forwards messages bidirectionally between a client-facing and a
//! backend-facing transport without inspecting or modifying payloads, and
//! without any initialization handshake of its own. It may bridge differing
//! framings while preserving message boundaries and payload bytes.

use std::io;

use crate::transport::{MessageRead, MessageWrite};

pub struct Relay;

impl Relay {
    /// Pump both directions until either side reaches end of stream. Read and
    /// write halves are passed separately so the two pumps never alias one
    /// transport.
    pub async fn run<CR, CW, BR, BW>(
        client_read: CR,
        client_write: CW,
        backend_read: BR,
        backend_write: BW,
    ) -> io::Result<()>
    where
        CR: MessageRead,
        CW: MessageWrite,
        BR: MessageRead,
        BW: MessageWrite,
    {
        tokio::try_join!(
            pump(client_read, backend_write),
            pump(backend_read, client_write),
        )?;
        Ok(())
    }
}

async fn pump<R: MessageRead, W: MessageWrite>(mut src: R, mut dst: W) -> io::Result<()> {
    loop {
        match src.receive().await? {
            None => {
                dst.send_eof().await?;
                return Ok(());
            }
            Some(message) => dst.send(&message).await?,
        }
    }
}
