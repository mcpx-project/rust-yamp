//! Layer 1 transports (draft §6.1).
//!
//! A transport frames an opaque payload, a serialized JSON-RPC envelope, on
//! the wire. In δ0 it never inspects payload contents. The relay is
//! byte-faithful, so every message's exact payload bytes survive a framing
//! bridge. Read and write halves are separate types so the relay can pump both
//! directions concurrently without aliasing a single transport.

use std::io;

mod framed;
mod line;
mod sse;

pub use framed::{parse_content_length, FramedReader, FramedWriter, MAX_FRAME_BYTES};
pub use line::{LineReader, LineWriter};
pub use sse::{SseReader, SseWriter};

/// The read half of a message transport.
///
/// The returned futures are `Send` so a reader can run in a spawned task (the
/// bidirectional router spawns a demuxing reader per backend).
pub trait MessageRead {
    /// Return the next message payload, or `None` at end of stream.
    fn receive(&mut self) -> impl std::future::Future<Output = io::Result<Option<Vec<u8>>>> + Send;
}

/// The write half of a message transport.
pub trait MessageWrite {
    /// Frame and write one message payload.
    fn send(&mut self, payload: &[u8]) -> impl std::future::Future<Output = io::Result<()>> + Send;

    /// Signal end of stream to the peer.
    fn send_eof(&mut self) -> impl std::future::Future<Output = io::Result<()>> + Send;
}

/// Trim leading and trailing ASCII whitespace without relying on a specific
/// std version's slice helpers.
pub(crate) fn trim(mut bytes: &[u8]) -> &[u8] {
    while let [first, rest @ ..] = bytes {
        if first.is_ascii_whitespace() {
            bytes = rest;
        } else {
            break;
        }
    }
    while let [rest @ .., last] = bytes {
        if last.is_ascii_whitespace() {
            bytes = rest;
        } else {
            break;
        }
    }
    bytes
}
