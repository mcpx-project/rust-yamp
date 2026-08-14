//! Parser-robustness / fuzz tests for the hand-rolled framings.
//!
//! Feeds malformed and adversarial byte streams through the Content-Length, SSE,
//! and line decoders and asserts they degrade gracefully: a bounded result
//! (payload, partial, or None at EOF) or a controlled error, never a hang or an
//! unbounded allocation. Mirrors the Python arm's test_parser_robustness.py.

use tokio::io::BufReader;

use yamp::transport::{
    parse_content_length, FramedReader, LineReader, MessageRead, SseReader, MAX_FRAME_BYTES,
};

async fn framed(data: &[u8]) -> std::io::Result<Option<Vec<u8>>> {
    FramedReader::new(BufReader::new(data)).receive().await
}

async fn sse(data: &[u8]) -> Option<Vec<u8>> {
    SseReader::new(BufReader::new(data)).receive().await.unwrap()
}

async fn line(data: &[u8]) -> Option<Vec<u8>> {
    LineReader::new(BufReader::new(data)).receive().await.unwrap()
}

// --- Content-Length ---------------------------------------------------------

#[test]
fn content_length_rejects_negative_and_non_numeric_and_oversized() {
    assert!(parse_content_length(b"Content-Length: -1\r\n\r\n").is_err());
    assert!(parse_content_length(b"Content-Length: abc\r\n\r\n").is_err());
    let huge = format!("Content-Length: {}\r\n\r\n", MAX_FRAME_BYTES as u64 + 1);
    assert!(parse_content_length(huge.as_bytes()).is_err());
}

#[test]
fn content_length_at_the_cap_is_accepted() {
    let at = format!("Content-Length: {MAX_FRAME_BYTES}\r\n\r\n");
    assert_eq!(parse_content_length(at.as_bytes()).unwrap(), MAX_FRAME_BYTES);
}

#[tokio::test]
async fn content_length_ignores_junk_headers_before_the_real_one() {
    let out = framed(b"X-Junk: nonsense\r\nContent-Length: 2\r\n\r\nhi").await.unwrap();
    assert_eq!(out, Some(b"hi".to_vec()));
}

#[tokio::test]
async fn content_length_no_terminator_is_eof() {
    let out = framed(b"Content-Length: 5\r\n").await.unwrap();
    assert_eq!(out, None);
}

#[tokio::test]
async fn content_length_oversized_frame_errors_rather_than_allocates() {
    let huge = format!("Content-Length: {}\r\n\r\n", MAX_FRAME_BYTES as u64 + 1);
    assert!(framed(huge.as_bytes()).await.is_err());
}

// --- SSE --------------------------------------------------------------------

#[tokio::test]
async fn sse_comments_only_is_eof() {
    assert_eq!(sse(b": keepalive\n: another\n\n").await, None);
}

#[tokio::test]
async fn sse_joins_multiple_data_lines() {
    assert_eq!(sse(b"data: a\ndata: b\n\n").await, Some(b"a\nb".to_vec()));
}

#[tokio::test]
async fn sse_unterminated_event_at_eof_is_returned() {
    assert_eq!(sse(b"data: tail").await, Some(b"tail".to_vec()));
}

#[tokio::test]
async fn sse_ignores_unknown_fields() {
    assert_eq!(sse(b"event: message\nid: 7\nretry: 100\ndata: x\n\n").await, Some(b"x".to_vec()));
}

// --- line -------------------------------------------------------------------

#[tokio::test]
async fn line_unterminated_at_eof_is_partial() {
    assert_eq!(line(b"no newline here").await, Some(b"no newline here".to_vec()));
}

#[tokio::test]
async fn line_empty_stream_is_eof() {
    assert_eq!(line(b"").await, None);
}

// --- fuzz sweep -------------------------------------------------------------

/// A tiny deterministic xorshift PRNG (Math.random / rand are unavailable and we
/// want a fixed seed anyway for reproducibility).
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

#[tokio::test]
async fn random_bytes_never_hang_or_crash_unexpectedly() {
    // Every decoder must, on arbitrary bytes ending in EOF, return a bounded
    // result or a controlled error, and always terminate (EOF-bounded input).
    let alphabet = b"{}\":,\r\n data:Content-Length 0123456789xyz";
    let mut rng = Rng(0xC0FFEE);
    for _ in 0..400 {
        let length = (rng.next() % 65) as usize;
        let blob: Vec<u8> = (0..length).map(|_| alphabet[(rng.next() as usize) % alphabet.len()]).collect();
        // Content-Length may error (malformed); SSE and line never do.
        let _ = framed(&blob).await;
        let _ = sse(&blob).await;
        let _ = line(&blob).await;
    }
}
