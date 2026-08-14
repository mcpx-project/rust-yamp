//! δ0 integration tests for the Rust arm.
//!
//! NOTE: uncompiled in the authoring environment (no cargo). Mirrors the
//! Python arm's tests. Run with `cargo test` on a machine with a Rust
//! toolchain.

use std::time::Instant;

use tokio::io::{duplex, BufReader};

use yamp::instrument::{within_budget, LATENCY_BUDGET_MS};
use yamp::transport::{
    parse_content_length, FramedReader, FramedWriter, LineReader, LineWriter, MessageRead,
    MessageWrite,
};
use yamp::Relay;

const CAP: usize = 1 << 16;

#[tokio::test]
async fn line_round_trip() {
    let (w, r) = duplex(CAP);
    let mut writer = LineWriter::new(w);
    writer.send(b"{\"id\":1}").await.unwrap();
    writer.send_eof().await.unwrap();

    let mut reader = LineReader::new(BufReader::new(r));
    assert_eq!(reader.receive().await.unwrap().as_deref(), Some(&b"{\"id\":1}"[..]));
    assert_eq!(reader.receive().await.unwrap(), None);
}

#[tokio::test]
async fn framed_round_trip_and_byte_faithful() {
    let payload = b"{\"m\":\"has \r\n and Content-Length: 5 inside\"}";
    let (w, r) = duplex(CAP);
    let mut writer = FramedWriter::new(w);
    writer.send(payload).await.unwrap();
    writer.send_eof().await.unwrap();

    let mut reader = FramedReader::new(BufReader::new(r));
    assert_eq!(reader.receive().await.unwrap().as_deref(), Some(&payload[..]));
    assert_eq!(reader.receive().await.unwrap(), None);
}

#[test]
fn content_length_parsing() {
    assert_eq!(parse_content_length(b"Content-Length: 42\r\n").unwrap(), 42);
    assert_eq!(parse_content_length(b"content-length:  7 \r\n").unwrap(), 7);
    assert!(parse_content_length(b"X-Other: 1\r\n").is_err());
    assert!(parse_content_length(b"Content-Length: not-a-number\r\n").is_err());
}

/// Backend that speaks content-length framing and echoes each message.
async fn echo_framed<R, W>(mut reader: R, mut writer: W) -> std::io::Result<()>
where
    R: MessageRead,
    W: MessageWrite,
{
    loop {
        match reader.receive().await? {
            None => {
                writer.send_eof().await?;
                return Ok(());
            }
            Some(message) => writer.send(&message).await?,
        }
    }
}

async fn run_bridge(messages: Vec<Vec<u8>>) -> std::io::Result<Vec<Vec<u8>>> {
    // Client speaks line framing; backend speaks content-length framing.
    let (client_w, relay_reads_client) = duplex(CAP);
    let (relay_writes_client, client_r) = duplex(CAP);
    let (relay_writes_backend, backend_r) = duplex(CAP);
    let (backend_w, relay_reads_backend) = duplex(CAP);

    let relay = Relay::run(
        LineReader::new(BufReader::new(relay_reads_client)),
        LineWriter::new(relay_writes_client),
        FramedReader::new(BufReader::new(relay_reads_backend)),
        FramedWriter::new(relay_writes_backend),
    );

    let backend = echo_framed(
        FramedReader::new(BufReader::new(backend_r)),
        FramedWriter::new(backend_w),
    );

    let client = async {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));
        for message in &messages {
            cw.send(message).await?;
        }
        cw.send_eof().await?;

        let mut received = Vec::new();
        while let Some(message) = cr.receive().await? {
            received.push(message);
        }
        Ok::<_, std::io::Error>(received)
    };

    let (_, _, received) = tokio::try_join!(relay, backend, client)?;
    Ok(received)
}

#[tokio::test]
async fn bridge_preserves_messages_and_boundaries() {
    let messages = vec![
        b"{\"jsonrpc\":\"2.0\",\"id\":1}".to_vec(),
        b"{\"jsonrpc\":\"2.0\",\"id\":2}".to_vec(),
        b"ping".to_vec(),
    ];
    let received = run_bridge(messages.clone()).await.unwrap();
    assert_eq!(received, messages);
}

#[tokio::test]
async fn bridge_handles_immediate_client_close() {
    let received = run_bridge(Vec::new()).await.unwrap();
    assert!(received.is_empty());
}

#[tokio::test]
async fn relay_added_latency_within_budget() {
    // Keep the relay and backend running; time single round-trips.
    let (client_w, relay_reads_client) = duplex(CAP);
    let (relay_writes_client, client_r) = duplex(CAP);
    let (relay_writes_backend, backend_r) = duplex(CAP);
    let (backend_w, relay_reads_backend) = duplex(CAP);

    let relay = Relay::run(
        LineReader::new(BufReader::new(relay_reads_client)),
        LineWriter::new(relay_writes_client),
        FramedReader::new(BufReader::new(relay_reads_backend)),
        FramedWriter::new(relay_writes_backend),
    );
    let backend = echo_framed(
        FramedReader::new(BufReader::new(backend_r)),
        FramedWriter::new(backend_w),
    );

    let driver = async {
        let mut cw = LineWriter::new(client_w);
        let mut cr = LineReader::new(BufReader::new(client_r));

        for _ in 0..50 {
            cw.send(b"{\"warm\":true}").await?;
            cr.receive().await?;
        }

        let mut samples = Vec::new();
        for i in 0..500u32 {
            let payload = format!("{{\"id\":{}}}", i);
            let start = Instant::now();
            cw.send(payload.as_bytes()).await?;
            let echoed = cr.receive().await?;
            samples.push(start.elapsed().as_secs_f64() * 1000.0);
            assert!(echoed.is_some());
        }
        cw.send_eof().await?;

        samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let median = samples[samples.len() / 2];
        let under = samples.iter().filter(|&&x| within_budget(x)).count() as f64
            / samples.len() as f64;
        println!("[latency] median={median:.4}ms budget={LATENCY_BUDGET_MS}ms within={under:.3}");
        assert!(within_budget(median));
        assert!(under >= 0.99);
        Ok::<_, std::io::Error>(())
    };

    tokio::try_join!(relay, backend, driver).unwrap();
}
