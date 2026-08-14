//! SSE framing tests (Rust arm). Mirrors the Python arm, plus a bridge.

use tokio::io::{duplex, AsyncWriteExt, BufReader};

use yamp::relay::Relay;
use yamp::transport::{LineReader, LineWriter, MessageRead, MessageWrite, SseReader, SseWriter};

const CAP: usize = 1 << 16;

#[tokio::test]
async fn round_trip_and_send_encoding() {
    let (mut w, r) = duplex(CAP);
    w.write_all(b"data: {\"id\":1}\n\n").await.unwrap();
    w.shutdown().await.unwrap();
    let mut reader = SseReader::new(BufReader::new(r));
    assert_eq!(reader.receive().await.unwrap().as_deref(), Some(&b"{\"id\":1}"[..]));
    assert_eq!(reader.receive().await.unwrap(), None);

    let (w2, r2) = duplex(CAP);
    let mut writer = SseWriter::new(w2);
    writer.send(b"{\"x\":1}").await.unwrap();
    writer.send_eof().await.unwrap();
    let mut all = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut BufReader::new(r2), &mut all).await.unwrap();
    assert_eq!(all, b"data: {\"x\":1}\n\n");
}

#[tokio::test]
async fn multiline_payload_round_trips() {
    let (w, r) = duplex(CAP);
    let mut writer = SseWriter::new(w);
    writer.send(b"line1\nline2").await.unwrap();
    writer.send_eof().await.unwrap();
    let mut reader = SseReader::new(BufReader::new(r));
    assert_eq!(reader.receive().await.unwrap().as_deref(), Some(&b"line1\nline2"[..]));
}

#[tokio::test]
async fn comments_fields_and_leading_blanks_ignored() {
    let (mut w, r) = duplex(CAP);
    w.write_all(b"\n: keepalive\nevent: message\nid: 7\ndata: payload\n\n").await.unwrap();
    w.shutdown().await.unwrap();
    let mut reader = SseReader::new(BufReader::new(r));
    assert_eq!(reader.receive().await.unwrap().as_deref(), Some(&b"payload"[..]));
}

#[tokio::test]
async fn leading_space_stripped_once_and_unterminated_tail() {
    let (mut w, r) = duplex(CAP);
    w.write_all(b"data:nospace\n\ndata:  onespace\n\ndata: tail").await.unwrap();
    w.shutdown().await.unwrap();
    let mut reader = SseReader::new(BufReader::new(r));
    assert_eq!(reader.receive().await.unwrap().as_deref(), Some(&b"nospace"[..]));
    assert_eq!(reader.receive().await.unwrap().as_deref(), Some(&b" onespace"[..]));
    assert_eq!(reader.receive().await.unwrap().as_deref(), Some(&b"tail"[..]));
    assert_eq!(reader.receive().await.unwrap(), None);
}

// Backend that speaks stdio line framing and echoes each message.
async fn line_echo<R, W>(mut reader: R, mut writer: W) -> std::io::Result<()>
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
            Some(m) => writer.send(&m).await?,
        }
    }
}

#[tokio::test]
async fn relay_bridges_sse_client_to_stdio_backend() {
    let (client_w, relay_reads_client) = duplex(CAP);
    let (relay_writes_client, client_r) = duplex(CAP);
    let (relay_to_backend, backend_r) = duplex(CAP);
    let (backend_w, relay_reads_backend) = duplex(CAP);

    let relay = Relay::run(
        SseReader::new(BufReader::new(relay_reads_client)),
        SseWriter::new(relay_writes_client),
        LineReader::new(BufReader::new(relay_reads_backend)),
        LineWriter::new(relay_to_backend),
    );
    let backend = line_echo(
        LineReader::new(BufReader::new(backend_r)),
        LineWriter::new(backend_w),
    );
    let client = async {
        let mut cw = SseWriter::new(client_w);
        let mut cr = SseReader::new(BufReader::new(client_r));
        cw.send(b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}").await?;
        let echoed = cr.receive().await?;
        cw.send_eof().await?;
        Ok::<_, std::io::Error>(echoed)
    };

    let (_relay, _backend, echoed) = tokio::try_join!(relay, backend, client).unwrap();
    assert_eq!(echoed.as_deref(), Some(&b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}"[..]));
}
