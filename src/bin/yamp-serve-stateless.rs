//! Run a yamp stateless forward proxy over TCP.
//!
//! The stateless counterpart of yamp-serve. Each accepted client connection
//! gets its own StatelessForwarder, which opens its own connections to the
//! backends. There is no initialize handshake and no session id: every request
//! is self-describing, routed on its Mcp-Name header, and carries its protocol
//! version in `_meta` (SEP-2575). server/discover composes the backends' tool
//! surfaces.
//!
//! Usage:
//!   yamp-serve-stateless --listen 127.0.0.1:9100 --backend b0=127.0.0.1:9101 --backend b1=127.0.0.1:9102

use std::env;
use std::io;

use tokio::io::BufReader;
use tokio::net::{TcpListener, TcpStream};

use yamp::stateless::{StatelessBackend, StatelessForwarder};
use yamp::transport::{LineReader, LineWriter};

#[tokio::main]
async fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();
    let mut listen = String::new();
    let mut backends: Vec<(String, String)> = Vec::new();
    let mut i = 1;
    while i + 1 < args.len() {
        match args[i].as_str() {
            "--listen" => listen = args[i + 1].clone(),
            "--backend" => {
                let (id, addr) = args[i + 1].split_once('=').expect("backend as id=addr");
                backends.push((id.to_string(), addr.to_string()));
            }
            _ => {}
        }
        i += 2;
    }

    let listener = TcpListener::bind(&listen).await?;
    println!("listening on {listen}");
    loop {
        let (client, _) = listener.accept().await?;
        let specs = backends.clone();
        tokio::spawn(async move {
            let _ = handle(client, specs).await;
        });
    }
}

async fn handle(client: TcpStream, specs: Vec<(String, String)>) -> io::Result<()> {
    let (client_read, client_write) = client.into_split();
    let mut backends = Vec::new();
    for (id, addr) in &specs {
        let (backend_read, backend_write) = TcpStream::connect(addr).await?.into_split();
        backends.push(
            StatelessBackend::new(
                id.clone(),
                LineReader::new(BufReader::new(backend_read)),
                LineWriter::new(backend_write),
            )
            .expect("valid backend id"),
        );
    }
    let forwarder = StatelessForwarder::new(
        LineReader::new(BufReader::new(client_read)),
        LineWriter::new(client_write),
        backends,
    );
    forwarder.serve().await
}
