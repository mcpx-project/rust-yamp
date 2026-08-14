# yamp, Rust arm

Rust implementation of yamp (Yet Another MCP Proxy), a forward and transparent
proxy for the Model Context Protocol that works in both MCP protocol modes,
stateful and stateless. Built on tokio and serde_json.

Built in lockstep with the [Python arm](https://github.com/mcpx-project/python-yamp)
from one shared increment sequence. Specifications, user guide, conformance
declaration, and benchmarks live in the
[docs repository](https://github.com/mcpx-project/docs).

## Build and test

```
cargo test
cargo clippy --all-targets
```

## Running as a server

Point a server at one or more backends.

```
cargo run --release --bin yamp-serve-streamable -- \
  --listen 127.0.0.1:9100 --backend github=127.0.0.1:9101
```

Server entrypoints in `src/bin/`:

| Binary | Transport |
|---|---|
| `yamp-serve` | TCP with stdio framing |
| `yamp-serve-stateless` | TCP, stateless forwarder |
| `yamp-serve-http` | stateless HTTP |
| `yamp-serve-streamable` | Streamable HTTP with `Mcp-Session-Id` sessions and a `GET /mcp` SSE stream |

A non-loopback bind is refused without client authentication unless `--insecure`
is passed, and a client credential is structurally never forwarded to a backend.

## Configuration

For pools and resilience, the Streamable HTTP server reads a config file.

```
yamp-serve-streamable --config proxy.json
```

A backend id maps to one or more addresses, tried in order with failover at
connect time.

```json
{
  "listen": "127.0.0.1:9100",
  "backends": { "github": { "addresses": ["10.0.0.1:9101", "10.0.0.2:9101"] } },
  "resilience": { "failureThreshold": 3, "requestTimeout": 5, "healthInterval": 10 }
}
```

Reload without restarting. In-flight sessions are kept and the document is
validated before it is applied, so a typo leaves the running config in place.

```
kill -HUP <pid>
```

## Operator tooling

Two non-serving diagnostic binaries, each supporting `--json` and documented exit
codes.

```
yamp-doctor  --config proxy.json                 # nginx -t analog: server-surface preflight
yamp-config validate  --config proxy.json        # does the document load and conform
yamp-config explain   --config proxy.json listen
yamp-config effective --config proxy.json        # every key's resolved value and provenance
yamp-config diff --config a.json --to b.json
yamp-config adapt --config human.json            # normalize shorthands to canonical JSON
```

At runtime, `GET /status` returns a read-only operational snapshot and `--tap`
prints a credential-redacted capture of each client request.

## Modules (`src/`)

Transport and proxy core:

- `transport/`, `relay.rs`: Layer 1 transports (stdio line framing, HTTP
  content-length framing, SSE event framing) and the byte-faithful relay. The
  framings are independent per side and per backend, so the relay and router
  bridge any mix.
- `forward.rs`, `stateless.rs`: forward proxy, stateful dual handshake and
  stateless mode.
- `router.rs`, `namespace.rs`, `collision.rs`, `routing.rs`: multi-backend
  routing, `__` namespacing, the four collision strategies, filter pushdown, and
  keyword pre-selection.
- `transparent.rs`, `transparent_l2.rs`: transparent Level 1 (transport-aware)
  and Level 2 (protocol-aware).
- `variants.rs`, `cache.rs`: server variants with variant-bound composite
  cursors, and the list cache honoring `ttlMs` and `cacheScope`.

Resilience, policy, and accountability:

- `resilience.rs`: circuit breaker, partial-failure fan-out, timeouts, no-retry.
- `policy.rs`, `auth.rs`, `security.rs`: client auth, per-backend credential
  injection with confused-deputy protection, claim validation, bind safety.
- `signing.rs`, `tap.rs`: the signed hash-chained audit log, and the redacting
  live capture.
- `observability.rs`, `status.rs`, `instrument.rs`: hop tracing and W3C Trace
  Context, the status snapshot, and the latency budget.

Capability, server role, and extensions:

- `capability.rs`, `version.rs`: extension-aware capability composition and
  protocol version negotiation.
- `handler.rs`, `server.rs`, `schema.rs`, `pool.rs`, `doctor.rs`: the dispatch
  seam, server-origination concerns, schema validation, the worker pool, and the
  server-surface preflight.
- `tasks.rs`, `subscriptions.rs`: task routing and origination, resource
  subscription routing and origination.
- `rest.rs`: REST to MCP conversion.
- `filters.rs`, `content.rs`, `callout.rs`, `icap.rs`: the extension filter
  chain, the typed content-block iterator, the out-of-process callout transport,
  and the reference ICAP bridge.

Shared foundations:

- `jsonrpc.rs`, `errors.rs`, `config.rs`, `media.rs`, `base64.rs`: JSON-RPC
  helpers, the canonical error registry, the config loader, media type
  negotiation, and base64.

## Cross-arm corpus

`conformance/` pins this arm to the Python arm. `differential-corpus.json` pins
pure functions, `flow-corpus.json` pins whole message flows so both routers
produce an identical client-facing sequence, and `sep-0000-traceability.json`
maps spec clauses to the tests that evidence them. The Python arm generates the
corpus and the docs repository's `tools/sync-corpus.sh` propagates it here.

## Status

323 tests, zero compiler warnings, zero clippy warnings. Per-message proxy
overhead is held at or under 10 ms, enforced as a test tier.

## License

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
