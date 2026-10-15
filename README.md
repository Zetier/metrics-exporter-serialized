# metrics-exporter-serialized

Serialize metrics recorded through [`metrics`](https://docs.rs/metrics) and replay them into
another recorder. The application supplies the transport and decides when to export.

Counters and gauges export their current values. Histograms combine repeated observations of
the same value into a value and count. Intermediate counter and gauge values and ordering
between metrics are not preserved.

Use `Builder::new().install()?` to install the recorder and get an `Output`. Each
`output.export_pending().await?` returns a complete batch of bytes, or `None` if nothing is
pending. Updates recorded after capture belong to the next batch. Canceling an export leaves
its batch available for the next call.

For a continuous byte stream, consume the output with `into_reader()`. Async I/O uses
`futures_io`; Tokio applications can use `tokio-util`'s compatibility adapters. The blocking
API uses `BlockingOutput`, `BlockingDecoder`, and `std::io::Read`.

`Decoder::replay_next()` replays one description or emission into the current recorder.
Use `replay_next_with()` to map metric keys.

Compression defaults to raw DEFLATE and can be disabled through `Builder::compression()`.
The decoder reads the compression scheme from the stream. See the [protobuf schema](proto/metrics.proto).
Batches and pending metrics have no size limit. Export pacing and delivery retries are the
application's responsibility.

See the [blocking roundtrip](examples/roundtrip.rs) and [Tokio stream](examples/stream.rs) examples.
Building requires `protoc`.

Licensed under [Apache-2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT), at your option.
