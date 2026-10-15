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

For reconnecting transports, use `Builder::build_session_recorder()` and retain that recorder.
Call `recorder.start_session()` for each connection (`start_session_blocking()` without async).
Only one output or live reader may be active at a time. Dropping it discards pending metrics
and stops collection until the next session. Existing handles remain usable; counter/gauge
values and pending histogram observations start fresh, while descriptions survive reconnects.
Emissions while disconnected are ignored. Already returned bytes remain the application's
responsibility; sessions do not retry delivery. The existing `build()` and `build_blocking()`
APIs continue collecting after output drop.

`recorder.set_filter(|name| ...)` replaces name-based admission, including for cached handles.
Rejected operations never update aggregates. The filter also applies to descriptions and
pending emissions at capture, but cannot change an already captured batch. Previously admitted
counter/gauge state is retained within a session. Descriptions remain stored while excluded
and become exportable when allowed again. Use `|_| false` to disable admission and `|_| true`
to allow all names. Predicates run under the recorder lock and must not record metrics or
reenter the recorder.

For a continuous byte stream, consume the output with `into_reader()`. Async I/O uses
`futures_io`; Tokio applications can use `tokio-util`'s compatibility adapters. The blocking
API uses `BlockingOutput`, `BlockingDecoder`, and `std::io::Read`.

`Decoder::replay_next()` replays one description or emission into the current recorder.
Use `replay_next_with()` to choose a recorder and map metric keys.
Use `replay_next_filtered_with()` to return `Some(key)` for admitted events or `None` to skip
them. Description names are mapped as keys without labels. Apply the same name mapping to
descriptions and emissions. Continue after both `ReplayOutcome::Replayed` and
`ReplayOutcome::Skipped`; only `EndOfStream` indicates completion. The unfiltered methods
never return `Skipped`. Async replay with a `Sync` destination produces a `Send` future when
the reader and mapping are also `Send`.

Compression defaults to raw DEFLATE and can be disabled through `Builder::compression()`.
The decoder reads the compression scheme from the stream. See the [protobuf schema](proto/metrics.proto).
Batches and pending metrics have no size limit. Export pacing and delivery retries are the
application's responsibility.

See the [blocking roundtrip](examples/roundtrip.rs) and [Tokio stream](examples/stream.rs) examples.
Building requires `protoc`.

Licensed under [Apache-2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT), at your option.
