//! Serialized metric events for application-managed transports.
//!
//! [`Recorder`] aggregates counter and gauge state and histogram observation counts.
//! [`BlockingOutput`] provides serialized bytes for a transport, with optional raw DEFLATE
//! compression. [`BlockingDecoder`] reads those bytes and replays one description or emission
//! at a time.
//!
//! This example uses `metrics-util` with its `debugging` feature to inspect the received metric.
//!
//! ```
//! use metrics_exporter_serialized::{BlockingDecoder, Builder, ReplayOutcome};
//! use metrics_util::debugging::{DebugValue, DebuggingRecorder};
//!
//! let (source, mut output) = Builder::new().build_blocking();
//! metrics::with_local_recorder(&source, || {
//!     metrics::histogram!("request_duration_seconds").record(0.125);
//! });
//! let destination = DebuggingRecorder::new();
//! let _guard = metrics::set_default_local_recorder(&destination);
//! let batch = output.export_pending().unwrap().unwrap();
//! let mut decoder = BlockingDecoder::new(batch.as_slice());
//! assert_eq!(
//!     decoder.replay_next().unwrap(),
//!     ReplayOutcome::Replayed,
//! );
//! let received = destination.snapshotter().snapshot().into_vec();
//! assert_eq!(received.len(), 1);
//! assert_eq!(received[0].0.key().name(), "request_duration_seconds");
//! assert_eq!(received[0].3, DebugValue::Histogram(vec![0.125.into()]));
//! ```
//!
//! [`BlockingOutput::export_pending`] exports one finite batch without waiting for updates.
//! [`BlockingOutput::into_reader`] consumes the output to provide a live byte stream instead.
//! The default `async-runtime` feature provides async equivalents with `Output` and `Decoder`.
#![deny(missing_docs)]

mod builder;
mod compression;
mod decoder;
mod error;
mod event;
mod output;
mod proto {
    include!(concat!(env!("OUT_DIR"), "/metrics.serialized.v1.rs"));
}
mod recorder;

pub use builder::Builder;
pub use compression::{Compression, DeflateLevel};
#[cfg(feature = "async-runtime")]
pub use decoder::Decoder;
pub use decoder::{BlockingDecoder, ReplayOutcome};
pub use error::Error;
use error::Kind as ErrorKind;
use event::{Description, Emission, Event, Operation, Registration};
pub use output::BlockingOutput;
#[cfg(feature = "async-runtime")]
pub use output::Output;
pub use recorder::Recorder;
