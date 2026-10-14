use crate::Recorder;
use metrics::SetRecorderError;
use std::io;

/// Errors encountered while installing a recorder, exporting, or replaying metrics.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct Error(#[from] Kind);

#[derive(Debug, thiserror::Error)]
pub enum Kind {
    #[error("metric stream I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("invalid metric envelope")]
    InvalidEnvelope,
    #[error("invalid metric event")]
    InvalidEvent,
    #[error("unsupported metric compression scheme: {0}")]
    UnsupportedCompression(i32),
    #[error("histogram count cannot be represented on this platform")]
    HistogramCountOutOfRange,
    #[error("failed to install the recorder: {0}")]
    Recorder(#[from] SetRecorderError<Recorder>),
}
