#[cfg(feature = "async-runtime")]
use crate::Output;
use crate::{BlockingOutput, Compression, Error, ErrorKind, Recorder, output, recorder};

/// Configures serialized metric output.
#[derive(Debug, Default)]
pub struct Builder {
    compression: Compression,
}

impl Builder {
    /// Creates a builder with default compression.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets output compression. Defaults to DEFLATE at level 6.
    pub fn compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    /// Builds a recorder and asynchronous output.
    #[cfg(feature = "async-runtime")]
    pub fn build(self) -> (Recorder, Output) {
        let (recorder, output) = self.build_blocking();
        (recorder, output::new(output))
    }

    /// Builds a recorder and blocking output without requiring a runtime.
    pub fn build_blocking(self) -> (Recorder, BlockingOutput) {
        let recorder = Recorder::default();
        let output = output::new_blocking(recorder::shared(&recorder), self.compression);
        (recorder, output)
    }

    /// Builds an initially disconnected recorder whose handles survive output sessions.
    ///
    /// Descriptions are retained while disconnected; emissions are ignored until
    /// [`Recorder::start_session_blocking`] or `Recorder::start_session` creates an output.
    ///
    /// ```
    /// use metrics_exporter_serialized::Builder;
    ///
    /// let recorder = Builder::new().build_session_recorder();
    /// recorder.set_filter(|name| !name.starts_with("private_"));
    /// let counter = metrics::with_local_recorder(&recorder, || metrics::counter!("requests"));
    /// let mut output = recorder.start_session_blocking()?;
    /// counter.increment(7);
    /// let _bytes = output.export_pending()?.unwrap();
    /// drop(output);
    /// counter.increment(100); // Disconnected emissions are ignored.
    /// let mut output = recorder.start_session_blocking()?;
    /// counter.increment(3); // The same handle now records into fresh session state.
    /// let _bytes = output.export_pending()?.unwrap();
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn build_session_recorder(self) -> Recorder {
        recorder::session_recorder(self.compression)
    }

    /// Installs the global recorder and returns asynchronous output.
    #[cfg(feature = "async-runtime")]
    pub fn install(self) -> Result<Output, Error> {
        self.install_blocking().map(output::new)
    }

    /// Installs the global recorder and returns blocking output without requiring a runtime.
    pub fn install_blocking(self) -> Result<BlockingOutput, Error> {
        let (recorder, output) = self.build_blocking();
        metrics::set_global_recorder(recorder).map_err(ErrorKind::Recorder)?;
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_blocking_applies_selected_compression() {
        use crate::proto;
        use prost::Message;

        let (recorder, mut output) = Builder::new()
            .compression(Compression::None)
            .build_blocking();
        metrics::with_local_recorder(&recorder, || metrics::counter!("requests").increment(7));
        let bytes = output.export_pending().unwrap().unwrap();
        let header = proto::EnvelopeHeader::decode_length_delimited(bytes.as_slice()).unwrap();
        assert_eq!(header.scheme(), proto::CompressionScheme::Uncompressed);
    }

    #[cfg(feature = "async-runtime")]
    #[test]
    fn async_output_and_replay_work_with_a_futures_executor() {
        use crate::ReplayOutcome;
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};

        let (recorder, output) = Builder::new().build();
        metrics::with_local_recorder(&recorder, || metrics::counter!("requests").increment(7));
        let destination = DebuggingRecorder::new();
        let mut decoder = crate::Decoder::new(output.into_reader());
        futures_lite::future::block_on(async {
            assert_eq!(
                decoder
                    .replay_next_with(&destination, |key| key)
                    .await
                    .unwrap(),
                ReplayOutcome::Replayed
            );
        });
        assert_eq!(
            destination.snapshotter().snapshot().into_vec()[0].3,
            DebugValue::Counter(7)
        );
    }
}
