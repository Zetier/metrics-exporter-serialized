#[cfg(feature = "async-runtime")]
use std::{
    future::{self, Future},
    pin::Pin,
    task::{Context, Poll, ready},
};
use std::{
    io::{self, Cursor, Read, Write},
    sync::Arc,
};

#[cfg(feature = "async-runtime")]
use blocking::Task;
#[cfg(feature = "async-runtime")]
use futures_io::AsyncRead;

use flate2::{Compression as DeflateCompression, write::DeflateEncoder};
use prost::Message;

use crate::{
    Compression, Error, ErrorKind, Event,
    proto::{self, CompressionScheme},
    recorder::Shared,
};

fn capture_pending(shared: &Shared) -> Option<Vec<Event>> {
    let (descriptions, emissions) = {
        let mut state = shared.state.lock().expect("metric buffer lock poisoned");
        if !state.has_pending() {
            return None;
        }
        state.descriptions_changed = false;
        let descriptions: Vec<_> = state
            .descriptions
            .values()
            .filter(|description| state.allows(description.name.as_str()))
            .cloned()
            .collect();
        let emissions: Vec<_> = state
            .take_emissions()
            .into_iter()
            .filter(|emission| state.allows(emission.registration.key.name()))
            .collect();
        if descriptions.is_empty() && emissions.is_empty() {
            return None;
        }
        (descriptions, emissions)
    };
    Some(
        descriptions
            .into_iter()
            .map(Event::Description)
            .chain(emissions.into_iter().map(Event::Emission))
            .collect(),
    )
}

pub fn encode_pending(
    shared: &Shared,
    compression: Compression,
) -> Result<Option<Vec<u8>>, ErrorKind> {
    capture_pending(shared)
        .map(|events| encode_events(events.iter().map(proto::Event::from), compression))
        .transpose()
}

fn encode_events(
    events: impl Iterator<Item = proto::Event>,
    compression: Compression,
) -> Result<Vec<u8>, ErrorKind> {
    let data = match compression {
        Compression::None => {
            let mut data = Vec::new();
            write_events(events, &mut data)?;
            data
        }
        Compression::Deflate { level } => {
            let mut encoder = DeflateEncoder::new(Vec::new(), DeflateCompression::from(level));
            write_events(events, &mut encoder)?;
            encoder.finish()?
        }
    };
    let mut envelope = proto::EnvelopeHeader {
        scheme: CompressionScheme::from(compression) as i32,
        payload_length: data.len() as u64,
    }
    .encode_length_delimited_to_vec();
    envelope.extend(data);
    Ok(envelope)
}

fn write_events(
    events: impl Iterator<Item = proto::Event>,
    writer: &mut impl Write,
) -> Result<(), ErrorKind> {
    let mut buffer = Vec::new();
    for event in events {
        buffer.clear();
        event
            .encode_length_delimited(&mut buffer)
            .expect("Vec encoding cannot fail");
        writer.write_all(&buffer)?;
    }
    Ok(())
}

/// Exports finite batches of serialized metrics asynchronously.
///
/// This is the sole consumer of its recorder's pending metrics. Dropping it may discard
/// unexported data. For session recorders, dropping it also stops collection; otherwise
/// recording continues.
#[cfg(feature = "async-runtime")]
#[derive(Debug)]
pub struct Output {
    inner: BlockingOutput,
    pending: Option<Task<Result<Vec<u8>, ErrorKind>>>,
}

/// Exports finite batches of serialized metrics synchronously.
///
/// This is the sole consumer of its recorder's pending metrics. Dropping it may discard
/// unexported data. For session recorders, dropping it also stops collection; otherwise
/// recording continues. Use blocking exports outside async tasks.
#[derive(Debug)]
pub struct BlockingOutput {
    shared: Arc<Shared>,
    compression: Compression,
    session: bool,
}

#[cfg(feature = "async-runtime")]
pub fn new(inner: BlockingOutput) -> Output {
    Output {
        inner,
        pending: None,
    }
}

pub fn new_blocking(shared: Arc<Shared>, compression: Compression) -> BlockingOutput {
    BlockingOutput {
        shared,
        compression,
        session: false,
    }
}

pub fn new_session(shared: Arc<Shared>, compression: Compression) -> BlockingOutput {
    BlockingOutput {
        shared,
        compression,
        session: true,
    }
}

impl Drop for BlockingOutput {
    fn drop(&mut self) {
        if self.session {
            self.shared
                .state
                .lock()
                .expect("metric buffer lock poisoned")
                .end_session();
        }
    }
}

impl BlockingOutput {
    /// Captures pending metrics once and returns one complete envelope, or `None` if empty.
    /// Updates recorded after capture remain pending for the next export.
    pub fn export_pending(&mut self) -> Result<Option<Vec<u8>>, Error> {
        encode_pending(&self.shared, self.compression).map_err(Error::from)
    }

    /// Consumes this output to provide a live byte reader that waits for metric updates.
    /// Export errors end the reader.
    pub fn into_reader(self) -> impl Read + Send {
        Reader::new(self)
    }
}

#[cfg(feature = "async-runtime")]
impl Output {
    /// Captures pending metrics once and returns one complete envelope, or `None` if empty.
    ///
    /// Capture occurs when first polled; later updates remain pending for the next export.
    /// Serialization and compression run off-thread. Canceling this call retains its batch:
    /// the next export or live read resumes that work instead of capturing another batch.
    pub async fn export_pending(&mut self) -> Result<Option<Vec<u8>>, Error> {
        future::poll_fn(|cx| self.poll_export_pending(cx)).await
    }

    /// Consumes this output to provide a live byte reader that waits for metric updates.
    /// Canceling a read preserves pending data. Export errors end the reader.
    pub fn into_reader(self) -> impl AsyncRead + Unpin + Send {
        Reader::new(self)
    }

    fn poll_export_pending(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<Vec<u8>>, Error>> {
        if self.pending.is_none() {
            let Some(events) = capture_pending(&self.inner.shared) else {
                return Poll::Ready(Ok(None));
            };
            let compression = self.inner.compression;
            self.pending = Some(blocking::unblock(move || {
                encode_events(events.iter().map(proto::Event::from), compression)
            }));
        }
        let result = ready!(Pin::new(self.pending.as_mut().unwrap()).poll(cx));
        self.pending = None;
        Poll::Ready(result.map(Some).map_err(Error::from))
    }
}

#[derive(Debug)]
struct Reader<T> {
    output: T,
    buffer: Cursor<Vec<u8>>,
    done: bool,
}

impl<T> Reader<T> {
    fn new(output: T) -> Self {
        Self {
            output,
            buffer: Cursor::new(Vec::new()),
            done: false,
        }
    }

    fn receive(&mut self, item: Result<Option<Vec<u8>>, Error>) -> io::Result<()> {
        match item {
            Ok(Some(bytes)) => self.buffer = Cursor::new(bytes),
            Ok(None) => {}
            Err(error) => {
                self.done = true;
                return Err(io::Error::other(error));
            }
        }
        Ok(())
    }
}

impl Read for Reader<BlockingOutput> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        loop {
            let size = self.buffer.read(buffer)?;
            if size > 0 || buffer.is_empty() || self.done {
                return Ok(size);
            }
            let state = self
                .output
                .shared
                .state
                .lock()
                .expect("metric buffer lock poisoned");
            drop(
                self.output
                    .shared
                    .changed
                    .wait_while(state, |state| !state.has_pending())
                    .expect("metric buffer lock poisoned"),
            );
            let item = self.output.export_pending();
            self.receive(item)?;
        }
    }
}

#[cfg(feature = "async-runtime")]
impl AsyncRead for Reader<Output> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        loop {
            if buffer.is_empty() || self.done {
                return Poll::Ready(Ok(0));
            }
            let size = self.buffer.read(buffer)?;
            if size > 0 {
                return Poll::Ready(Ok(size));
            }
            self.output.inner.shared.notification.register(cx.waker());
            let item = ready!(self.output.poll_export_pending(cx));
            if matches!(item, Ok(None)) {
                return Poll::Pending;
            }
            self.receive(item)?;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Builder, DeflateLevel, Operation, Recorder, ReplayOutcome, decoder::BlockingDecoder,
        recorder,
    };
    use flate2::read::DeflateDecoder;
    #[cfg(feature = "async-runtime")]
    use futures_lite::{
        future,
        io::{self as async_io, AsyncReadExt},
    };
    use metrics::GaugeValue;
    #[cfg(feature = "async-runtime")]
    use std::sync::mpsc::{self, Sender};
    use std::{sync::Barrier, thread};

    fn counter(recorder: &Recorder) -> metrics::Counter {
        metrics::with_local_recorder(recorder, || metrics::counter!("requests"))
    }

    fn envelope(value: u64, compression: Compression) -> Vec<u8> {
        let recorder = Recorder::default();
        let counter = counter(&recorder);
        counter.increment(value);
        encode_pending(&recorder::shared(&recorder), compression)
            .unwrap()
            .unwrap()
    }

    #[test]
    fn empty_blocking_exports_return_none_without_waiting() {
        for compression in [Compression::None, Compression::default()] {
            let (_recorder, mut output) = crate::Builder::new()
                .compression(compression)
                .build_blocking();
            assert!(output.export_pending().unwrap().is_none());
        }
    }

    #[cfg(feature = "async-runtime")]
    #[test]
    fn empty_async_exports_are_immediately_ready() {
        future::block_on(async {
            for compression in [Compression::None, Compression::default()] {
                let (_recorder, mut output) =
                    crate::Builder::new().compression(compression).build();
                assert!(matches!(
                    future::poll_once(output.export_pending()).await,
                    Some(Ok(None))
                ));
            }
        });
    }

    #[test]
    fn sending_a_finite_batch_leaves_transport_metrics_for_the_next_export() {
        for compression in [Compression::None, Compression::default()] {
            let (recorder, mut output) = crate::Builder::new()
                .compression(compression)
                .build_blocking();
            let counter = counter(&recorder);
            counter.increment(7);
            let batch = output.export_pending().unwrap().unwrap();
            let mut transmitted = Vec::new();
            for chunk in batch.chunks(3) {
                transmitted.extend_from_slice(chunk);
                counter.increment(1);
            }
            assert_eq!(transmitted, envelope(7, compression));
            let next_value = 7 + batch.len().div_ceil(3) as u64;
            transmitted.extend(output.export_pending().unwrap().unwrap());
            let values: Vec<_> = BlockingDecoder::new(transmitted.as_slice())
                .map(|event| {
                    let Event::Emission(emission) = event.unwrap() else {
                        panic!("expected emission")
                    };
                    let Operation::AbsoluteCounter(value) = emission.operation else {
                        panic!("expected counter")
                    };
                    value
                })
                .collect();
            assert_eq!(values, vec![7, next_value]);
            assert!(output.export_pending().unwrap().is_none());
        }
    }

    #[cfg(feature = "async-runtime")]
    #[test]
    fn async_exports_capture_once_on_first_poll() {
        future::block_on(async {
            for compression in [Compression::None, Compression::default()] {
                let (recorder, mut output) = crate::Builder::new().compression(compression).build();
                let counter = counter(&recorder);
                counter.increment(7);
                let first = future::poll_once(output.export_pending()).await;
                thread::spawn(move || counter.increment(3)).join().unwrap();
                let bytes = match first {
                    Some(result) => result,
                    None => output.export_pending().await,
                }
                .unwrap()
                .unwrap();
                assert_eq!(bytes, envelope(7, compression));
                assert_eq!(
                    output.export_pending().await.unwrap().unwrap(),
                    envelope(10, compression)
                );
                assert!(matches!(
                    future::poll_once(output.export_pending()).await,
                    Some(Ok(None))
                ));
            }
        });
    }

    #[cfg(feature = "async-runtime")]
    fn pause_encoding(output: &mut Output) -> Sender<()> {
        let events = capture_pending(&output.inner.shared).unwrap();
        let compression = output.inner.compression;
        let (release, wait) = mpsc::channel();
        let (started, ready) = mpsc::channel();
        output.pending = Some(blocking::unblock(move || {
            started.send(()).unwrap();
            wait.recv().unwrap();
            encode_events(events.iter().map(proto::Event::from), compression)
        }));
        ready.recv().unwrap();
        release
    }

    #[cfg(feature = "async-runtime")]
    #[test]
    fn dropping_a_session_abandons_in_flight_compression_before_reconnecting() {
        future::block_on(async {
            let recorder = Builder::new().build_session_recorder();
            let counter = counter(&recorder);
            let mut first = recorder.start_session().unwrap();
            counter.increment(7);
            let release = pause_encoding(&mut first);
            assert!(future::poll_once(first.export_pending()).await.is_none());
            counter.increment(50);
            drop(first);
            counter.increment(100);
            let mut second = recorder.start_session().unwrap();
            counter.increment(3);
            release.send(()).unwrap();
            assert_eq!(
                second.export_pending().await.unwrap().unwrap(),
                envelope(3, Compression::default())
            );
            assert!(second.export_pending().await.unwrap().is_none());
        });
    }

    #[cfg(feature = "async-runtime")]
    #[test]
    fn filtering_does_not_rewrite_an_already_captured_batch() {
        future::block_on(async {
            let (recorder, mut output) = Builder::new().build();
            let counter = counter(&recorder);
            counter.increment(7);
            let release = pause_encoding(&mut output);
            recorder.set_filter(|_| false);
            counter.increment(100);
            release.send(()).unwrap();
            assert_eq!(
                output.export_pending().await.unwrap().unwrap(),
                envelope(7, Compression::default())
            );
            assert!(output.export_pending().await.unwrap().is_none());
        });
    }

    #[test]
    fn dropping_continuous_output_does_not_stop_recording() {
        let (recorder, output) = Builder::new().build_blocking();
        let counter = counter(&recorder);
        drop(output);
        counter.increment(7);
        assert_eq!(
            encode_pending(&recorder::shared(&recorder), Compression::default())
                .unwrap()
                .unwrap(),
            envelope(7, Compression::default())
        );
    }

    #[cfg(feature = "async-runtime")]
    #[test]
    fn canceled_exports_resume_the_captured_batch() {
        future::block_on(async {
            for compression in [Compression::None, Compression::default()] {
                let (recorder, mut output) = crate::Builder::new().compression(compression).build();
                let counter = counter(&recorder);
                counter.increment(7);
                let release = pause_encoding(&mut output);
                assert!(future::poll_once(output.export_pending()).await.is_none());
                counter.increment(3);
                release.send(()).unwrap();
                assert_eq!(
                    output.export_pending().await.unwrap().unwrap(),
                    envelope(7, compression)
                );
                assert_eq!(
                    output.export_pending().await.unwrap().unwrap(),
                    envelope(10, compression)
                );
                assert!(output.export_pending().await.unwrap().is_none());
            }
        });
    }

    #[cfg(feature = "async-runtime")]
    #[test]
    fn live_reader_resumes_a_canceled_export_before_capturing_more_metrics() {
        future::block_on(async {
            for compression in [Compression::None, Compression::default()] {
                let (recorder, mut output) = crate::Builder::new().compression(compression).build();
                let counter = counter(&recorder);
                counter.increment(7);
                let release = pause_encoding(&mut output);
                assert!(future::poll_once(output.export_pending()).await.is_none());
                counter.increment(3);
                let mut reader = output.into_reader();
                release.send(()).unwrap();
                let expected = [envelope(7, compression), envelope(10, compression)].concat();
                let mut bytes = vec![0; expected.len()];
                AsyncReadExt::read_exact(&mut reader, &mut bytes)
                    .await
                    .unwrap();
                assert_eq!(bytes, expected);
            }
        });
    }

    #[test]
    fn blocking_reads_finish_the_current_envelope_before_exporting_new_events() {
        let recorder = Recorder::default();
        let counter = counter(&recorder);
        let mut output = new_blocking(recorder::shared(&recorder), Compression::None).into_reader();
        counter.increment(1);
        counter.increment(2);
        let expected = envelope(3, Compression::None);
        let mut bytes = vec![0; expected.len()];
        output.read_exact(&mut bytes[..2]).unwrap();

        counter.increment(4);
        for byte in &mut bytes[2..] {
            output.read_exact(std::slice::from_mut(byte)).unwrap();
        }
        assert_eq!(bytes, expected);

        let expected = envelope(7, Compression::None);
        let mut bytes = vec![0; expected.len()];
        output.read_exact(&mut bytes).unwrap();
        assert_eq!(bytes, expected);
    }

    #[test]
    fn blocking_reads_receive_events_from_another_thread() {
        let recorder = Recorder::default();
        let counter = counter(&recorder);
        let mut output =
            new_blocking(recorder::shared(&recorder), Compression::default()).into_reader();
        let expected = envelope(7, Compression::default());
        let ready = Barrier::new(2);
        thread::scope(|scope| {
            let reader = scope.spawn(|| {
                let mut bytes = vec![0; expected.len()];
                ready.wait();
                output.read_exact(&mut bytes).unwrap();
                assert_eq!(bytes, expected);
            });
            ready.wait();
            counter.increment(7);
            reader.join().unwrap();
        });
    }

    #[test]
    fn empty_blocking_reads_do_not_discard_events() {
        let recorder = Recorder::default();
        counter(&recorder).increment(7);
        let mut output =
            new_blocking(recorder::shared(&recorder), Compression::default()).into_reader();
        assert_eq!(output.read(&mut []).unwrap(), 0);
        let expected = envelope(7, Compression::default());
        let mut bytes = vec![0; expected.len()];
        output.read_exact(&mut bytes).unwrap();
        assert_eq!(bytes, expected);
    }

    #[cfg(feature = "async-runtime")]
    #[test]
    fn async_copy_finishes_the_current_envelope_before_exporting_new_events() {
        future::block_on(async {
            let recorder = Recorder::default();
            let counter = counter(&recorder);
            let mut output = new(new_blocking(
                recorder::shared(&recorder),
                Compression::default(),
            ))
            .into_reader();
            counter.increment(1);
            counter.increment(2);
            let expected = envelope(3, Compression::default());
            let mut first = [0; 2];
            AsyncReadExt::read_exact(&mut output, &mut first)
                .await
                .unwrap();

            counter.increment(4);
            let mut rest = Vec::new();
            let remaining = (expected.len() - first.len()) as u64;
            let copied = async_io::copy(AsyncReadExt::take(&mut output, remaining), &mut rest)
                .await
                .unwrap();
            assert_eq!(copied, remaining);
            assert_eq!([first.as_slice(), rest.as_slice()].concat(), expected);

            let expected = envelope(7, Compression::default());
            let mut bytes = vec![0; expected.len()];
            AsyncReadExt::read_exact(&mut output, &mut bytes)
                .await
                .unwrap();
            assert_eq!(bytes, expected);
        });
    }

    #[cfg(feature = "async-runtime")]
    #[test]
    fn canceling_an_idle_read_preserves_the_next_events() {
        future::block_on(async {
            let recorder = Recorder::default();
            let mut output = new(new_blocking(
                recorder::shared(&recorder),
                Compression::default(),
            ))
            .into_reader();
            let mut buffer = [0; 1];
            assert!(
                future::poll_once(AsyncReadExt::read(&mut output, &mut buffer))
                    .await
                    .is_none()
            );
            counter(&recorder).increment(7);
            let expected = envelope(7, Compression::default());
            let mut bytes = vec![0; expected.len()];
            AsyncReadExt::read_exact(&mut output, &mut bytes)
                .await
                .unwrap();
            assert_eq!(bytes, expected);
        });
    }

    #[cfg(feature = "async-runtime")]
    #[test]
    fn canceling_a_read_during_export_preserves_the_envelope() {
        let (recorder, mut output) = crate::Builder::new().build();
        counter(&recorder).increment(7);
        let release = pause_encoding(&mut output);
        let mut output = output.into_reader();
        futures_lite::future::block_on(async {
            let mut buffer = [0; 1];
            assert!(
                future::poll_once(AsyncReadExt::read(&mut output, &mut buffer))
                    .await
                    .is_none()
            );
            release.send(()).unwrap();
            let expected = envelope(7, Compression::default());
            let mut bytes = vec![0; expected.len()];
            AsyncReadExt::read_exact(&mut output, &mut bytes)
                .await
                .unwrap();
            assert_eq!(bytes, expected);
        });
    }

    #[cfg(feature = "async-runtime")]
    #[test]
    fn empty_async_reads_do_not_discard_events() {
        future::block_on(async {
            let recorder = Recorder::default();
            counter(&recorder).increment(7);
            let mut output = new(new_blocking(
                recorder::shared(&recorder),
                Compression::default(),
            ))
            .into_reader();
            assert_eq!(AsyncReadExt::read(&mut output, &mut []).await.unwrap(), 0);
            let expected = envelope(7, Compression::default());
            let mut bytes = vec![0; expected.len()];
            AsyncReadExt::read_exact(&mut output, &mut bytes)
                .await
                .unwrap();
            assert_eq!(bytes, expected);
        });
    }

    #[cfg(feature = "async-runtime")]
    #[test]
    fn waiting_async_reads_wake_for_description_updates() {
        use std::{
            sync::atomic::{AtomicBool, Ordering},
            task::{Wake, Waker},
        };

        #[derive(Default)]
        struct WakeFlag(AtomicBool);

        impl Wake for WakeFlag {
            fn wake(self: Arc<Self>) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let (recorder, output) = crate::Builder::new().build();
        let mut output = output.into_reader();
        let flag = Arc::new(WakeFlag::default());
        let waker = Waker::from(Arc::clone(&flag));
        let mut cx = Context::from_waker(&waker);
        assert!(
            Pin::new(&mut output)
                .poll_read(&mut cx, &mut [0])
                .is_pending()
        );
        metrics::with_local_recorder(&recorder, || {
            metrics::describe_counter!("requests", "requests served")
        });
        assert!(flag.0.load(Ordering::SeqCst));
        let destination = metrics_util::debugging::DebuggingRecorder::new();
        let mut decoder = crate::Decoder::new(output);
        assert_eq!(
            future::block_on(decoder.replay_next_with(&destination, |key| key)).unwrap(),
            crate::ReplayOutcome::Replayed
        );
    }

    #[test]
    fn description_updates_are_exported_once_without_emissions() {
        let recorder = Recorder::default();
        let shared = recorder::shared(&recorder);
        let compression = Compression::default();
        assert!(encode_pending(&shared, compression).unwrap().is_none());
        for text in ["old", "new"] {
            metrics::with_local_recorder(&recorder, || {
                metrics::describe_counter!("requests", text)
            });
            let bytes = encode_pending(&shared, compression).unwrap().unwrap();
            let events = BlockingDecoder::new(bytes.as_slice())
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert!(
                matches!(events.as_slice(), [Event::Description(description)]
                if description.text.as_ref() == text)
            );
            assert!(encode_pending(&shared, compression).unwrap().is_none());
        }
    }

    #[test]
    fn emissions_include_previously_exported_descriptions() {
        let recorder = Recorder::default();
        let shared = recorder::shared(&recorder);
        let compression = Compression::default();
        metrics::with_local_recorder(&recorder, || {
            metrics::describe_counter!("requests", "requests served")
        });
        assert!(encode_pending(&shared, compression).unwrap().is_some());
        metrics::with_local_recorder(&recorder, || metrics::counter!("requests").increment(7));
        let bytes = encode_pending(&shared, compression).unwrap().unwrap();
        let events = BlockingDecoder::new(bytes.as_slice())
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(
            matches!(events.as_slice(), [Event::Description(description), Event::Emission(emission)]
            if description.text.as_ref() == "requests served" && matches!(emission.operation, Operation::AbsoluteCounter(7)))
        );
        assert!(encode_pending(&shared, compression).unwrap().is_none());
    }

    #[test]
    fn aggregated_values_replay_across_exports() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};

        for compression in [Compression::None, Compression::default()] {
            let (recorder, mut output) = crate::Builder::new()
                .compression(compression)
                .build_blocking();
            let destination = DebuggingRecorder::new();
            let (counter, gauge, histogram) = metrics::with_local_recorder(&recorder, || {
                (
                    metrics::counter!("requests"),
                    metrics::gauge!("load"),
                    metrics::histogram!("duration"),
                )
            });
            for window in 1..=2 {
                for _ in 0..100 {
                    counter.increment(1);
                    gauge.increment(0.5);
                    histogram.record(5.5);
                }
                let bytes = output.export_pending().unwrap().unwrap();
                let mut decoder = BlockingDecoder::new(bytes.as_slice());
                for _ in 0..3 {
                    assert_eq!(
                        decoder.replay_next_with(&destination, |key| key).unwrap(),
                        ReplayOutcome::Replayed
                    );
                }
                assert_eq!(
                    decoder.replay_next_with(&destination, |key| key).unwrap(),
                    ReplayOutcome::EndOfStream
                );
                let mut values: Vec<_> = destination
                    .snapshotter()
                    .snapshot()
                    .into_vec()
                    .into_iter()
                    .map(|(key, _, _, value)| (key.key().name().to_owned(), value))
                    .collect();
                values.sort_by(|left, right| left.0.cmp(&right.0));
                assert_eq!(
                    values,
                    vec![
                        (
                            "duration".into(),
                            DebugValue::Histogram(vec![5.5.into(); 100])
                        ),
                        (
                            "load".into(),
                            DebugValue::Gauge((window as f64 * 50.0).into())
                        ),
                        ("requests".into(), DebugValue::Counter(window as u64 * 100)),
                    ]
                );
                assert!(output.export_pending().unwrap().is_none());
            }
        }
    }

    #[test]
    fn compressed_payload_uses_raw_deflate_at_supported_levels() {
        for level in [
            DeflateLevel::L0,
            DeflateLevel::L1,
            DeflateLevel::L6,
            DeflateLevel::L9,
        ] {
            let recorder = Recorder::default();
            let shared = recorder::shared(&recorder);
            let compression = Compression::Deflate { level };
            metrics::with_local_recorder(&recorder, || metrics::counter!("requests").increment(7));
            let envelope = encode_pending(&shared, compression).unwrap().unwrap();
            let mut payload = envelope.as_slice();
            let header = proto::EnvelopeHeader::decode_length_delimited(&mut payload).unwrap();
            assert_eq!(header.scheme(), CompressionScheme::Deflate);
            assert_eq!(header.payload_length, payload.len() as u64);
            let mut raw = Vec::new();
            DeflateDecoder::new(payload).read_to_end(&mut raw).unwrap();
            let mut remaining = raw.as_slice();
            let event = proto::Event::decode_length_delimited(&mut remaining).unwrap();
            assert!(remaining.is_empty());
            let Some(proto::event::Payload::Emission(emission)) = &event.payload else {
                panic!("expected emission")
            };
            assert!(matches!(
                &emission.operation,
                Some(proto::emission::Operation::Counter(
                    proto::CounterOperation {
                        operation: Some(proto::counter_operation::Operation::Absolute(7))
                    }
                ))
            ));
        }
    }

    #[test]
    fn compression_reduces_repeated_event_bytes() {
        let recorder = Recorder::default();
        let histogram = metrics::with_local_recorder(&recorder, || metrics::histogram!("requests"));
        let mut sizes = Vec::new();
        for compression in [Compression::None, Compression::default()] {
            for value in 0..128 {
                histogram.record(value as f64);
            }
            sizes.push(
                encode_pending(&recorder::shared(&recorder), compression)
                    .unwrap()
                    .unwrap()
                    .len(),
            );
        }
        assert!(sizes[1] < sizes[0]);
    }
    #[test]
    fn encoding_keeps_names_labels_and_metric_kinds_separate() {
        let recorder = Recorder::default();
        metrics::with_local_recorder(&recorder, || {
            metrics::counter!("first", "route" => "a").increment(1);
            metrics::counter!("first", "route" => "a").increment(2);
            metrics::counter!("first", "route" => "b").increment(4);
            metrics::counter!("second", "route" => "a").increment(5);
            metrics::gauge!("first", "route" => "a").set(6.0);
            metrics::histogram!("first", "route" => "a").record_many(7.0, 8);
        });
        let envelope = encode_pending(&recorder::shared(&recorder), Compression::default())
            .unwrap()
            .unwrap();
        let events = BlockingDecoder::new(envelope.as_slice())
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(events.len(), 5);
        let mut values = Vec::new();
        for event in events {
            let Event::Emission(emission) = event else {
                panic!("expected emission")
            };
            let key = &emission.registration.key;
            let label = key.labels().next().unwrap().value();
            let (kind, value, count) = match emission.operation {
                Operation::AbsoluteCounter(value) => ("counter", value as f64, 1),
                Operation::Gauge(GaugeValue::Absolute(value)) => ("gauge", value, 1),
                Operation::Histogram { value, count } => ("histogram", value, count),
                _ => panic!("unexpected aggregate"),
            };
            values.push((key.name().to_owned(), label.to_owned(), kind, value, count));
        }
        values.sort_by(|left, right| left.partial_cmp(right).unwrap());
        assert_eq!(
            values,
            vec![
                ("first".into(), "a".into(), "counter", 3.0, 1),
                ("first".into(), "a".into(), "gauge", 6.0, 1),
                ("first".into(), "a".into(), "histogram", 7.0, 8),
                ("first".into(), "b".into(), "counter", 4.0, 1),
                ("second".into(), "a".into(), "counter", 5.0, 1),
            ]
        );
    }

    #[test]
    fn concurrent_recording_and_encoding_delivers_each_observation_once() {
        let recorder = Recorder::default();
        let shared = recorder::shared(&recorder);
        let compression = Compression::default();
        let barrier = Arc::new(Barrier::new(3));
        let workers: Vec<_> = (0..2)
            .map(|worker| {
                let histogram = metrics::with_local_recorder(
                    &recorder,
                    || metrics::histogram!("samples", "worker" => worker.to_string()),
                );
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    for value in 0..64 {
                        histogram.record(value as f64);
                    }
                })
            })
            .collect();
        let mut received = [Vec::new(), Vec::new()];
        let mut collect = || {
            let Some(envelope) = encode_pending(&shared, compression).unwrap() else {
                return;
            };
            for event in BlockingDecoder::new(envelope.as_slice()) {
                let Event::Emission(emission) = event.unwrap() else {
                    panic!("expected emission")
                };
                let worker: usize = emission
                    .registration
                    .key
                    .labels()
                    .next()
                    .unwrap()
                    .value()
                    .parse()
                    .unwrap();
                let Operation::Histogram { value, count: 1 } = emission.operation else {
                    panic!("expected one observation")
                };
                received[worker].push(value.to_bits());
            }
        };
        barrier.wait();
        while workers.iter().any(|worker| !worker.is_finished()) {
            collect();
            thread::yield_now();
        }
        for worker in workers {
            worker.join().unwrap();
        }
        collect();
        let expected: Vec<_> = (0..64).map(|value| (value as f64).to_bits()).collect();
        for mut observations in received {
            observations.sort_unstable();
            assert_eq!(observations, expected);
        }
    }
}
