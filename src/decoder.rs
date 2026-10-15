use std::io::{self, Read};

use prost::{
    Message,
    bytes::{Buf, BytesMut},
};

#[cfg(feature = "async-runtime")]
use futures_io::AsyncRead;
#[cfg(feature = "async-runtime")]
use futures_lite::{future, io::AsyncReadExt};

use metrics::Key;

use crate::{
    Error, ErrorKind, Event,
    compression::Decompressor,
    proto::{self, CompressionScheme},
};

const BUFFER_SIZE: usize = 8192;

/// The outcome of replaying the next description or emission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplayOutcome {
    /// One description or emission was replayed.
    Replayed,
    /// No further events will be replayed.
    EndOfStream,
}

/// Replays descriptions and emissions from a serialized metric stream.
///
/// Truncated input returns an error. Errors end replay and do not undo earlier events.
pub struct BlockingDecoder<R> {
    reader: R,
    bytes: BytesMut,
    payload: Option<PayloadDecoder>,
    remaining_payload: u64,
    done: bool,
}

impl<R> BlockingDecoder<R> {
    /// Creates a decoder over a synchronous reader, including a byte slice.
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            bytes: BytesMut::new(),
            payload: None,
            remaining_payload: 0,
            done: false,
        }
    }
}

impl<R: Read> BlockingDecoder<R> {
    /// Replays one description or emission into the current recorder.
    pub fn replay_next(&mut self) -> Result<ReplayOutcome, Error> {
        self.replay_next_with(|key| key)
    }

    /// Replays one description or emission into the current recorder with mapped keys.
    ///
    /// Description names are mapped as keys without labels. Map names consistently across
    /// descriptions and emissions so they remain associated.
    pub fn replay_next_with(
        &mut self,
        map_key: impl FnOnce(Key) -> Key,
    ) -> Result<ReplayOutcome, Error> {
        let event = self.decode_next()?;
        if let Some(event) = event {
            event.replay_with(map_key);
            Ok(ReplayOutcome::Replayed)
        } else {
            Ok(ReplayOutcome::EndOfStream)
        }
    }
}

impl<R> BlockingDecoder<R> {
    fn buffered_event(&mut self) -> Result<Option<Event>, ErrorKind> {
        loop {
            if let Some(payload) = &mut self.payload {
                let available = self
                    .bytes
                    .len()
                    .min(self.remaining_payload.min(BUFFER_SIZE as u64) as usize);
                let (consumed, event) = payload.decode(
                    &self.bytes[..available],
                    self.remaining_payload == available as u64,
                )?;
                self.bytes.advance(consumed);
                self.remaining_payload -= consumed as u64;
                if payload.decompression_finished && self.remaining_payload != 0 {
                    return Err(ErrorKind::InvalidEnvelope);
                }
                if event.is_some() {
                    return Ok(event);
                }
                if !payload.decompression_finished {
                    if self.bytes.is_empty() {
                        return Ok(None);
                    }
                    continue;
                }
                self.payload = None;
            }
            let Some(header) = decode_frame::<proto::EnvelopeHeader>(&mut self.bytes)? else {
                return Ok(None);
            };
            let scheme = CompressionScheme::try_from(header.scheme)
                .map_err(|_| ErrorKind::UnsupportedCompression(header.scheme))?;
            self.remaining_payload = header.payload_length;
            self.payload = Some(PayloadDecoder::new(scheme)?);
        }
    }

    fn finish(&self) -> Result<(), ErrorKind> {
        if self.payload.is_some() || !self.bytes.is_empty() {
            return Err(ErrorKind::InvalidEnvelope);
        }
        Ok(())
    }
}

impl<R: Read> BlockingDecoder<R> {
    fn decode_next(&mut self) -> Result<Option<Event>, ErrorKind> {
        if self.done {
            return Ok(None);
        }
        let result = self.read_event();
        if !matches!(result, Ok(Some(_))) {
            self.done = true;
        }
        result
    }

    fn read_event(&mut self) -> Result<Option<Event>, ErrorKind> {
        loop {
            if let Some(event) = self.buffered_event()? {
                return Ok(Some(event));
            }
            let mut input = [0; BUFFER_SIZE];
            let size = match self.reader.read(&mut input) {
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                result => result?,
            };
            if size == 0 {
                self.finish()?;
                return Ok(None);
            }
            self.bytes.extend_from_slice(&input[..size]);
        }
    }
}

/// The asynchronous counterpart to [`BlockingDecoder`], accepting an asynchronous byte stream.
#[cfg(feature = "async-runtime")]
pub struct Decoder<R> {
    inner: BlockingDecoder<R>,
}

#[cfg(feature = "async-runtime")]
impl<R> Decoder<R> {
    /// Creates a decoder over a reader implementing [`futures_io::AsyncRead`].
    pub fn new(reader: R) -> Self {
        Self {
            inner: BlockingDecoder::new(reader),
        }
    }
}

#[cfg(feature = "async-runtime")]
impl<R: AsyncRead + Unpin> Decoder<R> {
    /// Replays one description or emission into the current recorder.
    ///
    /// Canceling this call does not skip an event. Follows [`BlockingDecoder`]'s error behavior.
    pub async fn replay_next(&mut self) -> Result<ReplayOutcome, Error> {
        self.replay_next_with(|key| key).await
    }

    /// Replays one description or emission into the current recorder with mapped keys.
    ///
    /// Description names are mapped as keys without labels. Map names consistently across
    /// descriptions and emissions so they remain associated. Canceling this call does not skip
    /// an event. Follows [`BlockingDecoder`]'s error behavior.
    pub async fn replay_next_with(
        &mut self,
        map_key: impl FnOnce(Key) -> Key,
    ) -> Result<ReplayOutcome, Error> {
        let event = self.decode_next().await?;
        if let Some(event) = event {
            event.replay_with(map_key);
            Ok(ReplayOutcome::Replayed)
        } else {
            Ok(ReplayOutcome::EndOfStream)
        }
    }

    async fn decode_next(&mut self) -> Result<Option<Event>, ErrorKind> {
        if self.inner.done {
            return Ok(None);
        }
        let result = self.read_event().await;
        if !matches!(result, Ok(Some(_))) {
            self.inner.done = true;
        }
        result
    }

    async fn read_event(&mut self) -> Result<Option<Event>, ErrorKind> {
        loop {
            if let Some(event) = self.inner.buffered_event()? {
                return Ok(Some(event));
            }
            let mut input = [0; BUFFER_SIZE];
            let size = match self.inner.reader.read(&mut input).await {
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                result => result?,
            };
            if size == 0 {
                self.inner.finish()?;
                return Ok(None);
            }
            self.inner.bytes.extend_from_slice(&input[..size]);
            future::yield_now().await;
        }
    }
}

fn decode_frame<M: Message + Default>(bytes: &mut BytesMut) -> Result<Option<M>, ErrorKind> {
    let mut input = bytes.as_ref();
    let length = match prost::decode_length_delimiter(&mut input) {
        Ok(length) => length,
        Err(_) if bytes.len() < 10 => return Ok(None),
        Err(_) => return Err(ErrorKind::InvalidEnvelope),
    };
    if input.len() < length {
        return Ok(None);
    }
    let consumed = bytes.len() - input.len() + length;
    M::decode_length_delimited(bytes.split_to(consumed))
        .map(Some)
        .map_err(|_| ErrorKind::InvalidEnvelope)
}

struct PayloadDecoder {
    decompressor: Decompressor,
    uncompressed_bytes: BytesMut,
    decompression_finished: bool,
}

impl PayloadDecoder {
    fn new(scheme: CompressionScheme) -> Result<Self, ErrorKind> {
        Ok(Self {
            decompressor: Decompressor::try_from(scheme)?,
            uncompressed_bytes: BytesMut::new(),
            decompression_finished: false,
        })
    }

    fn decode(
        &mut self,
        compressed_input: &[u8],
        end_of_input: bool,
    ) -> Result<(usize, Option<Event>), ErrorKind> {
        let mut consumed = 0;
        loop {
            if let Some(event) = decode_frame::<proto::Event>(&mut self.uncompressed_bytes)? {
                return Event::try_from(event).map(|event| (consumed, Some(event)));
            }
            if self.decompression_finished {
                if !self.uncompressed_bytes.is_empty() {
                    return Err(ErrorKind::InvalidEnvelope);
                }
                return Ok((consumed, None));
            }
            let mut output = [0; BUFFER_SIZE];
            let input = &compressed_input[consumed..];
            let progress = self
                .decompressor
                .decompress(input, &mut output, end_of_input)?;
            consumed += progress.consumed;
            self.uncompressed_bytes
                .extend_from_slice(&output[..progress.produced]);
            self.decompression_finished = progress.finished;
            if !self.decompression_finished && progress.consumed == 0 && progress.produced == 0 {
                if end_of_input || !input.is_empty() {
                    return Err(ErrorKind::InvalidEnvelope);
                }
                return Ok((consumed, None));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{output, recorder};
    impl<R: Read> Iterator for BlockingDecoder<R> {
        type Item = Result<Event, ErrorKind>;

        fn next(&mut self) -> Option<Self::Item> {
            self.decode_next().transpose()
        }
    }

    use metrics::Level;
    use std::{cell::Cell, rc::Rc, sync::Arc};

    use proptest::prelude::*;
    use prost::Message;

    use super::*;
    use crate::{Compression, DeflateLevel, Emission, Operation, Recorder, Registration, proto};
    use flate2::{Compression as DeflateCompression, write::DeflateEncoder};
    use std::io::Write;

    #[cfg(feature = "async-runtime")]
    async fn poll_with_recorder<F: std::future::Future>(
        recorder: &impl metrics::Recorder,
        future: F,
    ) -> F::Output {
        let mut future = std::pin::pin!(future);
        future::poll_fn(|cx| metrics::with_local_recorder(recorder, || future.as_mut().poll(cx)))
            .await
    }

    fn encode(events: Vec<proto::Event>, compression: Compression) -> Result<Vec<u8>, ErrorKind> {
        let raw: Vec<_> = events
            .into_iter()
            .flat_map(|event| event.encode_length_delimited_to_vec())
            .collect();
        encode_payload(&raw, compression)
    }

    fn encode_payload(raw: &[u8], compression: Compression) -> Result<Vec<u8>, ErrorKind> {
        let (scheme, data) = match compression {
            Compression::None => (CompressionScheme::Uncompressed, raw.to_vec()),
            Compression::Deflate { level } => {
                let mut encoder = DeflateEncoder::new(Vec::new(), DeflateCompression::from(level));
                encoder.write_all(raw)?;
                (CompressionScheme::Deflate, encoder.finish()?)
            }
        };
        Ok(envelope(
            proto::EnvelopeHeader {
                scheme: scheme as i32,
                payload_length: data.len() as u64,
            },
            data,
        ))
    }

    fn envelope(header: proto::EnvelopeHeader, payload: Vec<u8>) -> Vec<u8> {
        let mut bytes = header.encode_length_delimited_to_vec();
        bytes.extend(payload);
        bytes
    }

    fn framed(bytes: &[u8]) -> Vec<u8> {
        let mut frame = Vec::new();
        prost::encode_length_delimiter(bytes.len(), &mut frame).unwrap();
        frame.extend_from_slice(bytes);
        frame
    }

    fn decode_all(bytes: &[u8]) -> Result<Vec<Event>, ErrorKind> {
        BlockingDecoder::new(bytes).collect()
    }

    struct Fragmented<'a> {
        bytes: &'a [u8],
        chunk: usize,
        consumed: Rc<Cell<usize>>,
    }

    impl Read for Fragmented<'_> {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            let size = buffer.len().min(self.chunk).min(self.bytes.len());
            buffer[..size].copy_from_slice(&self.bytes[..size]);
            self.bytes = &self.bytes[size..];
            self.consumed.set(self.consumed.get() + size);
            Ok(size)
        }
    }

    fn counter_event(value: u64) -> proto::Event {
        proto::Event::from(&Event::Emission(Emission {
            registration: Arc::new(Registration {
                key: Key::from_name("requests"),
                target: "test".into(),
                level: Level::INFO,
                module_path: None,
            }),
            operation: Operation::AbsoluteCounter(value),
        }))
    }

    fn counter_values(events: impl Iterator<Item = Result<Event, ErrorKind>>) -> Vec<u64> {
        events
            .map(|event| match event.unwrap() {
                Event::Emission(emission) => match emission.operation {
                    Operation::AbsoluteCounter(value) => value,
                    _ => panic!("expected counter value"),
                },
                _ => panic!("expected emission"),
            })
            .collect()
    }

    #[cfg(feature = "async-runtime")]
    #[tokio::test]
    async fn mapped_replay_can_run_in_a_send_task() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};

        let bytes = encode(vec![counter_event(7)], Compression::default()).unwrap();
        let task = tokio::spawn(async move {
            let destination = DebuggingRecorder::new();
            let mut decoder = Decoder::new(bytes.as_slice());
            assert_eq!(
                poll_with_recorder(&destination, decoder.replay_next_with(|key| key))
                    .await
                    .unwrap(),
                ReplayOutcome::Replayed
            );
            assert_eq!(
                poll_with_recorder(&destination, decoder.replay_next())
                    .await
                    .unwrap(),
                ReplayOutcome::EndOfStream
            );
            assert_eq!(
                destination.snapshotter().snapshot().into_vec()[0].3,
                DebugValue::Counter(7)
            );
        });
        task.await.unwrap();
    }

    #[test]
    fn replays_events_before_receiving_the_complete_payload() {
        for compression in [Compression::None, Compression::default()] {
            let envelope = encode((0..1000).map(counter_event).collect(), compression).unwrap();
            let consumed = Rc::new(Cell::new(0));
            let reader = Fragmented {
                bytes: &envelope,
                chunk: 7,
                consumed: Rc::clone(&consumed),
            };
            let mut decoder = BlockingDecoder::new(reader);
            assert!(matches!(
                decoder.decode_next().unwrap(),
                Some(Event::Emission(_))
            ));
            assert!(consumed.get() < envelope.len());
            assert_eq!(counter_values(decoder), (1..1000).collect::<Vec<_>>());
        }
    }

    #[cfg(feature = "async-runtime")]
    #[test]
    fn async_replay_starts_before_the_complete_payload_is_available() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};

        for compression in [Compression::None, Compression::default()] {
            let bytes = encode((1..=1000).map(counter_event).collect(), compression).unwrap();
            let mut decoder = Decoder::new(&bytes[..bytes.len() / 2]);
            let destination = DebuggingRecorder::new();
            assert_eq!(
                future::block_on(poll_with_recorder(&destination, decoder.replay_next())).unwrap(),
                ReplayOutcome::Replayed
            );
            assert_eq!(
                destination.snapshotter().snapshot().into_vec()[0].3,
                DebugValue::Counter(1)
            );
        }
    }

    #[test]
    fn crosses_mixed_and_empty_envelopes() {
        let source = Recorder::default();
        let mut bytes = Vec::new();
        for compression in [
            Compression::None,
            Compression::default(),
            Compression::Deflate {
                level: DeflateLevel::L0,
            },
        ] {
            let shared = recorder::shared(&source);
            bytes.extend(encode(Vec::new(), compression).unwrap());
            metrics::with_local_recorder(&source, || {
                let counter = metrics::counter!("requests");
                counter.increment(1);
                counter.increment(2);
            });
            bytes.extend(
                output::encode_pending(&shared, compression)
                    .unwrap()
                    .unwrap(),
            );
        }
        for compression in [Compression::None, Compression::default()] {
            bytes.extend(encode(Vec::new(), compression).unwrap());
        }
        let reader = Fragmented {
            bytes: &bytes,
            chunk: 1,
            consumed: Rc::default(),
        };
        let mut decoder = BlockingDecoder::new(reader);
        assert_eq!(counter_values(&mut decoder), vec![3, 6, 9]);
        assert!(decoder.decode_next().unwrap().is_none());
    }

    #[test]
    fn reads_a_single_event_across_multiple_working_buffers() {
        for compression in [
            Compression::None,
            Compression::default(),
            Compression::Deflate {
                level: DeflateLevel::L0,
            },
        ] {
            let source = Recorder::default();
            let text = "x".repeat(BUFFER_SIZE + 1);
            metrics::with_local_recorder(&source, || {
                metrics::describe_counter!("requests", text.clone())
            });
            let envelope = output::encode_pending(&recorder::shared(&source), compression)
                .unwrap()
                .unwrap();
            let mut decoder = BlockingDecoder::new(envelope.as_slice());
            let Some(Event::Description(description)) = decoder.decode_next().unwrap() else {
                panic!("expected description")
            };
            assert_eq!(description.text.as_ref(), text);
            assert!(decoder.decode_next().unwrap().is_none());
        }
    }

    #[test]
    fn preserves_event_order_when_descriptions_and_emissions_are_interleaved() {
        let description = proto::Event {
            payload: Some(proto::event::Payload::Description(proto::Description {
                kind: proto::MetricKind::Counter as i32,
                name: "requests".into(),
                description: "requests".into(),
                unit: None,
            })),
        };
        let emission = counter_event(7);
        for compression in [Compression::None, Compression::default()] {
            let bytes = encode(
                vec![emission.clone(), description.clone(), emission.clone()],
                compression,
            )
            .unwrap();
            let events = BlockingDecoder::new(bytes.as_slice())
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert!(matches!(
                events.as_slice(),
                [
                    Event::Emission(_),
                    Event::Description(_),
                    Event::Emission(_)
                ]
            ));
        }
    }

    #[test]
    fn reports_a_later_invalid_event_and_then_terminates() {
        let valid = counter_event(7);
        for compression in [Compression::None, Compression::default()] {
            let bytes = encode(
                vec![valid.clone(), proto::Event { payload: None }],
                compression,
            )
            .unwrap();
            let mut decoder = BlockingDecoder::new(bytes.as_slice());
            assert!(matches!(
                decoder.decode_next().unwrap(),
                Some(Event::Emission(_))
            ));
            assert!(matches!(
                decoder.decode_next(),
                Err(ErrorKind::InvalidEvent)
            ));
            assert!(decoder.decode_next().unwrap().is_none());
        }
    }

    #[test]
    fn rejects_an_overflowing_header_length() {
        let bytes = [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff];
        assert!(matches!(
            BlockingDecoder::new(bytes.as_slice()).decode_next(),
            Err(ErrorKind::InvalidEnvelope)
        ));
    }

    #[test]
    fn rejects_truncated_varints() {
        for bytes in [vec![0x80], framed(&[0x80])] {
            assert!(matches!(
                BlockingDecoder::new(bytes.as_slice()).decode_next(),
                Err(ErrorKind::InvalidEnvelope)
            ));
        }
    }

    #[test]
    fn skips_unknown_fields_in_headers_and_events() {
        let valid = counter_event(7);
        let unknown = [
            32, 1, 41, 0, 0, 0, 0, 0, 0, 0, 0, 50, 2, 0, 0, 61, 0, 0, 0, 0,
        ];
        let mut event = valid.encode_to_vec();
        event.extend_from_slice(&unknown);
        for compression in [Compression::None, Compression::default()] {
            let bytes = encode_payload(&framed(&event), compression).unwrap();
            let mut payload = bytes.as_slice();
            let header = proto::EnvelopeHeader::decode_length_delimited(&mut payload).unwrap();
            let mut header = header.encode_to_vec();
            header.extend_from_slice(&unknown);
            let mut bytes = framed(&header);
            bytes.extend_from_slice(payload);
            let reader = Fragmented {
                bytes: &bytes,
                chunk: 1,
                consumed: Rc::default(),
            };
            assert_eq!(counter_values(BlockingDecoder::new(reader)), vec![7]);
        }
    }

    #[test]
    fn rejects_unknown_fields_that_cross_header_boundaries() {
        for incomplete in [vec![32, 0x80], vec![41, 0], vec![50, 2, 0]] {
            let mut header = proto::EnvelopeHeader {
                scheme: CompressionScheme::Uncompressed as i32,
                ..Default::default()
            }
            .encode_to_vec();
            header.extend(incomplete);
            let mut bytes = framed(&header);
            bytes.extend(encode(Vec::new(), Compression::None).unwrap());
            let reader = Fragmented {
                bytes: &bytes,
                chunk: 1,
                consumed: Rc::default(),
            };
            assert!(matches!(
                BlockingDecoder::new(reader).collect::<Result<Vec<_>, _>>(),
                Err(ErrorKind::InvalidEnvelope)
            ));
        }
    }

    #[cfg(feature = "async-runtime")]
    #[tokio::test]
    async fn async_replay_selects_the_recorder_after_input_is_ready() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};
        use std::{future::Future, pin::pin, task::Poll};
        use tokio::io::{self, AsyncWriteExt};
        use tokio_util::compat::TokioAsyncReadCompatExt;

        let (header, payload) = compressed();
        let bytes = envelope(header, payload);
        let (mut writer, reader) = io::duplex(bytes.len());
        let mut decoder = Decoder::new(reader.compat());
        let waiting = DebuggingRecorder::new();
        let destination = DebuggingRecorder::new();
        let mut replay = pin!(decoder.replay_next_with(|key| key));
        future::poll_fn(|cx| {
            assert!(
                metrics::with_local_recorder(&waiting, || replay.as_mut().poll(cx)).is_pending()
            );
            Poll::Ready(())
        })
        .await;
        writer.write_all(&bytes).await.unwrap();
        assert_eq!(
            poll_with_recorder(&destination, replay).await.unwrap(),
            ReplayOutcome::Replayed
        );
        assert!(waiting.snapshotter().snapshot().into_vec().is_empty());
        let values = destination.snapshotter().snapshot().into_vec();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0].3, DebugValue::Counter(7));
    }

    #[cfg(feature = "async-runtime")]
    #[tokio::test]
    async fn cancellation_preserves_partially_read_envelopes() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};
        use std::{
            future::{self, Future},
            task::Poll,
        };
        use tokio::io::{self, AsyncWriteExt};
        use tokio_util::compat::TokioAsyncReadCompatExt;

        struct PendingReader {
            stream: tokio_util::compat::Compat<io::DuplexStream>,
            blocked: Rc<Cell<bool>>,
        }

        impl AsyncRead for PendingReader {
            fn poll_read(
                mut self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
                buffer: &mut [u8],
            ) -> Poll<io::Result<usize>> {
                let result = std::pin::Pin::new(&mut self.stream).poll_read(cx, buffer);
                self.blocked.set(result.is_pending());
                result
            }
        }

        async fn cancel_pending(decoder: &mut Decoder<PendingReader>) {
            let blocked = Rc::clone(&decoder.inner.reader.blocked);
            blocked.set(false);
            let mut next = Box::pin(decoder.replay_next());
            future::poll_fn(|cx| {
                assert!(next.as_mut().poll(cx).is_pending());
                if blocked.get() {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;
        }

        for compression in [
            Compression::None,
            Compression::Deflate {
                level: DeflateLevel::L0,
            },
        ] {
            let source = Recorder::default();
            let name = "x".repeat(4096);
            metrics::with_local_recorder(&source, || metrics::counter!(name.clone()).increment(7));
            let bytes = output::encode_pending(&recorder::shared(&source), compression)
                .unwrap()
                .unwrap();
            let (mut writer, reader) = io::duplex(bytes.len());
            let reader = PendingReader {
                stream: reader.compat(),
                blocked: Rc::default(),
            };
            let mut decoder = Decoder::new(reader);
            let destination = DebuggingRecorder::new();
            writer.write_all(&bytes[..1]).await.unwrap();
            cancel_pending(&mut decoder).await;
            let halfway = bytes.len() / 2;
            writer.write_all(&bytes[1..halfway]).await.unwrap();
            cancel_pending(&mut decoder).await;
            writer.write_all(&bytes[halfway..]).await.unwrap();
            writer.shutdown().await.unwrap();
            assert!(destination.snapshotter().snapshot().into_vec().is_empty());
            assert_eq!(
                poll_with_recorder(&destination, decoder.replay_next_with(|key| key))
                    .await
                    .unwrap(),
                ReplayOutcome::Replayed
            );
            assert_eq!(
                poll_with_recorder(&destination, decoder.replay_next_with(|key| key))
                    .await
                    .unwrap(),
                ReplayOutcome::EndOfStream
            );
            let values = destination.snapshotter().snapshot().into_vec();
            assert_eq!(values.len(), 1);
            assert_eq!(values[0].0.key().name(), name);
            assert_eq!(values[0].3, DebugValue::Counter(7));
        }
    }

    #[test]
    fn rejects_every_truncated_envelope_prefix() {
        for compression in [Compression::None, Compression::default()] {
            let source = Recorder::default();
            metrics::with_local_recorder(&source, || metrics::histogram!("samples").record(1.5));
            let bytes = output::encode_pending(&recorder::shared(&source), compression)
                .unwrap()
                .unwrap();
            for end in 1..bytes.len() {
                assert!(
                    decode_all(&bytes[..end]).is_err(),
                    "accepted truncation at {end}"
                );
            }
        }
    }

    fn compressed() -> (proto::EnvelopeHeader, Vec<u8>) {
        let bytes = encode(vec![counter_event(7)], Compression::default()).unwrap();
        let mut payload = bytes.as_slice();
        let header = proto::EnvelopeHeader::decode_length_delimited(&mut payload).unwrap();
        (header, payload.to_vec())
    }

    #[test]
    fn rejects_invalid_deflate_data() {
        let (header, mut data) = compressed();
        data[0] = 0x07;
        assert!(matches!(
            decode_all(&envelope(header, data)),
            Err(ErrorKind::InvalidEnvelope)
        ));
    }

    #[test]
    fn rejects_trailing_deflate_data() {
        for trailing in [1, BUFFER_SIZE * 2] {
            let (mut header, mut data) = compressed();
            data.extend(vec![0; trailing]);
            header.payload_length += trailing as u64;
            assert!(matches!(
                decode_all(&envelope(header, data)),
                Err(ErrorKind::InvalidEnvelope)
            ));
        }
    }

    #[test]
    fn rejects_truncated_deflate_data() {
        let (mut header, mut data) = compressed();
        data.pop().unwrap();
        header.payload_length -= 1;
        assert!(matches!(
            decode_all(&envelope(header, data)),
            Err(ErrorKind::InvalidEnvelope)
        ));
    }

    #[test]
    fn rejects_event_frames_that_cross_payload_boundaries() {
        for compression in [Compression::None, Compression::default()] {
            for raw in [vec![0x80], vec![2, 0x0a]] {
                let mut bytes = encode_payload(&raw, compression).unwrap();
                bytes.extend(encode(Vec::new(), Compression::None).unwrap());
                assert!(matches!(
                    decode_all(&bytes),
                    Err(ErrorKind::InvalidEnvelope)
                ));
            }
        }
    }

    #[test]
    fn rejects_unspecified_and_unknown_compression_schemes() {
        for scheme in [-1, 0, 3, 256] {
            let (mut header, data) = compressed();
            header.scheme = scheme;
            assert!(matches!(decode_all(&envelope(header, data)),
                Err(ErrorKind::UnsupportedCompression(value)) if value == scheme));
        }
    }

    #[test]
    fn empty_stream_ends_cleanly() {
        assert_eq!(
            BlockingDecoder::new(&[][..]).replay_next().unwrap(),
            ReplayOutcome::EndOfStream
        );
    }

    #[test]
    fn histogram_observations_roundtrip_through_facade_recording_and_replay() {
        use crate::Builder;
        use metrics::Unit;
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};

        for compression in [Compression::None, Compression::default()] {
            let (recorder, output) = Builder::new().compression(compression).build_blocking();
            metrics::with_local_recorder(&recorder, || {
                metrics::describe_histogram!("duration", Unit::Seconds, "request duration");
                let histogram = metrics::histogram!("duration", "route" => "/");
                histogram.record_many(0.125, 3);
            });
            let destination = DebuggingRecorder::new();
            let mut decoder = BlockingDecoder::new(output.into_reader());
            metrics::with_local_recorder(&destination, || {
                assert_eq!(decoder.replay_next().unwrap(), ReplayOutcome::Replayed);
                assert!(destination.snapshotter().snapshot().into_vec().is_empty());
                assert_eq!(decoder.replay_next().unwrap(), ReplayOutcome::Replayed);
            });
            let values = destination.snapshotter().snapshot().into_vec();
            assert_eq!(values.len(), 1);
            let (key, unit, text, value) = &values[0];
            assert_eq!(key.key().name(), "duration");
            assert_eq!(key.key().labels().next().unwrap().value(), "/");
            assert_eq!(*unit, Some(Unit::Seconds));
            assert_eq!(text.as_deref(), Some("request duration"));
            assert_eq!(*value, DebugValue::Histogram(vec![0.125.into(); 3]));
        }
    }

    proptest! {
        #[test]
        fn arbitrary_fragments_do_not_panic(bytes in prop::collection::vec(any::<u8>(), 0..4096), chunk in 1..32usize) {
            let reader = Fragmented { bytes: &bytes, chunk, consumed: Rc::default() };
            let _ = BlockingDecoder::new(reader).collect::<Result<Vec<_>, _>>();
        }
    }
}
