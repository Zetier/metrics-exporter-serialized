use std::sync::Arc;

use metrics::{
    GaugeValue, Key, KeyName, Label, Level, Metadata, Recorder as MetricRecorder, SharedString,
    Unit,
};
use metrics_util::MetricKind;

use crate::{
    ErrorKind,
    proto::{self, counter_operation, gauge_operation},
};

impl From<Level> for proto::MetricLevel {
    fn from(level: Level) -> Self {
        match level {
            Level::TRACE => Self::Trace,
            Level::DEBUG => Self::Debug,
            Level::INFO => Self::Info,
            Level::WARN => Self::Warn,
            Level::ERROR => Self::Error,
        }
    }
}

impl TryFrom<proto::MetricLevel> for Level {
    type Error = ErrorKind;

    fn try_from(level: proto::MetricLevel) -> Result<Self, Self::Error> {
        Ok(match level {
            proto::MetricLevel::Unspecified => return Err(ErrorKind::InvalidEvent),
            proto::MetricLevel::Trace => Self::TRACE,
            proto::MetricLevel::Debug => Self::DEBUG,
            proto::MetricLevel::Info => Self::INFO,
            proto::MetricLevel::Warn => Self::WARN,
            proto::MetricLevel::Error => Self::ERROR,
        })
    }
}

#[derive(Clone, Debug)]
pub struct Description {
    pub kind: MetricKind,
    pub name: KeyName,
    pub unit: Option<Unit>,
    pub text: SharedString,
}

#[derive(Clone, Debug)]
pub struct Registration {
    pub key: Key,
    pub target: String,
    pub level: Level,
    pub module_path: Option<String>,
}

#[derive(Clone, Debug)]
pub enum Operation {
    IncrementCounter(u64),
    AbsoluteCounter(u64),
    Gauge(GaugeValue),
    Histogram { value: f64, count: usize },
}

#[derive(Clone, Debug)]
pub struct Emission {
    pub registration: Arc<Registration>,
    pub operation: Operation,
}

#[derive(Clone, Debug)]
pub enum Event {
    Description(Description),
    Emission(Emission),
}

impl Event {
    pub fn replay_with(self, recorder: &dyn MetricRecorder, map_key: impl FnOnce(Key) -> Key) {
        match self {
            Self::Description(description) => {
                let name = map_key(Key::from_name(description.name)).into_parts().0;
                match description.kind {
                    MetricKind::Counter => {
                        recorder.describe_counter(name, description.unit, description.text)
                    }
                    MetricKind::Gauge => {
                        recorder.describe_gauge(name, description.unit, description.text)
                    }
                    MetricKind::Histogram => {
                        recorder.describe_histogram(name, description.unit, description.text)
                    }
                }
            }
            Self::Emission(emission) => {
                let registration = Arc::try_unwrap(emission.registration)
                    .unwrap_or_else(|registration| (*registration).clone());
                let key = map_key(registration.key);
                let metadata = Metadata::new(
                    &registration.target,
                    registration.level,
                    registration.module_path.as_deref(),
                );
                match emission.operation {
                    Operation::IncrementCounter(value) => {
                        recorder.register_counter(&key, &metadata).increment(value)
                    }
                    Operation::AbsoluteCounter(value) => {
                        recorder.register_counter(&key, &metadata).absolute(value)
                    }
                    Operation::Gauge(GaugeValue::Increment(value)) => {
                        recorder.register_gauge(&key, &metadata).increment(value)
                    }
                    Operation::Gauge(GaugeValue::Decrement(value)) => {
                        recorder.register_gauge(&key, &metadata).decrement(value)
                    }
                    Operation::Gauge(GaugeValue::Absolute(value)) => {
                        recorder.register_gauge(&key, &metadata).set(value)
                    }
                    Operation::Histogram { value, count } => recorder
                        .register_histogram(&key, &metadata)
                        .record_many(value, count),
                }
            }
        }
    }
}

impl TryFrom<proto::Event> for Event {
    type Error = ErrorKind;

    fn try_from(event: proto::Event) -> Result<Self, Self::Error> {
        match event.payload.ok_or(ErrorKind::InvalidEvent)? {
            proto::event::Payload::Description(description) => {
                let kind = match proto::MetricKind::try_from(description.kind) {
                    Ok(proto::MetricKind::Counter) => MetricKind::Counter,
                    Ok(proto::MetricKind::Gauge) => MetricKind::Gauge,
                    Ok(proto::MetricKind::Histogram) => MetricKind::Histogram,
                    _ => return Err(ErrorKind::InvalidEvent),
                };
                let unit = description
                    .unit
                    .as_deref()
                    .map(|unit| Unit::from_string(unit).ok_or(ErrorKind::InvalidEvent))
                    .transpose()?;
                Ok(Event::Description(Description {
                    kind,
                    name: description.name.into(),
                    unit,
                    text: description.description.into(),
                }))
            }
            proto::event::Payload::Emission(emission) => {
                let registration = emission.registration.ok_or(ErrorKind::InvalidEvent)?;
                let metadata = registration.metadata.ok_or(ErrorKind::InvalidEvent)?;
                let level = Level::try_from(
                    proto::MetricLevel::try_from(metadata.level)
                        .map_err(|_| ErrorKind::InvalidEvent)?,
                )?;
                let registration = Arc::new(Registration {
                    key: Key::from_parts(
                        registration.name,
                        registration
                            .labels
                            .into_iter()
                            .map(|label| Label::new(label.key, label.value))
                            .collect::<Vec<_>>(),
                    ),
                    target: metadata.target,
                    level,
                    module_path: metadata.module_path,
                });
                let operation = match emission.operation.ok_or(ErrorKind::InvalidEvent)? {
                    proto::emission::Operation::Counter(counter) => {
                        match counter.operation.ok_or(ErrorKind::InvalidEvent)? {
                            counter_operation::Operation::Increment(value) => {
                                Operation::IncrementCounter(value)
                            }
                            counter_operation::Operation::Absolute(value) => {
                                Operation::AbsoluteCounter(value)
                            }
                        }
                    }
                    proto::emission::Operation::Gauge(gauge) => {
                        match gauge.operation.ok_or(ErrorKind::InvalidEvent)? {
                            gauge_operation::Operation::Increment(value) => {
                                Operation::Gauge(GaugeValue::Increment(value))
                            }
                            gauge_operation::Operation::Decrement(value) => {
                                Operation::Gauge(GaugeValue::Decrement(value))
                            }
                            gauge_operation::Operation::Set(value) => {
                                Operation::Gauge(GaugeValue::Absolute(value))
                            }
                        }
                    }
                    proto::emission::Operation::Histogram(histogram) => Operation::Histogram {
                        value: histogram.value,
                        count: usize::try_from(histogram.count)
                            .map_err(|_| ErrorKind::HistogramCountOutOfRange)?,
                    },
                };
                Ok(Event::Emission(Emission {
                    registration,
                    operation,
                }))
            }
        }
    }
}

impl From<&Event> for proto::Event {
    fn from(event: &Event) -> Self {
        let payload = match event {
            Event::Description(description) => {
                proto::event::Payload::Description(proto::Description {
                    kind: match description.kind {
                        MetricKind::Counter => proto::MetricKind::Counter,
                        MetricKind::Gauge => proto::MetricKind::Gauge,
                        MetricKind::Histogram => proto::MetricKind::Histogram,
                    } as i32,
                    name: description.name.as_str().to_owned(),
                    unit: description.unit.map(|unit| unit.as_str().to_owned()),
                    description: description.text.to_string(),
                })
            }
            Event::Emission(emission) => {
                let registration = &emission.registration;
                let operation = match emission.operation {
                    Operation::IncrementCounter(value) => {
                        proto::emission::Operation::Counter(proto::CounterOperation {
                            operation: Some(counter_operation::Operation::Increment(value)),
                        })
                    }
                    Operation::AbsoluteCounter(value) => {
                        proto::emission::Operation::Counter(proto::CounterOperation {
                            operation: Some(counter_operation::Operation::Absolute(value)),
                        })
                    }
                    Operation::Gauge(GaugeValue::Increment(value)) => {
                        proto::emission::Operation::Gauge(proto::GaugeOperation {
                            operation: Some(gauge_operation::Operation::Increment(value)),
                        })
                    }
                    Operation::Gauge(GaugeValue::Decrement(value)) => {
                        proto::emission::Operation::Gauge(proto::GaugeOperation {
                            operation: Some(gauge_operation::Operation::Decrement(value)),
                        })
                    }
                    Operation::Gauge(GaugeValue::Absolute(value)) => {
                        proto::emission::Operation::Gauge(proto::GaugeOperation {
                            operation: Some(gauge_operation::Operation::Set(value)),
                        })
                    }
                    Operation::Histogram { value, count } => {
                        proto::emission::Operation::Histogram(proto::HistogramObservation {
                            value,
                            count: count as u64,
                        })
                    }
                };
                proto::event::Payload::Emission(proto::Emission {
                    registration: Some(proto::Registration {
                        name: registration.key.name().to_owned(),
                        labels: registration
                            .key
                            .labels()
                            .map(|label| proto::Label {
                                key: label.key().to_owned(),
                                value: label.value().to_owned(),
                            })
                            .collect(),
                        metadata: Some(proto::Metadata {
                            target: registration.target.clone(),
                            level: proto::MetricLevel::from(registration.level) as i32,
                            module_path: registration.module_path.clone(),
                        }),
                    }),
                    operation: Some(operation),
                })
            }
        };
        proto::Event {
            payload: Some(payload),
        }
    }
}

#[cfg(test)]
mod tests {
    use prost::Message;

    fn decode(bytes: &[u8]) -> Result<Event, ErrorKind> {
        Event::try_from(proto::Event::decode(bytes).unwrap())
    }

    use crate::recorder;
    use metrics::{Counter, Gauge, Histogram, HistogramFn};
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use proptest::prelude::*;
    use std::sync::Mutex;

    use super::*;

    fn emission(operation: Operation) -> Event {
        Event::Emission(Emission {
            registration: Arc::new(Registration {
                key: Key::from_name("requests"),
                target: "client".into(),
                level: Level::INFO,
                module_path: None,
            }),
            operation,
        })
    }

    fn proto_emission() -> proto::Emission {
        let Some(proto::event::Payload::Emission(emission)) =
            proto::Event::from(&emission(Operation::IncrementCounter(1))).payload
        else {
            unreachable!()
        };
        emission
    }

    fn convert_payload(payload: proto::event::Payload) -> Result<Event, ErrorKind> {
        Event::try_from(proto::Event {
            payload: Some(payload),
        })
    }

    #[test]
    fn descriptions_roundtrip_with_kind_name_unit_and_text() {
        for (kind, unit) in [
            (MetricKind::Counter, Some(Unit::Count)),
            (MetricKind::Gauge, None),
            (MetricKind::Histogram, Some(Unit::Seconds)),
        ] {
            let original = Event::Description(Description {
                kind,
                name: "測定".into(),
                unit,
                text: "measurement".into(),
            });
            let Event::Description(received) =
                decode(&proto::Event::from(&original).encode_to_vec()).unwrap()
            else {
                panic!("expected description")
            };
            assert_eq!(received.kind, kind);
            assert_eq!(received.name.as_str(), "測定");
            assert_eq!(received.unit, unit);
            assert_eq!(received.text.as_ref(), "measurement");
        }
    }

    #[test]
    fn registration_roundtrip_preserves_labels_and_source_metadata() {
        for (level, module_path) in [
            (Level::TRACE, Some("app::client")),
            (Level::DEBUG, None),
            (Level::INFO, Some("")),
            (Level::WARN, None),
            (Level::ERROR, None),
        ] {
            let registration = Registration {
                key: Key::from_parts(
                    "測定",
                    vec![Label::new("peer", "a"), Label::new("route", "/")],
                ),
                target: "client".into(),
                level,
                module_path: module_path.map(str::to_owned),
            };
            let original = Event::Emission(Emission {
                registration: Arc::new(registration.clone()),
                operation: Operation::IncrementCounter(1),
            });
            let Event::Emission(received) =
                decode(&proto::Event::from(&original).encode_to_vec()).unwrap()
            else {
                panic!("expected emission")
            };
            assert_eq!(received.registration.key, registration.key);
            assert_eq!(received.registration.target, registration.target);
            assert_eq!(received.registration.level, level);
            assert_eq!(received.registration.module_path.as_deref(), module_path);
        }
    }

    #[test]
    fn counter_operations_roundtrip_without_changing_variant_or_value() {
        for operation in [
            Operation::IncrementCounter(0),
            Operation::IncrementCounter(u64::MAX),
            Operation::AbsoluteCounter(17),
        ] {
            let Event::Emission(decoded) =
                decode(&proto::Event::from(&emission(operation.clone())).encode_to_vec()).unwrap()
            else {
                panic!("expected emission")
            };
            let (expected, received) = match (operation, decoded.operation) {
                (Operation::IncrementCounter(expected), Operation::IncrementCounter(received))
                | (Operation::AbsoluteCounter(expected), Operation::AbsoluteCounter(received)) => {
                    (expected, received)
                }
                _ => panic!("counter operation changed"),
            };
            assert_eq!(received, expected);
        }
    }

    #[test]
    fn gauge_operations_roundtrip_with_exact_float_bits() {
        for bits in [
            0,
            (-0.0f64).to_bits(),
            1.25f64.to_bits(),
            f64::INFINITY.to_bits(),
            0x7ff8_0000_0000_0001,
        ] {
            let value = f64::from_bits(bits);
            for operation in [
                Operation::Gauge(GaugeValue::Increment(value)),
                Operation::Gauge(GaugeValue::Decrement(value)),
                Operation::Gauge(GaugeValue::Absolute(value)),
            ] {
                let Event::Emission(decoded) =
                    decode(&proto::Event::from(&emission(operation.clone())).encode_to_vec())
                        .unwrap()
                else {
                    panic!("expected emission")
                };
                let actual = match (operation, decoded.operation) {
                    (
                        Operation::Gauge(GaugeValue::Increment(_)),
                        Operation::Gauge(GaugeValue::Increment(value)),
                    )
                    | (
                        Operation::Gauge(GaugeValue::Decrement(_)),
                        Operation::Gauge(GaugeValue::Decrement(value)),
                    )
                    | (
                        Operation::Gauge(GaugeValue::Absolute(_)),
                        Operation::Gauge(GaugeValue::Absolute(value)),
                    ) => value,
                    _ => panic!("gauge operation changed"),
                };
                assert_eq!(actual.to_bits(), bits);
            }
        }
    }

    #[test]
    fn histogram_count_roundtrips_zero_and_platform_maximum() {
        for count in [0, usize::MAX] {
            let original = emission(Operation::Histogram { value: 1.5, count });
            let Event::Emission(decoded) =
                decode(&proto::Event::from(&original).encode_to_vec()).unwrap()
            else {
                panic!("expected emission")
            };
            assert!(
                matches!(decoded.operation, Operation::Histogram { value: 1.5, count: received }
                if received == count)
            );
        }
    }

    #[test]
    fn histogram_special_float_bits_roundtrip_except_negative_zero() {
        for bits in [
            (-0.0f64).to_bits(),
            f64::INFINITY.to_bits(),
            f64::NEG_INFINITY.to_bits(),
            0x7ff8_0000_0000_0001,
        ] {
            let original = emission(Operation::Histogram {
                value: f64::from_bits(bits),
                count: 1,
            });
            let Event::Emission(decoded) =
                decode(&proto::Event::from(&original).encode_to_vec()).unwrap()
            else {
                panic!("expected emission")
            };
            let Operation::Histogram { value, .. } = decoded.operation else {
                panic!("expected histogram")
            };
            let expected = if bits == (-0.0f64).to_bits() { 0 } else { bits };
            assert_eq!(value.to_bits(), expected);
        }
    }

    proptest! {
        #[test]
        fn histogram_float_bits_roundtrip_except_negative_zero(bits in any::<u64>()) {
            let original = emission(Operation::Histogram { value: f64::from_bits(bits), count: 1 });
            let Event::Emission(decoded) = decode(&proto::Event::from(&original).encode_to_vec()).unwrap()
            else { panic!("expected emission") };
            let Operation::Histogram { value, .. } = decoded.operation
            else { panic!("expected histogram") };
            let expected = if bits == (-0.0f64).to_bits() { 0 } else { bits };
            prop_assert_eq!(value.to_bits(), expected);
        }
    }

    #[test]
    fn rejects_an_event_without_a_payload() {
        assert!(matches!(
            Event::try_from(proto::Event::default()),
            Err(ErrorKind::InvalidEvent)
        ));
    }

    #[test]
    fn rejects_an_emission_without_registration() {
        let invalid = proto::Emission {
            registration: None,
            ..proto_emission()
        };
        assert!(matches!(
            convert_payload(proto::event::Payload::Emission(invalid)),
            Err(ErrorKind::InvalidEvent)
        ));
    }

    #[test]
    fn rejects_registration_without_metadata() {
        let mut invalid = proto_emission();
        invalid.registration.as_mut().unwrap().metadata = None;
        assert!(matches!(
            convert_payload(proto::event::Payload::Emission(invalid)),
            Err(ErrorKind::InvalidEvent)
        ));
    }

    #[test]
    fn rejects_unspecified_and_unknown_metric_levels() {
        for level in [-1, 0, 6] {
            let mut invalid = proto_emission();
            invalid
                .registration
                .as_mut()
                .unwrap()
                .metadata
                .as_mut()
                .unwrap()
                .level = level;
            assert!(matches!(
                convert_payload(proto::event::Payload::Emission(invalid)),
                Err(ErrorKind::InvalidEvent)
            ));
        }
    }

    #[test]
    fn rejects_an_emission_without_an_operation() {
        let invalid = proto::Emission {
            operation: None,
            ..proto_emission()
        };
        assert!(matches!(
            convert_payload(proto::event::Payload::Emission(invalid)),
            Err(ErrorKind::InvalidEvent)
        ));
    }

    #[test]
    fn rejects_a_counter_without_an_operation() {
        let invalid = proto::Emission {
            operation: Some(proto::emission::Operation::Counter(
                proto::CounterOperation { operation: None },
            )),
            ..proto_emission()
        };
        assert!(matches!(
            convert_payload(proto::event::Payload::Emission(invalid)),
            Err(ErrorKind::InvalidEvent)
        ));
    }

    #[test]
    fn rejects_a_gauge_without_an_operation() {
        let invalid = proto::Emission {
            operation: Some(proto::emission::Operation::Gauge(proto::GaugeOperation {
                operation: None,
            })),
            ..proto_emission()
        };
        assert!(matches!(
            convert_payload(proto::event::Payload::Emission(invalid)),
            Err(ErrorKind::InvalidEvent)
        ));
    }

    #[test]
    fn rejects_unspecified_and_unknown_metric_kinds() {
        for kind in [-1, 0, 4] {
            let invalid = proto::Description {
                kind,
                ..Default::default()
            };
            assert!(matches!(
                convert_payload(proto::event::Payload::Description(invalid)),
                Err(ErrorKind::InvalidEvent)
            ));
        }
    }

    #[test]
    fn rejects_unknown_units() {
        let invalid = proto::Description {
            kind: proto::MetricKind::Counter as i32,
            unit: Some("unknown".into()),
            ..Default::default()
        };
        assert!(matches!(
            convert_payload(proto::event::Payload::Description(invalid)),
            Err(ErrorKind::InvalidEvent)
        ));
    }

    fn value(recorder: &DebuggingRecorder) -> DebugValue {
        let values = recorder.snapshotter().snapshot().into_vec();
        assert_eq!(values.len(), 1);
        values.into_iter().next().unwrap().3
    }

    #[test]
    fn replay_preserves_monotonic_absolute_counter_semantics() {
        let recorder = DebuggingRecorder::new();
        for operation in [
            Operation::IncrementCounter(3),
            Operation::AbsoluteCounter(20),
            Operation::AbsoluteCounter(5),
            Operation::IncrementCounter(2),
        ] {
            emission(operation).replay_with(&recorder, |key| key);
        }
        assert_eq!(value(&recorder), DebugValue::Counter(22));
    }

    #[test]
    fn replay_applies_each_gauge_operation() {
        let recorder = DebuggingRecorder::new();
        for operation in [
            Operation::Gauge(GaugeValue::Absolute(7.0)),
            Operation::Gauge(GaugeValue::Increment(2.0)),
            Operation::Gauge(GaugeValue::Decrement(3.0)),
        ] {
            emission(operation).replay_with(&recorder, |key| key);
        }
        assert_eq!(value(&recorder), DebugValue::Gauge(6.0.into()));
    }

    #[test]
    fn replay_maps_description_names_and_emission_keys_consistently() {
        let recorder = DebuggingRecorder::new();
        let description = Event::Description(Description {
            kind: MetricKind::Counter,
            name: "requests".into(),
            unit: Some(Unit::Count),
            text: "requests received".into(),
        });
        let mut event = emission(Operation::IncrementCounter(2));
        let Event::Emission(emission) = &mut event else {
            unreachable!()
        };
        Arc::make_mut(&mut emission.registration).key =
            Key::from_parts("requests", vec![Label::new("route", "/")]);
        for event in [description, event] {
            let peer = Label::new("peer", "a");
            event.replay_with(&recorder, |key| {
                let (name, mut labels) = key.into_parts();
                labels.push(peer);
                Key::from_parts(format!("client_{}", name.as_str()), labels)
            });
        }
        let values = recorder.snapshotter().snapshot().into_vec();
        assert_eq!(values.len(), 1);
        let (key, unit, text, count) = &values[0];
        assert_eq!(
            key.key(),
            &Key::from_parts(
                "client_requests",
                vec![Label::new("route", "/"), Label::new("peer", "a")]
            )
        );
        assert_eq!(*unit, Some(Unit::Count));
        assert_eq!(text.as_deref(), Some("requests received"));
        assert_eq!(*count, DebugValue::Counter(2));
    }

    #[test]
    fn replay_passes_registration_context_to_the_receiving_recorder() {
        let recorder = crate::Recorder::default();
        let key = Key::from_parts("requests", vec![Label::new("route", "/")]);
        let original = Event::Emission(Emission {
            registration: Arc::new(Registration {
                key: key.clone(),
                target: "client".into(),
                level: Level::WARN,
                module_path: Some("app::client".into()),
            }),
            operation: Operation::IncrementCounter(1),
        });
        original.replay_with(&recorder, |key| key);
        let shared = recorder::shared(&recorder);
        let received = shared.state.lock().unwrap().take_emissions().pop().unwrap();
        assert_eq!(received.registration.key, key);
        assert_eq!(received.registration.target, "client");
        assert_eq!(received.registration.level, Level::WARN);
        assert_eq!(
            received.registration.module_path.as_deref(),
            Some("app::client")
        );
    }

    #[test]
    fn replay_preserves_description_kind_and_fields() {
        for kind in [
            MetricKind::Counter,
            MetricKind::Gauge,
            MetricKind::Histogram,
        ] {
            let recorder = crate::Recorder::default();
            Event::Description(Description {
                kind,
                name: "measurement".into(),
                unit: Some(Unit::Count),
                text: "description".into(),
            })
            .replay_with(&recorder, |key| key);
            let shared = recorder::shared(&recorder);
            let state = shared.state.lock().unwrap();
            assert_eq!(state.descriptions.len(), 1);
            let received = &state.descriptions[&(kind, "measurement".into())];
            assert_eq!(received.kind, kind);
            assert_eq!(received.name.as_str(), "measurement");
            assert_eq!(received.unit, Some(Unit::Count));
            assert_eq!(received.text.as_ref(), "description");
        }
    }

    #[test]
    fn replay_passes_histogram_multiplicity_through_without_expansion() {
        #[derive(Clone, Default)]
        struct HistogramSink(Arc<Mutex<Vec<(u64, usize)>>>);

        impl HistogramFn for HistogramSink {
            fn record(&self, value: f64) {
                self.record_many(value, 1);
            }
            fn record_many(&self, value: f64, count: usize) {
                self.0.lock().unwrap().push((value.to_bits(), count));
            }
        }

        impl MetricRecorder for HistogramSink {
            fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
            fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
            fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
            fn register_counter(&self, _: &Key, _: &Metadata<'_>) -> Counter {
                panic!("expected histogram")
            }
            fn register_gauge(&self, _: &Key, _: &Metadata<'_>) -> Gauge {
                panic!("expected histogram")
            }
            fn register_histogram(&self, _: &Key, _: &Metadata<'_>) -> Histogram {
                Histogram::from_arc(Arc::new(self.clone()))
            }
        }

        let recorder = HistogramSink::default();
        for count in [0, usize::MAX] {
            emission(Operation::Histogram { value: 1.5, count }).replay_with(&recorder, |key| key);
        }
        assert_eq!(
            *recorder.0.lock().unwrap(),
            vec![(1.5f64.to_bits(), 0), (1.5f64.to_bits(), usize::MAX)]
        );
    }
}
