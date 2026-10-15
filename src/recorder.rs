use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, Condvar, Mutex},
};

use metrics::{
    Counter, CounterFn, Gauge, GaugeFn, GaugeValue, Histogram, HistogramFn, Key, KeyName, Metadata,
    Recorder as MetricRecorder, SharedString, Unit,
};

use metrics_util::MetricKind as Kind;

#[cfg(feature = "async-runtime")]
use crate::Output;
use crate::{BlockingOutput, Compression, Description, Emission, Operation, Registration, output};

/// Errors when starting an output session.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SessionError {
    /// The recorder was not built with [`crate::Builder::build_session_recorder`].
    #[error("recorder does not use output sessions")]
    NotSessionRecorder,
    /// The previous output or its live reader is still alive.
    #[error("an output session is already active")]
    AlreadyActive,
}

#[derive(Debug, Default, PartialEq)]
enum Collection {
    #[default]
    Continuous,
    Idle,
    Active,
}

struct Filter(Box<dyn Fn(&str) -> bool + Send + Sync>);

impl fmt::Debug for Filter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Filter").finish_non_exhaustive()
    }
}

#[derive(Debug, Default)]
pub struct State {
    collection: Collection,
    filter: Option<Filter>,
    pub descriptions_changed: bool,
    counters: HashMap<Key, Current<u64>>,
    gauges: HashMap<Key, Current<f64>>,
    histograms: HashMap<(Key, u64), Observations>,
    pub descriptions: HashMap<(Kind, KeyName), Description>,
}

impl State {
    pub fn allows(&self, name: &str) -> bool {
        self.filter.as_ref().is_none_or(|filter| (filter.0)(name))
    }

    fn clear_emissions(&mut self) {
        self.counters.clear();
        self.gauges.clear();
        self.histograms.clear();
    }

    pub fn end_session(&mut self) {
        self.collection = Collection::Idle;
        self.clear_emissions();
        self.descriptions_changed = false;
    }

    pub fn has_pending(&self) -> bool {
        self.collection != Collection::Idle
            && (self.descriptions_changed
                || self.counters.values().any(|current| current.dirty)
                || self.gauges.values().any(|current| current.dirty)
                || !self.histograms.is_empty())
    }

    pub fn take_emissions(&mut self) -> Vec<Emission> {
        let mut emissions = Vec::new();
        for current in self.counters.values_mut() {
            if let Some(emission) = current.take(Operation::AbsoluteCounter) {
                emissions.push(emission);
            }
        }
        for current in self.gauges.values_mut() {
            if let Some(emission) =
                current.take(|value| Operation::Gauge(GaugeValue::Absolute(value)))
            {
                emissions.push(emission);
            }
        }
        for ((_, bits), observations) in self.histograms.drain() {
            for count in observations.counts {
                emissions.push(Emission {
                    registration: Arc::clone(&observations.registration),
                    operation: Operation::Histogram {
                        value: f64::from_bits(bits),
                        count,
                    },
                });
            }
        }
        emissions
    }
}

#[derive(Debug)]
struct Current<T> {
    value: T,
    registration: Arc<Registration>,
    dirty: bool,
}

impl<T: Default + Copy> Current<T> {
    fn update(
        values: &mut HashMap<Key, Self>,
        registration: &Arc<Registration>,
        update: impl FnOnce(T) -> T,
    ) {
        let current = values
            .entry(registration.key.clone())
            .or_insert_with(|| Self {
                value: T::default(),
                registration: Arc::clone(registration),
                dirty: false,
            });
        current.value = update(current.value);
        current.registration = Arc::clone(registration);
        current.dirty = true;
    }

    fn take(&mut self, operation: impl FnOnce(T) -> Operation) -> Option<Emission> {
        if !self.dirty {
            return None;
        }
        self.dirty = false;
        Some(Emission {
            registration: Arc::clone(&self.registration),
            operation: operation(self.value),
        })
    }
}

#[derive(Debug)]
struct Observations {
    registration: Arc<Registration>,
    counts: Vec<usize>,
}

#[derive(Debug, Default)]
pub struct Shared {
    pub state: Mutex<State>,
    pub changed: Condvar,
    #[cfg(feature = "async-runtime")]
    pub notification: atomic_waker::AtomicWaker,
}

impl Shared {
    fn notify(&self) {
        self.changed.notify_one();
        #[cfg(feature = "async-runtime")]
        self.notification.wake();
    }
}

/// Collects current counter and gauge values and histogram observation counts.
///
/// Metrics with the same kind, name, and labels share state across registrations and clones.
/// Counter increments saturate at `u64::MAX`. Gauge operations are applied in recording order.
/// Histogram observations with identical floating-point bits are combined between exports.
/// Exported values carry the latest contributing update's metadata; operation history is not retained.
/// The number of distinct metrics and pending histogram values has no configured limit.
#[derive(Clone, Debug, Default)]
pub struct Recorder {
    shared: Arc<Shared>,
    compression: Compression,
}

impl Recorder {
    /// Starts a fresh asynchronous output session, preserving registered handles and descriptions.
    /// See [`Self::start_session_blocking`] for lifecycle and error behavior.
    #[cfg(feature = "async-runtime")]
    pub fn start_session(&self) -> Result<Output, SessionError> {
        self.start_session_blocking().map(output::new)
    }

    /// Starts a fresh output session with zero counter/gauge state and no pending observations.
    ///
    /// Only session recorders support this operation. A previous output (or its live reader)
    /// must be dropped first. Dropping this output stops collection and discards pending data;
    /// disconnected emissions are ignored. Descriptions and the admission filter survive.
    /// Bytes already returned to the caller are not recalled or retried.
    pub fn start_session_blocking(&self) -> Result<BlockingOutput, SessionError> {
        let mut state = self
            .shared
            .state
            .lock()
            .expect("metric buffer lock poisoned");
        match state.collection {
            Collection::Continuous => return Err(SessionError::NotSessionRecorder),
            Collection::Active => return Err(SessionError::AlreadyActive),
            Collection::Idle => {}
        }
        state.clear_emissions();
        state.descriptions_changed = !state.descriptions.is_empty();
        state.collection = Collection::Active;
        Ok(output::new_session(
            Arc::clone(&self.shared),
            self.compression,
        ))
    }

    /// Replaces name-based admission for operations through both new and cached handles.
    ///
    /// Rejected operations do not update aggregates. Captures also filter descriptions and
    /// pending emissions; already captured batches are unaffected. Previously admitted
    /// counter/gauge values remain until the session ends. Descriptions are retained even
    /// when excluded, so allowing a name later restores its description.
    ///
    /// The predicate runs under the recorder lock and must not record metrics or reenter the
    /// recorder. Use this method to change policy; `|_| true` restores admission of all names.
    pub fn set_filter(&self, filter: impl Fn(&str) -> bool + Send + Sync + 'static) {
        {
            let mut state = self
                .shared
                .state
                .lock()
                .expect("metric buffer lock poisoned");
            state.filter = Some(Filter(Box::new(filter)));
            state.descriptions_changed = true;
        }
        self.shared.notify();
    }

    fn describe(&self, kind: Kind, name: KeyName, unit: Option<Unit>, text: SharedString) {
        let description = Description {
            kind,
            name: name.clone(),
            unit,
            text,
        };
        {
            let mut state = self
                .shared
                .state
                .lock()
                .expect("metric buffer lock poisoned");
            state.descriptions.insert((kind, name), description);
            state.descriptions_changed = true;
        }
        self.shared.notify();
    }

    fn handle(&self, key: &Key, metadata: &Metadata<'_>) -> Arc<Handle> {
        Arc::new(Handle {
            shared: Arc::clone(&self.shared),
            registration: Arc::new(Registration {
                key: key.clone(),
                target: metadata.target().to_owned(),
                level: *metadata.level(),
                module_path: metadata.module_path().map(str::to_owned),
            }),
        })
    }
}

pub fn session_recorder(compression: Compression) -> Recorder {
    Recorder {
        shared: Arc::new(Shared {
            state: Mutex::new(State {
                collection: Collection::Idle,
                ..State::default()
            }),
            ..Shared::default()
        }),
        compression,
    }
}

pub fn shared(recorder: &Recorder) -> Arc<Shared> {
    Arc::clone(&recorder.shared)
}

impl MetricRecorder for Recorder {
    fn describe_counter(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.describe(Kind::Counter, key, unit, description);
    }

    fn describe_gauge(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.describe(Kind::Gauge, key, unit, description);
    }

    fn describe_histogram(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.describe(Kind::Histogram, key, unit, description);
    }

    fn register_counter(&self, key: &Key, metadata: &Metadata<'_>) -> Counter {
        Counter::from_arc(self.handle(key, metadata))
    }

    fn register_gauge(&self, key: &Key, metadata: &Metadata<'_>) -> Gauge {
        Gauge::from_arc(self.handle(key, metadata))
    }

    fn register_histogram(&self, key: &Key, metadata: &Metadata<'_>) -> Histogram {
        Histogram::from_arc(self.handle(key, metadata))
    }
}

#[derive(Debug)]
struct Handle {
    shared: Arc<Shared>,
    registration: Arc<Registration>,
}

impl Handle {
    fn update(&self, operation: Operation) {
        {
            let mut state = self
                .shared
                .state
                .lock()
                .expect("metric buffer lock poisoned");
            if state.collection == Collection::Idle || !state.allows(self.registration.key.name()) {
                return;
            }
            match operation {
                Operation::IncrementCounter(value) => {
                    Current::update(&mut state.counters, &self.registration, |current| {
                        current.saturating_add(value)
                    });
                }
                Operation::AbsoluteCounter(value) => {
                    Current::update(&mut state.counters, &self.registration, |current| {
                        current.max(value)
                    });
                }
                Operation::Gauge(value) => {
                    Current::update(&mut state.gauges, &self.registration, |current| {
                        value.update_value(current)
                    });
                }
                Operation::Histogram { value, count } => {
                    let observations = state
                        .histograms
                        .entry((self.registration.key.clone(), value.to_bits()))
                        .or_insert_with(|| Observations {
                            registration: Arc::clone(&self.registration),
                            counts: vec![0],
                        });
                    observations.registration = Arc::clone(&self.registration);
                    let last = observations.counts.last_mut().unwrap();
                    if let Some(total) = last.checked_add(count) {
                        *last = total;
                    } else {
                        observations.counts.push(count);
                    }
                }
            }
        }
        self.shared.notify();
    }
}

impl CounterFn for Handle {
    fn increment(&self, value: u64) {
        self.update(Operation::IncrementCounter(value));
    }

    fn absolute(&self, value: u64) {
        self.update(Operation::AbsoluteCounter(value));
    }
}

impl GaugeFn for Handle {
    fn increment(&self, value: f64) {
        self.update(Operation::Gauge(GaugeValue::Increment(value)));
    }

    fn decrement(&self, value: f64) {
        self.update(Operation::Gauge(GaugeValue::Decrement(value)));
    }

    fn set(&self, value: f64) {
        self.update(Operation::Gauge(GaugeValue::Absolute(value)));
    }
}

impl HistogramFn for Handle {
    fn record(&self, value: f64) {
        self.record_many(value, 1);
    }

    fn record_many(&self, value: f64, count: usize) {
        self.update(Operation::Histogram { value, count });
    }
}

#[cfg(test)]
mod tests {
    use crate::decoder::BlockingDecoder;
    use metrics::{Label, Level};

    use super::*;
    use crate::{Builder, Event, ReplayOutcome};
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    fn emissions(recorder: &Recorder) -> Vec<Emission> {
        recorder.shared.state.lock().unwrap().take_emissions()
    }

    #[test]
    fn sessions_reset_values_and_pending_observations_but_reuse_handles_and_descriptions() {
        let recorder = Builder::new().build_session_recorder();
        let (counter, gauge, histogram) = metrics::with_local_recorder(&recorder, || {
            metrics::describe_counter!("requests", Unit::Count, "Requests");
            (
                metrics::counter!("requests", "route" => "test"),
                metrics::gauge!("load"),
                metrics::histogram!("latency"),
            )
        });
        counter.increment(100);
        gauge.set(100.0);
        histogram.record(100.0);
        let mut first = recorder.start_session_blocking().unwrap();
        counter.increment(10);
        gauge.set(10.0);
        histogram.record(10.0);
        let bytes = first.export_pending().unwrap().unwrap();
        let destination = DebuggingRecorder::new();
        let mut decoder = BlockingDecoder::new(bytes.as_slice());
        while decoder.replay_next_with(&destination, |key| key).unwrap() == ReplayOutcome::Replayed
        {
        }
        let snapshot = destination.snapshotter().snapshot().into_vec();
        assert_eq!(
            snapshot
                .iter()
                .find(|(key, ..)| key.key().name() == "requests")
                .unwrap()
                .3,
            DebugValue::Counter(10)
        );
        counter.increment(50);
        gauge.set(50.0);
        histogram.record(50.0);
        drop(first);
        counter.increment(100);
        gauge.set(100.0);
        histogram.record(100.0);

        let mut second = recorder.start_session_blocking().unwrap();
        counter.increment(3);
        gauge.increment(2.0);
        histogram.record_many(0.5, 3);
        let bytes = second.export_pending().unwrap().unwrap();
        let destination = DebuggingRecorder::new();
        let mut decoder = BlockingDecoder::new(bytes.as_slice());
        while decoder.replay_next_with(&destination, |key| key).unwrap() == ReplayOutcome::Replayed
        {
        }
        let snapshot = destination.snapshotter().snapshot().into_vec();
        assert_eq!(snapshot.len(), 3);
        for (name, expected) in [
            ("requests", DebugValue::Counter(3)),
            ("load", DebugValue::Gauge(2.0.into())),
            ("latency", DebugValue::Histogram(vec![0.5.into(); 3])),
        ] {
            let (key, unit, description, value) = snapshot
                .iter()
                .find(|(key, ..)| key.key().name() == name)
                .unwrap();
            assert_eq!(*value, expected);
            if name == "requests" {
                assert_eq!(*unit, Some(Unit::Count));
                assert_eq!(description.as_deref(), Some("Requests"));
                assert_eq!(
                    key.key().labels().cloned().collect::<Vec<_>>(),
                    [Label::new("route", "test")]
                );
            }
        }
        assert!(second.export_pending().unwrap().is_none());
    }

    #[test]
    fn output_sessions_are_exclusive_across_clones_and_live_readers() {
        let recorder = Builder::new().build_session_recorder();
        let reader = recorder.start_session_blocking().unwrap().into_reader();
        assert_eq!(
            recorder.clone().start_session_blocking().unwrap_err(),
            SessionError::AlreadyActive
        );
        drop(reader);
        let _next = recorder.start_session_blocking().unwrap();
        assert_eq!(
            Recorder::default().start_session_blocking().unwrap_err(),
            SessionError::NotSessionRecorder
        );
    }

    #[test]
    fn replacing_admission_filters_cached_handle_operations_before_aggregation() {
        let (recorder, mut output) = Builder::new().build_blocking();
        let (counter, gauge, histogram) = metrics::with_local_recorder(&recorder, || {
            (
                metrics::counter!("requests"),
                metrics::gauge!("load"),
                metrics::histogram!("latency"),
            )
        });
        counter.increment(1);
        gauge.set(1.0);
        histogram.record(1.0);
        recorder.set_filter(|_| false);
        counter.absolute(100);
        counter.increment(100);
        gauge.set(100.0);
        gauge.increment(100.0);
        gauge.decrement(100.0);
        histogram.record_many(100.0, 5);
        recorder.set_filter(|_| true);
        counter.increment(2);
        gauge.increment(2.0);
        histogram.record_many(2.0, 2);

        let bytes = output.export_pending().unwrap().unwrap();
        let destination = DebuggingRecorder::new();
        let mut decoder = BlockingDecoder::new(bytes.as_slice());
        while decoder.replay_next_with(&destination, |key| key).unwrap() == ReplayOutcome::Replayed
        {
        }
        let snapshot = destination.snapshotter().snapshot().into_vec();
        for (name, expected) in [
            ("requests", DebugValue::Counter(3)),
            ("load", DebugValue::Gauge(3.0.into())),
        ] {
            assert_eq!(
                snapshot
                    .iter()
                    .find(|(key, ..)| key.key().name() == name)
                    .unwrap()
                    .3,
                expected
            );
        }
        let DebugValue::Histogram(values) = &snapshot
            .iter()
            .find(|(key, ..)| key.key().name() == "latency")
            .unwrap()
            .3
        else {
            panic!("expected histogram")
        };
        assert_eq!(values.len(), 3);
        assert_eq!(values.iter().filter(|value| **value == 1.0).count(), 1);
        assert_eq!(values.iter().filter(|value| **value == 2.0).count(), 2);
    }

    #[test]
    fn captures_filter_pending_metrics_but_retain_excluded_descriptions() {
        let recorder = Builder::new().build_session_recorder();
        recorder.describe_counter("secret".into(), Some(Unit::Count), "Private".into());
        let mut output = recorder.start_session_blocking().unwrap();
        metrics::with_local_recorder(&recorder, || metrics::counter!("secret").increment(7));
        recorder.set_filter(|name| name != "secret");
        assert!(output.export_pending().unwrap().is_none());
        drop(output);
        let mut output = recorder.start_session_blocking().unwrap();
        assert!(output.export_pending().unwrap().is_none());
        recorder.set_filter(|_| true);
        let bytes = output.export_pending().unwrap().unwrap();
        let events: Vec<_> = BlockingDecoder::new(bytes.as_slice())
            .collect::<Result<_, _>>()
            .unwrap();
        assert!(
            matches!(events.as_slice(), [Event::Description(description)]
            if description.name.as_str() == "secret" && description.unit == Some(Unit::Count) && description.text.as_ref() == "Private")
        );
        assert!(output.export_pending().unwrap().is_none());
    }

    #[test]
    fn counter_exports_current_value_across_exports_and_registrations() {
        let recorder = Recorder::default();
        let counter = metrics::with_local_recorder(&recorder, || metrics::counter!("requests"));
        counter.increment(2);
        counter.absolute(10);
        counter.absolute(8);
        counter.increment(3);
        assert!(matches!(
            emissions(&recorder).as_slice(),
            [Emission {
                operation: Operation::AbsoluteCounter(13),
                ..
            }]
        ));
        assert!(!recorder.shared.state.lock().unwrap().has_pending());

        let counter = metrics::with_local_recorder(&recorder, || metrics::counter!("requests"));
        counter.increment(5);
        assert!(matches!(
            emissions(&recorder).as_slice(),
            [Emission {
                operation: Operation::AbsoluteCounter(18),
                ..
            }]
        ));
    }

    #[test]
    fn counter_saturates_instead_of_exporting_a_decreasing_value() {
        let recorder = Recorder::default();
        let counter = metrics::with_local_recorder(&recorder, || metrics::counter!("requests"));
        counter.increment(u64::MAX);
        counter.increment(1);
        assert!(matches!(
            emissions(&recorder).as_slice(),
            [Emission {
                operation: Operation::AbsoluteCounter(u64::MAX),
                ..
            }]
        ));
    }

    #[test]
    fn gauge_applies_updates_in_recording_order() {
        let recorder = Recorder::default();
        let gauge = metrics::with_local_recorder(&recorder, || metrics::gauge!("load"));
        gauge.set(1e16);
        gauge.increment(1.0);
        gauge.decrement(1e16);
        assert!(matches!(
            emissions(&recorder).as_slice(),
            [Emission {
                operation: Operation::Gauge(GaugeValue::Absolute(0.0)),
                ..
            }]
        ));
    }

    #[test]
    fn gauge_retains_value_across_exports_and_registrations() {
        let recorder = Recorder::default();
        let gauge = metrics::with_local_recorder(&recorder, || metrics::gauge!("load"));
        gauge.set(10.0);
        gauge.set(20.0);
        gauge.increment(3.0);
        assert!(matches!(
            emissions(&recorder).as_slice(),
            [Emission {
                operation: Operation::Gauge(GaugeValue::Absolute(23.0)),
                ..
            }]
        ));

        let gauge = metrics::with_local_recorder(&recorder, || metrics::gauge!("load"));
        gauge.decrement(2.0);
        assert!(matches!(
            emissions(&recorder).as_slice(),
            [Emission {
                operation: Operation::Gauge(GaugeValue::Absolute(21.0)),
                ..
            }]
        ));
    }

    #[test]
    fn gauge_set_preserves_float_bits() {
        let recorder = Recorder::default();
        let gauge = metrics::with_local_recorder(&recorder, || metrics::gauge!("load"));
        for value in [-0.0, f64::INFINITY, f64::from_bits(0x7ff8000000000001)] {
            gauge.set(value);
            assert!(matches!(emissions(&recorder).as_slice(),
                [Emission { operation: Operation::Gauge(GaugeValue::Absolute(received)), .. }]
                if received.to_bits() == value.to_bits()));
        }
    }

    #[test]
    fn histogram_combines_matching_values_only_until_the_next_export() {
        let recorder = Recorder::default();
        let histogram = metrics::with_local_recorder(&recorder, || metrics::histogram!("samples"));
        histogram.record(5.5);
        histogram.record(2.5);
        histogram.record_many(5.5, 9);
        let mut captured = emissions(&recorder);
        captured.sort_by_key(|emission| match emission.operation {
            Operation::Histogram { value, .. } => value.to_bits(),
            _ => unreachable!(),
        });
        assert!(matches!(
            captured.as_slice(),
            [
                Emission {
                    operation: Operation::Histogram {
                        value: 2.5,
                        count: 1
                    },
                    ..
                },
                Emission {
                    operation: Operation::Histogram {
                        value: 5.5,
                        count: 10
                    },
                    ..
                },
            ]
        ));
        assert!(!recorder.shared.state.lock().unwrap().has_pending());
        histogram.record(5.5);
        assert!(matches!(
            emissions(&recorder).as_slice(),
            [Emission {
                operation: Operation::Histogram {
                    value: 5.5,
                    count: 1
                },
                ..
            }]
        ));
    }

    #[test]
    fn histogram_preserves_counts_that_cannot_fit_in_one_observation() {
        let recorder = Recorder::default();
        let histogram = metrics::with_local_recorder(&recorder, || metrics::histogram!("samples"));
        histogram.record_many(5.5, usize::MAX);
        histogram.record_many(5.5, 2);
        assert!(matches!(
            emissions(&recorder).as_slice(),
            [
                Emission {
                    operation: Operation::Histogram {
                        value: 5.5,
                        count: usize::MAX
                    },
                    ..
                },
                Emission {
                    operation: Operation::Histogram {
                        value: 5.5,
                        count: 2
                    },
                    ..
                },
            ]
        ));
    }

    #[test]
    fn histogram_accepts_zero_multiplicity() {
        let recorder = Recorder::default();
        let histogram = metrics::with_local_recorder(&recorder, || metrics::histogram!("samples"));
        histogram.record_many(5.5, 0);
        assert!(matches!(
            emissions(&recorder).as_slice(),
            [Emission {
                operation: Operation::Histogram {
                    value: 5.5,
                    count: 0
                },
                ..
            }]
        ));
    }

    #[test]
    fn histogram_combines_nan_values_by_bits() {
        let recorder = Recorder::default();
        let histogram = metrics::with_local_recorder(&recorder, || metrics::histogram!("samples"));
        let first = 0x7ff8000000000001;
        let second = 0x7ff8000000000002;
        histogram.record(f64::from_bits(first));
        histogram.record(f64::from_bits(second));
        histogram.record(f64::from_bits(first));
        let mut observations: Vec<_> = emissions(&recorder)
            .into_iter()
            .map(|emission| {
                let Operation::Histogram { value, count } = emission.operation else {
                    unreachable!()
                };
                (value.to_bits(), count)
            })
            .collect();
        observations.sort_unstable();
        assert_eq!(observations, vec![(first, 2), (second, 1)]);
    }

    #[test]
    fn shared_gauge_uses_the_latest_updates_metadata() {
        let recorder = Recorder::default();
        let key = Key::from_name("load");
        let first =
            recorder.register_gauge(&key, &Metadata::new("first", Level::DEBUG, Some("app")));
        let second = recorder.register_gauge(&key, &Metadata::new("second", Level::WARN, None));
        first.set(1.0);
        second.increment(2.0);
        let captured = emissions(&recorder);
        assert!(matches!(
            captured.as_slice(),
            [Emission {
                operation: Operation::Gauge(GaugeValue::Absolute(3.0)),
                ..
            }]
        ));
        let registration = &captured[0].registration;
        assert_eq!(registration.key, key);
        assert_eq!(registration.target, "second");
        assert_eq!(registration.level, Level::WARN);
        assert_eq!(registration.module_path, None);
    }

    #[test]
    fn descriptions_are_replaced_only_for_the_same_name_and_kind() {
        let recorder = Recorder::default();
        metrics::with_local_recorder(&recorder, || {
            metrics::describe_counter!("shared", Unit::Count, "old");
            metrics::describe_gauge!("shared", "gauge");
            metrics::describe_histogram!("shared", Unit::Seconds, "histogram");
            metrics::describe_counter!("shared", "new");
        });
        let state = recorder.shared.state.lock().unwrap();
        let descriptions = &state.descriptions;
        assert_eq!(descriptions.len(), 3);
        for (kind, unit, text) in [
            (Kind::Counter, None, "new"),
            (Kind::Gauge, None, "gauge"),
            (Kind::Histogram, Some(Unit::Seconds), "histogram"),
        ] {
            let description = &descriptions[&(kind, "shared".into())];
            assert_eq!(description.unit, unit);
            assert_eq!(description.text.as_ref(), text);
        }
    }

    #[test]
    fn output_and_registered_handles_outlive_recorder() {
        let (recorder, output) = crate::Builder::new().build_blocking();
        let counter = metrics::with_local_recorder(&recorder, || metrics::counter!("requests"));
        drop(recorder);
        counter.increment(7);
        let event = BlockingDecoder::new(output.into_reader())
            .next()
            .unwrap()
            .unwrap();
        assert!(matches!(
            event,
            Event::Emission(Emission {
                operation: Operation::AbsoluteCounter(7),
                ..
            })
        ));
    }
}
