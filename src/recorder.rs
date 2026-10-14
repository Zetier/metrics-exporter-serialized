use std::{
    collections::HashMap,
    sync::{Arc, Condvar, Mutex},
};

use metrics::{
    Counter, CounterFn, Gauge, GaugeFn, GaugeValue, Histogram, HistogramFn, Key, KeyName, Metadata,
    Recorder as MetricRecorder, SharedString, Unit,
};

use metrics_util::MetricKind as Kind;

use crate::{Description, Emission, Operation, Registration};

#[derive(Debug, Default)]
pub struct State {
    pub descriptions_changed: bool,
    counters: HashMap<Key, Current<u64>>,
    gauges: HashMap<Key, Current<f64>>,
    histograms: HashMap<(Key, u64), Observations>,
    pub descriptions: HashMap<(Kind, KeyName), Description>,
}

impl State {
    pub fn has_pending(&self) -> bool {
        self.descriptions_changed
            || self.counters.values().any(|current| current.dirty)
            || self.gauges.values().any(|current| current.dirty)
            || !self.histograms.is_empty()
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
}

impl Recorder {
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
    use metrics::Level;

    use super::*;
    use crate::Event;

    fn emissions(recorder: &Recorder) -> Vec<Emission> {
        recorder.shared.state.lock().unwrap().take_emissions()
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
