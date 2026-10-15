use std::error::Error;

use metrics::{Key, Unit};
use metrics_exporter_serialized::{BlockingDecoder, Builder, ReplayOutcome};
use metrics_util::debugging::DebuggingRecorder;

fn main() -> Result<(), Box<dyn Error>> {
    let (source, mut output) = Builder::new().build_blocking();
    let durations = [0.1, 0.2, 0.3];
    metrics::with_local_recorder(&source, || {
        metrics::describe_histogram!(
            "request_duration_seconds",
            Unit::Seconds,
            "Request duration"
        );
        metrics::counter!("requests", "route" => "/").increment(durations.len() as u64);
        let histogram = metrics::histogram!("request_duration_seconds", "route" => "/");
        for duration in durations {
            histogram.record(duration);
        }
    });

    let destination = DebuggingRecorder::new();
    let _guard = metrics::set_default_local_recorder(&destination);
    let bytes = output.export_pending()?.expect("recorded metrics");
    let mut decoder = BlockingDecoder::new(bytes.as_slice());
    while let ReplayOutcome::Replayed = decoder.replay_next_with(|key| {
        let (name, labels) = key.into_parts();
        Key::from_parts(format!("client_{}", name.as_str()), labels)
    })? {}

    for (key, unit, description, value) in destination.snapshotter().snapshot().into_vec() {
        println!(
            "{} ({unit:?}, {description:?}): {value:?}",
            key.key().name()
        );
    }
    Ok(())
}
