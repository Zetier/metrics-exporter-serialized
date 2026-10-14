use metrics_exporter_serialized::{BlockingDecoder, Builder, ReplayOutcome};
use metrics_util::debugging::{DebugValue, DebuggingRecorder};

#[test]
fn rejected_install_leaves_the_original_global_recorder_working() {
    let output = Builder::new().install_blocking().unwrap();
    assert!(Builder::new().install_blocking().is_err());
    metrics::counter!("requests").increment(7);

    let destination = DebuggingRecorder::new();
    let mut decoder = BlockingDecoder::new(output.into_reader());
    assert_eq!(
        decoder.replay_next_with(&destination, |key| key).unwrap(),
        ReplayOutcome::Replayed
    );
    let values = destination.snapshotter().snapshot().into_vec();
    assert_eq!(values.len(), 1);
    assert_eq!(values[0].3, DebugValue::Counter(7));
}
