use std::error::Error;

use metrics_exporter_serialized::{Builder, Decoder, ReplayOutcome};
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use tokio::io;
use tokio_util::compat::{FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let (source, output) = Builder::new().build();
    let mut output = output.into_reader().compat();
    metrics::with_local_recorder(&source, || metrics::counter!("requests").increment(100));

    let (mut writer, reader) = io::duplex(1024);
    let destination = DebuggingRecorder::new();
    let snapshotter = destination.snapshotter();
    metrics::set_global_recorder(destination)?;
    let receive = async {
        let mut decoder = Decoder::new(reader.compat());
        assert_eq!(decoder.replay_next().await?, ReplayOutcome::Replayed);
        Ok::<_, Box<dyn Error>>(())
    };
    tokio::select! {
        result = io::copy(&mut output, &mut writer) => {
            result?;
            return Err("export ended before reception".into());
        }
        result = receive => result?,
    }
    let values = snapshotter.snapshot().into_vec();
    assert_eq!(values.len(), 1);
    assert_eq!(values[0].3, DebugValue::Counter(100));
    println!("requests: 100, copied through a Tokio byte stream");
    Ok(())
}
