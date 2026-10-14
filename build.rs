fn main() {
    println!("cargo:rerun-if-changed=proto/metrics.proto");
    prost_build::compile_protos(&["proto/metrics.proto"], &["proto/"]).unwrap();
}
