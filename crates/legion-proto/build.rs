// protox compiles the protos in Rust, so building needs no `protoc`.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protos = [
        "proto/legion/v1/api.proto",
        "proto/legion/v1/dispatcher.proto",
        "proto/legion/v1/raft.proto",
        "proto/legion/v1/session.proto",
    ];
    for proto in protos {
        println!("cargo:rerun-if-changed={proto}");
    }
    let fds = protox::compile(protos, ["proto"])?;
    tonic_prost_build::configure().compile_fds(fds)?;
    Ok(())
}
