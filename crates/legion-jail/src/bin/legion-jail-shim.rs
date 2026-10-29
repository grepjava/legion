//! The supervisor on its own, for this crate's tests; Legion runs it as
//! `legion jail-shim`.

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    std::process::exit(legion_jail::shim::main(&args));
}
