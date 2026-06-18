#![forbid(unsafe_code)]

fn main() {
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();

    if let Err(error) = protoc_gen_trevrpc_rust::run_plugin(stdin.lock(), stdout.lock()) {
        eprintln!("protoc-gen-trevrpc-rust: {error}");
        std::process::exit(1);
    }
}
