use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    match trevrpc_bench_peer::run(std::env::args().skip(1)).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            trevrpc_bench_peer::emit_error(&error);
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}
