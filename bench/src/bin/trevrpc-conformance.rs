use std::path::Path;
use std::process::ExitCode;

use trevrpc_bench::BoxError;
use trevrpc_bench::conformance::runner::{self, PeerOverride};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("trevrpc-conformance: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), BoxError> {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("validate") => {
            let suite_path = args.next().ok_or_else(usage)?;
            ensure_finished(args)?;
            let loaded = runner::validate(Path::new(&suite_path))?;
            println!(
                "suite {} is valid ({} cases)",
                loaded.suite.suite_id,
                loaded.case_count()
            );
        }
        Some("run") => {
            let suite_path = args.next().ok_or_else(usage)?;
            if args.next().as_deref() != Some("--out") {
                return Err(usage().into());
            }
            let output = args.next().ok_or_else(usage)?;
            let mut overrides = Vec::<PeerOverride>::new();
            while let Some(argument) = args.next() {
                if argument != "--peer" {
                    return Err(format!("unexpected argument {argument:?}").into());
                }
                overrides.push(runner::parse_override(&args.next().ok_or_else(usage)?)?);
            }
            let loaded = runner::validate(Path::new(&suite_path))?;
            runner::run(&loaded, Path::new(&output), &overrides)?;
        }
        _ => return Err(usage().into()),
    }
    Ok(())
}

fn ensure_finished(mut args: impl Iterator<Item = String>) -> Result<(), BoxError> {
    if let Some(argument) = args.next() {
        return Err(format!("unexpected argument {argument:?}").into());
    }
    Ok(())
}

fn usage() -> String {
    "usage: trevrpc-conformance validate SUITE.json\n       trevrpc-conformance run SUITE.json --out DIRECTORY [--peer ID=/absolute/executable]...".to_owned()
}
