use std::path::Path;
use std::process::ExitCode;

use trevrpc_bench::BoxError;
use trevrpc_bench::campaign::Campaign;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("trevrpc-bench: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), BoxError> {
    let mut args = std::env::args().skip(1);
    let command = args.next().ok_or_else(usage)?;
    match command.as_str() {
        "validate" => {
            let campaign_path = args.next().ok_or_else(usage)?;
            ensure_finished(args)?;
            let campaign = Campaign::read(Path::new(&campaign_path))?;
            campaign.validate()?;
            println!("campaign {} is valid", campaign.campaign_id);
        }
        "capabilities" => {
            let campaign_path = args.next().ok_or_else(usage)?;
            ensure_finished(args)?;
            let campaign = Campaign::read(Path::new(&campaign_path))?;
            campaign.validate()?;
            trevrpc_bench::runner::print_capabilities(&campaign)?;
        }
        "run" => {
            let campaign_path = args.next().ok_or_else(usage)?;
            if args.next().as_deref() != Some("--out") {
                return Err(usage().into());
            }
            let output = args.next().ok_or_else(usage)?;
            ensure_finished(args)?;
            let campaign = Campaign::read(Path::new(&campaign_path))?;
            campaign.validate()?;
            trevrpc_bench::runner::run(&campaign, Path::new(&campaign_path), Path::new(&output))?;
        }
        "report" => {
            let output = args.next().ok_or_else(usage)?;
            ensure_finished(args)?;
            trevrpc_bench::report::generate(Path::new(&output))?;
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
    "usage: trevrpc-bench validate|capabilities CAMPAIGN.json\n       trevrpc-bench run CAMPAIGN.json --out DIRECTORY\n       trevrpc-bench report DIRECTORY".to_owned()
}
