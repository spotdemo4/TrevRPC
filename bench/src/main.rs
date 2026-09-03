use std::net::IpAddr;
use std::path::{Path, PathBuf};
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
        "__network-holder" => {
            ensure_finished(args)?;
            trevrpc_bench::network::hold_namespace()?;
        }
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
            let arguments = RunArguments::parse(args)?;
            let campaign = arguments.read_campaign()?;
            trevrpc_bench::network::enter_owner_namespace_if_needed(
                &campaign.network,
                &arguments.campaign_path,
                &arguments.output,
                arguments.cell.as_deref(),
            )?;
            trevrpc_bench::runner::run(&campaign, &arguments.campaign_path, &arguments.output)?;
        }
        "__network-run" => {
            let arguments = RunArguments::parse(args)?;
            let campaign = arguments.read_campaign()?;
            trevrpc_bench::network::prepare_owner_namespace(&campaign.network)?;
            trevrpc_bench::runner::run(&campaign, &arguments.campaign_path, &arguments.output)?;
        }
        "certificates" => {
            let arguments = CertificateArguments::parse(args)?;
            let certificates =
                trevrpc_bench::certificate::generate(&arguments.output, &arguments.server_ips)?;
            println!(
                "{}",
                serde_json::json!({
                    "ca": certificates.ca,
                    "certificate": certificates.certificate,
                    "private_key": certificates.private_key,
                })
            );
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

#[derive(Debug, Eq, PartialEq)]
struct CertificateArguments {
    output: PathBuf,
    server_ips: Vec<IpAddr>,
}

impl CertificateArguments {
    fn parse(mut args: impl Iterator<Item = String>) -> Result<Self, BoxError> {
        let mut output = None;
        let mut server_ips = Vec::new();
        while let Some(argument) = args.next() {
            match argument.as_str() {
                "--out" if output.is_none() => output = Some(args.next().ok_or_else(usage)?.into()),
                "--server-ip" => {
                    let address = args.next().ok_or_else(usage)?;
                    server_ips.push(
                        address
                            .parse()
                            .map_err(|error| format!("invalid server IP {address:?}: {error}"))?,
                    );
                }
                _ => return Err(format!("unexpected argument {argument:?}").into()),
            }
        }
        Ok(Self {
            output: output.ok_or_else(usage)?,
            server_ips,
        })
    }
}

#[derive(Debug, Eq, PartialEq)]
struct RunArguments {
    campaign_path: PathBuf,
    output: PathBuf,
    cell: Option<String>,
}

impl RunArguments {
    fn parse(mut args: impl Iterator<Item = String>) -> Result<Self, BoxError> {
        let campaign_path = args.next().ok_or_else(usage)?;
        if args.next().as_deref() != Some("--out") {
            return Err(usage().into());
        }
        let output = args.next().ok_or_else(usage)?;
        let cell = match args.next() {
            Some(argument) if argument == "--cell" => Some(args.next().ok_or_else(usage)?),
            Some(argument) => return Err(format!("unexpected argument {argument:?}").into()),
            None => None,
        };
        ensure_finished(args)?;
        Ok(Self {
            campaign_path: campaign_path.into(),
            output: output.into(),
            cell,
        })
    }

    fn read_campaign(&self) -> Result<Campaign, BoxError> {
        let campaign = Campaign::read(&self.campaign_path)?;
        campaign.validate()?;
        match &self.cell {
            Some(cell) => {
                let campaign = campaign.select_cell(cell)?;
                campaign.validate()?;
                Ok(campaign)
            }
            None => Ok(campaign),
        }
    }
}

fn ensure_finished(mut args: impl Iterator<Item = String>) -> Result<(), BoxError> {
    if let Some(argument) = args.next() {
        return Err(format!("unexpected argument {argument:?}").into());
    }
    Ok(())
}

fn usage() -> String {
    "usage: trevrpc-bench validate|capabilities CAMPAIGN.json\n       trevrpc-bench run CAMPAIGN.json --out DIRECTORY [--cell CELL_ID]\n       trevrpc-bench certificates --out DIRECTORY [--server-ip IP]...\n       trevrpc-bench report DIRECTORY".to_owned()
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};
    use std::path::PathBuf;

    use super::{CertificateArguments, RunArguments};

    fn parse(arguments: &[&str]) -> Result<RunArguments, trevrpc_bench::BoxError> {
        RunArguments::parse(arguments.iter().map(|argument| (*argument).to_owned()))
    }

    fn parse_certificates(
        arguments: &[&str],
    ) -> Result<CertificateArguments, trevrpc_bench::BoxError> {
        CertificateArguments::parse(arguments.iter().map(|argument| (*argument).to_owned()))
    }

    #[test]
    fn parses_certificate_arguments() {
        assert_eq!(
            parse_certificates(&[
                "--out",
                "run",
                "--server-ip",
                "127.0.0.1",
                "--server-ip",
                "10.0.2.2",
            ])
            .expect("certificate arguments"),
            CertificateArguments {
                output: PathBuf::from("run"),
                server_ips: vec![
                    IpAddr::V4(Ipv4Addr::LOCALHOST),
                    IpAddr::V4(Ipv4Addr::new(10, 0, 2, 2)),
                ],
            }
        );
    }

    #[test]
    fn rejects_invalid_certificate_arguments() {
        for arguments in [
            vec![],
            vec!["--out"],
            vec!["--server-ip", "10.0.2.2"],
            vec!["--out", "run", "--server-ip"],
            vec!["--out", "run", "--server-ip", "invalid"],
            vec!["--out", "run", "--out", "other"],
        ] {
            assert!(
                parse_certificates(&arguments).is_err(),
                "accepted {arguments:?}"
            );
        }
    }

    #[test]
    fn parses_run_arguments_without_a_cell() {
        assert_eq!(
            parse(&["campaign.json", "--out", "run"]).expect("run arguments"),
            RunArguments {
                campaign_path: PathBuf::from("campaign.json"),
                output: PathBuf::from("run"),
                cell: None,
            }
        );
    }

    #[test]
    fn parses_run_arguments_with_a_cell() {
        assert_eq!(
            parse(&["campaign.json", "--out", "run", "--cell", "go-to-rust",])
                .expect("run arguments"),
            RunArguments {
                campaign_path: PathBuf::from("campaign.json"),
                output: PathBuf::from("run"),
                cell: Some("go-to-rust".to_owned()),
            }
        );
    }

    #[test]
    fn rejects_invalid_run_arguments() {
        for arguments in [
            vec!["campaign.json", "run"],
            vec!["campaign.json", "--out", "run", "--cell"],
            vec!["campaign.json", "--out", "run", "--unknown"],
            vec![
                "campaign.json",
                "--out",
                "run",
                "--cell",
                "go-to-rust",
                "--cell",
                "c-to-c",
            ],
        ] {
            assert!(parse(&arguments).is_err(), "accepted {arguments:?}");
        }
    }
}
