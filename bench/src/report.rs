use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::fs;
use std::path::Path;

use serde::Deserialize;

use crate::campaign::{Campaign, Stack};
use crate::protocol::{expected_message_counts, histogram_quantiles, validate_timing};
use crate::runner::SampleRecord;
use crate::{BoxError, SCHEMA_VERSION};

#[derive(Deserialize)]
struct ReportManifest {
    schema_version: u32,
    campaign: Campaign,
}

#[derive(Debug)]
struct Aggregate<'a> {
    cell_id: String,
    client_peer: String,
    server_peer: String,
    stack: Stack,
    rpc_kind: String,
    concurrency: usize,
    samples: Vec<&'a SampleRecord>,
}

type AggregateKey = (String, String, String, Stack, String, usize);

impl Aggregate<'_> {
    fn runs(&self) -> usize {
        self.samples.len()
    }

    fn p50_ns(&self) -> u64 {
        median_u64(self.samples.iter().map(|sample| sample.latency_p50_ns))
    }

    fn p99_ns(&self) -> u64 {
        median_u64(self.samples.iter().map(|sample| sample.latency_p99_ns))
    }

    fn throughput(&self) -> f64 {
        median_f64(
            self.samples
                .iter()
                .map(|sample| sample.operations_per_second),
        )
    }

    fn client_cpu_ns_per_op(&self) -> f64 {
        median_f64(
            self.samples
                .iter()
                .map(|sample| sample.client.cpu_ns as f64 / sample.completed as f64),
        )
    }

    fn server_cpu_ns_per_op(&self) -> f64 {
        median_f64(
            self.samples
                .iter()
                .map(|sample| sample.server.cpu_ns as f64 / sample.completed as f64),
        )
    }

    fn client_peak_rss(&self) -> u64 {
        median_u64(
            self.samples
                .iter()
                .map(|sample| sample.client.peak_rss_bytes),
        )
    }

    fn server_peak_rss(&self) -> u64 {
        median_u64(
            self.samples
                .iter()
                .map(|sample| sample.server.peak_rss_bytes),
        )
    }

    fn label(&self) -> String {
        format!(
            "{} {} {} c{}",
            self.cell_id,
            self.stack.as_str(),
            self.rpc_kind,
            self.concurrency
        )
    }
}

pub fn generate(output: &Path) -> Result<(), BoxError> {
    let manifest: ReportManifest =
        serde_json::from_str(&fs::read_to_string(output.join("manifest.json"))?)?;
    if manifest.schema_version != SCHEMA_VERSION
        || manifest.schema_version != manifest.campaign.schema_version
    {
        return Err("unsupported benchmark manifest schema".into());
    }
    let samples = read_samples(&output.join("samples.jsonl"))?;
    validate_samples(&manifest.campaign, &samples)?;
    let aggregates = aggregate(&samples);
    write_csv(&output.join("aggregate.csv"), &aggregates)?;
    write_markdown(&output.join("report.md"), &aggregates)?;
    write_html(&output.join("report.html"), &aggregates)?;
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn validate_samples(campaign: &Campaign, samples: &[SampleRecord]) -> Result<(), BoxError> {
    campaign.validate()?;
    let expected_count = usize::try_from(campaign.repetitions)?
        .saturating_mul(campaign.cells.len())
        .saturating_mul(campaign.rpc_kinds.len())
        .saturating_mul(campaign.concurrencies.len());
    if samples.len() != expected_count {
        return Err(format!(
            "recorded {} samples, expected {expected_count}",
            samples.len()
        )
        .into());
    }

    let mut sample_ids = BTreeSet::new();
    let mut cells = BTreeSet::new();
    for sample in samples {
        if sample.schema_version != campaign.schema_version
            || sample.campaign_id != campaign.campaign_id
        {
            return Err(format!(
                "sample {} has inconsistent schema or campaign",
                sample.sample_id
            )
            .into());
        }
        if !sample_ids.insert(sample.sample_id.as_str()) {
            return Err(format!("duplicate sample id {}", sample.sample_id).into());
        }
        let cell = campaign
            .cells
            .iter()
            .find(|cell| cell.id == sample.cell_id)
            .ok_or_else(|| format!("sample {} references an unknown cell", sample.sample_id))?;
        if sample.client_peer != cell.client
            || sample.server_peer != cell.server
            || sample.stack != cell.stack
            || !campaign.rpc_kinds.contains(&sample.rpc_kind)
            || !campaign.concurrencies.contains(&sample.concurrency)
            || sample.repetition == 0
            || sample.repetition > campaign.repetitions
        {
            return Err(
                format!("sample {} does not match its matrix cell", sample.sample_id).into(),
            );
        }
        if sample.warmup_ms != campaign.timing.warmup_ms
            || sample.measurement_ms != campaign.timing.measurement_ms
            || sample.request_bytes != campaign.workload.request_bytes
            || sample.response_bytes != campaign.workload.response_bytes
            || sample.messages_per_stream != campaign.workload.messages_per_stream
            || sample.completed == 0
            || sample.failed != 0
        {
            return Err(format!(
                "sample {} mixes immutable workload settings",
                sample.sample_id
            )
            .into());
        }
        validate_timing(
            sample.admission_ns,
            sample.elapsed_ns,
            sample.drain_ns,
            campaign.timing.measurement_ms,
        )
        .map_err(|error| {
            format!(
                "sample {} has inconsistent timing: {error}",
                sample.sample_id
            )
        })?;
        let expected_messages = expected_message_counts(
            sample.rpc_kind,
            sample.completed,
            sample.messages_per_stream,
        )?;
        if (sample.request_messages, sample.response_messages) != expected_messages {
            return Err(format!(
                "sample {} has inconsistent message counts",
                sample.sample_id
            )
            .into());
        }
        let histogram = histogram_quantiles(&sample.histogram)?;
        if histogram
            != (
                sample.completed,
                sample.latency_p50_ns,
                sample.latency_p99_ns,
                sample.latency_max_ns,
            )
        {
            return Err(format!(
                "sample {} has inconsistent histogram data",
                sample.sample_id
            )
            .into());
        }
        validate_rate(
            sample.operations_per_second,
            sample.completed,
            sample.admission_ns,
            &sample.sample_id,
            "operation",
        )?;
        validate_rate(
            sample.request_messages_per_second,
            sample.request_messages,
            sample.admission_ns,
            &sample.sample_id,
            "request-message",
        )?;
        validate_rate(
            sample.response_messages_per_second,
            sample.response_messages,
            sample.admission_ns,
            &sample.sample_id,
            "response-message",
        )?;
        if !cells.insert((
            sample.cell_id.as_str(),
            sample.rpc_kind,
            sample.concurrency,
            sample.repetition,
        )) {
            return Err(format!("duplicate matrix cell for sample {}", sample.sample_id).into());
        }
    }
    Ok(())
}

fn validate_rate(
    actual: f64,
    count: u64,
    admission_ns: u64,
    sample_id: &str,
    name: &str,
) -> Result<(), BoxError> {
    let expected = count as f64 * 1_000_000_000.0 / admission_ns as f64;
    let tolerance = expected.abs().max(1.0) * 1e-9;
    if !actual.is_finite() || (actual - expected).abs() > tolerance {
        return Err(format!("sample {sample_id} has inconsistent {name} throughput").into());
    }
    Ok(())
}

fn read_samples(path: &Path) -> Result<Vec<SampleRecord>, BoxError> {
    let input = fs::read_to_string(path)?;
    let mut samples = Vec::new();
    for (index, line) in input.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let sample = serde_json::from_str(line)
            .map_err(|error| format!("invalid sample line {}: {error}", index + 1))?;
        samples.push(sample);
    }
    if samples.is_empty() {
        return Err("samples file is empty".into());
    }
    Ok(samples)
}

fn aggregate(samples: &[SampleRecord]) -> Vec<Aggregate<'_>> {
    let mut groups: BTreeMap<AggregateKey, Vec<&SampleRecord>> = BTreeMap::new();
    for sample in samples {
        groups
            .entry((
                sample.cell_id.clone(),
                sample.client_peer.clone(),
                sample.server_peer.clone(),
                sample.stack,
                sample.rpc_kind.as_str().to_owned(),
                sample.concurrency,
            ))
            .or_default()
            .push(sample);
    }
    groups
        .into_iter()
        .map(
            |((cell_id, client_peer, server_peer, stack, rpc_kind, concurrency), samples)| {
                Aggregate {
                    cell_id,
                    client_peer,
                    server_peer,
                    stack,
                    rpc_kind,
                    concurrency,
                    samples,
                }
            },
        )
        .collect()
}

fn write_csv(path: &Path, aggregates: &[Aggregate<'_>]) -> Result<(), BoxError> {
    let mut output = String::from(
        "cell,stack,client,server,rpc_kind,concurrency,runs,latency_p50_ns_median,latency_p99_ns_median,operations_per_second_median,client_cpu_ns_per_op_median,server_cpu_ns_per_op_median,client_peak_rss_bytes_median,server_peak_rss_bytes_median\n",
    );
    for aggregate in aggregates {
        writeln!(
            output,
            "{},{},{},{},{},{},{},{},{},{:.3},{:.3},{:.3},{},{}",
            aggregate.cell_id,
            aggregate.stack.as_str(),
            aggregate.client_peer,
            aggregate.server_peer,
            aggregate.rpc_kind,
            aggregate.concurrency,
            aggregate.runs(),
            aggregate.p50_ns(),
            aggregate.p99_ns(),
            aggregate.throughput(),
            aggregate.client_cpu_ns_per_op(),
            aggregate.server_cpu_ns_per_op(),
            aggregate.client_peak_rss(),
            aggregate.server_peak_rss(),
        )?;
    }
    fs::write(path, output)?;
    Ok(())
}

fn write_markdown(path: &Path, aggregates: &[Aggregate<'_>]) -> Result<(), BoxError> {
    let mut output = String::from(
        "# RPC Benchmark Report\n\n| Cell | Stack | Client | Server | RPC | Concurrency | Runs | p50 us | p99 us | ops/s | Client CPU us/op | Server CPU us/op | Client peak MiB | Server peak MiB |\n| --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n",
    );
    for aggregate in aggregates {
        writeln!(
            output,
            "| `{}` | `{}` | `{}` | `{}` | `{}` | {} | {} | {:.3} | {:.3} | {:.3} | {:.3} | {:.3} | {:.2} | {:.2} |",
            aggregate.cell_id,
            aggregate.stack.as_str(),
            aggregate.client_peer,
            aggregate.server_peer,
            aggregate.rpc_kind,
            aggregate.concurrency,
            aggregate.runs(),
            aggregate.p50_ns() as f64 / 1000.0,
            aggregate.p99_ns() as f64 / 1000.0,
            aggregate.throughput(),
            aggregate.client_cpu_ns_per_op() / 1000.0,
            aggregate.server_cpu_ns_per_op() / 1000.0,
            aggregate.client_peak_rss() as f64 / 1024.0 / 1024.0,
            aggregate.server_peak_rss() as f64 / 1024.0 / 1024.0,
        )?;
    }
    output.push_str("\nLatency is complete bounded-RPC latency under closed-loop load. Throughput uses the fixed admission window; connection setup and warmup are excluded. CPU and RSS cover complete peer process groups and are sampled externally through procfs.\n");
    fs::write(path, output)?;
    Ok(())
}

fn write_html(path: &Path, aggregates: &[Aggregate<'_>]) -> Result<(), BoxError> {
    let throughput_max = aggregates
        .iter()
        .map(Aggregate::throughput)
        .fold(0.0_f64, f64::max)
        .max(1.0);
    let p99_max = aggregates
        .iter()
        .map(|aggregate| aggregate.p99_ns() as f64 / 1000.0)
        .fold(0.0_f64, f64::max)
        .max(1.0);
    let height = 70 + aggregates.len() * 30;
    let mut throughput_bars = String::new();
    let mut latency_bars = String::new();
    let mut table_rows = String::new();
    for (index, aggregate) in aggregates.iter().enumerate() {
        let y = 35 + index * 30;
        let throughput_width = aggregate.throughput() / throughput_max * 500.0;
        let p99_us = aggregate.p99_ns() as f64 / 1000.0;
        let latency_width = p99_us / p99_max * 500.0;
        let label = html_escape(&aggregate.label());
        writeln!(
            throughput_bars,
            "<text x=\"0\" y=\"{}\">{}</text><rect x=\"230\" y=\"{}\" width=\"{:.1}\" height=\"18\"/><text x=\"{:.1}\" y=\"{}\">{:.3}</text>",
            y + 14,
            label,
            y,
            throughput_width,
            238.0 + throughput_width,
            y + 14,
            aggregate.throughput()
        )?;
        writeln!(
            latency_bars,
            "<text x=\"0\" y=\"{}\">{}</text><rect x=\"230\" y=\"{}\" width=\"{:.1}\" height=\"18\"/><text x=\"{:.1}\" y=\"{}\">{:.3} us</text>",
            y + 14,
            label,
            y,
            latency_width,
            238.0 + latency_width,
            y + 14,
            p99_us
        )?;
        writeln!(
            table_rows,
            "<tr><td><code>{}</code></td><td><code>{}</code></td><td><code>{}</code></td><td><code>{}</code></td><td><code>{}</code></td><td>{}</td><td>{}</td><td>{:.3}</td><td>{:.3}</td><td>{:.3}</td></tr>",
            html_escape(&aggregate.cell_id),
            html_escape(aggregate.stack.as_str()),
            html_escape(&aggregate.client_peer),
            html_escape(&aggregate.server_peer),
            html_escape(&aggregate.rpc_kind),
            aggregate.concurrency,
            aggregate.runs(),
            aggregate.p50_ns() as f64 / 1000.0,
            p99_us,
            aggregate.throughput(),
        )?;
    }
    let html = format!(
        "<!doctype html><html><head><meta charset=\"utf-8\"><title>RPC Benchmark Report</title><style>body{{font:14px system-ui,sans-serif;max-width:1100px;margin:40px auto;padding:0 20px;color:#18202b}}svg{{width:100%;overflow:visible}}rect{{fill:#176b87}}text{{font-size:12px;fill:#18202b}}h1,h2{{letter-spacing:-.02em}}table{{border-collapse:collapse;width:100%}}th,td{{border-bottom:1px solid #ccd3da;padding:8px;text-align:left}}th{{background:#eef2f5}}</style></head><body><h1>RPC Benchmark Report</h1><h2>Results</h2><table><thead><tr><th>Cell</th><th>Stack</th><th>Client</th><th>Server</th><th>RPC</th><th>Concurrency</th><th>Runs</th><th>p50 us</th><th>p99 us</th><th>ops/s</th></tr></thead><tbody>{table_rows}</tbody></table><h2>Median throughput (operations/s)</h2><svg viewBox=\"0 0 850 {height}\">{throughput_bars}</svg><h2>Median p99 latency</h2><svg viewBox=\"0 0 850 {height}\">{latency_bars}</svg><p>See <code>aggregate.csv</code>, <code>samples.jsonl</code>, and <code>manifest.json</code> for canonical data and provenance.</p></body></html>"
    );
    fs::write(path, html)?;
    Ok(())
}

fn html_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn median_u64(values: impl Iterator<Item = u64>) -> u64 {
    let mut values = values.collect::<Vec<_>>();
    values.sort_unstable();
    match values.len() {
        0 => 0,
        length if length % 2 == 1 => values[length / 2],
        length => values[length / 2 - 1].saturating_add(values[length / 2]) / 2,
    }
}

fn median_f64(values: impl Iterator<Item = f64>) -> f64 {
    let mut values = values.collect::<Vec<_>>();
    values.sort_by(f64::total_cmp);
    match values.len() {
        0 => 0.0,
        length if length % 2 == 1 => values[length / 2],
        length => f64::midpoint(values[length / 2 - 1], values[length / 2]),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::{aggregate, validate_samples, write_html, write_markdown};
    use crate::SCHEMA_VERSION;
    use crate::campaign::{Campaign, Cell, Network, Peer, RpcKind, Stack, Timing, Workload};
    use crate::metrics::ProcessDelta;
    use crate::protocol::HistogramBucket;
    use crate::runner::SampleRecord;

    fn campaign(stack: Stack) -> Campaign {
        Campaign {
            schema_version: SCHEMA_VERSION,
            campaign_id: "comparison".to_owned(),
            repetitions: 1,
            peers: vec![Peer {
                id: "rust".to_owned(),
                command: vec!["peer".to_owned()],
            }],
            cells: vec![Cell {
                id: "rust-rust".to_owned(),
                client: "rust".to_owned(),
                server: "rust".to_owned(),
                stack,
            }],
            rpc_kinds: vec![RpcKind::Unary],
            concurrencies: vec![1],
            timing: Timing {
                warmup_ms: 0,
                measurement_ms: 1,
            },
            workload: Workload {
                request_bytes: 16,
                response_bytes: 16,
                messages_per_stream: 1,
            },
            network: Network::default(),
            startup_timeout_ms: 1000,
            drain_timeout_ms: 1000,
        }
    }

    fn sample(stack: Stack) -> SampleRecord {
        SampleRecord {
            schema_version: SCHEMA_VERSION,
            campaign_id: "comparison".to_owned(),
            sample_id: format!("rust-rust-{}-unary-c1-r1", stack.as_str()),
            cell_id: "rust-rust".to_owned(),
            repetition: 1,
            client_peer: "rust".to_owned(),
            server_peer: "rust".to_owned(),
            stack,
            rpc_kind: RpcKind::Unary,
            concurrency: 1,
            warmup_ms: 0,
            measurement_ms: 1,
            request_bytes: 16,
            response_bytes: 16,
            messages_per_stream: 1,
            admission_ns: 1_000_000,
            elapsed_ns: 1_000_000,
            drain_ns: 0,
            completed: 1,
            failed: 0,
            request_messages: 1,
            response_messages: 1,
            operations_per_second: 1000.0,
            request_messages_per_second: 1000.0,
            response_messages_per_second: 1000.0,
            latency_p50_ns: 10,
            latency_p99_ns: 10,
            latency_max_ns: 10,
            client: ProcessDelta::default(),
            server: ProcessDelta::default(),
            histogram: vec![HistogramBucket {
                upper_bound_ns: "10".to_owned(),
                count: "1".to_owned(),
            }],
        }
    }

    #[test]
    fn aggregates_identical_dimensions_separately_by_stack() {
        let samples = vec![
            sample(Stack::TrevrpcNativeQuic),
            sample(Stack::TrevrpcWebtransport),
        ];
        let aggregates = aggregate(&samples);
        assert_eq!(aggregates.len(), 2);
        assert_ne!(aggregates[0].stack, aggregates[1].stack);
    }

    #[test]
    fn rejects_sample_stack_that_differs_from_its_campaign_cell() {
        let error = validate_samples(
            &campaign(Stack::TrevrpcNativeQuic),
            &[sample(Stack::TrevrpcWebtransport)],
        )
        .expect_err("mismatched sample stack");
        assert!(error.to_string().contains("does not match its matrix cell"));
    }

    #[test]
    fn preserves_fractional_throughput_in_generated_reports() {
        let mut sample = sample(Stack::TrevrpcNativeQuic);
        sample.operations_per_second = 15.4;
        let samples = vec![sample];
        let aggregates = aggregate(&samples);
        let output = std::env::temp_dir();
        let markdown_path = output.join(format!("trevrpc-bench-report-{}.md", std::process::id()));
        let html_path = output.join(format!("trevrpc-bench-report-{}.html", std::process::id()));

        write_markdown(&markdown_path, &aggregates).expect("write Markdown report");
        write_html(&html_path, &aggregates).expect("write HTML report");
        let markdown = fs::read_to_string(&markdown_path).expect("read Markdown report");
        let html = fs::read_to_string(&html_path).expect("read HTML report");

        assert!(markdown.contains("| 15.400 |"));
        assert_eq!(html.matches(">15.400<").count(), 2);

        fs::remove_file(markdown_path).expect("remove Markdown report");
        fs::remove_file(html_path).expect("remove HTML report");
    }
}
