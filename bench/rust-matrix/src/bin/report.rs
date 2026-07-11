use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::fs;
use std::path::Path;

use trevrpc_rust_matrix::BoxError;
use trevrpc_rust_matrix::SCHEMA_VERSION;
use trevrpc_rust_matrix::config::Stack;
use trevrpc_rust_matrix::events::SampleResult;

#[derive(Debug)]
struct Aggregate<'a> {
    stack: Stack,
    concurrency: usize,
    samples: Vec<&'a SampleResult>,
}

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
        median_f64(self.samples.iter().map(|sample| sample.throughput_per_s))
    }

    fn client_cpu_ns_per_op(&self) -> f64 {
        median_f64(
            self.samples
                .iter()
                .map(|sample| sample.client.cpu_ns as f64 / sample.completed.max(1) as f64),
        )
    }

    fn server_cpu_ns_per_op(&self) -> f64 {
        median_f64(
            self.samples
                .iter()
                .map(|sample| sample.server.cpu_ns as f64 / sample.completed.max(1) as f64),
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
}

fn main() -> Result<(), BoxError> {
    let mut args = std::env::args().skip(1);
    let samples_path = args.next().ok_or("missing samples JSONL path")?;
    let aggregate_path = args.next().ok_or("missing aggregate CSV path")?;
    let markdown_path = args.next().ok_or("missing Markdown path")?;
    let expected_runs = args
        .next()
        .ok_or("missing expected run count")?
        .parse::<usize>()?;
    let expected_concurrencies = args
        .next()
        .ok_or("missing expected concurrency list")?
        .split(',')
        .map(str::parse::<usize>)
        .collect::<Result<BTreeSet<_>, _>>()?;
    if args.next().is_some() {
        return Err("unexpected report arguments".into());
    }

    let samples = read_samples(Path::new(&samples_path))?;
    let aggregates = aggregate(&samples, expected_runs, &expected_concurrencies)?;
    write_csv(Path::new(&aggregate_path), &aggregates)?;
    write_markdown(Path::new(&markdown_path), &samples, &aggregates)?;
    Ok(())
}

fn read_samples(path: &Path) -> Result<Vec<SampleResult>, BoxError> {
    let input = fs::read_to_string(path)?;
    let mut samples = Vec::new();
    let mut run_ids = BTreeSet::new();
    for (index, line) in input.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let sample: SampleResult = serde_json::from_str(line)
            .map_err(|error| format!("invalid sample line {}: {error}", index + 1))?;
        if sample.schema_version != SCHEMA_VERSION || sample.event != "sample" {
            return Err(format!("unsupported sample event on line {}", index + 1).into());
        }
        if sample.failed != 0 || sample.completed == 0 {
            return Err(format!("failed or empty sample {}", sample.run_id).into());
        }
        validate_sample_measurements(&sample)?;
        if !run_ids.insert(sample.run_id.clone()) {
            return Err(format!("duplicate run id {}", sample.run_id).into());
        }
        samples.push(sample);
    }
    if samples.is_empty() {
        return Err("samples file contains no results".into());
    }
    Ok(samples)
}

fn validate_sample_measurements(sample: &SampleResult) -> Result<(), BoxError> {
    let admission_ns = sample.measurement_ms.saturating_mul(1_000_000);
    if sample.elapsed_ns < admission_ns
        || sample.drain_ns != sample.elapsed_ns.saturating_sub(admission_ns)
    {
        return Err(format!("sample {} has inconsistent elapsed timing", sample.run_id).into());
    }
    let histogram_count = sample
        .histogram
        .iter()
        .map(|bucket| bucket.count)
        .sum::<u64>();
    if histogram_count != sample.completed {
        return Err(format!(
            "sample {} histogram count {histogram_count} does not match {} completions",
            sample.run_id, sample.completed
        )
        .into());
    }
    let expected_throughput = sample.completed as f64 * 1000.0 / sample.measurement_ms as f64;
    let tolerance = expected_throughput.abs().max(1.0) * 1e-9;
    if (sample.throughput_per_s - expected_throughput).abs() > tolerance {
        return Err(format!("sample {} has inconsistent throughput", sample.run_id).into());
    }
    if sample.latency_p50_ns == 0
        || sample.latency_p50_ns > sample.latency_p90_ns
        || sample.latency_p90_ns > sample.latency_p95_ns
        || sample.latency_p95_ns > sample.latency_p99_ns
        || sample.latency_p99_ns > sample.latency_p999_ns
        || sample.latency_p999_ns > sample.latency_max_ns
    {
        return Err(format!("sample {} has invalid latency quantiles", sample.run_id).into());
    }
    Ok(())
}

fn aggregate<'a>(
    samples: &'a [SampleResult],
    expected_runs: usize,
    expected_concurrencies: &BTreeSet<usize>,
) -> Result<Vec<Aggregate<'a>>, BoxError> {
    validate_matrix_invariants(samples)?;
    let mut grouped: BTreeMap<(String, usize), Vec<&SampleResult>> = BTreeMap::new();
    for sample in samples {
        grouped
            .entry((sample.stack.as_str().to_owned(), sample.concurrency))
            .or_default()
            .push(sample);
    }

    let concurrencies = samples
        .iter()
        .map(|sample| sample.concurrency)
        .collect::<BTreeSet<_>>();
    if &concurrencies != expected_concurrencies {
        return Err(format!(
            "recorded concurrencies {concurrencies:?} do not match expected {expected_concurrencies:?}"
        )
        .into());
    }
    for concurrency in concurrencies {
        for stack in [Stack::TrevrpcQuinn, Stack::GrpcTonicGenerated] {
            if !grouped.contains_key(&(stack.as_str().to_owned(), concurrency)) {
                return Err(format!(
                    "missing {} concurrency {concurrency} comparison cell",
                    stack.as_str()
                )
                .into());
            }
        }
    }

    let mut aggregates = Vec::with_capacity(grouped.len());
    for ((_stack_name, concurrency), mut group) in grouped {
        group.sort_unstable_by_key(|sample| sample.repetition);
        if group.len() != expected_runs {
            return Err(format!(
                "{} concurrency {concurrency} has {} runs, expected {expected_runs}",
                group[0].stack.as_str(),
                group.len()
            )
            .into());
        }
        let expected_repetitions = (1..=u32::try_from(expected_runs)?).collect::<Vec<_>>();
        let actual_repetitions = group
            .iter()
            .map(|sample| sample.repetition)
            .collect::<Vec<_>>();
        if actual_repetitions != expected_repetitions {
            return Err(format!(
                "{} concurrency {concurrency} has repetitions {actual_repetitions:?}",
                group[0].stack.as_str()
            )
            .into());
        }
        let first = group[0];
        if group.iter().any(|sample| {
            sample.config_hash != first.config_hash
                || sample.measurement_ms != first.measurement_ms
                || sample.warmup_ms != first.warmup_ms
                || sample.application_encoding != first.application_encoding
                || sample.workload != first.workload
                || sample.operation != first.operation
        }) {
            return Err(format!(
                "{} concurrency {concurrency} mixes immutable configuration",
                first.stack.as_str()
            )
            .into());
        }
        aggregates.push(Aggregate {
            stack: first.stack,
            concurrency,
            samples: group,
        });
    }
    Ok(aggregates)
}

fn validate_matrix_invariants(samples: &[SampleResult]) -> Result<(), BoxError> {
    let first = &samples[0];
    for sample in &samples[1..] {
        if sample.source_commit != first.source_commit
            || sample.artifact_sha256 != first.artifact_sha256
            || sample.application_encoding != first.application_encoding
            || sample.workload != first.workload
            || sample.operation != first.operation
            || sample.connections != first.connections
            || sample.warmup_ms != first.warmup_ms
            || sample.measurement_ms != first.measurement_ms
            || sample.application_request_bytes != first.application_request_bytes
            || sample.application_response_bytes != first.application_response_bytes
            || sample.transport_security_mode != first.transport_security_mode
            || sample.certificate_verification_mode != first.certificate_verification_mode
            || sample.batching_policy != first.batching_policy
            || sample.network_profile != first.network_profile
        {
            return Err(format!(
                "sample {} does not match matrix-wide immutable configuration",
                sample.run_id
            )
            .into());
        }
    }
    Ok(())
}

fn write_csv(path: &Path, aggregates: &[Aggregate<'_>]) -> Result<(), BoxError> {
    let mut output = String::from(
        "stack,application_encoding,workload,operation,concurrency,runs,latency_p50_ns_median,latency_p99_ns_median,throughput_per_s_median,client_cpu_ns_per_op_median,server_cpu_ns_per_op_median,client_peak_rss_bytes_median,server_peak_rss_bytes_median\n",
    );
    for aggregate in aggregates {
        writeln!(
            output,
            "{},protobuf,tiny,unary_closed_loop,{},{},{},{},{:.3},{:.3},{:.3},{},{}",
            aggregate.stack.as_str(),
            aggregate.concurrency,
            aggregate.runs(),
            aggregate.p50_ns(),
            aggregate.p99_ns(),
            aggregate.throughput(),
            aggregate.client_cpu_ns_per_op(),
            aggregate.server_cpu_ns_per_op(),
            aggregate.client_peak_rss(),
            aggregate.server_peak_rss()
        )?;
    }
    fs::write(path, output)?;
    Ok(())
}

fn write_markdown(
    path: &Path,
    samples: &[SampleResult],
    aggregates: &[Aggregate<'_>],
) -> Result<(), BoxError> {
    let first = &samples[0];
    let mut output = format!(
        "# Rust RPC Matrix\n\n## Context\n\n| Setting | Value |\n| --- | --- |\n| Schema version | `{}` |\n| Source commit | `{}` |\n| Application encoding | `protobuf` |\n| Workload | `tiny` |\n| Operation | `unary_closed_loop` |\n| Warmup | {} ms |\n| Measurement admission window | {} ms |\n| Security | encrypted; private CA verified; hostname verified |\n| Network | loopback |\n\n## Results\n\n| RPC stack | Concurrency | Runs | Median p50 us | Median p99 us | Median ops/s | Client CPU us/op | Server CPU us/op | Client peak RSS MiB | Server peak RSS MiB |\n| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n",
        SCHEMA_VERSION, first.source_commit, first.warmup_ms, first.measurement_ms,
    );
    for aggregate in aggregates {
        writeln!(
            output,
            "| `{}` | {} | {} | {:.3} | {:.3} | {:.0} | {:.3} | {:.3} | {:.2} | {:.2} |",
            aggregate.stack.as_str(),
            aggregate.concurrency,
            aggregate.runs(),
            aggregate.p50_ns() as f64 / 1000.0,
            aggregate.p99_ns() as f64 / 1000.0,
            aggregate.throughput(),
            aggregate.client_cpu_ns_per_op() / 1000.0,
            aggregate.server_cpu_ns_per_op() / 1000.0,
            aggregate.client_peak_rss() as f64 / 1024.0 / 1024.0,
            aggregate.server_peak_rss() as f64 / 1024.0 / 1024.0
        )?;
    }

    output.push_str(
        "\n## Interpretation Boundary\n\nThese rows compare complete Rust RPC stacks under one controlled loopback workload. They do not isolate QUIC from HTTP/2, and they do not represent cross-language interoperability. Latency is measured per operation under closed-loop load; throughput is completed operations divided by the fixed admission window. Lifetime peak RSS includes process initialization and warmup. CPU windows include the minimal Unix control-socket work needed to synchronize each process boundary.\n",
    );
    fs::write(path, output)?;
    Ok(())
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
    use super::{median_f64, median_u64};

    #[test]
    fn medians_handle_even_and_odd_inputs() {
        assert_eq!(median_u64([3, 1, 2].into_iter()), 2);
        assert_eq!(median_u64([4, 2].into_iter()), 3);
        assert!((median_f64([4.0, 2.0].into_iter()) - 3.0).abs() < f64::EPSILON);
    }
}
