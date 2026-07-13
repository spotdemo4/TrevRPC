use std::fs;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::BoxError;

#[derive(Clone, Copy, Debug, Default)]
struct Snapshot {
    cpu_ns: u64,
    rss_bytes: u64,
    peak_rss_bytes: u64,
    voluntary_context_switches: u64,
    involuntary_context_switches: u64,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
pub struct ProcessDelta {
    pub cpu_ns: u64,
    pub rss_before_bytes: u64,
    pub rss_after_bytes: u64,
    pub peak_rss_bytes: u64,
    pub voluntary_context_switches: u64,
    pub involuntary_context_switches: u64,
}

pub struct ProcessMonitor {
    baseline: Snapshot,
    latest: Arc<Mutex<Snapshot>>,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

impl ProcessMonitor {
    pub fn start(pid: u32) -> Result<Self, BoxError> {
        let baseline = Snapshot::capture(pid)?;
        let latest = Arc::new(Mutex::new(baseline));
        let stop = Arc::new(AtomicBool::new(false));
        let thread_latest = Arc::clone(&latest);
        let thread_stop = Arc::clone(&stop);
        let thread = thread::spawn(move || {
            while !thread_stop.load(Ordering::Relaxed) {
                if let Ok(snapshot) = Snapshot::capture(pid) {
                    let mut aggregate = thread_latest
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    aggregate.cpu_ns = snapshot.cpu_ns;
                    aggregate.rss_bytes = snapshot.rss_bytes;
                    aggregate.peak_rss_bytes =
                        aggregate.peak_rss_bytes.max(snapshot.peak_rss_bytes);
                    aggregate.voluntary_context_switches = snapshot.voluntary_context_switches;
                    aggregate.involuntary_context_switches = snapshot.involuntary_context_switches;
                }
                thread::sleep(Duration::from_millis(10));
            }
        });
        Ok(Self {
            baseline,
            latest,
            stop,
            thread: Some(thread),
        })
    }

    #[must_use]
    pub fn finish(mut self, pid: u32) -> ProcessDelta {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        let mut end = *self
            .latest
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Ok(snapshot) = Snapshot::capture(pid) {
            end.cpu_ns = snapshot.cpu_ns;
            end.rss_bytes = snapshot.rss_bytes;
            end.peak_rss_bytes = end.peak_rss_bytes.max(snapshot.peak_rss_bytes);
            end.voluntary_context_switches = snapshot.voluntary_context_switches;
            end.involuntary_context_switches = snapshot.involuntary_context_switches;
        }
        ProcessDelta {
            cpu_ns: end.cpu_ns.saturating_sub(self.baseline.cpu_ns),
            rss_before_bytes: self.baseline.rss_bytes,
            rss_after_bytes: end.rss_bytes,
            peak_rss_bytes: end.peak_rss_bytes.max(self.baseline.peak_rss_bytes),
            voluntary_context_switches: end
                .voluntary_context_switches
                .saturating_sub(self.baseline.voluntary_context_switches),
            involuntary_context_switches: end
                .involuntary_context_switches
                .saturating_sub(self.baseline.involuntary_context_switches),
        }
    }
}

impl Snapshot {
    fn capture(pid: u32) -> Result<Self, BoxError> {
        let stat = fs::read_to_string(format!("/proc/{pid}/stat"))?;
        let close = stat.rfind(')').ok_or("malformed process stat")?;
        let fields = stat[close + 1..].split_whitespace().collect::<Vec<_>>();
        let user_ticks = fields
            .get(11)
            .ok_or("missing process user time")?
            .parse::<u64>()?;
        let system_ticks = fields
            .get(12)
            .ok_or("missing process system time")?
            .parse::<u64>()?;
        // SAFETY: sysconf has no pointer arguments and does not mutate Rust-owned memory.
        let ticks_per_second = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
        if ticks_per_second <= 0 {
            return Err("could not determine process clock tick frequency".into());
        }
        let cpu_ns = user_ticks
            .saturating_add(system_ticks)
            .saturating_mul(1_000_000_000)
            / u64::try_from(ticks_per_second)?;

        let status = fs::read_to_string(format!("/proc/{pid}/status"))?;
        let rss_bytes = status_kib(&status, "VmRSS:")
            .unwrap_or(0)
            .saturating_mul(1024);
        Ok(Self {
            cpu_ns,
            rss_bytes,
            // VmHWM is the process lifetime high-water mark. Track sampled
            // VmRSS instead so startup and warmup allocations stay excluded.
            peak_rss_bytes: rss_bytes,
            voluntary_context_switches: status_count(&status, "voluntary_ctxt_switches:")
                .unwrap_or(0),
            involuntary_context_switches: status_count(&status, "nonvoluntary_ctxt_switches:")
                .unwrap_or(0),
        })
    }
}

fn status_kib(status: &str, key: &str) -> Option<u64> {
    status
        .lines()
        .find_map(|line| line.strip_prefix(key))?
        .split_whitespace()
        .next()?
        .parse()
        .ok()
}

fn status_count(status: &str, key: &str) -> Option<u64> {
    status
        .lines()
        .find_map(|line| line.strip_prefix(key))?
        .trim()
        .parse()
        .ok()
}

#[cfg(test)]
mod tests {
    use super::Snapshot;

    #[test]
    fn captures_current_process() {
        let snapshot = Snapshot::capture(std::process::id()).expect("process snapshot");
        assert!(snapshot.rss_bytes > 0);
        assert!(snapshot.peak_rss_bytes >= snapshot.rss_bytes);
    }
}
