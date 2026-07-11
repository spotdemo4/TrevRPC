use std::fs;

use serde::{Deserialize, Serialize};

use crate::BoxError;

#[derive(Clone, Copy, Debug, Default)]
pub struct ProcessSnapshot {
    pub cpu_ns: u64,
    pub rss_bytes: u64,
    pub peak_rss_bytes: u64,
    pub voluntary_context_switches: u64,
    pub involuntary_context_switches: u64,
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

impl ProcessSnapshot {
    pub fn capture() -> Result<Self, BoxError> {
        let mut cpu_time = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: clock_gettime writes to the valid timespec pointer supplied here.
        if unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &raw mut cpu_time) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }

        let status = fs::read_to_string("/proc/self/status")?;
        Ok(Self {
            cpu_ns: u64::try_from(cpu_time.tv_sec)?
                .saturating_mul(1_000_000_000)
                .saturating_add(u64::try_from(cpu_time.tv_nsec)?),
            rss_bytes: status_kib(&status, "VmRSS:")?.saturating_mul(1024),
            peak_rss_bytes: status_kib(&status, "VmHWM:")?.saturating_mul(1024),
            voluntary_context_switches: status_count(&status, "voluntary_ctxt_switches:")?,
            involuntary_context_switches: status_count(&status, "nonvoluntary_ctxt_switches:")?,
        })
    }

    #[must_use]
    pub fn delta(self, end: Self) -> ProcessDelta {
        ProcessDelta {
            cpu_ns: end.cpu_ns.saturating_sub(self.cpu_ns),
            rss_before_bytes: self.rss_bytes,
            rss_after_bytes: end.rss_bytes,
            peak_rss_bytes: end.peak_rss_bytes.max(self.peak_rss_bytes),
            voluntary_context_switches: end
                .voluntary_context_switches
                .saturating_sub(self.voluntary_context_switches),
            involuntary_context_switches: end
                .involuntary_context_switches
                .saturating_sub(self.involuntary_context_switches),
        }
    }
}

fn status_kib(status: &str, key: &str) -> Result<u64, BoxError> {
    let value = status
        .lines()
        .find_map(|line| line.strip_prefix(key))
        .ok_or_else(|| format!("missing {key} in /proc/self/status"))?
        .split_whitespace()
        .next()
        .ok_or_else(|| format!("missing value for {key}"))?;
    Ok(value.parse()?)
}

fn status_count(status: &str, key: &str) -> Result<u64, BoxError> {
    let value = status
        .lines()
        .find_map(|line| line.strip_prefix(key))
        .ok_or_else(|| format!("missing {key} in /proc/self/status"))?
        .trim();
    Ok(value.parse()?)
}

#[cfg(test)]
mod tests {
    use super::ProcessSnapshot;

    #[test]
    fn process_snapshot_is_readable() {
        let snapshot = ProcessSnapshot::capture().expect("capture process metrics");
        assert!(snapshot.cpu_ns > 0);
        assert!(snapshot.rss_bytes > 0);
        assert!(snapshot.peak_rss_bytes >= snapshot.rss_bytes);
    }
}
