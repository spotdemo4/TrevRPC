use serde::{Deserialize, Serialize};

use crate::BoxError;

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
pub struct ProcessDelta {
    pub cpu_ns: u64,
    pub rss_before_bytes: u64,
    pub rss_after_bytes: u64,
    pub peak_rss_bytes: u64,
    pub voluntary_context_switches: u64,
    pub involuntary_context_switches: u64,
}

#[cfg(target_os = "linux")]
pub const METRICS_SCOPE: &str = "peer_process_group_procfs_10ms";
#[cfg(not(target_os = "linux"))]
pub const METRICS_SCOPE: &str = "peer_process_group_metrics_unavailable";

#[cfg(target_os = "linux")]
mod platform {
    use std::collections::BTreeMap;
    use std::fs;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::Duration;

    use super::{BoxError, ProcessDelta};

    #[derive(Clone, Copy, Debug, Default)]
    struct Snapshot {
        cpu_ns: u64,
        rss_bytes: u64,
        peak_rss_bytes: u64,
        voluntary_context_switches: u64,
        involuntary_context_switches: u64,
    }

    #[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
    struct ProcessIdentity {
        pid: u32,
        start_ticks: u64,
    }

    #[derive(Clone, Copy, Debug, Default)]
    struct MemberSnapshot {
        cpu_ticks: u64,
        rss_bytes: u64,
        voluntary_context_switches: u64,
        involuntary_context_switches: u64,
    }

    #[derive(Debug, Default)]
    struct GroupSnapshot {
        members: BTreeMap<ProcessIdentity, MemberSnapshot>,
    }

    #[derive(Debug)]
    struct GroupAccumulator {
        ticks_per_second: u64,
        cpu_ticks: BTreeMap<ProcessIdentity, u64>,
        rss_bytes: u64,
        peak_rss_bytes: u64,
        context_switches: BTreeMap<ProcessIdentity, (u64, u64)>,
    }

    pub struct ProcessMonitor {
        baseline: Snapshot,
        latest: Arc<Mutex<GroupAccumulator>>,
        stop: Arc<AtomicBool>,
        thread: Option<thread::JoinHandle<()>>,
        process_group: u32,
    }

    impl ProcessMonitor {
        pub fn start(process_group: u32) -> Result<Self, BoxError> {
            let initial = GroupSnapshot::capture(process_group)?;
            let accumulator = GroupAccumulator::new(&initial)?;
            let baseline = accumulator.snapshot();
            let latest = Arc::new(Mutex::new(accumulator));
            let stop = Arc::new(AtomicBool::new(false));
            let thread_latest = Arc::clone(&latest);
            let thread_stop = Arc::clone(&stop);
            let thread = thread::spawn(move || {
                while !thread_stop.load(Ordering::Relaxed) {
                    if let Ok(snapshot) = GroupSnapshot::capture(process_group) {
                        thread_latest
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .absorb(&snapshot);
                    }
                    thread::sleep(Duration::from_millis(10));
                }
            });
            Ok(Self {
                baseline,
                latest,
                stop,
                thread: Some(thread),
                process_group,
            })
        }

        #[must_use]
        pub fn finish(mut self) -> ProcessDelta {
            self.stop.store(true, Ordering::Relaxed);
            if let Some(thread) = self.thread.take() {
                let _ = thread.join();
            }
            let mut aggregate = self
                .latest
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Ok(snapshot) = GroupSnapshot::capture(self.process_group) {
                aggregate.absorb(&snapshot);
            }
            let end = aggregate.snapshot();
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

    impl Drop for ProcessMonitor {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Relaxed);
            if let Some(thread) = self.thread.take() {
                let _ = thread.join();
            }
        }
    }

    impl GroupAccumulator {
        fn new(initial: &GroupSnapshot) -> Result<Self, BoxError> {
            // SAFETY: sysconf has no pointer arguments and does not mutate Rust-owned memory.
            let ticks_per_second = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
            if ticks_per_second <= 0 {
                return Err("could not determine process clock tick frequency".into());
            }
            let mut accumulator = Self {
                ticks_per_second: u64::try_from(ticks_per_second)?,
                cpu_ticks: BTreeMap::new(),
                rss_bytes: 0,
                peak_rss_bytes: 0,
                context_switches: BTreeMap::new(),
            };
            accumulator.absorb(initial);
            Ok(accumulator)
        }

        fn absorb(&mut self, snapshot: &GroupSnapshot) {
            let rss_bytes = snapshot.members.values().fold(0_u64, |total, member| {
                total.saturating_add(member.rss_bytes)
            });
            self.rss_bytes = rss_bytes;
            self.peak_rss_bytes = self.peak_rss_bytes.max(rss_bytes);
            for (&identity, member) in &snapshot.members {
                let cpu_ticks = self.cpu_ticks.entry(identity).or_default();
                *cpu_ticks = (*cpu_ticks).max(member.cpu_ticks);
                let counters = self.context_switches.entry(identity).or_default();
                counters.0 = counters.0.max(member.voluntary_context_switches);
                counters.1 = counters.1.max(member.involuntary_context_switches);
            }
        }

        fn snapshot(&self) -> Snapshot {
            let (voluntary_context_switches, involuntary_context_switches) = self
                .context_switches
                .values()
                .fold((0_u64, 0_u64), |total, counters| {
                    (
                        total.0.saturating_add(counters.0),
                        total.1.saturating_add(counters.1),
                    )
                });
            Snapshot {
                cpu_ns: self
                    .cpu_ticks
                    .values()
                    .fold(0_u64, |total, ticks| total.saturating_add(*ticks))
                    .saturating_mul(1_000_000_000)
                    / self.ticks_per_second,
                rss_bytes: self.rss_bytes,
                peak_rss_bytes: self.peak_rss_bytes,
                voluntary_context_switches,
                involuntary_context_switches,
            }
        }
    }

    impl GroupSnapshot {
        fn capture(process_group: u32) -> Result<Self, BoxError> {
            let mut members = BTreeMap::new();
            for entry in fs::read_dir("/proc")? {
                let Ok(entry) = entry else {
                    continue;
                };
                let Some(pid) = entry
                    .file_name()
                    .to_str()
                    .and_then(|name| name.parse::<u32>().ok())
                else {
                    continue;
                };
                let Ok(stat) = fs::read_to_string(entry.path().join("stat")) else {
                    continue;
                };
                let Some((identity, mut member, member_group)) = parse_stat(pid, &stat) else {
                    continue;
                };
                if member_group != process_group {
                    continue;
                }
                if let Ok(status) = fs::read_to_string(entry.path().join("status")) {
                    member.rss_bytes = status_kib(&status, "VmRSS:")
                        .unwrap_or(0)
                        .saturating_mul(1024);
                    member.voluntary_context_switches =
                        status_count(&status, "voluntary_ctxt_switches:").unwrap_or(0);
                    member.involuntary_context_switches =
                        status_count(&status, "nonvoluntary_ctxt_switches:").unwrap_or(0);
                }
                members.insert(identity, member);
            }
            if members.is_empty() {
                return Err(format!("process group {process_group} has no visible members").into());
            }
            Ok(Self { members })
        }
    }

    fn parse_stat(pid: u32, stat: &str) -> Option<(ProcessIdentity, MemberSnapshot, u32)> {
        let close = stat.rfind(')')?;
        let fields = stat
            .get(close + 1..)?
            .split_whitespace()
            .collect::<Vec<_>>();
        let process_group = fields.get(2)?.parse().ok()?;
        let user_ticks = fields.get(11)?.parse::<u64>().ok()?;
        let system_ticks = fields.get(12)?.parse::<u64>().ok()?;
        let start_ticks = fields.get(19)?.parse().ok()?;
        Some((
            ProcessIdentity { pid, start_ticks },
            MemberSnapshot {
                cpu_ticks: user_ticks.saturating_add(system_ticks),
                ..MemberSnapshot::default()
            },
            process_group,
        ))
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
        use std::collections::BTreeMap;

        use super::{GroupAccumulator, GroupSnapshot, MemberSnapshot, ProcessIdentity};

        #[test]
        fn captures_current_process_group() {
            // SAFETY: getpgrp has no arguments and only reads process metadata.
            let process_group = unsafe { libc::getpgrp() };
            let snapshot = GroupSnapshot::capture(process_group.cast_unsigned())
                .expect("process-group snapshot");
            assert!(snapshot.members.values().any(|member| member.rss_bytes > 0));
        }

        #[test]
        fn retains_context_switches_from_exited_members() {
            let identity = ProcessIdentity {
                pid: 10,
                start_ticks: 20,
            };
            let initial = GroupSnapshot {
                members: BTreeMap::from([(
                    identity,
                    MemberSnapshot {
                        voluntary_context_switches: 3,
                        involuntary_context_switches: 2,
                        ..MemberSnapshot::default()
                    },
                )]),
            };
            let mut accumulator = GroupAccumulator::new(&initial).expect("group accumulator");
            accumulator.absorb(&GroupSnapshot::default());
            let snapshot = accumulator.snapshot();
            assert_eq!(snapshot.voluntary_context_switches, 3);
            assert_eq!(snapshot.involuntary_context_switches, 2);
        }

        #[test]
        fn retains_self_cpu_from_exited_members_without_parent_child_time() {
            let parent = ProcessIdentity {
                pid: 10,
                start_ticks: 20,
            };
            let child = ProcessIdentity {
                pid: 11,
                start_ticks: 21,
            };
            let initial = GroupSnapshot {
                members: BTreeMap::from([
                    (
                        parent,
                        MemberSnapshot {
                            cpu_ticks: 5,
                            ..MemberSnapshot::default()
                        },
                    ),
                    (
                        child,
                        MemberSnapshot {
                            cpu_ticks: 7,
                            ..MemberSnapshot::default()
                        },
                    ),
                ]),
            };
            let mut accumulator = GroupAccumulator::new(&initial).expect("group accumulator");
            accumulator.absorb(&GroupSnapshot {
                members: BTreeMap::from([(
                    parent,
                    MemberSnapshot {
                        cpu_ticks: 8,
                        ..MemberSnapshot::default()
                    },
                )]),
            });
            assert_eq!(accumulator.cpu_ticks.get(&parent), Some(&8));
            assert_eq!(accumulator.cpu_ticks.get(&child), Some(&7));
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod platform {
    use super::{BoxError, ProcessDelta};

    pub struct ProcessMonitor;

    impl ProcessMonitor {
        pub fn start(_process_group: u32) -> Result<Self, BoxError> {
            Ok(Self)
        }

        #[must_use]
        pub fn finish(self) -> ProcessDelta {
            ProcessDelta::default()
        }
    }

    #[cfg(test)]
    mod tests {
        use super::ProcessMonitor;

        #[test]
        fn reports_unavailable_metrics_as_zero() {
            let delta = ProcessMonitor::start(1)
                .expect("unavailable metrics monitor")
                .finish();
            assert_eq!(delta.cpu_ns, 0);
            assert_eq!(delta.peak_rss_bytes, 0);
        }
    }
}

pub use platform::ProcessMonitor;
