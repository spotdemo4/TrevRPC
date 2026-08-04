use std::fs::{self, File};
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use crate::BoxError;

const READ_CHUNK_BYTES: usize = 8192;
const POLL_INTERVAL: Duration = Duration::from_millis(10);

#[derive(Clone, Copy, Debug)]
pub struct OutputLimits {
    pub max_line_bytes: usize,
    pub max_stdout_bytes: usize,
    pub max_stderr_bytes: usize,
    pub diagnostic_tail_bytes: usize,
}

#[derive(Debug)]
enum ReaderEvent {
    Line(Vec<u8>),
    LineTooLong,
    Io(String),
    Eof,
}

#[derive(Debug, Eq, PartialEq)]
pub enum PollLine {
    Line(String),
    Empty,
    Disconnected,
    LineTooLong,
    StreamTooLong,
}

pub struct ManagedChild {
    child: Child,
    process_group: Option<u32>,
    stdin: ChildStdin,
    stdout_events: mpsc::Receiver<ReaderEvent>,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
    stdout_overflow: Arc<AtomicBool>,
    stderr_overflow: Arc<AtomicBool>,
    stdout_reader: Option<thread::JoinHandle<()>>,
    stderr_reader: Option<thread::JoinHandle<()>>,
    diagnostic_tail_bytes: usize,
    reaped: bool,
}

impl ManagedChild {
    pub fn spawn(
        mut command: Command,
        stdout_path: &Path,
        stderr_path: &Path,
        limits: OutputLimits,
    ) -> Result<Self, BoxError> {
        configure_session(&mut command);
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let stdin = child.stdin.take().ok_or("child stdin was not piped")?;
        let stdout = child.stdout.take().ok_or("child stdout was not piped")?;
        let stderr = child.stderr.take().ok_or("child stderr was not piped")?;
        let queue_capacity = limits
            .max_stdout_bytes
            .checked_div(limits.max_line_bytes.max(1))
            .unwrap_or(0)
            .clamp(1, 1024);
        let (sender, stdout_events) = mpsc::sync_channel(queue_capacity);
        let stdout_overflow = Arc::new(AtomicBool::new(false));
        let stderr_overflow = Arc::new(AtomicBool::new(false));

        let stdout_reader = spawn_stdout_reader(
            stdout,
            stdout_path.to_path_buf(),
            limits.max_line_bytes,
            limits.max_stdout_bytes,
            Arc::clone(&stdout_overflow),
            sender,
        );
        let stderr_reader = spawn_stderr_reader(
            stderr,
            stderr_path.to_path_buf(),
            limits.max_stderr_bytes,
            Arc::clone(&stderr_overflow),
        );

        let process_group = child.id();
        Ok(Self {
            child,
            process_group: Some(process_group),
            stdin,
            stdout_events,
            stdout_path: stdout_path.to_path_buf(),
            stderr_path: stderr_path.to_path_buf(),
            stdout_overflow,
            stderr_overflow,
            stdout_reader: Some(stdout_reader),
            stderr_reader: Some(stderr_reader),
            diagnostic_tail_bytes: limits.diagnostic_tail_bytes,
            reaped: false,
        })
    }

    #[must_use]
    pub fn id(&self) -> u32 {
        self.child.id()
    }

    #[must_use]
    pub fn stdout_path(&self) -> &Path {
        &self.stdout_path
    }

    #[must_use]
    pub fn stderr_path(&self) -> &Path {
        &self.stderr_path
    }

    #[must_use]
    pub fn stdout_overflowed(&self) -> bool {
        self.stdout_overflow.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn stderr_overflowed(&self) -> bool {
        self.stderr_overflow.load(Ordering::Relaxed)
    }

    pub fn send(&mut self, line: &str) -> Result<(), BoxError> {
        self.stdin.write_all(line.as_bytes())?;
        self.stdin.write_all(b"\n")?;
        self.stdin.flush()?;
        Ok(())
    }

    pub fn poll_line(&mut self) -> Result<PollLine, BoxError> {
        if self.stdout_overflowed() {
            return Ok(PollLine::StreamTooLong);
        }
        match self.stdout_events.try_recv().ok() {
            Some(ReaderEvent::Line(bytes)) => Ok(PollLine::Line(String::from_utf8(bytes)?)),
            Some(ReaderEvent::LineTooLong) => Ok(PollLine::LineTooLong),
            Some(ReaderEvent::Io(error)) => {
                Err(format!("failed to read child output: {error}").into())
            }
            Some(ReaderEvent::Eof) => Ok(PollLine::Disconnected),
            None => Ok(PollLine::Empty),
        }
    }

    pub fn recv_line(&mut self, deadline: Instant) -> Result<String, BoxError> {
        loop {
            match self.poll_line()? {
                PollLine::Line(line) => return Ok(line),
                PollLine::LineTooLong => {
                    return Err("child emitted an overlong protocol line".into());
                }
                PollLine::StreamTooLong => {
                    self.terminate_group();
                    return Err("child stdout exceeded the configured limit".into());
                }
                PollLine::Disconnected => {
                    let detail = self.try_wait()?.map_or_else(
                        || "child closed stdout".to_owned(),
                        |status| format!("child exited with {status}"),
                    );
                    return Err(detail.into());
                }
                PollLine::Empty => {}
            }
            if let Some(status) = self.try_wait()? {
                self.terminate_group();
                self.join_readers()?;
                return match self.poll_line()? {
                    PollLine::Line(line) => Ok(line),
                    PollLine::LineTooLong => Err("child emitted an overlong protocol line".into()),
                    PollLine::StreamTooLong => {
                        Err("child stdout exceeded the configured limit".into())
                    }
                    PollLine::Empty | PollLine::Disconnected => {
                        Err(format!("child exited with {status}").into())
                    }
                };
            }
            if Instant::now() >= deadline {
                return Err("child protocol receive timed out".into());
            }
            thread::sleep(POLL_INTERVAL.min(deadline.saturating_duration_since(Instant::now())));
        }
    }

    pub fn try_wait(&mut self) -> Result<Option<ExitStatus>, BoxError> {
        let status = self.child.try_wait()?;
        if status.is_some() {
            self.reaped = true;
            self.terminate_group();
            self.join_readers()?;
        }
        Ok(status)
    }

    pub fn wait(&mut self, timeout: Duration) -> Result<ExitStatus, BoxError> {
        let deadline = Instant::now() + timeout;
        let status = loop {
            if let Some(status) = self.try_wait()? {
                break status;
            }
            if Instant::now() >= deadline {
                self.terminate_group();
                let status = self.child.wait()?;
                self.reaped = true;
                break status;
            }
            thread::sleep(POLL_INTERVAL.min(deadline.saturating_duration_since(Instant::now())));
        };
        self.terminate_group();
        self.join_readers()?;
        Ok(status)
    }

    pub fn drain_output(&mut self) -> Result<Vec<String>, BoxError> {
        if self.stdout_overflowed() {
            return Err("child stdout exceeded the configured limit".into());
        }
        let mut lines = Vec::new();
        loop {
            match self.stdout_events.recv() {
                Ok(ReaderEvent::Line(bytes)) => lines.push(String::from_utf8(bytes)?),
                Ok(ReaderEvent::LineTooLong) => {
                    return Err("child emitted an overlong protocol line".into());
                }
                Ok(ReaderEvent::Io(error)) => {
                    return Err(format!("failed to read child output: {error}").into());
                }
                Ok(ReaderEvent::Eof) | Err(_) => return Ok(lines),
            }
        }
    }

    fn join_readers(&mut self) -> Result<(), BoxError> {
        if let Some(reader) = self.stdout_reader.take() {
            reader
                .join()
                .map_err(|_| "child stdout reader thread panicked")?;
        }
        if let Some(reader) = self.stderr_reader.take() {
            reader
                .join()
                .map_err(|_| "child stderr reader thread panicked")?;
        }
        Ok(())
    }

    pub fn terminate_group(&mut self) {
        let Some(process_group) = self.process_group.take() else {
            return;
        };
        if kill_process_group(process_group).is_err() {
            let _ = self.child.kill();
        }
    }

    pub fn diagnostic_tail(&self, stderr: bool) -> Result<(String, bool), io::Error> {
        read_tail(
            if stderr {
                &self.stderr_path
            } else {
                &self.stdout_path
            },
            self.diagnostic_tail_bytes,
        )
    }
}

impl Drop for ManagedChild {
    fn drop(&mut self) {
        self.terminate_group();
        if !self.reaped && self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.wait();
        }
        let _ = self.join_readers();
    }
}

fn spawn_stdout_reader<R: Read + Send + 'static>(
    reader: R,
    path: PathBuf,
    max_line_bytes: usize,
    max_stream_bytes: usize,
    overflow: Arc<AtomicBool>,
    sender: mpsc::SyncSender<ReaderEvent>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let result = capture_lines(
            reader,
            &path,
            max_line_bytes,
            max_stream_bytes,
            &overflow,
            |event| match sender.try_send(event) {
                Ok(()) => true,
                Err(mpsc::TrySendError::Full(_)) => {
                    overflow.store(true, Ordering::Relaxed);
                    false
                }
                Err(mpsc::TrySendError::Disconnected(_)) => false,
            },
        );
        if let Err(error) = result {
            let _ = sender.try_send(ReaderEvent::Io(error.to_string()));
        }
        let _ = sender.try_send(ReaderEvent::Eof);
    })
}

fn spawn_stderr_reader<R: Read + Send + 'static>(
    mut reader: R,
    path: PathBuf,
    max_stream_bytes: usize,
    overflow: Arc<AtomicBool>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let Ok(mut output) = File::create(path) else {
            return;
        };
        let mut retained = 0usize;
        let mut buffer = [0u8; READ_CHUNK_BYTES];
        loop {
            match reader.read(&mut buffer) {
                Ok(0) | Err(_) => return,
                Ok(read) => retain_bytes(
                    &mut output,
                    &buffer[..read],
                    &mut retained,
                    max_stream_bytes,
                    &overflow,
                ),
            }
        }
    })
}

fn capture_lines<R: Read>(
    reader: R,
    path: &Path,
    max_line_bytes: usize,
    max_stream_bytes: usize,
    overflow: &AtomicBool,
    mut emit: impl FnMut(ReaderEvent) -> bool,
) -> io::Result<()> {
    let mut reader = BufReader::with_capacity(READ_CHUNK_BYTES, reader);
    let mut output = File::create(path)?;
    let mut retained = 0usize;
    let mut line = Vec::with_capacity(max_line_bytes.min(READ_CHUNK_BYTES));
    let mut too_long = false;
    let mut emitting = true;
    let mut buffer = [0u8; READ_CHUNK_BYTES];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            if emitting && (!line.is_empty() || too_long) {
                let _ = emit(if too_long {
                    ReaderEvent::LineTooLong
                } else {
                    ReaderEvent::Line(line)
                });
            }
            return Ok(());
        }
        retain_bytes(
            &mut output,
            &buffer[..read],
            &mut retained,
            max_stream_bytes,
            overflow,
        );
        for &byte in &buffer[..read] {
            if byte == b'\n' {
                let event = if too_long {
                    ReaderEvent::LineTooLong
                } else {
                    if line.last() == Some(&b'\r') {
                        line.pop();
                    }
                    ReaderEvent::Line(std::mem::take(&mut line))
                };
                too_long = false;
                if emitting {
                    emitting = emit(event);
                }
            } else if !too_long {
                if line.len() < max_line_bytes {
                    line.push(byte);
                } else {
                    line.clear();
                    too_long = true;
                }
            }
        }
    }
}

fn retain_bytes(
    output: &mut File,
    bytes: &[u8],
    retained: &mut usize,
    max_stream_bytes: usize,
    overflow: &AtomicBool,
) {
    let remaining = max_stream_bytes.saturating_sub(*retained);
    let keep = remaining.min(bytes.len());
    if keep > 0 {
        let _ = output.write_all(&bytes[..keep]);
        *retained += keep;
    }
    if keep < bytes.len() {
        overflow.store(true, Ordering::Relaxed);
    }
}

pub fn configure_session(command: &mut Command) {
    let parent_pid = std::process::id();
    // SAFETY: these libc calls run in the forked child before exec and only
    // affect that child. Errors are returned through Command's exec error pipe.
    unsafe {
        command.pre_exec(move || {
            if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                return Err(io::Error::last_os_error());
            }
            if libc::getppid() != parent_pid.cast_signed() {
                return Err(io::Error::from_raw_os_error(libc::ECHILD));
            }
            if libc::setsid() < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

pub fn kill_process_group(process_group: u32) -> Result<(), io::Error> {
    let process_group =
        i32::try_from(process_group).map_err(|_| io::Error::from_raw_os_error(libc::EINVAL))?;
    // SAFETY: a negative PID asks kill(2) to signal the process group. Managed
    // children are session leaders whose process-group ID is their child PID.
    if unsafe { libc::kill(-process_group, libc::SIGKILL) } == 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        Ok(())
    } else {
        Err(error)
    }
}

pub fn read_tail(path: &Path, max_bytes: usize) -> Result<(String, bool), io::Error> {
    let mut file = File::open(path)?;
    let length = file.metadata()?.len();
    let max_bytes_u64 = u64::try_from(max_bytes).unwrap_or(u64::MAX);
    let truncated = length > max_bytes_u64;
    if truncated {
        file.seek(SeekFrom::Start(length - max_bytes_u64))?;
    }
    let mut bytes = Vec::with_capacity(usize::try_from(length.min(max_bytes_u64)).unwrap_or(0));
    file.take(max_bytes_u64).read_to_end(&mut bytes)?;
    Ok((String::from_utf8_lossy(&bytes).into_owned(), truncated))
}

pub fn sha256_file(path: &Path) -> Result<String, BoxError> {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(fs::read(path)?);
    Ok(base16ct::lower::encode_string(digest.as_slice()))
}

#[cfg(test)]
mod tests {
    use std::process::Command;
    use std::time::{Duration, Instant};

    use super::{ManagedChild, OutputLimits};

    fn paths(name: &str) -> (std::path::PathBuf, std::path::PathBuf) {
        let base =
            std::env::temp_dir().join(format!("trevrpc-process-{name}-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&base);
        (base.join("stdout"), base.join("stderr"))
    }

    fn limits() -> OutputLimits {
        OutputLimits {
            max_line_bytes: 16,
            max_stdout_bytes: 32,
            max_stderr_bytes: 32,
            diagnostic_tail_bytes: 16,
        }
    }

    #[test]
    fn reports_timeout_and_overlong_line() {
        let (stdout, stderr) = paths("line");
        let mut command = Command::new("/bin/sh");
        command.args(["-c", "printf '01234567890123456\\n'; sleep 1"]);
        let mut child = ManagedChild::spawn(command, &stdout, &stderr, limits()).expect("spawn");
        let error = child
            .recv_line(Instant::now() + Duration::from_secs(1))
            .expect_err("long line");
        assert!(error.to_string().contains("overlong"));
    }

    #[test]
    fn bounds_streams_while_draining() {
        let (stdout, stderr) = paths("caps");
        let mut command = Command::new("/bin/sh");
        command.args(["-c", "printf '1234567890123456789012345678901234567890\\n'; printf 'abcdefghijklmnopqrstuvwxyz0123456789' >&2"]);
        let mut child = ManagedChild::spawn(command, &stdout, &stderr, limits()).expect("spawn");
        let _ = child.wait(Duration::from_secs(2)).expect("wait");
        assert!(child.stdout_overflowed());
        assert!(child.stderr_overflowed());
        assert!(std::fs::metadata(stdout).expect("stdout").len() <= 32);
        assert!(std::fs::metadata(stderr).expect("stderr").len() <= 32);
    }

    #[test]
    fn short_line_flood_is_bounded_and_reported() {
        let (stdout, stderr) = paths("flood");
        let mut command = Command::new("/bin/sh");
        command.args([
            "-c",
            "i=0; while [ $i -lt 10000 ]; do printf 'x\\n'; i=$((i+1)); done",
        ]);
        let mut child = ManagedChild::spawn(command, &stdout, &stderr, limits()).expect("spawn");
        let deadline = Instant::now() + Duration::from_secs(2);
        let error = loop {
            if let Err(error) = child.recv_line(deadline) {
                break error;
            }
        };
        assert!(error.to_string().contains("stdout exceeded"));
        let _ = child.wait(Duration::from_secs(2)).expect("wait");
        assert!(std::fs::metadata(stdout).expect("stdout").len() <= 32);
    }

    #[test]
    fn termination_kills_descendants_and_closes_inherited_pipes() {
        let (stdout, stderr) = paths("group");
        let pid_path = stdout.with_extension("grandchild-pid");
        let mut command = Command::new("/bin/sh");
        command.args([
            "-c",
            &format!(
                "sleep 60 & child=$!; printf '%s\\n' \"$child\" > {}; printf 'ready\\n'; wait",
                pid_path.display()
            ),
        ]);
        let mut child = ManagedChild::spawn(command, &stdout, &stderr, limits()).expect("spawn");
        assert_eq!(
            child
                .recv_line(Instant::now() + Duration::from_secs(1))
                .expect("ready line"),
            "ready"
        );
        let grandchild = std::fs::read_to_string(&pid_path)
            .expect("read grandchild pid")
            .trim()
            .parse::<u32>()
            .expect("parse grandchild pid");
        child.terminate_group();
        let _ = child.wait(Duration::from_secs(2)).expect("wait after kill");

        let proc_path = std::path::PathBuf::from(format!("/proc/{grandchild}"));
        let deadline = Instant::now() + Duration::from_secs(2);
        while proc_path.exists() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            !proc_path.exists(),
            "grandchild {grandchild} survived group kill"
        );
    }
}
