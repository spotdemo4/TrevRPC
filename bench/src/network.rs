use std::net::{IpAddr, Ipv4Addr};
use std::path::Path;
use std::process::Command;

#[cfg(target_os = "linux")]
use std::env;
#[cfg(target_os = "linux")]
use std::fs::File;
#[cfg(target_os = "linux")]
use std::io::{self, BufRead, Write};
#[cfg(target_os = "linux")]
use std::os::fd::{AsRawFd, RawFd};
#[cfg(target_os = "linux")]
use std::os::unix::process::CommandExt;
#[cfg(target_os = "linux")]
use std::process::{Child, ChildStdin, Stdio};
#[cfg(target_os = "linux")]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(target_os = "linux")]
use std::thread;
#[cfg(target_os = "linux")]
use std::time::{Duration, Instant};

use serde::Serialize;

use crate::BoxError;
#[cfg(any(target_os = "linux", test))]
use crate::campaign::LinkCondition;
use crate::campaign::{Network, NetworkBackend};

#[cfg(not(target_os = "linux"))]
const NETNS_LINUX_ONLY_ERROR: &str = "netns network backend is only supported on Linux";
#[cfg(target_os = "linux")]
const OWNER_ENV: &str = "TREVRPC_BENCH_PARENT_NAMESPACES";
#[cfg(target_os = "linux")]
const CLIENT_ADDRESS: Ipv4Addr = Ipv4Addr::new(198, 18, 0, 1);
#[cfg(target_os = "linux")]
const SERVER_ADDRESS: Ipv4Addr = Ipv4Addr::new(198, 18, 0, 2);
#[cfg(target_os = "linux")]
const CLIENT_CIDR: &str = "198.18.0.1/30";
#[cfg(target_os = "linux")]
const SERVER_CIDR: &str = "198.18.0.2/30";
#[cfg(target_os = "linux")]
const CLIENT_INTERFACE: &str = "trc0";
#[cfg(target_os = "linux")]
const SERVER_INTERFACE: &str = "trs0";
#[cfg(target_os = "linux")]
static OWNER_VERIFIED: AtomicBool = AtomicBool::new(false);

#[derive(Clone, Copy)]
pub enum Endpoint {
    Client,
    Server,
}

#[derive(Clone, Debug, Serialize)]
pub struct NetworkSnapshot {
    pub backend: NetworkBackend,
    pub client_address: String,
    pub server_address: String,
    pub client_interface: Option<String>,
    pub server_interface: Option<String>,
    pub client_qdisc: Option<String>,
    pub server_qdisc: Option<String>,
}

pub struct NetworkTopology {
    inner: TopologyInner,
    snapshot: NetworkSnapshot,
}

enum TopologyInner {
    Loopback,
    #[cfg(target_os = "linux")]
    Netns {
        client: NamespaceHolder,
        server: NamespaceHolder,
    },
}

impl NetworkTopology {
    pub fn create(network: &Network) -> Result<Self, BoxError> {
        match network.backend {
            NetworkBackend::Loopback => Ok(Self {
                inner: TopologyInner::Loopback,
                snapshot: NetworkSnapshot {
                    backend: NetworkBackend::Loopback,
                    client_address: "127.0.0.1".to_owned(),
                    server_address: "127.0.0.1".to_owned(),
                    client_interface: None,
                    server_interface: None,
                    client_qdisc: None,
                    server_qdisc: None,
                },
            }),
            NetworkBackend::Netns => Self::create_netns(network),
        }
    }

    #[cfg(target_os = "linux")]
    fn create_netns(network: &Network) -> Result<Self, BoxError> {
        if !OWNER_VERIFIED.load(Ordering::Acquire) {
            return Err("netns backend owner namespace was not verified".into());
        }

        let client = NamespaceHolder::spawn("client")?;
        let server = NamespaceHolder::spawn("server")?;

        run_owner(
            "ip",
            &[
                "link",
                "add",
                CLIENT_INTERFACE,
                "type",
                "veth",
                "peer",
                "name",
                SERVER_INTERFACE,
            ],
            "create benchmark veth pair",
        )?;
        let mut veth_guard = OwnerVethGuard::new();
        run_owner(
            "ip",
            &[
                "link",
                "set",
                CLIENT_INTERFACE,
                "netns",
                &client.id().to_string(),
            ],
            "move client veth into its network namespace",
        )?;
        run_owner(
            "ip",
            &[
                "link",
                "set",
                SERVER_INTERFACE,
                "netns",
                &server.id().to_string(),
            ],
            "move server veth into its network namespace",
        )?;
        veth_guard.disarm();

        configure_endpoint(
            &client,
            CLIENT_INTERFACE,
            CLIENT_CIDR,
            network.mtu,
            &network.client_to_server,
        )?;
        configure_endpoint(
            &server,
            SERVER_INTERFACE,
            SERVER_CIDR,
            network.mtu,
            &network.server_to_client,
        )?;

        verify_route(&client, SERVER_ADDRESS)?;
        verify_route(&server, CLIENT_ADDRESS)?;

        let client_qdisc = inspect_qdisc(&client, CLIENT_INTERFACE)?;
        let server_qdisc = inspect_qdisc(&server, SERVER_INTERFACE)?;
        Ok(Self {
            inner: TopologyInner::Netns { client, server },
            snapshot: NetworkSnapshot {
                backend: NetworkBackend::Netns,
                client_address: CLIENT_ADDRESS.to_string(),
                server_address: SERVER_ADDRESS.to_string(),
                client_interface: Some(CLIENT_INTERFACE.to_owned()),
                server_interface: Some(SERVER_INTERFACE.to_owned()),
                client_qdisc: Some(client_qdisc),
                server_qdisc: Some(server_qdisc),
            },
        })
    }

    #[cfg(not(target_os = "linux"))]
    fn create_netns(_network: &Network) -> Result<Self, BoxError> {
        Err(NETNS_LINUX_ONLY_ERROR.into())
    }

    #[must_use]
    pub fn server_listen(&self) -> String {
        match self.inner {
            TopologyInner::Loopback => "127.0.0.1:0".to_owned(),
            #[cfg(target_os = "linux")]
            TopologyInner::Netns { .. } => format!("{SERVER_ADDRESS}:0"),
        }
    }

    #[must_use]
    pub fn certificate_ips(&self) -> Vec<IpAddr> {
        let ips = vec![IpAddr::V4(Ipv4Addr::LOCALHOST)];
        #[cfg(target_os = "linux")]
        {
            let mut ips = ips;
            if matches!(self.inner, TopologyInner::Netns { .. }) {
                ips.push(IpAddr::V4(SERVER_ADDRESS));
            }
            ips
        }
        #[cfg(not(target_os = "linux"))]
        {
            ips
        }
    }

    #[must_use]
    pub const fn snapshot(&self) -> &NetworkSnapshot {
        &self.snapshot
    }

    pub fn configure_command(&self, command: &mut Command, endpoint: Endpoint) {
        #[cfg(target_os = "linux")]
        {
            let TopologyInner::Netns { client, server } = &self.inner else {
                return;
            };
            let namespace = match endpoint {
                Endpoint::Client => client,
                Endpoint::Server => server,
            };
            enter_namespace_before_exec(command, namespace.file.as_raw_fd());
        }
        #[cfg(not(target_os = "linux"))]
        let _ = (command, endpoint);
    }
}

#[cfg(target_os = "linux")]
pub fn enter_owner_namespace_if_needed(
    network: &Network,
    campaign_path: &Path,
    output: &Path,
) -> Result<(), BoxError> {
    if network.backend != NetworkBackend::Netns {
        return Ok(());
    }

    let executable = env::current_exe()?;
    let parent_namespaces = format!(
        "{}|{}",
        namespace_identity("/proc/self/ns/user")?,
        namespace_identity("/proc/self/ns/net")?
    );
    let error = Command::new("unshare")
        .args(["--user", "--map-root-user", "--net", "--"])
        .arg(executable)
        .arg("__network-run")
        .arg(campaign_path)
        .arg("--out")
        .arg(output)
        .env(OWNER_ENV, parent_namespaces)
        .exec();
    Err(format!("failed to start rootless netns backend with unshare: {error}").into())
}

#[cfg(not(target_os = "linux"))]
pub fn enter_owner_namespace_if_needed(
    network: &Network,
    _campaign_path: &Path,
    _output: &Path,
) -> Result<(), BoxError> {
    if network.backend == NetworkBackend::Netns {
        Err(NETNS_LINUX_ONLY_ERROR.into())
    } else {
        Ok(())
    }
}

#[cfg(target_os = "linux")]
pub fn prepare_owner_namespace(network: &Network) -> Result<(), BoxError> {
    if network.backend != NetworkBackend::Netns {
        return Err("internal network runner requires the netns backend".into());
    }
    verify_owner_namespace()?;
    // SAFETY: the internal runner calls this before capabilities or metrics
    // create any threads. Removing the launcher-only marker keeps it out of
    // every external peer process.
    unsafe { env::remove_var(OWNER_ENV) };
    OWNER_VERIFIED.store(true, Ordering::Release);
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn prepare_owner_namespace(_network: &Network) -> Result<(), BoxError> {
    Err(NETNS_LINUX_ONLY_ERROR.into())
}

#[cfg(target_os = "linux")]
fn verify_owner_namespace() -> Result<(), BoxError> {
    let parent = env::var(OWNER_ENV)
        .map_err(|_| "netns backend was not started through its rootless namespace launcher")?;
    let (parent_user, parent_network) = parent
        .split_once('|')
        .ok_or("netns launcher namespace marker is malformed")?;
    let current_user = namespace_identity("/proc/self/ns/user")?;
    let current_network = namespace_identity("/proc/self/ns/net")?;
    if current_user == parent_user || current_network == parent_network {
        return Err("netns launcher did not isolate both user and network namespaces".into());
    }
    // SAFETY: geteuid has no arguments and only reads process credentials.
    if unsafe { libc::geteuid() } != 0 {
        return Err("netns owner namespace lacks mapped root credentials".into());
    }
    let uid_map = std::fs::read_to_string("/proc/self/uid_map")?;
    let fields = uid_map.split_whitespace().collect::<Vec<_>>();
    if fields.len() != 3 || fields[0] != "0" || fields[2] != "1" {
        return Err("netns backend refuses to configure a non-isolated user mapping".into());
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn namespace_identity(path: &str) -> Result<String, BoxError> {
    Ok(std::fs::read_link(path)
        .map_err(|error| format!("failed to inspect namespace {path}: {error}"))?
        .to_string_lossy()
        .into_owned())
}

#[cfg(target_os = "linux")]
pub fn hold_namespace() -> Result<(), BoxError> {
    let mut line = String::new();
    io::stdin().lock().read_line(&mut line)?;
    if line.trim() != "SHUTDOWN" {
        return Err("network namespace holder closed without SHUTDOWN".into());
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn hold_namespace() -> Result<(), BoxError> {
    Err(NETNS_LINUX_ONLY_ERROR.into())
}

#[cfg(target_os = "linux")]
struct NamespaceHolder {
    child: Child,
    stdin: Option<ChildStdin>,
    file: File,
}

#[cfg(target_os = "linux")]
impl NamespaceHolder {
    fn spawn(role: &str) -> Result<Self, BoxError> {
        let executable = env::current_exe()?;
        let parent_pid = std::process::id();
        let mut command = Command::new(executable);
        command
            .arg("__network-holder")
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        // SAFETY: pre_exec runs after fork and before exec. These libc calls do
        // not access memory shared with another thread, and failures are
        // returned to Command::spawn through its exec error pipe.
        unsafe {
            command.pre_exec(move || {
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                    return Err(io::Error::last_os_error());
                }
                if libc::getppid() != parent_pid.cast_signed() {
                    return Err(io::Error::from_raw_os_error(libc::ECHILD));
                }
                if libc::unshare(libc::CLONE_NEWNET) != 0 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut child = command
            .spawn()
            .map_err(|error| format!("failed to create {role} network namespace: {error}"))?;
        let stdin = child
            .stdin
            .take()
            .ok_or("network namespace holder stdin was not piped")?;
        let namespace_path = format!("/proc/{}/ns/net", child.id());
        let file = File::open(&namespace_path).map_err(|error| {
            format!("failed to open {role} network namespace {namespace_path}: {error}")
        })?;
        Ok(Self {
            child,
            stdin: Some(stdin),
            file,
        })
    }

    fn id(&self) -> u32 {
        self.child.id()
    }
}

#[cfg(target_os = "linux")]
struct OwnerVethGuard {
    active: bool,
}

#[cfg(target_os = "linux")]
impl OwnerVethGuard {
    const fn new() -> Self {
        Self { active: true }
    }

    const fn disarm(&mut self) {
        self.active = false;
    }
}

#[cfg(target_os = "linux")]
impl Drop for OwnerVethGuard {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let _ = run_owner(
            "ip",
            &["link", "delete", CLIENT_INTERFACE],
            "roll back client veth",
        );
        let _ = run_owner(
            "ip",
            &["link", "delete", SERVER_INTERFACE],
            "roll back server veth",
        );
    }
}

#[cfg(target_os = "linux")]
impl Drop for NamespaceHolder {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_some() {
            return;
        }
        if let Some(mut stdin) = self.stdin.take() {
            let _ = writeln!(stdin, "SHUTDOWN");
            let _ = stdin.flush();
        }
        let deadline = Instant::now() + Duration::from_millis(100);
        while Instant::now() < deadline {
            if self.child.try_wait().ok().flatten().is_some() {
                return;
            }
            thread::sleep(Duration::from_millis(5));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[cfg(target_os = "linux")]
fn configure_endpoint(
    namespace: &NamespaceHolder,
    interface: &str,
    cidr: &str,
    mtu: u32,
    condition: &LinkCondition,
) -> Result<(), BoxError> {
    run_in_namespace(
        namespace,
        "ip",
        &["link", "set", "lo", "up"],
        "enable endpoint loopback interface",
    )?;
    run_in_namespace(
        namespace,
        "ip",
        &["link", "set", "dev", interface, "mtu", &mtu.to_string()],
        "set endpoint MTU",
    )?;
    run_in_namespace(
        namespace,
        "ip",
        &["address", "add", cidr, "dev", interface],
        "assign endpoint address",
    )?;
    run_in_namespace(
        namespace,
        "ip",
        &["link", "set", interface, "up"],
        "enable endpoint veth interface",
    )?;

    let arguments = netem_arguments(interface, condition);
    if !arguments.is_empty() {
        run_in_namespace(
            namespace,
            "tc",
            &arguments.iter().map(String::as_str).collect::<Vec<_>>(),
            "configure endpoint network emulation",
        )?;
    }
    Ok(())
}

#[cfg(any(target_os = "linux", test))]
fn netem_arguments(interface: &str, condition: &LinkCondition) -> Vec<String> {
    if condition.is_unrestricted() {
        return Vec::new();
    }
    let mut arguments = vec![
        "qdisc".to_owned(),
        "replace".to_owned(),
        "dev".to_owned(),
        interface.to_owned(),
        "root".to_owned(),
        "netem".to_owned(),
    ];
    if let Some(limit) = condition.queue_packets {
        arguments.extend(["limit".to_owned(), limit.to_string()]);
    }
    if condition.delay_ms > 0 {
        arguments.extend(["delay".to_owned(), format!("{}ms", condition.delay_ms)]);
        if condition.jitter_ms > 0 {
            arguments.push(format!("{}ms", condition.jitter_ms));
        }
    }
    if condition.loss_percent > 0.0 {
        arguments.extend([
            "loss".to_owned(),
            "random".to_owned(),
            format!("{}%", condition.loss_percent),
        ]);
    }
    if let Some(rate) = condition.rate_mbit {
        arguments.extend(["rate".to_owned(), format!("{rate}mbit")]);
    }
    arguments
}

#[cfg(target_os = "linux")]
fn verify_route(namespace: &NamespaceHolder, destination: Ipv4Addr) -> Result<(), BoxError> {
    run_in_namespace(
        namespace,
        "ip",
        &["route", "get", &destination.to_string()],
        "verify benchmark endpoint route",
    )?;
    Ok(())
}

#[cfg(target_os = "linux")]
fn inspect_qdisc(namespace: &NamespaceHolder, interface: &str) -> Result<String, BoxError> {
    run_in_namespace(
        namespace,
        "tc",
        &["qdisc", "show", "dev", interface],
        "inspect endpoint qdisc",
    )
}

#[cfg(target_os = "linux")]
fn run_owner(program: &str, arguments: &[&str], description: &str) -> Result<String, BoxError> {
    run_command(Command::new(program).args(arguments), description)
}

#[cfg(target_os = "linux")]
fn run_in_namespace(
    namespace: &NamespaceHolder,
    program: &str,
    arguments: &[&str],
    description: &str,
) -> Result<String, BoxError> {
    let mut command = Command::new(program);
    command.args(arguments);
    enter_namespace_before_exec(&mut command, namespace.file.as_raw_fd());
    run_command(&mut command, description)
}

#[cfg(target_os = "linux")]
fn run_command(command: &mut Command, description: &str) -> Result<String, BoxError> {
    let output = command
        .output()
        .map_err(|error| format!("failed to launch command to {description}: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "failed to {description}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    Ok(String::from_utf8(output.stdout)?.trim().to_owned())
}

#[cfg(target_os = "linux")]
fn enter_namespace_before_exec(command: &mut Command, namespace_fd: RawFd) {
    let parent_pid = std::process::id();
    // SAFETY: setns is called in the forked child immediately before exec and
    // only changes that child's network namespace. The parent-death signal
    // prevents namespace peers and setup commands from surviving the controller.
    unsafe {
        command.pre_exec(move || {
            if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                return Err(io::Error::last_os_error());
            }
            if libc::getppid() != parent_pid.cast_signed() {
                return Err(io::Error::from_raw_os_error(libc::ECHILD));
            }
            if libc::setns(namespace_fd, libc::CLONE_NEWNET) != 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

#[cfg(test)]
mod tests {
    use super::netem_arguments;
    #[cfg(not(target_os = "linux"))]
    use super::{NETNS_LINUX_ONLY_ERROR, NetworkTopology};
    use crate::campaign::LinkCondition;
    #[cfg(not(target_os = "linux"))]
    use crate::campaign::{Network, NetworkBackend};

    #[test]
    fn omits_netem_for_unrestricted_links() {
        assert!(netem_arguments("test0", &LinkCondition::default()).is_empty());
    }

    #[test]
    fn builds_directional_netem_arguments() {
        let arguments = netem_arguments(
            "test0",
            &LinkCondition {
                delay_ms: 15,
                jitter_ms: 2,
                loss_percent: 0.1,
                rate_mbit: Some(100),
                queue_packets: Some(500),
            },
        );
        assert_eq!(
            arguments,
            [
                "qdisc", "replace", "dev", "test0", "root", "netem", "limit", "500", "delay",
                "15ms", "2ms", "loss", "random", "0.1%", "rate", "100mbit",
            ]
        );
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn netns_backend_reports_linux_requirement() {
        let network = Network {
            backend: NetworkBackend::Netns,
            ..Network::default()
        };
        let Err(error) = NetworkTopology::create(&network) else {
            panic!("netns must be rejected");
        };
        assert_eq!(error.to_string(), NETNS_LINUX_ONLY_ERROR);
    }
}
