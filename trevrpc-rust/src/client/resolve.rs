//! Where a channel's connections go: one address through the caller's endpoint, or a host name
//! resolved before every dial, whose addresses are tried Happy Eyeballs style (RFC 8305).

use std::future::Future;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::StreamExt as _;
use futures_util::stream::FuturesUnordered;

use crate::sync::MutexExt as _;
use crate::{Error, Result, Status};

/// How long an attempt to connect to one address has before the next address is tried alongside
/// it, the Connection Attempt Delay RFC 8305 recommends.
pub const CONNECTION_ATTEMPT_DELAY: Duration = Duration::from_millis(250);

/// Where a channel's connections go.
#[derive(Clone)]
pub enum Target {
    /// One address, through the caller's endpoint.
    Addr {
        endpoint: quinn::Endpoint,
        addr: SocketAddr,
    },
    /// A host name, resolved before every dial.
    Host(Arc<HostTarget>),
}

/// A host name and port, with an endpoint for each address family, bound when an address of that
/// family is first dialed.
pub struct HostTarget {
    host: String,
    port: u16,
    quic: quinn::ClientConfig,
    v4: Mutex<Option<quinn::Endpoint>>,
    v6: Mutex<Option<quinn::Endpoint>>,
}

impl HostTarget {
    /// `quic` is every endpoint's client configuration.
    pub fn new(host: String, port: u16, quic: quinn::ClientConfig) -> Self {
        Self {
            host,
            port,
            quic,
            v4: Mutex::new(None),
            v6: Mutex::new(None),
        }
    }

    /// The endpoint for `addr`'s address family. Binding fails where the family is unavailable,
    /// such as IPv6 in some sandboxes; it is tried again on the next dial.
    fn endpoint(&self, addr: SocketAddr) -> Result<quinn::Endpoint> {
        let (slot, local) = if addr.is_ipv4() {
            (&self.v4, SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)))
        } else {
            (&self.v6, SocketAddr::from((Ipv6Addr::UNSPECIFIED, 0)))
        };
        let mut slot = slot.lock_unpoisoned();
        if let Some(endpoint) = &*slot {
            return Ok(endpoint.clone());
        }
        let mut endpoint = quinn::Endpoint::client(local).map_err(Error::transport)?;
        endpoint.set_default_client_config(self.quic.clone());
        *slot = Some(endpoint.clone());
        drop(slot);
        Ok(endpoint)
    }
}

impl Target {
    /// The addresses to dial, in the order to try them.
    pub async fn resolve(&self) -> Result<Vec<SocketAddr>> {
        match self {
            Self::Addr { addr, .. } => Ok(vec![*addr]),
            Self::Host(host) => {
                let addrs = tokio::net::lookup_host((host.host.as_str(), host.port))
                    .await
                    .map_err(|error| {
                        Error::from(Status::unavailable(format!(
                            "cannot resolve {}: {error}",
                            host.host
                        )))
                    })?;
                let addrs = interleave(addrs);
                if addrs.is_empty() {
                    return Err(Error::from(Status::unavailable(format!(
                        "{} has no addresses",
                        host.host
                    ))));
                }
                Ok(addrs)
            }
        }
    }

    /// Waits until no endpoint of the target has a connection.
    pub async fn wait_idle(&self) {
        match self {
            Self::Addr { endpoint, .. } => endpoint.wait_idle().await,
            Self::Host(host) => {
                let endpoints = [&host.v4, &host.v6]
                    .into_iter()
                    .filter_map(|slot| slot.lock_unpoisoned().clone())
                    .collect::<Vec<_>>();
                futures_util::future::join_all(endpoints.iter().map(quinn::Endpoint::wait_idle))
                    .await;
            }
        }
    }

    /// The endpoint that dials `addr`.
    pub fn endpoint(&self, addr: SocketAddr) -> Result<quinn::Endpoint> {
        match self {
            Self::Addr { endpoint, .. } => Ok(endpoint.clone()),
            Self::Host(host) => host.endpoint(addr),
        }
    }
}

/// The address of a channel's most recent connection, and the endpoint of its most recent QUIC
/// connection.
#[derive(Clone)]
pub struct Route {
    pub(crate) addr: SocketAddr,
    pub(crate) endpoint: Option<quinn::Endpoint>,
}

/// Orders resolved addresses for Happy Eyeballs: without duplicates, alternating between address
/// families, starting with the family of the resolver's first, most preferred address.
pub fn interleave(addrs: impl IntoIterator<Item = SocketAddr>) -> Vec<SocketAddr> {
    let mut first = Vec::new();
    let mut second = Vec::new();
    let mut first_is_ipv4 = None;
    for addr in addrs {
        if first.contains(&addr) || second.contains(&addr) {
            continue;
        }
        if *first_is_ipv4.get_or_insert_with(|| addr.is_ipv4()) == addr.is_ipv4() {
            first.push(addr);
        } else {
            second.push(addr);
        }
    }
    let mut ordered = Vec::with_capacity(first.len().saturating_add(second.len()));
    let mut second = second.into_iter();
    for addr in first {
        ordered.push(addr);
        ordered.extend(second.next());
    }
    ordered.extend(second);
    ordered
}

/// Connects to the first of `addrs` to answer. Attempts start [`CONNECTION_ATTEMPT_DELAY`] apart,
/// or as soon as the one before fails; the first to connect wins, and the others are dropped.
/// When every attempt fails, a single address's error is returned as it is.
pub async fn happy_eyeballs<T, F, Fut>(
    addrs: &[SocketAddr],
    mut connect: F,
) -> Result<(SocketAddr, T)>
where
    F: FnMut(SocketAddr) -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let mut pending = addrs.iter().copied().peekable();
    let mut attempts = FuturesUnordered::new();
    let mut failures = Vec::new();
    let mut start = |attempts: &mut FuturesUnordered<_>, addr: SocketAddr| {
        let attempt = connect(addr);
        attempts.push(async move { (addr, attempt.await) });
    };
    if let Some(addr) = pending.next() {
        start(&mut attempts, addr);
    }
    let mut more = pending.peek().is_some();
    while !attempts.is_empty() {
        tokio::select! {
            Some((addr, result)) = attempts.next() => match result {
                Ok(connection) => return Ok((addr, connection)),
                Err(error) => {
                    failures.push((addr, error));
                    if let Some(addr) = pending.next() {
                        start(&mut attempts, addr);
                    }
                }
            },
            () = tokio::time::sleep(CONNECTION_ATTEMPT_DELAY), if more => {
                if let Some(addr) = pending.next() {
                    start(&mut attempts, addr);
                }
            }
        }
        more = pending.peek().is_some();
    }
    Err(every_attempt_failed(failures))
}

fn every_attempt_failed(mut failures: Vec<(SocketAddr, Error)>) -> Error {
    if failures.len() == 1
        && let Some((_, error)) = failures.pop()
    {
        return error;
    }
    if failures.is_empty() {
        return Error::from(Status::unavailable("no address to connect to"));
    }
    let failures = failures
        .iter()
        .map(|(addr, error)| format!("{addr}: {error}"))
        .collect::<Vec<_>>()
        .join("; ");
    Error::from(Status::unavailable(format!(
        "transport unavailable: {failures}"
    )))
}

#[cfg(test)]
mod tests {
    use super::{CONNECTION_ATTEMPT_DELAY, happy_eyeballs, interleave};
    use crate::{Error, Status};
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::time::Instant;

    fn addr(text: &str) -> SocketAddr {
        text.parse()
            .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 0)))
    }

    #[test]
    fn interleave_alternates_families_from_the_first() {
        let addrs = [
            "[::1]:1",
            "[::2]:1",
            "[::1]:1",
            "10.0.0.1:1",
            "10.0.0.2:1",
            "10.0.0.3:1",
        ]
        .map(addr);
        let ordered = [
            "[::1]:1",
            "10.0.0.1:1",
            "[::2]:1",
            "10.0.0.2:1",
            "10.0.0.3:1",
        ]
        .map(addr);
        assert_eq!(interleave(addrs), ordered);

        let addrs = ["10.0.0.1:1", "[::1]:1", "[::2]:1", "[::3]:1"].map(addr);
        let ordered = ["10.0.0.1:1", "[::1]:1", "[::2]:1", "[::3]:1"].map(addr);
        assert_eq!(interleave(addrs), ordered);
    }

    /// Records when each address was attempted, relative to the start, and answers after its
    /// delay, connecting when it has no error.
    async fn race(
        plan: &[(&str, Duration, bool)],
    ) -> (crate::Result<(SocketAddr, ())>, Vec<(SocketAddr, Duration)>) {
        let started = Instant::now();
        let attempts = Arc::new(Mutex::new(Vec::new()));
        let addrs = plan.iter().map(|(text, ..)| addr(text)).collect::<Vec<_>>();
        let plan = plan
            .iter()
            .map(|&(text, delay, connects)| (addr(text), (delay, connects)))
            .collect::<std::collections::HashMap<_, _>>();
        let result = happy_eyeballs(&addrs, |addr| {
            attempts
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push((addr, started.elapsed()));
            let (delay, connects) = plan.get(&addr).copied().unwrap_or_default();
            async move {
                tokio::time::sleep(delay).await;
                if connects {
                    Ok(())
                } else {
                    Err(Error::from(Status::unavailable(format!("{addr} refused"))))
                }
            }
        })
        .await;
        let attempts = std::mem::take(
            &mut *attempts
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        (result, attempts)
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_attempt_starts_the_next_at_once() {
        let ms = Duration::from_millis;
        let (result, attempts) =
            race(&[("[::1]:1", ms(10), false), ("10.0.0.1:1", ms(10), true)]).await;
        assert_eq!(result.ok().map(|(addr, ())| addr), Some(addr("10.0.0.1:1")));
        assert_eq!(
            attempts,
            [(addr("[::1]:1"), ms(0)), (addr("10.0.0.1:1"), ms(10))]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_slow_attempt_is_joined_after_the_delay_and_loses() {
        let ms = Duration::from_millis;
        let (result, attempts) = race(&[
            ("[::1]:1", ms(10_000), true),
            ("10.0.0.1:1", ms(10), true),
            ("10.0.0.2:1", ms(10), true),
        ])
        .await;
        assert_eq!(result.ok().map(|(addr, ())| addr), Some(addr("10.0.0.1:1")));
        assert_eq!(
            attempts,
            [
                (addr("[::1]:1"), ms(0)),
                (addr("10.0.0.1:1"), CONNECTION_ATTEMPT_DELAY)
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn every_failure_is_reported() {
        let ms = Duration::from_millis;
        let (result, _) = race(&[("10.0.0.1:1", ms(1), false)]).await;
        let message = result
            .err()
            .map(|error| error.to_string())
            .unwrap_or_default();
        assert!(message.contains("10.0.0.1:1 refused"), "{message}");
        assert!(!message.contains("transport unavailable"), "{message}");

        let (result, _) = race(&[("10.0.0.1:1", ms(1), false), ("[::1]:1", ms(1), false)]).await;
        let message = result
            .err()
            .map(|error| error.to_string())
            .unwrap_or_default();
        assert!(message.contains("10.0.0.1:1 refused"), "{message}");
        assert!(message.contains("[::1]:1 refused"), "{message}");
    }
}
