//! Threads that each own whole QUIC connections; see
//! [`ServerOptions::with_connection_threads`](crate::server::ServerOptions::with_connection_threads).
//!
//! Every operation on a Quinn stream takes its connection's lock, so a connection whose streams
//! run on many threads spends its time handing that lock and its driver between them. A thread
//! here runs a single-threaded runtime: a connection accepted on it keeps its driver, its streams'
//! tasks and its handlers there, and the threads share out the connections.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::runtime::{Builder, Handle};
use tokio::sync::oneshot;

use crate::error::{Error, Result};

/// The connection threads of one server.
pub struct Shards {
    shards: Vec<Shard>,
}

struct Shard {
    handle: Handle,
    /// The connections this thread runs.
    connections: Arc<AtomicUsize>,
    /// Ends the thread when sent or dropped, cancelling whatever its runtime still runs.
    stop: Option<oneshot::Sender<()>>,
}

impl Shards {
    /// Starts `threads` connection threads.
    ///
    /// # Errors
    ///
    /// Returns a transport error when a runtime or thread cannot be started.
    pub fn start(threads: usize) -> Result<Self> {
        let shards = (0..threads)
            .map(|index| {
                let runtime = Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(Error::transport)?;
                let handle = runtime.handle().clone();
                let (stop, stopped) = oneshot::channel::<()>();
                std::thread::Builder::new()
                    .name(format!("trevrpc-connections-{index}"))
                    .spawn(move || {
                        runtime.block_on(async move {
                            let _ = stopped.await;
                        });
                    })
                    .map_err(Error::transport)?;
                Ok(Shard {
                    handle,
                    connections: Arc::default(),
                    stop: Some(stop),
                })
            })
            .collect::<Result<_>>()?;
        Ok(Self { shards })
    }

    /// The thread with the fewest connections, and a guard that counts one more on it until it
    /// is dropped.
    pub fn pick(&self) -> Option<(&Handle, ShardGuard)> {
        let shard = self
            .shards
            .iter()
            .min_by_key(|shard| shard.connections.load(Ordering::Relaxed))?;
        shard.connections.fetch_add(1, Ordering::Relaxed);
        Some((&shard.handle, ShardGuard(Arc::clone(&shard.connections))))
    }
}

impl Drop for Shards {
    fn drop(&mut self) {
        for shard in &mut self.shards {
            if let Some(stop) = shard.stop.take() {
                let _ = stop.send(());
            }
        }
    }
}

/// Counts a connection on its thread while it lives.
pub struct ShardGuard(Arc<AtomicUsize>);

impl Drop for ShardGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::Shards;

    #[tokio::test]
    async fn connections_go_to_the_least_loaded_thread_and_run_there() {
        let shards = Shards::start(2).unwrap();
        let (first, first_guard) = shards.pick().unwrap();
        let (second, second_guard) = shards.pick().unwrap();
        assert_ne!(first.id(), second.id());
        drop(first_guard);
        let (again, _guard) = shards.pick().unwrap();
        assert_eq!(again.id(), first.id());

        let name = again
            .spawn(async { std::thread::current().name().map(str::to_owned) })
            .await
            .unwrap();
        assert!(name.unwrap().starts_with("trevrpc-connections-"));
        drop(second_guard);
    }

    #[test]
    fn no_threads_pick_nothing() {
        assert!(Shards::start(0).unwrap().pick().is_none());
    }
}
