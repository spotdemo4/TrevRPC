//! A cell that holds the latest value of something that rarely changes and is waited on often.
//!
//! This is the part of [`tokio::sync::watch`] that the request pump needs, with a cheaper wait. A
//! `watch` receiver registers in a shared wait list under a lock every time it waits for a change,
//! and again to leave it, which a task that waits between every message of a stream does twice
//! per message. This cell has one receiver and wakes it through a lock-free waker slot, so waiting
//! costs a few atomic operations and the value's lock is taken only when the value is read.

use std::future::poll_fn;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;

use futures_util::task::AtomicWaker;

use crate::sync::MutexExt as _;

/// The sender was dropped.
#[derive(Debug, Eq, PartialEq)]
pub struct Closed;

struct Shared<T> {
    value: Mutex<T>,
    /// Counts the values sent, so a receiver can tell whether it has seen the latest.
    version: AtomicUsize,
    closed: AtomicBool,
    waker: AtomicWaker,
}

/// Creates a cell holding `initial`.
pub fn channel<T: Copy>(initial: T) -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(Shared {
        value: Mutex::new(initial),
        version: AtomicUsize::new(0),
        closed: AtomicBool::new(false),
        waker: AtomicWaker::new(),
    });
    (
        Sender {
            shared: Arc::clone(&shared),
        },
        Receiver { shared, seen: 0 },
    )
}

pub struct Sender<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Sender<T> {
    /// Replaces the value and wakes the receiver.
    pub fn send(&self, value: T) {
        *self.shared.value.lock_unpoisoned() = value;
        self.shared.version.fetch_add(1, Ordering::Release);
        self.shared.waker.wake();
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        self.shared.closed.store(true, Ordering::Release);
        self.shared.waker.wake();
    }
}

/// The only receiver of a cell: it keeps a single waker.
pub struct Receiver<T> {
    shared: Arc<Shared<T>>,
    /// The version the receiver last looked at.
    seen: usize,
}

impl<T: Copy> Receiver<T> {
    /// Waits for a value newer than the one last looked at, and counts the latest as looked at.
    /// Fails once the sender is gone and nothing newer was sent.
    pub async fn changed(&mut self) -> Result<(), Closed> {
        poll_fn(|cx| {
            // Register before looking, so a change between the two wakes this task.
            self.shared.waker.register(cx.waker());
            let version = self.shared.version.load(Ordering::Acquire);
            if version != self.seen {
                self.seen = version;
                return Poll::Ready(Ok(()));
            }
            if self.shared.closed.load(Ordering::Acquire) {
                // A value sent just before the sender dropped is newer than the first load.
                let version = self.shared.version.load(Ordering::Acquire);
                if version != self.seen {
                    self.seen = version;
                    return Poll::Ready(Ok(()));
                }
                return Poll::Ready(Err(Closed));
            }
            Poll::Pending
        })
        .await
    }

    /// The latest value, counted as looked at.
    pub fn borrow_and_update(&mut self) -> T {
        // The version comes first: a send in between leaves the receiver with an older version
        // than value, which `changed` reports once more instead of missing.
        let version = self.shared.version.load(Ordering::Acquire);
        let value = *self.shared.value.lock_unpoisoned();
        self.seen = version;
        value
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{Closed, channel};

    #[tokio::test]
    async fn a_waiting_receiver_wakes_for_a_new_value() {
        let (sender, mut receiver) = channel(None);
        let waiting = tokio::spawn(async move {
            receiver.changed().await.unwrap();
            receiver.borrow_and_update()
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!waiting.is_finished());

        sender.send(Some(7));

        assert_eq!(waiting.await.unwrap(), Some(7));
    }

    #[tokio::test]
    async fn a_value_looked_at_is_not_a_change() {
        let (sender, mut receiver) = channel(0);
        sender.send(1);
        assert_eq!(receiver.borrow_and_update(), 1);

        assert!(
            tokio::time::timeout(Duration::from_millis(20), receiver.changed())
                .await
                .is_err()
        );
        sender.send(2);
        receiver.changed().await.unwrap();
        assert_eq!(receiver.borrow_and_update(), 2);
    }

    #[tokio::test]
    async fn several_values_sent_before_the_wait_are_one_change() {
        let (sender, mut receiver) = channel(0);
        sender.send(1);
        sender.send(2);

        receiver.changed().await.unwrap();

        assert_eq!(receiver.borrow_and_update(), 2);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), receiver.changed())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn dropping_the_sender_ends_the_wait_after_the_last_value() {
        let (sender, mut receiver) = channel(0);
        sender.send(5);
        drop(sender);

        receiver.changed().await.unwrap();
        assert_eq!(receiver.borrow_and_update(), 5);
        assert_eq!(receiver.changed().await, Err(Closed));
    }

    #[tokio::test]
    async fn dropping_the_sender_wakes_a_waiting_receiver() {
        let (sender, mut receiver) = channel(0);
        let waiting = tokio::spawn(async move { receiver.changed().await });
        tokio::time::sleep(Duration::from_millis(10)).await;

        drop(sender);

        assert_eq!(waiting.await.unwrap(), Err(Closed));
    }
}
