//! A queue that hands values from one task to another, bounded by count and by bytes.
//!
//! A call's messages cross a task boundary on both sides: a client's `send` hands them to the task
//! that writes the request stream, and the server's request pump hands them to the handler. A
//! channel that holds one value makes the two tasks wake each other for every message. This queue
//! lets the producer run ahead of the consumer by up to [`MESSAGES`] values, and the consumer
//! take them in a row, while still bounding what is buffered: values are charged their size in
//! bytes, so a few large messages fill the queue as fast as many small ones, and a message larger
//! than the byte limit is queued alone.

use std::collections::VecDeque;
use std::future::{Future, poll_fn};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use crate::sync::MutexExt as _;

/// What a queue may hold before its producers wait.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// The most values queued.
    pub values: usize,
    /// The queued bytes at which producers wait. The value that crosses it is still queued, so a
    /// queue holds up to this many bytes plus one value.
    pub bytes: usize,
}

/// The limits of the queues that carry a call's messages between tasks.
///
/// The count matches [`crate::framed::MESSAGE_FRAME_BATCH`], so a writer can fill a whole batch
/// of frames from one wake-up. The byte limit keeps a peer's large messages from piling up: a
/// queue never holds more than this plus one message.
pub const MESSAGES: Limits = Limits {
    values: crate::framed::MESSAGE_FRAME_BATCH,
    bytes: 64 * 1024,
};

/// The other end of the queue is gone.
#[derive(Debug, Eq, PartialEq)]
pub struct Closed;

/// Why a value was not queued by [`Sender::try_send`].
#[derive(Debug, Eq, PartialEq)]
pub enum TrySendError<T> {
    /// The queue is at its limits; the value is handed back.
    Full(T),
    /// The receiver is gone.
    Closed(T),
}

struct State<T> {
    /// Queued values with their sizes.
    queue: VecDeque<(T, usize)>,
    queued_bytes: usize,
    senders: usize,
    receiver_open: bool,
    receiver_waker: Option<Waker>,
    /// Producers waiting for room.
    sender_wakers: Vec<Waker>,
}

impl<T> State<T> {
    fn has_room(&self, limits: Limits) -> bool {
        self.queue.len() < limits.values && self.queued_bytes < limits.bytes
    }

    fn push(&mut self, value: T, size: usize) -> Option<Waker> {
        self.queued_bytes = self.queued_bytes.saturating_add(size);
        self.queue.push_back((value, size));
        self.receiver_waker.take()
    }
}

struct Shared<T> {
    state: Mutex<State<T>>,
    limits: Limits,
}

/// Creates a queue bounded by `limits`.
pub fn channel<T>(limits: Limits) -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            queue: VecDeque::new(),
            queued_bytes: 0,
            senders: 1,
            receiver_open: true,
            receiver_waker: None,
            sender_wakers: Vec::new(),
        }),
        limits,
    });
    (
        Sender {
            shared: Arc::clone(&shared),
        },
        Receiver { shared },
    )
}

/// The producing end. Cloning it adds a producer; the queue ends when the last one is dropped.
pub struct Sender<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Sender<T> {
    /// Queues `value`, which weighs `size` bytes, if there is room.
    pub fn try_send(&self, value: T, size: usize) -> Result<(), TrySendError<T>> {
        let mut state = self.shared.state.lock_unpoisoned();
        if !state.receiver_open {
            return Err(TrySendError::Closed(value));
        }
        if !state.has_room(self.shared.limits) {
            return Err(TrySendError::Full(value));
        }
        let waker = state.push(value, size);
        drop(state);
        if let Some(waker) = waker {
            waker.wake();
        }
        Ok(())
    }

    /// Queues `value`, which weighs `size` bytes, once there is room.
    ///
    /// Dropping the future before it completes drops `value`.
    pub async fn send(&self, value: T, size: usize) -> Result<(), Closed> {
        let mut value = Some(value);
        poll_fn(|cx| {
            let mut state = self.shared.state.lock_unpoisoned();
            if !state.receiver_open {
                return Poll::Ready(Err(Closed));
            }
            if !state.has_room(self.shared.limits) {
                register(&mut state.sender_wakers, cx);
                return Poll::Pending;
            }
            let Some(value) = value.take() else {
                return Poll::Ready(Err(Closed));
            };
            let waker = state.push(value, size);
            drop(state);
            if let Some(waker) = waker {
                waker.wake();
            }
            Poll::Ready(Ok(()))
        })
        .await
    }

    /// Waits until there is room for a value, so the caller can queue one without waiting.
    ///
    /// The permit holds no space. It stands for the room that existed when it was issued, which
    /// only the one producer that asked can use, so a queue must have a single producer to
    /// reserve.
    pub fn reserve(&self) -> impl Future<Output = Result<Permit<'_, T>, Closed>> {
        poll_fn(|cx| {
            let mut state = self.shared.state.lock_unpoisoned();
            if !state.receiver_open {
                return Poll::Ready(Err(Closed));
            }
            if state.has_room(self.shared.limits) {
                return Poll::Ready(Ok(Permit { sender: self }));
            }
            register(&mut state.sender_wakers, cx);
            Poll::Pending
        })
    }
}

/// Remembers the waker of a waiting producer unless it is there already.
fn register(wakers: &mut Vec<Waker>, cx: &Context<'_>) {
    if !wakers.iter().any(|waker| waker.will_wake(cx.waker())) {
        wakers.push(cx.waker().clone());
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        let mut state = self.shared.state.lock_unpoisoned();
        state.senders = state.senders.saturating_add(1);
        drop(state);
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let mut state = self.shared.state.lock_unpoisoned();
        state.senders = state.senders.saturating_sub(1);
        let waker = if state.senders == 0 {
            state.receiver_waker.take()
        } else {
            None
        };
        drop(state);
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

/// Room for one value, from [`Sender::reserve`].
pub struct Permit<'a, T> {
    sender: &'a Sender<T>,
}

impl<T> Permit<'_, T> {
    /// Queues `value`, which weighs `size` bytes. It is dropped if the receiver is gone.
    pub fn send(self, value: T, size: usize) {
        let mut state = self.sender.shared.state.lock_unpoisoned();
        if !state.receiver_open {
            return;
        }
        let waker = state.push(value, size);
        drop(state);
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

/// The consuming end. Dropping it drops the queued values and fails the producers.
pub struct Receiver<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Receiver<T> {
    /// Takes the oldest value, `None` once every sender is gone and the queue is empty.
    #[expect(
        clippy::needless_pass_by_ref_mut,
        reason = "one consumer polls at a time: the queue keeps a single receiver waker"
    )]
    pub fn poll_recv(&mut self, cx: &Context<'_>) -> Poll<Option<T>> {
        self.poll_pop(Some(cx))
    }

    /// Takes the oldest value if one is queued, without waiting.
    #[expect(
        clippy::needless_pass_by_ref_mut,
        reason = "one consumer takes values at a time, like `poll_recv`"
    )]
    pub fn try_recv(&mut self) -> Option<T> {
        match self.poll_pop(None) {
            Poll::Ready(value) => value,
            Poll::Pending => None,
        }
    }

    /// Takes the oldest value, remembering `cx`'s waker if there is none and senders remain.
    fn poll_pop(&self, cx: Option<&Context<'_>>) -> Poll<Option<T>> {
        let mut state = self.shared.state.lock_unpoisoned();
        let Some((value, size)) = state.queue.pop_front() else {
            if state.senders == 0 {
                return Poll::Ready(None);
            }
            if let Some(cx) = cx {
                state.receiver_waker = Some(cx.waker().clone());
            }
            return Poll::Pending;
        };
        state.queued_bytes = state.queued_bytes.saturating_sub(size);
        let wakers = if state.has_room(self.shared.limits) {
            std::mem::take(&mut state.sender_wakers)
        } else {
            Vec::new()
        };
        drop(state);
        for waker in wakers {
            waker.wake();
        }
        Poll::Ready(Some(value))
    }

    /// Takes the oldest value, waiting for one.
    pub async fn recv(&mut self) -> Option<T> {
        poll_fn(|cx| self.poll_recv(cx)).await
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let mut state = self.shared.state.lock_unpoisoned();
        state.receiver_open = false;
        state.queued_bytes = 0;
        let queue = std::mem::take(&mut state.queue);
        let wakers = std::mem::take(&mut state.sender_wakers);
        drop(state);
        drop(queue);
        for waker in wakers {
            waker.wake();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{Closed, Limits, TrySendError, channel};

    const LIMITS: Limits = Limits {
        values: 3,
        bytes: 100,
    };

    #[tokio::test]
    async fn values_arrive_in_order_and_the_end_follows_the_last_sender() {
        let (sender, mut receiver) = channel(LIMITS);
        let second = sender.clone();
        sender.send(1, 1).await.unwrap();
        second.send(2, 1).await.unwrap();
        drop(sender);
        drop(second);

        assert_eq!(receiver.recv().await, Some(1));
        assert_eq!(receiver.recv().await, Some(2));
        assert_eq!(receiver.recv().await, None);
    }

    #[test]
    fn try_recv_takes_queued_values_without_waiting_and_frees_room() {
        let (sender, mut receiver) = channel(LIMITS);
        assert_eq!(receiver.try_recv(), None);
        for value in 0..3 {
            sender.try_send(value, 1).unwrap();
        }
        assert_eq!(sender.try_send(3, 1), Err(TrySendError::Full(3)));

        assert_eq!(receiver.try_recv(), Some(0));

        sender.try_send(3, 1).unwrap();
        assert_eq!(receiver.try_recv(), Some(1));
        assert_eq!(receiver.try_recv(), Some(2));
        assert_eq!(receiver.try_recv(), Some(3));
        assert_eq!(receiver.try_recv(), None);
    }

    #[test]
    fn the_value_limit_holds_a_send_back() {
        let (sender, _receiver) = channel(LIMITS);
        for value in 0..3 {
            sender.try_send(value, 1).unwrap();
        }

        assert_eq!(sender.try_send(3, 1), Err(TrySendError::Full(3)));
    }

    #[tokio::test]
    async fn the_byte_limit_holds_a_send_back_but_a_large_value_is_queued_alone() {
        let (sender, mut receiver) = channel(LIMITS);
        sender.try_send("large", 5000).unwrap();

        assert_eq!(sender.try_send("next", 1), Err(TrySendError::Full("next")));
        assert_eq!(receiver.recv().await, Some("large"));
        sender.try_send("next", 1).unwrap();
    }

    #[tokio::test]
    async fn the_last_value_may_cross_the_byte_limit() {
        let (sender, _receiver) = channel(LIMITS);
        sender.try_send(0, 60).unwrap();
        sender.try_send(1, 60).unwrap();

        assert_eq!(sender.try_send(2, 1), Err(TrySendError::Full(2)));
    }

    #[tokio::test]
    async fn a_waiting_send_completes_when_the_receiver_takes_a_value() {
        let (sender, mut receiver) = channel(LIMITS);
        for value in 0..3 {
            sender.try_send(value, 1).unwrap();
        }
        let waiting = tokio::spawn(async move { sender.send(3, 1).await });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!waiting.is_finished());

        assert_eq!(receiver.recv().await, Some(0));

        assert_eq!(waiting.await.unwrap(), Ok(()));
        for expected in 1..4 {
            assert_eq!(receiver.recv().await, Some(expected));
        }
    }

    #[tokio::test]
    async fn dropping_the_receiver_fails_senders() {
        let (sender, receiver) = channel::<u8>(LIMITS);
        for value in 0..3 {
            sender.try_send(value, 1).unwrap();
        }
        let waiting = tokio::spawn({
            let sender = sender.clone();
            async move { sender.send(3, 1).await }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;

        drop(receiver);

        assert_eq!(waiting.await.unwrap(), Err(Closed));
        assert_eq!(sender.try_send(4, 1), Err(TrySendError::Closed(4)));
        assert!(sender.reserve().await.is_err());
    }

    #[tokio::test]
    async fn a_waiting_receiver_wakes_for_a_value_and_for_the_end() {
        let (sender, mut receiver) = channel(LIMITS);
        let reading = tokio::spawn(async move {
            let first = receiver.recv().await;
            (first, receiver.recv().await)
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        sender.try_send(7, 1).unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        drop(sender);

        assert_eq!(reading.await.unwrap(), (Some(7), None));
    }

    #[tokio::test]
    async fn a_permit_waits_for_room_and_queues_a_value() {
        let (sender, mut receiver) = channel(LIMITS);
        for value in 0..3 {
            sender.reserve().await.unwrap().send(value, 1);
        }
        let reserved = tokio::spawn(async move {
            sender.reserve().await.unwrap().send(3, 1);
            sender
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!reserved.is_finished());

        assert_eq!(receiver.recv().await, Some(0));

        let sender = reserved.await.unwrap();
        drop(sender);
        for expected in 1..4 {
            assert_eq!(receiver.recv().await, Some(expected));
        }
        assert_eq!(receiver.recv().await, None);
    }
}
