//! A stream that is polled only after it asked to be.
//!
//! A future or stream that merges several sources polls every pending one each time any of them
//! wakes it. That is free for a channel but not for a Quinn stream, whose every poll takes its
//! connection's lock: a call that merges its datagram messages with its stream would take that
//! lock once more for every datagram. [`Gated`] remembers whether the stream it wraps woke since
//! it last returned `Pending`, and skips the poll when it did not.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use futures_core::Stream;
use futures_util::task::{ArcWake, AtomicWaker, waker_ref};

/// A stream polled only when it woke since it last returned `Pending`, or before its first poll.
pub struct Gated<S> {
    inner: S,
    gate: Arc<Gate>,
}

struct Gate {
    /// The inner stream may be ready.
    woken: AtomicBool,
    /// The task polling the gated stream.
    task: AtomicWaker,
}

impl ArcWake for Gate {
    fn wake_by_ref(gate: &Arc<Self>) {
        gate.woken.store(true, Ordering::Release);
        gate.task.wake();
    }
}

impl<S> Gated<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            gate: Arc::new(Gate {
                woken: AtomicBool::new(true),
                task: AtomicWaker::new(),
            }),
        }
    }
}

impl<S: Stream + Unpin> Stream for Gated<S> {
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<S::Item>> {
        let this = &mut *self;
        this.gate.task.register(cx.waker());
        if !this.gate.woken.swap(false, Ordering::AcqRel) {
            return Poll::Pending;
        }
        let waker = waker_ref(&this.gate);
        let polled = Pin::new(&mut this.inner).poll_next(&mut Context::from_waker(&waker));
        if polled.is_ready() {
            // A stream that yielded may have more ready at once.
            this.gate.woken.store(true, Ordering::Release);
        }
        polled
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::task::Waker;

    use futures_util::StreamExt as _;

    use super::*;

    /// A stream that counts its polls and is ready only once woken.
    struct Counting {
        polls: Arc<AtomicUsize>,
        ready: Arc<AtomicBool>,
        waker: Arc<std::sync::Mutex<Option<Waker>>>,
    }

    impl Stream for Counting {
        type Item = u8;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<u8>> {
            self.polls.fetch_add(1, Ordering::Relaxed);
            if self.ready.swap(false, Ordering::Relaxed) {
                return Poll::Ready(Some(1));
            }
            *self.waker.lock().unwrap() = Some(cx.waker().clone());
            Poll::Pending
        }
    }

    #[test]
    fn polls_the_inner_stream_only_after_it_woke() {
        let polls = Arc::new(AtomicUsize::new(0));
        let ready = Arc::new(AtomicBool::new(false));
        let waker = Arc::default();
        let mut gated = Gated::new(Counting {
            polls: Arc::clone(&polls),
            ready: Arc::clone(&ready),
            waker: Arc::clone(&waker),
        });
        let task = futures_util::task::noop_waker();
        let mut cx = Context::from_waker(&task);

        assert!(gated.poll_next_unpin(&mut cx).is_pending());
        // Woken by something else: the inner stream is not polled again.
        assert!(gated.poll_next_unpin(&mut cx).is_pending());
        assert!(gated.poll_next_unpin(&mut cx).is_pending());
        assert_eq!(polls.load(Ordering::Relaxed), 1);

        ready.store(true, Ordering::Relaxed);
        waker.lock().unwrap().take().unwrap().wake();
        assert_eq!(gated.poll_next_unpin(&mut cx), Poll::Ready(Some(1)));
        // After yielding, it is polled again at once.
        assert!(gated.poll_next_unpin(&mut cx).is_pending());
        assert_eq!(polls.load(Ordering::Relaxed), 3);
    }
}
