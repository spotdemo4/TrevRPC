//! Optimistic stream compression on the client: a call stream-compresses its requests without
//! waiting for the server's answer, keeps a copy of each request it sends until the answer
//! arrives, and replays the call without stream compression if the server refuses it.

use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use futures_core::Stream;
use futures_util::StreamExt as _;

use crate::compression::{Reservation, StreamBudget};
use crate::sync::MutexExt as _;
use crate::{BoxStream, Result};

/// The most request bytes an optimistic call keeps until the server accepts it. A send beyond it
/// waits for the answer.
pub const MAX_KEPT_BYTES: usize = 1 << 20;

/// Whether the calls on one connection are optimistic: they are until the server refuses one,
/// and again once a call gets a stream compression answer.
#[derive(Debug)]
pub struct Optimism(AtomicBool);

impl Default for Optimism {
    fn default() -> Self {
        Self(AtomicBool::new(true))
    }
}

impl Optimism {
    pub fn get(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }

    /// The server refused an optimistic call.
    pub fn refused(&self) {
        self.0.store(false, Ordering::Release);
    }

    /// A call got a stream compression answer, so the server decodes stream-compressed requests
    /// again.
    pub fn answered(&self) {
        self.0.store(true, Ordering::Release);
    }
}

/// The server's decision on an optimistic call, as far as the client knows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Decision {
    Pending,
    /// The call's HEADERS arrived, or it ended: nothing will be replayed.
    Settled,
    /// The server refused the call: the replay owns the requests.
    Refused,
}

struct State {
    /// The caller's requests not yet pulled, `None` once they ended.
    source: Option<BoxStream<Vec<u8>>>,
    /// Copies of the requests sent so far, while the decision is pending.
    kept: Vec<Vec<u8>>,
    kept_bytes: usize,
    reservation: Option<Reservation>,
    /// A request pulled but not sent, because keeping it would exceed the limits.
    held: Option<Vec<u8>>,
    budget: Arc<StreamBudget>,
    decision: Decision,
    waker: Option<Waker>,
}

impl State {
    /// Keeps a copy of `request`, if the per-call cap and the budget have room for it.
    fn keep(&mut self, request: &[u8]) -> bool {
        let Some(kept_bytes) = self
            .kept_bytes
            .checked_add(request.len())
            .filter(|&bytes| bytes <= MAX_KEPT_BYTES)
        else {
            return false;
        };
        let Some(reservation) = self.budget.reserve(request.len()) else {
            return false;
        };
        match &mut self.reservation {
            Some(held) => held.absorb(reservation),
            None => self.reservation = Some(reservation),
        }
        self.kept.push(request.to_vec());
        self.kept_bytes = kept_bytes;
        true
    }

    fn wake(&mut self) {
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }

    /// Hands the requests over to the replay: the copies and the held request, then the rest.
    fn refuse(&mut self) -> (Vec<Vec<u8>>, Option<BoxStream<Vec<u8>>>) {
        self.decision = Decision::Refused;
        self.reservation = None;
        self.kept_bytes = 0;
        let mut requests = std::mem::take(&mut self.kept);
        requests.extend(self.held.take());
        let rest = self.source.take();
        self.wake();
        (requests, rest)
    }

    /// The next request for the original upload.
    fn poll_upload(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Vec<u8>>>> {
        match self.decision {
            // The replay owns the requests; the upload is about to be dropped.
            Decision::Refused => {
                self.waker = Some(cx.waker().clone());
                Poll::Pending
            }
            Decision::Settled => {
                if let Some(request) = self.held.take() {
                    return Poll::Ready(Some(Ok(request)));
                }
                self.poll_source(cx)
            }
            Decision::Pending => {
                if self.held.is_some() {
                    self.waker = Some(cx.waker().clone());
                    return Poll::Pending;
                }
                match self.poll_source(cx) {
                    Poll::Ready(Some(Ok(request))) => {
                        if self.keep(&request) {
                            Poll::Ready(Some(Ok(request)))
                        } else {
                            // Wait for the decision. The request stays here, so a replay still
                            // sends it if the upload is dropped meanwhile.
                            self.held = Some(request);
                            self.waker = Some(cx.waker().clone());
                            Poll::Pending
                        }
                    }
                    other => other,
                }
            }
        }
    }

    fn poll_source(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Vec<u8>>>> {
        let Some(source) = self.source.as_mut() else {
            return Poll::Ready(None);
        };
        let polled = source.poll_next_unpin(cx);
        if matches!(polled, Poll::Ready(None)) {
            self.source = None;
        }
        polled
    }
}

/// The requests of one optimistic call, shared by its upload and its response reader.
#[derive(Clone)]
pub struct Replay(Arc<Mutex<State>>);

impl std::fmt::Debug for Replay {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Replay").finish_non_exhaustive()
    }
}

impl Replay {
    /// Wraps the caller's `requests`, returning the replay and the stream the upload sends,
    /// which keeps a copy of each request, within `budget`, until the decision.
    pub fn new(
        requests: BoxStream<Vec<u8>>,
        budget: Arc<StreamBudget>,
    ) -> (Self, BoxStream<Vec<u8>>) {
        let replay = Self(Arc::new(Mutex::new(State {
            source: Some(requests),
            kept: Vec::new(),
            kept_bytes: 0,
            reservation: None,
            held: None,
            budget,
            decision: Decision::Pending,
            waker: None,
        })));
        let upload = Box::pin(Recorder(replay.clone()));
        (replay, upload)
    }

    /// The server accepted the call, or it ended otherwise: the copies are dropped and the
    /// upload goes on without keeping any.
    pub fn settle(&self) {
        let mut state = self.0.lock_unpoisoned();
        if state.decision != Decision::Pending {
            return;
        }
        state.decision = Decision::Settled;
        state.kept = Vec::new();
        state.kept_bytes = 0;
        state.reservation = None;
        state.wake();
    }

    /// The server refused the call: returns the requests to send on the replayed call, the
    /// copies of those already sent and then the rest, and stops the original upload from
    /// pulling any more.
    pub fn refuse(&self) -> BoxStream<Vec<u8>> {
        let (requests, rest) = self.0.lock_unpoisoned().refuse();
        let kept = futures_util::stream::iter(requests.into_iter().map(Ok));
        match rest {
            Some(rest) => Box::pin(kept.chain(rest)),
            None => Box::pin(kept),
        }
    }
}

/// The stream the original upload sends: the caller's requests, each kept until the decision.
struct Recorder(Replay);

impl Stream for Recorder {
    type Item = Result<Vec<u8>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.0.lock_unpoisoned().poll_upload(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn requests(sizes: &[usize]) -> BoxStream<Vec<u8>> {
        let items: Vec<_> = sizes
            .iter()
            .enumerate()
            .map(|(index, &size)| Ok(vec![u8::try_from(index).unwrap_or(0); size]))
            .collect();
        Box::pin(futures_util::stream::iter(items))
    }

    fn budget(bytes: usize) -> Arc<StreamBudget> {
        StreamBudget::new(bytes).expect("budget")
    }

    async fn collect(stream: BoxStream<Vec<u8>>) -> Vec<Vec<u8>> {
        stream.map(|item| item.expect("request")).collect().await
    }

    #[tokio::test]
    async fn a_refusal_replays_the_sent_requests_then_the_rest() {
        let (replay, mut upload) = Replay::new(requests(&[1, 2, 3]), budget(1 << 20));
        assert_eq!(
            upload.next().await.transpose().expect("first"),
            Some(vec![0])
        );
        assert_eq!(
            upload.next().await.transpose().expect("second"),
            Some(vec![1, 1])
        );
        let replayed = collect(replay.refuse()).await;
        assert_eq!(replayed, vec![vec![0], vec![1, 1], vec![2, 2, 2]]);
        // The original upload pulls nothing more.
        assert!(futures_util::FutureExt::now_or_never(upload.next()).is_none());
    }

    #[tokio::test]
    async fn settling_drops_the_copies_and_returns_their_memory() {
        let budget = budget(4);
        let (replay, mut upload) = Replay::new(requests(&[4, 4]), Arc::clone(&budget));
        assert_eq!(
            upload.next().await.transpose().expect("first"),
            Some(vec![0; 4])
        );
        // The budget is spent: the second request waits for the decision.
        assert!(futures_util::FutureExt::now_or_never(upload.next()).is_none());
        assert!(budget.reserve(1).is_none());
        replay.settle();
        assert!(budget.reserve(4).is_some());
        assert_eq!(collect(upload).await, vec![vec![1; 4]]);
    }

    #[tokio::test]
    async fn a_request_past_the_cap_waits_and_is_replayed_if_refused() {
        let (replay, mut upload) = Replay::new(requests(&[MAX_KEPT_BYTES, 1]), budget(4 << 20));
        assert!(upload.next().await.is_some());
        assert!(futures_util::FutureExt::now_or_never(upload.next()).is_none());
        let replayed = collect(replay.refuse()).await;
        assert_eq!(replayed.len(), 2);
        assert_eq!(replayed[1], vec![1]);
    }

    #[test]
    fn optimism_stops_on_a_refusal_until_an_answer() {
        let optimism = Optimism::default();
        assert!(optimism.get());
        optimism.refused();
        assert!(!optimism.get());
        optimism.answered();
        assert!(optimism.get());
    }
}
