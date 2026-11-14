//! In-memory byte streams for unit tests.

#![allow(clippy::unused_async_trait_impl)]

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;

use crate::Result;
use crate::datagram::{DatagramTransport, Unsent};
use crate::framed::{ChunkSource, FrameRead, FrameWrite};

/// A readable stream that serves its bytes in small reads and counts what it served.
pub struct Script {
    data: VecDeque<u8>,
    read_size: usize,
    /// Bytes handed out so far.
    pub served: usize,
}

impl Script {
    /// A stream served in three-byte reads, so frames straddle reads.
    pub fn new(data: Vec<u8>) -> Self {
        Self::with_read_size(data, 3)
    }

    pub fn with_read_size(data: Vec<u8>, read_size: usize) -> Self {
        Self {
            data: data.into(),
            read_size,
            served: 0,
        }
    }
}

impl FrameRead for Script {
    async fn read_frame_bytes(&mut self, bytes: &mut [u8]) -> Result<Option<usize>> {
        if self.data.is_empty() {
            return Ok(None);
        }
        let take = bytes.len().min(self.read_size).min(self.data.len());
        for slot in bytes.iter_mut().take(take) {
            *slot = self.data.pop_front().expect("data was checked");
        }
        self.served = self.served.saturating_add(take);
        Ok(Some(take))
    }
}

/// A writable stream that records each write.
#[derive(Default)]
pub struct Sink(pub Vec<Vec<u8>>);

impl FrameWrite for Sink {
    async fn write_frame_chunks(&mut self, chunks: &mut [Bytes]) -> Result<()> {
        self.0
            .push(chunks.iter().flat_map(|chunk| chunk.to_vec()).collect());
        Ok(())
    }
}

/// A datagram transport that takes a limited number of datagrams, refuses the rest, and receives
/// none. Clones share what it took.
#[derive(Clone)]
pub struct Datagrams {
    max: usize,
    room: Arc<AtomicUsize>,
    sent: Arc<Mutex<Vec<Bytes>>>,
}

impl Datagrams {
    /// A transport that sends datagrams of up to `max` bytes, `room` of them before it refuses.
    pub fn new(max: usize, room: usize) -> Self {
        Self {
            max,
            room: Arc::new(AtomicUsize::new(room)),
            sent: Arc::default(),
        }
    }

    /// The datagrams it took.
    pub fn sent(&self) -> Vec<Bytes> {
        self.sent.lock().expect("sent lock").clone()
    }
}

impl DatagramTransport for Datagrams {
    fn max_size(&self) -> Option<usize> {
        Some(self.max)
    }

    async fn send(&self, datagram: Bytes) -> std::result::Result<(), Unsent> {
        self.room
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |room| {
                room.checked_sub(1)
            })
            .map_err(|_| Unsent)?;
        self.sent.lock().expect("sent lock").push(datagram);
        Ok(())
    }

    async fn recv(&self) -> Option<Bytes> {
        std::future::pending().await
    }
}

/// A [`ChunkSource`] that serves fixed chunks and counts how it was read.
pub struct Chunks {
    queue: VecDeque<Bytes>,
    /// Calls to `next_chunk`.
    pub chunk_reads: usize,
    /// Calls to `read_into`.
    pub direct_reads: usize,
}

impl Chunks {
    pub fn new(chunks: impl IntoIterator<Item = Bytes>) -> Self {
        Self {
            queue: chunks.into_iter().collect(),
            chunk_reads: 0,
            direct_reads: 0,
        }
    }
}

impl ChunkSource for Chunks {
    async fn next_chunk(&mut self) -> Result<Option<Bytes>> {
        self.chunk_reads = self.chunk_reads.saturating_add(1);
        Ok(self.queue.pop_front())
    }

    async fn read_into(&mut self, bytes: &mut [u8]) -> Result<Option<usize>> {
        self.direct_reads = self.direct_reads.saturating_add(1);
        let Some(chunk) = self.queue.front_mut() else {
            return Ok(None);
        };
        let take = bytes.len().min(chunk.len());
        let (head, _) = bytes.split_at_mut(take);
        head.copy_from_slice(&chunk.split_to(take));
        if chunk.is_empty() {
            self.queue.pop_front();
        }
        Ok(Some(take))
    }
}
