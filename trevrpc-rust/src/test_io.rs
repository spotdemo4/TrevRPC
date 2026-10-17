//! In-memory byte streams for unit tests.

#![allow(clippy::unused_async_trait_impl)]

use std::collections::VecDeque;

use bytes::Bytes;

use crate::Result;
use crate::framed::{FrameRead, FrameWrite};

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
