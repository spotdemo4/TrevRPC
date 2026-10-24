//! The `records` payload of the load benchmarks: realistic, compressible message text in which
//! every message differs. It follows `bench/payload.py` byte for byte (see `bench/README.md`).

use std::fmt::Write as _;

/// The seed of worker `i` is `WORKER_SEED + i`.
pub const WORKER_SEED: u64 = 0x9E37_79B9_7F4A_7C15;

const SERVICES: [&str; 8] = [
    "checkout",
    "cart",
    "payments",
    "inventory",
    "search",
    "auth",
    "gateway",
    "notifier",
];
const REGIONS: [&str; 5] = [
    "us-east-1",
    "us-west-2",
    "eu-west-1",
    "eu-central-1",
    "ap-southeast-1",
];
const METHODS: [&str; 4] = ["GET", "POST", "PUT", "DELETE"];
const ROUTES: [&str; 6] = [
    "/api/v1/orders",
    "/api/v1/orders/{id}",
    "/api/v1/cart",
    "/api/v1/users/{id}",
    "/api/v1/search",
    "/healthz",
];
const STATUSES: [u64; 10] = [200, 200, 200, 200, 200, 201, 204, 400, 404, 500];
const HEX: &[u8; 16] = b"0123456789abcdef";
const BASE_TIMESTAMP_MS: u64 = 1_790_000_000_000;

/// A generator of `records` messages: an xorshift64 state and the number of records it made.
pub struct Records {
    state: u64,
    count: u64,
}

impl Records {
    pub const fn new(seed: u64) -> Self {
        Self {
            state: seed,
            count: 0,
        }
    }

    const fn next(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x.wrapping_shl(13);
        x ^= x.wrapping_shr(7);
        x ^= x.wrapping_shl(17);
        self.state = x;
        x
    }

    /// `x % len`, for a length that is a small non-zero constant.
    fn below(&mut self, len: usize) -> usize {
        let len = u64::try_from(len).unwrap_or(u64::MAX);
        usize::try_from(self.next().checked_rem(len).unwrap_or(0)).unwrap_or(0)
    }

    fn pick<T: Copy + Default, const N: usize>(&mut self, values: &[T; N]) -> T {
        values.get(self.below(N)).copied().unwrap_or_default()
    }

    fn between(&mut self, low: u64, high: u64) -> u64 {
        let span = high.saturating_sub(low).saturating_add(1);
        low.saturating_add(self.next().checked_rem(span).unwrap_or(0))
    }

    /// Appends one record, one JSON object and a newline, to `out`.
    fn record(&mut self, out: &mut String) {
        let timestamp = BASE_TIMESTAMP_MS
            .wrapping_add(self.count.wrapping_mul(13))
            .wrapping_add(self.between(0, 12));
        self.count = self.count.wrapping_add(1);
        let service = self.pick(&SERVICES);
        let region = self.pick(&REGIONS);
        let method = self.pick(&METHODS);
        let route = self.pick(&ROUTES);
        let status = self.pick(&STATUSES);
        let latency = self.between(1, 2000);
        let mut trace = [0_u8; 32];
        for digit in &mut trace {
            *digit = HEX.get(self.below(HEX.len())).copied().unwrap_or(b'0');
        }
        let trace = std::str::from_utf8(&trace).unwrap_or_default();
        let user = self.between(1, 1_000_000);
        let _ = writeln!(
            out,
            r#"{{"ts":{timestamp},"service":"{service}","region":"{region}","method":"{method}","route":"{route}","status":{status},"latency_ms":{latency},"trace":"{trace}","user":{user}}}"#
        );
    }

    /// The next message: records appended until they reach `size` bytes, cut to exactly `size`.
    pub fn message(&mut self, size: usize) -> String {
        let mut out = String::with_capacity(size.saturating_add(256));
        while out.len() < size {
            self.record(&mut out);
        }
        // Records are ASCII, so every byte is a character boundary.
        out.truncate(size);
        out
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Write as _;

    use super::{Records, WORKER_SEED};

    /// `payload.GOLDEN_SHA256` in `bench/payload.py`.
    const GOLDEN_SHA256: &str = "1414f2e3835a891eecc8579d6736157e456eee7f4f3547b5a326b48f625a8a98";

    #[test]
    fn messages_match_the_reference_generator() {
        let mut records = Records::new(WORKER_SEED);
        let mut digest = ring::digest::Context::new(&ring::digest::SHA256);
        for size in [64, 256, 1024, 4096] {
            let message = records.message(size);
            assert_eq!(message.len(), size);
            digest.update(message.as_bytes());
        }
        let hex = digest
            .finish()
            .as_ref()
            .iter()
            .fold(String::new(), |mut hex, byte| {
                let _ = write!(hex, "{byte:02x}");
                hex
            });
        assert_eq!(hex, GOLDEN_SHA256);
    }

    #[test]
    fn every_message_differs() {
        let mut records = Records::new(WORKER_SEED);
        let first = records.message(64);
        assert_ne!(first, records.message(64));
        assert!(first.starts_with(r#"{"ts":17900000000"#), "{first}");
    }
}
