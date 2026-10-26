//! Congestion control for the QUIC connections this crate configures: Quinn's Cubic, except that a
//! sender that recently used much of its window counts as limited by it.
//!
//! Cubic grows the window only while the window limits the sender, and Quinn counts a sender as
//! limited only when the window blocked the last packet it wanted to send. A paced sender, such as
//! one sending datagram messages for real-time traffic, almost always empties its queue before the
//! window fills, so its window never grows past what it has in flight: every loss then cuts the
//! window below that, and the next packets wait a round trip for acknowledgements. On a 20 ms
//! round trip with 1% loss that delayed one message in ten by a round trip.
//!
//! As RFC 7661 does for TCP, this counts the window as used while the most the sender had in
//! flight in the last two round trips reaches half of it, and, like quic-go, while it comes
//! within [`HEADROOM_PACKETS`] of it. The window then grows to about twice the sender's bursts,
//! so a loss's cut leaves it room for the next one. Bulk senders, which fill the window anyway,
//! see no change.

use std::any::Any;
use std::sync::Arc;
use std::time::{Duration, Instant};

use quinn::congestion::{Controller, ControllerFactory, ControllerMetrics, CubicConfig};
use quinn_proto::RttEstimator;

/// How many full-size packets of room in the window still count as being limited by it: quic-go's
/// maximum burst.
const HEADROOM_PACKETS: u64 = 3;

/// The controller factory of every QUIC connection this crate configures.
pub fn factory() -> Arc<dyn ControllerFactory + Send + Sync> {
    Arc::new(HeadroomFactory(Arc::new(CubicConfig::default())))
}

struct HeadroomFactory(Arc<CubicConfig>);

impl ControllerFactory for HeadroomFactory {
    fn build(self: Arc<Self>, now: Instant, current_mtu: u16) -> Box<dyn Controller> {
        Box::new(Headroom {
            inner: Arc::clone(&self.0).build(now, current_mtu),
            in_flight: 0,
            peak: 0,
            peak_at: now,
            rtt: Duration::ZERO,
            mtu: u64::from(current_mtu),
        })
    }
}

/// Wraps a controller so that a sender that recently used much of its window is not
/// application-limited.
struct Headroom {
    inner: Box<dyn Controller>,
    /// The bytes in flight as of the last acknowledgements, plus those sent and minus those acked
    /// or lost since. Quinn reports the exact figure after every batch of acknowledgements.
    in_flight: u64,
    /// The most bytes in flight within the last two round trips, and when that was.
    peak: u64,
    peak_at: Instant,
    /// The smoothed round trip, as of the last acknowledgement.
    rtt: Duration,
    mtu: u64,
}

impl Headroom {
    /// Whether the sender counts as application-limited: Quinn says it is, and the most it had in
    /// flight recently left more than half the window, and more than [`HEADROOM_PACKETS`], free.
    fn app_limited(&self, app_limited: bool) -> bool {
        let window = self.inner.window();
        let room = window.saturating_sub(self.peak.max(self.in_flight));
        app_limited && room > self.mtu.saturating_mul(HEADROOM_PACKETS) && room > window / 2
    }
}

impl Controller for Headroom {
    fn on_sent(&mut self, now: Instant, bytes: u64, last_packet_number: u64) {
        self.in_flight = self.in_flight.saturating_add(bytes);
        let stale = now.saturating_duration_since(self.peak_at) > self.rtt.saturating_mul(2);
        if stale || self.in_flight >= self.peak {
            self.peak = self.in_flight;
            self.peak_at = now;
        }
        self.inner.on_sent(now, bytes, last_packet_number);
    }

    fn on_ack(
        &mut self,
        now: Instant,
        sent: Instant,
        bytes: u64,
        app_limited: bool,
        rtt: &RttEstimator,
    ) {
        self.rtt = rtt.get();
        let app_limited = self.app_limited(app_limited);
        self.inner.on_ack(now, sent, bytes, app_limited, rtt);
        self.in_flight = self.in_flight.saturating_sub(bytes);
    }

    fn on_end_acks(
        &mut self,
        now: Instant,
        in_flight: u64,
        app_limited: bool,
        largest_packet_num_acked: Option<u64>,
    ) {
        self.in_flight = in_flight;
        let app_limited = self.app_limited(app_limited);
        self.inner
            .on_end_acks(now, in_flight, app_limited, largest_packet_num_acked);
    }

    fn on_congestion_event(
        &mut self,
        now: Instant,
        sent: Instant,
        is_persistent_congestion: bool,
        lost_bytes: u64,
    ) {
        self.in_flight = self.in_flight.saturating_sub(lost_bytes);
        self.inner
            .on_congestion_event(now, sent, is_persistent_congestion, lost_bytes);
    }

    fn on_mtu_update(&mut self, new_mtu: u16) {
        self.mtu = u64::from(new_mtu);
        self.inner.on_mtu_update(new_mtu);
    }

    fn window(&self) -> u64 {
        self.inner.window()
    }

    fn metrics(&self) -> ControllerMetrics {
        self.inner.metrics()
    }

    fn clone_box(&self) -> Box<dyn Controller> {
        Box::new(Self {
            inner: self.inner.clone_box(),
            in_flight: self.in_flight,
            peak: self.peak,
            peak_at: self.peak_at,
            rtt: self.rtt,
            mtu: self.mtu,
        })
    }

    fn initial_window(&self) -> u64 {
        self.inner.initial_window()
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A controller with a fixed window that records what it is told.
    #[derive(Clone)]
    struct Fixed {
        window: u64,
        app_limited: Arc<std::sync::Mutex<Vec<bool>>>,
    }

    impl Controller for Fixed {
        fn on_end_acks(&mut self, _: Instant, _: u64, app_limited: bool, _: Option<u64>) {
            self.app_limited.lock().unwrap().push(app_limited);
        }

        fn on_congestion_event(&mut self, _: Instant, _: Instant, _: bool, _: u64) {}

        fn on_mtu_update(&mut self, _: u16) {}

        fn window(&self) -> u64 {
            self.window
        }

        fn clone_box(&self) -> Box<dyn Controller> {
            Box::new(self.clone())
        }

        fn initial_window(&self) -> u64 {
            self.window
        }

        fn into_any(self: Box<Self>) -> Box<dyn Any> {
            self
        }
    }

    const MTU: u64 = 1200;

    fn headroom(window: u64, now: Instant) -> (Headroom, Arc<std::sync::Mutex<Vec<bool>>>) {
        let app_limited = Arc::default();
        let inner = Fixed {
            window,
            app_limited: Arc::clone(&app_limited),
        };
        let controller = Headroom {
            inner: Box::new(inner),
            in_flight: 0,
            peak: 0,
            peak_at: now,
            rtt: Duration::from_millis(20),
            mtu: MTU,
        };
        (controller, app_limited)
    }

    #[test]
    fn a_sender_that_used_half_the_window_is_limited_by_it() {
        let now = Instant::now();
        let (mut controller, _) = headroom(20 * MTU, now);
        controller.on_sent(now, 9 * MTU, 0);
        assert!(controller.app_limited(true));
        controller.on_sent(now, MTU, 1);
        assert!(!controller.app_limited(true));
        // Quinn's own verdict that the window blocked the sender stands.
        controller.on_end_acks(now, 0, false, None);
        assert!(!controller.app_limited(false));
    }

    #[test]
    fn a_sender_within_three_packets_of_a_small_window_is_limited_by_it() {
        let now = Instant::now();
        let (mut controller, _) = headroom(6 * MTU, now);
        controller.on_sent(now, 2 * MTU, 0);
        assert!(controller.app_limited(true));
        controller.on_sent(now, MTU, 1);
        assert!(!controller.app_limited(true));
    }

    #[test]
    fn a_burst_counts_until_two_round_trips_have_passed() {
        let now = Instant::now();
        let (mut controller, recorded) = headroom(20 * MTU, now);
        controller.on_sent(now, 12 * MTU, 0);
        // Acknowledged: nothing is in flight, but the burst was recent.
        controller.on_end_acks(now, 0, true, Some(0));
        assert!(!controller.app_limited(true));
        let later = now + Duration::from_millis(50);
        controller.on_sent(later, MTU, 1);
        assert!(controller.app_limited(true));
        assert_eq!(*recorded.lock().unwrap(), [false]);
    }

    #[test]
    fn losses_leave_the_flight() {
        let now = Instant::now();
        let (mut controller, _) = headroom(20 * MTU, now);
        controller.on_sent(now, 12 * MTU, 0);
        controller.on_congestion_event(now, now, false, 12 * MTU);
        assert_eq!(controller.in_flight, 0);
    }
}
