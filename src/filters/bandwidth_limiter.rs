// Copyright (c) 2026 FastPubSub Network
// SPDX-License-Identifier: MIT OR Apache-2.0
// Project: https://fastpubsub.com

//! Filter that limits outbound bandwidth.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use super::{
    FilterError, FilterInboundResult, FilterSendMessage, FilterTimerContext, FilterTrait,
    RouteContext,
};

const MICROS_PER_SEC: u64 = 1_000_000;

/// This filter limits outbound publish packets by shared bandwidth.
///
/// This filter works only from the client to the network. It does not limit
/// inbound packets from the network, does not queue them, and passes them on.
///
/// One filter instance has one FIFO queue and one token bucket. If this filter
/// is added with `add_filter` on a channel prefix, all channels inside that
/// prefix share one queue and one limit. The filter does not create a separate
/// queue for each channel.
///
/// Parameters:
///
/// - `bandwidth_bytes_per_sec` sets how many bytes per second can be sent.
/// - `max_queue_delay` sets how long a packet may wait in the queue. If it
///   waits longer, it is dropped.
/// - `max_queue_messages` sets the maximum packet count in the inner queue. If
///   the queue is already full, a new packet is dropped at once.
/// - `max_queue_bytes` sets the maximum total byte size in the inner queue. If
///   a new packet does not fit this limit, it is dropped at once.
/// - `max_burst_bytes` sets how many tokens can be saved after idle time. This
///   is not a limit for one tick. In one call, the filter sends everything that
///   the current token bucket policy allows.
///
/// If a packet is larger than `max_burst_bytes`, the filter can send it with
/// token debt. After that, next packets wait until refill pays the debt.
///
/// # SDK Timer
///
/// The SDK implementation for WebSocket or WebTransport calls `on_timer` on
/// background filters. For this filter, the timer is only an alarm: it tries to
/// send packets that are already in the queue. Bandwidth math does not depend
/// on a fixed interval. The filter checks real time with `Instant::now()` each
/// time and refills tokens by elapsed time.
///
/// If a new `publish` arrives and tokens already exist, the packet can be sent
/// at once, without waiting for the next timer tick.
///
/// Available timer modes:
///
/// - `FilterTimerMode::UltraLowLatency1000Hz` - 1000 times per second, 1 ms.
/// - `FilterTimerMode::HighFrequency500Hz` - 500 times per second, 2 ms.
/// - `FilterTimerMode::LowLatency200Hz` - 200 times per second, 5 ms.
/// - `FilterTimerMode::SdkDefault100Hz` - 100 times per second, 10 ms. This is
///   the default mode.
/// - `FilterTimerMode::Economy20Hz` - 20 times per second, 50 ms.
/// - `FilterTimerMode::Slow10Hz` - 10 times per second, 100 ms.
///
/// A faster timer usually lowers delay for packets that wait for tokens in the
/// queue. But it wakes the SDK task more often. The byte-per-second limit does
/// not change.
///
/// # One Filter Example
///
/// ```ignore
/// use std::time::Duration;
///
/// use fastpubsub_sdk::client::create_web_socket;
/// use fastpubsub_sdk::filters::{BandwidthLimiterFilter, FilterTimerMode};
///
/// let fastpubsub = create_web_socket("overlay_name", "AT_token")
///     .filter_timer_mode(FilterTimerMode::SdkDefault100Hz)
///     .add_filter(
///         "tenant_1",
///         "public.",
///         BandwidthLimiterFilter::new(
///             1_000_000,
///             Duration::from_millis(500),
///             1024,
///             8 * 1024 * 1024,
///         ),
///     )
///     .build()
///     .await?;
/// ```
///
/// In this example, all channels with the `public.` prefix share one bandwidth
/// limit of `1_000_000` bytes per second and one inner queue. The timer works
/// in the normal 100 Hz mode. The `filter_timer_mode` line can be skipped
/// because 100 Hz is the default.
///
/// # Two Independent Filters Example
///
/// ```ignore
/// use std::time::Duration;
///
/// use fastpubsub_sdk::client::create_web_socket;
/// use fastpubsub_sdk::filters::{BandwidthLimiterFilter, FilterTimerMode};
///
/// let fastpubsub = create_web_socket("overlay_name", "AT_token")
///     .filter_timer_mode(FilterTimerMode::HighFrequency500Hz)
///     .add_filter(
///         "tenant_1",
///         "public.",
///         BandwidthLimiterFilter::new(
///             5_000_000,
///             Duration::from_millis(500),
///             4096,
///             32 * 1024 * 1024,
///         ),
///     )
///     .add_filter(
///         "tenant_1",
///         "public.video.",
///         BandwidthLimiterFilter::new(
///             500_000,
///             Duration::from_millis(200),
///             256,
///             2 * 1024 * 1024,
///         ),
///     )
///     .build()
///     .await?;
/// ```
///
/// In this example, these are two different filter instances. They have two
/// different queues and two different token buckets. The wider filter works for
/// `public.`, and the more specific filter works for `public.video.` with its
/// own lower bandwidth. The timer is shared by the SDK task, so it wakes both
/// filters 500 times per second. The filter limits stay different.
pub struct BandwidthLimiterFilter {
    bandwidth_bytes_per_sec: u64,
    max_burst_bytes: u64,
    max_queue_delay: Duration,
    max_queue_messages: usize,
    max_queue_bytes: usize,
    state: Mutex<BandwidthLimiterState>,
}

/// Inner state of the token bucket and queue.
struct BandwidthLimiterState {
    queue: VecDeque<QueuedMessage>,
    queued_bytes: usize,
    available_tokens: i64,
    last_refill: Instant,
}

/// Message that waits for permission to send.
struct QueuedMessage {
    tenant: String,
    channel: String,
    payload: Vec<u8>,
    queued_at: Instant,
}

impl QueuedMessage {
    /// Creates a message for the inner queue.
    fn new(ctx: RouteContext<'_>, payload: Vec<u8>, queued_at: Instant) -> Self {
        Self {
            tenant: ctx.tenant.to_string(),
            channel: ctx.channel.to_string(),
            payload,
            queued_at,
        }
    }

    /// Returns the payload size in bytes.
    fn size(&self) -> usize {
        self.payload.len()
    }

    /// Checks if the message has waited in the queue too long.
    fn is_expired(&self, now: Instant, max_queue_delay: Duration) -> bool {
        now.saturating_duration_since(self.queued_at) >= max_queue_delay
    }
}

impl BandwidthLimiterState {
    /// Creates empty state with a full bucket.
    fn new(now: Instant, max_burst_bytes: u64) -> Self {
        Self {
            queue: VecDeque::new(),
            queued_bytes: 0,
            available_tokens: u64_to_i64_saturating(max_burst_bytes),
            last_refill: now,
        }
    }

    /// Adds tokens by real elapsed time.
    fn refill(&mut self, now: Instant, bandwidth_bytes_per_sec: u64, max_burst_bytes: u64) {
        let elapsed = now.saturating_duration_since(self.last_refill);
        self.last_refill = now;

        let new_tokens = refill_tokens(elapsed, bandwidth_bytes_per_sec);
        let max_burst = u64_to_i64_saturating(max_burst_bytes);
        self.available_tokens = self
            .available_tokens
            .saturating_add(new_tokens)
            .min(max_burst);
    }

    /// Removes all messages that are already too old.
    fn drop_expired(&mut self, now: Instant, max_queue_delay: Duration) {
        let mut kept = VecDeque::with_capacity(self.queue.len());
        let mut kept_bytes = 0usize;

        while let Some(message) = self.queue.pop_front() {
            if message.is_expired(now, max_queue_delay) {
                continue;
            }

            kept_bytes = kept_bytes.saturating_add(message.size());
            kept.push_back(message);
        }

        self.queue = kept;
        self.queued_bytes = kept_bytes;
    }

    /// Checks if a new packet can be added to the queue.
    fn can_enqueue(
        &self,
        payload_len: usize,
        max_queue_messages: usize,
        max_queue_bytes: usize,
    ) -> bool {
        if self.queue.len() >= max_queue_messages {
            return false;
        }

        self.queued_bytes
            .checked_add(payload_len)
            .is_some_and(|total| total <= max_queue_bytes)
    }

    /// Adds a packet to the end of the shared queue.
    fn enqueue(&mut self, message: QueuedMessage) {
        self.queued_bytes = self.queued_bytes.saturating_add(message.size());
        self.queue.push_back(message);
    }

    /// Removes the first packet and updates the byte counter.
    fn pop_front(&mut self) -> Option<QueuedMessage> {
        let message = self.queue.pop_front()?;
        self.queued_bytes = self.queued_bytes.saturating_sub(message.size());
        Some(message)
    }

    /// Checks if a packet of this size can be sent.
    fn can_send(&self, payload_len: usize, max_burst_bytes: u64) -> bool {
        if payload_len == 0 {
            return true;
        }
        let payload_tokens = usize_to_i64_saturating(payload_len);
        if self.available_tokens >= payload_tokens {
            return true;
        }

        let max_burst_tokens = u64_to_i64_saturating(max_burst_bytes);
        max_burst_bytes > 0
            && usize_to_u64_saturating(payload_len) > max_burst_bytes
            && self.available_tokens >= max_burst_tokens
    }

    /// Spends tokens after sending.
    fn spend_tokens(&mut self, payload_len: usize) {
        self.available_tokens = self
            .available_tokens
            .saturating_sub(usize_to_i64_saturating(payload_len));
    }

    /// Sends ready packets for the current channel.
    ///
    /// This method is needed for `apply_outbound` because that contract returns
    /// only payloads for the current `tenant/channel`.
    fn drain_for_current_channel(
        &mut self,
        tenant: &str,
        channel: &str,
        max_burst_bytes: u64,
    ) -> Vec<Vec<u8>> {
        let mut payloads = Vec::new();

        loop {
            let Some(message) = self.queue.front() else {
                break;
            };
            if message.tenant != tenant || message.channel != channel {
                break;
            }
            if !self.can_send(message.size(), max_burst_bytes) {
                break;
            }

            let Some(message) = self.pop_front() else {
                break;
            };
            self.spend_tokens(message.size());
            payloads.push(message.payload);
        }

        payloads
    }

    /// Sends all ready packets for the timer.
    fn drain_for_timer(&mut self, max_burst_bytes: u64) -> Vec<FilterSendMessage> {
        let mut messages = Vec::new();

        loop {
            let Some(message) = self.queue.front() else {
                break;
            };
            if !self.can_send(message.size(), max_burst_bytes) {
                break;
            }

            let Some(message) = self.pop_front() else {
                break;
            };
            self.spend_tokens(message.size());
            messages.push(FilterSendMessage::new(
                message.tenant,
                message.channel,
                message.payload,
            ));
        }

        messages
    }
}

/// Counts new tokens without float and without `u128`.
fn refill_tokens(elapsed: Duration, bandwidth_bytes_per_sec: u64) -> i64 {
    let whole_seconds = bandwidth_bytes_per_sec.saturating_mul(elapsed.as_secs());
    let subsec_micros = u64::from(elapsed.subsec_micros());

    let bytes_per_micro = bandwidth_bytes_per_sec / MICROS_PER_SEC;
    let bytes_remainder = bandwidth_bytes_per_sec % MICROS_PER_SEC;
    let subsecond_tokens = bytes_per_micro
        .saturating_mul(subsec_micros)
        .saturating_add(bytes_remainder.saturating_mul(subsec_micros) / MICROS_PER_SEC);

    u64_to_i64_saturating(whole_seconds.saturating_add(subsecond_tokens))
}

/// Converts `u64` to `i64` safely.
fn u64_to_i64_saturating(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

/// Converts `usize` to `i64` safely.
fn usize_to_i64_saturating(value: usize) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

/// Converts `usize` to `u64` safely.
fn usize_to_u64_saturating(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

impl BandwidthLimiterFilter {
    /// Creates a filter with a burst size equal to one second of bandwidth.
    pub fn new(
        bandwidth_bytes_per_sec: u64,
        max_queue_delay: Duration,
        max_queue_messages: usize,
        max_queue_bytes: usize,
    ) -> Self {
        Self::with_max_burst(
            bandwidth_bytes_per_sec,
            bandwidth_bytes_per_sec,
            max_queue_delay,
            max_queue_messages,
            max_queue_bytes,
        )
    }

    /// Creates a filter with queue delay in milliseconds.
    pub fn from_millis(
        bandwidth_bytes_per_sec: u64,
        max_queue_delay_ms: u64,
        max_queue_messages: usize,
        max_queue_bytes: usize,
    ) -> Self {
        Self::new(
            bandwidth_bytes_per_sec,
            Duration::from_millis(max_queue_delay_ms),
            max_queue_messages,
            max_queue_bytes,
        )
    }

    /// Creates a filter with an explicit burst size.
    pub fn with_max_burst(
        bandwidth_bytes_per_sec: u64,
        max_burst_bytes: u64,
        max_queue_delay: Duration,
        max_queue_messages: usize,
        max_queue_bytes: usize,
    ) -> Self {
        let now = Instant::now();
        Self {
            bandwidth_bytes_per_sec,
            max_burst_bytes,
            max_queue_delay,
            max_queue_messages,
            max_queue_bytes,
            state: Mutex::new(BandwidthLimiterState::new(now, max_burst_bytes)),
        }
    }

    /// Returns the byte-per-second limit.
    pub fn bandwidth_bytes_per_sec(&self) -> u64 {
        self.bandwidth_bytes_per_sec
    }

    /// Returns the maximum burst size.
    pub fn max_burst_bytes(&self) -> u64 {
        self.max_burst_bytes
    }

    /// Returns the maximum wait time in the queue.
    pub fn max_queue_delay(&self) -> Duration {
        self.max_queue_delay
    }

    /// Returns the maximum packet count in the queue.
    pub fn max_queue_messages(&self) -> usize {
        self.max_queue_messages
    }

    /// Returns the maximum total queue size.
    pub fn max_queue_bytes(&self) -> usize {
        self.max_queue_bytes
    }
}

impl FilterTrait for BandwidthLimiterFilter {
    fn id(&self) -> &'static str {
        "bandwidth_limiter.v1"
    }

    fn apply_outbound(
        &self,
        ctx: RouteContext<'_>,
        payload: Vec<u8>,
    ) -> Result<Vec<Vec<u8>>, FilterError> {
        let now = Instant::now();
        let mut state = self
            .state
            .lock()
            .map_err(|_| FilterError::Other("failed to lock bandwidth limiter filter".into()))?;

        state.refill(now, self.bandwidth_bytes_per_sec, self.max_burst_bytes);
        state.drop_expired(now, self.max_queue_delay);

        if !state.can_enqueue(payload.len(), self.max_queue_messages, self.max_queue_bytes) {
            return Ok(Vec::new());
        }

        state.enqueue(QueuedMessage::new(ctx, payload, now));
        Ok(state.drain_for_current_channel(ctx.tenant, ctx.channel, self.max_burst_bytes))
    }

    fn apply_inbound(
        &self,
        _ctx: RouteContext<'_>,
        payload: Vec<u8>,
    ) -> Result<FilterInboundResult, FilterError> {
        Ok(FilterInboundResult::single(payload))
    }

    fn on_timer(&self, _ctx: FilterTimerContext) -> Result<Vec<FilterSendMessage>, FilterError> {
        let now = Instant::now();
        let mut state = self
            .state
            .lock()
            .map_err(|_| FilterError::Other("failed to lock bandwidth limiter filter".into()))?;

        state.refill(now, self.bandwidth_bytes_per_sec, self.max_burst_bytes);
        state.drop_expired(now, self.max_queue_delay);
        Ok(state.drain_for_timer(self.max_burst_bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filters::FilterTimerMode;

    /// Creates route context for tests.
    fn ctx(channel: &'static str) -> RouteContext<'static> {
        RouteContext {
            tenant: "tenant_1",
            channel,
        }
    }

    /// Creates timer context for tests.
    fn timer(ms: u64) -> FilterTimerContext {
        FilterTimerContext {
            mode: FilterTimerMode::SdkDefault100Hz,
            interval: Duration::from_millis(ms),
        }
    }

    /// Creates a payload of the needed size.
    fn bytes(len: usize, value: u8) -> Vec<u8> {
        vec![value; len]
    }

    #[test]
    fn bandwidth_limiter_sends_first_payload_at_once() {
        let filter =
            BandwidthLimiterFilter::with_max_burst(1_000, 10, Duration::from_secs(1), 10, 1_000);

        let sent = filter
            .apply_outbound(ctx("public.chat"), b"abc".to_vec())
            .unwrap();

        assert_eq!(sent, vec![b"abc".to_vec()]);
    }

    #[test]
    fn bandwidth_limiter_waits_when_tokens_are_missing() {
        let filter =
            BandwidthLimiterFilter::with_max_burst(1_000, 3, Duration::from_secs(1), 10, 1_000);

        let first = filter
            .apply_outbound(ctx("public.chat"), bytes(3, b'a'))
            .unwrap();
        assert_eq!(first, vec![bytes(3, b'a')]);

        let second = filter
            .apply_outbound(ctx("public.chat"), bytes(3, b'b'))
            .unwrap();
        assert!(second.is_empty());
        assert!(filter.on_timer(timer(0)).unwrap().is_empty());

        std::thread::sleep(Duration::from_millis(10));

        let sent = filter.on_timer(timer(0)).unwrap();
        assert_eq!(
            sent,
            vec![FilterSendMessage::new(
                "tenant_1",
                "public.chat",
                bytes(3, b'b')
            )]
        );
    }

    #[test]
    fn bandwidth_limiter_uses_real_elapsed_time_not_timer_interval() {
        let filter =
            BandwidthLimiterFilter::with_max_burst(10, 5, Duration::from_secs(1), 10, 1_000);

        filter
            .apply_outbound(ctx("public.chat"), bytes(5, b'a'))
            .unwrap();
        filter
            .apply_outbound(ctx("public.chat"), bytes(5, b'b'))
            .unwrap();

        assert!(filter.on_timer(timer(10_000)).unwrap().is_empty());

        std::thread::sleep(Duration::from_millis(600));

        let sent = filter.on_timer(timer(0)).unwrap();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].payload, bytes(5, b'b'));
    }

    #[test]
    fn bandwidth_limiter_inbound_passes_without_queue() {
        let filter = BandwidthLimiterFilter::with_max_burst(1, 1, Duration::from_secs(1), 1, 1);

        let result = filter
            .apply_inbound(ctx("public.chat"), b"from-network".to_vec())
            .unwrap();

        assert_eq!(result.payloads, vec![b"from-network".to_vec()]);
        assert!(result.messages.is_empty());
    }

    #[test]
    fn bandwidth_limiter_drops_expired_payload() {
        let filter =
            BandwidthLimiterFilter::with_max_burst(1, 1, Duration::from_millis(5), 10, 1_000);

        filter
            .apply_outbound(ctx("public.chat"), b"a".to_vec())
            .unwrap();
        filter
            .apply_outbound(ctx("public.chat"), b"b".to_vec())
            .unwrap();

        std::thread::sleep(Duration::from_millis(15));

        assert!(filter.on_timer(timer(0)).unwrap().is_empty());
    }

    #[test]
    fn bandwidth_limiter_keeps_fifo_order() {
        let filter =
            BandwidthLimiterFilter::with_max_burst(1_000, 3, Duration::from_secs(1), 10, 1_000);

        filter
            .apply_outbound(ctx("public.chat"), bytes(3, b'a'))
            .unwrap();
        filter
            .apply_outbound(ctx("public.chat"), b"b".to_vec())
            .unwrap();
        filter
            .apply_outbound(ctx("public.chat"), b"c".to_vec())
            .unwrap();

        std::thread::sleep(Duration::from_millis(10));

        let sent = filter.on_timer(timer(0)).unwrap();
        assert_eq!(sent.len(), 2);
        assert_eq!(sent[0].payload, b"b".to_vec());
        assert_eq!(sent[1].payload, b"c".to_vec());
    }

    #[test]
    fn bandwidth_limiter_oversized_payload_uses_debt() {
        let filter =
            BandwidthLimiterFilter::with_max_burst(1_000, 10, Duration::from_secs(1), 10, 1_000);

        let big = filter
            .apply_outbound(ctx("public.chat"), bytes(30, b'a'))
            .unwrap();
        assert_eq!(big, vec![bytes(30, b'a')]);

        assert!(filter
            .apply_outbound(ctx("public.chat"), b"b".to_vec())
            .unwrap()
            .is_empty());
        std::thread::sleep(Duration::from_millis(10));
        assert!(filter.on_timer(timer(0)).unwrap().is_empty());

        std::thread::sleep(Duration::from_millis(30));
        let sent = filter.on_timer(timer(0)).unwrap();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].payload, b"b".to_vec());
    }

    #[test]
    fn bandwidth_limiter_drops_when_message_queue_is_full() {
        let filter =
            BandwidthLimiterFilter::with_max_burst(1_000, 1, Duration::from_secs(5), 1, 1_000);

        filter
            .apply_outbound(ctx("public.chat"), b"a".to_vec())
            .unwrap();
        filter
            .apply_outbound(ctx("public.chat"), b"b".to_vec())
            .unwrap();
        filter
            .apply_outbound(ctx("public.chat"), b"c".to_vec())
            .unwrap();

        std::thread::sleep(Duration::from_millis(10));

        let sent = filter.on_timer(timer(0)).unwrap();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].payload, b"b".to_vec());
    }

    #[test]
    fn bandwidth_limiter_drops_when_byte_queue_is_full() {
        let filter =
            BandwidthLimiterFilter::with_max_burst(1_000, 1, Duration::from_secs(5), 10, 2);

        filter
            .apply_outbound(ctx("public.chat"), b"a".to_vec())
            .unwrap();
        filter
            .apply_outbound(ctx("public.chat"), b"bb".to_vec())
            .unwrap();
        filter
            .apply_outbound(ctx("public.chat"), b"cc".to_vec())
            .unwrap();

        std::thread::sleep(Duration::from_millis(10));

        let sent = filter.on_timer(timer(0)).unwrap();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].payload, b"bb".to_vec());
    }

    #[test]
    fn bandwidth_limiter_uses_one_queue_for_channels_inside_prefix() {
        let filter =
            BandwidthLimiterFilter::with_max_burst(100, 2, Duration::from_secs(1), 10, 1_000);

        filter
            .apply_outbound(ctx("public.a"), bytes(2, b'a'))
            .unwrap();
        filter
            .apply_outbound(ctx("public.a"), bytes(2, b'b'))
            .unwrap();
        filter
            .apply_outbound(ctx("public.b"), bytes(2, b'c'))
            .unwrap();

        std::thread::sleep(Duration::from_millis(25));

        let first = filter.on_timer(timer(0)).unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].channel, "public.a");
        assert_eq!(first[0].payload, bytes(2, b'b'));

        std::thread::sleep(Duration::from_millis(25));

        let second = filter.on_timer(timer(0)).unwrap();
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].channel, "public.b");
        assert_eq!(second[0].payload, bytes(2, b'c'));
    }

    #[test]
    fn bandwidth_limiter_timer_messages_keep_route() {
        let filter =
            BandwidthLimiterFilter::with_max_burst(1_000, 3, Duration::from_secs(1), 10, 1_000);

        filter
            .apply_outbound(ctx("public.a"), bytes(3, b'a'))
            .unwrap();
        filter
            .apply_outbound(ctx("public.b"), b"b".to_vec())
            .unwrap();

        std::thread::sleep(Duration::from_millis(10));

        let sent = filter.on_timer(timer(0)).unwrap();
        assert_eq!(
            sent,
            vec![FilterSendMessage::new(
                "tenant_1",
                "public.b",
                b"b".to_vec()
            )]
        );
    }

    #[test]
    fn bandwidth_limiter_has_options() {
        let filter =
            BandwidthLimiterFilter::with_max_burst(100, 25, Duration::from_millis(30), 7, 512);

        assert_eq!(filter.id(), "bandwidth_limiter.v1");
        assert_eq!(filter.bandwidth_bytes_per_sec(), 100);
        assert_eq!(filter.max_burst_bytes(), 25);
        assert_eq!(filter.max_queue_delay(), Duration::from_millis(30));
        assert_eq!(filter.max_queue_messages(), 7);
        assert_eq!(filter.max_queue_bytes(), 512);
    }

    #[test]
    fn filter_timer_mode_has_high_frequency_modes() {
        assert_eq!(FilterTimerMode::HighFrequency500Hz.hz(), 500);
        assert_eq!(
            FilterTimerMode::HighFrequency500Hz.interval(),
            Duration::from_millis(2)
        );
        assert_eq!(FilterTimerMode::UltraLowLatency1000Hz.hz(), 1_000);
        assert_eq!(
            FilterTimerMode::UltraLowLatency1000Hz.interval(),
            Duration::from_millis(1)
        );
    }

    #[test]
    fn filter_timer_mode_default_stays_100hz() {
        assert_eq!(FilterTimerMode::default(), FilterTimerMode::SdkDefault100Hz);
        assert_eq!(FilterTimerMode::default().hz(), 100);
    }
}
