// Copyright (c) 2026 FastPubSub Network
// SPDX-License-Identifier: MIT OR Apache-2.0
// Project: https://fastpubsub.com

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use super::{FilterError, FilterSendMessage, FilterTimerContext, FilterTrait, RouteContext};

/// Send filter that emits at most one message for each interval.
///
/// The filter works for outbound messages. It stores the latest payload for
/// each `tenant + channel` pair. If the interval already passed, the new
/// message is sent at once and the interval starts again. If the interval did
/// not pass yet, the filter keeps only the latest message for the next timer.
///
/// Filters that run before `SendRateFilter` in the outbound pipeline process
/// the payload before the delay. The rest of the outbound pipeline runs after
/// the timer.
///
/// # Example
///
/// ```ignore
/// use fastpubsub_sdk::client::create_web_socket;
/// use fastpubsub_sdk::filters::SendRateFilter;
///
/// let fastpubsub = create_web_socket("overlay_name", "AT_token")
///     .add_filter("tenant_1", "player.", SendRateFilter::from_millis(33))
///     .build()
///     .await?;
/// ```
pub struct SendRateFilter {
    min_interval: Duration,
    state: Mutex<SendRateState>,
}

/// Old short name for the send rate filter.
pub type SendRateLimit = SendRateFilter;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SendRateKey {
    tenant: String,
    channel: String,
}

struct SendRateState {
    entries: BTreeMap<SendRateKey, SendRateEntry>,
}

struct SendRateEntry {
    pending: Option<FilterSendMessage>,
    last_sent: Option<Instant>,
}

impl SendRateEntry {
    fn new() -> Self {
        Self {
            pending: None,
            last_sent: None,
        }
    }
}

impl SendRateFilter {
    /// Creates a filter with the minimum time between sends.
    pub fn new(min_interval: Duration) -> Self {
        Self {
            min_interval,
            state: Mutex::new(SendRateState {
                entries: BTreeMap::new(),
            }),
        }
    }

    /// Creates a filter with the interval in milliseconds.
    pub fn from_millis(min_interval_ms: u64) -> Self {
        Self::new(Duration::from_millis(min_interval_ms))
    }

    /// Returns the minimum time between sends.
    pub fn min_interval(&self) -> Duration {
        self.min_interval
    }
}

impl FilterTrait for SendRateFilter {
    fn id(&self) -> &'static str {
        "send_rate.latest_at_interval"
    }

    fn apply_outbound(
        &self,
        ctx: RouteContext<'_>,
        payload: Vec<u8>,
    ) -> Result<Vec<Vec<u8>>, FilterError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| FilterError::Other("failed to lock send rate filter state".into()))?;

        let key = SendRateKey {
            tenant: ctx.tenant.to_string(),
            channel: ctx.channel.to_string(),
        };
        let now = Instant::now();
        let entry = state.entries.entry(key).or_insert_with(SendRateEntry::new);

        if can_send_now(entry.last_sent, now, self.min_interval) {
            entry.last_sent = Some(now);
            entry.pending = None;
            return Ok(vec![payload]);
        }

        entry.pending = Some(FilterSendMessage::new(ctx.tenant, ctx.channel, payload));
        Ok(Vec::new())
    }

    fn on_timer(&self, _ctx: FilterTimerContext) -> Result<Vec<FilterSendMessage>, FilterError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| FilterError::Other("failed to lock send rate filter state".into()))?;

        let now = Instant::now();
        let mut messages = Vec::new();
        for entry in state.entries.values_mut() {
            if entry.pending.is_none() || !can_send_now(entry.last_sent, now, self.min_interval) {
                continue;
            }
            if let Some(message) = entry.pending.take() {
                entry.last_sent = Some(now);
                messages.push(message);
            }
        }

        Ok(messages)
    }
}

fn can_send_now(last_sent: Option<Instant>, now: Instant, min_interval: Duration) -> bool {
    match last_sent {
        Some(last_sent) => now.duration_since(last_sent) >= min_interval,
        None => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filters::FilterTimerMode;

    fn ctx(channel: &'static str) -> RouteContext<'static> {
        RouteContext {
            tenant: "tenant_1",
            channel,
        }
    }

    fn timer(ms: u64) -> FilterTimerContext {
        FilterTimerContext {
            mode: FilterTimerMode::SdkDefault100Hz,
            interval: Duration::from_millis(ms),
        }
    }

    #[test]
    fn send_rate_filter_sends_first_payload_at_once() {
        let filter = SendRateFilter::from_millis(30);

        let first = filter
            .apply_outbound(ctx("player.42.position"), b"first".to_vec())
            .unwrap();

        assert_eq!(first, vec![b"first".to_vec()]);
    }

    #[test]
    fn send_rate_filter_keeps_latest_payload_until_interval() {
        let filter = SendRateFilter::from_millis(30);

        let first = filter
            .apply_outbound(ctx("player.42.position"), b"first".to_vec())
            .unwrap();
        assert_eq!(first, vec![b"first".to_vec()]);

        let second = filter
            .apply_outbound(ctx("player.42.position"), b"second".to_vec())
            .unwrap();
        assert!(second.is_empty());
        let third = filter
            .apply_outbound(ctx("player.42.position"), b"third".to_vec())
            .unwrap();
        assert!(third.is_empty());

        assert!(filter.on_timer(timer(10)).unwrap().is_empty());
        std::thread::sleep(Duration::from_millis(35));

        let sent = filter.on_timer(timer(10)).unwrap();
        assert_eq!(
            sent,
            vec![FilterSendMessage::new(
                "tenant_1",
                "player.42.position",
                b"third".to_vec()
            )]
        );
    }

    #[test]
    fn send_rate_filter_sends_new_payload_at_once_after_interval() {
        let filter = SendRateFilter::from_millis(5);

        let first = filter
            .apply_outbound(ctx("player.42.position"), b"first".to_vec())
            .unwrap();
        assert_eq!(first, vec![b"first".to_vec()]);

        std::thread::sleep(Duration::from_millis(10));

        let second = filter
            .apply_outbound(ctx("player.42.position"), b"second".to_vec())
            .unwrap();
        assert_eq!(second, vec![b"second".to_vec()]);
        assert!(filter.on_timer(timer(10)).unwrap().is_empty());
    }

    #[test]
    fn send_rate_filter_tracks_channels_separately() {
        let filter = SendRateFilter::from_millis(20);

        let one = filter
            .apply_outbound(ctx("player.1.position"), b"one".to_vec())
            .unwrap();
        let two = filter
            .apply_outbound(ctx("player.2.position"), b"two".to_vec())
            .unwrap();
        assert_eq!(one, vec![b"one".to_vec()]);
        assert_eq!(two, vec![b"two".to_vec()]);

        filter
            .apply_outbound(ctx("player.1.position"), b"one-new".to_vec())
            .unwrap();
        filter
            .apply_outbound(ctx("player.2.position"), b"two-new".to_vec())
            .unwrap();

        std::thread::sleep(Duration::from_millis(25));

        let sent = filter.on_timer(timer(20)).unwrap();
        assert_eq!(
            sent,
            vec![
                FilterSendMessage::new("tenant_1", "player.1.position", b"one-new".to_vec()),
                FilterSendMessage::new("tenant_1", "player.2.position", b"two-new".to_vec()),
            ]
        );
    }

    #[test]
    fn send_rate_filter_has_stable_id_and_interval() {
        let filter = SendRateFilter::from_millis(33);

        assert_eq!(filter.id(), "send_rate.latest_at_interval");
        assert_eq!(filter.min_interval(), Duration::from_millis(33));
    }
}
