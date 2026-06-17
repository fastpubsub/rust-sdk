// Copyright (c) 2026 FastPubSub Network
// SPDX-License-Identifier: MIT OR Apache-2.0
// Project: https://fastpubsub.com

//! Filter that passes only the latest message from each client.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use chacha20poly1305::aead::{rand_core::RngCore, OsRng};

use super::{
    emit_filter_notice, FilterError, FilterInboundResult, FilterNotice, FilterSendMessage,
    FilterTimerContext, FilterTrait, RouteContext,
};

/// Default counter lifetime in seconds.
pub const LATEST_ONLY_DEFAULT_RESET_TIMEOUT_SECS: u64 = 60;

const LATEST_ONLY_MAGIC: &[u8] = b"FPSLATE1";
const U64_BYTES: usize = 8;
const HEADER_BYTES: usize = 24;

/// Policy for an inbound message without a valid prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum InvalidPrefixPolicy {
    /// Drop the message without an error.
    Drop,
    /// Pass the message through unchanged.
    #[default]
    PassThrough,
    /// Return a filter error. The transport will emit an error event.
    ErrorEventAndDrop,
}

/// Filter that adds a sequence number to outbound messages.
///
/// The number consists of a random `client_id` and a message counter. The
/// outbound counter is stored separately for each `tenant + channel` pair.
///
/// On inbound, the filter keeps the last accepted counter for
/// `tenant + channel + client_id`. A message passes only when its counter is
/// greater than the last accepted one.
///
/// # Example
///
/// ```ignore
/// use std::time::Duration;
///
/// use fastpubsub_sdk::client::create_web_socket;
/// use fastpubsub_sdk::filters::LatestOnlyFilter;
///
/// let client = create_web_socket("overlay_name", "AT_token")
///     .add_filter(
///         "tenant_1",
///         "player.position.",
///         LatestOnlyFilter::new(Duration::from_millis(10_000)),
///     )
///     .build()
///     .await?;
/// ```
///
/// If an inbound message does not contain this filter header (sent without LatestOnlyFilter),
/// handling depends on `invalid_prefix_policy`:
/// - [`Drop`](InvalidPrefixPolicy::Drop): the message is dropped without an error.
/// - [`PassThrough`](InvalidPrefixPolicy::PassThrough) (default): the message is passed through unchanged.
/// - [`ErrorEventAndDrop`](InvalidPrefixPolicy::ErrorEventAndDrop): the filter returns an error and the transport emits an error event.
///
/// If a message has a valid header but its counter is less than or equal to the
/// last accepted one for that (`tenant + channel + client_id`) pair, it is
/// considered stale. The filter emits a warning and the message does not reach
/// the application. This is useful for debugging streams such as "player position",
/// "sensor data", and similar.
///
pub struct LatestOnlyFilter {
    reset_timeout: Duration,
    invalid_prefix_policy: InvalidPrefixPolicy,
    state: Mutex<LatestOnlyState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RouteKey {
    tenant: String,
    channel: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct InboundKey {
    tenant: String,
    channel: String,
    client_id: u64,
}

struct LatestOnlyState {
    outbound: HashMap<RouteKey, OutboundCounter>,
    inbound: HashMap<InboundKey, InboundCounter>,
}

struct OutboundCounter {
    client_id: u64,
    counter: u64,
    last_seen: Instant,
}

struct InboundCounter {
    last_counter: u64,
    last_seen: Instant,
}

struct DecodedLatestOnly<'a> {
    client_id: u64,
    counter: u64,
    payload: &'a [u8],
}

impl LatestOnlyFilter {
    /// Creates a filter with a counter reset timeout.
    pub fn new(reset_timeout: Duration) -> Self {
        Self {
            reset_timeout,
            invalid_prefix_policy: InvalidPrefixPolicy::default(),
            state: Mutex::new(LatestOnlyState {
                outbound: HashMap::new(),
                inbound: HashMap::new(),
            }),
        }
    }

    /// Creates a filter with the default reset timeout.
    pub fn with_default_timeout() -> Self {
        Self::new(Duration::from_secs(LATEST_ONLY_DEFAULT_RESET_TIMEOUT_SECS))
    }

    /// Returns the counter reset timeout.
    pub fn reset_timeout(&self) -> Duration {
        self.reset_timeout
    }

    /// Returns the invalid-prefix policy.
    pub fn invalid_prefix_policy(&self) -> InvalidPrefixPolicy {
        self.invalid_prefix_policy
    }

    /// Sets the invalid-prefix policy.
    pub fn with_invalid_prefix_policy(mut self, policy: InvalidPrefixPolicy) -> Self {
        self.invalid_prefix_policy = policy;
        self
    }

    /// Handles a message without a valid prefix.
    fn handle_invalid_prefix(&self, payload: Vec<u8>) -> Result<FilterInboundResult, FilterError> {
        match self.invalid_prefix_policy {
            InvalidPrefixPolicy::Drop => Ok(FilterInboundResult::new(Vec::new(), Vec::new())),
            InvalidPrefixPolicy::PassThrough => Ok(FilterInboundResult::single(payload)),
            InvalidPrefixPolicy::ErrorEventAndDrop => Err(FilterError::Other(
                "latest only prefix is missing or invalid".into(),
            )),
        }
    }
}

impl Default for LatestOnlyFilter {
    /// Creates a filter with default settings.
    fn default() -> Self {
        Self::with_default_timeout()
    }
}

impl FilterTrait for LatestOnlyFilter {
    /// Returns the stable filter identifier.
    fn id(&self) -> &'static str {
        "latest_only.v1"
    }

    /// Adds a prefix with `client_id` and counter to the outbound payload.
    fn apply_outbound(
        &self,
        ctx: RouteContext<'_>,
        payload: Vec<u8>,
    ) -> Result<Vec<Vec<u8>>, FilterError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| FilterError::Other("failed to lock latest only filter state".into()))?;
        let now = Instant::now();
        let key = route_key(ctx);
        let entry = state
            .outbound
            .entry(key)
            .or_insert_with(|| OutboundCounter {
                client_id: new_client_id(),
                counter: 0,
                last_seen: now,
            });

        if now.duration_since(entry.last_seen) >= self.reset_timeout {
            entry.client_id = new_client_id();
            entry.counter = 0;
        }
        if entry.counter == u64::MAX {
            entry.client_id = new_client_id();
            entry.counter = 0;
        }

        entry.counter += 1;
        entry.last_seen = now;
        let out = encode_latest_only(entry.client_id, entry.counter, &payload);
        Ok(vec![out])
    }

    /// Checks the inbound sequence number and passes only a new message.
    fn apply_inbound(
        &self,
        ctx: RouteContext<'_>,
        payload: Vec<u8>,
    ) -> Result<FilterInboundResult, FilterError> {
        let Some(decoded) = decode_latest_only(&payload) else {
            return self.handle_invalid_prefix(payload);
        };

        let mut state = self
            .state
            .lock()
            .map_err(|_| FilterError::Other("failed to lock latest only filter state".into()))?;
        let now = Instant::now();
        let key = InboundKey {
            tenant: ctx.tenant.to_string(),
            channel: ctx.channel.to_string(),
            client_id: decoded.client_id,
        };

        let (is_stale, last_counter) = {
            let entry = state.inbound.entry(key).or_insert_with(|| InboundCounter {
                last_counter: 0,
                last_seen: now,
            });
            if now.duration_since(entry.last_seen) >= self.reset_timeout {
                entry.last_counter = 0;
            }
            entry.last_seen = now;

            if decoded.counter <= entry.last_counter {
                (true, entry.last_counter)
            } else {
                entry.last_counter = decoded.counter;
                (false, entry.last_counter)
            }
        };

        if is_stale {
            emit_filter_notice(FilterNotice::warning(format!(
                    "latest only dropped stale message: tenant={}, channel={}, client_id={}, counter={}, last_counter={}",
                    ctx.tenant, ctx.channel, decoded.client_id, decoded.counter, last_counter
                )));
            return Ok(FilterInboundResult::new(Vec::new(), Vec::new()));
        }

        Ok(FilterInboundResult::single(decoded.payload.to_vec()))
    }

    /// Removes outbound and inbound counters that have not been seen recently.
    fn on_timer(&self, _ctx: FilterTimerContext) -> Result<Vec<FilterSendMessage>, FilterError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| FilterError::Other("failed to lock latest only filter state".into()))?;
        let now = Instant::now();
        state
            .outbound
            .retain(|_, entry| now.duration_since(entry.last_seen) < self.reset_timeout);
        state
            .inbound
            .retain(|_, entry| now.duration_since(entry.last_seen) < self.reset_timeout);
        Ok(Vec::new())
    }
}

/// Builds a route key.
fn route_key(ctx: RouteContext<'_>) -> RouteKey {
    RouteKey {
        tenant: ctx.tenant.to_string(),
        channel: ctx.channel.to_string(),
    }
}

/// Generates a random client id.
fn new_client_id() -> u64 {
    OsRng.next_u64()
}

/// Encodes a payload with a LatestOnly prefix.
fn encode_latest_only(client_id: u64, counter: u64, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_BYTES + payload.len());
    out.extend_from_slice(LATEST_ONLY_MAGIC);
    out.extend_from_slice(&client_id.to_be_bytes());
    out.extend_from_slice(&counter.to_be_bytes());
    out.extend_from_slice(payload);
    out
}

/// Decodes a payload with a LatestOnly prefix.
fn decode_latest_only(payload: &[u8]) -> Option<DecodedLatestOnly<'_>> {
    if payload.len() < LATEST_ONLY_MAGIC.len() {
        return None;
    }
    if &payload[..LATEST_ONLY_MAGIC.len()] != LATEST_ONLY_MAGIC {
        return None;
    }
    if payload.len() < HEADER_BYTES {
        return None;
    }

    let mut offset = LATEST_ONLY_MAGIC.len();
    let client_id = read_u64(payload, offset)?;
    offset += U64_BYTES;
    let counter = read_u64(payload, offset)?;
    offset += U64_BYTES;

    Some(DecodedLatestOnly {
        client_id,
        counter,
        payload: &payload[offset..],
    })
}

/// Reads a `u64` in network byte order.
fn read_u64(payload: &[u8], offset: usize) -> Option<u64> {
    let bytes = payload.get(offset..offset + U64_BYTES)?;
    let mut out = [0u8; U64_BYTES];
    out.copy_from_slice(bytes);
    Some(u64::from_be_bytes(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filters::{with_filter_notice_queue, FilterTimerMode};

    /// Builds a test route context.
    fn ctx(channel: &'static str) -> RouteContext<'static> {
        RouteContext {
            tenant: "tenant_1",
            channel,
        }
    }

    /// Builds a test timer context.
    fn timer(ms: u64) -> FilterTimerContext {
        FilterTimerContext {
            mode: FilterTimerMode::SdkDefault100Hz,
            interval: Duration::from_millis(ms),
        }
    }

    #[test]
    fn latest_only_encodes_and_decodes_payload() {
        let encoded = encode_latest_only(7, 3, b"hello");
        let decoded = decode_latest_only(&encoded).unwrap();

        assert_eq!(decoded.client_id, 7);
        assert_eq!(decoded.counter, 3);
        assert_eq!(decoded.payload, b"hello");
    }

    #[test]
    fn latest_only_adds_counter_to_outbound_payload() {
        let filter = LatestOnlyFilter::new(Duration::from_millis(1000));

        let first = filter
            .apply_outbound(ctx("player.1"), b"first".to_vec())
            .unwrap();
        let second = filter
            .apply_outbound(ctx("player.1"), b"second".to_vec())
            .unwrap();

        let first_decoded = decode_latest_only(&first[0]).unwrap();
        let second_decoded = decode_latest_only(&second[0]).unwrap();
        assert_eq!(first_decoded.counter, 1);
        assert_eq!(second_decoded.counter, 2);
        assert_eq!(first_decoded.client_id, second_decoded.client_id);
        assert_eq!(second_decoded.payload, b"second");
    }

    #[test]
    fn latest_only_keeps_separate_outbound_counter_per_route() {
        let filter = LatestOnlyFilter::new(Duration::from_millis(1000));

        let first = filter
            .apply_outbound(ctx("player.1"), b"first".to_vec())
            .unwrap();
        let second = filter
            .apply_outbound(ctx("player.2"), b"second".to_vec())
            .unwrap();

        let first_decoded = decode_latest_only(&first[0]).unwrap();
        let second_decoded = decode_latest_only(&second[0]).unwrap();
        assert_eq!(first_decoded.counter, 1);
        assert_eq!(second_decoded.counter, 1);
    }

    #[test]
    fn latest_only_passes_newer_inbound_payload() {
        let filter = LatestOnlyFilter::new(Duration::from_millis(1000));
        let encoded = encode_latest_only(9, 1, b"new");

        let result = filter.apply_inbound(ctx("player.1"), encoded).unwrap();

        assert_eq!(result.payloads, vec![b"new".to_vec()]);
    }

    #[test]
    fn latest_only_creates_warning_for_old_inbound_payload() {
        let filter = LatestOnlyFilter::new(Duration::from_millis(1000));
        let newer = encode_latest_only(9, 2, b"newer");
        let older = encode_latest_only(9, 1, b"older");

        let first = filter.apply_inbound(ctx("player.1"), newer).unwrap();
        let (second, notices) =
            with_filter_notice_queue(|| filter.apply_inbound(ctx("player.1"), older).unwrap());

        assert_eq!(first.payloads, vec![b"newer".to_vec()]);
        assert!(second.payloads.is_empty());
        assert_eq!(notices.len(), 1);
        assert!(notices[0]
            .message
            .contains("latest only dropped stale message"));
        assert!(notices[0].message.contains("counter=1"));
        assert!(notices[0].message.contains("last_counter=2"));
    }

    #[test]
    fn latest_only_resets_inbound_counter_after_timeout() {
        let filter = LatestOnlyFilter::new(Duration::from_millis(5));
        let first = encode_latest_only(9, 2, b"newer");
        let second = encode_latest_only(9, 1, b"after_reset");

        assert_eq!(
            filter
                .apply_inbound(ctx("player.1"), first)
                .unwrap()
                .payloads,
            vec![b"newer".to_vec()]
        );
        std::thread::sleep(Duration::from_millis(10));
        filter.on_timer(timer(1)).unwrap();
        let result = filter.apply_inbound(ctx("player.1"), second).unwrap();

        assert_eq!(result.payloads, vec![b"after_reset".to_vec()]);
    }

    #[test]
    fn latest_only_resets_outbound_counter_after_timeout() {
        let filter = LatestOnlyFilter::new(Duration::from_millis(5));
        let first = filter
            .apply_outbound(ctx("player.1"), b"first".to_vec())
            .unwrap();
        let first_decoded = decode_latest_only(&first[0]).unwrap();

        std::thread::sleep(Duration::from_millis(10));
        let second = filter
            .apply_outbound(ctx("player.1"), b"second".to_vec())
            .unwrap();
        let second_decoded = decode_latest_only(&second[0]).unwrap();

        assert_eq!(second_decoded.counter, 1);
        assert_ne!(first_decoded.client_id, second_decoded.client_id);
    }

    #[test]
    fn latest_only_passes_invalid_prefix_by_default() {
        let filter = LatestOnlyFilter::new(Duration::from_millis(1000));

        let result = filter
            .apply_inbound(ctx("player.1"), b"plain".to_vec())
            .unwrap();

        assert_eq!(result.payloads, vec![b"plain".to_vec()]);
    }

    #[test]
    fn latest_only_can_drop_invalid_prefix() {
        let filter = LatestOnlyFilter::new(Duration::from_millis(1000))
            .with_invalid_prefix_policy(InvalidPrefixPolicy::Drop);

        let result = filter
            .apply_inbound(ctx("player.1"), b"plain".to_vec())
            .unwrap();

        assert!(result.payloads.is_empty());
    }

    #[test]
    fn latest_only_can_return_error_for_invalid_prefix() {
        let filter = LatestOnlyFilter::new(Duration::from_millis(1000))
            .with_invalid_prefix_policy(InvalidPrefixPolicy::ErrorEventAndDrop);

        let result = filter.apply_inbound(ctx("player.1"), b"plain".to_vec());

        assert!(result.is_err());
    }
}
