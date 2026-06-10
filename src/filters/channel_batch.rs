// Copyright (c) 2026 FastPubSub Network
// SPDX-License-Identifier: MIT OR Apache-2.0
// Project: https://fastpubsub.com

use std::collections::{hash_map::Entry, HashMap};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::metadata::{MetaWriter, META_RECORD_CHANNEL_BATCH};

use super::{
    FilterError, FilterInboundResult, FilterSendMessage, FilterTimerContext, FilterTrait,
    RouteContext,
};

/// Default batch size.
pub const CHANNEL_BATCH_DEFAULT_MAX_BYTES: usize = 1024;

const BATCH_MAGIC: &[u8] = b"FPSBATCH1";
const U32_BYTES: usize = 4;

/// Filter that groups small messages from one channel into one batch.
///
/// The filter works in pairs: the sender encodes a batch, and the receiver with
/// the same filter decodes it back into the original messages.
///
/// # What it does
///
/// Without this filter, every small publish is sent as a separate transport
/// frame with its own tenant, channel, and delivery overhead. With this filter,
/// several payloads for the same `tenant + channel` are delayed for a short time
/// and then sent as one payload on that same channel.
///
/// For example, three small messages:
///
/// ```text
/// public.chat -> "a"
/// public.chat -> "b"
/// public.chat -> "c"
/// ```
///
/// can be sent as one transport message:
///
/// ```text
/// public.chat -> batch("a", "b", "c")
/// ```
///
/// The receiver decodes the batch and delivers the original messages to the
/// local subscriber in the same order:
///
/// ```text
/// "a"
/// "b"
/// "c"
/// ```
///
/// # When it helps
///
/// This is useful for many small messages on the same channel, for example
/// game state deltas, cursor positions, small chat events, metrics, or telemetry.
/// The main saving is that tenant and channel names are written once for the
/// whole batch, and the transport has fewer frames to deliver.
///
/// `max_bytes` is a hard boundary for the encoded batch size. It includes the
/// batch header and per-message length fields, not only the useful payload bytes.
/// A batch is flushed before a new message would cross the limit, so older
/// buffered messages keep their order before the new message.
///
/// If one payload is larger than `max_bytes`, it is still sent immediately as a
/// batch with one message. The receiver always sees the same batch format and can
/// decode the stream without a special case.
///
/// `flush_interval` is the maximum time a small buffered message can wait when
/// the size limit is not reached.
///
/// # Locking and WebTransport
///
/// The filter keeps a short `Mutex` lock around channel buffers. For WebSocket
/// this is usually not a problem, because filters run from one transport task.
/// For future WebTransport with several tasks, do not put one filter over the
/// whole channel tree. Prefer several filters for different channel prefixes,
/// with separate flush intervals or size limits.
///
/// # Example
///
/// ```ignore
/// use std::time::Duration;
///
/// use fastpubsub_sdk::client::create_web_socket;
/// use fastpubsub_sdk::filters::ChannelBatchFilter;
///
/// let fastpubsub = create_web_socket("overlay_name", "AT_token")
///     .add_filter(
///         "tenant_1",
///         "public.",
///         ChannelBatchFilter::with_max_bytes(
///             Duration::from_millis(10),
///             1024,
///         ),
///     )
///     .build()
///     .await?;
/// ```
pub struct ChannelBatchFilter {
    flush_interval: Duration,
    max_bytes: usize,
    state: Mutex<ChannelBatchState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ChannelBatchKey {
    tenant: String,
    channel: String,
}

struct ChannelBatchState {
    entries: HashMap<ChannelBatchKey, ChannelBatchEntry>,
}

struct ChannelBatchEntry {
    created_at: Instant,
    encoded_len: usize,
    payloads: Vec<Vec<u8>>,
}

impl ChannelBatchEntry {
    /// Creates an empty buffer for one channel.
    fn new(now: Instant) -> Self {
        Self {
            created_at: now,
            encoded_len: batch_header_len(),
            payloads: Vec::new(),
        }
    }

    /// Adds a payload to the buffer.
    fn push(&mut self, payload: Vec<u8>) {
        self.encoded_len += U32_BYTES + payload.len();
        self.payloads.push(payload);
    }

    /// Checks whether the buffer must be sent by time.
    fn is_expired(&self, now: Instant, flush_interval: Duration) -> bool {
        now.duration_since(self.created_at) >= flush_interval
    }
}

impl ChannelBatchFilter {
    /// Creates a filter with the default size limit.
    pub fn new(flush_interval: Duration) -> Self {
        Self::with_max_bytes(flush_interval, CHANNEL_BATCH_DEFAULT_MAX_BYTES)
    }

    /// Creates a filter with an interval in milliseconds.
    pub fn from_millis(flush_interval_ms: u64) -> Self {
        Self::new(Duration::from_millis(flush_interval_ms))
    }

    /// Creates a filter with a flush interval and batch size limit.
    pub fn with_max_bytes(flush_interval: Duration, max_bytes: usize) -> Self {
        Self {
            flush_interval,
            max_bytes,
            state: Mutex::new(ChannelBatchState {
                entries: HashMap::new(),
            }),
        }
    }

    /// Returns the buffer flush interval.
    pub fn flush_interval(&self) -> Duration {
        self.flush_interval
    }

    /// Returns the batch size limit.
    pub fn max_bytes(&self) -> usize {
        self.max_bytes
    }
}

impl Default for ChannelBatchFilter {
    fn default() -> Self {
        Self::new(Duration::from_millis(10))
    }
}

impl FilterTrait for ChannelBatchFilter {
    fn id(&self) -> &'static str {
        "channel_batch.v1"
    }

    fn apply_outbound(
        &self,
        ctx: RouteContext<'_>,
        payload: Vec<u8>,
    ) -> Result<Vec<Vec<u8>>, FilterError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| FilterError::Other("failed to lock channel batch filter state".into()))?;

        let key = ChannelBatchKey {
            tenant: ctx.tenant.to_string(),
            channel: ctx.channel.to_string(),
        };
        let batches = match state.entries.entry(key) {
            Entry::Occupied(mut occupied) => {
                let payload_len = payload_item_len(&payload);
                if occupied.get().encoded_len + payload_len <= self.max_bytes {
                    let entry = occupied.get_mut();
                    entry.push(payload);
                    if entry.encoded_len >= self.max_bytes {
                        let entry = occupied.remove();

                        vec![encode_batch(entry.payloads)?]
                    } else {
                        Vec::new()
                    }
                } else {
                    let (key, entry) = occupied.remove_entry();
                    let mut batches = vec![encode_batch(entry.payloads)?];
                    let mut entry = ChannelBatchEntry::new(Instant::now());
                    entry.push(payload);
                    if entry.encoded_len < self.max_bytes {
                        state.entries.insert(key, entry);
                    } else {
                        batches.push(encode_batch(entry.payloads)?);
                    }

                    batches
                }
            }
            Entry::Vacant(vacant) => {
                let mut entry = ChannelBatchEntry::new(Instant::now());
                entry.push(payload);
                if entry.encoded_len < self.max_bytes {
                    vacant.insert(entry);

                    Vec::new()
                } else {
                    vec![encode_batch(entry.payloads)?]
                }
            }
        };
        Ok(batches)
    }

    fn apply_inbound(
        &self,
        _ctx: RouteContext<'_>,
        payload: Vec<u8>,
    ) -> Result<FilterInboundResult, FilterError> {
        let payloads = decode_batch(&payload)?;
        Ok(FilterInboundResult::new(payloads, Vec::new()))
    }

    fn apply_inbound_with_meta(
        &self,
        _ctx: RouteContext<'_>,
        payload: Vec<u8>,
        meta: &mut MetaWriter,
    ) -> Result<FilterInboundResult, FilterError> {
        let payloads = decode_batch(&payload)?;
        let count = u16::try_from(payloads.len()).map_err(|_| {
            FilterError::BatchDecodeFailed("batch contains too many payloads".into())
        })?;
        meta.push_record(META_RECORD_CHANNEL_BATCH, &count.to_be_bytes())
            .map_err(|e| FilterError::Other(e.to_string()))?;
        Ok(FilterInboundResult::new(payloads, Vec::new()))
    }

    fn on_timer(&self, _ctx: FilterTimerContext) -> Result<Vec<FilterSendMessage>, FilterError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| FilterError::Other("failed to lock channel batch filter state".into()))?;

        let now = Instant::now();
        let expired_keys: Vec<ChannelBatchKey> = state
            .entries
            .iter()
            .filter(|(_, entry)| entry.is_expired(now, self.flush_interval))
            .map(|(key, _)| key.clone())
            .collect();

        let mut messages = Vec::new();
        for key in expired_keys {
            let Some(entry) = state.entries.remove(&key) else {
                continue;
            };
            let payload = encode_batch(entry.payloads)?;
            messages.push(FilterSendMessage::new(key.tenant, key.channel, payload));
        }

        Ok(messages)
    }
}

/// Returns the batch header size.
fn batch_header_len() -> usize {
    BATCH_MAGIC.len() + U32_BYTES
}

/// Returns the encoded size of one payload inside a batch.
fn payload_item_len(payload: &[u8]) -> usize {
    U32_BYTES + payload.len()
}

/// Encodes several payloads into one batch.
fn encode_batch(payloads: Vec<Vec<u8>>) -> Result<Vec<u8>, FilterError> {
    let count = u32::try_from(payloads.len())
        .map_err(|_| FilterError::BatchEncodeFailed("too many messages in channel batch".into()))?;
    let mut encoded_len = batch_header_len();
    for payload in &payloads {
        u32::try_from(payload.len()).map_err(|_| {
            FilterError::BatchEncodeFailed("message is too large for channel batch".into())
        })?;
        encoded_len += U32_BYTES + payload.len();
    }

    let mut out = Vec::with_capacity(encoded_len);
    out.extend_from_slice(BATCH_MAGIC);
    out.extend_from_slice(&count.to_be_bytes());
    for payload in payloads {
        let len = payload.len() as u32;
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(&payload);
    }
    Ok(out)
}

/// Decodes a batch back into the original payload list.
fn decode_batch(payload: &[u8]) -> Result<Vec<Vec<u8>>, FilterError> {
    if payload.len() < batch_header_len() {
        return Err(FilterError::BatchDecodeFailed(
            "channel batch is too short".into(),
        ));
    }
    if &payload[..BATCH_MAGIC.len()] != BATCH_MAGIC {
        return Err(FilterError::BatchDecodeFailed(
            "channel batch has bad magic".into(),
        ));
    }

    let mut index = BATCH_MAGIC.len();
    let count = read_u32(payload, &mut index)? as usize;
    let mut payloads = Vec::with_capacity(count);
    for _ in 0..count {
        let len = read_u32(payload, &mut index)? as usize;
        let end = index.checked_add(len).ok_or_else(|| {
            FilterError::BatchDecodeFailed("channel batch length is too large".into())
        })?;
        if end > payload.len() {
            return Err(FilterError::BatchDecodeFailed(
                "channel batch message is truncated".into(),
            ));
        }
        payloads.push(payload[index..end].to_vec());
        index = end;
    }

    if index != payload.len() {
        return Err(FilterError::BatchDecodeFailed(
            "channel batch has extra bytes".into(),
        ));
    }
    Ok(payloads)
}

/// Reads a `u32` number from the batch.
fn read_u32(payload: &[u8], index: &mut usize) -> Result<u32, FilterError> {
    let end = index
        .checked_add(U32_BYTES)
        .ok_or_else(|| FilterError::BatchDecodeFailed("channel batch index is too large".into()))?;
    if end > payload.len() {
        return Err(FilterError::BatchDecodeFailed(
            "channel batch number is truncated".into(),
        ));
    }

    let bytes: [u8; U32_BYTES] = payload[*index..end]
        .try_into()
        .map_err(|_| FilterError::BatchDecodeFailed("channel batch number is invalid".into()))?;
    *index = end;
    Ok(u32::from_be_bytes(bytes))
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
    fn channel_batch_filter_waits_until_limit() {
        let filter = ChannelBatchFilter::with_max_bytes(Duration::from_secs(1), 64);

        let first = filter
            .apply_outbound(ctx("public.chat"), b"one".to_vec())
            .unwrap();
        let second = filter
            .apply_outbound(ctx("public.chat"), b"two".to_vec())
            .unwrap();

        assert!(first.is_empty());
        assert!(second.is_empty());
    }

    #[test]
    fn channel_batch_filter_flushes_by_size() {
        let filter = ChannelBatchFilter::with_max_bytes(Duration::from_secs(1), 27);

        assert!(filter
            .apply_outbound(ctx("public.chat"), b"one".to_vec())
            .unwrap()
            .is_empty());
        let sent = filter
            .apply_outbound(ctx("public.chat"), b"two".to_vec())
            .unwrap();

        assert_eq!(sent.len(), 1);
        let decoded = filter
            .apply_inbound(ctx("public.chat"), sent[0].clone())
            .unwrap();
        assert_eq!(decoded.payloads, vec![b"one".to_vec(), b"two".to_vec()]);
    }

    #[test]
    fn channel_batch_filter_flushes_buffer_before_large_payload() {
        let filter = ChannelBatchFilter::with_max_bytes(Duration::from_secs(1), 24);

        assert!(filter
            .apply_outbound(ctx("public.chat"), b"one".to_vec())
            .unwrap()
            .is_empty());
        let sent = filter
            .apply_outbound(ctx("public.chat"), b"large-data".to_vec())
            .unwrap();

        assert_eq!(sent.len(), 2);
        let first = filter
            .apply_inbound(ctx("public.chat"), sent[0].clone())
            .unwrap();
        let second = filter
            .apply_inbound(ctx("public.chat"), sent[1].clone())
            .unwrap();
        assert_eq!(first.payloads, vec![b"one".to_vec()]);
        assert_eq!(second.payloads, vec![b"large-data".to_vec()]);
    }

    #[test]
    fn channel_batch_filter_starts_new_buffer_after_limit_cross() {
        let filter = ChannelBatchFilter::with_max_bytes(Duration::from_millis(0), 24);

        filter
            .apply_outbound(ctx("public.chat"), b"one".to_vec())
            .unwrap();
        let sent = filter
            .apply_outbound(ctx("public.chat"), b"two".to_vec())
            .unwrap();

        assert_eq!(sent.len(), 1);
        let first = filter
            .apply_inbound(ctx("public.chat"), sent[0].clone())
            .unwrap();
        assert_eq!(first.payloads, vec![b"one".to_vec()]);

        let sent = filter.on_timer(timer(0)).unwrap();
        assert_eq!(sent.len(), 1);
        let second = filter
            .apply_inbound(ctx("public.chat"), sent[0].payload.clone())
            .unwrap();
        assert_eq!(second.payloads, vec![b"two".to_vec()]);
    }

    #[test]
    fn channel_batch_filter_uses_network_byte_order() {
        let encoded = encode_batch(vec![b"one".to_vec(), b"two".to_vec()]).unwrap();

        assert_eq!(&encoded[..BATCH_MAGIC.len()], BATCH_MAGIC);
        assert_eq!(&encoded[9..13], &[0, 0, 0, 2]);
        assert_eq!(&encoded[13..17], &[0, 0, 0, 3]);
        assert_eq!(&encoded[17..20], b"one");
        assert_eq!(&encoded[20..24], &[0, 0, 0, 3]);
        assert_eq!(&encoded[24..27], b"two");
    }

    #[test]
    fn channel_batch_filter_flushes_by_timer() {
        let filter = ChannelBatchFilter::with_max_bytes(Duration::from_millis(5), 1024);

        filter
            .apply_outbound(ctx("public.chat"), b"one".to_vec())
            .unwrap();
        filter
            .apply_outbound(ctx("public.chat"), b"two".to_vec())
            .unwrap();
        std::thread::sleep(Duration::from_millis(10));

        let sent = filter.on_timer(timer(10)).unwrap();

        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].tenant, "tenant_1");
        assert_eq!(sent[0].channel, "public.chat");
        let decoded = filter
            .apply_inbound(ctx("public.chat"), sent[0].payload.clone())
            .unwrap();
        assert_eq!(decoded.payloads, vec![b"one".to_vec(), b"two".to_vec()]);
    }

    #[test]
    fn channel_batch_filter_keeps_channels_separate() {
        let filter = ChannelBatchFilter::with_max_bytes(Duration::from_millis(5), 1024);

        filter
            .apply_outbound(ctx("public.one"), b"one".to_vec())
            .unwrap();
        filter
            .apply_outbound(ctx("public.two"), b"two".to_vec())
            .unwrap();
        std::thread::sleep(Duration::from_millis(10));

        let sent = filter.on_timer(timer(10)).unwrap();

        assert_eq!(sent.len(), 2);
        let mut channels: Vec<&str> = sent
            .iter()
            .map(|message| message.channel.as_str())
            .collect();
        channels.sort();
        assert_eq!(channels, vec!["public.one", "public.two"]);
    }

    #[test]
    fn channel_batch_filter_reports_bad_payload() {
        let filter = ChannelBatchFilter::default();

        let err = filter
            .apply_inbound(ctx("public.chat"), b"plain".to_vec())
            .unwrap_err();

        assert!(matches!(err, FilterError::BatchDecodeFailed(_)));
    }

    #[test]
    fn channel_batch_filter_has_options() {
        let filter = ChannelBatchFilter::with_max_bytes(Duration::from_millis(33), 2048);

        assert_eq!(filter.id(), "channel_batch.v1");
        assert_eq!(filter.flush_interval(), Duration::from_millis(33));
        assert_eq!(filter.max_bytes(), 2048);
    }
}
