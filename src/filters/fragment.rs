// Copyright (c) 2026 FastPubSub Network
// SPDX-License-Identifier: MIT OR Apache-2.0
// Project: https://fastpubsub.com

use std::collections::{hash_map::Entry, HashMap};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use chacha20poly1305::aead::{rand_core::RngCore, OsRng};

use crate::metadata::{MetaWriter, META_RECORD_FRAGMENTATION};

use super::{
    FilterError, FilterInboundResult, FilterSendMessage, FilterTimerContext, FilterTrait,
    RouteContext,
};

/// Default fragment size.
pub const FRAGMENT_DEFAULT_MAX_FRAGMENT_BYTES: usize = 16 * 1024;

/// Default payload size that starts fragmentation.
pub const FRAGMENT_DEFAULT_THRESHOLD_BYTES: usize = 16 * 1024;

/// Default defrag timeout, in seconds.
pub const FRAGMENT_DEFAULT_DEFRAG_TIMEOUT_SECS: u64 = 60;

/// Default delay between restore requests, in milliseconds.
pub const FRAGMENT_DEFAULT_REQUEST_INTERVAL_MS: u64 = 250;

const FRAGMENT_MAGIC: &[u8] = b"FPSFRAG1";
const REQUEST_MAGIC: &[u8] = b"FPSFREQ1";
const MESSAGE_ID_BYTES: usize = 8;
const U32_BYTES: usize = 4;
const CONTROL_MARK: &str = "._contrl.";

/// Filter that splits one large message into small messages.
///
/// The filter fragments only payloads that are not smaller than
/// `fragment_threshold_bytes`.
/// Small payloads pass through without changes.
///
/// `fragment_threshold_bytes` is the payload size that starts fragmentation.
///
/// `max_fragment_bytes` is the maximum size of one encoded fragment. It
/// includes the fragment header and a part of the original payload.
/// The useful chunk size is `max_fragment_bytes - header size`.
///
/// Each large payload gets a random 8-byte `message_id`.
/// The receiver joins fragments with the same `message_id` back into one
/// payload.
///
/// `request_interval` sets how often the receiver asks for missing fragments.
///
/// `defrag_timeout` sets the lifetime of an inbound assembly and the outbound
/// fragment cache. If an inbound message is not complete in this time, the
/// filter removes it and returns a defrag error from the timer.
///
/// Restore requests use a control channel like this:
///
/// ```text
/// public.fotos._contrl.0123456789abcdef
/// ```
///
/// `0123456789abcdef` is the `message_id` in hex.
///
/// # Simple Example
///
/// Put the filter on a common prefix for the main channel and control channel.
/// In this example the filter sees `public.fotos` and
/// `public.fotos._contrl.{message_id}`.
///
/// ```ignore
/// use std::time::Duration;
///
/// use fastpubsub_sdk::client::create_web_socket;
/// use fastpubsub_sdk::filters::FragmentFilter;
///
/// let fragment_threshold_bytes = 64 * 1024;
/// let max_fragment_bytes = 16 * 1024;
/// let request_interval = Duration::from_millis(250);
/// let defrag_timeout = Duration::from_secs(10);
///
/// let fastpubsub = create_web_socket("overlay_name", "AT_token")
///     .add_filter(
///         "tenant_1",
///         "public.fotos",
///         FragmentFilter::new(
///             fragment_threshold_bytes,
///             max_fragment_bytes,
///             request_interval,
///             defrag_timeout,
///         ),
///     )
///     .build()
///     .await?;
/// ```
///
/// # Subscription
///
/// The subscription must cover the main channel and the control channel, so the
/// filter can receive restore requests.
///
/// ```ignore
/// use fastpubsub_sdk::transport::SubscribeOptions;
///
/// let mut rx = fastpubsub
///     .subscribe("tenant_1", "public.fotos.#", &SubscribeOptions::default())
///     .await?;
/// ```
///
/// # Direct Settings
///
/// All sizes and times are set explicitly.
///
/// ```ignore
/// use std::time::Duration;
///
/// use fastpubsub_sdk::filters::FragmentFilter;
///
/// let fragment_threshold_bytes = 64 * 1024;
/// let max_fragment_bytes = 16 * 1024;
/// let request_interval = Duration::from_millis(100);
/// let defrag_timeout = Duration::from_secs(10);
///
/// let filter = FragmentFilter::new(
///     fragment_threshold_bytes,
///     max_fragment_bytes,
///     request_interval,
///     defrag_timeout,
/// );
/// ```
pub struct FragmentFilter {
    fragment_threshold_bytes: usize,
    max_fragment_bytes: usize,
    request_interval: Duration,
    defrag_timeout: Duration,
    state: Mutex<FragmentState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct FragmentMessageKey {
    tenant: String,
    channel: String,
    message_id: [u8; MESSAGE_ID_BYTES],
}

struct FragmentState {
    outbound: HashMap<FragmentMessageKey, OutboundMessage>,
    inbound: HashMap<FragmentMessageKey, InboundMessage>,
    completed: HashMap<FragmentMessageKey, Instant>,
}

struct OutboundMessage {
    created_at: Instant,
    fragments: Vec<Vec<u8>>,
}

struct InboundMessage {
    created_at: Instant,
    original_len: usize,
    fragment_count: u32,
    fragments: Vec<Option<Vec<u8>>>,
    last_request_at: Option<Instant>,
}

struct DecodedFragment<'a> {
    message_id: [u8; MESSAGE_ID_BYTES],
    original_len: usize,
    fragment_index: u32,
    fragment_count: u32,
    chunk: &'a [u8],
}

struct FragmentRequest {
    message_id: [u8; MESSAGE_ID_BYTES],
    indexes: Vec<u32>,
}

impl FragmentFilter {
    /// Creates a fragmentation filter.
    ///
    /// `fragment_threshold_bytes` sets the payload size that starts
    /// fragmentation.
    ///
    /// `max_fragment_bytes` sets the upper limit for each encoded fragment.
    /// This size includes the fragment header.
    ///
    /// `request_interval` sets how often the receiver may ask again for missing
    /// fragments.
    ///
    /// `defrag_timeout` sets when the cache is cleaned and when an error is
    /// returned if an inbound message is not complete.
    pub fn new(
        fragment_threshold_bytes: usize,
        max_fragment_bytes: usize,
        request_interval: Duration,
        defrag_timeout: Duration,
    ) -> Self {
        Self {
            fragment_threshold_bytes,
            max_fragment_bytes,
            request_interval,
            defrag_timeout,
            state: Mutex::new(FragmentState {
                outbound: HashMap::new(),
                inbound: HashMap::new(),
                completed: HashMap::new(),
            }),
        }
    }

    /// Returns the payload size that starts fragmentation.
    pub fn fragment_threshold_bytes(&self) -> usize {
        self.fragment_threshold_bytes
    }

    /// Returns the maximum size of one sent fragment.
    pub fn max_fragment_bytes(&self) -> usize {
        self.max_fragment_bytes
    }

    /// Returns the delay between restore requests.
    pub fn request_interval(&self) -> Duration {
        self.request_interval
    }

    /// Returns the defrag and cache cleanup timeout.
    pub fn defrag_timeout(&self) -> Duration {
        self.defrag_timeout
    }

    /// Handles an inbound payload and writes metadata when needed.
    fn apply_inbound_inner(
        &self,
        ctx: RouteContext<'_>,
        payload: Vec<u8>,
        mut meta: Option<&mut MetaWriter>,
    ) -> Result<FilterInboundResult, FilterError> {
        if let Some((base_channel, channel_message_id)) = parse_control_channel(ctx.channel)? {
            return self.handle_request(ctx.tenant, base_channel, channel_message_id, payload);
        }

        let Some(fragment) = decode_fragment(&payload)? else {
            return Ok(FilterInboundResult::single(payload));
        };

        let message_id = fragment.message_id;
        let original_len = fragment.original_len;
        let fragment_count = fragment.fragment_count;
        let key = FragmentMessageKey {
            tenant: ctx.tenant.to_string(),
            channel: ctx.channel.to_string(),
            message_id,
        };
        let now = Instant::now();
        let mut state = self
            .state
            .lock()
            .map_err(|_| FilterError::Other("failed to lock fragment filter state".into()))?;

        if state.completed.contains_key(&key) {
            return Ok(FilterInboundResult::new(Vec::new(), Vec::new()));
        }

        let completed = {
            let entry = match state.inbound.entry(key.clone()) {
                Entry::Occupied(occupied) => occupied.into_mut(),
                Entry::Vacant(vacant) => vacant.insert(InboundMessage::new(
                    fragment.original_len,
                    fragment.fragment_count,
                    now,
                )?),
            };
            entry.push(fragment)?
        };

        if let Some(payload) = completed {
            state.inbound.remove(&key);
            state.completed.insert(key, now + self.defrag_timeout);
            if let Some(meta) = meta.as_deref_mut() {
                meta.push_record_with(META_RECORD_FRAGMENTATION, |out| {
                    out.extend_from_slice(&message_id);
                    out.extend_from_slice(&(original_len as u32).to_be_bytes());
                    out.extend_from_slice(&fragment_count.to_be_bytes());
                    Ok(())
                })
                .map_err(|e| FilterError::Other(e.to_string()))?;
            }
            return Ok(FilterInboundResult::single(payload));
        }

        Ok(FilterInboundResult::new(Vec::new(), Vec::new()))
    }
}

impl Default for FragmentFilter {
    /// Creates a filter with default settings.
    fn default() -> Self {
        Self::new(
            FRAGMENT_DEFAULT_THRESHOLD_BYTES,
            FRAGMENT_DEFAULT_MAX_FRAGMENT_BYTES,
            Duration::from_millis(FRAGMENT_DEFAULT_REQUEST_INTERVAL_MS),
            Duration::from_secs(FRAGMENT_DEFAULT_DEFRAG_TIMEOUT_SECS),
        )
    }
}

impl FilterTrait for FragmentFilter {
    fn id(&self) -> &'static str {
        "fragment.v1"
    }

    fn apply_outbound(
        &self,
        ctx: RouteContext<'_>,
        payload: Vec<u8>,
    ) -> Result<Vec<Vec<u8>>, FilterError> {
        if is_control_channel(ctx.channel) || payload.len() < self.fragment_threshold_bytes {
            return Ok(vec![payload]);
        }

        let max_chunk = max_chunk_len(self.max_fragment_bytes)?;
        let original_len = u32::try_from(payload.len()).map_err(|_| {
            FilterError::FragmentEncodeFailed("payload is too large for fragment header".into())
        })?;
        let fragment_count = fragment_count(payload.len(), max_chunk)?;
        let message_id = make_message_id();
        let mut fragments = Vec::with_capacity(fragment_count as usize);

        for (index, chunk) in payload.chunks(max_chunk).enumerate() {
            let fragment = encode_fragment(
                message_id,
                original_len,
                index as u32,
                fragment_count,
                chunk,
            );
            fragments.push(fragment);
        }

        let key = FragmentMessageKey {
            tenant: ctx.tenant.to_string(),
            channel: ctx.channel.to_string(),
            message_id,
        };
        let mut state = self
            .state
            .lock()
            .map_err(|_| FilterError::Other("failed to lock fragment filter state".into()))?;
        state.outbound.insert(
            key,
            OutboundMessage {
                created_at: Instant::now(),
                fragments: fragments.clone(),
            },
        );

        Ok(fragments)
    }

    fn apply_inbound(
        &self,
        ctx: RouteContext<'_>,
        payload: Vec<u8>,
    ) -> Result<FilterInboundResult, FilterError> {
        self.apply_inbound_inner(ctx, payload, None)
    }

    fn apply_inbound_with_meta(
        &self,
        ctx: RouteContext<'_>,
        payload: Vec<u8>,
        meta: &mut MetaWriter,
    ) -> Result<FilterInboundResult, FilterError> {
        self.apply_inbound_inner(ctx, payload, Some(meta))
    }

    fn on_timer(&self, _ctx: FilterTimerContext) -> Result<Vec<FilterSendMessage>, FilterError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| FilterError::Other("failed to lock fragment filter state".into()))?;
        let now = Instant::now();

        state
            .outbound
            .retain(|_, entry| now.duration_since(entry.created_at) < self.defrag_timeout);

        let mut timeout_error = None;
        state.inbound.retain(|key, entry| {
            if now.duration_since(entry.created_at) < self.defrag_timeout {
                return true;
            }

            if timeout_error.is_none() {
                let missing = entry.missing_indexes();
                timeout_error = Some(format!(
                    "fragment defrag timeout: tenant={} channel={} message_id={} missing={:?}",
                    key.tenant,
                    key.channel,
                    hex_id(&key.message_id),
                    missing
                ));
            }
            false
        });
        state.completed.retain(|_, expires_at| *expires_at > now);

        if let Some(error) = timeout_error {
            return Err(FilterError::FragmentDecodeFailed(error));
        }

        let mut messages = Vec::new();
        for (key, entry) in state.inbound.iter_mut() {
            if !entry.should_request(now, self.request_interval) {
                continue;
            }

            let missing = entry.missing_indexes();
            if missing.is_empty() {
                continue;
            }

            entry.last_request_at = Some(now);
            messages.push(FilterSendMessage::new(
                key.tenant.clone(),
                control_channel(&key.channel, &key.message_id),
                encode_request(key.message_id, &missing)?,
            ));
        }

        Ok(messages)
    }
}

impl InboundMessage {
    /// Creates assembly state for one inbound message.
    fn new(
        original_len: usize,
        fragment_count: u32,
        created_at: Instant,
    ) -> Result<Self, FilterError> {
        if original_len == 0 || fragment_count == 0 {
            return Err(FilterError::FragmentDecodeFailed(
                "fragment message has empty length or count".into(),
            ));
        }
        let count = usize::try_from(fragment_count).map_err(|_| {
            FilterError::FragmentDecodeFailed("fragment count does not fit usize".into())
        })?;
        if count > original_len {
            return Err(FilterError::FragmentDecodeFailed(
                "fragment count is larger than original length".into(),
            ));
        }

        Ok(Self {
            created_at,
            original_len,
            fragment_count,
            fragments: vec![None; count],
            last_request_at: None,
        })
    }

    /// Adds one fragment and returns the payload when the message is complete.
    fn push(&mut self, fragment: DecodedFragment<'_>) -> Result<Option<Vec<u8>>, FilterError> {
        if self.original_len != fragment.original_len
            || self.fragment_count != fragment.fragment_count
        {
            return Err(FilterError::FragmentDecodeFailed(
                "fragment header does not match previous fragments".into(),
            ));
        }

        let index = usize::try_from(fragment.fragment_index).map_err(|_| {
            FilterError::FragmentDecodeFailed("fragment index does not fit usize".into())
        })?;
        let Some(slot) = self.fragments.get_mut(index) else {
            return Err(FilterError::FragmentDecodeFailed(
                "fragment index is out of range".into(),
            ));
        };
        if slot.is_none() {
            *slot = Some(fragment.chunk.to_vec());
        }

        if !self.is_complete() {
            return Ok(None);
        }

        self.assemble().map(Some)
    }

    /// Checks if all fragments are present.
    fn is_complete(&self) -> bool {
        self.fragments.iter().all(Option::is_some)
    }

    /// Builds the original payload from all fragments.
    fn assemble(&self) -> Result<Vec<u8>, FilterError> {
        let mut payload = Vec::with_capacity(self.original_len);
        for fragment in &self.fragments {
            let Some(fragment) = fragment else {
                return Err(FilterError::FragmentDecodeFailed(
                    "fragment message is not complete".into(),
                ));
            };
            payload.extend_from_slice(fragment);
        }

        if payload.len() != self.original_len {
            return Err(FilterError::FragmentDecodeFailed(
                "assembled payload length does not match original length".into(),
            ));
        }

        Ok(payload)
    }

    /// Checks if it is time to send a restore request.
    fn should_request(&self, now: Instant, request_interval: Duration) -> bool {
        match self.last_request_at {
            Some(last_request_at) => now.duration_since(last_request_at) >= request_interval,
            None => now.duration_since(self.created_at) >= request_interval,
        }
    }

    /// Returns indexes of fragments that are still missing.
    fn missing_indexes(&self) -> Vec<u32> {
        self.fragments
            .iter()
            .enumerate()
            .filter_map(|(index, fragment)| {
                if fragment.is_none() {
                    Some(index as u32)
                } else {
                    None
                }
            })
            .collect()
    }
}

impl FragmentFilter {
    /// Handles a restore request from the control channel.
    fn handle_request(
        &self,
        tenant: &str,
        base_channel: &str,
        channel_message_id: [u8; MESSAGE_ID_BYTES],
        payload: Vec<u8>,
    ) -> Result<FilterInboundResult, FilterError> {
        let request = decode_request(&payload)?;
        if request.message_id != channel_message_id {
            return Err(FilterError::FragmentDecodeFailed(
                "fragment request id does not match control channel".into(),
            ));
        }

        let key = FragmentMessageKey {
            tenant: tenant.to_string(),
            channel: base_channel.to_string(),
            message_id: request.message_id,
        };
        let state = self
            .state
            .lock()
            .map_err(|_| FilterError::Other("failed to lock fragment filter state".into()))?;
        let Some(outbound) = state.outbound.get(&key) else {
            return Ok(FilterInboundResult::new(Vec::new(), Vec::new()));
        };

        let mut messages = Vec::new();
        for index in request.indexes {
            let Some(fragment) = outbound.fragments.get(index as usize) else {
                continue;
            };
            messages.push(FilterSendMessage::new(
                tenant,
                base_channel,
                fragment.clone(),
            ));
        }

        Ok(FilterInboundResult::new(Vec::new(), messages))
    }
}

/// Returns the fragment header size.
fn fragment_header_len() -> usize {
    FRAGMENT_MAGIC.len() + MESSAGE_ID_BYTES + U32_BYTES + U32_BYTES + U32_BYTES
}

/// Returns the maximum data size inside one fragment.
fn max_chunk_len(max_fragment_bytes: usize) -> Result<usize, FilterError> {
    let header_len = fragment_header_len();
    if max_fragment_bytes <= header_len {
        return Err(FilterError::InvalidConfig(format!(
            "fragment max_fragment_bytes must be greater than {header_len}"
        )));
    }
    Ok(max_fragment_bytes - header_len)
}

/// Counts how many fragments a payload needs.
fn fragment_count(payload_len: usize, max_chunk: usize) -> Result<u32, FilterError> {
    let count = payload_len.div_ceil(max_chunk);
    u32::try_from(count).map_err(|_| FilterError::FragmentEncodeFailed("too many fragments".into()))
}

/// Generates a random 8-byte `message_id`.
fn make_message_id() -> [u8; MESSAGE_ID_BYTES] {
    let mut id = [0u8; MESSAGE_ID_BYTES];
    let mut rng = OsRng;
    rng.fill_bytes(&mut id);
    id
}

/// Encodes one fragment.
fn encode_fragment(
    message_id: [u8; MESSAGE_ID_BYTES],
    original_len: u32,
    fragment_index: u32,
    fragment_count: u32,
    chunk: &[u8],
) -> Vec<u8> {
    let mut out = Vec::with_capacity(fragment_header_len() + chunk.len());
    out.extend_from_slice(FRAGMENT_MAGIC);
    out.extend_from_slice(&message_id);
    out.extend_from_slice(&original_len.to_be_bytes());
    out.extend_from_slice(&fragment_index.to_be_bytes());
    out.extend_from_slice(&fragment_count.to_be_bytes());
    out.extend_from_slice(chunk);
    out
}

/// Decodes a fragment or returns `None` when this is a normal payload.
fn decode_fragment(payload: &[u8]) -> Result<Option<DecodedFragment<'_>>, FilterError> {
    if payload.len() < FRAGMENT_MAGIC.len() || &payload[..FRAGMENT_MAGIC.len()] != FRAGMENT_MAGIC {
        return Ok(None);
    }
    if payload.len() < fragment_header_len() {
        return Err(FilterError::FragmentDecodeFailed(
            "fragment payload is too short".into(),
        ));
    }

    let mut index = FRAGMENT_MAGIC.len();
    let message_id = read_message_id(payload, &mut index)?;
    let original_len = read_u32(payload, &mut index)? as usize;
    let fragment_index = read_u32(payload, &mut index)?;
    let fragment_count = read_u32(payload, &mut index)?;

    if fragment_count == 0 || fragment_index >= fragment_count {
        return Err(FilterError::FragmentDecodeFailed(
            "fragment index or count is invalid".into(),
        ));
    }
    if payload.len() == index {
        return Err(FilterError::FragmentDecodeFailed(
            "fragment chunk is empty".into(),
        ));
    }

    Ok(Some(DecodedFragment {
        message_id,
        original_len,
        fragment_index,
        fragment_count,
        chunk: &payload[index..],
    }))
}

/// Encodes a restore request.
fn encode_request(
    message_id: [u8; MESSAGE_ID_BYTES],
    indexes: &[u32],
) -> Result<Vec<u8>, FilterError> {
    let count = u32::try_from(indexes.len())
        .map_err(|_| FilterError::FragmentEncodeFailed("too many missing fragments".into()))?;
    let mut out = Vec::with_capacity(
        REQUEST_MAGIC.len() + MESSAGE_ID_BYTES + U32_BYTES + indexes.len() * U32_BYTES,
    );
    out.extend_from_slice(REQUEST_MAGIC);
    out.extend_from_slice(&message_id);
    out.extend_from_slice(&count.to_be_bytes());
    for index in indexes {
        out.extend_from_slice(&index.to_be_bytes());
    }
    Ok(out)
}

/// Decodes a restore request.
fn decode_request(payload: &[u8]) -> Result<FragmentRequest, FilterError> {
    if payload.len() < REQUEST_MAGIC.len() || &payload[..REQUEST_MAGIC.len()] != REQUEST_MAGIC {
        return Err(FilterError::FragmentDecodeFailed(
            "fragment request has bad magic".into(),
        ));
    }

    let mut index = REQUEST_MAGIC.len();
    let message_id = read_message_id(payload, &mut index)?;
    let count = read_u32(payload, &mut index)? as usize;
    let expected_len = REQUEST_MAGIC.len() + MESSAGE_ID_BYTES + U32_BYTES + count * U32_BYTES;
    if payload.len() != expected_len {
        return Err(FilterError::FragmentDecodeFailed(
            "fragment request has bad length".into(),
        ));
    }

    let mut indexes = Vec::with_capacity(count);
    for _ in 0..count {
        indexes.push(read_u32(payload, &mut index)?);
    }
    Ok(FragmentRequest {
        message_id,
        indexes,
    })
}

/// Checks if the channel is a fragment control channel.
fn is_control_channel(channel: &str) -> bool {
    channel.contains(CONTROL_MARK)
}

/// Returns the control channel for restore requests.
fn control_channel(base_channel: &str, message_id: &[u8; MESSAGE_ID_BYTES]) -> String {
    format!("{base_channel}{CONTROL_MARK}{}", hex_id(message_id))
}

/// Parses a control channel and returns the base channel with `message_id`.
fn parse_control_channel(
    channel: &str,
) -> Result<Option<(&str, [u8; MESSAGE_ID_BYTES])>, FilterError> {
    let Some((base_channel, id_hex)) = channel.split_once(CONTROL_MARK) else {
        return Ok(None);
    };
    if base_channel.is_empty() {
        return Err(FilterError::FragmentDecodeFailed(
            "fragment control channel has empty base".into(),
        ));
    }
    Ok(Some((base_channel, parse_hex_id(id_hex)?)))
}

/// Encodes `message_id` as hex.
fn hex_id(message_id: &[u8; MESSAGE_ID_BYTES]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(MESSAGE_ID_BYTES * 2);
    for byte in message_id {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

/// Decodes hex back into `message_id`.
fn parse_hex_id(hex: &str) -> Result<[u8; MESSAGE_ID_BYTES], FilterError> {
    if hex.len() != MESSAGE_ID_BYTES * 2 {
        return Err(FilterError::FragmentDecodeFailed(
            "fragment message id has bad hex length".into(),
        ));
    }

    let bytes = hex.as_bytes();
    let mut id = [0u8; MESSAGE_ID_BYTES];
    for i in 0..MESSAGE_ID_BYTES {
        let hi = hex_value(bytes[i * 2])?;
        let lo = hex_value(bytes[i * 2 + 1])?;
        id[i] = (hi << 4) | lo;
    }
    Ok(id)
}

/// Returns the value of one hex character.
fn hex_value(byte: u8) -> Result<u8, FilterError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(FilterError::FragmentDecodeFailed(
            "fragment message id has bad hex digit".into(),
        )),
    }
}

/// Reads `message_id` from the payload.
fn read_message_id(
    payload: &[u8],
    index: &mut usize,
) -> Result<[u8; MESSAGE_ID_BYTES], FilterError> {
    let end = index
        .checked_add(MESSAGE_ID_BYTES)
        .ok_or_else(|| FilterError::FragmentDecodeFailed("fragment index is too large".into()))?;
    if end > payload.len() {
        return Err(FilterError::FragmentDecodeFailed(
            "fragment message id is truncated".into(),
        ));
    }

    let bytes: [u8; MESSAGE_ID_BYTES] = payload[*index..end]
        .try_into()
        .map_err(|_| FilterError::FragmentDecodeFailed("fragment message id is invalid".into()))?;
    *index = end;
    Ok(bytes)
}

/// Reads a `u32` from the payload.
fn read_u32(payload: &[u8], index: &mut usize) -> Result<u32, FilterError> {
    let end = index
        .checked_add(U32_BYTES)
        .ok_or_else(|| FilterError::FragmentDecodeFailed("fragment index is too large".into()))?;
    if end > payload.len() {
        return Err(FilterError::FragmentDecodeFailed(
            "fragment number is truncated".into(),
        ));
    }

    let bytes: [u8; U32_BYTES] = payload[*index..end]
        .try_into()
        .map_err(|_| FilterError::FragmentDecodeFailed("fragment number is invalid".into()))?;
    *index = end;
    Ok(u32::from_be_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filters::FilterTimerMode;

    /// Creates route context for a test.
    fn ctx(channel: &'static str) -> RouteContext<'static> {
        RouteContext {
            tenant: "tenant_1",
            channel,
        }
    }

    /// Creates timer context for a test.
    fn timer() -> FilterTimerContext {
        FilterTimerContext {
            mode: FilterTimerMode::SdkDefault100Hz,
            interval: Duration::from_millis(10),
        }
    }

    /// Creates a payload that is larger than the test fragment limit.
    fn large_payload() -> Vec<u8> {
        b"abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ".to_vec()
    }

    #[test]
    fn fragment_filter_passes_small_payload() {
        let filter =
            FragmentFilter::new(64, 34, Duration::from_millis(250), Duration::from_secs(1));

        let sent = filter
            .apply_outbound(ctx("public.fotos"), b"small".to_vec())
            .unwrap();
        let inbound = filter
            .apply_inbound(ctx("public.fotos"), sent[0].clone())
            .unwrap();

        assert_eq!(sent, vec![b"small".to_vec()]);
        assert_eq!(inbound.payloads, vec![b"small".to_vec()]);
    }

    #[test]
    fn fragment_filter_splits_and_assembles_payload() {
        let filter =
            FragmentFilter::new(32, 34, Duration::from_millis(250), Duration::from_secs(1));
        let payload = large_payload();

        let sent = filter
            .apply_outbound(ctx("public.fotos"), payload.clone())
            .unwrap();

        assert!(sent.len() > 1);
        assert!(sent.iter().all(|fragment| fragment.len() <= 34));

        for fragment in sent.iter().take(sent.len() - 1) {
            let inbound = filter
                .apply_inbound(ctx("public.fotos"), fragment.clone())
                .unwrap();
            assert!(inbound.payloads.is_empty());
        }

        let inbound = filter
            .apply_inbound(ctx("public.fotos"), sent[sent.len() - 1].clone())
            .unwrap();
        assert_eq!(inbound.payloads, vec![payload]);
    }

    #[test]
    fn fragment_filter_requests_missing_fragments() {
        let filter = FragmentFilter::new(32, 34, Duration::from_millis(1), Duration::from_secs(1));
        let payload = large_payload();
        let sent = filter.apply_outbound(ctx("public.fotos"), payload).unwrap();

        filter
            .apply_inbound(ctx("public.fotos"), sent[0].clone())
            .unwrap();
        std::thread::sleep(Duration::from_millis(2));

        let messages = filter.on_timer(timer()).unwrap();

        assert_eq!(messages.len(), 1);
        assert!(messages[0].channel.starts_with("public.fotos._contrl."));
        assert!(decode_request(&messages[0].payload)
            .unwrap()
            .indexes
            .contains(&1));
    }

    #[test]
    fn fragment_filter_answers_restore_request() {
        let filter =
            FragmentFilter::new(32, 34, Duration::from_millis(250), Duration::from_secs(1));
        let payload = large_payload();
        let sent = filter.apply_outbound(ctx("public.fotos"), payload).unwrap();
        let fragment = decode_fragment(&sent[0]).unwrap().unwrap();
        let request = encode_request(fragment.message_id, &[1]).unwrap();
        let channel = control_channel("public.fotos", &fragment.message_id);

        let result = filter
            .apply_inbound(
                RouteContext {
                    tenant: "tenant_1",
                    channel: &channel,
                },
                request,
            )
            .unwrap();

        assert!(result.payloads.is_empty());
        assert_eq!(result.messages.len(), 1);
        assert_eq!(result.messages[0].channel, "public.fotos");
        assert_eq!(result.messages[0].payload, sent[1]);
    }

    #[test]
    fn fragment_filter_cleans_old_fragments() {
        let filter =
            FragmentFilter::new(32, 34, Duration::from_millis(250), Duration::from_millis(1));
        let payload = large_payload();
        let sent = filter.apply_outbound(ctx("public.fotos"), payload).unwrap();
        let fragment = decode_fragment(&sent[0]).unwrap().unwrap();
        let request = encode_request(fragment.message_id, &[1]).unwrap();
        let channel = control_channel("public.fotos", &fragment.message_id);

        std::thread::sleep(Duration::from_millis(2));
        filter.on_timer(timer()).unwrap();
        let result = filter
            .apply_inbound(
                RouteContext {
                    tenant: "tenant_1",
                    channel: &channel,
                },
                request,
            )
            .unwrap();

        assert!(result.messages.is_empty());
    }

    #[test]
    fn fragment_filter_ignores_duplicate_after_complete() {
        let filter =
            FragmentFilter::new(32, 34, Duration::from_millis(250), Duration::from_secs(1));
        let payload = large_payload();
        let sent = filter
            .apply_outbound(ctx("public.fotos"), payload.clone())
            .unwrap();

        for fragment in sent.clone() {
            filter.apply_inbound(ctx("public.fotos"), fragment).unwrap();
        }
        let duplicate = filter
            .apply_inbound(ctx("public.fotos"), sent[0].clone())
            .unwrap();

        assert!(duplicate.payloads.is_empty());
    }

    #[test]
    fn fragment_filter_reports_defrag_timeout() {
        let filter =
            FragmentFilter::new(32, 34, Duration::from_millis(250), Duration::from_millis(1));
        let payload = large_payload();
        let sent = filter.apply_outbound(ctx("public.fotos"), payload).unwrap();

        filter
            .apply_inbound(ctx("public.fotos"), sent[0].clone())
            .unwrap();
        std::thread::sleep(Duration::from_millis(2));

        let err = filter.on_timer(timer()).unwrap_err();

        assert!(matches!(err, FilterError::FragmentDecodeFailed(_)));
    }

    #[test]
    fn fragment_filter_has_options() {
        let filter = FragmentFilter::new(
            1024,
            2048,
            Duration::from_millis(77),
            Duration::from_secs(3),
        );

        assert_eq!(filter.id(), "fragment.v1");
        assert_eq!(filter.fragment_threshold_bytes(), 1024);
        assert_eq!(filter.max_fragment_bytes(), 2048);
        assert_eq!(filter.request_interval(), Duration::from_millis(77));
        assert_eq!(filter.defrag_timeout(), Duration::from_secs(3));
    }
}
