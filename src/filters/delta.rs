// Copyright (c) 2026 FastPubSub Network
// SPDX-License-Identifier: MIT OR Apache-2.0
// Project: https://fastpubsub.com

//! Delta filters for binary state payloads.
//!
//! A delta filter reduces traffic when the same `tenant/channel` sends payloads
//! with the same layout many times, and only a small part of the payload changes
//! between sends. This is common for game state, physics state, fixed-size
//! arrays, binary snapshots, and telemetry frames.
//!
//! The filter keeps state per `tenant/channel`. The first outbound payload is
//! always sent as a full snapshot. Later payloads can be sent as a delta against
//! the stored base. The receiver restores the full payload before it is
//! delivered to local subscribers.
//!
//! There are three public filters:
//!
//! - [`Delta16Filter`] compares payloads as 2-byte words.
//! - [`Delta32Filter`] compares payloads as 4-byte words.
//! - [`Delta64Filter`] compares payloads as 8-byte words.
//!
//! Choose the word size that matches the data layout. For example, use
//! [`Delta32Filter`] for arrays of `u32`, `f32`, or other 4-byte fields. A word
//! is the smallest changed unit. If one byte inside a 4-byte word changes, the
//! whole 4-byte word is written into the delta.
//!
//! A delta is sent only when it is useful and safe:
//!
//! - the receiver must have seen a snapshot first;
//! - the new payload length must match the base length;
//! - the payload length must be aligned to the filter word size;
//! - the snapshot interval must not be expired;
//! - the encoded delta must be smaller than a full snapshot.
//!
//! If any of these checks says that a delta is not a good choice, the filter
//! sends a new full snapshot instead.
//!
//! Delta bodies are encoded in the shortest of three formats:
//!
//! - `IndexList`: changed word indexes with their new values;
//! - `Ranges`: adjacent changed word ranges with their new values;
//! - `Bitmap`: one bit per word plus values for changed words.
//!
//! # Base Modes
//!
//! [`DeltaBaseMode::SnapshotBase`] keeps the last full snapshot as the base.
//! Each delta is calculated from that snapshot until the next snapshot is sent.
//! This mode is simple and more tolerant when a delta is lost.
//!
//! [`DeltaBaseMode::RollingBase`] updates the base after every decoded delta.
//! This can make deltas smaller for step-by-step changes, but each delta depends
//! on the previous restored state.
//!
//! # Example
//!
//! ```ignore
//! use std::time::Duration;
//!
//! use fastpubsub_sdk::client::create_web_socket;
//! use fastpubsub_sdk::filters::{Delta32Filter, DeltaBaseMode};
//!
//! let client = create_web_socket("overlay_name", "AT_token")
//!     .endpoint("wss://edge.example/ws")
//!     .add_filter(
//!         "tenant_1",
//!         "game.state.",
//!         Delta32Filter::with_base_mode(
//!             Duration::from_secs(1),
//!             DeltaBaseMode::SnapshotBase,
//!         ),
//!     )
//!     .build()
//!     .await?;
//! ```
//!
//! Put the same delta filter on both sender and receiver for the same route
//! prefix. Outbound data is encoded into snapshots or deltas. Inbound data is
//! decoded back into the original full payload.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::metadata::{MetaWriter, META_RECORD_DELTA};

use super::{FilterError, FilterInboundResult, FilterTrait, RouteContext};

/// Default interval between full snapshots.
pub const DELTA_DEFAULT_SNAPSHOT_INTERVAL_MS: u64 = 1000;

const DELTA_MAGIC: &[u8] = b"FPSDELT1";
const HEADER_BYTES: usize = 24;
const U32_BYTES: usize = 4;
const FRAME_SNAPSHOT: u8 = 1;
const FRAME_DELTA: u8 = 2;
const ENCODING_NONE: u8 = 0;
const ENCODING_INDEX_LIST: u8 = 1;
const ENCODING_RANGES: u8 = 2;
const ENCODING_BITMAP: u8 = 3;

/// Base selection mode for the next delta.
///
/// This controls which stored payload is used as the base when the next delta
/// is encoded or decoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DeltaBaseMode {
    /// Delta is calculated from the last full snapshot.
    ///
    /// This is the default mode. It sends deltas against the last snapshot until
    /// a new snapshot is needed. It is easier to reason about because deltas do
    /// not update the sender base.
    #[default]
    SnapshotBase,
    /// Delta is calculated from the last restored state.
    ///
    /// This mode updates the base after each delta. It can produce smaller
    /// deltas for smooth step-by-step changes, but each delta depends on the
    /// previous restored state.
    RollingBase,
}

impl DeltaBaseMode {
    /// Returns the binary code for payload and FFI.
    pub fn code(self) -> u8 {
        match self {
            Self::SnapshotBase => 1,
            Self::RollingBase => 2,
        }
    }

    /// Reads the mode from a binary code.
    pub fn from_code(value: u8) -> Result<Self, FilterError> {
        match value {
            1 => Ok(Self::SnapshotBase),
            2 => Ok(Self::RollingBase),
            _ => Err(FilterError::DeltaDecodeFailed(
                "delta base mode is unknown".into(),
            )),
        }
    }
}

/// Delta filter for payloads made of 16-bit words.
///
/// Use this when the payload layout is mostly 2-byte fields. Payloads that are
/// not aligned to 2 bytes are still sent as snapshots, but they are not encoded
/// as deltas.
pub struct Delta16Filter {
    core: DeltaFilterCore<2>,
}

/// Delta filter for payloads made of 32-bit words.
///
/// This is a good default for many game and state payloads because `u32`, `i32`,
/// and `f32` fields are 4 bytes. Payloads that are not aligned to 4 bytes are
/// still sent as snapshots, but they are not encoded as deltas.
pub struct Delta32Filter {
    core: DeltaFilterCore<4>,
}

/// Delta filter for payloads made of 64-bit words.
///
/// Use this when the payload layout is mostly 8-byte fields, such as `u64`,
/// `i64`, or `f64`. Payloads that are not aligned to 8 bytes are still sent as
/// snapshots, but they are not encoded as deltas.
pub struct Delta64Filter {
    core: DeltaFilterCore<8>,
}

struct DeltaFilterCore<const WORD_BYTES: usize> {
    snapshot_interval: Duration,
    base_mode: DeltaBaseMode,
    state: Mutex<DeltaState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DeltaKey {
    tenant: String,
    channel: String,
}

struct DeltaState {
    outbound: HashMap<DeltaKey, DeltaEntry>,
    inbound: HashMap<DeltaKey, DeltaEntry>,
}

struct DeltaEntry {
    base: Vec<u8>,
    last_snapshot_at: Instant,
}

struct EncodedDelta {
    payload: Vec<u8>,
}

struct DecodedFrame {
    base_mode: DeltaBaseMode,
    frame_kind: DeltaFrameKind,
    encoding: DeltaEncoding,
    original_len: usize,
    word_count: usize,
    changed_count: u32,
    body_start: usize,
}

struct DecodedPayload {
    payload: Vec<u8>,
    base_mode: DeltaBaseMode,
    frame_kind: DeltaFrameKind,
    encoding: DeltaEncoding,
    changed_count: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeltaFrameKind {
    Snapshot,
    Delta,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeltaEncoding {
    None,
    IndexList,
    Ranges,
    Bitmap,
}

impl DeltaFrameKind {
    /// Returns the binary frame code.
    fn code(self) -> u8 {
        match self {
            Self::Snapshot => FRAME_SNAPSHOT,
            Self::Delta => FRAME_DELTA,
        }
    }

    /// Reads the frame kind from a binary code.
    fn from_code(value: u8) -> Result<Self, FilterError> {
        match value {
            FRAME_SNAPSHOT => Ok(Self::Snapshot),
            FRAME_DELTA => Ok(Self::Delta),
            _ => Err(FilterError::DeltaDecodeFailed(
                "delta frame kind is unknown".into(),
            )),
        }
    }
}

impl DeltaEncoding {
    /// Returns the binary algorithm code.
    fn code(self) -> u8 {
        match self {
            Self::None => ENCODING_NONE,
            Self::IndexList => ENCODING_INDEX_LIST,
            Self::Ranges => ENCODING_RANGES,
            Self::Bitmap => ENCODING_BITMAP,
        }
    }

    /// Reads the algorithm from a binary code.
    fn from_code(value: u8) -> Result<Self, FilterError> {
        match value {
            ENCODING_NONE => Ok(Self::None),
            ENCODING_INDEX_LIST => Ok(Self::IndexList),
            ENCODING_RANGES => Ok(Self::Ranges),
            ENCODING_BITMAP => Ok(Self::Bitmap),
            _ => Err(FilterError::DeltaDecodeFailed(
                "delta encoding is unknown".into(),
            )),
        }
    }
}

impl DeltaEntry {
    /// Creates new base state.
    fn new(base: Vec<u8>, last_snapshot_at: Instant) -> Self {
        Self {
            base,
            last_snapshot_at,
        }
    }

    /// Updates the base after a snapshot.
    fn set_snapshot(&mut self, payload: Vec<u8>, now: Instant) {
        self.base = payload;
        self.last_snapshot_at = now;
    }

    /// Updates the base after a rolling delta.
    fn set_base(&mut self, payload: Vec<u8>) {
        self.base = payload;
    }
}

impl<const WORD_BYTES: usize> DeltaFilterCore<WORD_BYTES> {
    /// Creates a filter with the default base mode.
    fn new(snapshot_interval: Duration) -> Self {
        Self::with_base_mode(snapshot_interval, DeltaBaseMode::default())
    }

    /// Creates a filter with an explicit base mode.
    fn with_base_mode(snapshot_interval: Duration, base_mode: DeltaBaseMode) -> Self {
        Self {
            snapshot_interval,
            base_mode,
            state: Mutex::new(DeltaState {
                outbound: HashMap::new(),
                inbound: HashMap::new(),
            }),
        }
    }

    /// Returns the interval between full snapshots.
    fn snapshot_interval(&self) -> Duration {
        self.snapshot_interval
    }

    /// Returns the base mode.
    fn base_mode(&self) -> DeltaBaseMode {
        self.base_mode
    }

    /// Returns the word size in bytes.
    fn word_bytes(&self) -> usize {
        WORD_BYTES
    }

    /// Encodes an outbound payload.
    fn apply_outbound(
        &self,
        ctx: RouteContext<'_>,
        payload: Vec<u8>,
    ) -> Result<Vec<Vec<u8>>, FilterError> {
        let key = DeltaKey {
            tenant: ctx.tenant.to_string(),
            channel: ctx.channel.to_string(),
        };
        let now = Instant::now();
        let mut state = self
            .state
            .lock()
            .map_err(|_| FilterError::Other("failed to lock delta filter state".into()))?;

        let Some(entry) = state.outbound.get_mut(&key) else {
            let snapshot = encode_snapshot::<WORD_BYTES>(self.base_mode, &payload)?;
            state.outbound.insert(key, DeltaEntry::new(payload, now));
            return Ok(vec![snapshot]);
        };

        if must_send_snapshot::<WORD_BYTES>(entry, &payload, now, self.snapshot_interval) {
            let snapshot = encode_snapshot::<WORD_BYTES>(self.base_mode, &payload)?;
            entry.set_snapshot(payload, now);
            return Ok(vec![snapshot]);
        }

        let delta = encode_best_delta::<WORD_BYTES>(self.base_mode, &entry.base, &payload)?;
        let snapshot_len = encoded_snapshot_len(&payload);
        if delta.payload.len() >= snapshot_len {
            let snapshot = encode_snapshot::<WORD_BYTES>(self.base_mode, &payload)?;
            entry.set_snapshot(payload, now);
            return Ok(vec![snapshot]);
        }

        if self.base_mode == DeltaBaseMode::RollingBase {
            entry.set_base(payload);
        }
        Ok(vec![delta.payload])
    }

    /// Decodes an inbound payload without metadata.
    fn apply_inbound(
        &self,
        ctx: RouteContext<'_>,
        payload: Vec<u8>,
    ) -> Result<FilterInboundResult, FilterError> {
        self.apply_inbound_inner(ctx, payload, None)
    }

    /// Decodes an inbound payload and writes metadata.
    fn apply_inbound_with_meta(
        &self,
        ctx: RouteContext<'_>,
        payload: Vec<u8>,
        meta: &mut MetaWriter,
    ) -> Result<FilterInboundResult, FilterError> {
        self.apply_inbound_inner(ctx, payload, Some(meta))
    }

    /// Common handling for an inbound payload.
    fn apply_inbound_inner(
        &self,
        ctx: RouteContext<'_>,
        payload: Vec<u8>,
        mut meta: Option<&mut MetaWriter>,
    ) -> Result<FilterInboundResult, FilterError> {
        let Some(frame) = decode_frame::<WORD_BYTES>(&payload)? else {
            return Ok(FilterInboundResult::single(payload));
        };

        let key = DeltaKey {
            tenant: ctx.tenant.to_string(),
            channel: ctx.channel.to_string(),
        };
        let decoded = match frame.frame_kind {
            DeltaFrameKind::Snapshot => self.decode_snapshot(key, frame, &payload)?,
            DeltaFrameKind::Delta => self.decode_delta(key, frame, &payload)?,
        };

        if let Some(meta) = meta.as_deref_mut() {
            write_delta_meta::<WORD_BYTES>(meta, &decoded)?;
        }

        Ok(FilterInboundResult::single(decoded.payload))
    }

    /// Decodes a full snapshot.
    fn decode_snapshot(
        &self,
        key: DeltaKey,
        frame: DecodedFrame,
        payload: &[u8],
    ) -> Result<DecodedPayload, FilterError> {
        let body = &payload[frame.body_start..];
        if body.len() != frame.original_len {
            return Err(FilterError::DeltaDecodeFailed(
                "delta snapshot length does not match header".into(),
            ));
        }

        let now = Instant::now();
        let plain = body.to_vec();
        let mut state = self
            .state
            .lock()
            .map_err(|_| FilterError::Other("failed to lock delta filter state".into()))?;
        match state.inbound.get_mut(&key) {
            Some(entry) => entry.set_snapshot(plain.clone(), now),
            None => {
                state
                    .inbound
                    .insert(key, DeltaEntry::new(plain.clone(), now));
            }
        }

        Ok(DecodedPayload {
            payload: plain,
            base_mode: frame.base_mode,
            frame_kind: frame.frame_kind,
            encoding: frame.encoding,
            changed_count: frame.changed_count,
        })
    }

    /// Decodes a delta and builds a full payload.
    fn decode_delta(
        &self,
        key: DeltaKey,
        frame: DecodedFrame,
        payload: &[u8],
    ) -> Result<DecodedPayload, FilterError> {
        let body = &payload[frame.body_start..];
        let mut state = self
            .state
            .lock()
            .map_err(|_| FilterError::Other("failed to lock delta filter state".into()))?;
        let Some(entry) = state.inbound.get_mut(&key) else {
            return Err(FilterError::DeltaDecodeFailed(
                "delta arrived before snapshot".into(),
            ));
        };
        if entry.base.len() != frame.original_len {
            return Err(FilterError::DeltaDecodeFailed(
                "delta base length does not match frame".into(),
            ));
        }

        let mut plain = entry.base.clone();
        apply_delta_body::<WORD_BYTES>(&mut plain, &frame, body)?;
        if frame.base_mode == DeltaBaseMode::RollingBase {
            entry.set_base(plain.clone());
        }

        Ok(DecodedPayload {
            payload: plain,
            base_mode: frame.base_mode,
            frame_kind: frame.frame_kind,
            encoding: frame.encoding,
            changed_count: frame.changed_count,
        })
    }
}

impl Delta16Filter {
    /// Creates a filter with SnapshotBase mode.
    pub fn new(snapshot_interval: Duration) -> Self {
        Self {
            core: DeltaFilterCore::new(snapshot_interval),
        }
    }

    /// Creates a filter with an interval in milliseconds.
    pub fn from_millis(snapshot_interval_ms: u64) -> Self {
        Self::new(Duration::from_millis(snapshot_interval_ms))
    }

    /// Creates a filter with an explicit base mode.
    pub fn with_base_mode(snapshot_interval: Duration, base_mode: DeltaBaseMode) -> Self {
        Self {
            core: DeltaFilterCore::with_base_mode(snapshot_interval, base_mode),
        }
    }

    /// Returns the interval between snapshots.
    pub fn snapshot_interval(&self) -> Duration {
        self.core.snapshot_interval()
    }

    /// Returns the base mode.
    pub fn base_mode(&self) -> DeltaBaseMode {
        self.core.base_mode()
    }

    /// Returns the word size in bytes.
    pub fn word_bytes(&self) -> usize {
        self.core.word_bytes()
    }
}

impl Delta32Filter {
    /// Creates a filter with SnapshotBase mode.
    pub fn new(snapshot_interval: Duration) -> Self {
        Self {
            core: DeltaFilterCore::new(snapshot_interval),
        }
    }

    /// Creates a filter with an interval in milliseconds.
    pub fn from_millis(snapshot_interval_ms: u64) -> Self {
        Self::new(Duration::from_millis(snapshot_interval_ms))
    }

    /// Creates a filter with an explicit base mode.
    pub fn with_base_mode(snapshot_interval: Duration, base_mode: DeltaBaseMode) -> Self {
        Self {
            core: DeltaFilterCore::with_base_mode(snapshot_interval, base_mode),
        }
    }

    /// Returns the interval between snapshots.
    pub fn snapshot_interval(&self) -> Duration {
        self.core.snapshot_interval()
    }

    /// Returns the base mode.
    pub fn base_mode(&self) -> DeltaBaseMode {
        self.core.base_mode()
    }

    /// Returns the word size in bytes.
    pub fn word_bytes(&self) -> usize {
        self.core.word_bytes()
    }
}

impl Delta64Filter {
    /// Creates a filter with SnapshotBase mode.
    pub fn new(snapshot_interval: Duration) -> Self {
        Self {
            core: DeltaFilterCore::new(snapshot_interval),
        }
    }

    /// Creates a filter with an interval in milliseconds.
    pub fn from_millis(snapshot_interval_ms: u64) -> Self {
        Self::new(Duration::from_millis(snapshot_interval_ms))
    }

    /// Creates a filter with an explicit base mode.
    pub fn with_base_mode(snapshot_interval: Duration, base_mode: DeltaBaseMode) -> Self {
        Self {
            core: DeltaFilterCore::with_base_mode(snapshot_interval, base_mode),
        }
    }

    /// Returns the interval between snapshots.
    pub fn snapshot_interval(&self) -> Duration {
        self.core.snapshot_interval()
    }

    /// Returns the base mode.
    pub fn base_mode(&self) -> DeltaBaseMode {
        self.core.base_mode()
    }

    /// Returns the word size in bytes.
    pub fn word_bytes(&self) -> usize {
        self.core.word_bytes()
    }
}

impl Default for Delta16Filter {
    /// Creates a filter with the default snapshot interval.
    fn default() -> Self {
        Self::from_millis(DELTA_DEFAULT_SNAPSHOT_INTERVAL_MS)
    }
}

impl Default for Delta32Filter {
    /// Creates a filter with the default snapshot interval.
    fn default() -> Self {
        Self::from_millis(DELTA_DEFAULT_SNAPSHOT_INTERVAL_MS)
    }
}

impl Default for Delta64Filter {
    /// Creates a filter with the default snapshot interval.
    fn default() -> Self {
        Self::from_millis(DELTA_DEFAULT_SNAPSHOT_INTERVAL_MS)
    }
}

impl FilterTrait for Delta16Filter {
    fn id(&self) -> &'static str {
        "delta16.v1"
    }

    fn apply_outbound(
        &self,
        ctx: RouteContext<'_>,
        payload: Vec<u8>,
    ) -> Result<Vec<Vec<u8>>, FilterError> {
        self.core.apply_outbound(ctx, payload)
    }

    fn apply_inbound(
        &self,
        ctx: RouteContext<'_>,
        payload: Vec<u8>,
    ) -> Result<FilterInboundResult, FilterError> {
        self.core.apply_inbound(ctx, payload)
    }

    fn apply_inbound_with_meta(
        &self,
        ctx: RouteContext<'_>,
        payload: Vec<u8>,
        meta: &mut MetaWriter,
    ) -> Result<FilterInboundResult, FilterError> {
        self.core.apply_inbound_with_meta(ctx, payload, meta)
    }
}

impl FilterTrait for Delta32Filter {
    fn id(&self) -> &'static str {
        "delta32.v1"
    }

    fn apply_outbound(
        &self,
        ctx: RouteContext<'_>,
        payload: Vec<u8>,
    ) -> Result<Vec<Vec<u8>>, FilterError> {
        self.core.apply_outbound(ctx, payload)
    }

    fn apply_inbound(
        &self,
        ctx: RouteContext<'_>,
        payload: Vec<u8>,
    ) -> Result<FilterInboundResult, FilterError> {
        self.core.apply_inbound(ctx, payload)
    }

    fn apply_inbound_with_meta(
        &self,
        ctx: RouteContext<'_>,
        payload: Vec<u8>,
        meta: &mut MetaWriter,
    ) -> Result<FilterInboundResult, FilterError> {
        self.core.apply_inbound_with_meta(ctx, payload, meta)
    }
}

impl FilterTrait for Delta64Filter {
    fn id(&self) -> &'static str {
        "delta64.v1"
    }

    fn apply_outbound(
        &self,
        ctx: RouteContext<'_>,
        payload: Vec<u8>,
    ) -> Result<Vec<Vec<u8>>, FilterError> {
        self.core.apply_outbound(ctx, payload)
    }

    fn apply_inbound(
        &self,
        ctx: RouteContext<'_>,
        payload: Vec<u8>,
    ) -> Result<FilterInboundResult, FilterError> {
        self.core.apply_inbound(ctx, payload)
    }

    fn apply_inbound_with_meta(
        &self,
        ctx: RouteContext<'_>,
        payload: Vec<u8>,
        meta: &mut MetaWriter,
    ) -> Result<FilterInboundResult, FilterError> {
        self.core.apply_inbound_with_meta(ctx, payload, meta)
    }
}

/// Checks if a new snapshot is needed.
fn must_send_snapshot<const WORD_BYTES: usize>(
    entry: &DeltaEntry,
    payload: &[u8],
    now: Instant,
    snapshot_interval: Duration,
) -> bool {
    entry.base.len() != payload.len()
        || payload.len() % WORD_BYTES != 0
        || now.duration_since(entry.last_snapshot_at) >= snapshot_interval
}

/// Returns the encoded snapshot size.
fn encoded_snapshot_len(payload: &[u8]) -> usize {
    HEADER_BYTES + payload.len()
}

/// Encodes a full snapshot.
fn encode_snapshot<const WORD_BYTES: usize>(
    base_mode: DeltaBaseMode,
    payload: &[u8],
) -> Result<Vec<u8>, FilterError> {
    let word_count = word_count::<WORD_BYTES>(payload)?;
    let changed_count = u32::try_from(word_count)
        .map_err(|_| FilterError::DeltaEncodeFailed("delta word count is too large".into()))?;
    let mut out = Vec::with_capacity(encoded_snapshot_len(payload));
    write_header::<WORD_BYTES>(
        &mut out,
        base_mode,
        DeltaFrameKind::Snapshot,
        DeltaEncoding::None,
        payload.len(),
        word_count,
        changed_count,
    )?;
    out.extend_from_slice(payload);
    Ok(out)
}

/// Encodes the shortest delta.
fn encode_best_delta<const WORD_BYTES: usize>(
    base_mode: DeltaBaseMode,
    base: &[u8],
    payload: &[u8],
) -> Result<EncodedDelta, FilterError> {
    let word_count = word_count::<WORD_BYTES>(payload)?;
    let changed = changed_indexes::<WORD_BYTES>(base, payload);
    let changed_count = u32::try_from(changed.len())
        .map_err(|_| FilterError::DeltaEncodeFailed("too many changed words".into()))?;

    let index_len = index_list_len::<WORD_BYTES>(changed.len())?;
    let range_count = count_ranges(&changed);
    let range_len = ranges_len::<WORD_BYTES>(range_count, changed.len())?;
    let bitmap_len = bitmap_len::<WORD_BYTES>(word_count, changed.len())?;
    let (encoding, body_len) = choose_encoding(index_len, range_len, bitmap_len);

    let mut out = Vec::with_capacity(HEADER_BYTES + body_len);
    write_header::<WORD_BYTES>(
        &mut out,
        base_mode,
        DeltaFrameKind::Delta,
        encoding,
        payload.len(),
        word_count,
        changed_count,
    )?;
    match encoding {
        DeltaEncoding::IndexList => {
            encode_index_list_into::<WORD_BYTES>(&mut out, payload, &changed)?
        }
        DeltaEncoding::Ranges => {
            encode_ranges_into::<WORD_BYTES>(&mut out, payload, &changed, range_count)?
        }
        DeltaEncoding::Bitmap => {
            encode_bitmap_into::<WORD_BYTES>(&mut out, payload, word_count, &changed)?
        }
        DeltaEncoding::None => {
            return Err(FilterError::DeltaEncodeFailed(
                "delta encoding is not selected".into(),
            ))
        }
    }

    Ok(EncodedDelta { payload: out })
}

/// Returns indexes of changed words.
fn changed_indexes<const WORD_BYTES: usize>(base: &[u8], payload: &[u8]) -> Vec<usize> {
    let mut indexes = Vec::new();
    for index in 0..payload.len() / WORD_BYTES {
        let start = index * WORD_BYTES;
        let end = start + WORD_BYTES;
        if base[start..end] != payload[start..end] {
            indexes.push(index);
        }
    }
    indexes
}

/// Returns the IndexList size.
fn index_list_len<const WORD_BYTES: usize>(changed_count: usize) -> Result<usize, FilterError> {
    changed_count
        .checked_mul(U32_BYTES + WORD_BYTES)
        .ok_or_else(|| FilterError::DeltaEncodeFailed("delta index list is too large".into()))
}

/// Returns the Ranges size.
fn ranges_len<const WORD_BYTES: usize>(
    range_count: usize,
    changed_count: usize,
) -> Result<usize, FilterError> {
    let headers_len = range_count
        .checked_mul(U32_BYTES + U32_BYTES)
        .and_then(|value| value.checked_add(U32_BYTES))
        .ok_or_else(|| FilterError::DeltaEncodeFailed("delta ranges are too large".into()))?;
    let values_len = changed_count
        .checked_mul(WORD_BYTES)
        .ok_or_else(|| FilterError::DeltaEncodeFailed("delta range values are too large".into()))?;
    headers_len
        .checked_add(values_len)
        .ok_or_else(|| FilterError::DeltaEncodeFailed("delta ranges are too large".into()))
}

/// Returns the RawBitmap size.
fn bitmap_len<const WORD_BYTES: usize>(
    word_count: usize,
    changed_count: usize,
) -> Result<usize, FilterError> {
    let header_len = word_count.div_ceil(8);
    let values_len = changed_count.checked_mul(WORD_BYTES).ok_or_else(|| {
        FilterError::DeltaEncodeFailed("delta bitmap values are too large".into())
    })?;
    header_len
        .checked_add(values_len)
        .ok_or_else(|| FilterError::DeltaEncodeFailed("delta bitmap is too large".into()))
}

/// Selects the shortest encoding method.
fn choose_encoding(
    index_len: usize,
    range_len: usize,
    bitmap_len: usize,
) -> (DeltaEncoding, usize) {
    let mut encoding = DeltaEncoding::IndexList;
    let mut len = index_len;
    if range_len < len {
        encoding = DeltaEncoding::Ranges;
        len = range_len;
    }
    if bitmap_len < len {
        encoding = DeltaEncoding::Bitmap;
        len = bitmap_len;
    }
    (encoding, len)
}

/// Writes changes as an index list.
fn encode_index_list_into<const WORD_BYTES: usize>(
    out: &mut Vec<u8>,
    payload: &[u8],
    changed: &[usize],
) -> Result<(), FilterError> {
    for index in changed {
        let index_u32 = u32::try_from(*index)
            .map_err(|_| FilterError::DeltaEncodeFailed("delta index is too large".into()))?;
        out.extend_from_slice(&index_u32.to_be_bytes());
        write_word::<WORD_BYTES>(out, payload, *index);
    }
    Ok(())
}

/// Writes changes as ranges.
fn encode_ranges_into<const WORD_BYTES: usize>(
    out: &mut Vec<u8>,
    payload: &[u8],
    changed: &[usize],
    range_count: usize,
) -> Result<(), FilterError> {
    let range_count = u32::try_from(range_count)
        .map_err(|_| FilterError::DeltaEncodeFailed("too many delta ranges".into()))?;
    out.extend_from_slice(&range_count.to_be_bytes());
    for (start, len) in RangeIter::new(changed) {
        let start_u32 = u32::try_from(start)
            .map_err(|_| FilterError::DeltaEncodeFailed("delta range start is too large".into()))?;
        let len_u32 = u32::try_from(len).map_err(|_| {
            FilterError::DeltaEncodeFailed("delta range length is too large".into())
        })?;
        out.extend_from_slice(&start_u32.to_be_bytes());
        out.extend_from_slice(&len_u32.to_be_bytes());
        for offset in 0..len {
            write_word::<WORD_BYTES>(out, payload, start + offset);
        }
    }
    Ok(())
}

/// Writes changes as a bitmap.
fn encode_bitmap_into<const WORD_BYTES: usize>(
    out: &mut Vec<u8>,
    payload: &[u8],
    word_count: usize,
    changed: &[usize],
) -> Result<(), FilterError> {
    let bitmap_len = word_count.div_ceil(8);
    let bitmap_start = out.len();
    out.resize(bitmap_start + bitmap_len, 0);
    for index in changed {
        let Some(slot) = out.get_mut(bitmap_start + index / 8) else {
            return Err(FilterError::DeltaEncodeFailed(
                "delta bitmap index is out of range".into(),
            ));
        };
        *slot |= 1 << (index % 8);
    }

    for index in changed {
        write_word::<WORD_BYTES>(out, payload, *index);
    }
    Ok(())
}

/// Counts ranges.
fn count_ranges(changed: &[usize]) -> usize {
    RangeIter::new(changed).count()
}

struct RangeIter<'a> {
    rest: &'a [usize],
}

impl<'a> RangeIter<'a> {
    /// Creates an iterator over adjacent ranges.
    fn new(changed: &'a [usize]) -> Self {
        Self { rest: changed }
    }
}

impl Iterator for RangeIter<'_> {
    type Item = (usize, usize);

    fn next(&mut self) -> Option<Self::Item> {
        let (&start, rest) = self.rest.split_first()?;
        let mut len = 1usize;
        let mut previous = start;
        let mut used = 1usize;
        for index in rest {
            if *index != previous + 1 {
                break;
            }
            len += 1;
            used += 1;
            previous = *index;
        }
        self.rest = &self.rest[used..];
        Some((start, len))
    }
}

/// Writes one word to output.
fn write_word<const WORD_BYTES: usize>(out: &mut Vec<u8>, payload: &[u8], word_index: usize) {
    let start = word_index * WORD_BYTES;
    let end = start + WORD_BYTES;
    out.extend_from_slice(&payload[start..end]);
}

/// Writes the common header.
fn write_header<const WORD_BYTES: usize>(
    out: &mut Vec<u8>,
    base_mode: DeltaBaseMode,
    frame_kind: DeltaFrameKind,
    encoding: DeltaEncoding,
    original_len: usize,
    word_count: usize,
    changed_count: u32,
) -> Result<(), FilterError> {
    let original_len = u32::try_from(original_len)
        .map_err(|_| FilterError::DeltaEncodeFailed("delta payload is too large".into()))?;
    let word_count = u32::try_from(word_count)
        .map_err(|_| FilterError::DeltaEncodeFailed("delta word count is too large".into()))?;

    out.extend_from_slice(DELTA_MAGIC);
    out.push(WORD_BYTES as u8);
    out.push(base_mode.code());
    out.push(frame_kind.code());
    out.push(encoding.code());
    out.extend_from_slice(&original_len.to_be_bytes());
    out.extend_from_slice(&word_count.to_be_bytes());
    out.extend_from_slice(&changed_count.to_be_bytes());
    Ok(())
}

/// Decodes the header or returns None for a normal payload.
fn decode_frame<const WORD_BYTES: usize>(
    payload: &[u8],
) -> Result<Option<DecodedFrame>, FilterError> {
    if payload.len() < DELTA_MAGIC.len() || &payload[..DELTA_MAGIC.len()] != DELTA_MAGIC {
        return Ok(None);
    }
    if payload.len() < HEADER_BYTES {
        return Err(FilterError::DeltaDecodeFailed(
            "delta payload is too short".into(),
        ));
    }

    let mut index = DELTA_MAGIC.len();
    let word_bytes = read_u8(payload, &mut index)?;
    if word_bytes != WORD_BYTES as u8 {
        return Err(FilterError::DeltaDecodeFailed(
            "delta word size does not match filter".into(),
        ));
    }
    let base_mode = DeltaBaseMode::from_code(read_u8(payload, &mut index)?)?;
    let frame_kind = DeltaFrameKind::from_code(read_u8(payload, &mut index)?)?;
    let encoding = DeltaEncoding::from_code(read_u8(payload, &mut index)?)?;
    let original_len = read_u32(payload, &mut index)? as usize;
    let word_count = read_u32(payload, &mut index)? as usize;
    let changed_count = read_u32(payload, &mut index)?;

    if frame_kind == DeltaFrameKind::Snapshot && encoding != DeltaEncoding::None {
        return Err(FilterError::DeltaDecodeFailed(
            "delta snapshot has bad encoding".into(),
        ));
    }
    if frame_kind == DeltaFrameKind::Delta && encoding == DeltaEncoding::None {
        return Err(FilterError::DeltaDecodeFailed(
            "delta frame has no encoding".into(),
        ));
    }
    if original_len % WORD_BYTES == 0 {
        let expected_word_count = original_len / WORD_BYTES;
        if word_count != expected_word_count {
            return Err(FilterError::DeltaDecodeFailed(
                "delta word count does not match length".into(),
            ));
        }
    } else if frame_kind == DeltaFrameKind::Delta {
        return Err(FilterError::DeltaDecodeFailed(
            "delta frame length is not aligned".into(),
        ));
    }

    Ok(Some(DecodedFrame {
        base_mode,
        frame_kind,
        encoding,
        original_len,
        word_count,
        changed_count,
        body_start: index,
    }))
}

/// Applies the delta body to the base.
fn apply_delta_body<const WORD_BYTES: usize>(
    plain: &mut [u8],
    frame: &DecodedFrame,
    body: &[u8],
) -> Result<(), FilterError> {
    match frame.encoding {
        DeltaEncoding::IndexList => apply_index_list::<WORD_BYTES>(plain, frame, body),
        DeltaEncoding::Ranges => apply_ranges::<WORD_BYTES>(plain, frame, body),
        DeltaEncoding::Bitmap => apply_bitmap::<WORD_BYTES>(plain, frame, body),
        DeltaEncoding::None => Err(FilterError::DeltaDecodeFailed(
            "delta body has no encoding".into(),
        )),
    }
}

/// Applies IndexList.
fn apply_index_list<const WORD_BYTES: usize>(
    plain: &mut [u8],
    frame: &DecodedFrame,
    body: &[u8],
) -> Result<(), FilterError> {
    let item_len = U32_BYTES + WORD_BYTES;
    let expected_len = usize::try_from(frame.changed_count)
        .map_err(|_| FilterError::DeltaDecodeFailed("delta count is too large".into()))?
        * item_len;
    if body.len() != expected_len {
        return Err(FilterError::DeltaDecodeFailed(
            "delta index list has bad length".into(),
        ));
    }

    let mut offset = 0usize;
    for _ in 0..frame.changed_count {
        let index = read_u32(body, &mut offset)? as usize;
        copy_word::<WORD_BYTES>(plain, index, &body[offset..offset + WORD_BYTES])?;
        offset += WORD_BYTES;
    }
    Ok(())
}

/// Applies Ranges.
fn apply_ranges<const WORD_BYTES: usize>(
    plain: &mut [u8],
    frame: &DecodedFrame,
    body: &[u8],
) -> Result<(), FilterError> {
    let mut offset = 0usize;
    let range_count = read_u32(body, &mut offset)? as usize;
    let mut changed = 0u32;
    for _ in 0..range_count {
        let start = read_u32(body, &mut offset)? as usize;
        let len = read_u32(body, &mut offset)? as usize;
        for item in 0..len {
            let end = offset.checked_add(WORD_BYTES).ok_or_else(|| {
                FilterError::DeltaDecodeFailed("delta range offset is too large".into())
            })?;
            if end > body.len() {
                return Err(FilterError::DeltaDecodeFailed(
                    "delta range value is truncated".into(),
                ));
            }
            copy_word::<WORD_BYTES>(plain, start + item, &body[offset..end])?;
            offset = end;
            changed = changed.checked_add(1).ok_or_else(|| {
                FilterError::DeltaDecodeFailed("delta range count is too large".into())
            })?;
        }
    }
    if offset != body.len() || changed != frame.changed_count {
        return Err(FilterError::DeltaDecodeFailed(
            "delta ranges have bad length".into(),
        ));
    }
    Ok(())
}

/// Applies RawBitmap.
fn apply_bitmap<const WORD_BYTES: usize>(
    plain: &mut [u8],
    frame: &DecodedFrame,
    body: &[u8],
) -> Result<(), FilterError> {
    let bitmap_len = frame.word_count.div_ceil(8);
    if body.len() < bitmap_len {
        return Err(FilterError::DeltaDecodeFailed(
            "delta bitmap is truncated".into(),
        ));
    }
    let bitmap = &body[..bitmap_len];
    let values = &body[bitmap_len..];
    let expected_values_len = usize::try_from(frame.changed_count)
        .map_err(|_| FilterError::DeltaDecodeFailed("delta count is too large".into()))?
        * WORD_BYTES;
    if values.len() != expected_values_len {
        return Err(FilterError::DeltaDecodeFailed(
            "delta bitmap values have bad length".into(),
        ));
    }

    let mut value_offset = 0usize;
    let mut changed = 0u32;
    for index in 0..frame.word_count {
        let bit = (bitmap[index / 8] >> (index % 8)) & 1;
        if bit == 0 {
            continue;
        }
        let end = value_offset + WORD_BYTES;
        copy_word::<WORD_BYTES>(plain, index, &values[value_offset..end])?;
        value_offset = end;
        changed = changed.checked_add(1).ok_or_else(|| {
            FilterError::DeltaDecodeFailed("delta bitmap count is too large".into())
        })?;
    }
    if changed != frame.changed_count {
        return Err(FilterError::DeltaDecodeFailed(
            "delta bitmap count does not match header".into(),
        ));
    }
    Ok(())
}

/// Copies one word into the payload.
fn copy_word<const WORD_BYTES: usize>(
    plain: &mut [u8],
    word_index: usize,
    value: &[u8],
) -> Result<(), FilterError> {
    let start = word_index
        .checked_mul(WORD_BYTES)
        .ok_or_else(|| FilterError::DeltaDecodeFailed("delta word index is too large".into()))?;
    let end = start
        .checked_add(WORD_BYTES)
        .ok_or_else(|| FilterError::DeltaDecodeFailed("delta word index is too large".into()))?;
    if end > plain.len() || value.len() != WORD_BYTES {
        return Err(FilterError::DeltaDecodeFailed(
            "delta word is out of range".into(),
        ));
    }
    plain[start..end].copy_from_slice(value);
    Ok(())
}

/// Returns the number of words in a payload.
fn word_count<const WORD_BYTES: usize>(payload: &[u8]) -> Result<usize, FilterError> {
    if payload.len() % WORD_BYTES == 0 {
        Ok(payload.len() / WORD_BYTES)
    } else {
        Ok(0)
    }
}

/// Writes metadata about a delta frame.
fn write_delta_meta<const WORD_BYTES: usize>(
    meta: &mut MetaWriter,
    decoded: &DecodedPayload,
) -> Result<(), FilterError> {
    meta.push_record_with(META_RECORD_DELTA, |out| {
        out.push(WORD_BYTES as u8);
        out.push(decoded.base_mode.code());
        out.push(decoded.frame_kind.code());
        out.push(decoded.encoding.code());
        out.extend_from_slice(&decoded.changed_count.to_be_bytes());
        Ok(())
    })
    .map_err(|e| FilterError::Other(e.to_string()))
}

/// Reads one byte.
fn read_u8(payload: &[u8], index: &mut usize) -> Result<u8, FilterError> {
    if *index >= payload.len() {
        return Err(FilterError::DeltaDecodeFailed(
            "delta byte is truncated".into(),
        ));
    }
    let value = payload[*index];
    *index += 1;
    Ok(value)
}

/// Reads a u32 in big-endian order.
fn read_u32(payload: &[u8], index: &mut usize) -> Result<u32, FilterError> {
    let end = index
        .checked_add(U32_BYTES)
        .ok_or_else(|| FilterError::DeltaDecodeFailed("delta index is too large".into()))?;
    if end > payload.len() {
        return Err(FilterError::DeltaDecodeFailed(
            "delta number is truncated".into(),
        ));
    }

    let bytes: [u8; U32_BYTES] = payload[*index..end]
        .try_into()
        .map_err(|_| FilterError::DeltaDecodeFailed("delta number is invalid".into()))?;
    *index = end;
    Ok(u32::from_be_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filters::FilterTrait;

    fn ctx(channel: &'static str) -> RouteContext<'static> {
        RouteContext {
            tenant: "tenant_1",
            channel,
        }
    }

    fn roundtrip<F: FilterTrait>(filter: &F, payloads: &[Vec<u8>]) {
        for payload in payloads {
            let mut encoded = filter
                .apply_outbound(ctx("public.state"), payload.clone())
                .unwrap();
            assert_eq!(encoded.len(), 1);
            let decoded = filter
                .apply_inbound(ctx("public.state"), encoded.pop().unwrap())
                .unwrap();
            assert_eq!(decoded.payloads, vec![payload.clone()]);
            assert!(decoded.messages.is_empty());
        }
    }

    #[test]
    fn delta16_roundtrip_returns_original_payloads() {
        let filter = Delta16Filter::from_millis(60_000);
        roundtrip(
            &filter,
            &[
                vec![1, 0, 2, 0, 3, 0, 4, 0],
                vec![1, 0, 8, 0, 3, 0, 4, 0],
                vec![1, 0, 8, 0, 9, 0, 4, 0],
            ],
        );
    }

    #[test]
    fn delta32_roundtrip_returns_original_payloads() {
        let filter = Delta32Filter::from_millis(60_000);
        roundtrip(
            &filter,
            &[
                vec![1, 0, 0, 0, 2, 0, 0, 0],
                vec![1, 0, 0, 0, 7, 0, 0, 0],
                vec![5, 0, 0, 0, 7, 0, 0, 0],
            ],
        );
    }

    #[test]
    fn delta64_roundtrip_returns_original_payloads() {
        let filter = Delta64Filter::from_millis(60_000);
        roundtrip(
            &filter,
            &[vec![1, 0, 0, 0, 0, 0, 0, 0], vec![2, 0, 0, 0, 0, 0, 0, 0]],
        );
    }

    #[test]
    fn snapshot_interval_sends_snapshot_again() {
        let filter = Delta32Filter::from_millis(0);
        let first = vec![1, 0, 0, 0];
        let second = vec![2, 0, 0, 0];

        let encoded_first = filter.apply_outbound(ctx("public.state"), first).unwrap();
        let encoded_second = filter.apply_outbound(ctx("public.state"), second).unwrap();

        let first_frame = decode_frame::<4>(&encoded_first[0]).unwrap().unwrap();
        let second_frame = decode_frame::<4>(&encoded_second[0]).unwrap().unwrap();
        assert_eq!(first_frame.frame_kind, DeltaFrameKind::Snapshot);
        assert_eq!(second_frame.frame_kind, DeltaFrameKind::Snapshot);
    }

    #[test]
    fn snapshot_base_keeps_original_base() {
        let filter =
            Delta32Filter::with_base_mode(Duration::from_secs(60), DeltaBaseMode::SnapshotBase);
        roundtrip(
            &filter,
            &[
                vec![1, 0, 0, 0, 1, 0, 0, 0],
                vec![2, 0, 0, 0, 1, 0, 0, 0],
                vec![1, 0, 0, 0, 3, 0, 0, 0],
            ],
        );
    }

    #[test]
    fn rolling_base_uses_previous_state() {
        let filter =
            Delta32Filter::with_base_mode(Duration::from_secs(60), DeltaBaseMode::RollingBase);
        roundtrip(
            &filter,
            &[
                vec![1, 0, 0, 0, 1, 0, 0, 0],
                vec![2, 0, 0, 0, 1, 0, 0, 0],
                vec![2, 0, 0, 0, 3, 0, 0, 0],
            ],
        );
    }

    #[test]
    fn encoder_chooses_ranges_for_adjacent_changes() {
        let base = vec![0u8; 1024];
        let mut payload = base.clone();
        payload[8] = 1;
        payload[12] = 2;
        payload[16] = 3;
        payload[20] = 4;

        let encoded = encode_best_delta::<4>(DeltaBaseMode::SnapshotBase, &base, &payload).unwrap();
        let frame = decode_frame::<4>(&encoded.payload).unwrap().unwrap();

        assert_eq!(frame.encoding, DeltaEncoding::Ranges);
        assert_eq!(frame.changed_count, 4);
    }

    #[test]
    fn encoder_chooses_bitmap_for_many_changes() {
        let base = vec![0u8; 256];
        let mut payload = base.clone();
        for index in (0..64).step_by(2) {
            payload[index * 4] = 1;
        }

        let encoded = encode_best_delta::<4>(DeltaBaseMode::SnapshotBase, &base, &payload).unwrap();
        let frame = decode_frame::<4>(&encoded.payload).unwrap().unwrap();

        assert_eq!(frame.encoding, DeltaEncoding::Bitmap);
    }

    #[test]
    fn delta_that_is_not_shorter_falls_back_to_snapshot() {
        let filter = Delta32Filter::from_millis(60_000);
        let first = vec![0u8; 8];
        let second = vec![1u8; 8];

        let _ = filter.apply_outbound(ctx("public.state"), first).unwrap();
        let encoded = filter.apply_outbound(ctx("public.state"), second).unwrap();
        let frame = decode_frame::<4>(&encoded[0]).unwrap().unwrap();

        assert_eq!(frame.frame_kind, DeltaFrameKind::Snapshot);
    }

    #[test]
    fn different_channels_keep_separate_state() {
        let filter = Delta32Filter::from_millis(60_000);
        let first = vec![1, 0, 0, 0];
        let second = vec![2, 0, 0, 0];

        let _ = filter.apply_outbound(ctx("public.a"), first).unwrap();
        let encoded = filter.apply_outbound(ctx("public.b"), second).unwrap();
        let frame = decode_frame::<4>(&encoded[0]).unwrap().unwrap();

        assert_eq!(frame.frame_kind, DeltaFrameKind::Snapshot);
    }

    #[test]
    fn bad_delta_payload_returns_error() {
        let filter = Delta32Filter::from_millis(60_000);
        let mut payload = DELTA_MAGIC.to_vec();
        payload.push(4);

        let err = filter
            .apply_inbound(ctx("public.state"), payload)
            .unwrap_err();
        assert!(matches!(err, FilterError::DeltaDecodeFailed(_)));
    }

    #[test]
    fn plain_payload_passes_through() {
        let filter = Delta32Filter::from_millis(60_000);
        let payload = b"plain".to_vec();

        let result = filter
            .apply_inbound(ctx("public.state"), payload.clone())
            .unwrap();

        assert_eq!(result.payloads, vec![payload]);
    }
}
