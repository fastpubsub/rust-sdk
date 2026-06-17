// Copyright (c) 2026 FastPubSub Network
// SPDX-License-Identifier: MIT OR Apache-2.0
// Project: https://fastpubsub.com

//! Route filters (tenant + channel pattern): trait and implementations.
//!
//! Contract filters run inline in the connection pipeline, without network I/O.

mod bandwidth_limiter;
mod channel_batch;
mod compressed;
mod debug_log;
mod delta;
mod dummy;
mod encryption;
mod fragment;
mod latest_only;
mod send_rate;

pub use bandwidth_limiter::BandwidthLimiterFilter;
pub use channel_batch::{ChannelBatchFilter, CHANNEL_BATCH_DEFAULT_MAX_BYTES};
pub use compressed::{CompressedFilter, DEFLATE_RAW_ALGORITHM};
pub use debug_log::{DebugLogFilter, DebugLogOptions};
pub use delta::{
    Delta16Filter, Delta32Filter, Delta64Filter, DeltaBaseMode, DELTA_DEFAULT_SNAPSHOT_INTERVAL_MS,
};
pub use dummy::DummyFilter;
pub use encryption::{
    CryptoKeyManager, EncryptionAlgorithm, EncryptionFilter, AES256_GCM_ALGORITHM,
    CHACHA20_POLY1305_ALGORITHM,
};
pub use fragment::{
    FragmentFilter, FRAGMENT_DEFAULT_DEFRAG_TIMEOUT_SECS, FRAGMENT_DEFAULT_MAX_FRAGMENT_BYTES,
    FRAGMENT_DEFAULT_REQUEST_INTERVAL_MS, FRAGMENT_DEFAULT_THRESHOLD_BYTES,
};
pub use latest_only::{
    InvalidPrefixPolicy, LatestOnlyFilter, LATEST_ONLY_DEFAULT_RESET_TIMEOUT_SECS,
};
pub use send_rate::{SendRateFilter, SendRateLimit};

use std::cell::RefCell;
use std::fmt;
use std::time::Duration;

use crate::metadata::MetaWriter;

/// Route context: tenant and subject or pattern.
///
/// For outbound publish, `channel` is usually an exact channel without `#` or `*`.
#[derive(Debug, Clone, Copy)]
pub struct RouteContext<'a> {
    /// Tenant id.
    pub tenant: &'a str,
    /// Route channel.
    pub channel: &'a str,
}

/// Message direction inside a filter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterDirection {
    /// From the app to the network.
    Outbound,
    /// From the network to the app.
    Inbound,
}

/// Timer mode for background filters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterTimerMode {
    /// Ultra low latency mode: 1000 times per second, one tick every 1 ms.
    UltraLowLatency1000Hz,
    /// High frequency mode: 500 times per second, one tick every 2 ms.
    HighFrequency500Hz,
    /// Low latency mode: 200 times per second, one tick every 5 ms.
    LowLatency200Hz,
    /// Normal SDK mode: 100 times per second, one tick every 10 ms.
    SdkDefault100Hz,
    /// Economy mode: 20 times per second, one tick every 50 ms.
    Economy20Hz,
    /// Slow mode: 10 times per second, one tick every 100 ms.
    Slow10Hz,
}

impl FilterTimerMode {
    /// Returns the timer rate in hertz.
    pub fn hz(self) -> u16 {
        match self {
            FilterTimerMode::UltraLowLatency1000Hz => 1_000,
            FilterTimerMode::HighFrequency500Hz => 500,
            FilterTimerMode::LowLatency200Hz => 200,
            FilterTimerMode::SdkDefault100Hz => 100,
            FilterTimerMode::Economy20Hz => 20,
            FilterTimerMode::Slow10Hz => 10,
        }
    }

    /// Returns the time between ticks.
    pub fn interval(self) -> Duration {
        match self {
            FilterTimerMode::UltraLowLatency1000Hz => Duration::from_millis(1),
            FilterTimerMode::HighFrequency500Hz => Duration::from_millis(2),
            FilterTimerMode::LowLatency200Hz => Duration::from_millis(5),
            FilterTimerMode::SdkDefault100Hz => Duration::from_millis(10),
            FilterTimerMode::Economy20Hz => Duration::from_millis(50),
            FilterTimerMode::Slow10Hz => Duration::from_millis(100),
        }
    }
}

impl Default for FilterTimerMode {
    fn default() -> Self {
        Self::SdkDefault100Hz
    }
}

/// Context for one filter timer tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FilterTimerContext {
    /// Mode chosen by the user or the SDK.
    pub mode: FilterTimerMode,
    /// Time between ticks.
    pub interval: Duration,
}

impl FilterTimerContext {
    /// Creates timer context from a mode.
    pub fn new(mode: FilterTimerMode) -> Self {
        Self {
            mode,
            interval: mode.interval(),
        }
    }
}

/// Message that a filter wants to send through the transport.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilterSendMessage {
    /// Tenant id.
    pub tenant: String,
    /// Publish channel.
    pub channel: String,
    /// Payload that the transport sends through the outbound pipeline.
    pub payload: Vec<u8>,
}

impl FilterSendMessage {
    /// Creates a message that a filter wants to send.
    pub fn new(
        tenant: impl Into<String>,
        channel: impl Into<String>,
        payload: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            tenant: tenant.into(),
            channel: channel.into(),
            payload: payload.into(),
        }
    }
}

/// Filter notice level from a filter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterNoticeLevel {
    /// Informational notice.
    Info,
    /// Debug warning.
    Warning,
}

/// Filter notice from a filter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilterNotice {
    /// Notice level.
    pub level: FilterNoticeLevel,
    /// Notice text.
    pub message: String,
}

impl FilterNotice {
    /// Creates an informational notice.
    pub fn info(message: impl Into<String>) -> Self {
        Self {
            level: FilterNoticeLevel::Info,
            message: message.into(),
        }
    }

    /// Creates a warning.
    pub fn warning(message: impl Into<String>) -> Self {
        Self {
            level: FilterNoticeLevel::Warning,
            message: message.into(),
        }
    }
}

thread_local! {
    static FILTER_NOTICE_QUEUE: RefCell<Option<Vec<FilterNotice>>> = RefCell::new(None);
}

/// Runs code with the shared filter notice queue.
pub(crate) fn with_filter_notice_queue<T>(work: impl FnOnce() -> T) -> (T, Vec<FilterNotice>) {
    let previous = FILTER_NOTICE_QUEUE.with(|queue| queue.replace(Some(Vec::new())));
    let result = work();
    let notices = FILTER_NOTICE_QUEUE.with(|queue| queue.replace(previous).unwrap_or_default());
    (result, notices)
}

/// Adds a filter notice to the current transport queue.
pub(crate) fn emit_filter_notice(notice: FilterNotice) {
    FILTER_NOTICE_QUEUE.with(|queue| {
        if let Some(notices) = queue.borrow_mut().as_mut() {
            notices.push(notice);
        }
    });
}

/// Result of one inbound message after filter processing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilterInboundResult {
    /// Payloads that must be delivered to local subscribers.
    pub payloads: Vec<Vec<u8>>,
    /// Messages that the filter asks the transport to send.
    pub messages: Vec<FilterSendMessage>,
}

impl FilterInboundResult {
    /// Creates a result from payloads and transport messages.
    pub fn new(payloads: Vec<Vec<u8>>, messages: Vec<FilterSendMessage>) -> Self {
        Self { payloads, messages }
    }

    /// Creates a result with one payload and no transport messages.
    pub fn single(payload: Vec<u8>) -> Self {
        Self {
            payloads: vec![payload],
            messages: Vec::new(),
        }
    }

    /// Creates a result with one payload and transport messages.
    pub fn single_with_messages(payload: Vec<u8>, messages: Vec<FilterSendMessage>) -> Self {
        Self {
            payloads: vec![payload],
            messages,
        }
    }
}

/// Filter with an exact tenant and a static channel prefix.
pub struct FilterRegistration {
    tenant: String,
    prefix: String,
    filter: Box<dyn FilterTrait + Send + Sync>,
}

impl FilterRegistration {
    /// Creates a filter entry for the pipeline.
    pub fn new<F>(tenant: impl Into<String>, prefix: impl Into<String>, filter: F) -> Self
    where
        F: FilterTrait + 'static,
    {
        Self {
            tenant: tenant.into(),
            prefix: prefix.into(),
            filter: Box::new(filter),
        }
    }

    /// Returns the exact tenant.
    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    /// Returns the channel prefix.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Returns the filter.
    pub fn filter(&self) -> &(dyn FilterTrait + Send + Sync) {
        self.filter.as_ref()
    }

    /// Checks if this filter must run for the route.
    pub fn matches(&self, ctx: RouteContext<'_>) -> bool {
        ctx.tenant == self.tenant && ctx.channel.starts_with(&self.prefix)
    }
}

/// Error while applying a filter.
#[derive(Debug, Clone)]
pub enum FilterError {
    /// Bad config, for example a bad encryption key.
    InvalidConfig(String),
    /// Policy requires encryption, but the message is not marked as encrypted.
    CryptoRequiredButMissing,
    /// Encryption failed.
    EncryptFailed,
    /// Decryption failed.
    DecryptFailed,
    /// Compression failed.
    CompressFailed(String),
    /// Decompression failed.
    DecompressFailed(String),
    /// Batch encode failed.
    BatchEncodeFailed(String),
    /// Batch decode failed.
    BatchDecodeFailed(String),
    /// Fragment encode failed.
    FragmentEncodeFailed(String),
    /// Fragment decode failed.
    FragmentDecodeFailed(String),
    /// Delta encode failed.
    DeltaEncodeFailed(String),
    /// Delta decode failed.
    DeltaDecodeFailed(String),
    /// Other reason.
    Other(String),
}

impl fmt::Display for FilterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FilterError::InvalidConfig(s) => write!(f, "invalid filter config: {s}"),
            FilterError::CryptoRequiredButMissing => {
                write!(f, "encryption is required but the message is not encrypted")
            }
            FilterError::EncryptFailed => write!(f, "encrypt failed"),
            FilterError::DecryptFailed => write!(f, "decrypt failed"),
            FilterError::CompressFailed(s) => write!(f, "compress failed: {s}"),
            FilterError::DecompressFailed(s) => write!(f, "decompress failed: {s}"),
            FilterError::BatchEncodeFailed(s) => write!(f, "batch encode failed: {s}"),
            FilterError::BatchDecodeFailed(s) => write!(f, "batch decode failed: {s}"),
            FilterError::FragmentEncodeFailed(s) => write!(f, "fragment encode failed: {s}"),
            FilterError::FragmentDecodeFailed(s) => write!(f, "fragment decode failed: {s}"),
            FilterError::DeltaEncodeFailed(s) => write!(f, "delta encode failed: {s}"),
            FilterError::DeltaDecodeFailed(s) => write!(f, "delta decode failed: {s}"),
            FilterError::Other(s) => write!(f, "{s}"),
        }
    }
}

impl std::error::Error for FilterError {}

/// Route filter: processing rule for tenant and channel.
///
/// The implementation must be fast and must not make network requests.
pub trait FilterTrait: Send + Sync {
    /// Returns a stable filter id for logs and metrics.
    fn id(&self) -> &'static str;

    /// Handles an outbound message.
    ///
    /// A filter can return one payload or many payloads.
    /// This is useful for splitting a large message into parts.
    /// By default, the outbound message passes through unchanged.
    fn apply_outbound(
        &self,
        ctx: RouteContext<'_>,
        payload: Vec<u8>,
    ) -> Result<Vec<Vec<u8>>, FilterError> {
        let _ = ctx;
        Ok(vec![payload])
    }

    /// Handles an inbound message.
    ///
    /// A filter can change the payload, for example decompress or decrypt it.
    /// A filter can also split one payload into many payloads.
    /// A filter can also return messages to send through the transport.
    /// By default, the inbound message passes through unchanged.
    fn apply_inbound(
        &self,
        ctx: RouteContext<'_>,
        payload: Vec<u8>,
    ) -> Result<FilterInboundResult, FilterError> {
        let _ = ctx;
        Ok(FilterInboundResult::single(payload))
    }

    /// Handles an inbound message and can write metadata.
    ///
    /// By default, calls the old inbound method without metadata.
    fn apply_inbound_with_meta(
        &self,
        ctx: RouteContext<'_>,
        payload: Vec<u8>,
        meta: &mut MetaWriter,
    ) -> Result<FilterInboundResult, FilterError> {
        let _ = meta;
        self.apply_inbound(ctx, payload)
    }

    /// Called by the transport timer from the main transport task.
    ///
    /// Normal filters can do nothing. A stateful filter can update its state,
    /// for example refill tokens for bandwidth control.
    fn on_timer(&self, ctx: FilterTimerContext) -> Result<Vec<FilterSendMessage>, FilterError> {
        let _ = ctx;
        Ok(Vec::new())
    }
}
