// Copyright (c) 2026 FastPubSub Network
// SPDX-License-Identifier: MIT OR Apache-2.0
// Project: https://fastpubsub.com

//! Transport layer: abstraction and WebSocket / WebTransport implementations.
//!
//! Transport-level TLS is separate from payload encryption (see the crypto filter contract).
//! The async [`Transport`] API is built for the **Tokio** runtime, for example `#[tokio::main]` or a custom `Runtime`.
//! Connection and subscriptions are released when the transport is **dropped**. There is no separate `close`.

use async_trait::async_trait;

mod route_pattern;

#[cfg(feature = "websocket")]
mod link_quality;
#[cfg(feature = "websocket")]
mod overlap_dedup;
#[cfg(feature = "websocket")]
mod websocket;
#[cfg(feature = "websocket")]
mod ws_connection;
#[cfg(feature = "websocket")]
mod ws_delivery;
#[cfg(feature = "websocket")]
mod ws_protocol;
#[cfg(feature = "websocket")]
mod ws_proxy;
#[cfg(feature = "websocket")]
mod ws_reconnect;
#[cfg(feature = "websocket")]
mod ws_subscriptions;
#[cfg(feature = "websocket")]
mod ws_task;

#[cfg(feature = "webtransport")]
mod webtransport;

#[cfg(any(feature = "websocket", feature = "rest"))]
use crate::client::HttpClientConfig;
use crate::filters::{FilterRegistration, FilterTimerMode};
use crate::metadata::{MetaMode, SdkMessage};

pub use bytes::Bytes;
pub use route_pattern::make_route_subscription_key as make_subscription_key;
pub use route_pattern::{make_route_subscription_key, validate_channel_pattern};
#[cfg(feature = "websocket")]
pub use link_quality::{LinkQuality, LinkQualitySnapshot};
#[cfg(feature = "websocket")]
pub use ws_protocol::{
    format_ok_subscribe, format_ok_unsubscribe, is_pong_line, parse_sub_ack_line,
    InboundDeliverFrame, SubWireAck, FRAME_TAG_DELIVER, FRAME_TAG_PUBLISH_V2, WS_PING_LINE,
    WS_PONG_LINE,
};

#[cfg(feature = "websocket")]
pub use websocket::WebSocketTransport;

#[cfg(feature = "webtransport")]
pub use webtransport::WebTransport;

use std::fmt;
use std::io;

use tokio::sync::mpsc;

/// Events from the WebSocket background task.
#[cfg(feature = "websocket")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WebSocketEvent {
    /// Protocol, network, filter, or local delivery error.
    Error(WebSocketError),
    /// Filter notice from a filter.
    FilterNotice {
        /// Notice level: `info` or `warning`.
        level: &'static str,
        /// Notice text.
        message: String,
    },
    /// SDK started smooth reconnect and is opening the candidate connection.
    ReconnectStarted,
    /// Candidate connection became active after resubscribe.
    ReconnectReady,
    /// Perimeter confirmed subscription `OK:SUB:tenant:pattern`.
    SubscriptionConfirmed {
        /// Tenant of the confirmed subscription.
        tenant: String,
        /// One confirmed channel pattern.
        channel_pattern: String,
    },
    /// Perimeter confirmed unsubscribe `OK:UNSUB:tenant:pattern`.
    UnsubscriptionConfirmed {
        /// Tenant of the confirmed unsubscribe.
        tenant: String,
        /// One confirmed channel pattern.
        channel_pattern: String,
    },
    /// Duplicate inbound frame was dropped during the overlap window.
    DuplicateDropped,
    /// Measured WS round-trip after `PING`/`PONG`.
    RttMeasured {
        /// RTT in milliseconds.
        rtt_ms: u32,
    },
}

/// Typed WebSocket transport error.
#[cfg(feature = "websocket")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WebSocketError {
    /// Perimeter sent text `ERR:...`.
    Server { message: String },
    /// Error while opening a reconnect connection.
    Reconnect {
        /// Local connection id.
        connection_id: u64,
        /// Transport/TLS/WebSocket error text.
        message: String,
    },
    /// WebSocket read error.
    Read {
        /// Local connection id.
        connection_id: u64,
        /// Read error text.
        message: String,
    },
    /// Write error or local command error.
    Transport {
        /// Operation where the error happened.
        operation: String,
        /// Error text.
        message: String,
    },
    /// Error while decoding binary deliver-frame.
    Decode { message: String },
    /// Inbound filter rejected or changed the message in a bad way.
    InboundFilter { message: String },
    /// Local subscription queue is full.
    SubscriptionQueueFull {
        /// Subscription tenant.
        tenant: String,
        /// Subscription pattern.
        channel_pattern: String,
    },
    /// Edge sent a pattern that is not in the local task registry.
    MissingSubscription {
        /// Delivery tenant.
        tenant: String,
        /// Patterns without local queues.
        channel_patterns: Vec<String>,
    },
    /// Edge sent a deliver-frame without matching patterns.
    EmptyDelivery {
        /// Delivery tenant.
        tenant: String,
        /// Delivery channel.
        channel: String,
    },
}

/// Parameters for [`Transport::connect`]: filters, token, and typed events.
#[derive(Default)]
pub struct TransportConnectParams {
    /// Route filters. Inbound uses config order, outbound uses reverse order.
    pub filters: Vec<FilterRegistration>,
    /// Timer mode that the transport uses for all filters.
    pub filter_timer_mode: FilterTimerMode,
    /// Metadata mode for inbound messages.
    pub meta_mode: MetaMode,
    /// Access token (`AT_...`) for handshake.
    pub access_token: String,
    /// Optional WebSocket task events.
    #[cfg(feature = "websocket")]
    pub on_websocket_event: Option<mpsc::Sender<WebSocketEvent>>,
    /// Squid proxy (REST + CONNECT for WSS), user/password in [`HttpClientConfig`].
    #[cfg(any(feature = "websocket", feature = "rest"))]
    pub http_config: Option<HttpClientConfig>,
    /// WS application ping interval: 1, 3, or 5 seconds. None disables ping.
    #[cfg(feature = "websocket")]
    pub ping_interval_secs: Option<u8>,
}

/// Options for [`Transport::subscribe`].
#[derive(Debug, Clone)]
pub struct SubscribeOptions {
    /// `mpsc` queue size for backpressure.
    pub queue_capacity: usize,
}

impl Default for SubscribeOptions {
    fn default() -> Self {
        Self {
            queue_capacity: 256,
        }
    }
}

/// Delivery mode for outbound publish on the edge side (router hint).
///
/// The real behavior is in the FastPubSubNetwork / perimeter protocol. SDK only passes the value in the frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PublishDeliveryMode {
    /// To all subscribers on the route (broadcast / fan-out).
    #[default]
    Broadcast,
    /// Exactly one target with the lowest latency among candidates.
    DeliverOneLowLatency,
    /// Exactly one randomly selected target (balance between equal candidates).
    DeliverOneRandom,
}

/// Options for transport publish call (contract v0.1, can grow).
#[derive(Debug, Clone, Default)]
pub struct PublishOptions {
    /// Delivery mode: [`PublishDeliveryMode`].
    pub delivery: PublishDeliveryMode,
}

/// Transport kind for logs, metrics, and implementation selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransportKind {
    /// WebSocket, required in contract v0.1.
    WebSocket,
    /// WebTransport, optional / future.
    WebTransport,
}

/// Transport layer errors without FastPubSubNetwork protocol details.
#[derive(Debug)]
pub enum TransportError {
    /// General I/O error: network, TLS, and so on.
    Io(io::Error),
    /// Transport is not connected yet or is already closed.
    NotConnected,
    /// Operation is not supported by this implementation.
    Unsupported,
    /// Subscription with this tenant+pattern pair already exists.
    DuplicateSubscription,
    /// Route pattern is empty or invalid for local registration.
    InvalidRoutePattern,
    /// Custom error message for stubs and debugging.
    Other(String),
}

impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransportError::Io(e) => write!(f, "I/O error: {e}"),
            TransportError::NotConnected => write!(f, "transport is not connected"),
            TransportError::Unsupported => write!(f, "operation is not supported"),
            TransportError::DuplicateSubscription => {
                write!(
                    f,
                    "subscription with this tenant+channel pattern pair already exists"
                )
            }
            TransportError::InvalidRoutePattern => {
                write!(f, "channel pattern is invalid or empty")
            }
            TransportError::Other(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for TransportError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            TransportError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for TransportError {
    fn from(value: io::Error) -> Self {
        TransportError::Io(value)
    }
}

/// Transport abstraction between the SDK and FastPubSubNetwork edge.
///
/// WebSocket: background task, inbound/outbound filters, commands over `mpsc`.
/// Close happens in [`Drop`]. There is no separate `close` in the trait.
#[async_trait]
pub trait Transport: Send {
    /// Returns the transport kind for diagnostics.
    fn kind(&self) -> TransportKind;

    /// Connects to an endpoint, for example `wss://.../ws`.
    ///
    /// Async: DNS/TCP/TLS and WebSocket handshake run here on Tokio.
    /// The stub has no real network yet.
    async fn connect(
        &mut self,
        endpoint: &str,
        params: TransportConnectParams,
    ) -> Result<(), TransportError>;

    /// Publishes a frame: `tenant` + concrete subject `channel`. No wildcards in publish body.
    ///
    /// [`Bytes`] gives cheap `Clone` and ownership transfer without copying the whole array.
    /// Success means accepted locally by transport/queue, not delivered to listeners.
    /// Async: real network sending does not block the executor thread.
    async fn publish(
        &mut self,
        tenant: &str,
        channel: &str,
        payload: Bytes,
        options: &PublishOptions,
    ) -> Result<(), TransportError>;

    /// Subscribes by channel **pattern**: `#`, `*`, segments separated by `.`.
    ///
    /// Key in [`DashMap`]: [`make_subscription_key`]. Response is a direct [`mpsc::Receiver`].
    /// After reading, call [`Transport::unsubscribe`] with the same pattern.
    async fn subscribe(
        &mut self,
        tenant: &str,
        channel_pattern: &str,
        options: &SubscribeOptions,
    ) -> Result<mpsc::Receiver<SdkMessage>, TransportError>;

    /// Removes a subscription from the map by tenant and the same `channel_pattern` as in [`Transport::subscribe`].
    async fn unsubscribe(
        &mut self,
        tenant: &str,
        channel_pattern: &str,
    ) -> Result<(), TransportError>;
}
