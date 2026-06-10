// Copyright (c) 2026 FastPubSub Network
// SPDX-License-Identifier: MIT OR Apache-2.0
// Project: https://fastpubsub.com

//! Client facade: `create_web_*` -> [`FastPubSubBuilder::add_filter`] -> [`FastPubSubBuilder::build`] (`.await`) -> [`FastPubSub::publish`].
//!
//! Filters and the WebSocket task are configured in `connect`; `publish` and `subscribe` go to the transport.

use tokio::sync::mpsc;

use crate::metadata::SdkMessage;
use crate::transport::{Bytes, PublishOptions, SubscribeOptions, Transport, TransportError};

#[cfg(feature = "websocket")]
use crate::transport::WebSocketTransport;

#[cfg(feature = "webtransport")]
use crate::transport::WebTransport;

mod http_config;
pub use http_config::{HttpClientConfig, HttpConfigError, ParsedProxy};

#[cfg(any(feature = "rest", feature = "websocket"))]
pub(crate) mod tls_roots;

#[cfg(feature = "rest")]
mod api;
#[cfg(feature = "rest")]
pub use api::{
    create_access_token, create_access_token_with_config, expires_at_after_seconds,
    expires_at_max_ttl, format_expires_at_rfc3339_z, parse_expires_at_input,
    parse_expires_at_input_clamp, ping, ping_fastest, ping_fastest_with_config, ping_many,
    ping_many_with_config, ping_with_config, utc_now_with_margin, AccessTokenBuildError,
    AccessTokenJsonError, ApiError, CreateAccessTokenBuilder, CreateAccessTokenRequest,
    CreateAccessTokenResponse, ExpiresAtParseError, PingManyResult, PingTiming, TenantGrant,
    TokenRights, DEFAULT_NOW_MARGIN_SECS, MAX_TOKEN_TTL_HOURS, MAX_TOKEN_TTL_MARGIN_SECS,
};

#[cfg(feature = "rest")]
mod discovery;
#[cfg(feature = "rest")]
pub use discovery::{
    default_bootstrap_url, fetch_edge_candidates, select_fastest_edge, DiscoveryError,
    EdgeCandidate, SelectedEdge,
};

#[cfg(feature = "rest")]
mod session;
#[cfg(feature = "rest")]
pub use session::{open, FastPubSubSession};

mod builder;
mod shared;
mod subscriptions;

pub use builder::FastPubSubBuilder;
pub use shared::SharedFastPubSub;
pub use subscriptions::{SubscriptionRegistry, SubscriptionSpec};

/// Error during [`FastPubSubBuilder::build`], for example when the transport cannot connect.
#[derive(Debug)]
pub enum BuildError {
    /// Transport error during `connect`.
    Transport(TransportError),
}

impl std::fmt::Display for BuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BuildError::Transport(e) => write!(f, "transport error: {e}"),
        }
    }
}

impl std::error::Error for BuildError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            BuildError::Transport(e) => Some(e),
        }
    }
}

impl From<TransportError> for BuildError {
    fn from(value: TransportError) -> Self {
        BuildError::Transport(value)
    }
}

/// Error from [`FastPubSub::publish`]: transport.
#[derive(Debug)]
pub enum PublishError {
    /// The transport did not accept the command.
    Transport(TransportError),
}

impl std::fmt::Display for PublishError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PublishError::Transport(e) => write!(f, "transport: {e}"),
        }
    }
}

impl std::error::Error for PublishError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PublishError::Transport(e) => Some(e),
        }
    }
}

impl From<TransportError> for PublishError {
    fn from(value: TransportError) -> Self {
        PublishError::Transport(value)
    }
}

/// Builds an edge WebSocket URL from the overlay network name. This is a stub without discovery.
pub fn overlay_network_endpoint(overlay: &str) -> String {
    format!("wss://{overlay}.fastpubsub.invalid/ws")
}

/// Starts the builder chain for WebSocket transport.
#[cfg(feature = "websocket")]
pub fn create_web_socket(
    overlay_network_name: impl Into<String>,
    at_token: impl Into<String>,
) -> FastPubSubBuilder<WebSocketTransport> {
    FastPubSubBuilder::new(overlay_network_name.into(), at_token.into())
}

/// Starts the builder chain for WebTransport transport.
#[cfg(feature = "webtransport")]
pub fn create_web_transport(
    overlay_network_name: impl Into<String>,
    at_token: impl Into<String>,
) -> FastPubSubBuilder<WebTransport> {
    FastPubSubBuilder::new(overlay_network_name.into(), at_token.into())
}

/// Ready client with selected transport `T`.
///
/// When the client is dropped, the transport is dropped too. There is no separate `close`.
/// Active subscriptions are stored in [`SubscriptionRegistry`], not in [`FastPubSubSession`].
/// The session lives only until `build()`, subscriptions live for the connection.
pub struct FastPubSub<T: Transport> {
    pub(super) transport: T,
    pub(super) overlay: String,
    /// REST base URL of the selected edge, or an overlay stub if it was not set in the builder.
    pub(super) api_base: String,
    /// Subscriptions for resubscribe and for application reads ([`Self::subscriptions`]).
    subscriptions: SubscriptionRegistry,
}

impl<T: Transport> FastPubSub<T> {
    /// Overlay network name from the builder.
    pub fn overlay_network_name(&self) -> &str {
        &self.overlay
    }

    /// REST base URL, either the selected edge or a stub.
    pub fn api_base(&self) -> &str {
        &self.api_base
    }

    /// `GET /ping` on the edge REST API. Requires the `rest` feature.
    #[cfg(feature = "rest")]
    pub async fn ping(&self) -> Result<(), ApiError> {
        api::ping_with_config(&self.api_base, &HttpClientConfig::default()).await
    }

    /// Publishes a payload through the transport with default options.
    ///
    /// Success in contract v0.1 means "accepted locally", not "delivered to listeners".
    pub async fn publish(
        &mut self,
        tenant: &str,
        channel: &str,
        payload: &[u8],
    ) -> Result<(), PublishError> {
        self.publish_with_options(tenant, channel, payload, &PublishOptions::default())
            .await
    }

    /// Publishes a payload through the transport with explicit [`PublishOptions`].
    ///
    /// Success in contract v0.1 means "accepted locally", not "delivered to listeners".
    pub async fn publish_with_options(
        &mut self,
        tenant: &str,
        channel: &str,
        payload: &[u8],
        options: &PublishOptions,
    ) -> Result<(), PublishError> {
        self.transport
            .publish(tenant, channel, Bytes::copy_from_slice(payload), options)
            .await?;
        Ok(())
    }

    /// Set of active subscriptions: tenant, pattern, and queue capacity.
    pub fn subscriptions(&self) -> &std::collections::BTreeSet<SubscriptionSpec> {
        self.subscriptions.subscriptions()
    }

    /// Subscription registry, with the same set as [`Self::subscriptions`].
    pub fn subscription_registry(&self) -> &SubscriptionRegistry {
        &self.subscriptions
    }

    /// Subscribes by pattern. The queue is a direct [`mpsc::Receiver`] from the transport.
    pub async fn subscribe(
        &mut self,
        tenant: &str,
        channel_pattern: &str,
        options: &SubscribeOptions,
    ) -> Result<mpsc::Receiver<SdkMessage>, TransportError> {
        let rx = self
            .transport
            .subscribe(tenant, channel_pattern, options)
            .await?;
        self.subscriptions
            .insert(SubscriptionSpec::new(tenant, channel_pattern, options));
        Ok(rx)
    }

    /// Removes a subscription locally and sends `UNSUB:` on the WebSocket wire.
    pub async fn unsubscribe(
        &mut self,
        tenant: &str,
        channel_pattern: &str,
    ) -> Result<(), TransportError> {
        self.transport.unsubscribe(tenant, channel_pattern).await?;
        self.subscriptions.remove(tenant, channel_pattern);
        Ok(())
    }

    /// Converts this client into a cloneable shared handle.
    ///
    /// One background task owns the original client. Cloned handles send commands
    /// to that task through an internal queue.
    pub fn into_shared(self) -> SharedFastPubSub<T>
    where
        T: 'static,
    {
        SharedFastPubSub::new(self)
    }
}
