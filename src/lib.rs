// Copyright (c) 2026 FastPubSub Network
// SPDX-License-Identifier: MIT OR Apache-2.0
// Project: https://fastpubsub.com

//! FastPubSubNetwork SDK draft for contract v0.1.
//!
//! Modules: [`transport`], [`filters`], [`client`] (builder and `publish`), [`helpers`].
//! The async layer is built for the **Tokio** runtime (`tokio` dependency, common entry point is `#[tokio::main]`).
//!
//! ## Features
//!
//! - `rest` (**enabled by default**) - discovery, `get-token`, ping, `reqwest`.
//! - `websocket` - WebSocket transport, `tokio-tungstenite`.
//! - `webtransport` - WebTransport (stub for now).
//!
//! Transport only without REST: `default-features = false`, `features = ["websocket"]`.
//! REST only: `default-features = false`, `features = ["rest"]`.

pub mod client;
pub mod filters;
pub mod helpers;
pub mod metadata;
pub mod transport;

pub use client::{
    overlay_network_endpoint, BuildError, FastPubSubBuilder, HttpClientConfig, HttpConfigError,
    ParsedProxy, PublishError,
};
pub use helpers::{
    CleanupPolicy, LatestMessageReceiver, LatestStateSync, LatestStateSyncOptions, StateSyncError,
};
pub use metadata::{
    MessageMeta, MetaDecodeError, MetaEncodeError, MetaIter, MetaMode, MetaRecordView, MetaWriter,
    SdkMessage, META_COMPRESSION_DEFLATE_RAW, META_ENCRYPTION_AES256_GCM,
    META_ENCRYPTION_CHACHA20_POLY1305, META_RECORD_ACK, META_RECORD_CHANNEL_BATCH,
    META_RECORD_COMPRESSION, META_RECORD_DEDUP, META_RECORD_DELTA, META_RECORD_ENCRYPTION,
    META_RECORD_FRAGMENTATION, META_RECORD_MESSAGE_ID, META_RECORD_PRIORITY, META_RECORD_TIMESTAMP,
    META_RECORD_TRACE, META_VERSION,
};

#[cfg(feature = "rest")]
pub use client::{
    create_access_token, create_access_token_with_config, default_bootstrap_url,
    expires_at_after_seconds, expires_at_max_ttl, format_expires_at_rfc3339_z, open,
    parse_expires_at_input, parse_expires_at_input_clamp, ping, ping_fastest,
    ping_fastest_with_config, ping_many, ping_with_config, utc_now_with_margin,
    AccessTokenBuildError, AccessTokenJsonError, ApiError, CreateAccessTokenBuilder,
    CreateAccessTokenRequest, CreateAccessTokenResponse, DiscoveryError, EdgeCandidate,
    ExpiresAtParseError, FastPubSubSession, PingManyResult, PingTiming, SelectedEdge, TenantGrant,
    TokenRights, DEFAULT_NOW_MARGIN_SECS, MAX_TOKEN_TTL_HOURS, MAX_TOKEN_TTL_MARGIN_SECS,
};

#[cfg(any(feature = "websocket", feature = "webtransport"))]
pub use client::{FastPubSub, SharedFastPubSub, SubscriptionRegistry, SubscriptionSpec};

#[cfg(feature = "websocket")]
pub use transport::{
    format_ok_subscribe, format_ok_unsubscribe, parse_sub_ack_line, SubWireAck, WebSocketError,
    WebSocketEvent,
};

#[cfg(feature = "websocket")]
pub use client::create_web_socket;

#[cfg(feature = "webtransport")]
pub use client::create_web_transport;
