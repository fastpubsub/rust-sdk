// Copyright (c) 2026 FastPubSub Network
// SPDX-License-Identifier: MIT OR Apache-2.0
// Project: https://fastpubsub.com

//! Fluent client builder: filters and [`FastPubSubBuilder::build`].
//!
//! Type parameter `T` is the concrete transport ([`crate::transport::WebSocketTransport`] or [`crate::transport::WebTransport`]).
//! EN: Submodule of `client`; `build` wires overlay → endpoint and returns [`super::FastPubSub`].

use std::marker::PhantomData;

#[cfg(feature = "websocket")]
use tokio::sync::mpsc;

use crate::filters::{FilterRegistration, FilterTimerMode, FilterTrait};
use crate::metadata::MetaMode;
#[cfg(feature = "websocket")]
use crate::transport::WebSocketEvent;
use crate::transport::{Transport, TransportConnectParams};

use super::http_config::HttpClientConfig;
use super::{overlay_network_endpoint, BuildError, FastPubSub};

/// Chain: network name, token, filters, then [`FastPubSubBuilder::build`].
pub struct FastPubSubBuilder<T: Transport + Default + 'static> {
    overlay: String,
    at_token: String,
    /// Explicit WS/WT URL after discovery. Otherwise a stub is built from the overlay name.
    endpoint: Option<String>,
    /// REST API of the selected edge. Otherwise a stub is built from the overlay name.
    api_base: Option<String>,
    filters: Vec<FilterRegistration>,
    filter_timer_mode: FilterTimerMode,
    meta_mode: MetaMode,
    #[cfg(feature = "websocket")]
    on_websocket_event: Option<mpsc::Sender<WebSocketEvent>>,
    http_config: Option<HttpClientConfig>,
    _t: PhantomData<T>,
}

impl<T: Transport + Default + 'static> FastPubSubBuilder<T> {
    /// Creates a builder, usually via [`super::create_web_socket`] or [`super::create_web_transport`].
    pub fn new(overlay: String, at_token: String) -> Self {
        Self {
            overlay,
            at_token,
            endpoint: None,
            api_base: None,
            filters: Vec::new(),
            filter_timer_mode: FilterTimerMode::default(),
            meta_mode: MetaMode::default(),
            #[cfg(feature = "websocket")]
            on_websocket_event: None,
            http_config: None,
            _t: PhantomData,
        }
    }

    /// Proxy and user/password for REST and WebSocket (Squid).
    pub fn http_config(mut self, config: HttpClientConfig) -> Self {
        self.http_config = Some(config);
        self
    }

    /// Transport endpoint from bootstrap or the selected edge.
    pub fn endpoint(mut self, url: impl Into<String>) -> Self {
        self.endpoint = Some(url.into());
        self
    }

    /// REST base URL of the selected edge.
    pub fn api_base(mut self, url: impl Into<String>) -> Self {
        self.api_base = Some(url.into());
        self
    }

    /// Adds a route filter for an exact tenant and static channel prefix.
    pub fn add_filter<F>(
        mut self,
        tenant: impl Into<String>,
        prefix: impl Into<String>,
        filter: F,
    ) -> Self
    where
        F: FilterTrait + 'static,
    {
        self.filters
            .push(FilterRegistration::new(tenant, prefix, filter));
        self
    }

    /// Sets the timer mode for background filters.
    pub fn filter_timer_mode(mut self, mode: FilterTimerMode) -> Self {
        self.filter_timer_mode = mode;
        self
    }

    /// Sets metadata mode for inbound messages.
    pub fn meta_mode(mut self, mode: MetaMode) -> Self {
        self.meta_mode = mode;
        self
    }

    /// WebSocket events: errors, reconnect, subscription confirmations, and dedup.
    #[cfg(feature = "websocket")]
    pub fn on_websocket_event(mut self, tx: mpsc::Sender<WebSocketEvent>) -> Self {
        self.on_websocket_event = Some(tx);
        self
    }

    /// Connects the transport to the endpoint and returns a client (Tokio).
    pub async fn build(self) -> Result<FastPubSub<T>, BuildError> {
        let endpoint = self
            .endpoint
            .unwrap_or_else(|| overlay_network_endpoint(&self.overlay));
        let api_base = self
            .api_base
            .unwrap_or_else(|| default_api_base(&self.overlay));
        let mut transport = T::default();
        let params = TransportConnectParams {
            filters: self.filters,
            filter_timer_mode: self.filter_timer_mode,
            meta_mode: self.meta_mode,
            access_token: self.at_token,
            #[cfg(feature = "websocket")]
            on_websocket_event: self.on_websocket_event,
            #[cfg(any(feature = "websocket", feature = "rest"))]
            http_config: self.http_config,
        };
        transport.connect(&endpoint, params).await?;
        Ok(FastPubSub {
            transport,
            overlay: self.overlay,
            api_base,
            subscriptions: super::subscriptions::SubscriptionRegistry::new(),
        })
    }
}

/// REST base URL by overlay. With `rest` it comes from the API module, otherwise it is a stub.
fn default_api_base(overlay: &str) -> String {
    format!("https://{overlay}.fastpubsub.com")
}
