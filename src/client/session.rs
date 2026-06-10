// Copyright (c) 2026 FastPubSub Network
// SPDX-License-Identifier: MIT OR Apache-2.0
// Project: https://fastpubsub.com

//! Session before transport connection: overlay -> bootstrap -> edge selection -> WS/WT.

#[cfg(feature = "websocket")]
use crate::transport::WebSocketTransport;

#[cfg(feature = "webtransport")]
use crate::transport::WebTransport;

#[cfg(any(feature = "websocket", feature = "webtransport"))]
use super::builder::FastPubSubBuilder;
use super::discovery::{
    default_bootstrap_url, fetch_edge_candidates_with_config, select_fastest_edge_with_config,
    DiscoveryError, EdgeCandidate, SelectedEdge,
};
use super::http_config::HttpClientConfig;

/// Client during edge selection, before WebSocket/WebTransport.
///
/// Order: [`FastPubSubSession::discover_edges`] -> [`FastPubSubSession::select_nearest_edge`]
/// (or [`FastPubSubSession::resolve_edge`]) -> REST AT / [`FastPubSubSession::web_socket`].
pub struct FastPubSubSession {
    overlay: String,
    bootstrap_url: String,
    http_config: HttpClientConfig,
    candidates: Vec<EdgeCandidate>,
    selected: Option<SelectedEdge>,
}

impl FastPubSubSession {
    /// New session for an overlay network.
    pub fn new(overlay: impl Into<String>) -> Self {
        let overlay = overlay.into();
        let bootstrap_url = default_bootstrap_url(&overlay);
        Self {
            overlay,
            bootstrap_url,
            http_config: HttpClientConfig::from_env(),
            candidates: Vec::new(),
            selected: None,
        }
    }

    /// REST settings: Squid proxy, `NO_PROXY`, timeout.
    pub fn http_config(&self) -> &HttpClientConfig {
        &self.http_config
    }

    /// Change REST settings: proxy, timeout.
    pub fn http_config_mut(&mut self) -> &mut HttpClientConfig {
        &mut self.http_config
    }

    /// Squid / company proxy for HTTP and HTTPS.
    pub fn with_proxy(mut self, url: impl Into<String>) -> Self {
        self.http_config = self.http_config.clone().proxy(url);
        self
    }

    /// Proxy user and password (REST + WebSocket CONNECT).
    pub fn with_proxy_auth(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.http_config = self.http_config.clone().proxy_auth(username, password);
        self
    }

    /// Company CA file (`.pem` / `.crt`) for MITM proxy.
    pub fn with_ca_file(mut self, path: impl Into<String>) -> Self {
        self.http_config = self.http_config.clone().add_ca_file(path);
        self
    }

    /// Custom bootstrap URL. Otherwise [`default_bootstrap_url`] is used.
    pub fn with_bootstrap_url(mut self, url: impl Into<String>) -> Self {
        self.bootstrap_url = url.into();
        self
    }

    /// Overlay network name.
    pub fn overlay_network_name(&self) -> &str {
        &self.overlay
    }

    /// Bootstrap URL from the session.
    pub fn bootstrap_url(&self) -> &str {
        &self.bootstrap_url
    }

    /// Candidates after [`Self::discover_edges`].
    pub fn edge_candidates(&self) -> &[EdgeCandidate] {
        &self.candidates
    }

    /// Selected edge after [`Self::select_nearest_edge`].
    pub fn selected_edge(&self) -> Option<&SelectedEdge> {
        self.selected.as_ref()
    }

    /// REST base URL of the selected edge, for `get-token` and ping.
    pub fn api_base(&self) -> Result<&str, DiscoveryError> {
        self.selected
            .as_ref()
            .map(|s| s.candidate.api_base.as_str())
            .ok_or(DiscoveryError::NoEdgeSelected)
    }

    /// Bootstrap: list of nearby edges, from a stub or real API.
    pub async fn discover_edges(&mut self) -> Result<&[EdgeCandidate], DiscoveryError> {
        self.candidates =
            fetch_edge_candidates_with_config(&self.bootstrap_url, &self.http_config).await?;
        Ok(&self.candidates)
    }

    /// Pings all candidates and stores the fastest one.
    pub async fn select_nearest_edge(&mut self) -> Result<&SelectedEdge, DiscoveryError> {
        if self.candidates.is_empty() {
            return Err(DiscoveryError::NoCandidates);
        }
        self.selected =
            Some(select_fastest_edge_with_config(&self.candidates, &self.http_config).await?);
        Ok(self.selected.as_ref().expect("just stored"))
    }

    /// `discover_edges` + `select_nearest_edge`.
    pub async fn resolve_edge(&mut self) -> Result<&SelectedEdge, DiscoveryError> {
        self.discover_edges().await?;
        self.select_nearest_edge().await
    }

    /// WebSocket builder for the selected edge. It needs AT after `create_access_token` on `api_base`.
    #[cfg(feature = "websocket")]
    pub fn web_socket(
        &self,
        at_token: impl Into<String>,
    ) -> Result<FastPubSubBuilder<WebSocketTransport>, DiscoveryError> {
        let edge = self
            .selected
            .as_ref()
            .ok_or(DiscoveryError::NoEdgeSelected)?;
        Ok(
            FastPubSubBuilder::new(self.overlay.clone(), at_token.into())
                .endpoint(edge.candidate.ws_endpoint.clone())
                .api_base(edge.candidate.api_base.clone())
                .http_config(self.http_config.clone()),
        )
    }

    /// WebTransport builder for the selected edge.
    #[cfg(feature = "webtransport")]
    pub fn web_transport(
        &self,
        at_token: impl Into<String>,
    ) -> Result<FastPubSubBuilder<WebTransport>, DiscoveryError> {
        let edge = self
            .selected
            .as_ref()
            .ok_or(DiscoveryError::NoEdgeSelected)?;
        let wt = edge
            .candidate
            .wt_endpoint
            .clone()
            .ok_or(DiscoveryError::NoEdgeSelected)?;
        Ok(
            FastPubSubBuilder::new(self.overlay.clone(), at_token.into())
                .endpoint(wt)
                .api_base(edge.candidate.api_base.clone()),
        )
    }
}

/// Entry point: session by overlay name, without AT and without transport.
pub fn open(overlay: impl Into<String>) -> FastPubSubSession {
    FastPubSubSession::new(overlay)
}
