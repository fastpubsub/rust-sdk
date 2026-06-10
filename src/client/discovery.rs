// Copyright (c) 2026 FastPubSub Network
// SPDX-License-Identifier: MIT OR Apache-2.0
// Project: https://fastpubsub.com

//! Bootstrap edge discovery and nearest server selection by ping.

use std::time::Duration;

use serde::Deserialize;

use crate::client::api::{build_http_client, ping_fastest_with_config, ApiError, PingTiming};
use crate::client::http_config::HttpClientConfig;

const BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(5);

/// Edge candidate from bootstrap (Cloudflare / control plane).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeCandidate {
    /// Node id.
    pub id: String,
    /// REST base URL (`GET /ping`, `/v1/get-token`).
    pub api_base: String,
    /// WebSocket endpoint.
    pub ws_endpoint: String,
    /// WebTransport endpoint, if any.
    pub wt_endpoint: Option<String>,
    /// Region hint from bootstrap.
    pub region: Option<String>,
}

/// Selected edge after ping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectedEdge {
    /// Full candidate data.
    pub candidate: EdgeCandidate,
    /// `GET /ping` time to this edge.
    pub ping: Duration,
}

/// Discovery error.
#[derive(Debug)]
pub enum DiscoveryError {
    /// HTTP / ping.
    Api(ApiError),
    /// Bootstrap returned no candidates.
    NoCandidates,
    /// Edge is not selected yet ([`FastPubSubSession::select_nearest_edge`]).
    NoEdgeSelected,
    /// Candidate from ping was not found in the list (internal error).
    CandidateNotFound,
}

impl std::fmt::Display for DiscoveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DiscoveryError::Api(e) => write!(f, "API: {e}"),
            DiscoveryError::NoCandidates => write!(f, "no edge candidates"),
            DiscoveryError::NoEdgeSelected => write!(f, "edge is not selected, call resolve_edge"),
            DiscoveryError::CandidateNotFound => write!(f, "candidate was not found after ping"),
        }
    }
}

impl std::error::Error for DiscoveryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            DiscoveryError::Api(e) => Some(e),
            _ => None,
        }
    }
}

impl From<ApiError> for DiscoveryError {
    fn from(value: ApiError) -> Self {
        DiscoveryError::Api(value)
    }
}

/// Bootstrap API URL.
pub fn default_bootstrap_url(overlay: &str) -> String {
    format!("https://{overlay}.fastpubsub.workers.dev/")
}

/// Bootstrap request: list of geographically close edges.
pub async fn fetch_edge_candidates(
    _overlay: &str,
    bootstrap_url: &str,
) -> Result<Vec<EdgeCandidate>, DiscoveryError> {
    fetch_edge_candidates_with_config(bootstrap_url, &HttpClientConfig::default()).await
}

/// Bootstrap request with explicit HTTP settings.
pub async fn fetch_edge_candidates_with_config(
    bootstrap_url: &str,
    config: &HttpClientConfig,
) -> Result<Vec<EdgeCandidate>, DiscoveryError> {
    let client = build_http_client(config, BOOTSTRAP_TIMEOUT)?;
    let response = client
        .get(bootstrap_url)
        .send()
        .await
        .map_err(ApiError::from)?;
    let status = response.status();
    let body = response.text().await.map_err(ApiError::from)?;
    if !status.is_success() {
        return Err(ApiError::UnexpectedStatus {
            status: status.as_u16(),
            body,
        }
        .into());
    }

    let payload: BootstrapResponse =
        serde_json::from_str(&body).map_err(|_| ApiError::UnexpectedBody { body })?;
    let candidates = payload
        .edges
        .into_iter()
        .map(|edge| edge.into_candidate())
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        return Err(DiscoveryError::NoCandidates);
    }
    Ok(candidates)
}

#[derive(Debug, Deserialize)]
struct BootstrapResponse {
    edges: Vec<BootstrapEdge>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum BootstrapEdge {
    Host(String),
    Record { host: String },
}

impl BootstrapEdge {
    fn into_candidate(self) -> EdgeCandidate {
        let host = match self {
            BootstrapEdge::Host(host) => host,
            BootstrapEdge::Record { host } => host,
        };
        edge_candidate_from_host(&host)
    }
}

fn edge_candidate_from_host(host: &str) -> EdgeCandidate {
    let host = host.trim().trim_end_matches('/');
    let id = host
        .split('.')
        .next()
        .filter(|value| !value.is_empty())
        .unwrap_or(host);
    EdgeCandidate {
        id: format!("edge-{id}"),
        api_base: format!("https://{host}"),
        ws_endpoint: format!("wss://{host}/ws"),
        wt_endpoint: Some(format!("https://{host}/wt")),
        region: Some(id.to_uppercase()),
    }
}

/// Pings all candidates in parallel and returns the fastest by RTT.
pub async fn select_fastest_edge(
    candidates: &[EdgeCandidate],
) -> Result<SelectedEdge, DiscoveryError> {
    select_fastest_edge_with_config(candidates, &HttpClientConfig::default()).await
}

/// Same as [`select_fastest_edge`], but ping uses proxy from `config`.
pub async fn select_fastest_edge_with_config(
    candidates: &[EdgeCandidate],
    config: &HttpClientConfig,
) -> Result<SelectedEdge, DiscoveryError> {
    if candidates.is_empty() {
        return Err(DiscoveryError::NoCandidates);
    }
    let bases: Vec<&str> = candidates.iter().map(|c| c.api_base.as_str()).collect();
    let fastest: PingTiming = ping_fastest_with_config(&bases, config).await?;
    let candidate = candidates
        .iter()
        .find(|c| c.api_base == fastest.api_base)
        .cloned()
        .ok_or(DiscoveryError::CandidateNotFound)?;
    Ok(SelectedEdge {
        candidate,
        ping: fastest.duration,
    })
}

#[cfg(test)]
mod tests {
    use super::{default_bootstrap_url, edge_candidate_from_host, BootstrapResponse};

    #[test]
    fn test_default_bootstrap_url() {
        assert_eq!(
            default_bootstrap_url("globaltest"),
            "https://globaltest.fastpubsub.workers.dev/"
        );
    }

    #[test]
    fn test_worker_edges_as_strings() {
        let raw = r#"{
            "source": "memory-fresh",
            "version": 1,
            "updated_at": 1780663294,
            "edges": ["mon-01.globaltest.fastpubsub.com", "fra-01.globaltest.fastpubsub.com"]
        }"#;
        let payload: BootstrapResponse = serde_json::from_str(raw).expect("bootstrap json");
        let candidates = payload
            .edges
            .into_iter()
            .map(|edge| edge.into_candidate())
            .collect::<Vec<_>>();

        assert_eq!(candidates.len(), 2);
        assert_eq!(candidates[0].id, "edge-mon-01");
        assert_eq!(
            candidates[0].api_base,
            "https://mon-01.globaltest.fastpubsub.com"
        );
        assert_eq!(
            candidates[0].ws_endpoint,
            "wss://mon-01.globaltest.fastpubsub.com/ws"
        );
    }

    #[test]
    fn test_worker_edges_as_records() {
        let raw = r#"{
            "version": 1,
            "updated_at": 1780663294,
            "edges": [{"host": "fra-01.globaltest.fastpubsub.com", "lat": 50.1109, "lon": 8.6821, "priority": 100}]
        }"#;
        let payload: BootstrapResponse = serde_json::from_str(raw).expect("bootstrap json");
        let candidate = payload
            .edges
            .into_iter()
            .next()
            .expect("edge")
            .into_candidate();

        assert_eq!(candidate.id, "edge-fra-01");
        assert_eq!(
            candidate.wt_endpoint,
            Some("https://fra-01.globaltest.fastpubsub.com/wt".to_string())
        );
    }

    #[test]
    fn test_edge_candidate_from_host() {
        let candidate = edge_candidate_from_host("fra-01.globaltest.fastpubsub.com");
        assert_eq!(candidate.region, Some("FRA-01".to_string()));
    }
}
