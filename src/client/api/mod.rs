// Copyright (c) 2026 FastPubSub Network
// SPDX-License-Identifier: MIT OR Apache-2.0
// Project: https://fastpubsub.com

//! REST control plane: ping, access token (feature `rest`).

mod access_token;
mod expires_at;
#[cfg(feature = "rest")]
mod http_client;
mod ping;

pub use access_token::{
    create_access_token, create_access_token_with_config, AccessTokenBuildError,
    AccessTokenJsonError, CreateAccessTokenBuilder, CreateAccessTokenRequest,
    CreateAccessTokenResponse, TenantGrant, TokenRights,
};
pub use expires_at::{
    expires_at_after_seconds, expires_at_max_ttl, format_expires_at_rfc3339_z,
    parse_expires_at_input, parse_expires_at_input_clamp, utc_now_with_margin, ExpiresAtParseError,
    DEFAULT_NOW_MARGIN_SECS, MAX_TOKEN_TTL_HOURS, MAX_TOKEN_TTL_MARGIN_SECS,
};
pub use ping::{
    ping, ping_fastest, ping_fastest_with_config, ping_many, ping_many_with_config,
    ping_with_config, PingManyResult, PingTiming,
};

/// REST call error.
#[derive(Debug)]
pub enum ApiError {
    /// HTTP client: network, TLS.
    Http(reqwest::Error),
    /// Unexpected response status.
    UnexpectedStatus { status: u16, body: String },
    /// Unexpected response body.
    UnexpectedBody { body: String },
    /// Invalid request body: token builder and similar.
    Build(AccessTokenBuildError),
    /// `get-token` JSON body: parse or fields.
    TokenJson(AccessTokenJsonError),
    /// TLS / company CA.
    Tls(String),
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ApiError::Http(e) => write!(f, "HTTP: {e}"),
            ApiError::UnexpectedStatus { status, body } => {
                write!(f, "unexpected status {status}: {body}")
            }
            ApiError::UnexpectedBody { body } => write!(f, "unexpected body: {body}"),
            ApiError::Build(e) => write!(f, "request build: {e}"),
            ApiError::TokenJson(e) => write!(f, "token JSON: {e}"),
            ApiError::Tls(msg) => write!(f, "TLS: {msg}"),
        }
    }
}

impl std::error::Error for ApiError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ApiError::Http(e) => Some(e),
            ApiError::Build(e) => Some(e),
            ApiError::TokenJson(e) => Some(e),
            _ => None,
        }
    }
}

impl From<reqwest::Error> for ApiError {
    fn from(value: reqwest::Error) -> Self {
        ApiError::Http(value)
    }
}

/// Joins REST base URL and path (`/ping`, `/v1/get-token`, ...).
pub(crate) fn api_url(api_base: &str, path: &str) -> String {
    let base = api_base.trim_end_matches('/');
    let path = path.trim_start_matches('/');
    format!("{base}/{path}")
}

#[cfg(feature = "rest")]
pub(crate) use http_client::build_http_client;
