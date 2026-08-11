// Copyright (c) 2026 FastPubSub Network
// SPDX-License-Identifier: MIT OR Apache-2.0
// Project: https://fastpubsub.com

//! Create, refresh, and revoke access tokens via edge REST (`AT_...`).
//!
//! - `POST /v1/get-token` — create
//! - `PUT /v1/refresh-token` — extend TTL (`expires_at` only)
//! - `DELETE /v1/revoke-token` — revoke
//!
//! All three calls require a master token in `Authorization: Bearer ...`.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::expires_at::{
    expires_at_after_seconds_with_margin, expires_at_max_ttl_with_margin,
    format_expires_at_rfc3339_z, parse_expires_at_input_clamp_with_margin, ExpiresAtParseError,
    DEFAULT_NOW_MARGIN_SECS,
};
use super::{api_url, build_http_client, ApiError};

impl From<AccessTokenJsonError> for ApiError {
    fn from(value: AccessTokenJsonError) -> Self {
        ApiError::TokenJson(value)
    }
}
use crate::client::http_config::HttpClientConfig;

/// Timeout for token creation request.
const TOKEN_TIMEOUT: Duration = Duration::from_secs(5);

/// Rights for one or more tenants (`tenant_grants` item in JSON).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TenantGrant {
    /// Tenant ids.
    pub tenant_ids: Vec<String>,
    /// Channels for publish (patterns).
    pub allow_channels_pub: Vec<String>,
    /// Channels for subscribe (patterns).
    pub allow_channels_sub: Vec<String>,
}

impl TenantGrant {
    /// New rights record for tenant list.
    pub fn new(tenant_ids: Vec<String>) -> Self {
        Self {
            tenant_ids,
            allow_channels_pub: Vec::new(),
            allow_channels_sub: Vec::new(),
        }
    }

    /// Allowed publish channels.
    pub fn allow_pub(mut self, channels: Vec<String>) -> Self {
        self.allow_channels_pub = channels;
        self
    }

    /// Allowed subscribe channels.
    pub fn allow_sub(mut self, channels: Vec<String>) -> Self {
        self.allow_channels_sub = channels;
        self
    }
}

/// `right` object in `POST /v1/get-token` body.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TokenRights {
    /// Rights by tenants.
    #[serde(default)]
    pub tenant_grants: Vec<TenantGrant>,
    /// Allowed IP masks (CIDR).
    #[serde(default)]
    pub allow_ip_masks: Vec<String>,
    /// Allowed regions.
    #[serde(default)]
    pub allow_regions: Vec<String>,
    /// Allowed WebSocket origins.
    #[serde(default)]
    pub allowed_ws_origin: Vec<String>,
    /// Expiration time, ISO 8601, no more than 24 hours from server "now".
    pub expires_at: String,
}

/// Request body for `POST /v1/get-token`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CreateAccessTokenRequest {
    /// Access rights (`right` in JSON).
    #[serde(rename = "right")]
    pub right: TokenRights,
    /// Who creates the token.
    pub created_by: String,
    /// Optional description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// Response from `POST /v1/get-token`.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct CreateAccessTokenResponse {
    /// Token `AT_{id}_{secret}`.
    pub token: String,
}

impl CreateAccessTokenRequest {
    /// Parses JSON body for `POST /v1/get-token`, as in Swagger / `token-request.json`.
    #[cfg(feature = "access_token_json")]
    pub fn from_json_str(json: &str) -> Result<Self, AccessTokenJsonError> {
        let request: Self =
            serde_json::from_str(json).map_err(|e| AccessTokenJsonError::Parse {
                message: e.to_string(),
            })?;
        request.validate().map_err(AccessTokenJsonError::Validate)?;
        Ok(request)
    }

    /// Serializes the request body to JSON.
    #[cfg(feature = "access_token_json")]
    pub fn to_json_string(&self) -> Result<String, AccessTokenJsonError> {
        serde_json::to_string(self).map_err(|e| AccessTokenJsonError::Parse {
            message: e.to_string(),
        })
    }

    /// Validates fields after the builder or JSON parsing.
    pub fn validate(&self) -> Result<(), AccessTokenBuildError> {
        if self.created_by.trim().is_empty() {
            return Err(AccessTokenBuildError::MissingCreatedBy);
        }
        if self.right.expires_at.trim().is_empty() {
            return Err(AccessTokenBuildError::MissingExpiresAt);
        }
        if self.right.tenant_grants.is_empty() {
            return Err(AccessTokenBuildError::NoTenantGrants);
        }
        if chrono::DateTime::parse_from_rfc3339(self.right.expires_at.trim()).is_err() {
            return Err(AccessTokenBuildError::InvalidExpiresAt);
        }
        Ok(())
    }
}

/// Error while parsing token request JSON body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccessTokenJsonError {
    /// Invalid JSON.
    Parse { message: String },
    /// JSON is valid, but fields failed [`CreateAccessTokenRequest::validate`].
    Validate(AccessTokenBuildError),
}

impl std::fmt::Display for AccessTokenJsonError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AccessTokenJsonError::Parse { message } => {
                write!(f, "JSON error: {message}")
            }
            AccessTokenJsonError::Validate(e) => write!(f, "request fields: {e}"),
        }
    }
}

impl std::error::Error for AccessTokenJsonError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            AccessTokenJsonError::Validate(e) => Some(e),
            _ => None,
        }
    }
}

/// Error while building JSON request in the builder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccessTokenBuildError {
    /// `created_by` is missing.
    MissingCreatedBy,
    /// `expires_at` is missing.
    MissingExpiresAt,
    /// `tenant_grants` is empty.
    NoTenantGrants,
    /// `expires_at` is not RFC3339.
    InvalidExpiresAt,
}

impl std::fmt::Display for AccessTokenBuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AccessTokenBuildError::MissingCreatedBy => {
                write!(f, "created_by is missing")
            }
            AccessTokenBuildError::MissingExpiresAt => {
                write!(f, "expires_at is missing")
            }
            AccessTokenBuildError::NoTenantGrants => {
                write!(f, "at least one tenant_grant is required")
            }
            AccessTokenBuildError::InvalidExpiresAt => {
                write!(
                    f,
                    "expires_at must be RFC3339, for example 2026-05-16T12:00:00Z"
                )
            }
        }
    }
}

impl std::error::Error for AccessTokenBuildError {}

/// Step-by-step builder for `POST /v1/get-token` body.
#[derive(Debug)]
pub struct CreateAccessTokenBuilder {
    created_by: Option<String>,
    description: Option<String>,
    expires_at: Option<String>,
    tenant_grants: Vec<TenantGrant>,
    allow_ip_masks: Vec<String>,
    allow_regions: Vec<String>,
    allowed_ws_origin: Vec<String>,
    /// Shift of base "now" for relative expiration, in seconds. Default is 1.
    now_margin_secs: i64,
}

impl Default for CreateAccessTokenBuilder {
    fn default() -> Self {
        Self {
            created_by: None,
            description: None,
            expires_at: None,
            tenant_grants: Vec::new(),
            allow_ip_masks: Vec::new(),
            allow_regions: Vec::new(),
            allowed_ws_origin: Vec::new(),
            now_margin_secs: DEFAULT_NOW_MARGIN_SECS,
        }
    }
}

impl CreateAccessTokenBuilder {
    /// Empty builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Shift of base "now" for any relative expiration (`+30m`, `+24h`, `max`, `expires_in`, ...).
    ///
    /// Not used for ready ISO strings in [`Self::expires_at`]. Default is [`DEFAULT_NOW_MARGIN_SECS`] (1 s).
    /// `0` means use `Utc::now()` without shift.
    pub fn now_margin_secs(mut self, secs: i64) -> Self {
        self.now_margin_secs = secs.max(0);
        self
    }

    /// No shift from `now()`, same as `now_margin_secs(0)`.
    pub fn without_now_margin(mut self) -> Self {
        self.now_margin_secs = 0;
        self
    }

    /// Who creates the token. Required.
    pub fn created_by(mut self, value: impl Into<String>) -> Self {
        self.created_by = Some(value.into());
        self
    }

    /// Token description.
    pub fn description(mut self, value: impl Into<String>) -> Self {
        self.description = Some(value.into());
        self
    }

    /// Expiration time, ISO 8601 UTC, ready string for API.
    pub fn expires_at(mut self, value: impl Into<String>) -> Self {
        self.expires_at = Some(value.into());
        self
    }

    /// Expiration from string: RFC3339, `YYYY-MM-DD`, `+30m`, `+24h`, `max`, and so on.
    ///
    /// Relative time is based on `(now - now_margin_secs)`. See [`Self::now_margin_secs`].
    pub fn expires_at_input(mut self, input: impl AsRef<str>) -> Result<Self, ExpiresAtParseError> {
        self.expires_at = Some(parse_expires_at_input_clamp_with_margin(
            input.as_ref(),
            self.now_margin_secs,
        )?);
        Ok(self)
    }

    /// Expiration = (now - [`Self::now_margin_secs`]) + interval, no more than 24 hours on server.
    pub fn expires_in(mut self, delta: Duration) -> Self {
        let secs = delta.as_secs().min(i64::MAX as u64) as i64;
        self.expires_at = Some(format_expires_at_rfc3339_z(
            expires_at_after_seconds_with_margin(secs, self.now_margin_secs),
        ));
        self
    }

    /// Maximum TTL: 24 hours from base "now", with [`Self::now_margin_secs`].
    pub fn expires_max_ttl(mut self) -> Self {
        self.expires_at = Some(format_expires_at_rfc3339_z(expires_at_max_ttl_with_margin(
            self.now_margin_secs,
        )));
        self
    }

    /// One more record in `tenant_grants`.
    pub fn tenant_grant(mut self, grant: TenantGrant) -> Self {
        self.tenant_grants.push(grant);
        self
    }

    /// Allowed IP mask (CIDR).
    pub fn allow_ip_mask(mut self, mask: impl Into<String>) -> Self {
        self.allow_ip_masks.push(mask.into());
        self
    }

    /// Allowed region.
    pub fn allow_region(mut self, region: impl Into<String>) -> Self {
        self.allow_regions.push(region.into());
        self
    }

    /// Allowed WebSocket Origin.
    pub fn allowed_ws_origin(mut self, origin: impl Into<String>) -> Self {
        self.allowed_ws_origin.push(origin.into());
        self
    }

    /// Builds the JSON request body.
    pub fn build(self) -> Result<CreateAccessTokenRequest, AccessTokenBuildError> {
        let created_by = self
            .created_by
            .ok_or(AccessTokenBuildError::MissingCreatedBy)?;
        let expires_at = self
            .expires_at
            .ok_or(AccessTokenBuildError::MissingExpiresAt)?;
        if self.tenant_grants.is_empty() {
            return Err(AccessTokenBuildError::NoTenantGrants);
        }
        let request = CreateAccessTokenRequest {
            right: TokenRights {
                tenant_grants: self.tenant_grants,
                allow_ip_masks: self.allow_ip_masks,
                allow_regions: self.allow_regions,
                allowed_ws_origin: self.allowed_ws_origin,
                expires_at,
            },
            created_by,
            description: self.description,
        };
        request.validate()?;
        Ok(request)
    }

    /// `POST /v1/get-token` with ready JSON body (`right`, `created_by`, ...).
    #[cfg(feature = "access_token_json")]
    pub async fn create_from_json_with_config(
        json: &str,
        api_base: &str,
        master_token: &str,
        config: &HttpClientConfig,
    ) -> Result<String, ApiError> {
        let request = CreateAccessTokenRequest::from_json_str(json)?;
        create_access_token_with_config(api_base, master_token, &request, config).await
    }

    /// Same as [`Self::create_from_json_with_config`] without proxy.
    #[cfg(feature = "access_token_json")]
    pub async fn create_from_json(
        json: &str,
        api_base: &str,
        master_token: &str,
    ) -> Result<String, ApiError> {
        Self::create_from_json_with_config(
            json,
            api_base,
            master_token,
            &HttpClientConfig::default(),
        )
        .await
    }

    /// Builds the body and calls [`create_access_token`].
    pub async fn create(self, api_base: &str, master_token: &str) -> Result<String, ApiError> {
        self.create_with_config(api_base, master_token, &HttpClientConfig::default())
            .await
    }

    /// Same as [`Self::create`], with proxy (Squid) and timeout.
    pub async fn create_with_config(
        self,
        api_base: &str,
        master_token: &str,
        config: &HttpClientConfig,
    ) -> Result<String, ApiError> {
        let request = self.build().map_err(ApiError::Build)?;
        create_access_token_with_config(api_base, master_token, &request, config).await
    }
}

/// `POST /v1/get-token` creates an access token.
///
/// * `api_base` - REST base URL, for example `https://api.example.com`
/// * `master_token` - master secret without `Bearer` prefix
/// * `request` - body from [`CreateAccessTokenBuilder::build`]
pub async fn create_access_token(
    api_base: &str,
    master_token: &str,
    request: &CreateAccessTokenRequest,
) -> Result<String, ApiError> {
    create_access_token_with_config(
        api_base,
        master_token,
        request,
        &HttpClientConfig::default(),
    )
    .await
}

/// `POST /v1/get-token` with HTTP client settings.
pub async fn create_access_token_with_config(
    api_base: &str,
    master_token: &str,
    request: &CreateAccessTokenRequest,
    config: &HttpClientConfig,
) -> Result<String, ApiError> {
    let url = api_url(api_base, "v1/get-token");
    let client = build_http_client(config, TOKEN_TIMEOUT)?;
    let response = client
        .post(&url)
        .header("Authorization", format!("Bearer {master_token}"))
        .json(request)
        .send()
        .await?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(ApiError::UnexpectedStatus {
            status: status.as_u16(),
            body,
        });
    }
    let parsed: CreateAccessTokenResponse = response.json().await?;
    if parsed.token.is_empty() {
        return Err(ApiError::UnexpectedBody {
            body: "empty token".to_string(),
        });
    }
    Ok(parsed.token)
}

/// Error while parsing an access token string (`AT_...`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccessTokenParseError {
    /// String is not `AT_{token_id}_{secret}`.
    InvalidFormat,
}

impl std::fmt::Display for AccessTokenParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AccessTokenParseError::InvalidFormat => {
                write!(f, "expected format AT_{{token_id}}_{{secret}}")
            }
        }
    }
}

impl std::error::Error for AccessTokenParseError {}

/// Parses a full AT into `(token_id, secret)`.
///
/// For myself: same layout as edge `parse_token` — prefix `AT_`, first `_` splits id/secret.
pub fn parse_access_token(full_token: &str) -> Result<(&str, &str), AccessTokenParseError> {
    let rest = full_token
        .strip_prefix("AT_")
        .ok_or(AccessTokenParseError::InvalidFormat)?;
    let underscore_pos = rest
        .find('_')
        .ok_or(AccessTokenParseError::InvalidFormat)?;
    let token_id = &rest[..underscore_pos];
    let secret = &rest[underscore_pos + 1..];
    if token_id.is_empty() || secret.is_empty() {
        return Err(AccessTokenParseError::InvalidFormat);
    }
    Ok((token_id, secret))
}

/// Extracts `token_id` from a full AT (needed for `PUT /v1/refresh-token`).
pub fn access_token_id(full_token: &str) -> Result<&str, AccessTokenParseError> {
    parse_access_token(full_token).map(|(id, _)| id)
}

/// Body for `PUT /v1/refresh-token`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RefreshAccessTokenRequest {
    /// Token id only (no secret), hex.
    pub token_id: String,
    /// New `expires_at` (RFC3339 UTC). Max 24 hours from server "now".
    pub expires_at: String,
}

/// Response from `PUT /v1/refresh-token`.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct RefreshAccessTokenResponse {
    /// Result message from the API.
    pub message: String,
    /// New expiration time.
    pub new_expires_at: String,
}

/// Body for `DELETE /v1/revoke-token`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RevokeAccessTokenRequest {
    /// Full AT `AT_{id}_{secret}`.
    pub token: String,
}

/// Response from `DELETE /v1/revoke-token`.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct RevokeAccessTokenResponse {
    /// Result message from the API.
    pub message: String,
}

/// `PUT /v1/refresh-token` — extends TTL of an existing AT (master token required).
///
/// * `token_id` — id from `AT_{token_id}_{secret}` (see [`access_token_id`])
/// * `expires_at` — ready RFC3339 string (see [`parse_expires_at_input_clamp`])
///
/// For myself: does not change IP masks or ACL; expired tokens cannot be refreshed.
pub async fn refresh_access_token(
    api_base: &str,
    master_token: &str,
    token_id: &str,
    expires_at: &str,
) -> Result<RefreshAccessTokenResponse, ApiError> {
    refresh_access_token_with_config(
        api_base,
        master_token,
        token_id,
        expires_at,
        &HttpClientConfig::default(),
    )
    .await
}

/// Same as [`refresh_access_token`], with HTTP client settings.
pub async fn refresh_access_token_with_config(
    api_base: &str,
    master_token: &str,
    token_id: &str,
    expires_at: &str,
    config: &HttpClientConfig,
) -> Result<RefreshAccessTokenResponse, ApiError> {
    if token_id.trim().is_empty() {
        return Err(ApiError::UnexpectedBody {
            body: "token_id is empty".to_string(),
        });
    }
    if expires_at.trim().is_empty() {
        return Err(ApiError::UnexpectedBody {
            body: "expires_at is empty".to_string(),
        });
    }
    let url = api_url(api_base, "v1/refresh-token");
    let client = build_http_client(config, TOKEN_TIMEOUT)?;
    let request = RefreshAccessTokenRequest {
        token_id: token_id.trim().to_string(),
        expires_at: expires_at.trim().to_string(),
    };
    let response = client
        .put(&url)
        .header("Authorization", format!("Bearer {master_token}"))
        .json(&request)
        .send()
        .await?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(ApiError::UnexpectedStatus {
            status: status.as_u16(),
            body,
        });
    }
    let parsed: RefreshAccessTokenResponse = response.json().await?;
    if parsed.new_expires_at.is_empty() {
        return Err(ApiError::UnexpectedBody {
            body: "empty new_expires_at".to_string(),
        });
    }
    Ok(parsed)
}

/// Extends TTL from a full AT: extracts `token_id` and calls refresh.
pub async fn refresh_access_token_from_at(
    api_base: &str,
    master_token: &str,
    full_token: &str,
    expires_at: &str,
) -> Result<RefreshAccessTokenResponse, ApiError> {
    refresh_access_token_from_at_with_config(
        api_base,
        master_token,
        full_token,
        expires_at,
        &HttpClientConfig::default(),
    )
    .await
}

/// Same as [`refresh_access_token_from_at`], with HTTP client settings.
pub async fn refresh_access_token_from_at_with_config(
    api_base: &str,
    master_token: &str,
    full_token: &str,
    expires_at: &str,
    config: &HttpClientConfig,
) -> Result<RefreshAccessTokenResponse, ApiError> {
    let token_id = access_token_id(full_token).map_err(|e| ApiError::UnexpectedBody {
        body: e.to_string(),
    })?;
    refresh_access_token_with_config(api_base, master_token, token_id, expires_at, config).await
}

/// `DELETE /v1/revoke-token` — revokes an AT (master token required).
///
/// * `full_token` — full `AT_{id}_{secret}`
pub async fn revoke_access_token(
    api_base: &str,
    master_token: &str,
    full_token: &str,
) -> Result<RevokeAccessTokenResponse, ApiError> {
    revoke_access_token_with_config(
        api_base,
        master_token,
        full_token,
        &HttpClientConfig::default(),
    )
    .await
}

/// Same as [`revoke_access_token`], with HTTP client settings.
pub async fn revoke_access_token_with_config(
    api_base: &str,
    master_token: &str,
    full_token: &str,
    config: &HttpClientConfig,
) -> Result<RevokeAccessTokenResponse, ApiError> {
    if full_token.trim().is_empty() {
        return Err(ApiError::UnexpectedBody {
            body: "token is empty".to_string(),
        });
    }
    let url = api_url(api_base, "v1/revoke-token");
    let client = build_http_client(config, TOKEN_TIMEOUT)?;
    let request = RevokeAccessTokenRequest {
        token: full_token.trim().to_string(),
    };
    let response = client
        .delete(&url)
        .header("Authorization", format!("Bearer {master_token}"))
        .json(&request)
        .send()
        .await?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(ApiError::UnexpectedStatus {
            status: status.as_u16(),
            body,
        });
    }
    let parsed: RevokeAccessTokenResponse = response.json().await?;
    Ok(parsed)
}

#[cfg(test)]
mod parse_tests {
    use super::*;

    #[test]
    fn parse_access_token_ok() {
        let (id, secret) =
            parse_access_token("AT_aabbccddeeff00112233445566778899_11223344556677889900aabbccddeeff")
                .expect("ok");
        assert_eq!(id, "aabbccddeeff00112233445566778899");
        assert_eq!(secret, "11223344556677889900aabbccddeeff");
        assert_eq!(
            access_token_id("AT_aabbccddeeff00112233445566778899_11223344556677889900aabbccddeeff")
                .expect("id"),
            "aabbccddeeff00112233445566778899"
        );
    }

    #[test]
    fn parse_access_token_rejects_bad() {
        assert!(parse_access_token("MT_x_y").is_err());
        assert!(parse_access_token("AT_onlyid").is_err());
        assert!(parse_access_token("AT__secret").is_err());
        assert!(parse_access_token("AT_id_").is_err());
    }
}

#[cfg(all(test, feature = "access_token_json"))]
mod tests {
    use super::*;

    const SAMPLE_JSON: &str = r#"{
        "right": {
            "tenant_grants": [{
                "tenant_ids": ["tenant_1"],
                "allow_channels_pub": ["public.#"],
                "allow_channels_sub": ["public.#"]
            }],
            "allow_ip_masks": [],
            "allow_regions": [],
            "allowed_ws_origin": [],
            "expires_at": "2026-05-16T10:00:00Z"
        },
        "created_by": "json-import",
        "description": "from test"
    }"#;

    #[test]
    fn request_from_json_str() {
        let req = CreateAccessTokenRequest::from_json_str(SAMPLE_JSON).expect("ok");
        assert_eq!(req.created_by, "json-import");
        assert_eq!(req.right.tenant_grants.len(), 1);
    }

    #[test]
    fn request_to_json_roundtrip() {
        let req = CreateAccessTokenRequest::from_json_str(SAMPLE_JSON).expect("ok");
        let again = CreateAccessTokenRequest::from_json_str(&req.to_json_string().expect("json"))
            .expect("roundtrip");
        assert_eq!(req, again);
    }
}
