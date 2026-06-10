// Copyright (c) 2026 FastPubSub Network
// SPDX-License-Identifier: MIT OR Apache-2.0
// Project: https://fastpubsub.com

//! Proxy, timeout, and company CA (REST and WebSocket CONNECT).

use std::fmt;
use std::time::Duration;

/// Error while parsing proxy or TLS settings, not tied to REST.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpConfigError(pub String);

impl fmt::Display for HttpConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for HttpConfigError {}

/// HTTP(S) proxy and trusted CA settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpClientConfig {
    /// Request timeout. If `None`, caller sets it (ping / token).
    pub timeout: Option<Duration>,
    /// Proxy for `http://` (Squid: `http://127.0.0.1:3128`).
    pub http_proxy: Option<String>,
    /// Proxy for `https://` / `wss://`. If `None`, [`Self::http_proxy`] is used.
    pub https_proxy: Option<String>,
    /// Host list without proxy, like `NO_PROXY`.
    pub no_proxy: Option<String>,
    /// Proxy user (Squid `proxy_auth`) when it is not in URL.
    pub proxy_username: Option<String>,
    /// Proxy password.
    pub proxy_password: Option<String>,
    /// Extra root CA in PEM. One string may contain several `CERTIFICATE` blocks.
    pub extra_ca_pem: Vec<String>,
    /// Paths to `.pem` / `.crt` files. Several CA can be several paths or one bundle.
    pub extra_ca_cert_paths: Vec<String>,
    /// Trust public Mozilla/webpki roots. Default is `true`.
    pub trust_webpki_roots: bool,
}

/// Parsed HTTP proxy (REST + CONNECT tunnel for WebSocket).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedProxy {
    /// Proxy host.
    pub host: String,
    /// Proxy port.
    pub port: u16,
    /// User name.
    pub username: Option<String>,
    /// Password.
    pub password: Option<String>,
}

impl Default for HttpClientConfig {
    fn default() -> Self {
        Self {
            timeout: None,
            http_proxy: None,
            https_proxy: None,
            no_proxy: None,
            proxy_username: None,
            proxy_password: None,
            extra_ca_pem: Vec::new(),
            extra_ca_cert_paths: Vec::new(),
            trust_webpki_roots: true,
        }
    }
}

impl HttpClientConfig {
    /// Empty config without proxy.
    pub fn new() -> Self {
        Self::default()
    }

    /// From env: `HTTP_PROXY`, `HTTPS_PROXY`, `NO_PROXY`, `HTTP_PROXY_USER`, `HTTP_PROXY_PASSWORD`.
    pub fn from_env() -> Self {
        let mut cfg = Self::new();
        if let Some(v) = read_env_proxy(&["HTTP_PROXY", "http_proxy"]) {
            cfg.http_proxy = Some(v);
        }
        if let Some(v) = read_env_proxy(&["HTTPS_PROXY", "https_proxy"]) {
            cfg.https_proxy = Some(v);
        }
        if let Some(v) = read_env_proxy(&["NO_PROXY", "no_proxy"]) {
            cfg.no_proxy = Some(v);
        }
        if let Some(v) = read_env_proxy(&[
            "HTTP_PROXY_USER",
            "http_proxy_user",
            "PROXY_USER",
            "proxy_user",
        ]) {
            cfg.proxy_username = Some(v);
        }
        if let Some(v) = read_env_proxy(&[
            "HTTP_PROXY_PASSWORD",
            "http_proxy_password",
            "PROXY_PASSWORD",
            "proxy_password",
        ]) {
            cfg.proxy_password = Some(v);
        }
        if let Some(v) = read_env_proxy(&["FPS_EXTRA_CA_CERT", "fps_extra_ca_cert"]) {
            for path in v.split(',') {
                let path = path.trim();
                if !path.is_empty() {
                    cfg.extra_ca_cert_paths.push(path.to_string());
                }
            }
        }
        cfg
    }

    /// Company CA PEM. Can be called several times.
    pub fn add_ca_pem(mut self, pem: impl Into<String>) -> Self {
        self.extra_ca_pem.push(pem.into());
        self
    }

    /// CA file (`.pem`, `.crt`). One file may contain several certificates.
    pub fn add_ca_file(mut self, path: impl Into<String>) -> Self {
        self.extra_ca_cert_paths.push(path.into());
        self
    }

    /// Only custom CA, without public webpki roots. Rare, for closed networks.
    pub fn trust_only_extra_ca(mut self) -> Self {
        self.trust_webpki_roots = false;
        self
    }

    /// Checks if extra company CA exists.
    pub fn has_extra_ca(&self) -> bool {
        !self.extra_ca_pem.is_empty() || !self.extra_ca_cert_paths.is_empty()
    }

    /// One proxy URL for HTTP and HTTPS.
    pub fn proxy(mut self, url: impl Into<String>) -> Self {
        let url = url.into();
        self.http_proxy = Some(url.clone());
        self.https_proxy = Some(url);
        self
    }

    /// Squid user and password (REST + WebSocket CONNECT).
    pub fn proxy_auth(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.proxy_username = Some(username.into());
        self.proxy_password = Some(password.into());
        self
    }

    /// HTTP proxy only.
    pub fn http_proxy(mut self, url: impl Into<String>) -> Self {
        self.http_proxy = Some(url.into());
        self
    }

    /// HTTPS proxy only.
    pub fn https_proxy(mut self, url: impl Into<String>) -> Self {
        self.https_proxy = Some(url.into());
        self
    }

    /// Proxy exceptions (`NO_PROXY`).
    pub fn no_proxy(mut self, hosts: impl Into<String>) -> Self {
        self.no_proxy = Some(hosts.into());
        self
    }

    /// Default timeout for this config.
    pub fn timeout(mut self, d: Duration) -> Self {
        self.timeout = Some(d);
        self
    }

    /// Proxy for WSS: `https_proxy` or `http_proxy`.
    pub fn parsed_wss_proxy(&self) -> Option<ParsedProxy> {
        let url = self.https_proxy.as_ref().or(self.http_proxy.as_ref())?;
        parse_proxy_url(
            url,
            self.proxy_username.as_deref(),
            self.proxy_password.as_deref(),
        )
        .ok()
    }
}

fn read_env_proxy(names: &[&str]) -> Option<String> {
    for name in names {
        if let Ok(v) = std::env::var(name) {
            let v = v.trim();
            if !v.is_empty() {
                return Some(v.to_string());
            }
        }
    }
    None
}

/// Parses proxy URL. User/password can come from URL or separate fields.
pub fn parse_proxy_url(
    proxy_url: &str,
    username: Option<&str>,
    password: Option<&str>,
) -> Result<ParsedProxy, HttpConfigError> {
    let parsed =
        url::Url::parse(proxy_url).map_err(|e| HttpConfigError(format!("proxy URL: {e}")))?;
    let host = parsed
        .host_str()
        .ok_or_else(|| HttpConfigError("proxy URL has no host".to_string()))?
        .to_string();
    let port = parsed
        .port_or_known_default()
        .ok_or_else(|| HttpConfigError("unknown proxy port".to_string()))?;
    let user = parsed.username();
    let username = if !user.is_empty() {
        Some(user.to_string())
    } else {
        username.map(|s| s.to_string())
    };
    let password = parsed
        .password()
        .filter(|p| !p.is_empty())
        .map(|p| p.to_string())
        .or_else(|| password.map(|s| s.to_string()));
    Ok(ParsedProxy {
        host,
        port,
        username,
        password,
    })
}

#[cfg(feature = "websocket")]
impl ParsedProxy {
    /// `Proxy-Authorization: Basic ...` header for CONNECT (WebSocket).
    pub fn proxy_authorization_header(&self) -> Option<String> {
        let user = self.username.as_deref()?;
        let pass = self.password.as_deref().unwrap_or("");
        let token = base64_encode(&format!("{user}:{pass}"));
        Some(format!("Basic {token}"))
    }
}

#[cfg(feature = "websocket")]
fn base64_encode(input: &str) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(input.as_bytes())
}
