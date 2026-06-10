// Copyright (c) 2026 FastPubSub Network
// SPDX-License-Identifier: MIT OR Apache-2.0
// Project: https://fastpubsub.com

//! Builds `reqwest::Client` (feature `rest`).

use std::time::Duration;

use crate::client::http_config::{parse_proxy_url, HttpClientConfig};
use crate::client::tls_roots;

use super::ApiError;

/// Adds company CA to `reqwest::ClientBuilder`.
pub fn apply_extra_ca_to_reqwest(
    builder: reqwest::ClientBuilder,
    config: &HttpClientConfig,
) -> Result<reqwest::ClientBuilder, ApiError> {
    let mut builder = builder;
    for pem in tls_roots::load_all_ca_pem(config).map_err(|e| ApiError::Tls(e.0))? {
        for cert_pem in tls_roots::split_pem_certificates(&pem) {
            let cert = reqwest::Certificate::from_pem(cert_pem)
                .map_err(|e| ApiError::Tls(format!("reqwest CA: {e}")))?;
            builder = builder.add_root_certificate(cert);
        }
    }
    Ok(builder)
}

/// Builds `reqwest::Client` with proxy and timeout.
pub(crate) fn build_http_client(
    config: &HttpClientConfig,
    default_timeout: Duration,
) -> Result<reqwest::Client, ApiError> {
    let timeout = config.timeout.unwrap_or(default_timeout);
    let mut builder = reqwest::Client::builder().timeout(timeout);

    let no_proxy = config
        .no_proxy
        .as_ref()
        .and_then(|list| reqwest::NoProxy::from_string(list));

    let apply_proxy = |proxy_url: &str, is_https: bool| -> Result<reqwest::Proxy, ApiError> {
        let parsed = parse_proxy_url(
            proxy_url,
            config.proxy_username.as_deref(),
            config.proxy_password.as_deref(),
        )
        .map_err(|e| ApiError::UnexpectedBody { body: e.0 })?;
        let base = format!("http://{}:{}", parsed.host, parsed.port);
        let _ = is_https;
        let mut p = if is_https {
            reqwest::Proxy::https(&base)?
        } else {
            reqwest::Proxy::http(&base)?
        };
        p = p.no_proxy(no_proxy.clone());
        if let (Some(u), Some(pw)) = (&parsed.username, &parsed.password) {
            p = p.basic_auth(u, pw);
        }
        Ok(p)
    };

    match (&config.http_proxy, &config.https_proxy) {
        (Some(h), Some(s)) => {
            builder = builder
                .proxy(apply_proxy(h, false)?)
                .proxy(apply_proxy(s, true)?);
        }
        (Some(h), None) => {
            builder = builder
                .proxy(apply_proxy(h, false)?)
                .proxy(apply_proxy(h, true)?);
        }
        (None, Some(s)) => {
            builder = builder.proxy(apply_proxy(s, true)?);
        }
        (None, None) => {}
    }

    builder = apply_extra_ca_to_reqwest(builder, config)?;
    Ok(builder.build()?)
}
