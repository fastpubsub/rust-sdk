// Copyright (c) 2026 FastPubSub Network
// SPDX-License-Identifier: MIT OR Apache-2.0
// Project: https://fastpubsub.com

//! `GET /ping` checks REST API without authorization.

use std::time::{Duration, Instant};

use tokio::sync::mpsc;

use super::{api_url, build_http_client, ApiError};
use crate::client::http_config::HttpClientConfig;

/// Response time for one base URL, used in batch ping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PingTiming {
    /// REST API base URL.
    pub api_base: String,
    /// Time spent on `GET /ping`.
    pub duration: Duration,
}

/// Result of parallel ping: up to three earliest responses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PingManyResult {
    /// Up to three earliest successful responses with measured time.
    pub timings: Vec<PingTiming>,
}

/// Timeout for one ping request.
const PING_TIMEOUT: Duration = Duration::from_secs(3);

/// `GET /ping` through a ready client.
async fn ping_with_client(client: &reqwest::Client, api_base: &str) -> Result<(), ApiError> {
    let url = api_url(api_base, "ping");
    let response = client.get(&url).send().await?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(ApiError::UnexpectedStatus {
            status: status.as_u16(),
            body,
        });
    }
    if body.trim() != "pong" {
        return Err(ApiError::UnexpectedBody { body });
    }
    Ok(())
}

/// `GET /ping` checks API availability, **without** Authorization.
pub async fn ping(api_base: &str) -> Result<(), ApiError> {
    ping_with_config(api_base, &HttpClientConfig::default()).await
}

/// `GET /ping` with client settings: proxy, timeout.
pub async fn ping_with_config(api_base: &str, config: &HttpClientConfig) -> Result<(), ApiError> {
    let client = build_http_client(config, PING_TIMEOUT)?;
    ping_with_client(&client, api_base).await
}

/// Message from one background check.
struct PingDone {
    api_base: String,
    duration: Duration,
    result: Result<(), ApiError>,
}

/// Pings all base URLs in parallel. The channel returns **ready** responses.
///
/// [`PingManyResult::timings`] contains up to **3** earliest successful responses. Others are not awaited.
pub async fn ping_many(api_bases: &[&str]) -> Result<PingManyResult, ApiError> {
    ping_many_with_config(api_bases, &HttpClientConfig::default()).await
}

/// Same as [`ping_many`], with proxy and timeout from `config`.
pub async fn ping_many_with_config(
    api_bases: &[&str],
    config: &HttpClientConfig,
) -> Result<PingManyResult, ApiError> {
    if api_bases.is_empty() {
        return Ok(PingManyResult {
            timings: Vec::new(),
        });
    }

    let client = build_http_client(config, PING_TIMEOUT)?;
    let want_timings = 3.min(api_bases.len());
    let (tx, mut rx) = mpsc::channel(api_bases.len());

    for &base in api_bases {
        let client = client.clone();
        let base = base.to_string();
        let tx = tx.clone();
        tokio::spawn(async move {
            let started = Instant::now();
            let result = ping_with_client(&client, &base).await;
            let msg = PingDone {
                api_base: base,
                duration: started.elapsed(),
                result,
            };
            let _ = tx.send(msg).await;
        });
    }
    drop(tx);

    let mut timings = Vec::with_capacity(want_timings);

    while timings.len() < want_timings {
        let Some(done) = rx.recv().await else {
            break;
        };
        done.result?;
        timings.push(PingTiming {
            api_base: done.api_base,
            duration: done.duration,
        });
    }

    Ok(PingManyResult { timings })
}

/// Pings all base URLs in parallel and returns the **fastest** by RTT.
///
/// Waits for a response from every base URL. Ready responses arrive as they finish.
/// If at least one fails, returns `Err` immediately. Edge selection needs all successful.
pub async fn ping_fastest(api_bases: &[&str]) -> Result<PingTiming, ApiError> {
    ping_fastest_with_config(api_bases, &HttpClientConfig::default()).await
}

/// Same as [`ping_fastest`], with proxy from `config`, for edge selection behind Squid.
pub async fn ping_fastest_with_config(
    api_bases: &[&str],
    config: &HttpClientConfig,
) -> Result<PingTiming, ApiError> {
    if api_bases.is_empty() {
        return Err(ApiError::UnexpectedBody {
            body: "no base URLs for ping".to_string(),
        });
    }

    let client = build_http_client(config, PING_TIMEOUT)?;
    let total = api_bases.len();
    let (tx, mut rx) = mpsc::channel(total);

    for &base in api_bases {
        let client = client.clone();
        let base = base.to_string();
        let tx = tx.clone();
        tokio::spawn(async move {
            let started = Instant::now();
            let result = ping_with_client(&client, &base).await;
            let msg = PingDone {
                api_base: base,
                duration: started.elapsed(),
                result,
            };
            let _ = tx.send(msg).await;
        });
    }
    drop(tx);

    let mut timings = Vec::with_capacity(total);
    let mut received = 0usize;

    while received < total {
        let Some(done) = rx.recv().await else {
            break;
        };
        received += 1;
        done.result?;
        timings.push(PingTiming {
            api_base: done.api_base,
            duration: done.duration,
        });
    }

    timings
        .into_iter()
        .min_by_key(|t| t.duration)
        .ok_or(ApiError::UnexpectedBody {
            body: "no ping completed".to_string(),
        })
}
