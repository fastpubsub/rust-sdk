// Copyright (c) 2026 FastPubSub Network
// SPDX-License-Identifier: MIT OR Apache-2.0
// Project: https://fastpubsub.com

//! Example: overlay -> bootstrap -> ping edge -> AT -> WebSocket publish.
//!
//! Squid proxy:
//! - `HTTP_PROXY` / `HTTPS_PROXY` - proxy address
//! - `HTTP_PROXY_USER` / `HTTP_PROXY_PASSWORD` - user name and password
//! - `FPS_EXTRA_CA_CERT` - path to company CA files, separated by comma, for MITM TLS
//! - or `.with_proxy_auth(...)`, `.with_ca_file("/etc/ssl/corp-ca.pem")`
//!
//! `FPS_MASTER_TOKEN` - master token for `POST /v1/get-token`.

use fastpubsub_sdk::client::{open, FastPubSub};
use fastpubsub_sdk::filters::DummyFilter;
use fastpubsub_sdk::transport::{SubscribeOptions, WebSocketTransport};
use fastpubsub_sdk::{CreateAccessTokenBuilder, TenantGrant, WebSocketError, WebSocketEvent};
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};

const DEFAULT_TEST_MASTER_TOKEN: &str = "MT_notoken";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let master_token =
        std::env::var("FPS_MASTER_TOKEN").unwrap_or_else(|_| DEFAULT_TEST_MASTER_TOKEN.to_string());

    let proxy_user = std::env::var("HTTP_PROXY_USER").ok();
    let proxy_pass = std::env::var("HTTP_PROXY_PASSWORD").ok();

    // 1. Session: overlay and proxy from env. User and password can be set below.
    let mut session = open("globaltest");
    if let (Some(u), Some(p)) = (&proxy_user, &proxy_pass) {
        session = session.with_proxy_auth(u.clone(), p.clone());
    }
    // Or: session = open("...")
    //     .with_proxy("http://127.0.0.1:3128")
    //     .with_proxy_auth("squid_user", "squid_pass");

    let edge = session.resolve_edge().await?;
    println!(
        "selected edge id={} api={} ping={:?}",
        edge.candidate.id, edge.candidate.api_base, edge.ping
    );

    let at_token = CreateAccessTokenBuilder::new()
        .created_by("websocket-demo")
        .description("example websocket")
        .expires_at_input("+1h")?
        .tenant_grant(
            TenantGrant::new(vec!["tenant_1".to_string()])
                .allow_pub(vec!["public.#".to_string()])
                .allow_sub(vec!["public.#".to_string()]),
        )
        .create_with_config(session.api_base()?, &master_token, session.http_config())
        .await?;

    println!("access token: {}…", &at_token[..at_token.len().min(20)]);

    let (event_tx, mut event_rx) = mpsc::channel(128);
    tokio::spawn(async move {
        while let Some(event) = event_rx.recv().await {
            print_websocket_event(event);
        }
    });

    let mut fastpubsub: FastPubSub<WebSocketTransport> = session
        .web_socket(at_token)?
        .add_filter("tenant_1", "", DummyFilter)
        .on_websocket_event(event_tx)
        .build()
        .await?;

    let mut messages = fastpubsub
        .subscribe("tenant_1", "public.#", &SubscribeOptions::default())
        .await?;

    tokio::spawn(async move {
        while let Some(message) = messages.recv().await {
            println!(
                "websocket: received message payload={}",
                String::from_utf8_lossy(message.payload.as_ref())
            );
        }
    });

    fastpubsub
        .publish("tenant_1", "public.demo", b"payload-bytes")
        .await?;

    println!(
        "websocket: overlay={} api_base={} publish=ok",
        fastpubsub.overlay_network_name(),
        fastpubsub.api_base()
    );
    sleep(Duration::from_secs(1)).await;
    Ok(())
}

/// Prints WebSocket transport events from the SDK.
fn print_websocket_event(event: WebSocketEvent) {
    match event {
        WebSocketEvent::Error(error) => print_websocket_error(error),
        WebSocketEvent::ReconnectStarted => {
            println!("websocket: smooth reconnect started");
        }
        WebSocketEvent::ReconnectReady => {
            println!("websocket: reconnect is ready, new connection is active");
        }
        WebSocketEvent::SubscriptionConfirmed {
            tenant,
            channel_pattern,
        } => {
            println!("websocket: subscription confirmed tenant={tenant} pattern={channel_pattern}");
        }
        WebSocketEvent::UnsubscriptionConfirmed {
            tenant,
            channel_pattern,
        } => {
            println!("websocket: unsubscribe confirmed tenant={tenant} pattern={channel_pattern}");
        }
        WebSocketEvent::DuplicateDropped => {
            println!("websocket: duplicate message dropped during reconnect");
        }
        WebSocketEvent::FilterNotice { level, message } => {
            println!("websocket: filter notice [{level}] {message}");
        }
        WebSocketEvent::RttMeasured { rtt_ms } => {
            println!("websocket: rtt {rtt_ms} ms");
        }
    }
}

/// Prints a typed WebSocket error as simple text.
fn print_websocket_error(error: WebSocketError) {
    match error {
        WebSocketError::Server { message } => {
            eprintln!("websocket server error: {message}");
        }
        WebSocketError::Reconnect {
            connection_id,
            message,
        } => {
            eprintln!("websocket reconnect error: connection_id={connection_id} error={message}");
        }
        WebSocketError::Read {
            connection_id,
            message,
        } => {
            eprintln!("websocket read error: connection_id={connection_id} error={message}");
        }
        WebSocketError::Transport { operation, message } => {
            eprintln!("websocket transport error: operation={operation} error={message}");
        }
        WebSocketError::Decode { message } => {
            eprintln!("websocket decode error: {message}");
        }
        WebSocketError::InboundFilter { message } => {
            eprintln!("websocket inbound filter error: {message}");
        }
        WebSocketError::SubscriptionQueueFull {
            tenant,
            channel_pattern,
        } => {
            eprintln!("websocket queue full: tenant={tenant} pattern={channel_pattern}");
        }
        WebSocketError::MissingSubscription {
            tenant,
            channel_patterns,
        } => {
            eprintln!(
                "websocket missing subscription: tenant={tenant} patterns={channel_patterns:?}"
            );
        }
        WebSocketError::EmptyDelivery { tenant, channel } => {
            eprintln!("websocket empty delivery: tenant={tenant} channel={channel}");
        }
    }
}
