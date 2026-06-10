// Copyright (c) 2026 FastPubSub Network
// SPDX-License-Identifier: MIT OR Apache-2.0
// Project: https://fastpubsub.com

//! Chat broadcast example.
//!
//! Run:
//! `FPS_MASTER_TOKEN=MT_... cargo run --features examples --example chat_broadcast`
//!
//! The example subscribes to `public.chat.#` first, then reads lines from the
//! console and publishes them to `public.chat.room1`. Because this client is
//! subscribed to the same room pattern, its own published messages appear in
//! the console through the subscription.

use std::io::{self, Write};

use fastpubsub_sdk::FastPubSubSession;

use fastpubsub_sdk::client::FastPubSub;
use fastpubsub_sdk::metadata::SdkMessage;
use fastpubsub_sdk::transport::{SubscribeOptions, WebSocketTransport};
use fastpubsub_sdk::{CreateAccessTokenBuilder, TenantGrant};

const OVERLAY: &str = "globaltest";
const TENANT: &str = "tenant_1";
const CHAT_PATTERN: &str = "public.chat.#";
const CHAT_CHANNEL: &str = "public.chat.room1";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let master_token = std::env::var("FPS_MASTER_TOKEN").expect("set FPS_MASTER_TOKEN=MT_...");

    let mut session = FastPubSubSession::new(OVERLAY);
    let edge = session.resolve_edge().await?;
    println!(
        "selected edge id={} api={} ping={:?}",
        edge.candidate.id, edge.candidate.api_base, edge.ping
    );

    let access_token = CreateAccessTokenBuilder::new()
        .created_by("chat-broadcast-example")
        .description("example chat broadcast")
        .expires_at_input("+1h")?
        .tenant_grant(
            TenantGrant::new(vec![TENANT.to_string()])
                .allow_pub(vec![CHAT_PATTERN.to_string()])
                .allow_sub(vec![CHAT_PATTERN.to_string()]),
        )
        .create_with_config(session.api_base()?, &master_token, session.http_config())
        .await?;

    let mut fastpubsub: FastPubSub<WebSocketTransport> =
        session.web_socket(access_token)?.build().await?;

    let mut chat_messages = fastpubsub
        .subscribe(TENANT, CHAT_PATTERN, &SubscribeOptions::default())
        .await?;

    let receiver_task = tokio::spawn(async move {
        while let Some(message) = chat_messages.recv().await {
            println!(
                "\nsubscriber matched={} channel={} payload={}",
                message.matched_pattern(),
                message.channel(),
                payload_text(&message)
            );
            print!("chat> ");
            let _ = io::stdout().flush();
        }
    });

    println!("type chat messages and press Enter");
    println!("empty line or /quit exits");

    let stdin = io::stdin();
    let mut line = String::new();
    loop {
        print!("chat> ");
        io::stdout().flush()?;

        line.clear();
        if stdin.read_line(&mut line)? == 0 {
            break;
        }

        let payload = line.trim_end_matches(['\r', '\n']);
        if payload.trim().is_empty() || payload.trim() == "/quit" {
            break;
        }

        fastpubsub
            .publish(TENANT, CHAT_CHANNEL, payload.as_bytes())
            .await?;
    }

    fastpubsub.unsubscribe(TENANT, CHAT_PATTERN).await?;
    receiver_task.abort();
    Ok(())
}

/// Returns the payload as simple text.
fn payload_text(message: &SdkMessage) -> String {
    String::from_utf8_lossy(message.payload.as_ref()).into_owned()
}
