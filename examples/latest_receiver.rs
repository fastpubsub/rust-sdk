// Copyright (c) 2026 FastPubSub Network
// SPDX-License-Identifier: MIT OR Apache-2.0
// Project: https://fastpubsub.com

//! Example that reads only the latest message for a game loop.
//!
//! Run:
//! `FPS_ACCESS_TOKEN=AT_... FPS_WS_ENDPOINT=wss://edge/ws cargo run --features examples --example latest_receiver`

use fastpubsub_sdk::client::{create_web_socket, FastPubSub};
use fastpubsub_sdk::helpers::LatestMessageReceiver;
use fastpubsub_sdk::transport::{SubscribeOptions, WebSocketTransport};
use fastpubsub_sdk::SdkMessage;
use tokio::time::{sleep, Duration};

/// Connects to WebSocket transport and reads only the latest message.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let access_token = std::env::var("FPS_ACCESS_TOKEN").expect("set FPS_ACCESS_TOKEN=AT_...");
    let endpoint = std::env::var("FPS_WS_ENDPOINT").expect("set FPS_WS_ENDPOINT=wss://...");

    let mut fastpubsub: FastPubSub<WebSocketTransport> =
        create_web_socket("globaltest", access_token)
            .endpoint(endpoint)
            .build()
            .await?;

    let rx = fastpubsub
        .subscribe("tenant_1", "game.#", &SubscribeOptions::default())
        .await?;

    let mut messages = LatestMessageReceiver::new(rx);

    fastpubsub
        .publish("tenant_1", "game.player.position", b"position=1")
        .await?;
    fastpubsub
        .publish("tenant_1", "game.player.position", b"position=2")
        .await?;
    fastpubsub
        .publish("tenant_1", "game.player.position", b"position=3")
        .await?;

    if let Some(message) = messages.poll_latest() {
        println!(
            "poll_latest took only the latest message: {}",
            payload_text(&message)
        );
    }

    for frame in 0..3 {
        if let Some(message) = messages.poll_latest_or_cached() {
            println!(
                "frame={frame}: use the latest state: {}",
                payload_text(&message)
            );
        }

        sleep(Duration::from_millis(16)).await;
    }

    fastpubsub
        .publish("tenant_1", "game.player.position", b"position=4")
        .await?;
    sleep(Duration::from_millis(50)).await;

    if let Some(message) = messages.poll_latest_or_cached() {
        println!(
            "cache was updated after a new message: {}",
            payload_text(&message)
        );
    }

    Ok(())
}

/// Returns the payload as simple text.
fn payload_text(message: &SdkMessage) -> String {
    String::from_utf8_lossy(message.payload.as_ref()).into_owned()
}
