// Copyright (c) 2026 FastPubSub Network
// SPDX-License-Identifier: MIT OR Apache-2.0
// Project: https://fastpubsub.com

//! Latest state sync example for different game object types.
//!
//! Run:
//! `FPS_ACCESS_TOKEN=AT_... FPS_WS_ENDPOINT=wss://edge/ws cargo run --features examples --example latest_state_sync`

use std::time::Duration;

use fastpubsub_sdk::client::{create_web_socket, FastPubSub};
use fastpubsub_sdk::helpers::{CleanupPolicy, LatestStateSync, LatestStateSyncOptions};
use fastpubsub_sdk::transport::WebSocketTransport;
use tokio::time::sleep;

/// Connects to the network and creates two independent state sync handles.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let access_token = std::env::var("FPS_ACCESS_TOKEN").expect("set FPS_ACCESS_TOKEN=AT_...");
    let endpoint = std::env::var("FPS_WS_ENDPOINT").expect("set FPS_WS_ENDPOINT=wss://...");

    let fastpubsub: FastPubSub<WebSocketTransport> = create_web_socket("globaltest", access_token)
        .endpoint(endpoint)
        .build()
        .await?;
    let client = fastpubsub.into_shared();

    let user_options = LatestStateSyncOptions::default()
        .cleanup_policy(CleanupPolicy::AfterIdle(Duration::from_secs(30)));
    let npc_options = LatestStateSyncOptions::default()
        .cleanup_policy(CleanupPolicy::AfterIdle(Duration::from_secs(5)));

    let user_states =
        LatestStateSync::connect(client.clone(), "tenant_1", "game.users.state", user_options)
            .await?;
    let npc_states =
        LatestStateSync::connect(client.clone(), "tenant_1", "game.npc.state", npc_options).await?;

    user_states.push("user:123", b"{\"x\":10,\"y\":20}").await?;
    npc_states.push("npc:88", b"{\"state\":\"idle\"}").await?;

    sleep(Duration::from_millis(50)).await;

    if let Some(state) = user_states.get_state("user:123") {
        println!("user:123 state={}", String::from_utf8_lossy(state.as_ref()));
    }
    if let Some(state) = npc_states.get_state("npc:88") {
        println!("npc:88 state={}", String::from_utf8_lossy(state.as_ref()));
    }

    println!(
        "state counts: users={} npc={}",
        user_states.len(),
        npc_states.len()
    );
    Ok(())
}
