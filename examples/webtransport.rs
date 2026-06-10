// Copyright (c) 2026 FastPubSub Network
// SPDX-License-Identifier: MIT OR Apache-2.0
// Project: https://fastpubsub.com

//! Example: same flow as `websocket`, but with WebTransport.
//!
//! Example, like JS: `create_web_transport(...).add_filter(...).build().await`,
//! then `publish(...).await`.

use fastpubsub_sdk::client::{create_web_transport, FastPubSub};
use fastpubsub_sdk::filters::DummyFilter;
use fastpubsub_sdk::transport::WebTransport;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Explicit type: `fastpubsub: WebTransport = ...`
    let mut fastpubsub: FastPubSub<WebTransport> =
        create_web_transport("overleynetworkname", "AT_token")
            .add_filter("tenant_1", "", DummyFilter)
            .add_filter("tenant_1", "", DummyFilter)
            .build()
            .await?;

    fastpubsub
        .publish("tenant_1", "public.demo", b"payload-bytes")
        .await?;
    println!(
        "webtransport: overlay={} publish=ok",
        fastpubsub.overlay_network_name()
    );
    Ok(())
}
