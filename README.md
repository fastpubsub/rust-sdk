# FastPubSubNetwork Rust SDK

Rust client library for **FastPubSubNetwork** contract v0.1.

**FastPubSubNetwork is a Realtime Message CDN.** It is built by
**FastPubSub Network**. Website: [fastpubsub.com](https://fastpubsub.com).

The SDK is made for realtime publish/subscribe systems. An app does not send
data directly to another peer like with raw TCP or UDP. Instead, it publishes a
small binary payload to a topic-like channel, and FastPubSubNetwork delivers that
payload to every connected client whose subscription pattern matches the
channel. This is a global service, not a single-datacenter message broker.
Delivery goes through the internal FastPubSubNetwork overlay network, where
routing prefers the path with the lowest latency. It also has REST helpers for
edge discovery, ping, and access token creation.


## What This SDK Is For

Use this crate when a Rust service, server, worker, or desktop tool needs
to talk to FastPubSubNetwork.

Main tasks:

- connect to a FastPubSubNetwork edge with WebSocket;
- create short-lived access tokens from a master token;
- publish binary payloads to `tenant + channel`;
- subscribe to channel patterns like `public.#`;
- receive SDK messages with tenant, real channel, matched pattern, payload, and
  local metadata;
- add route filters for compression, encryption, fragmentation, batching,
  delta encoding, latest-only delivery, debug logging, send rate control, and
  bandwidth control;
- use helper types for game loops and latest-state storage.

WebTransport is present as a feature and demo path, but the real network task is
still a stub in contract v0.1.

## Build And Check

From the repository root:

```bash
cargo check
cargo build --release
```

Run formatting before a change is committed:

```bash
cargo fmt --manifest-path Cargo.toml
```

## Cargo Features

Default features enable REST, WebSocket, access token JSON, and JSON debug log
formatting.

The official crate is published on crates.io as `fastpubsub-sdk`. The dependency is
renamed to `fastpubsub_sdk` here so the Rust import path matches the SDK
examples.

```toml
[dependencies]
fastpubsub_sdk = { package = "fastpubsub-sdk", version = "0.1" }
```

Transport only, without REST discovery and token helpers:

```toml
[dependencies]
fastpubsub_sdk = {
    package = "fastpubsub-sdk",
    version = "0.1",
    default-features = false,
    features = ["websocket"]
}
```

Important features:

| Feature | Purpose |
|---------|---------|
| `rest` | REST discovery, `/ping`, `/v1/get-token`, `/v1/refresh-token`, `/v1/revoke-token`, `reqwest` |
| `websocket` | WebSocket transport with a Tokio background task |
| `webtransport` | WebTransport API stub |
| `access_token_json` | JSON builder for token requests |
| `debug_log_format_json` | JSON formatting for debug log filters |

## Basic Concepts

`overlay` is the FastPubSubNetwork overlay name. It is similar to an environment
name, for example dev, test, or prod. In these examples, `globaltest` is the
shared dev/test overlay name. A session starts from the overlay and resolves an
edge.

`tenant` is a logical namespace inside an access token. Publish and subscribe
rights are checked per tenant.

`channel` is the concrete publish route, for example `public.chat.room1`.

`channel_pattern` is the subscription pattern, for example `public.#`.

`payload` is binary data. The SDK does not force JSON, protobuf, or any other
application format.

`SdkMessage` is what subscribers receive after inbound filters finish.

## Recommended Client Flow

Normal production flow:

```text
open(overlay)
  -> resolve_edge()
  -> create_access_token()
  -> session.web_socket(access_token)?.build().await?
  -> publish / subscribe
```

### What `resolve_edge()` Does

`resolve_edge()` is the shortcut for two steps:

```rust
session.discover_edges().await?;
session.select_nearest_edge().await?;
```

First, the SDK asks the bootstrap service for edge candidates close to the
client:

```text
GET https://{overlay}.fastpubsub.workers.dev/
```

The bootstrap response contains edge hosts. The SDK turns each host into:

- `api_base`, used for REST calls like `/ping` and `/v1/get-token`;
- `ws_endpoint`, used for WebSocket transport;
- `wt_endpoint`, used for WebTransport when available;
- `region`, a region hint from the edge host.

After that, the SDK checks all returned edge servers in parallel. It calls a
very small unauthenticated ping API on every candidate:

```text
GET {edge_api_base}/ping
```

The ping request is not a business request. It is used only to measure which
edge is closest from this client. The measured time includes the network path to
the edge and the HTTP request work. On a new connection this also includes TCP
handshake and TLS handshake time. That is useful because WebSocket connection
setup will pay the same kind of network cost.

The ping API should stay simple and comparable between edges. A fixed small
payload, for example about 1 KB, is enough to avoid measuring only an empty
response while still keeping the request cheap.

When all candidates are checked, the SDK stores the fastest one as
`SelectedEdge`. Later calls use it:

- `session.api_base()?` returns the selected edge REST URL for token creation;
- `session.web_socket(access_token)?` uses the selected WebSocket endpoint;
- `session.selected_edge()` returns the selected candidate and measured ping.

Direct endpoint flow for tests and local experiments:

```rust
use fastpubsub_sdk::client::create_web_socket;
use fastpubsub_sdk::transport::WebSocketTransport;
use fastpubsub_sdk::FastPubSub;

let client: FastPubSub<WebSocketTransport> =
    create_web_socket("overlay_name", "AT_token")
        .endpoint("wss://edge.example/ws")
        .build()
        .await?;
```

## Full WebSocket Flow

This creates a session, selects an edge, creates an access token, connects over
WebSocket, subscribes, and publishes one payload.

```rust
use fastpubsub_sdk::client::{open, FastPubSub};
use fastpubsub_sdk::transport::{SubscribeOptions, WebSocketTransport};
use fastpubsub_sdk::{CreateAccessTokenBuilder, TenantGrant};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let master_token = std::env::var("FPS_MASTER_TOKEN")?;

    let mut session = open("globaltest");
    let edge = session.resolve_edge().await?;
    println!("selected edge: {}", edge.candidate.api_base);

    let access_token = CreateAccessTokenBuilder::new()
        .created_by("my-rust-app")
        .expires_at_input("+1h")?
        .tenant_grant(
            TenantGrant::new(vec!["tenant_1".to_string()])
                .allow_pub(vec!["public.#".to_string()])
                .allow_sub(vec!["public.#".to_string()]),
        )
        .create_with_config(session.api_base()?, &master_token, session.http_config())
        .await?;

    let mut client: FastPubSub<WebSocketTransport> =
        session.web_socket(access_token)?.build().await?;

    let mut rx = client
        .subscribe("tenant_1", "public.#", &SubscribeOptions::default())
        .await?;

    client
        .publish("tenant_1", "public.demo", b"hello from rust")
        .await?;

    if let Some(message) = rx.recv().await {
        println!(
            "received from channel={} payload={}",
            message.channel(),
            String::from_utf8_lossy(message.payload().as_ref())
        );
    }

    Ok(())
}
```

Runnable version: [`examples/websocket.rs`](examples/websocket.rs).

More WebSocket protocol details: [`README.websocket.md`](README.websocket.md).

## Publish

Publish sends one payload to one concrete channel. Do not use wildcards in a
publish channel.

```rust
client
    .publish("tenant_1", "public.chat.room1", b"hello")
    .await?;
```

Use `publish_with_options` when you need explicit transport options:

```rust
use fastpubsub_sdk::transport::{PublishDeliveryMode, PublishOptions};

let options = PublishOptions {
    delivery: PublishDeliveryMode::Broadcast,
};

client
    .publish_with_options("tenant_1", "public.chat.room1", b"hello", &options)
    .await?;
```

Delivery modes:

| `PublishDeliveryMode` | Wire | Behaviour |
|-----------------------|------|-----------|
| `Broadcast` | v1 frame (no tag) | All overlay target nodes and all local subscribers |
| `DeliverOneLowLatency` | v2, mode `1` | One overlay node with lowest inter-node latency among nodes with subscribers; one random local subscribed connection on the edge |
| `DeliverOneRandom` | v2, mode `2` | One random overlay node with subscribers; one random local subscribed connection |

If no remote or local subscribers exist, deliver-one modes do nothing. Routing
on the overlay requires an updated perimeter.

## WS Link Quality

HTTP `GET /ping` during `resolve_edge()` measures which edge to connect to.
After the WebSocket is open, optional application `PING`/`PONG` measures RTT on
the active connection. The perimeter only answers `PONG`; it does not track
client RTT.

```rust
use fastpubsub_sdk::client::create_web_socket;
use fastpubsub_sdk::transport::WebSocketEvent;

let client = create_web_socket("overlay_name", "AT_token")
    .ping_interval_secs(3)
    .build()
    .await?;

let quality = client.link_quality().await?;
println!("last={:?} median={:?}", quality.last_rtt_ms, quality.median_rtt_ms);

// In the event loop:
match event {
    WebSocketEvent::RttMeasured { rtt_ms } => println!("rtt={rtt_ms} ms"),
    _ => {}
}
```

Allowed intervals are `1`, `3`, and `5` seconds. Omit `ping_interval_secs` to
disable WS ping.

In v0.1, success means the SDK accepted the payload and passed it to the
transport. It does not mean that every remote subscriber already received it.

## Subscribe

Subscribe opens a local `mpsc::Receiver<SdkMessage>`.

```rust
use fastpubsub_sdk::transport::SubscribeOptions;

let mut rx = client
    .subscribe("tenant_1", "public.#", &SubscribeOptions::default())
    .await?;

while let Some(message) = rx.recv().await {
    println!(
        "tenant={} channel={} matched={} bytes={}",
        message.tenant(),
        message.channel(),
        message.matched_pattern(),
        message.payload().len()
    );
}
```

Call `unsubscribe` with the same tenant and pattern when the subscription is no
longer needed:

```rust
client.unsubscribe("tenant_1", "public.#").await?;
```

## Helpers

### LatestMessageReceiver

`LatestMessageReceiver` is useful for game loops and UI loops. If many messages
arrive between frames, it drops old local queued messages and returns only the
newest one.

```rust
use fastpubsub_sdk::helpers::LatestMessageReceiver;

let rx = client
    .subscribe("tenant_1", "game.#", &SubscribeOptions::default())
    .await?;

let mut latest = LatestMessageReceiver::new(rx);

if let Some(message) = latest.poll_latest_or_cached() {
    println!("latest payload bytes={}", message.payload().len());
}
```

Runnable example: [`examples/latest_receiver.rs`](examples/latest_receiver.rs).

### LatestStateSync

`LatestStateSync` stores the latest state by key on top of normal pub/sub. It is
useful for player positions, NPC state, room state, or other values where only
the newest value for a key matters.

```rust
use std::time::Duration;

use fastpubsub_sdk::helpers::{CleanupPolicy, LatestStateSync, LatestStateSyncOptions};

let shared = client.into_shared();
let options = LatestStateSyncOptions::default()
    .cleanup_policy(CleanupPolicy::AfterIdle(Duration::from_secs(30)));

let states = LatestStateSync::connect(
    shared.clone(),
    "tenant_1",
    "game.players.state",
    options,
)
.await?;

states.push("player:42", br#"{"x":10,"y":20}"#).await?;

if let Some(state) = states.get_state("player:42") {
    println!("state={}", String::from_utf8_lossy(state.as_ref()));
}
```

Runnable example:
[`examples/latest_state_sync.rs`](examples/latest_state_sync.rs).

## Filters

Filters run inside the transport pipeline. They can change, split, join, or
drop payloads. A filter is registered for an exact tenant and a static channel
prefix.

```rust
use fastpubsub_sdk::filters::CompressedFilter;

let client = create_web_socket("overlay_name", "AT_token")
    .endpoint("wss://edge.example/ws")
    .add_filter("tenant_1", "public.", CompressedFilter::new())
    .build()
    .await?;
```

Inbound filters run in registration order. Outbound filters run in reverse
registration order.

Available filter implementations are in [`src/filters/`](src/filters/).

| Filter | Purpose |
|--------|---------|
| `CompressedFilter` | Deflate raw compression |
| `EncryptionFilter` | Payload encryption |
| `FragmentFilter` | Split large payloads and restore missing fragments |
| `ChannelBatchFilter` | Batch small messages on one channel |
| `Delta16Filter`, `Delta32Filter`, `Delta64Filter` | Send snapshots and compact deltas |
| `BandwidthLimiterFilter` | Limit outbound bytes per second |
| `SendRateFilter` | Limit send rate |
| `LatestOnlyFilter` | Keep only the newest message per client on inbound routes |
| `DebugLogFilter` | Log filter stages |
| `DummyFilter` | Test and demo pass-through filter |

Timer-based filters use `filter_timer_mode` on the builder:

```rust
use fastpubsub_sdk::filters::{BandwidthLimiterFilter, FilterTimerMode};

let client = create_web_socket("overlay_name", "AT_token")
    .endpoint("wss://edge.example/ws")
    .filter_timer_mode(FilterTimerMode::SdkDefault100Hz)
    .add_filter(
        "tenant_1",
        "public.",
        BandwidthLimiterFilter::from_millis(
            1_000_000,
            500,
            1024,
            8 * 1024 * 1024,
        ),
    )
    .build()
    .await?;
```

### LatestOnlyFilter

`LatestOnlyFilter` is for routes where only the newest message per client
should reach subscribers. On outbound publish it adds a `client_id` and monotonic
counter prefix to the payload. On inbound delivery it keeps the last accepted
counter per `tenant + channel + client_id` and drops older messages.

Use it for high-frequency streams such as player position or sensor data where
late packets are useless.

```rust
use std::time::Duration;

use fastpubsub_sdk::filters::LatestOnlyFilter;

let client = create_web_socket("overlay_name", "AT_token")
    .endpoint("wss://edge.example/ws")
    .add_filter(
        "tenant_1",
        "player.position.",
        LatestOnlyFilter::new(Duration::from_secs(60)),
    )
    .build()
    .await?;
```

Inbound messages without a `LatestOnlyFilter` header follow
`InvalidPrefixPolicy`:

| Policy | Behavior |
|--------|----------|
| `PassThrough` (default) | Pass the payload through unchanged |
| `Drop` | Drop the message without an error |
| `ErrorEventAndDrop` | Return a filter error and drop the message |

Change the policy with `with_invalid_prefix_policy`:

```rust
use fastpubsub_sdk::filters::{InvalidPrefixPolicy, LatestOnlyFilter};

LatestOnlyFilter::with_default_timeout()
    .with_invalid_prefix_policy(InvalidPrefixPolicy::Drop);
```

When a stale message is dropped, the filter emits a warning
`FilterNotice`. Enable `on_websocket_event` on the builder to receive it as
`WebSocketEvent::FilterNotice`:

```rust
use fastpubsub_sdk::WebSocketEvent;

match event {
    WebSocketEvent::FilterNotice { level, message } => {
        println!("filter {level}: {message}");
    }
    _ => {}
}
```

## Metadata

Inbound filters can write local metadata into `SdkMessage::meta`. This metadata
is local to the SDK message and is not part of the application payload.

Enable metadata stages on the builder:

```rust
use fastpubsub_sdk::MetaMode;

let client = create_web_socket("overlay_name", "AT_token")
    .endpoint("wss://edge.example/ws")
    .meta_mode(MetaMode::Stages)
    .build()
    .await?;
```

Read records from a received message:

```rust
if message.has_metadata() {
    for record in message.meta().records()? {
        let record = record?;
        println!("record type={} bytes={}", record.record_type, record.data.len());
    }
}
```

Metadata types are defined in [`src/metadata.rs`](src/metadata.rs).

## REST API Helpers

REST is used before transport connect.

| Method | Path | Auth | Purpose |
|--------|------|------|---------|
| `GET` | `/ping` | none | Measure edge latency |
| `POST` | `/v1/get-token` | `Bearer` master token | Create an access token |
| `PUT` | `/v1/refresh-token` | `Bearer` master token | Extend AT expiry (`token_id` + `expires_at`) |
| `DELETE` | `/v1/revoke-token` | `Bearer` master token | Revoke a full `AT_...` |

Common helper types:

| Type or function | Purpose |
|------------------|---------|
| `open()` | Starts a `FastPubSubSession` for an overlay |
| `FastPubSubSession::resolve_edge()` | Finds and selects an edge |
| `CreateAccessTokenBuilder` | Builds the access token request |
| `refresh_access_token` / `refresh_access_token_from_at` | Extends AT TTL (master token) |
| `revoke_access_token` | Revokes AT (master token) |
| `access_token_id` / `parse_access_token` | Parses `AT_{id}_{secret}` |
| `TenantGrant` | Defines tenant publish and subscribe rights |
| `ping_many()` / `ping_fastest()` | Measures edge latency |
| `HttpClientConfig` | Proxy, custom CA, and HTTP settings |

Discovery uses the bootstrap service for the selected overlay. The bootstrap
service returns edge hosts, and the SDK selects the nearest edge by parallel
`/ping` checks.

## HTTP Proxy And Corporate CA

Proxy and custom CA settings can come from environment variables or from
`HttpClientConfig`.

Environment variables:

| Variable | Purpose |
|----------|---------|
| `HTTP_PROXY` / `HTTPS_PROXY` | Proxy URL, for example Squid |
| `HTTP_PROXY_USER` / `HTTP_PROXY_PASSWORD` | Proxy user and password |
| `NO_PROXY` | Bypass list |
| `FPS_EXTRA_CA_CERT` | Extra CA file paths, comma-separated |
| `FPS_MASTER_TOKEN` | Master token for `/v1/get-token` |
| `FPS_ACCESS_TOKEN` | Access token for direct endpoint examples |
| `FPS_WS_ENDPOINT` | WebSocket endpoint for direct endpoint examples |

Session methods:

```rust
let session = open("globaltest")
    .with_proxy("http://127.0.0.1:3128")
    .with_proxy_auth("user", "password")
    .with_ca_file("/etc/ssl/corp-ca.pem");
```

By default, TLS trust uses public `webpki` roots plus extra CA files. Use
`trust_only_extra_ca()` when only the provided CA files must be trusted.

## Examples In This Repository

Examples are in [`examples/`](examples/). Run commands from `rust-sdk/`.

| Example | What it shows |
|---------|---------------|
| `websocket` | Full session: edge resolve, access token, WebSocket, subscribe, publish, and events |
| `webtransport` | WebTransport builder path. The transport is still a stub |
| `chat_broadcast` | Broadcast chat messages to every subscriber matching `public.chat.#` |
| `latest_receiver` | Read only the newest queued message |
| `latest_state_sync` | Store latest state values by key |

## Main Modules

| Module | Role |
|--------|------|
| [`src/client`](src/client) | Session, builder, REST helpers, shared client, publish/subscribe API |
| [`src/transport`](src/transport) | Transport trait, WebSocket implementation, WebTransport stub |
| [`src/filters`](src/filters) | Route filters for inbound and outbound payloads |
| [`src/helpers`](src/helpers) | Latest message reader and latest state sync |
| [`src/metadata.rs`](src/metadata.rs) | Local metadata written by inbound filters |

## Limits In v0.1

- `PublishDeliveryMode` is encoded in publish frame v2 (`0x02` tag +
  `delivery_mode` byte). `Broadcast` keeps the legacy v1 frame.
- WS application `PING`/`PONG` measures RTT in the SDK
  (`ping_interval_secs(1|3|5)`, `link_quality()`, `RttMeasured`). HTTP `/ping`
  is only for edge selection.
- `DeliverOneLowLatency` uses overlay latency between nodes; on the local edge
  it picks one random subscribed connection.
- WebTransport has no real network task yet.
- Delivery success means local SDK/transport acceptance, not confirmed delivery
  to every remote subscriber.

## Changelog

Release notes are in [`CHANGELOG.md`](CHANGELOG.md).

## License

MIT OR Apache-2.0
