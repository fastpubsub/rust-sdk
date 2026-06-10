# WebSocket in FastPubSubNetwork SDK

Transport: `WebSocketTransport`, background task in `transport/ws_task.rs`, wire format in `transport/ws_protocol.rs`.

## Quick start (full flow)

Same as [`examples/websocket.rs`](examples/websocket.rs):

```rust
use fastpubsub_sdk::client::{open, FastPubSub};
use fastpubsub_sdk::transport::WebSocketTransport;
use fastpubsub_sdk::{CreateAccessTokenBuilder, TenantGrant};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let master_token = std::env::var("FPS_MASTER_TOKEN")?;

    let mut session = open("overleynetworkname");
    let edge = session.resolve_edge().await?;

    let at_token = CreateAccessTokenBuilder::new()
        .created_by("my-app")
        .expires_at("2026-05-16T23:59:59Z")
        .tenant_grant(
            TenantGrant::new(vec!["tenant_1".into()])
                .allow_pub(vec!["public.#".into()])
                .allow_sub(vec!["public.#".into()]),
        )
        .create_with_config(session.api_base()?, &master_token, session.http_config())
        .await?;

    let mut client: FastPubSub<WebSocketTransport> = session
        .web_socket(at_token)?
        .build()
        .await?;

    client
        .publish("tenant_1", "public.demo", b"hello")
        .await?;

    Ok(())
}
```

Run:

```bash
export FPS_MASTER_TOKEN=your_master_secret
cargo run --release --features examples --example websocket
```

## Handshake

On connect to `wss://…/ws`:

- header `Authorization: Bearer {AT_token}`
- `Sec-WebSocket-Protocol: llps.v1,at.{AT_token}`

Get the `AT_{id}_{secret}` token via REST on the **selected** edge (`session.api_base()`).

## Publish

```rust
client
    .publish("tenant", "channel.name", payload)
    .await?;
```

Success means the SDK accepted publish and passed it to the transport. SDK does not generate `message_id`.

If explicit settings are needed, use `publish_with_options(..., &PublishOptions::default())`.

Outbound **binary** frame: `u16 tenant | u16 channel | payload`. Filters run in the WS task (outbound: reverse order).

`PublishOptions::delivery` (`Broadcast`, `DeliverOneLowLatency`, `DeliverOneRandom`) is **not** on the wire yet.

## Subscribe

```rust
let mut rx = client
    .subscribe("tenant", "public.#", &SubscribeOptions::default())
    .await?;

while let Some(bytes) = rx.recv().await {
    // handle payload
}

client.unsubscribe("tenant", "public.#").await?;
```

Wire: text `SUB:tenant:pattern` / `UNSUB:tenant:pattern`.

Inbound delivery: binary tag `0x01`, `matched_patterns` from edge; SDK routes to queues by `tenant:pattern` (no local pattern match).

Perimeter text:

- `ERR:…` → optional `on_websocket_event` as `WebSocketEvent::Error(WebSocketError::Server { .. })`
- `OK:SUB:tenant:pattern` → optional `WebSocketEvent::SubscriptionConfirmed { .. }`
- `OK:UNSUB:tenant:pattern` → optional `WebSocketEvent::UnsubscriptionConfirmed { .. }`

`SUB` / `UNSUB` use **one** pattern per command. Several subscriptions mean several commands. Comma in pattern is not supported.

## Filters

```rust
session
    .web_socket(at_token)?
    .add_filter(Box::new(MyFilter))
    .on_websocket_event(event_tx)
    .build()
    .await?;
```

Inbound: add order. Outbound: reverse order.

## Squid proxy and corporate CA

Same `HttpClientConfig` as REST:

```rust
let session = open("overlay")
    .with_proxy("http://127.0.0.1:3128")
    .with_proxy_auth("user", "pass")
    .with_ca_file("/etc/ssl/corp-root.pem");
```

For `wss://`:

1. **CONNECT** to proxy with `Proxy-Authorization: Basic …` (`transport/ws_proxy.rs`)
2. TLS to edge with trust store (webpki + corp CA)
3. WebSocket upgrade

Environment (see also [README.md](README.md)):

```bash
export HTTP_PROXY=http://127.0.0.1:3128
export HTTP_PROXY_USER=squid_user
export HTTP_PROXY_PASSWORD=secret
export FPS_EXTRA_CA_CERT=/path/to/corp-ca.pem
```

Without proxy and extra CA, the client uses normal `connect_async` (public roots).

## Direct connect (no discovery)

```rust
use fastpubsub_sdk::client::create_web_socket;

let mut client = create_web_socket("overlaynetworkname", "AT_…")
    .build()
    .await?;
```

Without `.endpoint()`: stub `wss://{overlay}.fastpubsub.com/ws`.

## Ping edges before WebSocket

`resolve_edge()` / `select_nearest_edge()` run `GET /ping` on each candidate in parallel and pick the lowest RTT (`ping_fastest`).

`ping_many` keeps up to 3 earliest responses (diagnostics only).

## Lifecycle

- No separate `close()`: on `drop`, the client sends `Shutdown` and the WS task stops.
- On `RECONNECT`, the SDK opens a second WebSocket before closing the old one, sends all active `SUB` commands again, and switches after `OK:SUB` for the stored subscriptions.
- During this overlap the SDK drops duplicate inbound frames by `XXH32x2(tenant, channel, payload)`. The cache is short-lived and only active while two WebSockets are alive.
- Without a message id this is best-effort: two different messages with the same tenant, channel and payload inside the overlap window can be treated as one.

## See also

- REST ping / get-token: `client/api/`
- Open items: `todo`
