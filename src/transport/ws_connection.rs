// Copyright (c) 2026 FastPubSub Network
// SPDX-License-Identifier: MIT OR Apache-2.0
// Project: https://fastpubsub.com

//! One physical WebSocket connection: connect, reader task, and close.

use futures_util::{SinkExt, StreamExt};
use http::header::{HeaderValue, SEC_WEBSOCKET_PROTOCOL};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::{
    connect_async, connect_async_tls_with_config,
    tungstenite::{client::IntoClientRequest, Message},
    Connector,
};

use crate::client::tls_roots;
use crate::client::HttpClientConfig;

use super::TransportError;

pub(super) type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

pub(super) struct WsConnection {
    pub(super) id: u64,
    pub(super) write: futures_util::stream::SplitSink<WsStream, Message>,
    pub(super) read_task: JoinHandle<()>,
}

pub(super) enum WsTaskEvent {
    Connected {
        id: u64,
        result: Result<WsConnection, String>,
    },
    Message {
        id: u64,
        message: Message,
    },
    ReadError {
        id: u64,
        error: String,
    },
    Closed {
        id: u64,
    },
    OverlapExpired {
        old_id: u64,
    },
    ReconnectRetry,
}

pub(super) async fn open_connection(
    id: u64,
    endpoint: &str,
    access_token: &str,
    http_config: Option<&HttpClientConfig>,
    event_tx: mpsc::Sender<WsTaskEvent>,
) -> Result<WsConnection, TransportError> {
    let (write, read) = connect_ws(endpoint, access_token, http_config).await?;
    let read_task = spawn_reader(id, read, event_tx);
    Ok(WsConnection {
        id,
        write,
        read_task,
    })
}

pub(super) async fn close_connection(conn: Option<WsConnection>) {
    if let Some(mut conn) = conn {
        conn.read_task.abort();
        let _ = conn.write.send(Message::Close(None)).await;
    }
}

fn spawn_reader(
    id: u64,
    mut read: futures_util::stream::SplitStream<WsStream>,
    event_tx: mpsc::Sender<WsTaskEvent>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(message) = read.next().await {
            match message {
                Ok(message) => {
                    let is_close = matches!(message, Message::Close(_));
                    let _ = event_tx.send(WsTaskEvent::Message { id, message }).await;
                    if is_close {
                        return;
                    }
                }
                Err(e) => {
                    let _ = event_tx
                        .send(WsTaskEvent::ReadError {
                            id,
                            error: e.to_string(),
                        })
                        .await;
                    return;
                }
            }
        }
        let _ = event_tx.send(WsTaskEvent::Closed { id }).await;
    })
}

async fn connect_ws(
    endpoint: &str,
    access_token: &str,
    http_config: Option<&HttpClientConfig>,
) -> Result<
    (
        futures_util::stream::SplitSink<WsStream, Message>,
        futures_util::stream::SplitStream<WsStream>,
    ),
    TransportError,
> {
    let mut request = endpoint
        .into_client_request()
        .map_err(|e| TransportError::Other(e.to_string()))?;
    let proto = HeaderValue::from_str(&format!("llps.v1,at.{access_token}"))
        .map_err(|e| TransportError::Other(e.to_string()))?;
    request.headers_mut().insert(SEC_WEBSOCKET_PROTOCOL, proto);

    if let Some(cfg) = http_config {
        if let Some(proxy) = cfg.parsed_wss_proxy() {
            return super::ws_proxy::connect_wss_via_proxy(endpoint, access_token, &proxy, cfg)
                .await;
        }
        if cfg.has_extra_ca() || !cfg.trust_webpki_roots {
            let tls = tls_roots::build_rustls_client_config(cfg)
                .map_err(|e| TransportError::Other(e.to_string()))?;
            let (stream, _) =
                connect_async_tls_with_config(request, None, false, Some(Connector::Rustls(tls)))
                    .await
                    .map_err(|e| TransportError::Other(e.to_string()))?;
            return Ok(stream.split());
        }
    }

    let (stream, _) = connect_async(request)
        .await
        .map_err(|e| TransportError::Other(e.to_string()))?;
    Ok(stream.split())
}
