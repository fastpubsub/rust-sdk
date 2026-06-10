// Copyright (c) 2026 FastPubSub Network
// SPDX-License-Identifier: MIT OR Apache-2.0
// Project: https://fastpubsub.com

//! WebSocket through HTTP proxy (Squid): CONNECT + `Proxy-Authorization`.

use futures_util::StreamExt;
use http::header::{HeaderValue, SEC_WEBSOCKET_PROTOCOL};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_tungstenite::{
    client_async,
    tungstenite::{client::IntoClientRequest, Message},
    MaybeTlsStream,
};

use crate::client::tls_roots;
use crate::client::{HttpClientConfig, ParsedProxy};

use super::TransportError;

/// Opens WSS over HTTP CONNECT through a proxy with user/password.
type WsStream = tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>;

/// WSS through Squid CONNECT + TLS. It uses the same stream type as direct [`connect_async`].
pub async fn connect_wss_via_proxy(
    endpoint: &str,
    access_token: &str,
    proxy: &ParsedProxy,
    http_config: &HttpClientConfig,
) -> Result<
    (
        futures_util::stream::SplitSink<WsStream, Message>,
        futures_util::stream::SplitStream<WsStream>,
    ),
    TransportError,
> {
    let target = url::Url::parse(endpoint).map_err(|e| TransportError::Other(e.to_string()))?;
    let use_tls = target.scheme() == "wss" || target.scheme() == "https";
    if !use_tls {
        return Err(TransportError::Other(
            "only wss:// is supported through proxy".into(),
        ));
    }
    let host = target
        .host_str()
        .ok_or_else(|| TransportError::Other("endpoint has no host".into()))?
        .to_string();
    let port = target.port().unwrap_or(443);

    let mut tcp = TcpStream::connect((proxy.host.as_str(), proxy.port))
        .await
        .map_err(|e| TransportError::Other(format!("proxy TCP: {e}")))?;

    let connect_target = format!("{host}:{port}");
    let mut req = format!(
        "CONNECT {connect_target} HTTP/1.1\r\n\
         Host: {connect_target}\r\n"
    );
    if let Some(auth) = proxy.proxy_authorization_header() {
        req.push_str(&format!("Proxy-Authorization: {auth}\r\n"));
    }
    req.push_str("\r\n");

    tcp.write_all(req.as_bytes())
        .await
        .map_err(|e| TransportError::Other(format!("CONNECT write: {e}")))?;
    tcp.flush()
        .await
        .map_err(|e| TransportError::Other(format!("CONNECT flush: {e}")))?;

    let status = read_connect_status(&mut tcp).await?;
    if status != 200 {
        return Err(TransportError::Other(format!(
            "proxy CONNECT answered {status}"
        )));
    }

    let connector = tls_roots::build_tls_connector(http_config)
        .map_err(|e| TransportError::Other(e.to_string()))?;
    let server_name = ServerName::try_from(host.clone())
        .map_err(|e| TransportError::Other(format!("TLS name: {e}")))?;
    let tls_stream = connector
        .connect(server_name, tcp)
        .await
        .map_err(|e| TransportError::Other(format!("TLS: {e}")))?;
    let io = MaybeTlsStream::Rustls(tls_stream);

    let mut request = endpoint
        .into_client_request()
        .map_err(|e| TransportError::Other(e.to_string()))?;
    apply_ws_headers(&mut request, access_token)?;

    let (stream, _) = client_async(request, io)
        .await
        .map_err(|e| TransportError::Other(e.to_string()))?;
    Ok(stream.split())
}

fn apply_ws_headers(
    request: &mut http::Request<()>,
    access_token: &str,
) -> Result<(), TransportError> {
    let proto = HeaderValue::from_str(&format!("llps.v1,at.{access_token}"))
        .map_err(|e| TransportError::Other(e.to_string()))?;
    request.headers_mut().insert(SEC_WEBSOCKET_PROTOCOL, proto);
    Ok(())
}

async fn read_connect_status(stream: &mut TcpStream) -> Result<u16, TransportError> {
    let mut buf = Vec::with_capacity(512);
    let mut chunk = [0u8; 256];
    loop {
        let n = stream
            .read(&mut chunk)
            .await
            .map_err(|e| TransportError::Other(format!("CONNECT read: {e}")))?;
        if n == 0 {
            return Err(TransportError::Other("proxy closed CONNECT".into()));
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
        if buf.len() > 8192 {
            return Err(TransportError::Other("proxy response is too large".into()));
        }
    }
    let text = String::from_utf8_lossy(&buf);
    let line = text.lines().next().unwrap_or("");
    let status = line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    Ok(status)
}
