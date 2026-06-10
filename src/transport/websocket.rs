// Copyright (c) 2026 FastPubSub Network
// SPDX-License-Identifier: MIT OR Apache-2.0
// Project: https://fastpubsub.com

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

use crate::metadata::SdkMessage;

use super::route_pattern::validate_channel_pattern;
use super::ws_task::{spawn_ws_task, WsCommand};
use super::{
    PublishOptions, SubscribeOptions, Transport, TransportConnectParams, TransportError,
    TransportKind,
};

/// WebSocket transport: one Tokio background task, commands over `mpsc`, and WS read/write.
pub struct WebSocketTransport {
    cmd_tx: Option<mpsc::Sender<WsCommand>>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl WebSocketTransport {
    /// Creates a transport. The connection is opened in [`Transport::connect`].
    pub fn new() -> Self {
        Self {
            cmd_tx: None,
            task: None,
        }
    }
}

impl Default for WebSocketTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for WebSocketTransport {
    fn drop(&mut self) {
        if let Some(tx) = self.cmd_tx.take() {
            let _ = tx.try_send(WsCommand::Shutdown);
        }
        if let Some(handle) = self.task.take() {
            handle.abort();
        }
    }
}

#[async_trait]
impl Transport for WebSocketTransport {
    fn kind(&self) -> TransportKind {
        TransportKind::WebSocket
    }

    async fn connect(
        &mut self,
        endpoint: &str,
        params: TransportConnectParams,
    ) -> Result<(), TransportError> {
        if self.cmd_tx.is_some() {
            return Err(TransportError::Other("already connected".into()));
        }

        let (cmd_tx, cmd_rx) = mpsc::channel(256);
        let (ready_tx, ready_rx) = oneshot::channel();

        let filters = Arc::new(params.filters);
        let task = spawn_ws_task(
            endpoint.to_string(),
            params.access_token,
            params.http_config,
            filters,
            params.filter_timer_mode,
            params.meta_mode,
            params.on_websocket_event,
            cmd_rx,
            ready_tx,
        );

        ready_rx
            .await
            .map_err(|_| TransportError::Other("ws task stopped before handshake".into()))??;

        self.cmd_tx = Some(cmd_tx);
        self.task = Some(task);
        Ok(())
    }

    async fn publish(
        &mut self,
        tenant: &str,
        channel: &str,
        payload: Bytes,
        options: &PublishOptions,
    ) -> Result<(), TransportError> {
        let tx = self.cmd_tx.as_ref().ok_or(TransportError::NotConnected)?;
        tx.send(WsCommand::Publish {
            tenant: tenant.to_string(),
            channel: channel.to_string(),
            payload,
            options: options.clone(),
        })
        .await
        .map_err(|_| TransportError::NotConnected)?;
        Ok(())
    }

    async fn subscribe(
        &mut self,
        tenant: &str,
        channel_pattern: &str,
        options: &SubscribeOptions,
    ) -> Result<mpsc::Receiver<SdkMessage>, TransportError> {
        validate_channel_pattern(channel_pattern)?;
        let tx = self.cmd_tx.as_ref().ok_or(TransportError::NotConnected)?;
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(WsCommand::Subscribe {
            tenant: tenant.to_string(),
            channel_pattern: channel_pattern.to_string(),
            queue_capacity: options.queue_capacity,
            reply: reply_tx,
        })
        .await
        .map_err(|_| TransportError::NotConnected)?;
        reply_rx
            .await
            .map_err(|_| TransportError::Other("ws task did not answer subscribe".into()))?
    }

    async fn unsubscribe(
        &mut self,
        tenant: &str,
        channel_pattern: &str,
    ) -> Result<(), TransportError> {
        let tx = self.cmd_tx.as_ref().ok_or(TransportError::NotConnected)?;
        tx.send(WsCommand::Unsubscribe {
            tenant: tenant.to_string(),
            channel_pattern: channel_pattern.to_string(),
        })
        .await
        .map_err(|_| TransportError::NotConnected)?;
        Ok(())
    }
}
