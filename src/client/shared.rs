// Copyright (c) 2026 FastPubSub Network
// SPDX-License-Identifier: MIT OR Apache-2.0
// Project: https://fastpubsub.com

//! Shared client handle for modules that send commands from different tasks.

use std::marker::PhantomData;

use tokio::sync::{mpsc, oneshot};

use crate::metadata::SdkMessage;
use crate::transport::{PublishOptions, SubscribeOptions, Transport, TransportError};

use super::{FastPubSub, PublishError};

/// Cloneable handle to one `FastPubSub` owned by a separate Tokio task.
pub struct SharedFastPubSub<T: Transport + 'static> {
    cmd_tx: mpsc::Sender<SharedCommand>,
    overlay: String,
    api_base: String,
    _transport: PhantomData<T>,
}

impl<T: Transport + 'static> Clone for SharedFastPubSub<T> {
    fn clone(&self) -> Self {
        Self {
            cmd_tx: self.cmd_tx.clone(),
            overlay: self.overlay.clone(),
            api_base: self.api_base.clone(),
            _transport: PhantomData,
        }
    }
}

enum SharedCommand {
    Publish {
        tenant: String,
        channel: String,
        payload: Vec<u8>,
        options: PublishOptions,
        reply: oneshot::Sender<Result<(), PublishError>>,
    },
    Subscribe {
        tenant: String,
        channel_pattern: String,
        options: SubscribeOptions,
        reply: oneshot::Sender<Result<mpsc::Receiver<SdkMessage>, TransportError>>,
    },
    Unsubscribe {
        tenant: String,
        channel_pattern: String,
        reply: oneshot::Sender<Result<(), TransportError>>,
    },
}

impl<T: Transport + 'static> SharedFastPubSub<T> {
    /// Creates a shared handle and starts the dispatcher task.
    pub(super) fn new(client: FastPubSub<T>) -> Self {
        let overlay = client.overlay.clone();
        let api_base = client.api_base.clone();
        let (cmd_tx, cmd_rx) = mpsc::channel(256);
        tokio::spawn(run_dispatcher(client, cmd_rx));
        Self {
            cmd_tx,
            overlay,
            api_base,
            _transport: PhantomData,
        }
    }

    /// Overlay network name from the source client.
    pub fn overlay_network_name(&self) -> &str {
        &self.overlay
    }

    /// REST API base URL from the source client.
    pub fn api_base(&self) -> &str {
        &self.api_base
    }

    /// Publishes a payload through the shared dispatcher with default settings.
    pub async fn publish(
        &self,
        tenant: &str,
        channel: &str,
        payload: &[u8],
    ) -> Result<(), PublishError> {
        self.publish_with_options(tenant, channel, payload, &PublishOptions::default())
            .await
    }

    /// Publishes a payload through the shared dispatcher with explicit settings.
    pub async fn publish_with_options(
        &self,
        tenant: &str,
        channel: &str,
        payload: &[u8],
        options: &PublishOptions,
    ) -> Result<(), PublishError> {
        let (reply, result) = oneshot::channel();
        self.cmd_tx
            .send(SharedCommand::Publish {
                tenant: tenant.to_string(),
                channel: channel.to_string(),
                payload: payload.to_vec(),
                options: options.clone(),
                reply,
            })
            .await
            .map_err(|_| PublishError::Transport(TransportError::NotConnected))?;
        result
            .await
            .map_err(|_| PublishError::Transport(TransportError::NotConnected))?
    }

    /// Creates a subscription through the shared dispatcher.
    pub async fn subscribe(
        &self,
        tenant: &str,
        channel_pattern: &str,
        options: &SubscribeOptions,
    ) -> Result<mpsc::Receiver<SdkMessage>, TransportError> {
        let (reply, result) = oneshot::channel();
        self.cmd_tx
            .send(SharedCommand::Subscribe {
                tenant: tenant.to_string(),
                channel_pattern: channel_pattern.to_string(),
                options: options.clone(),
                reply,
            })
            .await
            .map_err(|_| TransportError::NotConnected)?;
        result.await.map_err(|_| TransportError::NotConnected)?
    }

    /// Removes a subscription through the shared dispatcher.
    pub async fn unsubscribe(
        &self,
        tenant: &str,
        channel_pattern: &str,
    ) -> Result<(), TransportError> {
        let (reply, result) = oneshot::channel();
        self.cmd_tx
            .send(SharedCommand::Unsubscribe {
                tenant: tenant.to_string(),
                channel_pattern: channel_pattern.to_string(),
                reply,
            })
            .await
            .map_err(|_| TransportError::NotConnected)?;
        result.await.map_err(|_| TransportError::NotConnected)?
    }
}

/// Handles shared handle commands one by one.
async fn run_dispatcher<T: Transport + 'static>(
    mut client: FastPubSub<T>,
    mut cmd_rx: mpsc::Receiver<SharedCommand>,
) {
    while let Some(command) = cmd_rx.recv().await {
        match command {
            SharedCommand::Publish {
                tenant,
                channel,
                payload,
                options,
                reply,
            } => {
                let result = client
                    .publish_with_options(&tenant, &channel, &payload, &options)
                    .await;
                let _ = reply.send(result);
            }
            SharedCommand::Subscribe {
                tenant,
                channel_pattern,
                options,
                reply,
            } => {
                let result = client.subscribe(&tenant, &channel_pattern, &options).await;
                let _ = reply.send(result);
            }
            SharedCommand::Unsubscribe {
                tenant,
                channel_pattern,
                reply,
            } => {
                let result = client.unsubscribe(&tenant, &channel_pattern).await;
                let _ = reply.send(result);
            }
        }
    }
}
