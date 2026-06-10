// Copyright (c) 2026 FastPubSub Network
// SPDX-License-Identifier: MIT OR Apache-2.0
// Project: https://fastpubsub.com

use async_trait::async_trait;
use dashmap::DashMap;
use tokio::sync::mpsc;

use crate::metadata::SdkMessage;

use super::route_pattern::{make_route_subscription_key, validate_channel_pattern};
use super::SubscribeOptions;
use super::{
    Bytes, PublishOptions, Transport, TransportConnectParams, TransportError, TransportKind,
};

/// WebTransport transport stub.
///
/// Subscriptions are stored inside this type in a [`DashMap`] keyed by tenant+pattern, like WebSocket.
/// Resources are released in [`Drop`].
/// Contract v0.1 requires only WebSocket. WebTransport is optional / future.
pub struct WebTransport {
    endpoint: Option<String>,
    connected: bool,
    subscription_senders: DashMap<String, mpsc::Sender<SdkMessage>>,
}

impl WebTransport {
    /// Creates a transport in the not connected state.
    pub fn new() -> Self {
        Self {
            endpoint: None,
            connected: false,
            subscription_senders: DashMap::new(),
        }
    }

    /// Returns the endpoint after successful [`super::Transport::connect`], otherwise `None`.
    pub fn endpoint(&self) -> Option<&str> {
        self.endpoint.as_deref()
    }

    /// Sender for a subscription queue by pattern, for a future read loop.
    pub fn subscription_sender_for(
        &self,
        tenant: &str,
        channel_pattern: &str,
    ) -> Option<mpsc::Sender<SdkMessage>> {
        let key = make_route_subscription_key(tenant, channel_pattern);
        self.subscription_senders.get(&key).map(|e| e.clone())
    }
}

impl Default for WebTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for WebTransport {
    /// Clears the endpoint, connection flag, and all subscriptions.
    fn drop(&mut self) {
        self.connected = false;
        self.endpoint = None;
        self.subscription_senders.clear();
    }
}

#[async_trait]
impl Transport for WebTransport {
    fn kind(&self) -> TransportKind {
        TransportKind::WebTransport
    }

    async fn connect(
        &mut self,
        endpoint: &str,
        _params: TransportConnectParams,
    ) -> Result<(), TransportError> {
        self.endpoint = Some(endpoint.to_string());
        self.connected = true;
        Ok(())
    }

    async fn publish(
        &mut self,
        _tenant: &str,
        _channel: &str,
        _payload: Bytes,
        _options: &PublishOptions,
    ) -> Result<(), TransportError> {
        if !self.connected {
            return Err(TransportError::NotConnected);
        }
        Ok(())
    }

    async fn subscribe(
        &mut self,
        tenant: &str,
        channel_pattern: &str,
        options: &SubscribeOptions,
    ) -> Result<mpsc::Receiver<SdkMessage>, TransportError> {
        if !self.connected {
            return Err(TransportError::NotConnected);
        }
        validate_channel_pattern(channel_pattern)?;
        let capacity = options.queue_capacity;
        if capacity == 0 {
            return Err(TransportError::Other(
                "subscription queue capacity cannot be 0".into(),
            ));
        }
        let key = make_route_subscription_key(tenant, channel_pattern);
        if self.subscription_senders.contains_key(&key) {
            return Err(TransportError::DuplicateSubscription);
        }
        let (tx, rx) = mpsc::channel(capacity);
        self.subscription_senders.insert(key, tx);
        Ok(rx)
    }

    async fn unsubscribe(
        &mut self,
        tenant: &str,
        channel_pattern: &str,
    ) -> Result<(), TransportError> {
        if !self.connected {
            return Err(TransportError::NotConnected);
        }
        let key = make_route_subscription_key(tenant, channel_pattern);
        let _ = self.subscription_senders.remove(&key);
        Ok(())
    }
}
