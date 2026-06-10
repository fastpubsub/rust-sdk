// Copyright (c) 2026 FastPubSub Network
// SPDX-License-Identifier: MIT OR Apache-2.0
// Project: https://fastpubsub.com

//! Local WebSocket transport subscriptions and SUB/UNSUB sending to live connections.

use dashmap::DashMap;
use futures_util::SinkExt;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use crate::metadata::SdkMessage;

use super::route_pattern::{make_route_subscription_key, validate_channel_pattern};
use super::ws_protocol::{format_subscribe, format_unsubscribe};
use super::ws_reconnect::WsTaskState;
use super::TransportError;

#[derive(Clone)]
pub(super) struct SubscriptionEntry {
    pub(super) tenant: String,
    pub(super) channel_pattern: String,
    pub(super) sender: mpsc::Sender<SdkMessage>,
}

pub(super) async fn handle_subscribe(
    subscriptions: &DashMap<String, SubscriptionEntry>,
    state: &mut WsTaskState,
    tenant: &str,
    channel_pattern: &str,
    queue_capacity: usize,
) -> Result<mpsc::Receiver<SdkMessage>, TransportError> {
    validate_channel_pattern(channel_pattern)?;
    if queue_capacity == 0 {
        return Err(TransportError::Other(
            "subscription queue capacity cannot be 0".into(),
        ));
    }
    let key = make_route_subscription_key(tenant, channel_pattern);
    if subscriptions.contains_key(&key) {
        return Err(TransportError::DuplicateSubscription);
    }
    let (tx, rx) = mpsc::channel(queue_capacity);
    subscriptions.insert(
        key.clone(),
        SubscriptionEntry {
            tenant: tenant.to_string(),
            channel_pattern: channel_pattern.to_string(),
            sender: tx,
        },
    );

    if let Err(e) = send_subscribe_to_all(state, tenant, channel_pattern).await {
        let _ = subscriptions.remove(&key);
        return Err(e);
    }
    if state.candidate.is_some() {
        state.pending_candidate_acks.insert(key);
    }
    Ok(rx)
}

pub(super) async fn handle_unsubscribe(
    subscriptions: &DashMap<String, SubscriptionEntry>,
    state: &mut WsTaskState,
    tenant: &str,
    channel_pattern: &str,
) -> Result<(), TransportError> {
    let key = make_route_subscription_key(tenant, channel_pattern);
    let _ = subscriptions.remove(&key);
    send_unsubscribe_to_all(state, tenant, channel_pattern).await
}

pub(super) async fn send_subscribe_line(
    ws_write: &mut (impl SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin),
    tenant: &str,
    channel_pattern: &str,
) -> Result<(), TransportError> {
    let line = format_subscribe(tenant, channel_pattern);
    ws_write
        .send(Message::Text(line.into()))
        .await
        .map_err(|e| TransportError::Other(e.to_string()))?;
    Ok(())
}

async fn send_unsubscribe_line(
    ws_write: &mut (impl SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin),
    tenant: &str,
    channel_pattern: &str,
) -> Result<(), TransportError> {
    let line = format_unsubscribe(tenant, channel_pattern);
    ws_write
        .send(Message::Text(line.into()))
        .await
        .map_err(|e| TransportError::Other(e.to_string()))?;
    Ok(())
}

async fn send_subscribe_to_all(
    state: &mut WsTaskState,
    tenant: &str,
    channel_pattern: &str,
) -> Result<(), TransportError> {
    let mut sent_any = false;
    if let Some(conn) = state.active.as_mut() {
        send_subscribe_line(&mut conn.write, tenant, channel_pattern).await?;
        sent_any = true;
    }
    if let Some(conn) = state.candidate.as_mut() {
        send_subscribe_line(&mut conn.write, tenant, channel_pattern).await?;
        sent_any = true;
    }
    if let Some(conn) = state.old.as_mut() {
        send_subscribe_line(&mut conn.write, tenant, channel_pattern).await?;
        sent_any = true;
    }
    if sent_any {
        Ok(())
    } else {
        Err(TransportError::NotConnected)
    }
}

async fn send_unsubscribe_to_all(
    state: &mut WsTaskState,
    tenant: &str,
    channel_pattern: &str,
) -> Result<(), TransportError> {
    let mut sent_any = false;
    if let Some(conn) = state.active.as_mut() {
        send_unsubscribe_line(&mut conn.write, tenant, channel_pattern).await?;
        sent_any = true;
    }
    if let Some(conn) = state.candidate.as_mut() {
        send_unsubscribe_line(&mut conn.write, tenant, channel_pattern).await?;
        sent_any = true;
    }
    if let Some(conn) = state.old.as_mut() {
        send_unsubscribe_line(&mut conn.write, tenant, channel_pattern).await?;
        sent_any = true;
    }
    if sent_any {
        Ok(())
    } else {
        Err(TransportError::NotConnected)
    }
}
