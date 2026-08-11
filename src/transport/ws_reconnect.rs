// Copyright (c) 2026 FastPubSub Network
// SPDX-License-Identifier: MIT OR Apache-2.0
// Project: https://fastpubsub.com

//! State machine for smooth WebSocket reconnect: active -> candidate -> overlap -> active.

use std::collections::HashSet;
use std::time::Duration;

use bytes::Bytes;
use dashmap::DashMap;
use futures_util::SinkExt;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use crate::client::HttpClientConfig;
use crate::filters::{FilterRegistration, FilterSendMessage};
use crate::metadata::MetaMode;

use super::overlap_dedup::OverlapDedup;
use super::route_pattern::make_route_subscription_key;
use super::ws_connection::{close_connection, open_connection, WsConnection, WsTaskEvent};
use super::ws_delivery::{
    handle_inbound_binary, handle_publish, handle_publish_until, InboundDeliveryResult,
};
use super::ws_protocol::{is_pong_line, parse_err_line, parse_sub_ack_line, WS_PING_LINE, SubWireAck};
use super::link_quality::LinkQuality;
use super::ws_subscriptions::{send_subscribe_line, SubscriptionEntry};
use super::{PublishOptions, WebSocketError, WebSocketEvent};

const OVERLAP_GRACE_SECS: u64 = 5;

pub(super) struct WsTaskState {
    pub(super) active: Option<WsConnection>,
    pub(super) candidate: Option<WsConnection>,
    pub(super) old: Option<WsConnection>,
    connecting: bool,
    retry_scheduled: bool,
    next_id: u64,
    pub(super) pending_candidate_acks: HashSet<String>,
    overlap_dedup: OverlapDedup,
    pub(super) link_quality: LinkQuality,
}

pub(super) struct WsTaskContext<'a> {
    pub(super) endpoint: &'a str,
    pub(super) access_token: &'a str,
    pub(super) http_config: Option<&'a HttpClientConfig>,
    pub(super) filters: &'a [FilterRegistration],
    pub(super) meta_mode: MetaMode,
    pub(super) subscriptions: &'a DashMap<String, SubscriptionEntry>,
    pub(super) events: &'a Option<mpsc::Sender<WebSocketEvent>>,
    pub(super) event_tx: &'a mpsc::Sender<WsTaskEvent>,
}

impl WsTaskState {
    /// Creates the state machine with the first active connection.
    pub(super) fn new(active: WsConnection) -> Self {
        Self {
            active: Some(active),
            candidate: None,
            old: None,
            connecting: false,
            retry_scheduled: false,
            next_id: 2,
            pending_candidate_acks: HashSet::new(),
            overlap_dedup: OverlapDedup::new(Duration::from_secs(6)),
            link_quality: LinkQuality::default(),
        }
    }

    /// Sends WS `PING` if no ping is in flight.
    pub(super) async fn maybe_send_ping(&mut self) -> Result<(), WebSocketError> {
        if !self.link_quality.begin_ping() {
            self.link_quality.expire_pending(Duration::from_secs(10));
            return Ok(());
        }
        let Some(active) = self.active.as_mut() else {
            return Ok(());
        };
        active
            .write
            .send(Message::Text(WS_PING_LINE.into()))
            .await
            .map_err(|e| WebSocketError::Transport {
                operation: "ping".into(),
                message: e.to_string(),
            })
    }

    /// Publishes a message only through the active connection.
    pub(super) async fn publish(
        &mut self,
        filters: &[FilterRegistration],
        tenant: &str,
        channel: &str,
        payload: Bytes,
        options: &PublishOptions,
    ) -> Result<(), WebSocketError> {
        let Some(active) = self.active.as_mut() else {
            return Err(WebSocketError::Transport {
                operation: "publish".into(),
                message: "transport is not connected".into(),
            });
        };
        handle_publish(filters, &mut active.write, tenant, channel, payload, options)
            .await
            .map_err(|e| WebSocketError::Transport {
                operation: "publish".into(),
                message: e.to_string(),
            })
    }

    /// Sends payload through the outbound pipeline up to the selected filter.
    pub(super) async fn publish_until_filter_index(
        &mut self,
        filters: &[FilterRegistration],
        end_index: usize,
        tenant: &str,
        channel: &str,
        payload: Vec<u8>,
        options: &PublishOptions,
    ) -> Result<(), WebSocketError> {
        let Some(active) = self.active.as_mut() else {
            return Err(WebSocketError::Transport {
                operation: "publish_until_filter_index".into(),
                message: "transport is not connected".into(),
            });
        };
        handle_publish_until(
            filters,
            end_index,
            &mut active.write,
            tenant,
            channel,
            payload,
            options,
        )
        .await
        .map_err(|e| WebSocketError::Transport {
            operation: "publish_until_filter_index".into(),
            message: e.to_string(),
        })
    }

    /// Handles an event from reader tasks and reconnect timers.
    pub(super) async fn handle_event(&mut self, event: WsTaskEvent, ctx: WsTaskContext<'_>) {
        match event {
            WsTaskEvent::Connected { id, result } => {
                self.connecting = false;
                match result {
                    Ok(mut conn) => {
                        self.pending_candidate_acks.clear();
                        for entry in ctx.subscriptions.iter() {
                            self.pending_candidate_acks.insert(entry.key().clone());
                            if let Err(e) = send_subscribe_line(
                                &mut conn.write,
                                &entry.value().tenant,
                                &entry.value().channel_pattern,
                            )
                            .await
                            {
                                notify_error(
                                    ctx.events,
                                    WebSocketError::Transport {
                                        operation: "resubscribe".into(),
                                        message: e.to_string(),
                                    },
                                );
                            }
                        }
                        self.candidate = Some(conn);
                        if self.pending_candidate_acks.is_empty() {
                            self.promote_candidate(ctx.event_tx, ctx.events).await;
                        }
                    }
                    Err(error) => {
                        notify_error(
                            ctx.events,
                            WebSocketError::Reconnect {
                                connection_id: id,
                                message: error,
                            },
                        );
                        self.schedule_reconnect_retry(ctx.event_tx.clone());
                    }
                }
            }
            WsTaskEvent::ReconnectRetry => {
                self.retry_scheduled = false;
                if self.candidate.is_none() {
                    self.start_candidate_connect(
                        ctx.endpoint,
                        ctx.access_token,
                        ctx.http_config,
                        ctx.event_tx.clone(),
                        ctx.events,
                    );
                }
            }
            WsTaskEvent::Message { id, message } => {
                self.handle_message(id, message, ctx).await;
            }
            WsTaskEvent::ReadError { id, error } => {
                notify_error(
                    ctx.events,
                    WebSocketError::Read {
                        connection_id: id,
                        message: error,
                    },
                );
                self.handle_connection_closed(id, &ctx).await;
            }
            WsTaskEvent::Closed { id } => {
                self.handle_connection_closed(id, &ctx).await;
            }
            WsTaskEvent::OverlapExpired { old_id } => {
                if self.old.as_ref().is_some_and(|c| c.id == old_id) {
                    close_connection(self.old.take()).await;
                    self.overlap_dedup.disable();
                }
            }
        }
    }

    /// Closes all live connections.
    pub(super) async fn shutdown(&mut self) {
        close_connection(self.active.take()).await;
        close_connection(self.candidate.take()).await;
        close_connection(self.old.take()).await;
    }

    fn connection_mut(&mut self, id: u64) -> Option<&mut WsConnection> {
        if self.active.as_ref().is_some_and(|c| c.id == id) {
            return self.active.as_mut();
        }
        if self.candidate.as_ref().is_some_and(|c| c.id == id) {
            return self.candidate.as_mut();
        }
        if self.old.as_ref().is_some_and(|c| c.id == id) {
            return self.old.as_mut();
        }
        None
    }

    fn is_candidate(&self, id: u64) -> bool {
        self.candidate.as_ref().is_some_and(|c| c.id == id)
    }

    fn is_active(&self, id: u64) -> bool {
        self.active.as_ref().is_some_and(|c| c.id == id)
    }

    fn take_connection(&mut self, id: u64) -> Option<WsConnection> {
        if self.active.as_ref().is_some_and(|c| c.id == id) {
            return self.active.take();
        }
        if self.candidate.as_ref().is_some_and(|c| c.id == id) {
            return self.candidate.take();
        }
        if self.old.as_ref().is_some_and(|c| c.id == id) {
            return self.old.take();
        }
        None
    }

    fn start_candidate_connect(
        &mut self,
        endpoint: &str,
        access_token: &str,
        http_config: Option<&HttpClientConfig>,
        event_tx: mpsc::Sender<WsTaskEvent>,
        events: &Option<mpsc::Sender<WebSocketEvent>>,
    ) {
        if self.connecting || self.retry_scheduled || self.candidate.is_some() {
            return;
        }
        notify_event(events, WebSocketEvent::ReconnectStarted);
        let id = self.next_id;
        self.next_id += 1;
        self.connecting = true;
        let endpoint = endpoint.to_string();
        let access_token = access_token.to_string();
        let http_config = http_config.cloned();
        tokio::spawn(async move {
            let result = open_connection(
                id,
                &endpoint,
                &access_token,
                http_config.as_ref(),
                event_tx.clone(),
            )
            .await
            .map_err(|e| e.to_string());
            let _ = event_tx.send(WsTaskEvent::Connected { id, result }).await;
        });
    }

    fn schedule_reconnect_retry(&mut self, event_tx: mpsc::Sender<WsTaskEvent>) {
        if self.retry_scheduled || self.connecting || self.candidate.is_some() {
            return;
        }
        self.retry_scheduled = true;
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let _ = event_tx.send(WsTaskEvent::ReconnectRetry).await;
        });
    }

    async fn handle_message(&mut self, id: u64, message: Message, ctx: WsTaskContext<'_>) {
        match message {
            Message::Binary(data) => {
                match handle_inbound_binary(
                    ctx.filters,
                    ctx.meta_mode,
                    ctx.subscriptions,
                    &mut self.overlap_dedup,
                    &data,
                )
                .await
                {
                    Ok(InboundDeliveryResult::Delivered { messages, notices }) => {
                        notify_filter_notices(ctx.events, notices);
                        self.publish_filter_messages(ctx.filters, messages, ctx.events)
                            .await;
                    }
                    Ok(InboundDeliveryResult::DuplicateDropped { messages, notices }) => {
                        notify_event(ctx.events, WebSocketEvent::DuplicateDropped);
                        notify_filter_notices(ctx.events, notices);
                        self.publish_filter_messages(ctx.filters, messages, ctx.events)
                            .await;
                    }
                    Err(e) => notify_error(ctx.events, e),
                }
            }
            Message::Text(text) => {
                self.handle_text(id, &text, ctx).await;
            }
            Message::Ping(payload) => {
                if let Some(conn) = self.connection_mut(id) {
                    let _ = conn.write.send(Message::Pong(payload)).await;
                }
            }
            Message::Close(_) => {
                self.handle_connection_closed(id, &ctx).await;
            }
            _ => {}
        }
    }

    async fn handle_text(&mut self, id: u64, text: &str, ctx: WsTaskContext<'_>) {
        if is_pong_line(text) {
            if let Some(rtt_ms) = self.link_quality.record_pong() {
                notify_event(
                    ctx.events,
                    WebSocketEvent::RttMeasured { rtt_ms },
                );
            }
            return;
        }

        if text == "RECONNECT" {
            if self.is_active(id) {
                self.start_candidate_connect(
                    ctx.endpoint,
                    ctx.access_token,
                    ctx.http_config,
                    ctx.event_tx.clone(),
                    ctx.events,
                );
            }
            return;
        }

        if let Some(ack) = parse_sub_ack_line(text) {
            if self.is_candidate(id) {
                if let SubWireAck::Sub {
                    tenant,
                    channel_pattern,
                } = &ack
                {
                    let key = make_route_subscription_key(tenant, channel_pattern);
                    self.pending_candidate_acks.remove(&key);
                    if self.pending_candidate_acks.is_empty() {
                        self.promote_candidate(ctx.event_tx, ctx.events).await;
                    }
                }
            }
            notify_sub_ack(ctx.events, ack);
        } else if let Some(err_body) = parse_err_line(text) {
            notify_error(
                ctx.events,
                WebSocketError::Server {
                    message: err_body.to_string(),
                },
            );
        }
    }

    async fn publish_filter_messages(
        &mut self,
        filters: &[FilterRegistration],
        messages: Vec<FilterSendMessage>,
        events: &Option<mpsc::Sender<WebSocketEvent>>,
    ) {
        for message in messages {
            let FilterSendMessage {
                tenant,
                channel,
                payload,
            } = message;
            if let Err(error) = self
                .publish_until_filter_index(
                    filters,
                    filters.len(),
                    &tenant,
                    &channel,
                    payload,
                    &PublishOptions::default(),
                )
                .await
            {
                notify_error(events, error);
            }
        }
    }

    async fn handle_connection_closed(&mut self, id: u64, ctx: &WsTaskContext<'_>) {
        let was_active = self.is_active(id);
        close_connection(self.take_connection(id)).await;
        if was_active {
            self.start_candidate_connect(
                ctx.endpoint,
                ctx.access_token,
                ctx.http_config,
                ctx.event_tx.clone(),
                ctx.events,
            );
        }
        if self.old.is_none() && self.candidate.is_none() {
            self.overlap_dedup.disable();
        }
    }

    async fn promote_candidate(
        &mut self,
        event_tx: &mpsc::Sender<WsTaskEvent>,
        events: &Option<mpsc::Sender<WebSocketEvent>>,
    ) {
        let Some(candidate) = self.candidate.take() else {
            return;
        };
        close_connection(self.old.take()).await;
        self.old = self.active.take();
        let old_id = self.old.as_ref().map(|c| c.id);
        self.active = Some(candidate);
        self.overlap_dedup
            .enable(Duration::from_secs(OVERLAP_GRACE_SECS + 1));
        notify_event(events, WebSocketEvent::ReconnectReady);
        if let Some(old_id) = old_id {
            let event_tx = event_tx.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(OVERLAP_GRACE_SECS)).await;
                let _ = event_tx.send(WsTaskEvent::OverlapExpired { old_id }).await;
            });
        }
    }
}

fn notify_error(tx: &Option<mpsc::Sender<WebSocketEvent>>, error: WebSocketError) {
    notify_event(tx, WebSocketEvent::Error(error));
}

fn notify_filter_notices(
    tx: &Option<mpsc::Sender<WebSocketEvent>>,
    notices: Vec<crate::filters::FilterNotice>,
) {
    for notice in notices {
        let level = match notice.level {
            crate::filters::FilterNoticeLevel::Info => "info",
            crate::filters::FilterNoticeLevel::Warning => "warning",
        };
        notify_event(
            tx,
            WebSocketEvent::FilterNotice {
                level,
                message: notice.message,
            },
        );
    }
}

fn notify_sub_ack(tx: &Option<mpsc::Sender<WebSocketEvent>>, ack: SubWireAck) {
    match ack {
        SubWireAck::Sub {
            tenant,
            channel_pattern,
        } => notify_event(
            tx,
            WebSocketEvent::SubscriptionConfirmed {
                tenant,
                channel_pattern,
            },
        ),
        SubWireAck::Unsub {
            tenant,
            channel_pattern,
        } => notify_event(
            tx,
            WebSocketEvent::UnsubscriptionConfirmed {
                tenant,
                channel_pattern,
            },
        ),
    }
}

fn notify_event(tx: &Option<mpsc::Sender<WebSocketEvent>>, event: WebSocketEvent) {
    if let Some(tx) = tx {
        let _ = tx.try_send(event);
    }
}
