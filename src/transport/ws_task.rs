// Copyright (c) 2026 FastPubSub Network
// SPDX-License-Identifier: MIT OR Apache-2.0
// Project: https://fastpubsub.com

//! WebSocket background task: `mpsc` commands, WS read/write, filters, and routing to subscription queues.

use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{self, MissedTickBehavior};

use crate::client::HttpClientConfig;
use crate::filters::{
    FilterError, FilterRegistration, FilterSendMessage, FilterTimerContext, FilterTimerMode,
};
use crate::metadata::{MetaMode, SdkMessage};

use super::ws_connection::open_connection;
use super::ws_reconnect::{WsTaskContext, WsTaskState};
use super::ws_subscriptions::{handle_subscribe, handle_unsubscribe, SubscriptionEntry};
use super::{PublishOptions, TransportError, WebSocketError, WebSocketEvent};

struct TimerFilterMessage {
    end_index: usize,
    message: FilterSendMessage,
}

/// Command sent to the transport background task.
pub enum WsCommand {
    /// Publish. Outbound filters run in reverse configured order.
    Publish {
        tenant: String,
        channel: String,
        payload: Bytes,
        /// Not encoded into the frame yet. Reserved for delivery in the envelope.
        options: PublishOptions,
    },
    /// Subscribe: register a queue and send `SUB:` on the wire.
    Subscribe {
        tenant: String,
        channel_pattern: String,
        queue_capacity: usize,
        reply: oneshot::Sender<Result<mpsc::Receiver<SdkMessage>, TransportError>>,
    },
    /// Unsubscribe: remove from the map and send `UNSUB:` on the wire.
    Unsubscribe {
        tenant: String,
        channel_pattern: String,
    },
    /// Stop the task.
    Shutdown,
    /// Returns current link quality snapshot.
    GetLinkQuality {
        reply: oneshot::Sender<super::LinkQualitySnapshot>,
    },
}

/// Starts the task. `ready` reports handshake success or error.
pub fn spawn_ws_task(
    endpoint: String,
    access_token: String,
    http_config: Option<HttpClientConfig>,
    filters: Arc<Vec<FilterRegistration>>,
    filter_timer_mode: FilterTimerMode,
    meta_mode: MetaMode,
    events: Option<mpsc::Sender<WebSocketEvent>>,
    ping_interval_secs: Option<u8>,
    mut cmd_rx: mpsc::Receiver<WsCommand>,
    ready: oneshot::Sender<Result<(), TransportError>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let subscriptions: Arc<DashMap<String, SubscriptionEntry>> = Arc::new(DashMap::new());
        let (event_tx, mut event_rx) = mpsc::channel(512);

        let active = match open_connection(
            1,
            &endpoint,
            &access_token,
            http_config.as_ref(),
            event_tx.clone(),
        )
        .await
        {
            Ok(conn) => {
                let _ = ready.send(Ok(()));
                conn
            }
            Err(e) => {
                let _ = ready.send(Err(e));
                return;
            }
        };

        let mut state = WsTaskState::new(active);
        let timer_interval = filter_timer_mode.interval();
        let mut filter_timer =
            time::interval_at(time::Instant::now() + timer_interval, timer_interval);
        filter_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let ping_interval = ping_interval_secs
            .filter(|s| matches!(s, 1 | 3 | 5))
            .map(|s| time::Duration::from_secs(u64::from(s)));
        let mut ping_timer = ping_interval.map(|d| {
            let mut t = time::interval(d);
            t.set_missed_tick_behavior(MissedTickBehavior::Delay);
            t
        });

        loop {
            tokio::select! {
                _ = async {
                    if let Some(ref mut t) = ping_timer {
                        t.tick().await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                } => {
                    if let Err(e) = state.maybe_send_ping().await {
                        notify_error(&events, e);
                    }
                }
                _ = filter_timer.tick() => {
                    let messages = notify_filter_timer(
                        filters.as_ref().as_slice(),
                        filter_timer_mode,
                        &events,
                    );
                    for message in messages {
                        publish_timer_message(
                            &mut state,
                            filters.as_ref().as_slice(),
                            message,
                            &events,
                        ).await;
                    }
                }
                cmd = cmd_rx.recv() => {
                    match cmd {
                        None | Some(WsCommand::Shutdown) => {
                            break;
                        }
                        Some(WsCommand::Publish {
                            tenant,
                            channel,
                            payload,
                            options,
                        }) => {
                            if let Err(e) = state
                                .publish(&filters, &tenant, &channel, payload, &options)
                                .await
                            {
                                notify_error(&events, e);
                            }
                        }
                        Some(WsCommand::Subscribe { tenant, channel_pattern, queue_capacity, reply }) => {
                            let result = handle_subscribe(
                                &subscriptions,
                                &mut state,
                                &tenant,
                                &channel_pattern,
                                queue_capacity,
                            ).await;
                            let _ = reply.send(result);
                        }
                        Some(WsCommand::Unsubscribe { tenant, channel_pattern }) => {
                            if let Err(e) = handle_unsubscribe(
                                &subscriptions,
                                &mut state,
                                &tenant,
                                &channel_pattern,
                            ).await {
                                notify_error(
                                    &events,
                                    WebSocketError::Transport {
                                        operation: "unsubscribe".into(),
                                        message: e.to_string(),
                                    },
                                );
                            }
                        }
                        Some(WsCommand::GetLinkQuality { reply }) => {
                            let _ = reply.send(state.link_quality.snapshot());
                        }
                    }
                }
                event = event_rx.recv() => {
                    let Some(event) = event else {
                        break;
                    };
                    state
                        .handle_event(
                            event,
                            WsTaskContext {
                                endpoint: &endpoint,
                                access_token: &access_token,
                                http_config: http_config.as_ref(),
                                filters: &filters,
                                meta_mode,
                                subscriptions: &subscriptions,
                                events: &events,
                                event_tx: &event_tx,
                            },
                        )
                        .await;
                }
            }
        }

        state.shutdown().await;
    })
}

fn notify_error(tx: &Option<mpsc::Sender<WebSocketEvent>>, error: WebSocketError) {
    if let Some(tx) = tx {
        let _ = tx.try_send(WebSocketEvent::Error(error));
    }
}

fn notify_filter_timer(
    filters: &[FilterRegistration],
    mode: FilterTimerMode,
    events: &Option<mpsc::Sender<WebSocketEvent>>,
) -> Vec<TimerFilterMessage> {
    let ctx = FilterTimerContext::new(mode);
    let mut messages = Vec::new();
    for (index, entry) in filters.iter().enumerate() {
        let filter = entry.filter();
        match filter.on_timer(ctx) {
            Ok(produced) => {
                for message in produced {
                    messages.push(TimerFilterMessage {
                        end_index: index,
                        message,
                    });
                }
            }
            Err(error) => notify_error(events, filter_timer_error(filter.id(), error)),
        }
    }
    messages
}

async fn publish_timer_message(
    state: &mut WsTaskState,
    filters: &[FilterRegistration],
    timer_message: TimerFilterMessage,
    events: &Option<mpsc::Sender<WebSocketEvent>>,
) {
    let TimerFilterMessage { end_index, message } = timer_message;
    let FilterSendMessage {
        tenant,
        channel,
        payload,
    } = message;
    if let Err(error) = state
        .publish_until_filter_index(filters, end_index, &tenant, &channel, payload, &PublishOptions::default())
        .await
    {
        notify_error(events, error);
    }
}

fn filter_timer_error(filter_id: &str, error: FilterError) -> WebSocketError {
    WebSocketError::Transport {
        operation: format!("filter_timer:{filter_id}"),
        message: error.to_string(),
    }
}
