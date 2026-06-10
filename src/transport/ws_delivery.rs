// Copyright (c) 2026 FastPubSub Network
// SPDX-License-Identifier: MIT OR Apache-2.0
// Project: https://fastpubsub.com

//! Publish and inbound delivery of WebSocket frames to local subscription queues.

use std::time::SystemTime;

use bytes::Bytes;
use dashmap::DashMap;
use futures_util::SinkExt;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use crate::filters::{FilterError, FilterRegistration, FilterSendMessage, RouteContext};
use crate::metadata::{MessageMeta, MetaMode, MetaWriter, SdkMessage};

use super::overlap_dedup::OverlapDedup;
use super::route_pattern::make_route_subscription_key;
use super::ws_protocol::{decode_deliver_frame, encode_publish_frame};
use super::ws_subscriptions::SubscriptionEntry;
use super::{TransportError, WebSocketError};

pub(super) enum InboundDeliveryResult {
    Delivered { messages: Vec<FilterSendMessage> },
    DuplicateDropped { messages: Vec<FilterSendMessage> },
}

pub(super) async fn handle_publish(
    filters: &[FilterRegistration],
    ws_write: &mut (impl SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin),
    tenant: &str,
    channel: &str,
    payload: Bytes,
) -> Result<(), TransportError> {
    handle_publish_until(
        filters,
        filters.len(),
        ws_write,
        tenant,
        channel,
        payload.to_vec(),
    )
    .await
}

pub(super) async fn handle_publish_until(
    filters: &[FilterRegistration],
    end_index: usize,
    ws_write: &mut (impl SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin),
    tenant: &str,
    channel: &str,
    payload: Vec<u8>,
) -> Result<(), TransportError> {
    let ctx = RouteContext { tenant, channel };
    let bodies = apply_outbound_filters_until(filters, end_index, ctx, payload)?;
    for body in bodies {
        handle_prepared_publish(ws_write, tenant, channel, body).await?;
    }
    Ok(())
}

/// Sends the payload after the outbound pipeline as a WebSocket frame.
pub(super) async fn handle_prepared_publish(
    ws_write: &mut (impl SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin),
    tenant: &str,
    channel: &str,
    payload: Vec<u8>,
) -> Result<(), TransportError> {
    let frame = encode_publish_frame(tenant, channel, &payload)
        .map_err(|e| TransportError::Other(e.into()))?;
    ws_write
        .send(Message::Binary(frame))
        .await
        .map_err(|e| TransportError::Other(e.to_string()))?;
    Ok(())
}

pub(super) async fn handle_inbound_binary(
    filters: &[FilterRegistration],
    meta_mode: MetaMode,
    subscriptions: &DashMap<String, SubscriptionEntry>,
    overlap_dedup: &mut OverlapDedup,
    data: &[u8],
) -> Result<InboundDeliveryResult, WebSocketError> {
    let frame =
        decode_deliver_frame(data).map_err(|e| WebSocketError::Decode { message: e.into() })?;
    let received_at = SystemTime::now();

    let ctx = RouteContext {
        tenant: &frame.tenant,
        channel: &frame.channel,
    };
    let result =
        apply_inbound_filters(filters, ctx, frame.payload.to_vec(), meta_mode).map_err(|e| {
            WebSocketError::InboundFilter {
                message: e.to_string(),
            }
        })?;
    let messages = result.messages;
    if result.payloads.is_empty() {
        return Ok(InboundDeliveryResult::Delivered { messages });
    }

    let dedup_payload = if result.payloads.len() == 1 {
        result.payloads[0].payload.as_slice()
    } else {
        frame.payload.as_ref()
    };
    if overlap_dedup.is_duplicate(&frame.tenant, &frame.channel, dedup_payload) {
        return Ok(InboundDeliveryResult::DuplicateDropped { messages });
    }

    let mut delivered_any = false;
    let mut missing = Vec::new();

    for delivered in result.payloads {
        let payload = Bytes::from(delivered.payload);
        for pattern in &frame.matched_patterns {
            let key = make_route_subscription_key(&frame.tenant, pattern);
            let Some(entry) = subscriptions.get(&key) else {
                missing.push(pattern.clone());
                continue;
            };
            let message = SdkMessage::new(
                frame.tenant.clone(),
                frame.channel.clone(),
                pattern.clone(),
                received_at,
                payload.clone(),
                delivered.meta.clone(),
            );
            match entry.value().sender.try_send(message) {
                Ok(()) => delivered_any = true,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    return Err(WebSocketError::SubscriptionQueueFull {
                        tenant: frame.tenant,
                        channel_pattern: pattern.clone(),
                    });
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    missing.push(pattern.clone());
                }
            }
        }
    }

    if !missing.is_empty() {
        return Err(WebSocketError::MissingSubscription {
            tenant: frame.tenant,
            channel_patterns: missing,
        });
    }
    if !delivered_any {
        return Err(WebSocketError::EmptyDelivery {
            tenant: frame.tenant,
            channel: frame.channel,
        });
    }
    Ok(InboundDeliveryResult::Delivered { messages })
}

fn apply_outbound_filters_until(
    filters: &[FilterRegistration],
    end_index: usize,
    ctx: RouteContext<'_>,
    payload: Vec<u8>,
) -> Result<Vec<Vec<u8>>, TransportError> {
    let mut payloads = vec![payload];
    let end_index = end_index.min(filters.len());
    for entry in filters[..end_index].iter().rev() {
        if !entry.matches(ctx) {
            continue;
        }
        let mut next_payloads = Vec::new();
        for payload in payloads {
            let mut produced = entry
                .filter()
                .apply_outbound(ctx, payload)
                .map_err(filter_err_to_transport)?;
            next_payloads.append(&mut produced);
        }
        payloads = next_payloads;
    }
    Ok(payloads)
}

fn apply_inbound_filters(
    filters: &[FilterRegistration],
    ctx: RouteContext<'_>,
    payload: Vec<u8>,
    meta_mode: MetaMode,
) -> Result<InboundPipelineResult, TransportError> {
    match meta_mode {
        MetaMode::None => apply_inbound_filters_without_meta(filters, ctx, payload),
        MetaMode::Stages => apply_inbound_filters_with_meta(filters, ctx, payload),
    }
}

struct InboundPipelineResult {
    payloads: Vec<InboundLocalPayload>,
    messages: Vec<FilterSendMessage>,
}

struct InboundLocalPayload {
    payload: Vec<u8>,
    meta: MessageMeta,
}

struct WorkingPayload {
    payload: Vec<u8>,
    meta: MetaWriter,
}

fn apply_inbound_filters_without_meta(
    filters: &[FilterRegistration],
    ctx: RouteContext<'_>,
    payload: Vec<u8>,
) -> Result<InboundPipelineResult, TransportError> {
    let mut payloads = vec![payload];
    let mut messages = Vec::new();
    for entry in filters {
        if !entry.matches(ctx) {
            continue;
        }
        let mut next_payloads = Vec::new();
        for payload in payloads {
            let mut produced = entry
                .filter()
                .apply_inbound(ctx, payload)
                .map_err(filter_err_to_transport)?;
            messages.append(&mut produced.messages);
            next_payloads.append(&mut produced.payloads);
        }
        payloads = next_payloads;
    }
    Ok(InboundPipelineResult {
        payloads: payloads
            .into_iter()
            .map(|payload| InboundLocalPayload {
                payload,
                meta: MessageMeta::None,
            })
            .collect(),
        messages,
    })
}

fn apply_inbound_filters_with_meta(
    filters: &[FilterRegistration],
    ctx: RouteContext<'_>,
    payload: Vec<u8>,
) -> Result<InboundPipelineResult, TransportError> {
    let mut payloads = vec![WorkingPayload {
        payload,
        meta: MetaWriter::new(),
    }];
    let mut messages = Vec::new();
    for entry in filters {
        if !entry.matches(ctx) {
            continue;
        }
        let mut next_payloads = Vec::new();
        for item in payloads {
            let mut meta = item.meta;
            let mut produced = entry
                .filter()
                .apply_inbound_with_meta(ctx, item.payload, &mut meta)
                .map_err(filter_err_to_transport)?;
            messages.append(&mut produced.messages);
            for payload in produced.payloads {
                next_payloads.push(WorkingPayload {
                    payload,
                    meta: meta.clone(),
                });
            }
        }
        payloads = next_payloads;
    }
    Ok(InboundPipelineResult {
        payloads: payloads
            .into_iter()
            .map(|item| InboundLocalPayload {
                payload: item.payload,
                meta: item.meta.finish(),
            })
            .collect(),
        messages,
    })
}

fn filter_err_to_transport(e: FilterError) -> TransportError {
    TransportError::Other(e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filters::{FilterInboundResult, FilterTrait};
    use bytes::{BufMut, BytesMut};

    fn push_text(buf: &mut BytesMut, text: &str) {
        buf.put_u16(text.len() as u16);
        buf.put_slice(text.as_bytes());
    }

    fn deliver_frame(tenant: &str, channel: &str, payload: &[u8], patterns: &[&str]) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u8(0x01);
        push_text(&mut buf, tenant);
        push_text(&mut buf, channel);
        buf.put_u32(payload.len() as u32);
        buf.put_slice(payload);
        buf.put_u16(patterns.len() as u16);
        for pattern in patterns {
            push_text(&mut buf, pattern);
        }
        buf.freeze()
    }

    fn add_subscription(
        subscriptions: &DashMap<String, SubscriptionEntry>,
        tenant: &str,
        pattern: &str,
    ) -> mpsc::Receiver<SdkMessage> {
        let (tx, rx) = mpsc::channel(4);
        subscriptions.insert(
            make_route_subscription_key(tenant, pattern),
            SubscriptionEntry {
                tenant: tenant.to_string(),
                channel_pattern: pattern.to_string(),
                sender: tx,
            },
        );
        rx
    }

    struct SplitFilter;

    impl FilterTrait for SplitFilter {
        fn id(&self) -> &'static str {
            "test.split"
        }

        fn apply_outbound(
            &self,
            _ctx: RouteContext<'_>,
            payload: Vec<u8>,
        ) -> Result<Vec<Vec<u8>>, FilterError> {
            let split_at = payload.len() / 2;
            Ok(vec![
                payload[..split_at].to_vec(),
                payload[split_at..].to_vec(),
            ])
        }
    }

    struct InboundReplyFilter;

    impl FilterTrait for InboundReplyFilter {
        fn id(&self) -> &'static str {
            "test.inbound_reply"
        }

        fn apply_inbound(
            &self,
            ctx: RouteContext<'_>,
            payload: Vec<u8>,
        ) -> Result<FilterInboundResult, FilterError> {
            Ok(FilterInboundResult::single_with_messages(
                payload,
                vec![FilterSendMessage::new(
                    ctx.tenant,
                    "public.reply",
                    b"ack".to_vec(),
                )],
            ))
        }
    }

    struct InboundSplitFilter;

    impl FilterTrait for InboundSplitFilter {
        fn id(&self) -> &'static str {
            "test.inbound_split"
        }

        fn apply_inbound(
            &self,
            _ctx: RouteContext<'_>,
            payload: Vec<u8>,
        ) -> Result<FilterInboundResult, FilterError> {
            let split_at = payload.len() / 2;
            Ok(FilterInboundResult::new(
                vec![payload[..split_at].to_vec(), payload[split_at..].to_vec()],
                Vec::new(),
            ))
        }
    }

    #[test]
    fn outbound_filters_can_split_one_payload_to_many() {
        let filters = vec![FilterRegistration::new("t1", "a.", SplitFilter)];
        let ctx = RouteContext {
            tenant: "t1",
            channel: "a.b",
        };

        let payloads =
            apply_outbound_filters_until(&filters, filters.len(), ctx, b"hello".to_vec()).unwrap();

        assert_eq!(payloads, vec![b"he".to_vec(), b"llo".to_vec()]);
    }

    struct MarkFilter {
        id: &'static str,
        outbound_mark: u8,
        inbound_mark: u8,
    }

    impl FilterTrait for MarkFilter {
        fn id(&self) -> &'static str {
            self.id
        }

        fn apply_outbound(
            &self,
            _ctx: RouteContext<'_>,
            mut payload: Vec<u8>,
        ) -> Result<Vec<Vec<u8>>, FilterError> {
            payload.push(self.outbound_mark);
            Ok(vec![payload])
        }

        fn apply_inbound(
            &self,
            _ctx: RouteContext<'_>,
            mut payload: Vec<u8>,
        ) -> Result<FilterInboundResult, FilterError> {
            payload.push(self.inbound_mark);
            Ok(FilterInboundResult::single(payload))
        }
    }

    struct MetaMarkFilter {
        id: &'static str,
        record_type: u16,
        value: u8,
    }

    impl FilterTrait for MetaMarkFilter {
        fn id(&self) -> &'static str {
            self.id
        }

        fn apply_inbound_with_meta(
            &self,
            _ctx: RouteContext<'_>,
            payload: Vec<u8>,
            meta: &mut MetaWriter,
        ) -> Result<FilterInboundResult, FilterError> {
            meta.push_record(self.record_type, &[self.value])
                .map_err(|e| FilterError::Other(e.to_string()))?;
            Ok(FilterInboundResult::single(payload))
        }
    }

    #[test]
    fn outbound_filters_follow_reverse_registration_order() {
        let filters = vec![
            FilterRegistration::new(
                "t1",
                "a.",
                MarkFilter {
                    id: "test.first",
                    outbound_mark: b'1',
                    inbound_mark: b'a',
                },
            ),
            FilterRegistration::new(
                "t1",
                "a.",
                MarkFilter {
                    id: "test.second",
                    outbound_mark: b'2',
                    inbound_mark: b'b',
                },
            ),
        ];
        let ctx = RouteContext {
            tenant: "t1",
            channel: "a.b",
        };

        let payloads =
            apply_outbound_filters_until(&filters, filters.len(), ctx, Vec::new()).unwrap();

        assert_eq!(payloads, vec![b"21".to_vec()]);
    }

    #[test]
    fn filters_require_exact_tenant_match() {
        let filters = vec![FilterRegistration::new(
            "t1",
            "a.",
            MarkFilter {
                id: "test.first",
                outbound_mark: b'1',
                inbound_mark: b'a',
            },
        )];
        let ctx = RouteContext {
            tenant: "t2",
            channel: "a.b",
        };

        let payloads =
            apply_outbound_filters_until(&filters, filters.len(), ctx, Vec::new()).unwrap();

        assert_eq!(payloads, vec![Vec::<u8>::new()]);
    }

    #[test]
    fn inbound_filters_follow_registration_order() {
        let filters = vec![
            FilterRegistration::new(
                "t1",
                "a.",
                MarkFilter {
                    id: "test.first",
                    outbound_mark: b'1',
                    inbound_mark: b'a',
                },
            ),
            FilterRegistration::new(
                "t1",
                "a.",
                MarkFilter {
                    id: "test.second",
                    outbound_mark: b'2',
                    inbound_mark: b'b',
                },
            ),
        ];
        let ctx = RouteContext {
            tenant: "t1",
            channel: "a.b",
        };
        let payload = Vec::new();

        let result = apply_inbound_filters(&filters, ctx, payload, MetaMode::None).unwrap();
        let payloads = result
            .payloads
            .into_iter()
            .map(|item| item.payload)
            .collect::<Vec<_>>();

        assert_eq!(payloads, vec![b"ab".to_vec()]);
        assert!(result.messages.is_empty());
    }

    #[test]
    fn inbound_filters_can_split_one_payload_to_many() {
        let filters = vec![
            FilterRegistration::new("t1", "a.", InboundSplitFilter),
            FilterRegistration::new(
                "t1",
                "a.",
                MarkFilter {
                    id: "test.mark",
                    outbound_mark: b'1',
                    inbound_mark: b'!',
                },
            ),
        ];
        let ctx = RouteContext {
            tenant: "t1",
            channel: "a.b",
        };

        let result =
            apply_inbound_filters(&filters, ctx, b"hell".to_vec(), MetaMode::None).unwrap();
        let payloads = result
            .payloads
            .into_iter()
            .map(|item| item.payload)
            .collect::<Vec<_>>();

        assert_eq!(payloads, vec![b"he!".to_vec(), b"ll!".to_vec()]);
        assert!(result.messages.is_empty());
    }

    #[test]
    fn inbound_metadata_keeps_filter_order_and_duplicates() {
        let filters = vec![
            FilterRegistration::new(
                "t1",
                "a.",
                MetaMarkFilter {
                    id: "test.meta.first",
                    record_type: 0x8001,
                    value: 1,
                },
            ),
            FilterRegistration::new(
                "t1",
                "a.",
                MetaMarkFilter {
                    id: "test.meta.second",
                    record_type: 0x8001,
                    value: 2,
                },
            ),
        ];
        let ctx = RouteContext {
            tenant: "t1",
            channel: "a.b",
        };

        let result =
            apply_inbound_filters(&filters, ctx, b"hello".to_vec(), MetaMode::Stages).unwrap();
        let meta = &result.payloads[0].meta;
        let records = meta
            .records()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(meta.stage_count().unwrap(), 2);
        assert_eq!(records[0].record_type, 0x8001);
        assert_eq!(records[0].data, &[1]);
        assert_eq!(records[1].record_type, 0x8001);
        assert_eq!(records[1].data, &[2]);
    }

    #[test]
    fn outbound_filters_can_continue_before_filter_index() {
        let filters = vec![
            FilterRegistration::new(
                "t1",
                "a.",
                MarkFilter {
                    id: "test.first",
                    outbound_mark: b'1',
                    inbound_mark: b'a',
                },
            ),
            FilterRegistration::new(
                "t1",
                "a.",
                MarkFilter {
                    id: "test.second",
                    outbound_mark: b'2',
                    inbound_mark: b'b',
                },
            ),
        ];
        let ctx = RouteContext {
            tenant: "t1",
            channel: "a.b",
        };

        let payloads = apply_outbound_filters_until(&filters, 1, ctx, Vec::new()).unwrap();

        assert_eq!(payloads, vec![b"1".to_vec()]);
    }

    #[tokio::test]
    async fn inbound_filters_can_return_messages_to_send() {
        let subscriptions = DashMap::new();
        let mut rx = add_subscription(&subscriptions, "t1", "a.#");
        let filters = vec![FilterRegistration::new("t1", "a.", InboundReplyFilter)];
        let mut dedup = OverlapDedup::new(std::time::Duration::from_secs(2));
        let frame = deliver_frame("t1", "a.b", b"hello", &["a.#"]);

        let result =
            handle_inbound_binary(&filters, MetaMode::None, &subscriptions, &mut dedup, &frame)
                .await
                .unwrap();

        assert_eq!(rx.try_recv().unwrap().payload.as_ref(), b"hello");
        match result {
            InboundDeliveryResult::Delivered { messages } => {
                assert_eq!(
                    messages,
                    vec![FilterSendMessage::new(
                        "t1",
                        "public.reply",
                        b"ack".to_vec()
                    )]
                );
            }
            InboundDeliveryResult::DuplicateDropped { .. } => panic!("message must be delivered"),
        }
    }

    #[tokio::test]
    async fn inbound_filters_deliver_split_payloads() {
        let subscriptions = DashMap::new();
        let mut rx = add_subscription(&subscriptions, "t1", "a.#");
        let filters = vec![FilterRegistration::new("t1", "a.", InboundSplitFilter)];
        let mut dedup = OverlapDedup::new(std::time::Duration::from_secs(2));
        let frame = deliver_frame("t1", "a.b", b"hell", &["a.#"]);

        let result =
            handle_inbound_binary(&filters, MetaMode::None, &subscriptions, &mut dedup, &frame)
                .await
                .unwrap();

        assert_eq!(rx.try_recv().unwrap().payload.as_ref(), b"he");
        assert_eq!(rx.try_recv().unwrap().payload.as_ref(), b"ll");
        match result {
            InboundDeliveryResult::Delivered { messages } => {
                assert!(messages.is_empty());
            }
            InboundDeliveryResult::DuplicateDropped { .. } => panic!("message must be delivered"),
        }
    }

    #[tokio::test]
    async fn overlap_dedup_drops_second_frame_before_fanout() {
        let subscriptions = DashMap::new();
        let mut rx_a = add_subscription(&subscriptions, "t1", "a.#");
        let mut rx_b = add_subscription(&subscriptions, "t1", "a.*");
        let filters = Vec::new();
        let mut dedup = OverlapDedup::new(std::time::Duration::from_secs(2));
        dedup.enable(std::time::Duration::from_secs(2));
        let frame = deliver_frame("t1", "a.b", b"hello", &["a.#", "a.*"]);

        handle_inbound_binary(&filters, MetaMode::None, &subscriptions, &mut dedup, &frame)
            .await
            .unwrap();
        assert_eq!(rx_a.try_recv().unwrap().payload.as_ref(), b"hello");
        assert_eq!(rx_b.try_recv().unwrap().payload.as_ref(), b"hello");

        handle_inbound_binary(&filters, MetaMode::None, &subscriptions, &mut dedup, &frame)
            .await
            .unwrap();
        assert!(rx_a.try_recv().is_err());
        assert!(rx_b.try_recv().is_err());
    }
}
