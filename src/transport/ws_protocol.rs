// Copyright (c) 2026 FastPubSub Network
// SPDX-License-Identifier: MIT OR Apache-2.0
// Project: https://fastpubsub.com

//! Encoding and decoding of perimeter-style WebSocket frames.
//!
//! Outbound publish (client -> perimeter): tenant, channel, payload, as in handler.rs.
//! Inbound delivery (perimeter -> client): same header + subscription pattern list from edge.

use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Inbound delivery frame tag. Perimeter already matched patterns, SDK does not match locally.
pub const FRAME_TAG_DELIVER: u8 = 0x01;

/// Outbound publish frame v2 tag (v1 has no tag; first byte is always 0x00 for tenant_len <= 128).
pub const FRAME_TAG_PUBLISH_V2: u8 = 0x02;

/// Client ping line (no nonce).
pub const WS_PING_LINE: &str = "PING";

/// Server pong line.
pub const WS_PONG_LINE: &str = "PONG";

use super::PublishDeliveryMode;

/// Builds a binary publish frame (outbound): v2 with delivery, or v1 for broadcast only.
pub fn encode_publish_frame(
    tenant: &str,
    channel: &str,
    payload: &[u8],
    delivery: PublishDeliveryMode,
) -> Result<Bytes, &'static str> {
    let tenant_bytes = tenant.as_bytes();
    let channel_bytes = channel.as_bytes();
    if tenant_bytes.is_empty() || channel_bytes.is_empty() {
        return Err("tenant and channel cannot be empty");
    }
    if tenant_bytes.len() > u16::MAX as usize || channel_bytes.len() > u16::MAX as usize {
        return Err("tenant or channel is too long");
    }
    let body_len = 4 + tenant_bytes.len() + channel_bytes.len() + payload.len();
    let header_len = if delivery == PublishDeliveryMode::Broadcast {
        0
    } else {
        2
    };
    let mut buf = BytesMut::with_capacity(header_len + body_len);
    if delivery != PublishDeliveryMode::Broadcast {
        buf.put_u8(FRAME_TAG_PUBLISH_V2);
        buf.put_u8(delivery_to_wire(delivery)?);
    }
    buf.put_u16(tenant_bytes.len() as u16);
    buf.put_slice(tenant_bytes);
    buf.put_u16(channel_bytes.len() as u16);
    buf.put_slice(channel_bytes);
    buf.put_slice(payload);
    Ok(buf.freeze())
}

fn delivery_to_wire(mode: PublishDeliveryMode) -> Result<u8, &'static str> {
    match mode {
        PublishDeliveryMode::Broadcast => Ok(0),
        PublishDeliveryMode::DeliverOneLowLatency => Ok(1),
        PublishDeliveryMode::DeliverOneRandom => Ok(2),
    }
}

/// True if text line is a perimeter `PONG` response.
pub fn is_pong_line(text: &str) -> bool {
    text == WS_PONG_LINE
}

/// Decodes an outbound publish frame without pattern list.
#[allow(dead_code)]
pub fn decode_publish_frame(data: &[u8]) -> Result<(String, String, Bytes), &'static str> {
    let mut buf = data;
    let tenant = read_utf16_blob(&mut buf, "tenant")?;
    let channel = read_utf16_blob(&mut buf, "channel")?;
    let payload_off = data.len() - buf.remaining();
    Ok((
        tenant,
        channel,
        Bytes::copy_from_slice(&data[payload_off..]),
    ))
}

/// Inbound delivery from perimeter: tenant, concrete channel, payload, subscription pattern list.
///
/// Wire after [`FRAME_TAG_DELIVER`] tag:
/// `u16 tenant | u16 channel | u32 payload_len | payload | u16 pattern_count | (u16 pattern)*`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundDeliverFrame {
    /// Tenant.
    pub tenant: String,
    /// Concrete subject for filters and logs.
    pub channel: String,
    /// Body after edge filters, or before them depending on perimeter. SDK filters again locally.
    pub payload: Bytes,
    /// Subscription patterns where edge placed this message. Queue key is `tenant:pattern`.
    pub matched_patterns: Vec<String>,
}

/// Decodes a delivery frame with [`FRAME_TAG_DELIVER`] tag.
pub fn decode_deliver_frame(data: &[u8]) -> Result<InboundDeliverFrame, &'static str> {
    if data.is_empty() {
        return Err("empty frame");
    }
    if data[0] != FRAME_TAG_DELIVER {
        return Err("expected FRAME_TAG_DELIVER tag");
    }
    let mut buf = &data[1..];
    let tenant = read_utf16_blob(&mut buf, "tenant")?;
    let channel = read_utf16_blob(&mut buf, "channel")?;
    if buf.remaining() < 4 {
        return Err("missing payload length");
    }
    let payload_len = buf.get_u32() as usize;
    if buf.remaining() < payload_len {
        return Err("truncated payload");
    }
    let payload = Bytes::copy_from_slice(&buf[..payload_len]);
    buf.advance(payload_len);

    if buf.remaining() < 2 {
        return Err("missing pattern count");
    }
    let pattern_count = buf.get_u16() as usize;
    let mut matched_patterns = Vec::with_capacity(pattern_count);
    for _ in 0..pattern_count {
        let pat = read_utf16_blob(&mut buf, "pattern")?;
        if pat.is_empty() {
            return Err("empty pattern in list");
        }
        matched_patterns.push(pat);
    }
    if !buf.is_empty() {
        return Err("extra bytes after pattern list");
    }
    if matched_patterns.is_empty() {
        return Err("pattern list is empty");
    }
    Ok(InboundDeliverFrame {
        tenant,
        channel,
        payload,
        matched_patterns,
    })
}

fn read_utf16_blob(buf: &mut &[u8], field: &'static str) -> Result<String, &'static str> {
    if buf.remaining() < 2 {
        return Err(match field {
            "tenant" => "missing tenant length",
            "channel" => "missing channel length",
            "pattern" => "missing pattern length",
            _ => "missing field length",
        });
    }
    let len = buf.get_u16() as usize;
    if len == 0 {
        return Err(match field {
            "tenant" => "empty tenant",
            "channel" => "empty channel",
            "pattern" => "empty pattern",
            _ => "empty field",
        });
    }
    if buf.remaining() < len {
        return Err(match field {
            "tenant" => "truncated tenant",
            "channel" => "truncated channel",
            "pattern" => "truncated pattern",
            _ => "truncated field",
        });
    }
    let s = std::str::from_utf8(&buf[..len]).map_err(|_| "not UTF-8")?;
    let out = s.to_string();
    buf.advance(len);
    Ok(out)
}

/// Subscribe text: `SUB:tenant:pattern` (one pattern).
pub fn format_subscribe(tenant: &str, channel_pattern: &str) -> String {
    format!("SUB:{tenant}:{channel_pattern}")
}

/// Unsubscribe text: `UNSUB:tenant:pattern`.
pub fn format_unsubscribe(tenant: &str, channel_pattern: &str) -> String {
    format!("UNSUB:{tenant}:{channel_pattern}")
}

/// Line starts with `ERR:` (error from perimeter).
pub fn parse_err_line(text: &str) -> Option<&str> {
    text.strip_prefix("ERR:")
}

/// Subscription/unsubscription confirmation from perimeter (`OK:SUB:...` / `OK:UNSUB:...`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubWireAck {
    /// `OK:SUB:tenant:pattern` (exactly one pattern).
    Sub {
        /// Tenant.
        tenant: String,
        /// Pattern from response.
        channel_pattern: String,
    },
    /// `OK:UNSUB:tenant:pattern`
    Unsub {
        /// Tenant.
        tenant: String,
        /// Pattern from response.
        channel_pattern: String,
    },
}

/// Builds `OK:SUB:tenant:pattern`.
pub fn format_ok_subscribe(tenant: &str, channel_pattern: &str) -> String {
    format!("OK:SUB:{tenant}:{channel_pattern}")
}

/// Builds `OK:UNSUB:tenant:pattern`.
pub fn format_ok_unsubscribe(tenant: &str, channel_pattern: &str) -> String {
    format!("OK:UNSUB:{tenant}:{channel_pattern}")
}

/// Parses `OK:SUB:` or `OK:UNSUB:` with `tenant:pattern` body.
pub fn parse_sub_ack_line(text: &str) -> Option<SubWireAck> {
    if let Some(rest) = text.strip_prefix("OK:SUB:") {
        let (tenant, channel_pattern) = parse_tenant_pattern_body(rest)?;
        return Some(SubWireAck::Sub {
            tenant,
            channel_pattern,
        });
    }
    if let Some(rest) = text.strip_prefix("OK:UNSUB:") {
        let (tenant, channel_pattern) = parse_tenant_pattern_body(rest)?;
        return Some(SubWireAck::Unsub {
            tenant,
            channel_pattern,
        });
    }
    None
}

/// Body after `OK:SUB:` / `OK:UNSUB:`: `tenant:pattern` (one pattern, no comma list).
fn parse_tenant_pattern_body(input: &str) -> Option<(String, String)> {
    let colon_pos = input.find(':')?;
    let tenant = input[..colon_pos].trim();
    if tenant.is_empty() {
        return None;
    }
    let pattern = input[colon_pos + 1..].trim();
    if pattern.is_empty() {
        return None;
    }
    Some((tenant.to_string(), pattern.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::PublishDeliveryMode;

    #[test]
    fn deliver_roundtrip_fields() {
        let mut buf = BytesMut::new();
        buf.put_u8(FRAME_TAG_DELIVER);
        buf.put_u16(2);
        buf.put_slice(b"t1");
        buf.put_u16(5);
        buf.put_slice(b"a.b.c");
        buf.put_u32(3);
        buf.put_slice(b"xyz");
        buf.put_u16(2);
        buf.put_u16(3);
        buf.put_slice(b"a.#");
        buf.put_u16(5);
        buf.put_slice(b"a.b.*");

        let frame = decode_deliver_frame(&buf).unwrap();
        assert_eq!(frame.tenant, "t1");
        assert_eq!(frame.channel, "a.b.c");
        assert_eq!(&frame.payload[..], b"xyz");
        assert_eq!(
            frame.matched_patterns,
            vec!["a.#".to_string(), "a.b.*".to_string()]
        );
    }

    #[test]
    fn parse_ok_sub_one_pattern() {
        let ack = parse_sub_ack_line("OK:SUB:t1:public.#").unwrap();
        assert_eq!(
            ack,
            SubWireAck::Sub {
                tenant: "t1".into(),
                channel_pattern: "public.#".into(),
            }
        );
    }

    #[test]
    fn parse_ok_unsub_one_pattern() {
        let ack = parse_sub_ack_line("OK:UNSUB:prod:orders.#").unwrap();
        assert_eq!(
            ack,
            SubWireAck::Unsub {
                tenant: "prod".into(),
                channel_pattern: "orders.#".into(),
            }
        );
    }

    #[test]
    fn publish_v2_encodes_delivery() {
        let frame = encode_publish_frame(
            "t1",
            "ch",
            b"data",
            PublishDeliveryMode::DeliverOneLowLatency,
        )
        .unwrap();
        assert_eq!(frame[0], FRAME_TAG_PUBLISH_V2);
        assert_eq!(frame[1], 1);
    }
}
