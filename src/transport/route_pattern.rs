// Copyright (c) 2026 FastPubSub Network
// SPDX-License-Identifier: MIT OR Apache-2.0
// Project: https://fastpubsub.com

//! Subscription route pattern: perimeter-style string (`patterns`), segments separated by `.`, including `#` / `*`.
//!
//! Channel-pattern matching runs on the perimeter. SDK receives the pattern list on the wire
//! and routes one message to queues by the `tenant:pattern` key.

use super::TransportError;

/// Builds the subscription key: `tenant` + `:` + channel pattern.
///
/// It must match what the perimeter puts into `matched_patterns` in the inbound frame.
///
/// # Arguments
///
/// * `tenant` - tenant namespace.
/// * `channel_pattern` - subscription pattern, for example `private.#`.
pub fn make_route_subscription_key(tenant: &str, channel_pattern: &str) -> String {
    format!("{tenant}:{channel_pattern}")
}

/// Minimal pattern check before registration. The value must not be empty after trim.
pub fn validate_channel_pattern(channel_pattern: &str) -> Result<(), TransportError> {
    if channel_pattern.trim().is_empty() {
        return Err(TransportError::InvalidRoutePattern);
    }
    Ok(())
}
