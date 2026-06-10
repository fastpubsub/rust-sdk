// Copyright (c) 2026 FastPubSub Network
// SPDX-License-Identifier: MIT OR Apache-2.0
// Project: https://fastpubsub.com

//! Latest state storage on top of normal transport publish/subscribe.

use std::collections::HashMap;
use std::fmt;
use std::time::Duration;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use dashmap::DashMap;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{self, Instant, MissedTickBehavior};

use crate::client::{PublishError, SharedFastPubSub};
use crate::metadata::SdkMessage;
use crate::transport::{SubscribeOptions, Transport, TransportError};

const ENVELOPE_VERSION: u8 = 1;
const ENVELOPE_HEADER_LEN: usize = 5;

/// Latest state sync error.
#[derive(Debug)]
pub enum StateSyncError {
    /// Transport subscribe error.
    Transport(TransportError),
    /// Publish error.
    Publish(PublishError),
    /// Payload does not look like a state envelope.
    InvalidEnvelope(String),
}

impl fmt::Display for StateSyncError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StateSyncError::Transport(error) => write!(f, "transport: {error}"),
            StateSyncError::Publish(error) => write!(f, "publish: {error}"),
            StateSyncError::InvalidEnvelope(message) => write!(f, "invalid envelope: {message}"),
        }
    }
}

impl std::error::Error for StateSyncError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            StateSyncError::Transport(error) => Some(error),
            StateSyncError::Publish(error) => Some(error),
            StateSyncError::InvalidEnvelope(_) => None,
        }
    }
}

impl From<TransportError> for StateSyncError {
    fn from(value: TransportError) -> Self {
        StateSyncError::Transport(value)
    }
}

impl From<PublishError> for StateSyncError {
    fn from(value: PublishError) -> Self {
        StateSyncError::Publish(value)
    }
}

/// Policy for cleaning old state keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanupPolicy {
    /// Clean only by an explicit [`LatestStateSync::prune_stale`] call.
    Manual,
    /// Automatically remove keys without updates after the given time.
    AfterIdle(Duration),
}

impl Default for CleanupPolicy {
    fn default() -> Self {
        CleanupPolicy::Manual
    }
}

/// Latest state sync settings.
#[derive(Debug, Clone)]
pub struct LatestStateSyncOptions {
    /// Local subscription queue size.
    pub queue_capacity: usize,
    /// Policy for cleaning old keys.
    pub cleanup_policy: CleanupPolicy,
}

impl LatestStateSyncOptions {
    /// Sets the local subscription queue size.
    pub fn queue_capacity(mut self, queue_capacity: usize) -> Self {
        self.queue_capacity = queue_capacity;
        self
    }

    /// Sets the policy for cleaning old keys.
    pub fn cleanup_policy(mut self, cleanup_policy: CleanupPolicy) -> Self {
        self.cleanup_policy = cleanup_policy;
        self
    }

    /// Returns options for a normal transport subscription.
    fn subscribe_options(&self) -> SubscribeOptions {
        SubscribeOptions {
            queue_capacity: self.queue_capacity,
        }
    }
}

impl Default for LatestStateSyncOptions {
    fn default() -> Self {
        Self {
            queue_capacity: 256,
            cleanup_policy: CleanupPolicy::Manual,
        }
    }
}

#[derive(Clone)]
struct StateEntry {
    state: Bytes,
    updated_at: Instant,
}

struct StateEnvelope {
    key: String,
    state: Bytes,
}

/// Latest state sync handle for one tenant/channel.
pub struct LatestStateSync<T: Transport + 'static> {
    client: SharedFastPubSub<T>,
    tenant: String,
    channel: String,
    states: std::sync::Arc<DashMap<String, StateEntry>>,
    task: Option<JoinHandle<()>>,
}

impl<T: Transport + 'static> LatestStateSync<T> {
    /// Creates state sync on top of a shared client.
    pub async fn connect(
        client: SharedFastPubSub<T>,
        tenant: impl Into<String>,
        channel: impl Into<String>,
        options: LatestStateSyncOptions,
    ) -> Result<Self, StateSyncError> {
        let tenant = tenant.into();
        let channel = channel.into();
        let rx = client
            .subscribe(&tenant, &channel, &options.subscribe_options())
            .await?;
        let states = std::sync::Arc::new(DashMap::new());
        let task = spawn_state_task(rx, states.clone(), options.cleanup_policy);
        Ok(Self {
            client,
            tenant,
            channel,
            states,
            task: Some(task),
        })
    }

    /// Publishes a new state value for a key.
    pub async fn push(
        &self,
        key: impl AsRef<str>,
        state: impl AsRef<[u8]>,
    ) -> Result<(), StateSyncError> {
        let payload = encode_state_envelope(key.as_ref(), state.as_ref())?;
        self.client
            .publish(&self.tenant, &self.channel, payload.as_ref())
            .await?;
        Ok(())
    }

    /// Returns the latest state value for a key.
    pub fn get_state(&self, key: &str) -> Option<Bytes> {
        self.states.get(key).map(|entry| entry.state.clone())
    }

    /// Returns a full snapshot of all state values.
    pub fn get_all_states(&self) -> HashMap<String, Bytes> {
        self.states
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().state.clone()))
            .collect()
    }

    /// Removes keys that were not updated longer than `max_age`.
    pub fn prune_stale(&self, max_age: Duration) -> usize {
        prune_stale_states(&self.states, max_age)
    }

    /// Returns the number of keys in the local store.
    pub fn len(&self) -> usize {
        self.states.len()
    }

    /// Returns true when the local store is empty.
    pub fn is_empty(&self) -> bool {
        self.states.is_empty()
    }
}

impl<T: Transport + 'static> Drop for LatestStateSync<T> {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// Starts a task that reads the subscription and updates the local store.
fn spawn_state_task(
    mut rx: mpsc::Receiver<SdkMessage>,
    states: std::sync::Arc<DashMap<String, StateEntry>>,
    cleanup_policy: CleanupPolicy,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        match cleanup_policy {
            CleanupPolicy::Manual => {
                while let Some(message) = rx.recv().await {
                    apply_message(&states, message);
                }
            }
            CleanupPolicy::AfterIdle(max_age) => {
                let mut cleanup_timer = time::interval(Duration::from_secs(1));
                cleanup_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
                loop {
                    tokio::select! {
                        message = rx.recv() => {
                            let Some(message) = message else {
                                break;
                            };
                            apply_message(&states, message);
                        }
                        _ = cleanup_timer.tick() => {
                            prune_stale_states(&states, max_age);
                        }
                    }
                }
            }
        }
    })
}

/// Applies one inbound state message to the store.
fn apply_message(states: &DashMap<String, StateEntry>, message: SdkMessage) {
    if let Ok(envelope) = decode_state_envelope(message.payload.as_ref()) {
        states.insert(
            envelope.key,
            StateEntry {
                state: envelope.state,
                updated_at: Instant::now(),
            },
        );
    }
}

/// Removes old state values.
fn prune_stale_states(states: &DashMap<String, StateEntry>, max_age: Duration) -> usize {
    let now = Instant::now();
    let before = states.len();
    states.retain(|_, entry| now.saturating_duration_since(entry.updated_at) <= max_age);
    before.saturating_sub(states.len())
}

/// Encodes a key and state into one payload.
fn encode_state_envelope(key: &str, state: &[u8]) -> Result<Bytes, StateSyncError> {
    if key.is_empty() {
        return Err(StateSyncError::InvalidEnvelope("key is empty".into()));
    }
    if key.len() > u32::MAX as usize {
        return Err(StateSyncError::InvalidEnvelope("key is too large".into()));
    }
    let mut bytes = BytesMut::with_capacity(ENVELOPE_HEADER_LEN + key.len() + state.len());
    bytes.put_u8(ENVELOPE_VERSION);
    bytes.put_u32(key.len() as u32);
    bytes.put_slice(key.as_bytes());
    bytes.put_slice(state);
    Ok(bytes.freeze())
}

/// Decodes a state sync payload.
fn decode_state_envelope(payload: &[u8]) -> Result<StateEnvelope, StateSyncError> {
    if payload.len() < ENVELOPE_HEADER_LEN {
        return Err(StateSyncError::InvalidEnvelope("payload is too short".into()));
    }
    let mut reader = payload;
    let version = reader.get_u8();
    if version != ENVELOPE_VERSION {
        return Err(StateSyncError::InvalidEnvelope(
            "unsupported envelope version".into(),
        ));
    }
    let key_len = reader.get_u32() as usize;
    if reader.remaining() < key_len {
        return Err(StateSyncError::InvalidEnvelope("key is truncated".into()));
    }
    let key_bytes = &reader[..key_len];
    let key = std::str::from_utf8(key_bytes)
        .map_err(|_| StateSyncError::InvalidEnvelope("key is not utf-8".into()))?
        .to_string();
    if key.is_empty() {
        return Err(StateSyncError::InvalidEnvelope("key is empty".into()));
    }
    reader.advance(key_len);
    Ok(StateEnvelope {
        key,
        state: Bytes::copy_from_slice(reader),
    })
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use bytes::Bytes;

    use super::*;
    use crate::metadata::{MessageMeta, SdkMessage};

    /// Creates a test message with a state envelope.
    fn message(key: &str, state: &[u8]) -> SdkMessage {
        SdkMessage::new(
            "tenant_1",
            "game.state",
            "game.state",
            SystemTime::UNIX_EPOCH,
            encode_state_envelope(key, state).unwrap(),
            MessageMeta::None,
        )
    }

    #[test]
    fn envelope_roundtrip_keeps_key_and_state() {
        let payload = encode_state_envelope("player:123", b"{\"x\":10}").unwrap();
        let envelope = decode_state_envelope(payload.as_ref()).unwrap();

        assert_eq!(envelope.key, "player:123");
        assert_eq!(envelope.state.as_ref(), b"{\"x\":10}");
    }

    #[test]
    fn envelope_rejects_empty_key() {
        let error = encode_state_envelope("", b"state").unwrap_err();

        assert!(matches!(error, StateSyncError::InvalidEnvelope(_)));
    }

    #[test]
    fn store_replaces_old_state_for_same_key() {
        let states = DashMap::new();

        apply_message(&states, message("player:123", b"old"));
        apply_message(&states, message("player:123", b"new"));

        assert_eq!(states.len(), 1);
        assert_eq!(states.get("player:123").unwrap().state.as_ref(), b"new");
    }

    #[test]
    fn store_keeps_different_keys() {
        let states = DashMap::new();

        apply_message(&states, message("player:123", b"user"));
        apply_message(&states, message("npc:88", b"npc"));

        assert_eq!(states.len(), 2);
        assert_eq!(states.get("player:123").unwrap().state.as_ref(), b"user");
        assert_eq!(states.get("npc:88").unwrap().state.as_ref(), b"npc");
    }

    #[test]
    fn get_all_states_returns_snapshot_copy() {
        let states = DashMap::new();
        apply_message(&states, message("player:123", b"user"));

        let snapshot = states
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().state.clone()))
            .collect::<HashMap<_, _>>();

        assert_eq!(snapshot["player:123"].as_ref(), b"user");
    }

    #[test]
    fn prune_stale_removes_old_entries() {
        let states = DashMap::new();
        states.insert(
            "old".to_string(),
            StateEntry {
                state: Bytes::from_static(b"old"),
                updated_at: Instant::now() - Duration::from_secs(10),
            },
        );
        states.insert(
            "new".to_string(),
            StateEntry {
                state: Bytes::from_static(b"new"),
                updated_at: Instant::now(),
            },
        );

        let removed = prune_stale_states(&states, Duration::from_secs(1));

        assert_eq!(removed, 1);
        assert!(!states.contains_key("old"));
        assert!(states.contains_key("new"));
    }
}
