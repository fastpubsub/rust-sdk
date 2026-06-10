// Copyright (c) 2026 FastPubSub Network
// SPDX-License-Identifier: MIT OR Apache-2.0
// Project: https://fastpubsub.com

//! Registry of active client subscriptions for resubscribe after reconnect.

use std::collections::BTreeSet;
use std::fmt;

use crate::transport::{make_route_subscription_key, SubscribeOptions};

/// Description of one subscription: tenant, channel pattern, and queue parameters.
///
/// In the set, uniqueness is only tenant + pattern. `queue_capacity` is metadata.
#[derive(Debug, Clone)]
pub struct SubscriptionSpec {
    /// Tenant.
    pub tenant: String,
    /// Channel pattern, as in `SUB:tenant:pattern`.
    pub channel_pattern: String,
    /// Local `mpsc` queue capacity.
    pub queue_capacity: usize,
}

impl SubscriptionSpec {
    /// Creates a record from [`crate::client::FastPubSub::subscribe`] arguments.
    pub fn new(
        tenant: impl Into<String>,
        channel_pattern: impl Into<String>,
        options: &SubscribeOptions,
    ) -> Self {
        Self {
            tenant: tenant.into(),
            channel_pattern: channel_pattern.into(),
            queue_capacity: options.queue_capacity,
        }
    }

    /// Route key (`tenant:pattern`), same as transport and perimeter.
    pub fn route_key(&self) -> String {
        make_route_subscription_key(&self.tenant, &self.channel_pattern)
    }
}

impl PartialEq for SubscriptionSpec {
    fn eq(&self, other: &Self) -> bool {
        self.tenant == other.tenant && self.channel_pattern == other.channel_pattern
    }
}

impl Eq for SubscriptionSpec {}

impl PartialOrd for SubscriptionSpec {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SubscriptionSpec {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (&self.tenant, &self.channel_pattern).cmp(&(&other.tenant, &other.channel_pattern))
    }
}

impl fmt::Display for SubscriptionSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}:{} (queue={})",
            self.tenant, self.channel_pattern, self.queue_capacity
        )
    }
}

/// Set of active subscriptions, ordered by `SubscriptionSpec`.
#[derive(Debug, Default, Clone)]
pub struct SubscriptionRegistry {
    items: BTreeSet<SubscriptionSpec>,
}

impl SubscriptionRegistry {
    /// Empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of records.
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Registry is empty.
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Immutable subscription set for reading and iteration.
    pub fn subscriptions(&self) -> &BTreeSet<SubscriptionSpec> {
        &self.items
    }

    /// Checks if a subscription exists for this tenant and pattern.
    pub fn contains(&self, tenant: &str, channel_pattern: &str) -> bool {
        self.items.contains(&SubscriptionSpec {
            tenant: tenant.to_string(),
            channel_pattern: channel_pattern.to_string(),
            queue_capacity: 0,
        })
    }

    /// Adds or updates a subscription after a successful transport `subscribe`.
    pub fn insert(&mut self, spec: SubscriptionSpec) {
        let _ = self.remove(&spec.tenant, &spec.channel_pattern);
        self.items.insert(spec);
    }

    /// Removes a subscription after a successful `unsubscribe`.
    pub fn remove(&mut self, tenant: &str, channel_pattern: &str) -> bool {
        self.items.remove(&SubscriptionSpec {
            tenant: tenant.to_string(),
            channel_pattern: channel_pattern.to_string(),
            queue_capacity: 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_remove() {
        let mut reg = SubscriptionRegistry::new();
        let spec = SubscriptionSpec::new("t1", "a.#", &SubscribeOptions::default());
        reg.insert(spec.clone());
        assert_eq!(reg.len(), 1);
        assert!(reg.contains("t1", "a.#"));
        assert!(reg.remove("t1", "a.#"));
        assert!(reg.is_empty());
    }

    #[test]
    fn duplicate_insert_replaces_queue_capacity() {
        let mut reg = SubscriptionRegistry::new();
        reg.insert(SubscriptionSpec::new(
            "t1",
            "a.#",
            &SubscribeOptions { queue_capacity: 10 },
        ));
        reg.insert(SubscriptionSpec::new(
            "t1",
            "a.#",
            &SubscribeOptions { queue_capacity: 20 },
        ));
        assert_eq!(reg.len(), 1);
        assert_eq!(
            reg.subscriptions().iter().next().unwrap().queue_capacity,
            20
        );
    }
}
