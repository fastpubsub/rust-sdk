// Copyright (c) 2026 FastPubSub Network
// SPDX-License-Identifier: MIT OR Apache-2.0
// Project: https://fastpubsub.com

//! Short deduplicator for inbound frames during double WebSocket reconnect.

use std::collections::HashMap;
use std::time::Duration;

use tokio::time::Instant;
use xxhash_rust::xxh32::Xxh32;

/// Deduplicator for the overlap window. It stores a short list of already seen frames.
pub(super) struct OverlapDedup {
    enabled: bool,
    ttl: Duration,
    seen: HashMap<u64, Instant>,
}

impl OverlapDedup {
    /// Creates a disabled deduplicator with the given TTL.
    pub(super) fn new(ttl: Duration) -> Self {
        Self {
            enabled: false,
            ttl,
            seen: HashMap::new(),
        }
    }

    /// Enables the deduplicator and clears the old cache.
    pub(super) fn enable(&mut self, ttl: Duration) {
        self.enabled = true;
        self.ttl = ttl;
        self.seen.clear();
    }

    /// Disables the deduplicator and clears the cache.
    pub(super) fn disable(&mut self) {
        self.enabled = false;
        self.seen.clear();
    }

    /// Returns `true` if this inbound frame was already seen in the overlap window.
    pub(super) fn is_duplicate(&mut self, tenant: &str, channel: &str, payload: &[u8]) -> bool {
        if !self.enabled {
            return false;
        }
        let now = Instant::now();
        self.seen.retain(|_, expires_at| *expires_at > now);
        let key = overlap_dedup_key(tenant, channel, payload);
        if self.seen.contains_key(&key) {
            return true;
        }
        self.seen.insert(key, now + self.ttl);
        false
    }
}

const SEED_LO: u32 = 0x9e37_79b1;
const SEED_HI: u32 = 0x85eb_ca77;

/// Builds a 64-bit key as `XXH32x2(tenant, channel, payload)`.
fn overlap_dedup_key(tenant: &str, channel: &str, payload: &[u8]) -> u64 {
    let mut lo_hasher = Xxh32::new(SEED_LO);
    let mut hi_hasher = Xxh32::new(SEED_HI);
    update_len_and_bytes(&mut lo_hasher, tenant.as_bytes());
    update_len_and_bytes(&mut hi_hasher, tenant.as_bytes());
    update_len_and_bytes(&mut lo_hasher, channel.as_bytes());
    update_len_and_bytes(&mut hi_hasher, channel.as_bytes());
    update_len_and_bytes(&mut lo_hasher, payload);
    update_len_and_bytes(&mut hi_hasher, payload);
    let lo = lo_hasher.digest();
    let hi = hi_hasher.digest();
    ((hi as u64) << 32) | lo as u64
}

/// Adds field length and bytes to the hash stream.
fn update_len_and_bytes(hasher: &mut Xxh32, bytes: &[u8]) {
    hasher.update(&(bytes.len() as u32).to_le_bytes());
    hasher.update(bytes);
}

/// Calculates 32-bit xxHash with the given seed.
#[cfg(test)]
fn xxh32(input: &[u8], seed: u32) -> u32 {
    let mut hasher = Xxh32::new(seed);
    hasher.update(input);
    hasher.digest()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xxh32_known_vectors_seed_zero() {
        assert_eq!(xxh32(b"", 0), 0x02cc_5d05);
        assert_eq!(xxh32(b"a", 0), 0x550d_7456);
        assert_eq!(xxh32(b"abc", 0), 0x32d1_53ff);
        assert_eq!(xxh32(b"hello", 0), 0xfb00_77f9);
    }

    #[test]
    fn streaming_xxh32_matches_one_shot() {
        let chunks = [
            b"ab".as_slice(),
            b"cdefghijklmnop".as_slice(),
            b"qrst".as_slice(),
        ];
        let mut data = Vec::new();
        let mut hasher = Xxh32::new(SEED_LO);
        for chunk in chunks {
            data.extend_from_slice(chunk);
            hasher.update(chunk);
        }
        assert_eq!(hasher.digest(), xxh32(&data, SEED_LO));
    }

    #[test]
    fn dedup_key_is_stable() {
        assert_eq!(
            overlap_dedup_key("t1", "a.b", b"hello"),
            0xd04a_58a1_ecd0_a68e
        );
    }

    #[test]
    fn dedup_key_uses_field_lengths() {
        let a = overlap_dedup_key("ab", "c", b"");
        let b = overlap_dedup_key("a", "bc", b"");
        assert_eq!(a, 0x4f2a_8c1b_f93f_bb80);
        assert_eq!(b, 0x42f3_31a3_4843_1421);
        assert_ne!(a, b);
    }

    #[test]
    fn disabled_dedup_never_drops() {
        let mut dedup = OverlapDedup::new(Duration::from_secs(1));
        assert!(!dedup.is_duplicate("t1", "a.b", b"hello"));
        assert!(!dedup.is_duplicate("t1", "a.b", b"hello"));
    }

    #[test]
    fn enabled_dedup_drops_second_equal_key() {
        let mut dedup = OverlapDedup::new(Duration::from_secs(1));
        dedup.enable(Duration::from_secs(1));
        assert!(!dedup.is_duplicate("t1", "a.b", b"hello"));
        assert!(dedup.is_duplicate("t1", "a.b", b"hello"));
    }
}
