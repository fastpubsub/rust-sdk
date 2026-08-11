// Copyright (c) 2026 FastPubSub Network
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Client-side WS link quality (RTT from PING/PONG).

use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// Snapshot of connection quality for the application.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LinkQualitySnapshot {
    /// Last successful RTT in milliseconds.
    pub last_rtt_ms: Option<u32>,
    /// Median RTT over the window in milliseconds.
    pub median_rtt_ms: Option<u32>,
}

/// Tracks RTT samples measured by the SDK (one in-flight PING at a time).
#[derive(Debug)]
pub struct LinkQuality {
    samples: VecDeque<(Instant, u32)>,
    pending_sent_at: Option<Instant>,
    max_samples: usize,
}

impl Default for LinkQuality {
    fn default() -> Self {
        Self::new(120)
    }
}

impl LinkQuality {
    /// Creates a tracker with a sample ring capacity.
    pub fn new(max_samples: usize) -> Self {
        Self {
            samples: VecDeque::with_capacity(max_samples.min(512)),
            pending_sent_at: None,
            max_samples: max_samples.max(8),
        }
    }

    /// Marks start of a PING (skip if previous is still pending).
    pub fn begin_ping(&mut self) -> bool {
        if self.pending_sent_at.is_some() {
            return false;
        }
        self.pending_sent_at = Some(Instant::now());
        true
    }

    /// Records PONG and returns measured RTT.
    pub fn record_pong(&mut self) -> Option<u32> {
        let sent = self.pending_sent_at.take()?;
        let rtt_ms = sent.elapsed().as_millis().min(u128::from(u32::MAX)) as u32;
        self.push_sample(Instant::now(), rtt_ms);
        Some(rtt_ms)
    }

    /// Drops stale pending ping after timeout.
    pub fn expire_pending(&mut self, timeout: Duration) {
        if let Some(sent) = self.pending_sent_at {
            if sent.elapsed() >= timeout {
                self.pending_sent_at = None;
            }
        }
    }

    fn push_sample(&mut self, at: Instant, rtt_ms: u32) {
        if self.samples.len() >= self.max_samples {
            self.samples.pop_front();
        }
        self.samples.push_back((at, rtt_ms));
    }

    /// Last RTT sample.
    pub fn last_rtt_ms(&self) -> Option<u32> {
        self.samples.back().map(|(_, ms)| *ms)
    }

    /// Median RTT over the last `window` seconds.
    pub fn median_rtt_ms(&self, window: Duration) -> Option<u32> {
        let cutoff = Instant::now().checked_sub(window)?;
        let mut values: Vec<u32> = self
            .samples
            .iter()
            .filter(|(t, _)| *t >= cutoff)
            .map(|(_, ms)| *ms)
            .collect();
        if values.is_empty() {
            return None;
        }
        values.sort_unstable();
        let mid = values.len() / 2;
        if values.len() % 2 == 1 {
            Some(values[mid])
        } else {
            Some((values[mid - 1] + values[mid]) / 2)
        }
    }

    /// Current snapshot (median window default 30s).
    pub fn snapshot(&self) -> LinkQualitySnapshot {
        LinkQualitySnapshot {
            last_rtt_ms: self.last_rtt_ms(),
            median_rtt_ms: self.median_rtt_ms(Duration::from_secs(30)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_rtt_on_pong() {
        let mut q = LinkQuality::new(16);
        assert!(q.begin_ping());
        assert!(!q.begin_ping());
        let rtt = q.record_pong().unwrap();
        assert!(rtt < 50);
        assert_eq!(q.last_rtt_ms(), Some(rtt));
    }
}
