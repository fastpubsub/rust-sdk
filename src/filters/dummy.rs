// Copyright (c) 2026 FastPubSub Network
// SPDX-License-Identifier: MIT OR Apache-2.0
// Project: https://fastpubsub.com

use super::FilterTrait;

/// Empty filter: does not change the payload and always succeeds.
///
/// Useful for pipeline tests and as an example implementation of [`FilterTrait`].
pub struct DummyFilter;

impl DummyFilter {
    /// Creates a stub instance.
    pub fn new() -> Self {
        Self
    }
}

impl Default for DummyFilter {
    fn default() -> Self {
        Self::new()
    }
}

impl FilterTrait for DummyFilter {
    fn id(&self) -> &'static str {
        "dummy"
    }
}
