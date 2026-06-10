// Copyright (c) 2026 FastPubSub Network
// SPDX-License-Identifier: MIT OR Apache-2.0
// Project: https://fastpubsub.com

//! Helpers on top of the normal transport.
//!
//! This module has a reader for only the latest message and a state sync
//! store for the latest state by key.

mod latest_receiver;
mod state_sync;

pub use latest_receiver::LatestMessageReceiver;
pub use state_sync::{CleanupPolicy, LatestStateSync, LatestStateSyncOptions, StateSyncError};
