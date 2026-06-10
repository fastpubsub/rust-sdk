// Copyright (c) 2026 FastPubSub Network
// SPDX-License-Identifier: MIT OR Apache-2.0
// Project: https://fastpubsub.com

//! Helper for game loops that need to read only the latest message.

use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;

use crate::metadata::SdkMessage;

/// Reads a subscription so the client gets only the latest message.
///
/// This type works on top of a normal [`mpsc::Receiver`]. Old messages from the
/// local queue are removed, and the latest message is saved for reuse.
pub struct LatestMessageReceiver {
    receiver: mpsc::Receiver<SdkMessage>,
    last: Option<SdkMessage>,
}

impl LatestMessageReceiver {
    /// Creates a reader on top of a normal subscription queue.
    pub fn new(receiver: mpsc::Receiver<SdkMessage>) -> Self {
        Self {
            receiver,
            last: None,
        }
    }

    /// Returns the inner queue back to the caller.
    pub fn into_inner(self) -> mpsc::Receiver<SdkMessage> {
        self.receiver
    }

    /// Waits for the first new message and returns the latest message from the queue.
    ///
    /// If the queue already has several messages while reading, old messages are
    /// removed and the client gets only the newest one.
    pub async fn recv_latest(&mut self) -> Option<SdkMessage> {
        let first = self.receiver.recv().await?;
        Some(self.keep_latest_from(first))
    }

    /// Checks the queue without waiting and returns the latest new message.
    ///
    /// If there are no new messages, returns `None` and keeps the saved cache.
    pub fn poll_latest(&mut self) -> Option<SdkMessage> {
        let first = match self.receiver.try_recv() {
            Ok(message) => message,
            Err(TryRecvError::Empty | TryRecvError::Disconnected) => return None,
        };
        Some(self.keep_latest_from(first))
    }

    /// Checks the queue without waiting and returns a new or saved message.
    ///
    /// If new messages arrived, the cache is updated with the latest message.
    /// If there are no new messages, the method returns a copy of the last cache.
    pub fn poll_latest_or_cached(&mut self) -> Option<SdkMessage> {
        if let Some(message) = self.poll_latest() {
            return Some(message);
        }
        self.last.clone()
    }

    /// Returns the last saved message without reading the queue.
    pub fn last(&self) -> Option<&SdkMessage> {
        self.last.as_ref()
    }

    /// Removes queued messages and saves only the newest one.
    fn keep_latest_from(&mut self, first: SdkMessage) -> SdkMessage {
        let mut latest = first;
        while let Ok(message) = self.receiver.try_recv() {
            latest = message;
        }
        self.last = Some(latest.clone());
        latest
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use bytes::Bytes;
    use tokio::sync::mpsc;

    use super::LatestMessageReceiver;
    use crate::metadata::{MessageMeta, SdkMessage};

    /// Creates a test message with a one-byte payload.
    fn message(value: u8) -> SdkMessage {
        SdkMessage::new(
            "tenant_1",
            "public.position",
            "public.#",
            SystemTime::UNIX_EPOCH,
            Bytes::from(vec![value]),
            MessageMeta::None,
        )
    }

    /// Puts a message into the test queue.
    async fn send(tx: &mpsc::Sender<SdkMessage>, value: u8) {
        tx.send(message(value)).await.unwrap();
    }

    #[tokio::test]
    async fn recv_latest_waits_and_returns_only_last_message() {
        let (tx, rx) = mpsc::channel(8);
        send(&tx, 1).await;
        send(&tx, 2).await;
        send(&tx, 3).await;
        let mut latest = LatestMessageReceiver::new(rx);

        let message = latest.recv_latest().await.unwrap();

        assert_eq!(message.payload.as_ref(), &[3]);
        assert_eq!(latest.last().unwrap().payload.as_ref(), &[3]);
        assert!(latest.poll_latest().is_none());
    }

    #[tokio::test]
    async fn poll_latest_returns_only_ready_last_message() {
        let (tx, rx) = mpsc::channel(8);
        send(&tx, 4).await;
        send(&tx, 5).await;
        let mut latest = LatestMessageReceiver::new(rx);

        let message = latest.poll_latest().unwrap();

        assert_eq!(message.payload.as_ref(), &[5]);
        assert_eq!(latest.last().unwrap().payload.as_ref(), &[5]);
        assert!(latest.poll_latest().is_none());
    }

    #[tokio::test]
    async fn poll_latest_or_cached_repeats_last_message() {
        let (tx, rx) = mpsc::channel(8);
        send(&tx, 7).await;
        let mut latest = LatestMessageReceiver::new(rx);

        assert_eq!(
            latest.poll_latest_or_cached().unwrap().payload.as_ref(),
            &[7]
        );
        assert_eq!(
            latest.poll_latest_or_cached().unwrap().payload.as_ref(),
            &[7]
        );
    }

    #[test]
    fn poll_latest_or_cached_returns_none_before_first_message() {
        let (_tx, rx) = mpsc::channel(8);
        let mut latest = LatestMessageReceiver::new(rx);

        assert!(latest.poll_latest_or_cached().is_none());
        assert!(latest.last().is_none());
    }

    #[test]
    fn into_inner_returns_original_receiver() {
        let (_tx, rx) = mpsc::channel(8);
        let latest = LatestMessageReceiver::new(rx);

        let _rx = latest.into_inner();
    }
}
