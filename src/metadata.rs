// Copyright (c) 2026 FastPubSub Network
// SPDX-License-Identifier: MIT OR Apache-2.0
// Project: https://fastpubsub.com

//! Local metadata for SDK messages.

use std::fmt;
use std::time::SystemTime;

use bytes::Bytes;

/// Binary metadata blob version.
pub const META_VERSION: u8 = 1;

/// Record with message id.
pub const META_RECORD_MESSAGE_ID: u16 = 0x0001;
/// Record with ack data.
pub const META_RECORD_ACK: u16 = 0x0002;
/// Record with compression data.
pub const META_RECORD_COMPRESSION: u16 = 0x0003;
/// Record with encryption data.
pub const META_RECORD_ENCRYPTION: u16 = 0x0004;
/// Record with fragmentation data.
pub const META_RECORD_FRAGMENTATION: u16 = 0x0005;
/// Record with dedup data.
pub const META_RECORD_DEDUP: u16 = 0x0006;
/// Record with delta data.
pub const META_RECORD_DELTA: u16 = 0x0007;
/// Record with priority data.
pub const META_RECORD_PRIORITY: u16 = 0x0008;
/// Record with timestamp data.
pub const META_RECORD_TIMESTAMP: u16 = 0x0009;
/// Record with trace data.
pub const META_RECORD_TRACE: u16 = 0x000A;
/// Record with SDK batch data.
pub const META_RECORD_CHANNEL_BATCH: u16 = 0x000B;

/// Deflate raw algorithm for the metadata compression record.
pub const META_COMPRESSION_DEFLATE_RAW: u8 = 1;
/// ChaCha20-Poly1305 algorithm for the metadata encryption record.
pub const META_ENCRYPTION_CHACHA20_POLY1305: u8 = 1;
/// AES-256-GCM algorithm for the metadata encryption record.
pub const META_ENCRYPTION_AES256_GCM: u8 = 2;

/// Metadata creation mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MetaMode {
    /// Do not create metadata.
    #[default]
    None,
    /// Create records for the inbound filter pipeline.
    Stages,
}

/// Metadata for one message.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum MessageMeta {
    /// No metadata.
    #[default]
    None,
    /// Ready binary blob.
    Blob(Bytes),
}

impl MessageMeta {
    /// Returns true when there is no metadata.
    pub fn is_empty(&self) -> bool {
        match self {
            MessageMeta::None => true,
            MessageMeta::Blob(bytes) => bytes.is_empty(),
        }
    }

    /// Returns metadata bytes.
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            MessageMeta::None => &[],
            MessageMeta::Blob(bytes) => bytes.as_ref(),
        }
    }

    /// Creates an iterator over records.
    pub fn records(&self) -> Result<MetaIter<'_>, MetaDecodeError> {
        MetaIter::new(self.as_bytes())
    }

    /// Returns the record count from the header.
    pub fn stage_count(&self) -> Result<u8, MetaDecodeError> {
        match self {
            MessageMeta::None => Ok(0),
            MessageMeta::Blob(bytes) => {
                if bytes.len() < 2 {
                    return Err(MetaDecodeError::TooShort);
                }
                if bytes[0] != META_VERSION {
                    return Err(MetaDecodeError::UnsupportedVersion);
                }
                Ok(bytes[1])
            }
        }
    }
}

/// Message that the SDK gives to a subscriber.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SdkMessage {
    /// Message tenant.
    pub tenant: String,
    /// Real message channel.
    pub channel: String,
    /// Subscription pattern that matched the message.
    pub matched_pattern: String,
    /// Time when the SDK received the delivery frame.
    pub received_at: SystemTime,
    /// Payload after inbound filters.
    pub payload: Bytes,
    /// Local metadata after inbound filters.
    pub meta: MessageMeta,
}

impl SdkMessage {
    /// Creates an SDK message.
    pub fn new(
        tenant: impl Into<String>,
        channel: impl Into<String>,
        matched_pattern: impl Into<String>,
        received_at: SystemTime,
        payload: Bytes,
        meta: MessageMeta,
    ) -> Self {
        Self {
            tenant: tenant.into(),
            channel: channel.into(),
            matched_pattern: matched_pattern.into(),
            received_at,
            payload,
            meta,
        }
    }

    /// Returns the tenant.
    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    /// Returns the real channel.
    pub fn channel(&self) -> &str {
        &self.channel
    }

    /// Returns the matched subscription pattern.
    pub fn matched_pattern(&self) -> &str {
        &self.matched_pattern
    }

    /// Returns the receive time.
    pub fn received_at(&self) -> SystemTime {
        self.received_at
    }

    /// Returns the payload.
    pub fn payload(&self) -> &Bytes {
        &self.payload
    }

    /// Returns the metadata.
    pub fn meta(&self) -> &MessageMeta {
        &self.meta
    }

    /// Returns true when metadata exists.
    pub fn has_metadata(&self) -> bool {
        !self.meta.is_empty()
    }
}

/// Metadata write error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetaEncodeError {
    /// More than 255 records.
    TooManyRecords,
    /// One record data is larger than 65535 bytes.
    RecordTooLarge,
}

impl fmt::Display for MetaEncodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MetaEncodeError::TooManyRecords => write!(f, "metadata records are more than 255"),
            MetaEncodeError::RecordTooLarge => write!(f, "metadata record is too large"),
        }
    }
}

impl std::error::Error for MetaEncodeError {}

/// Metadata read error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetaDecodeError {
    /// Blob is too short.
    TooShort,
    /// Metadata version is not supported.
    UnsupportedVersion,
    /// Blob is corrupted.
    Corrupted,
}

impl fmt::Display for MetaDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MetaDecodeError::TooShort => write!(f, "metadata blob is too short"),
            MetaDecodeError::UnsupportedVersion => {
                write!(f, "metadata version is not supported")
            }
            MetaDecodeError::Corrupted => write!(f, "metadata blob is corrupted"),
        }
    }
}

impl std::error::Error for MetaDecodeError {}

/// Writer that writes records directly into a binary blob.
#[derive(Debug, Clone)]
pub struct MetaWriter {
    buf: Vec<u8>,
    count: u8,
}

impl MetaWriter {
    /// Creates an empty writer.
    pub fn new() -> Self {
        Self {
            buf: vec![META_VERSION, 0],
            count: 0,
        }
    }

    /// Returns true when there are no records yet.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Adds a ready record.
    pub fn push_record(&mut self, record_type: u16, data: &[u8]) -> Result<(), MetaEncodeError> {
        self.push_record_with(record_type, |out| {
            out.extend_from_slice(data);
            Ok(())
        })
    }

    /// Adds a record and lets a callback write data.
    pub fn push_record_with(
        &mut self,
        record_type: u16,
        write_data: impl FnOnce(&mut Vec<u8>) -> Result<(), MetaEncodeError>,
    ) -> Result<(), MetaEncodeError> {
        if self.count == u8::MAX {
            return Err(MetaEncodeError::TooManyRecords);
        }

        let start = self.buf.len();
        self.buf.extend_from_slice(&record_type.to_be_bytes());
        self.buf.extend_from_slice(&0u16.to_be_bytes());

        let data_start = self.buf.len();
        if let Err(error) = write_data(&mut self.buf) {
            self.buf.truncate(start);
            return Err(error);
        }

        let len = self.buf.len() - data_start;
        if len > u16::MAX as usize {
            self.buf.truncate(start);
            return Err(MetaEncodeError::RecordTooLarge);
        }

        self.buf[start + 2..start + 4].copy_from_slice(&(len as u16).to_be_bytes());
        self.count += 1;
        self.buf[1] = self.count;
        Ok(())
    }

    /// Finishes the writer and returns metadata.
    pub fn finish(self) -> MessageMeta {
        if self.count == 0 {
            MessageMeta::None
        } else {
            MessageMeta::Blob(Bytes::from(self.buf))
        }
    }
}

impl Default for MetaWriter {
    fn default() -> Self {
        Self::new()
    }
}

/// One record while reading metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetaRecordView<'a> {
    /// Record type.
    pub record_type: u16,
    /// Record data.
    pub data: &'a [u8],
}

/// Iterator over metadata records.
pub struct MetaIter<'a> {
    data: &'a [u8],
    offset: usize,
    count: u8,
    index: u8,
    done: bool,
}

impl<'a> MetaIter<'a> {
    /// Creates an iterator over a blob.
    pub fn new(data: &'a [u8]) -> Result<Self, MetaDecodeError> {
        if data.len() < 2 {
            return Err(MetaDecodeError::TooShort);
        }
        if data[0] != META_VERSION {
            return Err(MetaDecodeError::UnsupportedVersion);
        }

        Ok(Self {
            data,
            offset: 2,
            count: data[1],
            index: 0,
            done: false,
        })
    }

    /// Returns the record count from the header.
    pub fn count(&self) -> u8 {
        self.count
    }
}

impl<'a> Iterator for MetaIter<'a> {
    type Item = Result<MetaRecordView<'a>, MetaDecodeError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.count {
            if !self.done && self.offset != self.data.len() {
                self.done = true;
                return Some(Err(MetaDecodeError::Corrupted));
            }
            self.done = true;
            return None;
        }

        if self.offset + 4 > self.data.len() {
            self.index = self.count;
            return Some(Err(MetaDecodeError::Corrupted));
        }

        let record_type = u16::from_be_bytes([self.data[self.offset], self.data[self.offset + 1]]);
        let len =
            u16::from_be_bytes([self.data[self.offset + 2], self.data[self.offset + 3]]) as usize;
        self.offset += 4;

        if self.offset + len > self.data.len() {
            self.index = self.count;
            return Some(Err(MetaDecodeError::Corrupted));
        }

        let data = &self.data[self.offset..self.offset + len];
        self.offset += len;
        self.index += 1;

        Some(Ok(MetaRecordView { record_type, data }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writer_preserves_order_and_duplicates() {
        let mut writer = MetaWriter::new();
        writer.push_record(META_RECORD_ENCRYPTION, &[1]).unwrap();
        writer.push_record(META_RECORD_COMPRESSION, &[2]).unwrap();
        writer.push_record(META_RECORD_ENCRYPTION, &[3]).unwrap();

        let meta = writer.finish();
        let records = meta
            .records()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(meta.stage_count().unwrap(), 3);
        assert_eq!(records[0].record_type, META_RECORD_ENCRYPTION);
        assert_eq!(records[0].data, &[1]);
        assert_eq!(records[1].record_type, META_RECORD_COMPRESSION);
        assert_eq!(records[1].data, &[2]);
        assert_eq!(records[2].record_type, META_RECORD_ENCRYPTION);
        assert_eq!(records[2].data, &[3]);
    }

    #[test]
    fn writer_without_records_returns_none() {
        let meta = MetaWriter::new().finish();

        assert_eq!(meta, MessageMeta::None);
        assert_eq!(meta.stage_count().unwrap(), 0);
    }

    #[test]
    fn reader_reports_trailing_bytes() {
        let data = [META_VERSION, 0, 1];
        let mut iter = MetaIter::new(&data).unwrap();

        assert_eq!(iter.next(), Some(Err(MetaDecodeError::Corrupted)));
        assert_eq!(iter.next(), None);
    }
}
