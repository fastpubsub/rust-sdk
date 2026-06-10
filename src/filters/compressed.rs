// Copyright (c) 2026 FastPubSub Network
// SPDX-License-Identifier: MIT OR Apache-2.0
// Project: https://fastpubsub.com

use std::io::Read;

use flate2::read::{DeflateDecoder, DeflateEncoder};
use flate2::Compression;

use crate::metadata::{MetaWriter, META_COMPRESSION_DEFLATE_RAW, META_RECORD_COMPRESSION};

use super::{FilterError, FilterInboundResult, FilterTrait, RouteContext};

/// Compression algorithm name without zlib or gzip headers.
///
/// # Examples
///
/// ```
/// use fastpubsub_sdk::filters::DEFLATE_RAW_ALGORITHM;
///
/// assert_eq!(DEFLATE_RAW_ALGORITHM, "deflate-raw");
/// ```
pub const DEFLATE_RAW_ALGORITHM: &str = "deflate-raw";

/// A filter that compresses outbound messages and decompresses inbound messages.
///
/// This filter does not add a header to the payload. Both sides must know
/// that the selected pipeline prefix uses the `deflate-raw` algorithm.
///
/// # Examples
///
/// Add the default compression filter to the client pipeline:
///
/// ```ignore
/// use fastpubsub_sdk::client::create_web_socket;
/// use fastpubsub_sdk::filters::CompressedFilter;
///
/// let fastpubsub = create_web_socket("overlay_name", "AT_token")
///     .add_filter("tenant_1", "public.", CompressedFilter::new())
///     .build()
///     .await?;
/// ```
///
/// Add the compression filter with a custom compression level:
///
/// ```ignore
/// use fastpubsub_sdk::client::create_web_socket;
/// use fastpubsub_sdk::filters::CompressedFilter;
///
/// let fastpubsub = create_web_socket("overlay_name", "AT_token")
///     .add_filter("tenant_1", "public.", CompressedFilter::with_level(6))
///     .build()
///     .await?;
/// ```
pub struct CompressedFilter {
    level: Compression,
}

impl CompressedFilter {
    /// Creates a filter with the default compression level.
    pub fn new() -> Self {
        Self {
            level: Compression::default(),
        }
    }

    /// Creates a filter with a compression level from 0 to 9.
    pub fn with_level(level: u32) -> Self {
        Self {
            level: Compression::new(level),
        }
    }

    /// Returns the algorithm name used by this filter.
    pub fn algorithm(&self) -> &'static str {
        DEFLATE_RAW_ALGORITHM
    }
}

impl Default for CompressedFilter {
    fn default() -> Self {
        Self::new()
    }
}

impl FilterTrait for CompressedFilter {
    fn id(&self) -> &'static str {
        "compressed.deflate-raw"
    }

    fn apply_outbound(
        &self,
        _ctx: RouteContext<'_>,
        payload: Vec<u8>,
    ) -> Result<Vec<Vec<u8>>, FilterError> {
        let mut encoder = DeflateEncoder::new(payload.as_slice(), self.level);
        let mut compressed = Vec::new();
        encoder
            .read_to_end(&mut compressed)
            .map_err(|e| FilterError::CompressFailed(e.to_string()))?;
        Ok(vec![compressed])
    }

    fn apply_inbound(
        &self,
        _ctx: RouteContext<'_>,
        payload: Vec<u8>,
    ) -> Result<FilterInboundResult, FilterError> {
        let plain = decompress_payload(payload)?;
        Ok(FilterInboundResult::single(plain))
    }

    fn apply_inbound_with_meta(
        &self,
        _ctx: RouteContext<'_>,
        payload: Vec<u8>,
        meta: &mut MetaWriter,
    ) -> Result<FilterInboundResult, FilterError> {
        let plain = decompress_payload(payload)?;
        meta.push_record_with(META_RECORD_COMPRESSION, |out| {
            out.push(META_COMPRESSION_DEFLATE_RAW);
            out.extend_from_slice(&(plain.len() as u32).to_be_bytes());
            Ok(())
        })
        .map_err(|e| FilterError::Other(e.to_string()))?;
        Ok(FilterInboundResult::single(plain))
    }
}

/// Decompresses a payload with deflate raw.
fn decompress_payload(payload: Vec<u8>) -> Result<Vec<u8>, FilterError> {
    let mut decoder = DeflateDecoder::new(payload.as_slice());
    let mut plain = Vec::new();
    decoder
        .read_to_end(&mut plain)
        .map_err(|e| FilterError::DecompressFailed(e.to_string()))?;
    Ok(plain)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> RouteContext<'static> {
        RouteContext {
            tenant: "tenant_1",
            channel: "public.demo",
        }
    }

    #[test]
    fn compressed_filter_uses_deflate_raw_name() {
        let filter = CompressedFilter::new();

        assert_eq!(filter.algorithm(), DEFLATE_RAW_ALGORITHM);
        assert_eq!(filter.id(), "compressed.deflate-raw");
    }

    #[test]
    fn compressed_filter_roundtrip_returns_original_payload() {
        let filter = CompressedFilter::new();
        let original = b"payload-bytes payload-bytes payload-bytes".to_vec();
        let payload = original.clone();

        let mut chunks = filter.apply_outbound(ctx(), payload).unwrap();
        assert_eq!(chunks.len(), 1);
        let payload = chunks.pop().unwrap();
        assert_ne!(payload, original);

        let result = filter.apply_inbound(ctx(), payload).unwrap();
        assert_eq!(result.payloads, vec![original]);
        assert!(result.messages.is_empty());
    }

    #[test]
    fn compressed_filter_reports_bad_payload() {
        let filter = CompressedFilter::new();
        let payload = b"not a deflate raw payload".to_vec();

        let err = filter.apply_inbound(ctx(), payload).unwrap_err();
        assert!(matches!(err, FilterError::DecompressFailed(_)));
    }
}
