// Copyright (c) 2026 FastPubSub Network
// SPDX-License-Identifier: MIT OR Apache-2.0
// Project: https://fastpubsub.com

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use super::{FilterError, FilterInboundResult, FilterTrait, RouteContext};

static DEBUG_LOG_FILES: OnceLock<Mutex<HashMap<PathBuf, Arc<Mutex<File>>>>> = OnceLock::new();

/// Options for the debug log filter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DebugLogOptions {
    /// Write inbound messages.
    pub inbound: bool,
    /// Write outbound messages.
    pub outbound: bool,
    /// Write hex near the text when the message is valid UTF-8.
    pub write_hex: bool,
    /// Try to format JSON in a readable way.
    pub format_json: bool,
}

impl DebugLogOptions {
    /// Creates options that write inbound and outbound messages.
    pub fn new() -> Self {
        Self::default()
    }

    /// Enables or disables inbound message logging.
    pub fn inbound(mut self, enabled: bool) -> Self {
        self.inbound = enabled;
        self
    }

    /// Enables or disables outbound message logging.
    pub fn outbound(mut self, enabled: bool) -> Self {
        self.outbound = enabled;
        self
    }

    /// Enables or disables an extra hex line for UTF-8 messages.
    pub fn write_hex(mut self, enabled: bool) -> Self {
        self.write_hex = enabled;
        self
    }

    /// Enables or disables readable JSON formatting.
    pub fn format_json(mut self, enabled: bool) -> Self {
        self.format_json = enabled;
        self
    }
}

impl Default for DebugLogOptions {
    /// Creates default options.
    fn default() -> Self {
        Self {
            inbound: true,
            outbound: true,
            write_hex: false,
            format_json: true,
        }
    }
}

/// Filter for message debug logs.
///
/// The filter does not change the payload. It only writes a record to a file
/// and sends the same bytes to the next pipeline step.
///
/// This filter is only for logs and debugging. Do not put it on a hot
/// production path without a clear reason. The filter calls `flush()` after
/// each record, so the record reaches the file at once.
///
/// You can use the same log file path in several `DebugLogFilter` instances.
/// Filters with the same path share one `Mutex<File>`, so each record is
/// written under one lock and records are not mixed together.
///
/// JSON is formatted only with the `debug_log_format_json` feature. Without
/// this feature, a UTF-8 payload is written as plain text.
///
/// # Example
///
/// ```ignore
/// use fastpubsub_sdk::client::create_web_socket;
/// use fastpubsub_sdk::filters::{DebugLogFilter, DebugLogOptions};
///
/// let options = DebugLogOptions::new()
///     .inbound(true)
///     .outbound(true)
///     .write_hex(true);
///
/// let fastpubsub = create_web_socket("overlay_name", "AT_token")
///     .add_filter(
///         "tenant_1",
///         "public.",
///         DebugLogFilter::with_options("debug.log", options)?,
///     )
///     .build()
///     .await?;
/// ```
pub struct DebugLogFilter {
    path: PathBuf,
    file: Arc<Mutex<File>>,
    options: DebugLogOptions,
}

impl DebugLogFilter {
    /// Creates a filter with default options.
    pub fn new(path: impl Into<PathBuf>) -> Result<Self, FilterError> {
        Self::with_options(path, DebugLogOptions::default())
    }

    /// Creates a filter with custom options.
    pub fn with_options(
        path: impl Into<PathBuf>,
        options: DebugLogOptions,
    ) -> Result<Self, FilterError> {
        let path = path.into();
        let file = open_shared_file(&path)
            .map_err(|e| FilterError::Other(format!("failed to open debug log file: {e}")))?;

        Ok(Self {
            path,
            file,
            options,
        })
    }

    /// Returns the log file path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the filter options.
    pub fn options(&self) -> DebugLogOptions {
        self.options
    }

    /// Writes one message to the file.
    fn write_message(
        &self,
        direction: DebugLogDirection,
        ctx: RouteContext<'_>,
        payload: &[u8],
    ) -> Result<(), FilterError> {
        if !self.should_write(direction) {
            return Ok(());
        }

        let payload_text = payload_to_text(payload, self.options.format_json);
        let mut record = String::new();
        record.push_str("---\n");
        record.push_str(&format!("time_ms={}\n", unix_time_ms()));
        record.push_str(&format!("direction={}\n", direction.as_str()));
        record.push_str(&format!("tenant={}\n", ctx.tenant));
        record.push_str(&format!("channel={}\n", ctx.channel));
        record.push_str(&format!("bytes={}\n", payload.len()));
        record.push_str(&format!("format={}\n", payload_text.format));
        record.push_str("payload_start\n");
        record.push_str(&payload_text.body);
        if !payload_text.body.ends_with('\n') {
            record.push('\n');
        }
        record.push_str("payload_end\n");

        if self.options.write_hex && payload_text.format != "hex" {
            record.push_str("hex=");
            record.push_str(&bytes_to_hex(payload));
            record.push('\n');
        }

        {
            let mut file = self
                .file
                .lock()
                .map_err(|_| FilterError::Other("failed to lock debug log file".into()))?;
            file.write_all(record.as_bytes())
                .map_err(|e| FilterError::Other(format!("failed to write debug log file: {e}")))?;
            file.flush()
                .map_err(|e| FilterError::Other(format!("failed to flush debug log file: {e}")))?;
        }
        Ok(())
    }

    /// Checks whether this direction must be written.
    fn should_write(&self, direction: DebugLogDirection) -> bool {
        match direction {
            DebugLogDirection::Inbound => self.options.inbound,
            DebugLogDirection::Outbound => self.options.outbound,
        }
    }
}

impl FilterTrait for DebugLogFilter {
    fn id(&self) -> &'static str {
        "debug_log.file"
    }

    fn apply_outbound(
        &self,
        ctx: RouteContext<'_>,
        payload: Vec<u8>,
    ) -> Result<Vec<Vec<u8>>, FilterError> {
        self.write_message(DebugLogDirection::Outbound, ctx, &payload)?;
        Ok(vec![payload])
    }

    fn apply_inbound(
        &self,
        ctx: RouteContext<'_>,
        payload: Vec<u8>,
    ) -> Result<FilterInboundResult, FilterError> {
        self.write_message(DebugLogDirection::Inbound, ctx, &payload)?;
        Ok(FilterInboundResult::single(payload))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DebugLogDirection {
    Inbound,
    Outbound,
}

impl DebugLogDirection {
    /// Returns the direction name for the file.
    fn as_str(self) -> &'static str {
        match self {
            DebugLogDirection::Inbound => "inbound",
            DebugLogDirection::Outbound => "outbound",
        }
    }
}

struct PayloadText {
    format: &'static str,
    body: String,
}

/// Opens a shared file for the given path.
fn open_shared_file(path: &Path) -> io::Result<Arc<Mutex<File>>> {
    let files = DEBUG_LOG_FILES.get_or_init(|| Mutex::new(HashMap::new()));
    let mut files = files
        .lock()
        .map_err(|_| io::Error::new(io::ErrorKind::Other, "failed to lock debug log registry"))?;

    if let Some(file) = files.get(path) {
        return Ok(Arc::clone(file));
    }

    let file = OpenOptions::new().create(true).append(true).open(path)?;
    let file = Arc::new(Mutex::new(file));
    files.insert(path.to_path_buf(), Arc::clone(&file));
    Ok(file)
}

/// Returns the current time in milliseconds.
fn unix_time_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

/// Converts a payload to log text.
fn payload_to_text(payload: &[u8], format_json: bool) -> PayloadText {
    match std::str::from_utf8(payload) {
        Ok(text) => utf8_to_text(text, format_json),
        Err(_) => PayloadText {
            format: "hex",
            body: bytes_to_hex(payload),
        },
    }
}

/// Converts UTF-8 text to a record body.
fn utf8_to_text(text: &str, format_json: bool) -> PayloadText {
    #[cfg(feature = "debug_log_format_json")]
    {
        if format_json {
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(text) {
                if let Ok(body) = serde_json::to_string_pretty(&value) {
                    return PayloadText {
                        format: "json",
                        body,
                    };
                }
            }
        }
    }

    #[cfg(not(feature = "debug_log_format_json"))]
    {
        let _ = format_json;
    }

    PayloadText {
        format: "utf8",
        body: text.to_string(),
    }
}

/// Converts bytes to a hex string.
fn bytes_to_hex(payload: &[u8]) -> String {
    let mut text = String::new();
    for (index, byte) in payload.iter().enumerate() {
        if index > 0 {
            text.push(' ');
        }
        text.push_str(&format!("{byte:02x}"));
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    /// Creates a route context for a test.
    fn ctx(channel: &'static str) -> RouteContext<'static> {
        RouteContext {
            tenant: "tenant_1",
            channel,
        }
    }

    /// Creates a unique file path for a test.
    fn test_log_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "fastpubsub-debug-log-{name}-{}-{nanos}.log",
            std::process::id()
        ))
    }

    #[test]
    fn debug_log_filter_passes_outbound_payload_and_writes_utf8() {
        let path = test_log_path("outbound");
        let filter = DebugLogFilter::new(&path).unwrap();
        let payload = b"hello".to_vec();

        let result = filter
            .apply_outbound(ctx("public.chat"), payload.clone())
            .unwrap();

        assert_eq!(result, vec![payload]);
        let log = fs::read_to_string(&path).unwrap();
        assert!(log.contains("direction=outbound"));
        assert!(log.contains("tenant=tenant_1"));
        assert!(log.contains("channel=public.chat"));
        assert!(log.contains("format=utf8"));
        assert!(log.contains("hello"));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn debug_log_filter_passes_inbound_payload_and_writes_hex_for_binary() {
        let path = test_log_path("inbound");
        let filter = DebugLogFilter::new(&path).unwrap();
        let payload = vec![0xff, 0x00, 0x10];

        let result = filter
            .apply_inbound(ctx("public.bin"), payload.clone())
            .unwrap();

        assert_eq!(result.payloads, vec![payload]);
        assert!(result.messages.is_empty());
        let log = fs::read_to_string(&path).unwrap();
        assert!(log.contains("direction=inbound"));
        assert!(log.contains("format=hex"));
        assert!(log.contains("ff 00 10"));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn debug_log_filter_can_share_one_file_between_instances() {
        let path = test_log_path("shared");
        let first = DebugLogFilter::new(&path).unwrap();
        let second = DebugLogFilter::new(&path).unwrap();

        first
            .apply_outbound(ctx("public.one"), b"one".to_vec())
            .unwrap();
        second
            .apply_inbound(ctx("public.two"), b"two".to_vec())
            .unwrap();

        let log = fs::read_to_string(&path).unwrap();
        assert!(log.contains("channel=public.one"));
        assert!(log.contains("channel=public.two"));
        assert!(log.contains("direction=outbound"));
        assert!(log.contains("direction=inbound"));
        let _ = fs::remove_file(path);
    }

    #[cfg(feature = "debug_log_format_json")]
    #[test]
    fn debug_log_filter_formats_json_when_feature_is_enabled() {
        let path = test_log_path("json");
        let filter = DebugLogFilter::new(&path).unwrap();

        filter
            .apply_outbound(ctx("public.json"), br#"{"b":1,"a":true}"#.to_vec())
            .unwrap();

        let log = fs::read_to_string(&path).unwrap();
        assert!(log.contains("format=json"));
        assert!(log.contains("\"b\": 1"));
        assert!(log.contains("\"a\": true"));
        let _ = fs::remove_file(path);
    }
}
