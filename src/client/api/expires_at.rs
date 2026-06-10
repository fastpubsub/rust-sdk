// Copyright (c) 2026 FastPubSub Network
// SPDX-License-Identifier: MIT OR Apache-2.0
// Project: https://fastpubsub.com

//! Parses `expires_at` for `POST /v1/get-token`: RFC3339, dates without TZ, relative time.

use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Utc};

/// Maximum token TTL on server, in hours.
pub const MAX_TOKEN_TTL_HOURS: i64 = 24;

/// Default base "now" shift in `CreateAccessTokenBuilder`.
pub const DEFAULT_NOW_MARGIN_SECS: i64 = 1;

/// Same as [`DEFAULT_NOW_MARGIN_SECS`] (old name).
pub const MAX_TOKEN_TTL_MARGIN_SECS: i64 = DEFAULT_NOW_MARGIN_SECS;

/// Error while parsing expiration string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExpiresAtParseError {
    /// Empty or unknown string.
    InvalidFormat(String),
    /// Date is after allowed TTL: 24 hours from "now".
    TtlTooLong {
        /// Parsed time.
        parsed: DateTime<Utc>,
        /// Upper bound on server.
        max_allowed: DateTime<Utc>,
    },
}

impl std::fmt::Display for ExpiresAtParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExpiresAtParseError::InvalidFormat(s) => {
                write!(f, "cannot parse expiration time: {s}")
            }
            ExpiresAtParseError::TtlTooLong {
                parsed,
                max_allowed,
            } => write!(
                f,
                "expiration {parsed} is after maximum {max_allowed} (TTL {MAX_TOKEN_TTL_HOURS} h)"
            ),
        }
    }
}

impl std::error::Error for ExpiresAtParseError {}

/// Base "now": `Utc::now() - now_margin_secs` (0 means no shift).
pub fn utc_now_with_margin_secs(now_margin_secs: i64) -> DateTime<Utc> {
    let margin = now_margin_secs.max(0);
    Utc::now() - chrono::Duration::seconds(margin)
}

/// Same as [`utc_now_with_margin_secs`] with [`DEFAULT_NOW_MARGIN_SECS`].
pub fn utc_now_with_margin() -> DateTime<Utc> {
    utc_now_with_margin_secs(DEFAULT_NOW_MARGIN_SECS)
}

/// Seconds in full TTL (24 h).
pub fn max_ttl_seconds() -> i64 {
    MAX_TOKEN_TTL_HOURS * 3600
}

/// Server upper bound for `expires_at`: base "now" + 24 h.
pub fn expires_at_server_max_with_margin(now_margin_secs: i64) -> DateTime<Utc> {
    utc_now_with_margin_secs(now_margin_secs) + chrono::Duration::seconds(max_ttl_seconds())
}

/// Maximum TTL for `max`, `+24h`, and clamping.
pub fn expires_at_max_ttl_with_margin(now_margin_secs: i64) -> DateTime<Utc> {
    expires_at_server_max_with_margin(now_margin_secs)
}

/// Same as [`expires_at_max_ttl_with_margin`] with [`DEFAULT_NOW_MARGIN_SECS`].
pub fn expires_at_max_ttl() -> DateTime<Utc> {
    expires_at_max_ttl_with_margin(DEFAULT_NOW_MARGIN_SECS)
}

/// Relative expiration: base "now" + `secs`, clamped by TTL.
pub fn expires_at_after_seconds_with_margin(secs: i64, now_margin_secs: i64) -> DateTime<Utc> {
    clamp_to_max_ttl(
        utc_now_with_margin_secs(now_margin_secs) + chrono::Duration::seconds(secs),
        now_margin_secs,
    )
}

/// Same as [`expires_at_after_seconds_with_margin`] with [`DEFAULT_NOW_MARGIN_SECS`].
pub fn expires_at_after_seconds(secs: i64) -> DateTime<Utc> {
    expires_at_after_seconds_with_margin(secs, DEFAULT_NOW_MARGIN_SECS)
}

/// Formats UTC for `right.expires_at`, as accepted by API.
pub fn format_expires_at_rfc3339_z(dt: DateTime<Utc>) -> String {
    dt.format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

/// Parses user input into ISO UTC for `expires_at`.
pub fn parse_expires_at_input_with_margin(
    input: &str,
    now_margin_secs: i64,
) -> Result<String, ExpiresAtParseError> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(ExpiresAtParseError::InvalidFormat(
            "empty string".to_string(),
        ));
    }
    let dt = parse_expires_at_datetime(trimmed, now_margin_secs)?;
    ensure_within_max_ttl(dt, now_margin_secs)?;
    Ok(format_expires_at_rfc3339_z(dt))
}

/// Same as [`parse_expires_at_input_with_margin`] with [`DEFAULT_NOW_MARGIN_SECS`].
pub fn parse_expires_at_input(input: &str) -> Result<String, ExpiresAtParseError> {
    parse_expires_at_input_with_margin(input, DEFAULT_NOW_MARGIN_SECS)
}

/// Same as [`parse_expires_at_input`], but clamps to max when TTL is exceeded.
pub fn parse_expires_at_input_clamp_with_margin(
    input: &str,
    now_margin_secs: i64,
) -> Result<String, ExpiresAtParseError> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(ExpiresAtParseError::InvalidFormat(
            "empty string".to_string(),
        ));
    }
    let dt = parse_expires_at_datetime(trimmed, now_margin_secs)?;
    Ok(format_expires_at_rfc3339_z(clamp_to_max_ttl(
        dt,
        now_margin_secs,
    )))
}

/// Same as [`parse_expires_at_input_clamp_with_margin`] with [`DEFAULT_NOW_MARGIN_SECS`].
pub fn parse_expires_at_input_clamp(input: &str) -> Result<String, ExpiresAtParseError> {
    parse_expires_at_input_clamp_with_margin(input, DEFAULT_NOW_MARGIN_SECS)
}

fn parse_expires_at_datetime(
    input: &str,
    now_margin_secs: i64,
) -> Result<DateTime<Utc>, ExpiresAtParseError> {
    let lower = input.to_ascii_lowercase();
    if matches!(lower.as_str(), "max" | "max_ttl" | "24h") {
        return Ok(expires_at_max_ttl_with_margin(now_margin_secs));
    }
    if let Some(dt) = parse_relative(&lower, now_margin_secs) {
        return Ok(clamp_to_max_ttl(dt, now_margin_secs));
    }
    parse_absolute(input)
}

fn parse_relative(lower: &str, now_margin_secs: i64) -> Option<DateTime<Utc>> {
    let body = lower.strip_prefix('+').unwrap_or(lower);
    let (num_str, unit) = split_duration_token(body)?;
    let amount: i64 = num_str.parse().ok()?;
    if amount < 0 {
        return None;
    }
    let secs = duration_amount_to_seconds(amount, unit)?;
    Some(expires_at_after_seconds_with_margin(secs, now_margin_secs))
}

fn split_duration_token(s: &str) -> Option<(&str, &str)> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let mut split = 0usize;
    for (i, ch) in s.char_indices() {
        if ch.is_ascii_digit() {
            split = i + ch.len_utf8();
        } else {
            break;
        }
    }
    if split == 0 {
        return None;
    }
    Some((&s[..split], s[split..].trim()))
}

fn duration_amount_to_seconds(amount: i64, unit: &str) -> Option<i64> {
    let secs = match unit {
        "" | "s" | "sec" | "secs" | "second" | "seconds" => amount,
        "m" | "min" | "mins" | "minute" | "minutes" => amount.checked_mul(60)?,
        "h" | "hr" | "hrs" | "hour" | "hours" => amount.checked_mul(3600)?,
        // Days: server token TTL is no more than 24 h, so we do not accept it yet.
        // "d" | "day" | "days" => amount.checked_mul(86400)?,
        _ => return None,
    };
    Some(secs)
}

fn parse_absolute(input: &str) -> Result<DateTime<Utc>, ExpiresAtParseError> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(input) {
        return Ok(dt.with_timezone(&Utc));
    }
    if let Ok(ndt) = NaiveDateTime::parse_from_str(input, "%Y-%m-%dT%H:%M:%S") {
        return Ok(Utc.from_utc_datetime(&ndt));
    }
    if let Ok(ndt) = NaiveDateTime::parse_from_str(input, "%Y-%m-%d %H:%M:%S") {
        return Ok(Utc.from_utc_datetime(&ndt));
    }
    if let Ok(date) = NaiveDate::parse_from_str(input, "%Y-%m-%d") {
        let ndt = date.and_hms_opt(23, 59, 59).ok_or_else(|| invalid(input))?;
        return Ok(Utc.from_utc_datetime(&ndt));
    }
    Err(invalid(input))
}

fn invalid(input: &str) -> ExpiresAtParseError {
    ExpiresAtParseError::InvalidFormat(format!(
        "expected RFC3339, date YYYY-MM-DD, +1h/+30m, or max; got: {input}"
    ))
}

fn clamp_to_max_ttl(dt: DateTime<Utc>, now_margin_secs: i64) -> DateTime<Utc> {
    let max = expires_at_server_max_with_margin(now_margin_secs);
    if dt > max {
        expires_at_max_ttl_with_margin(now_margin_secs)
    } else {
        dt
    }
}

fn ensure_within_max_ttl(
    dt: DateTime<Utc>,
    now_margin_secs: i64,
) -> Result<(), ExpiresAtParseError> {
    let max = expires_at_server_max_with_margin(now_margin_secs);
    if dt > max {
        return Err(ExpiresAtParseError::TtlTooLong {
            parsed: dt,
            max_allowed: max,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relative_hours() {
        let iso = parse_expires_at_input_clamp("+1h").expect("ok");
        assert!(iso.ends_with('Z'));
    }

    #[test]
    fn max_alias() {
        let iso = parse_expires_at_input("max").expect("ok");
        let parsed = DateTime::parse_from_rfc3339(&iso)
            .unwrap()
            .with_timezone(&Utc);
        let ceiling = expires_at_max_ttl();
        let diff = (parsed - ceiling).num_seconds().abs();
        assert!(diff <= 2, "parsed={parsed} ceiling={ceiling}");
    }

    #[test]
    fn zero_margin_uses_wall_now() {
        let with_margin = expires_at_after_seconds_with_margin(3600, 1);
        let no_margin = expires_at_after_seconds_with_margin(3600, 0);
        assert!(with_margin < no_margin);
    }

    #[test]
    fn date_only_end_of_day() {
        let iso = parse_expires_at_input("2020-06-10").expect("ok");
        assert_eq!(iso, "2020-06-10T23:59:59Z");
    }

    #[test]
    fn rfc3339_passthrough() {
        let iso = parse_expires_at_input("2026-05-16T10:00:00Z").expect("ok");
        assert_eq!(iso, "2026-05-16T10:00:00Z");
    }
}
