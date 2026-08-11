# Changelog

All notable changes to `fastpubsub-sdk` are documented in this file.

This project uses semantic versioning for public crate releases.

## 0.3.1 - 2026-08-11

### Added

- REST helpers for master-token token lifecycle on the edge API:
  - `refresh_access_token` / `refresh_access_token_with_config` → `PUT /v1/refresh-token`
  - `refresh_access_token_from_at` / `refresh_access_token_from_at_with_config` (full `AT_...`)
  - `revoke_access_token` / `revoke_access_token_with_config` → `DELETE /v1/revoke-token`
- `parse_access_token` and `access_token_id` to split `AT_{token_id}_{secret}`.
- Response types `RefreshAccessTokenResponse`, `RevokeAccessTokenResponse`, and
  request types `RefreshAccessTokenRequest`, `RevokeAccessTokenRequest`.

### Documentation

- README and `llms.txt`: refresh/revoke endpoints listed next to `get-token`.

## 0.3.0 - 2026-06-30

### Added

- WS application `PING`/`PONG` for link quality on an open WebSocket. The client
  sends plain-text `PING`; the perimeter answers `PONG`. RTT is measured only
  in the SDK (`last_rtt_ms`, `median_rtt_ms` over a sliding window).
- `WebSocketBuilder::ping_interval_secs(1|3|5)` and
  `WebSocketConnectParams::ping_interval_secs`. Omit the option to disable WS
  ping. At most one in-flight ping is kept; the next ping waits for `PONG` or a
  timeout.
- `LinkQuality`, `LinkQualitySnapshot`, and `FastPubSub::link_quality()` for
  reading RTT metrics from the transport task.
- `WebSocketEvent::RttMeasured { rtt_ms }` when a pong sample is recorded.
- Publish frame v2 on the wire when `PublishOptions::delivery` is not
  `Broadcast`: byte `0x02`, then `delivery_mode` (`0` broadcast, `1`
  deliver-one-low-latency, `2` deliver-one-random), then tenant, channel, and
  payload. Legacy v1 frames without the tag remain broadcast.

### Changed

- `publish_with_options` encodes `PublishDeliveryMode` instead of ignoring
  transport options on the wire.

### Documentation

- README: HTTP `/ping` (edge selection) vs WS `PING`/`PONG` (link quality on the
  active connection); publish delivery modes and server-side semantics.

### Notes

- `DeliverOneLowLatency` routes to one overlay node with the lowest reported
  inter-node latency  On the edge it delivers to one random subscribed connection, 
  not the client with the lowest network latency.
- `DeliverOneRandom` picks one random overlay node and one random local
  subscribed connection. If there are no subscribers, delivery is a silent
  no-op.

## 0.2.0 - 2026-06-17

### Added

- Added `LatestOnlyFilter` (`latest_only.v1`) for routes where only the newest
  message per client should be delivered. Outbound payloads get a `client_id`
  and monotonic counter prefix; inbound payloads with an older or equal counter
  are dropped.
  counter reset after idle time.
- Added `FilterNotice` and `FilterNoticeLevel` so filters can emit informational
  and warning events (for example when a stale message is dropped).
- Added `WebSocketEvent::FilterNotice` so filter notices can be read from the
  WebSocket event loop.

## 0.1.1 - 2026-06-12

### Changed

- Renamed the published crate package to `fastpubsub-sdk`.
- Updated README dependency examples to use the official crates.io package.
- Improved README layout for filters and repository examples.
- Updated crate metadata for repository, documentation, keywords, and categories.

## 0.1.0 - 2026-06-12

### Added

- Initial Rust SDK release for FastPubSubNetwork contract v0.1.
- Added WebSocket client support, REST helpers, access token builder, filters,
  helper types, and repository examples.
