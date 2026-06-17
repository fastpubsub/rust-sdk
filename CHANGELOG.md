# Changelog

All notable changes to `fastpubsub-sdk` are documented in this file.

This project uses semantic versioning for public crate releases.

## 0.1.2 - 2026-06-17

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
