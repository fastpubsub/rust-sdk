// Copyright (c) 2026 FastPubSub Network
// SPDX-License-Identifier: MIT OR Apache-2.0
// Project: https://fastpubsub.com

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use aes_gcm::{Aes256Gcm, Nonce as AesNonce};
use chacha20poly1305::aead::{Aead, AeadCore, KeyInit, OsRng, Payload};
use chacha20poly1305::{ChaCha20Poly1305, Nonce as ChaChaNonce};

use crate::metadata::{MetaWriter, META_RECORD_ENCRYPTION};

use super::{FilterError, FilterInboundResult, FilterTrait, RouteContext};

/// Encryption algorithm name for logs and settings.
pub const CHACHA20_POLY1305_ALGORITHM: &str = "chacha20poly1305";

/// AES-GCM algorithm name for logs and settings.
pub const AES256_GCM_ALGORITHM: &str = "aes256-gcm";

/// Key size for supported encryption algorithms.
pub const CRYPTO_KEY_BYTES: usize = 32;

const ENCRYPTION_MAGIC: &[u8] = b"FPSENC1";
const CHACHA20_POLY1305_ID: u8 = 1;
const AES256_GCM_ID: u8 = 2;
const ALGORITHM_BYTES: usize = 1;
const U32_BYTES: usize = 4;
const NONCE_BYTES: usize = 12;

/// Encryption algorithm for [`EncryptionFilter`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncryptionAlgorithm {
    /// ChaCha20-Poly1305.
    ChaCha20Poly1305,
    /// AES-256-GCM.
    Aes256Gcm,
}

impl EncryptionAlgorithm {
    /// Returns the algorithm name.
    pub fn name(self) -> &'static str {
        match self {
            EncryptionAlgorithm::ChaCha20Poly1305 => CHACHA20_POLY1305_ALGORITHM,
            EncryptionAlgorithm::Aes256Gcm => AES256_GCM_ALGORITHM,
        }
    }

    /// Returns the algorithm id for the envelope.
    fn id(self) -> u8 {
        match self {
            EncryptionAlgorithm::ChaCha20Poly1305 => CHACHA20_POLY1305_ID,
            EncryptionAlgorithm::Aes256Gcm => AES256_GCM_ID,
        }
    }

    /// Reads the algorithm from the envelope id.
    fn from_id(id: u8) -> Result<Self, FilterError> {
        match id {
            CHACHA20_POLY1305_ID => Ok(EncryptionAlgorithm::ChaCha20Poly1305),
            AES256_GCM_ID => Ok(EncryptionAlgorithm::Aes256Gcm),
            _ => Err(FilterError::DecryptFailed),
        }
    }
}

impl Default for EncryptionAlgorithm {
    /// Returns the default algorithm.
    fn default() -> Self {
        Self::ChaCha20Poly1305
    }
}

/// Key storage for the encryption filter.
///
/// The manager stores keys only by numeric `key_id`. It does not store tenant,
/// channel, or prefix. The route is selected outside the manager with
/// `add_filter`, so one manager can be shared by several filters on different
/// channels.
///
/// # How Keys Are Chosen
///
/// Outbound messages use the current key. It is set by
/// [`CryptoKeyManager::add_current`].
///
/// Inbound messages use the `key_id` stored in the encrypted payload. This lets
/// the manager keep old keys after rotation and decrypt old messages.
///
/// # Methods
///
/// - [`CryptoKeyManager::add_current`] adds a key and makes it current for new
///   outbound messages.
/// - [`CryptoKeyManager::add_decrypt_key`] adds a key only for decryption and
///   does not change the current key.
/// - [`CryptoKeyManager::remove_decrypt_key`] removes an old decryption key.
///   The current key cannot be removed.
///
/// # Key Rotation
///
/// ```ignore
/// let keys = CryptoKeyManager::new();
///
/// keys.add_current(202606, old_key)?;
/// keys.add_current(202607, new_key)?;
///
/// // New messages are encrypted with key 202607.
/// // Old messages with key_id=202606 can still be decrypted.
/// keys.remove_decrypt_key(202606)?;
/// ```
///
/// # Example
///
/// ```ignore
/// use std::sync::Arc;
///
/// use fastpubsub_sdk::client::create_web_socket;
/// use fastpubsub_sdk::filters::{CryptoKeyManager, EncryptionAlgorithm, EncryptionFilter};
///
/// let keys = Arc::new(CryptoKeyManager::new());
/// keys.add_current(202606, key_bytes)?;
///
/// let fastpubsub = create_web_socket("overlay_name", "AT_token")
///     .add_filter("tenant_1", "public.", EncryptionFilter::new(keys.clone()))
///     .add_filter(
///         "tenant_1",
///         "private.",
///         EncryptionFilter::with_algorithm(keys.clone(), EncryptionAlgorithm::Aes256Gcm),
///     )
///     .build()
///     .await?;
/// ```
pub struct CryptoKeyManager {
    state: RwLock<CryptoKeyState>,
}

struct CryptoKeyState {
    current_key_id: Option<u32>,
    keys: HashMap<u32, [u8; CRYPTO_KEY_BYTES]>,
}

#[derive(Clone)]
struct CryptoKey {
    id: u32,
    bytes: [u8; CRYPTO_KEY_BYTES],
}

struct CryptoEnvelope<'a> {
    algorithm: EncryptionAlgorithm,
    key_id: u32,
    nonce: &'a [u8],
    ciphertext: &'a [u8],
    header: &'a [u8],
}

impl CryptoKeyManager {
    /// Creates an empty manager without keys.
    pub fn new() -> Self {
        Self {
            state: RwLock::new(CryptoKeyState {
                current_key_id: None,
                keys: HashMap::new(),
            }),
        }
    }

    /// Adds a key only for decryption.
    pub fn add_decrypt_key(
        &self,
        key_id: u32,
        key_bytes: impl AsRef<[u8]>,
    ) -> Result<(), FilterError> {
        let key_bytes = validate_key_bytes(key_bytes.as_ref())?;
        let mut state = self
            .state
            .write()
            .map_err(|_| FilterError::Other("failed to lock keys for write".into()))?;

        state.keys.insert(key_id, key_bytes);
        Ok(())
    }

    /// Adds a key and makes it current for new outbound messages.
    ///
    /// The old current key stays in the manager and can decrypt old messages by
    /// its `key_id`.
    pub fn add_current(&self, key_id: u32, key_bytes: impl AsRef<[u8]>) -> Result<(), FilterError> {
        let key_bytes = validate_key_bytes(key_bytes.as_ref())?;
        let mut state = self
            .state
            .write()
            .map_err(|_| FilterError::Other("failed to lock keys for write".into()))?;

        state.keys.insert(key_id, key_bytes);
        state.current_key_id = Some(key_id);
        Ok(())
    }

    /// Removes a decryption key by `key_id`.
    ///
    /// The current key cannot be removed because new outbound messages need it.
    pub fn remove_decrypt_key(&self, key_id: u32) -> Result<bool, FilterError> {
        let mut state = self
            .state
            .write()
            .map_err(|_| FilterError::Other("failed to lock keys for write".into()))?;

        if state.current_key_id == Some(key_id) {
            return Err(FilterError::InvalidConfig(
                "cannot remove the current encryption key".into(),
            ));
        }

        Ok(state.keys.remove(&key_id).is_some())
    }

    /// Returns the current key id.
    pub fn current_key_id(&self) -> Result<Option<u32>, FilterError> {
        let state = self
            .state
            .read()
            .map_err(|_| FilterError::Other("failed to lock keys for read".into()))?;

        Ok(state.current_key_id.clone())
    }

    /// Checks if a key exists for this `key_id`.
    pub fn has_key(&self, key_id: u32) -> Result<bool, FilterError> {
        let state = self
            .state
            .read()
            .map_err(|_| FilterError::Other("failed to lock keys for read".into()))?;

        Ok(state.keys.contains_key(&key_id))
    }

    /// Returns the number of keys.
    pub fn key_count(&self) -> Result<usize, FilterError> {
        let state = self
            .state
            .read()
            .map_err(|_| FilterError::Other("failed to lock keys for read".into()))?;

        Ok(state.keys.len())
    }

    /// Returns the current key for encryption.
    fn current_key(&self) -> Result<CryptoKey, FilterError> {
        let state = self
            .state
            .read()
            .map_err(|_| FilterError::Other("failed to lock keys for read".into()))?;
        let key_id = state.current_key_id.as_ref().ok_or_else(|| {
            FilterError::InvalidConfig("current encryption key is not set".into())
        })?;
        let key_bytes = state.keys.get(key_id).ok_or_else(|| {
            FilterError::InvalidConfig("current encryption key was not found".into())
        })?;

        Ok(CryptoKey {
            id: *key_id,
            bytes: *key_bytes,
        })
    }

    /// Returns a decryption key by `key_id`.
    fn key_by_id(&self, key_id: u32) -> Result<CryptoKey, FilterError> {
        let state = self
            .state
            .read()
            .map_err(|_| FilterError::Other("failed to lock keys for read".into()))?;
        let key_bytes = state.keys.get(&key_id).ok_or_else(|| {
            FilterError::InvalidConfig(format!("encryption key was not found: {key_id}"))
        })?;

        Ok(CryptoKey {
            id: key_id,
            bytes: *key_bytes,
        })
    }
}

impl Default for CryptoKeyManager {
    /// Creates an empty manager without keys.
    fn default() -> Self {
        Self::new()
    }
}

/// Filter for message encryption and decryption.
///
/// This filter encrypts outbound payloads and decrypts inbound payloads inside
/// the normal filter pipeline. The SDK does not exchange keys and does not know
/// where keys come from. The application must create a [`CryptoKeyManager`],
/// put keys into it, and pass the same manager to one or more filters.
///
/// Route selection still belongs to [`crate::client::FastPubSubBuilder::add_filter`].
///
/// # Key Choice
///
/// For outbound messages, the filter uses the current key from
/// [`CryptoKeyManager::add_current`].
///
/// For inbound messages, the filter reads `key_id` from the encrypted payload
/// and gets the matching key from the same manager. This lets old messages be
/// decrypted after key rotation, as long as the old key is still stored in the
/// manager.
///
/// # Payload Format
///
/// The encrypted payload is:
///
/// ```text
/// magic + algorithm_id + key_id(u32) + nonce + ciphertext
/// ```
///
/// `key_id` is stored as a 4 byte big-endian number. The header is also used as
/// AEAD additional data, so changing the algorithm id, key id, or nonce makes
/// decryption fail.
///
/// # Default Algorithm
///
/// [`EncryptionFilter::new`] uses [`EncryptionAlgorithm::ChaCha20Poly1305`].
/// Use [`EncryptionFilter::with_algorithm`] to choose another algorithm.
///
/// # Example: Default Algorithm
///
/// ```ignore
/// use std::sync::Arc;
///
/// use fastpubsub_sdk::client::create_web_socket;
/// use fastpubsub_sdk::filters::{CryptoKeyManager, EncryptionFilter};
///
/// let keys = Arc::new(CryptoKeyManager::new());
/// keys.add_current(1, current_key_bytes)?;
/// keys.add_decrypt_key(0, old_key_bytes)?;
///
/// let fastpubsub = create_web_socket("overlay_name", "AT_token")
///     .add_filter("tenant_1", "private.", EncryptionFilter::new(keys.clone()))
///     .build()
///     .await?;
/// ```
///
/// # Example: AES-256-GCM
///
/// ```ignore
/// use std::sync::Arc;
///
/// use fastpubsub_sdk::client::create_web_socket;
/// use fastpubsub_sdk::filters::{CryptoKeyManager, EncryptionAlgorithm, EncryptionFilter};
///
/// let keys = Arc::new(CryptoKeyManager::new());
/// keys.add_current(7, key_bytes)?;
///
/// let fastpubsub = create_web_socket("overlay_name", "AT_token")
///     .add_filter(
///         "tenant_1",
///         "secure.",
///         EncryptionFilter::with_algorithm(keys.clone(), EncryptionAlgorithm::Aes256Gcm),
///     )
///     .build()
///     .await?;
/// ```
///
/// # Example: One Manager, Several Routes
///
/// ```ignore
/// use std::sync::Arc;
///
/// use fastpubsub_sdk::client::create_web_socket;
/// use fastpubsub_sdk::filters::{CryptoKeyManager, EncryptionFilter};
///
/// let keys = Arc::new(CryptoKeyManager::new());
/// keys.add_current(42, key_bytes)?;
///
/// let fastpubsub = create_web_socket("overlay_name", "AT_token")
///     .add_filter("tenant_1", "chat.", EncryptionFilter::new(keys.clone()))
///     .add_filter("tenant_1", "game.", EncryptionFilter::new(keys.clone()))
///     .build()
///     .await?;
/// ```
pub struct EncryptionFilter {
    keys: Arc<CryptoKeyManager>,
    algorithm: EncryptionAlgorithm,
}

impl EncryptionFilter {
    /// Creates a filter with a shared key manager.
    pub fn new(keys: Arc<CryptoKeyManager>) -> Self {
        Self::with_algorithm(keys, EncryptionAlgorithm::default())
    }

    /// Creates a filter with a shared key manager and a selected algorithm.
    pub fn with_algorithm(keys: Arc<CryptoKeyManager>, algorithm: EncryptionAlgorithm) -> Self {
        Self { keys, algorithm }
    }

    /// Returns the shared key manager.
    pub fn key_manager(&self) -> Arc<CryptoKeyManager> {
        self.keys.clone()
    }

    /// Returns the selected encryption algorithm.
    pub fn algorithm_kind(&self) -> EncryptionAlgorithm {
        self.algorithm
    }

    /// Returns the encryption algorithm name.
    pub fn algorithm(&self) -> &'static str {
        self.algorithm.name()
    }
}

impl FilterTrait for EncryptionFilter {
    fn id(&self) -> &'static str {
        match self.algorithm {
            EncryptionAlgorithm::ChaCha20Poly1305 => "encryption.chacha20poly1305",
            EncryptionAlgorithm::Aes256Gcm => "encryption.aes256-gcm",
        }
    }

    fn apply_outbound(
        &self,
        _ctx: RouteContext<'_>,
        payload: Vec<u8>,
    ) -> Result<Vec<Vec<u8>>, FilterError> {
        let key = self.keys.current_key()?;
        let nonce = make_nonce(self.algorithm);
        let header = encode_header(self.algorithm, key.id, nonce.as_ref())?;
        let ciphertext = encrypt_payload(self.algorithm, &key.bytes, &nonce, &header, &payload)?;

        let mut encrypted = Vec::with_capacity(header.len() + ciphertext.len());
        encrypted.extend_from_slice(&header);
        encrypted.extend_from_slice(&ciphertext);
        Ok(vec![encrypted])
    }

    fn apply_inbound(
        &self,
        _ctx: RouteContext<'_>,
        payload: Vec<u8>,
    ) -> Result<FilterInboundResult, FilterError> {
        let envelope = decode_envelope(&payload)?;
        let key = self.keys.key_by_id(envelope.key_id)?;
        let plain = decrypt_payload(envelope, &key.bytes)?;

        Ok(FilterInboundResult::single(plain))
    }

    fn apply_inbound_with_meta(
        &self,
        _ctx: RouteContext<'_>,
        payload: Vec<u8>,
        meta: &mut MetaWriter,
    ) -> Result<FilterInboundResult, FilterError> {
        let envelope = decode_envelope(&payload)?;
        let key = self.keys.key_by_id(envelope.key_id)?;
        let algorithm = envelope.algorithm;
        let key_id = envelope.key_id;
        let nonce = envelope.nonce.to_vec();
        let plain = decrypt_payload(envelope, &key.bytes)?;

        meta.push_record_with(META_RECORD_ENCRYPTION, |out| {
            out.push(algorithm.id());
            out.extend_from_slice(&key_id.to_be_bytes());
            out.extend_from_slice(&nonce);
            Ok(())
        })
        .map_err(|e| FilterError::Other(e.to_string()))?;

        Ok(FilterInboundResult::single(plain))
    }
}

/// Validates the key size.
fn validate_key_bytes(key_bytes: &[u8]) -> Result<[u8; CRYPTO_KEY_BYTES], FilterError> {
    key_bytes.try_into().map_err(|_| {
        FilterError::InvalidConfig(format!("key must be {CRYPTO_KEY_BYTES} bytes long"))
    })
}

/// Creates a random nonce for the selected algorithm.
fn make_nonce(algorithm: EncryptionAlgorithm) -> [u8; NONCE_BYTES] {
    match algorithm {
        EncryptionAlgorithm::ChaCha20Poly1305 => {
            let nonce = ChaCha20Poly1305::generate_nonce(&mut OsRng);
            nonce.into()
        }
        EncryptionAlgorithm::Aes256Gcm => {
            let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
            nonce.into()
        }
    }
}

/// Encrypts the payload with the selected algorithm.
fn encrypt_payload(
    algorithm: EncryptionAlgorithm,
    key_bytes: &[u8; CRYPTO_KEY_BYTES],
    nonce: &[u8],
    header: &[u8],
    payload: &[u8],
) -> Result<Vec<u8>, FilterError> {
    match algorithm {
        EncryptionAlgorithm::ChaCha20Poly1305 => {
            let cipher = ChaCha20Poly1305::new_from_slice(key_bytes)
                .map_err(|_| FilterError::InvalidConfig("encryption key has a bad size".into()))?;
            cipher
                .encrypt(
                    ChaChaNonce::from_slice(nonce),
                    Payload {
                        msg: payload,
                        aad: header,
                    },
                )
                .map_err(|_| FilterError::EncryptFailed)
        }
        EncryptionAlgorithm::Aes256Gcm => {
            let cipher = Aes256Gcm::new_from_slice(key_bytes)
                .map_err(|_| FilterError::InvalidConfig("encryption key has a bad size".into()))?;
            cipher
                .encrypt(
                    AesNonce::from_slice(nonce),
                    Payload {
                        msg: payload,
                        aad: header,
                    },
                )
                .map_err(|_| FilterError::EncryptFailed)
        }
    }
}

/// Decrypts the payload with the algorithm from the envelope.
fn decrypt_payload(
    envelope: CryptoEnvelope<'_>,
    key_bytes: &[u8; CRYPTO_KEY_BYTES],
) -> Result<Vec<u8>, FilterError> {
    match envelope.algorithm {
        EncryptionAlgorithm::ChaCha20Poly1305 => {
            let cipher = ChaCha20Poly1305::new_from_slice(key_bytes)
                .map_err(|_| FilterError::InvalidConfig("encryption key has a bad size".into()))?;
            cipher
                .decrypt(
                    ChaChaNonce::from_slice(envelope.nonce),
                    Payload {
                        msg: envelope.ciphertext,
                        aad: envelope.header,
                    },
                )
                .map_err(|_| FilterError::DecryptFailed)
        }
        EncryptionAlgorithm::Aes256Gcm => {
            let cipher = Aes256Gcm::new_from_slice(key_bytes)
                .map_err(|_| FilterError::InvalidConfig("encryption key has a bad size".into()))?;
            cipher
                .decrypt(
                    AesNonce::from_slice(envelope.nonce),
                    Payload {
                        msg: envelope.ciphertext,
                        aad: envelope.header,
                    },
                )
                .map_err(|_| FilterError::DecryptFailed)
        }
    }
}

/// Encodes the header before ciphertext.
fn encode_header(
    algorithm: EncryptionAlgorithm,
    key_id: u32,
    nonce: &[u8],
) -> Result<Vec<u8>, FilterError> {
    if nonce.len() != NONCE_BYTES {
        return Err(FilterError::InvalidConfig("nonce has a bad size".into()));
    }

    let mut header =
        Vec::with_capacity(ENCRYPTION_MAGIC.len() + ALGORITHM_BYTES + U32_BYTES + nonce.len());
    header.extend_from_slice(ENCRYPTION_MAGIC);
    header.push(algorithm.id());
    header.extend_from_slice(&key_id.to_be_bytes());
    header.extend_from_slice(nonce);
    Ok(header)
}

/// Decodes the payload with an encrypted message.
fn decode_envelope(payload: &[u8]) -> Result<CryptoEnvelope<'_>, FilterError> {
    if payload.len() < ENCRYPTION_MAGIC.len()
        || &payload[..ENCRYPTION_MAGIC.len()] != ENCRYPTION_MAGIC
    {
        return Err(FilterError::CryptoRequiredButMissing);
    }

    let mut index = ENCRYPTION_MAGIC.len();
    let algorithm_end = index
        .checked_add(ALGORITHM_BYTES)
        .ok_or(FilterError::DecryptFailed)?;
    if algorithm_end > payload.len() {
        return Err(FilterError::DecryptFailed);
    }
    let algorithm = EncryptionAlgorithm::from_id(payload[index])?;
    index = algorithm_end;

    let key_id = read_u32(payload, &mut index)?;

    let nonce_end = index
        .checked_add(NONCE_BYTES)
        .ok_or(FilterError::DecryptFailed)?;
    if nonce_end > payload.len() {
        return Err(FilterError::DecryptFailed);
    }
    let nonce = &payload[index..nonce_end];
    index = nonce_end;

    if index >= payload.len() {
        return Err(FilterError::DecryptFailed);
    }

    Ok(CryptoEnvelope {
        algorithm,
        key_id,
        nonce,
        ciphertext: &payload[index..],
        header: &payload[..index],
    })
}

/// Reads a `u32` number from the payload.
fn read_u32(payload: &[u8], index: &mut usize) -> Result<u32, FilterError> {
    let end = index
        .checked_add(U32_BYTES)
        .ok_or(FilterError::DecryptFailed)?;
    if end > payload.len() {
        return Err(FilterError::DecryptFailed);
    }

    let bytes: [u8; U32_BYTES] = payload[*index..end]
        .try_into()
        .map_err(|_| FilterError::DecryptFailed)?;
    *index = end;
    Ok(u32::from_be_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Creates a test route context.
    fn ctx(channel: &'static str) -> RouteContext<'static> {
        RouteContext {
            tenant: "tenant_1",
            channel,
        }
    }

    /// Creates a test key from one byte.
    fn key(byte: u8) -> [u8; CRYPTO_KEY_BYTES] {
        [byte; CRYPTO_KEY_BYTES]
    }

    /// Checks that the manager does not store tenant or channel.
    #[test]
    fn key_manager_keeps_keys_without_route() {
        let keys = CryptoKeyManager::new();

        keys.add_current(1, key(7)).unwrap();

        assert_eq!(keys.current_key_id().unwrap(), Some(1));
        assert!(keys.has_key(1).unwrap());
        assert_eq!(keys.key_count().unwrap(), 1);
    }

    /// Checks that a new current key does not remove the old key.
    #[test]
    fn add_current_keeps_old_key_for_decrypt() {
        let keys = CryptoKeyManager::new();

        keys.add_current(1, key(1)).unwrap();
        keys.add_current(2, key(2)).unwrap();

        assert_eq!(keys.current_key_id().unwrap(), Some(2));
        assert!(keys.has_key(1).unwrap());
        assert!(keys.has_key(2).unwrap());
        assert_eq!(keys.key_count().unwrap(), 2);
    }

    /// Checks that add_decrypt_key does not change the current key.
    #[test]
    fn add_decrypt_key_does_not_change_current_key() {
        let keys = CryptoKeyManager::new();

        keys.add_current(10, key(1)).unwrap();
        keys.add_decrypt_key(20, key(2)).unwrap();

        assert_eq!(keys.current_key_id().unwrap(), Some(10));
        assert!(keys.has_key(20).unwrap());
    }

    /// Checks removal of an old decryption key.
    #[test]
    fn remove_decrypt_key_removes_old_key() {
        let keys = CryptoKeyManager::new();

        keys.add_current(1, key(1)).unwrap();
        keys.add_current(2, key(2)).unwrap();

        assert!(keys.remove_decrypt_key(1).unwrap());
        assert!(!keys.has_key(1).unwrap());
        assert!(keys.has_key(2).unwrap());
    }

    /// Checks that the current key cannot be removed.
    #[test]
    fn remove_decrypt_key_rejects_current_key() {
        let keys = CryptoKeyManager::new();

        keys.add_current(1, key(1)).unwrap();
        let err = keys.remove_decrypt_key(1).unwrap_err();

        assert!(matches!(err, FilterError::InvalidConfig(_)));
        assert!(keys.has_key(1).unwrap());
        assert_eq!(keys.current_key_id().unwrap(), Some(1));
    }

    /// Checks a full encryption and decryption roundtrip.
    #[test]
    fn encryption_filter_roundtrip_returns_plain_payload() {
        let keys = Arc::new(CryptoKeyManager::new());
        keys.add_current(1, key(1)).unwrap();
        let filter = EncryptionFilter::new(keys);

        let encrypted = filter
            .apply_outbound(ctx("public.chat"), b"hello".to_vec())
            .unwrap()
            .remove(0);
        let plain = filter
            .apply_inbound(ctx("public.chat"), encrypted)
            .unwrap()
            .payloads
            .remove(0);

        assert_eq!(plain, b"hello".to_vec());
    }

    /// Checks a full roundtrip with AES-256-GCM.
    #[test]
    fn aes256_gcm_filter_roundtrip_returns_plain_payload() {
        let keys = Arc::new(CryptoKeyManager::new());
        keys.add_current(1, key(8)).unwrap();
        let filter = EncryptionFilter::with_algorithm(keys, EncryptionAlgorithm::Aes256Gcm);

        let encrypted = filter
            .apply_outbound(ctx("public.chat"), b"hello-aes".to_vec())
            .unwrap()
            .remove(0);
        let plain = filter
            .apply_inbound(ctx("public.chat"), encrypted)
            .unwrap()
            .payloads
            .remove(0);

        assert_eq!(plain, b"hello-aes".to_vec());
        assert_eq!(filter.algorithm_kind(), EncryptionAlgorithm::Aes256Gcm);
        assert_eq!(filter.algorithm(), AES256_GCM_ALGORITHM);
    }

    /// Checks the binary envelope layout.
    #[test]
    fn encrypted_payload_contains_expected_envelope_header() {
        let keys = Arc::new(CryptoKeyManager::new());
        keys.add_current(0x0102_0304, key(8)).unwrap();
        let filter = EncryptionFilter::with_algorithm(keys, EncryptionAlgorithm::Aes256Gcm);

        let encrypted = filter
            .apply_outbound(ctx("public.chat"), b"format".to_vec())
            .unwrap()
            .remove(0);

        let magic_end = ENCRYPTION_MAGIC.len();
        assert_eq!(&encrypted[..magic_end], ENCRYPTION_MAGIC);
        assert_eq!(encrypted[magic_end], AES256_GCM_ID);

        let key_id_start = magic_end + ALGORITHM_BYTES;
        let key_id_end = key_id_start + U32_BYTES;
        assert_eq!(
            &encrypted[key_id_start..key_id_end],
            &0x0102_0304_u32.to_be_bytes()
        );

        let nonce_start = key_id_end;
        let nonce_end = nonce_start + NONCE_BYTES;
        assert!(encrypted.len() > nonce_end);
    }

    /// Checks that changing authenticated bytes breaks decryption.
    #[test]
    fn encrypted_payload_rejects_header_tamper() {
        let keys = Arc::new(CryptoKeyManager::new());
        keys.add_current(1, key(1)).unwrap();
        keys.add_decrypt_key(0, key(2)).unwrap();
        let filter = EncryptionFilter::new(keys);

        let encrypted = filter
            .apply_outbound(ctx("public.chat"), b"hello".to_vec())
            .unwrap()
            .remove(0);

        assert_tamper_fails(&filter, encrypted.clone(), ENCRYPTION_MAGIC.len());
        assert_tamper_fails(
            &filter,
            encrypted.clone(),
            ENCRYPTION_MAGIC.len() + ALGORITHM_BYTES + U32_BYTES - 1,
        );
        assert_tamper_fails(
            &filter,
            encrypted,
            ENCRYPTION_MAGIC.len() + ALGORITHM_BYTES + U32_BYTES,
        );
    }

    /// Checks that changing ciphertext breaks decryption.
    #[test]
    fn encrypted_payload_rejects_ciphertext_tamper() {
        let keys = Arc::new(CryptoKeyManager::new());
        keys.add_current(1, key(1)).unwrap();
        let filter = EncryptionFilter::new(keys);

        let encrypted = filter
            .apply_outbound(ctx("public.chat"), b"hello".to_vec())
            .unwrap()
            .remove(0);
        let ciphertext_index = ENCRYPTION_MAGIC.len() + ALGORITHM_BYTES + U32_BYTES + NONCE_BYTES;

        assert_tamper_fails(&filter, encrypted, ciphertext_index);
    }

    /// Checks that the algorithm is read from the envelope.
    #[test]
    fn inbound_uses_algorithm_from_envelope() {
        let keys = Arc::new(CryptoKeyManager::new());
        keys.add_current(1, key(9)).unwrap();
        let aes_filter =
            EncryptionFilter::with_algorithm(keys.clone(), EncryptionAlgorithm::Aes256Gcm);
        let default_filter = EncryptionFilter::new(keys);

        let encrypted = aes_filter
            .apply_outbound(ctx("private.chat"), b"algorithm-id".to_vec())
            .unwrap()
            .remove(0);
        let plain = default_filter
            .apply_inbound(ctx("private.chat"), encrypted)
            .unwrap()
            .payloads
            .remove(0);

        assert_eq!(plain, b"algorithm-id".to_vec());
    }

    /// Checks that two filters can read one key manager.
    #[test]
    fn several_filters_can_share_one_key_manager() {
        let keys = Arc::new(CryptoKeyManager::new());
        keys.add_current(1, key(2)).unwrap();
        let public_filter = EncryptionFilter::new(keys.clone());
        let private_filter = EncryptionFilter::new(keys);

        let encrypted = public_filter
            .apply_outbound(ctx("public.chat"), b"shared".to_vec())
            .unwrap()
            .remove(0);
        let plain = private_filter
            .apply_inbound(ctx("private.chat"), encrypted)
            .unwrap()
            .payloads
            .remove(0);

        assert_eq!(plain, b"shared".to_vec());
    }

    /// Checks that an old key can decrypt an old message after rotation.
    #[test]
    fn old_key_can_decrypt_old_message_after_rotation() {
        let keys = Arc::new(CryptoKeyManager::new());
        keys.add_current(1, key(3)).unwrap();
        let filter = EncryptionFilter::new(keys.clone());
        let old_payload = filter
            .apply_outbound(ctx("public.chat"), b"old".to_vec())
            .unwrap()
            .remove(0);

        keys.add_current(2, key(4)).unwrap();

        let plain = filter
            .apply_inbound(ctx("public.chat"), old_payload)
            .unwrap()
            .payloads
            .remove(0);

        assert_eq!(plain, b"old".to_vec());
        assert_eq!(keys.current_key_id().unwrap(), Some(2));
    }

    /// Checks the error when an inbound message is not encrypted.
    #[test]
    fn inbound_plain_payload_is_rejected() {
        let keys = Arc::new(CryptoKeyManager::new());
        keys.add_current(1, key(5)).unwrap();
        let filter = EncryptionFilter::new(keys);

        let err = filter
            .apply_inbound(ctx("public.chat"), b"plain".to_vec())
            .unwrap_err();

        assert!(matches!(err, FilterError::CryptoRequiredButMissing));
    }

    /// Changes one byte and checks that decryption fails.
    fn assert_tamper_fails(filter: &EncryptionFilter, mut payload: Vec<u8>, index: usize) {
        payload[index] ^= 0x01;

        let err = filter
            .apply_inbound(ctx("public.chat"), payload)
            .unwrap_err();

        assert!(matches!(err, FilterError::DecryptFailed));
    }
}
