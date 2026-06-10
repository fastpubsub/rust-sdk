// Copyright (c) 2026 FastPubSub Network
// SPDX-License-Identifier: MIT OR Apache-2.0
// Project: https://fastpubsub.com

//! Trust store: public webpki roots + company CA (MITM).

use std::fs;

#[cfg(feature = "websocket")]
use std::io::Cursor;
#[cfg(feature = "websocket")]
use std::sync::Arc;

#[cfg(feature = "websocket")]
use tokio_rustls::rustls::pki_types::CertificateDer;
#[cfg(feature = "websocket")]
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
#[cfg(feature = "websocket")]
use tokio_rustls::TlsConnector;

use super::http_config::{HttpClientConfig, HttpConfigError};

/// Builds root store: webpki + extra CA from config.
#[cfg(feature = "websocket")]
pub fn build_root_cert_store(config: &HttpClientConfig) -> Result<RootCertStore, HttpConfigError> {
    let mut store = RootCertStore::empty();
    if config.trust_webpki_roots {
        store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }
    for pem in load_all_ca_pem(config)? {
        add_certs_from_pem(&mut store, &pem)?;
    }
    if store.is_empty() {
        return Err(HttpConfigError(
            "empty trust store: enable webpki or add CA".into(),
        ));
    }
    Ok(store)
}

/// `rustls` [`ClientConfig`] for WSS.
#[cfg(feature = "websocket")]
pub fn build_rustls_client_config(
    config: &HttpClientConfig,
) -> Result<Arc<ClientConfig>, HttpConfigError> {
    let store = build_root_cert_store(config)?;
    Ok(Arc::new(
        ClientConfig::builder()
            .with_root_certificates(store)
            .with_no_client_auth(),
    ))
}

/// TLS connector for `tokio-tungstenite`.
#[cfg(feature = "websocket")]
pub fn build_tls_connector(config: &HttpClientConfig) -> Result<TlsConnector, HttpConfigError> {
    Ok(TlsConnector::from(build_rustls_client_config(config)?))
}

/// Reads all PEM blocks from memory and disk.
pub(crate) fn load_all_ca_pem(config: &HttpClientConfig) -> Result<Vec<Vec<u8>>, HttpConfigError> {
    let mut out = Vec::new();
    for pem in &config.extra_ca_pem {
        if !pem.trim().is_empty() {
            out.push(pem.as_bytes().to_vec());
        }
    }
    for path in &config.extra_ca_cert_paths {
        let bytes =
            fs::read(path).map_err(|e| HttpConfigError(format!("reading CA {path}: {e}")))?;
        if !bytes.is_empty() {
            out.push(bytes);
        }
    }
    Ok(out)
}

/// Parses a PEM file. One file may contain several `CERTIFICATE` blocks.
#[cfg(feature = "websocket")]
fn add_certs_from_pem(store: &mut RootCertStore, pem: &[u8]) -> Result<(), HttpConfigError> {
    let mut reader = Cursor::new(pem);
    let certs = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| HttpConfigError(format!("parsing PEM CA: {e}")))?;
    if certs.is_empty() {
        return Err(HttpConfigError("PEM has no CERTIFICATE blocks".to_string()));
    }
    for cert in certs {
        store
            .add(CertificateDer::from(cert))
            .map_err(|e| HttpConfigError(format!("adding CA to store: {e}")))?;
    }
    Ok(())
}

/// Splits a buffer into separate PEM blocks for `reqwest::Certificate::from_pem`.
#[cfg(feature = "rest")]
pub(crate) fn split_pem_certificates(pem: &[u8]) -> Vec<&[u8]> {
    let text = match std::str::from_utf8(pem) {
        Ok(s) => s,
        Err(_) => return vec![pem],
    };
    let marker = "-----BEGIN CERTIFICATE-----";
    let mut blocks = Vec::new();
    let mut start = 0usize;
    while let Some(off) = text[start..].find(marker) {
        let begin = start + off;
        if let Some(end_off) = text[begin..].find("-----END CERTIFICATE-----") {
            let end = begin + end_off + "-----END CERTIFICATE-----".len();
            blocks.push(&pem[begin..end]);
            start = end;
        } else {
            break;
        }
    }
    if blocks.is_empty() {
        blocks.push(pem);
    }
    blocks
}
