use crate::error::Result;
use quinn::{ClientConfig, ServerConfig};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::sync::Arc;

/// Generate a self-signed certificate
pub fn generate_self_signed_cert() -> Result<(CertificateDer<'static>, PrivateKeyDer<'static>)> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    let key = PrivateKeyDer::Pkcs8(cert.key_pair.serialize_der().into());
    let cert_der = CertificateDer::from(cert.cert);

    Ok((cert_der, key))
}

/// Create server config with self-signed certificate
pub fn create_server_config(
    cert: CertificateDer<'static>,
    key: PrivateKeyDer<'static>,
) -> Result<ServerConfig> {
    // Install crypto provider if not already installed
    let _ =
        rustls::crypto::CryptoProvider::install_default(rustls::crypto::ring::default_provider());

    let mut crypto = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert], key)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    // Allow TLS 1.3 only for performance
    crypto.alpn_protocols = vec![b"octopii".to_vec()];

    let mut server_config = ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(crypto)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?,
    ));

    // Performance tuning
    let mut transport_config = quinn::TransportConfig::default();
    transport_config.max_concurrent_bidi_streams(1024u32.into());
    transport_config.max_concurrent_uni_streams(1024u32.into());

    server_config.transport_config(Arc::new(transport_config));

    Ok(server_config)
}

/// Create client config that accepts any certificate (for simplicity)
pub fn create_client_config() -> Result<ClientConfig> {
    // Install crypto provider if not already installed
    let _ =
        rustls::crypto::CryptoProvider::install_default(rustls::crypto::ring::default_provider());

    // For a minimal setup, we'll accept any certificate
    // In production, you'd want proper certificate validation
    let mut crypto = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(SkipServerVerification::new()))
        .with_no_client_auth();

    // Set ALPN protocol to match server
    crypto.alpn_protocols = vec![b"octopii".to_vec()];

    let mut client_config = ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(crypto)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?,
    ));

    // Performance tuning
    let mut transport_config = quinn::TransportConfig::default();
    transport_config.max_concurrent_bidi_streams(1024u32.into());
    transport_config.max_concurrent_uni_streams(1024u32.into());

    client_config.transport_config(Arc::new(transport_config));

    Ok(client_config)
}

/// Certificate verifier that accepts any certificate
/// WARNING: Only use for testing/development!
#[derive(Debug)]
struct SkipServerVerification(Arc<rustls::crypto::CryptoProvider>);

impl SkipServerVerification {
    fn new() -> Self {
        Self(Arc::new(rustls::crypto::ring::default_provider()))
    }
}

impl rustls::client::danger::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}
