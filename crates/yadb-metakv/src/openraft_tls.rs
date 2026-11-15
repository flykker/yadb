//! Opt-in TLS 1.3 mutual authentication for the Raft listener.
//! Trust is provisioned out of band: a CA validates certificates and an exact
//! leaf-certificate SHA-256 pin binds each certificate to one node ID.

use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    sync::Arc,
};
use tokio_rustls::{
    rustls::{
        self,
        pki_types::{CertificateDer, PrivateKeyDer, ServerName},
    },
    TlsAcceptor, TlsConnector,
};

const ALPN: &[u8] = b"yadb-openraft/1";

#[derive(Clone)]
pub struct MutualTls {
    pub(crate) client: TlsConnector,
    pub(crate) server: TlsAcceptor,
    names: BTreeMap<u32, ServerName<'static>>,
    pins: BTreeMap<u32, [u8; 32]>,
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;

    pub fn cert(node: u32) -> &'static [u8] {
        match node {
            1 => include_bytes!("../testdata/tls/node-1.test.cert.der"),
            2 => include_bytes!("../testdata/tls/node-2.test.cert.der"),
            3 => include_bytes!("../testdata/tls/node-3.test.cert.der"),
            4 => include_bytes!("../testdata/tls/untrusted.test.cert.der"),
            5 => include_bytes!("../testdata/tls/expired.test.cert.der"),
            _ => panic!("unknown test certificate"),
        }
    }

    pub fn config(node: u32) -> MutualTls {
        let key: &[u8] = match node {
            1 => include_bytes!("../testdata/tls/node-1.test.key.der"),
            2 => include_bytes!("../testdata/tls/node-2.test.key.der"),
            3 => include_bytes!("../testdata/tls/node-3.test.key.der"),
            4 => include_bytes!("../testdata/tls/untrusted.test.key.der"),
            5 => include_bytes!("../testdata/tls/expired.test.key.der"),
            _ => panic!("unknown test key"),
        };
        MutualTls::new(
            vec![CertificateDer::from(
                include_bytes!("../testdata/tls/ca.der").to_vec(),
            )],
            vec![CertificateDer::from(cert(node).to_vec())],
            PrivateKeyDer::Pkcs8(key.to_vec().into()),
            (1..=3).map(|n| (n, format!("node-{n}.test"))).collect(),
            (1..=3)
                .map(|n| (n, MutualTls::fingerprint(cert(n))))
                .collect(),
        )
        .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;
    use tokio::net::{TcpListener, TcpStream};

    #[tokio::test]
    async fn tls_rejects_client_without_certificate() {
        let server = test_support::config(3);
        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(CertificateDer::from(
                include_bytes!("../testdata/tls/ca.der").to_vec(),
            ))
            .unwrap();
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut client = rustls::ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
        client.alpn_protocols = vec![ALPN.to_vec()];
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            server.server.accept(stream).await.is_err()
        });
        let stream = TcpStream::connect(addr).await.unwrap();
        if let Ok(mut stream) = TlsConnector::from(Arc::new(client))
            .connect(ServerName::try_from("node-3.test").unwrap(), stream)
            .await
        {
            let _ = stream.read_u8().await;
        }
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(2), task)
                .await
                .unwrap()
                .unwrap()
        );
    }

    #[tokio::test]
    async fn tls_rejects_wrong_server_hostname_and_certificate_pin() {
        for wrong_name in [true, false] {
            let server = test_support::config(3);
            let mut client = test_support::config(1);
            if wrong_name {
                client.names.insert(
                    3,
                    ServerName::try_from("not-node-3.test".to_string()).unwrap(),
                );
            } else {
                client.pins.insert(3, [0; 32]);
            }
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let task = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                if let Ok(mut stream) = server.server.accept(stream).await {
                    // Keep the server alive until the client closes its side.
                    let _ = stream.read_u8().await;
                }
            });
            let stream = TcpStream::connect(addr).await.unwrap();
            let result = client.client.connect(client.name(3).unwrap(), stream).await;
            if wrong_name {
                assert!(result.is_err());
            } else {
                let stream = result.unwrap();
                let session = stream.get_ref().1;
                assert!(client
                    .peer(session.peer_certificates(), session.alpn_protocol())
                    .is_err());
                drop(stream);
            }
            tokio::time::timeout(std::time::Duration::from_secs(2), task)
                .await
                .unwrap()
                .unwrap();
        }
    }
}

impl MutualTls {
    /// Uses DER-encoded certificates and PKCS#8/PKCS#1 private keys. Never
    /// disables certificate, hostname or time validation. No plaintext fallback.
    pub fn new(
        roots: Vec<CertificateDer<'static>>,
        chain: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
        names: BTreeMap<u32, String>,
        pins: BTreeMap<u32, [u8; 32]>,
    ) -> io::Result<Self> {
        let invalid = |e| io::Error::new(io::ErrorKind::InvalidInput, e);
        if roots.is_empty()
            || chain.is_empty()
            || pins.is_empty()
            || pins.contains_key(&0)
            || pins.values().collect::<BTreeSet<_>>().len() != pins.len()
            || names.keys().ne(pins.keys())
        {
            return Err(invalid(
                "nonempty unique certificate pins and matching node names required".to_string(),
            ));
        }
        let names = names
            .into_iter()
            .map(|(id, name)| {
                ServerName::try_from(name)
                    .map(|name| (id, name))
                    .map_err(|e| invalid(e.to_string()))
            })
            .collect::<io::Result<_>>()?;
        let mut trust = rustls::RootCertStore::empty();
        for root in roots {
            trust.add(root).map_err(|e| invalid(e.to_string()))?;
        }
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
            Arc::new(trust.clone()),
            provider.clone(),
        )
        .build()
        .map_err(|e| invalid(e.to_string()))?;
        let mut server = rustls::ServerConfig::builder_with_provider(provider.clone())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(|e| invalid(e.to_string()))?
            .with_client_cert_verifier(verifier)
            .with_single_cert(chain.clone(), key.clone_key())
            .map_err(|e| invalid(e.to_string()))?;
        server.alpn_protocols = vec![ALPN.to_vec()];
        server.send_tls13_tickets = 0;
        let mut client = rustls::ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(|e| invalid(e.to_string()))?
            .with_root_certificates(trust)
            .with_client_auth_cert(chain, key)
            .map_err(|e| invalid(e.to_string()))?;
        client.alpn_protocols = vec![ALPN.to_vec()];
        client.resumption = rustls::client::Resumption::disabled();
        Ok(Self {
            client: TlsConnector::from(Arc::new(client)),
            server: TlsAcceptor::from(Arc::new(server)),
            names,
            pins,
        })
    }

    pub fn fingerprint(cert: &[u8]) -> [u8; 32] {
        ring::digest::digest(&ring::digest::SHA256, cert)
            .as_ref()
            .try_into()
            .unwrap()
    }

    pub(crate) fn name(&self, node: u32) -> io::Result<ServerName<'static>> {
        self.names.get(&node).cloned().ok_or_else(|| {
            io::Error::new(io::ErrorKind::PermissionDenied, "TLS peer not provisioned")
        })
    }

    pub(crate) fn peer(
        &self,
        certs: Option<&[CertificateDer<'_>]>,
        alpn: Option<&[u8]>,
    ) -> io::Result<u32> {
        let denied = || {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "TLS certificate pin or ALPN mismatch",
            )
        };
        if alpn != Some(ALPN) {
            return Err(denied());
        }
        let cert = certs.and_then(|chain| chain.first()).ok_or_else(denied)?;
        let pin = Self::fingerprint(cert.as_ref());
        self.pins
            .iter()
            .find_map(|(node, known)| (*known == pin).then_some(*node))
            .ok_or_else(denied)
    }
}
