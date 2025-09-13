use std::sync::OnceLock;

use quinn::{
    ClientConfig, ServerConfig,
    rustls::{
        RootCertStore,
        pki_types::{CertificateDer, pem::PemObject},
    },
};

const CERT: &str = include_str!("cert.pem");
const KEY_PAIR: &str = include_str!("key_pair.pem");

pub fn server_config() -> ServerConfig {
    static CONFIG: OnceLock<ServerConfig> = OnceLock::new();
    CONFIG
        .get_or_init(|| {
            let cert = CertificateDer::from_pem_slice(CERT.as_bytes()).unwrap();
            let key =
                quinn::rustls::pki_types::PrivatePkcs8KeyDer::from_pem_slice(KEY_PAIR.as_bytes())
                    .unwrap();
            ServerConfig::with_single_cert(vec![cert], key.into()).unwrap()
        })
        .clone()
}

pub fn client_config() -> ClientConfig {
    static CONFIG: OnceLock<ClientConfig> = OnceLock::new();
    CONFIG
        .get_or_init(|| {
            let mut roots = RootCertStore::empty();
            roots
                .add(CertificateDer::from_pem_slice(CERT.as_bytes()).unwrap())
                .unwrap();
            ClientConfig::with_root_certificates(roots.into()).unwrap()
        })
        .clone()
}
