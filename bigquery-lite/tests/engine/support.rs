//! How the gates reach an engine: a fresh root, served on a loopback port, and a client built
//! through the `bigquery` client's public URL constructors, as `bigquery::testing` reaches its
//! fake. [`client`] is the one place that names the constructor.

use bigquery::{BigQueryDb, BigQueryDbOptions};
use bigquery_lite::{Engine, EngineConfig, TcpEndpoint};
use std::net::SocketAddr;
use tempfile::TempDir;
use tokio::task::JoinHandle;

/// The project every gate's client works in.
pub(crate) const PROJECT: &str = "lite-project";

/// An engine on a temporary root, serving on a loopback port until dropped.
pub(crate) struct Served {
    pub engine: Engine,
    pub addr: SocketAddr,
    server: JoinHandle<()>,
    _root: TempDir,
}

impl Drop for Served {
    fn drop(&mut self) {
        self.server.abort();
    }
}

/// Opens an engine on a new temporary root and serves it on a free loopback port.
pub(crate) async fn serve() -> Served {
    let root = TempDir::new().expect("a temporary root");
    let url = url::Url::from_directory_path(root.path()).expect("the temporary root is absolute");
    let engine = Engine::open(EngineConfig::new(url))
        .await
        .expect("the engine opens");
    let server = engine
        .bind_tcp(TcpEndpoint::new(SocketAddr::from(([127, 0, 0, 1], 0))))
        .await
        .expect("a loopback port binds");
    let addr = server.local_addr();
    let server = tokio::spawn(async move {
        server.serve().await.expect("the engine serves");
    });
    Served {
        engine,
        addr,
        server,
        _root: root,
    }
}

/// A client whose v2 and Storage endpoints both point at `addr`, with a token the engine never
/// checks.
pub(crate) async fn client(addr: SocketAddr) -> BigQueryDb {
    let endpoint = url::Url::parse(&format!("http://{addr}")).expect("a loopback URL");
    let options = BigQueryDbOptions::new(PROJECT.to_string())
        .with_bigquery_api_url(endpoint.clone())
        .with_bigquery_storage_api_url(endpoint);
    BigQueryDb::with_options_token_source(
        options,
        Vec::new(),
        gcloud_sdk::TokenSourceType::ExternalSource(Box::new(UncheckedToken)),
    )
    .await
    .expect("the client connects")
}

/// A token for an engine that checks none.
struct UncheckedToken;

#[async_trait::async_trait]
impl gcloud_sdk::Source for UncheckedToken {
    async fn token(&self) -> gcloud_sdk::error::Result<gcloud_sdk::Token> {
        Ok(gcloud_sdk::Token::new(
            "Bearer".to_string(),
            "unchecked".to_string().into(),
            bigquery::jiff::Timestamp::MAX,
        ))
    }
}
