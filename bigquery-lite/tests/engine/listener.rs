//! The TCP listener's address rules.

use bigquery_lite::{Engine, EngineConfig, EngineError, TcpEndpoint};
use std::net::SocketAddr;
use tempfile::TempDir;

#[tokio::test]
async fn a_listener_on_an_address_that_is_not_loopback_is_refused() {
    let root = TempDir::new().expect("a temporary root");
    let url = url::Url::from_directory_path(root.path()).expect("the temporary root is absolute");
    let engine = Engine::open(EngineConfig::new(url))
        .await
        .expect("the engine opens");
    for addr in [
        SocketAddr::from(([192, 0, 2, 1], 9000)),
        SocketAddr::from(([0, 0, 0, 0], 0)),
    ] {
        let refused = engine.serve_tcp(TcpEndpoint::new(addr)).await;
        assert!(
            matches!(refused, Err(EngineError::Config { field: "addr", .. })),
            "{addr}: {refused:?}"
        );
    }
}
