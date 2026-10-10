use crate::catalog::{CatalogSettings, EngineCatalog};
use crate::config::{EngineConfig, TcpEndpoint};
use crate::error::EngineError;
use crate::runtime::IcebergRuntime;
use crate::services::{Route, Services};
use crate::storage::{Backend, Created, ObjectPath};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;
use tokio::io::DuplexStream;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::{TcpListenerStream, UnboundedReceiverStream};
use tonic::transport::{Channel, Endpoint, Server};
use tracing::{info, warn};

/// The buffer of one in-process connection, in each direction.
const DUPLEX_BUFFER: usize = 1024 * 1024;

/// The layout version of a root, which `_smallquery/engine.json` records.
const FORMAT_VERSION: u32 = 1;

/// What `_smallquery/engine.json` holds: written once, when the root is initialized.
#[derive(Debug, Serialize, Deserialize)]
struct RootMarker {
    format_version: u32,
    created_at_ms: i64,
}

/// An engine over one root. It serves the BigQuery v2 and Storage gRPC APIs in process through
/// [`Engine::channel`] and on loopback TCP through [`Engine::serve_tcp`].
///
/// Dropping the engine stops its in-process server and every TCP server it started.
pub struct Engine {
    inner: Arc<EngineInner>,
}

struct EngineInner {
    catalog: Arc<EngineCatalog>,
    channel: Channel,
    shutdown: watch::Sender<bool>,
    in_process: Mutex<Option<JoinHandle<()>>>,
}

impl Drop for EngineInner {
    fn drop(&mut self) {
        self.shutdown.send_replace(true);
    }
}

impl Engine {
    /// Opens the root of `config`, initializing an empty one.
    ///
    /// # Errors
    /// [`EngineError::Config`] for an unusable setting, a root of another layout version or a
    /// scheme without a backend, and the storage errors of reading the root.
    pub async fn open(config: EngineConfig) -> Result<Self, EngineError> {
        config.validate()?;
        let backend = Backend::open(&config.root, &config.storage)?;
        RootMarker::initialize(&backend, &config.root).await?;
        let runtime = Arc::new(IcebergRuntime::new(config.iceberg_threads)?);
        let catalog = Arc::new(EngineCatalog::new(
            backend.clone(),
            runtime,
            CatalogSettings::from(&config),
        ));
        let (shutdown, _) = watch::channel(false);
        let in_process = InProcess::start(
            &Services::new(Arc::clone(&catalog), Route::InProcess),
            &shutdown,
        );
        info!(
            root = %config.root,
            backend = %backend.kind(),
            credentials = "none",
            "Opened the engine's root.",
        );
        Ok(Self {
            inner: Arc::new(EngineInner {
                catalog,
                channel: in_process.channel,
                shutdown,
                in_process: Mutex::new(Some(in_process.server)),
            }),
        })
    }

    /// A channel to this engine over an in-memory connection, for `gcloud-sdk`'s generated
    /// clients or the `bigquery` client. Clones are cheap and share it. Calls on it are not
    /// authenticated: the `authorization` header is ignored.
    pub fn channel(&self) -> Channel {
        self.inner.channel.clone()
    }

    /// Binds a TCP listener for this engine, so that the caller learns its address before
    /// serving; [`TcpServer::serve`] then serves it.
    ///
    /// # Errors
    /// [`EngineError::Config`] for an address that is not loopback: this release serves
    /// plaintext without authentication, which only a loopback address keeps on this host;
    /// [`EngineError::Io`] when the address cannot be bound.
    pub async fn bind_tcp(&self, endpoint: TcpEndpoint) -> Result<TcpServer, EngineError> {
        if !endpoint.addr.ip().is_loopback() {
            return Err(EngineError::config(
                "addr",
                format!(
                    "{} is not a loopback address, and a listener on any other address needs \
                     TLS and authentication, which this release does not serve",
                    endpoint.addr
                ),
            ));
        }
        let listener = TcpListener::bind(endpoint.addr).await?;
        Ok(TcpServer {
            local_addr: listener.local_addr()?,
            listener,
            services: Services::new(Arc::clone(&self.inner.catalog), Route::Tcp),
            shutdown: self.inner.shutdown.subscribe(),
        })
    }

    /// Serves on TCP until [`Engine::shutdown`] is called, the engine is dropped or the future
    /// is dropped.
    ///
    /// # Errors
    /// As [`Engine::bind_tcp`], and a failure of the server.
    pub async fn serve_tcp(&self, endpoint: TcpEndpoint) -> Result<(), EngineError> {
        self.bind_tcp(endpoint).await?.serve().await
    }

    /// Stops the in-process server and every TCP server, and waits up to `grace` for the calls
    /// in flight on the in-process server to finish.
    pub async fn shutdown(&self, grace: Duration) {
        self.inner.shutdown.send_replace(true);
        let in_process = self
            .inner
            .in_process
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(in_process) = in_process {
            if tokio::time::timeout(grace, in_process).await.is_err() {
                warn!(?grace, "In-process calls were still running at shutdown.");
            }
        }
    }
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine").finish_non_exhaustive()
    }
}

/// A bound TCP listener of an engine; see [`Engine::bind_tcp`].
#[derive(Debug)]
pub struct TcpServer {
    listener: TcpListener,
    local_addr: SocketAddr,
    services: Services,
    shutdown: watch::Receiver<bool>,
}

impl TcpServer {
    /// The address the listener is bound to, with the port the system picked for port 0.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Serves until the engine shuts down or is dropped, or this future is dropped.
    ///
    /// # Errors
    /// [`EngineError::Io`] when the server fails.
    pub async fn serve(self) -> Result<(), EngineError> {
        info!(addr = %self.local_addr, "Serving the BigQuery APIs on TCP.");
        let mut shutdown = self.shutdown;
        // gRPC sends small frames whose replies the peer waits for; Nagle would hold them.
        let incoming = TcpListenerStream::new(self.listener).map(|accepted| {
            let stream = accepted?;
            stream.set_nodelay(true)?;
            Ok::<_, std::io::Error>(stream)
        });
        Server::builder()
            .add_routes(self.services.routes())
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = shutdown.wait_for(|stop| *stop).await;
            })
            .await
            .map_err(|err| EngineError::Io(std::io::Error::other(err)))
    }
}

impl RootMarker {
    /// Writes the root's marker on an empty root, or checks the one an earlier engine wrote.
    async fn initialize(backend: &Backend, root: &url::Url) -> Result<(), EngineError> {
        let path = ObjectPath::segment("_smallquery").join("engine.json");
        let marker = RootMarker {
            format_version: FORMAT_VERSION,
            created_at_ms: crate::catalog::now_ms(),
        };
        let bytes = serde_json::to_vec(&marker)
            .map_err(|err| EngineError::invalid("root marker", err.to_string()))?;
        if let Created::Created(_) = backend.create(&path, bytes.into()).await? {
            info!(%root, "Initialized an empty root.");
            return Ok(());
        }
        let existing: RootMarker = match backend.read(&path).await? {
            Some(bytes) => serde_json::from_slice(&bytes).map_err(|err| {
                EngineError::config("root", format!("{root} has an unreadable {path}: {err}"))
            })?,
            None => {
                return Err(EngineError::config(
                    "root",
                    format!("{root}: {path} vanished while the engine opened"),
                ))
            }
        };
        if existing.format_version != FORMAT_VERSION {
            return Err(EngineError::config(
                "root",
                format!(
                    "{root} has layout version {}, and this engine reads version {FORMAT_VERSION}",
                    existing.format_version
                ),
            ));
        }
        Ok(())
    }
}

/// The in-process route: a lazily connected channel whose every connection is an in-memory
/// duplex, and the task of the tonic server, on the current runtime, that serves them.
struct InProcess {
    channel: Channel,
    server: JoinHandle<()>,
}

impl InProcess {
    fn start(services: &Services, shutdown: &watch::Sender<bool>) -> Self {
        let (connections, accepted) = mpsc::unbounded_channel::<DuplexStream>();
        let routes = services.routes();
        let mut stop = shutdown.subscribe();
        let server = tokio::spawn(async move {
            let incoming = UnboundedReceiverStream::new(accepted).map(Ok::<_, std::io::Error>);
            let served = Server::builder()
                .add_routes(routes)
                .serve_with_incoming_shutdown(incoming, async move {
                    let _ = stop.wait_for(|stop| *stop).await;
                })
                .await;
            if let Err(err) = served {
                warn!(%err, "The in-process server failed.");
            }
        });
        let connector = tower::service_fn(move |_: tonic::codegen::http::Uri| {
            let connections = connections.clone();
            async move {
                let (client, server) = tokio::io::duplex(DUPLEX_BUFFER);
                connections.send(server).map_err(|_| {
                    std::io::Error::new(
                        std::io::ErrorKind::ConnectionRefused,
                        "the engine has shut down",
                    )
                })?;
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(client))
            }
        });
        let channel = Endpoint::from_static("http://in-process.smallquery")
            .connect_with_connector_lazy(connector);
        Self { channel, server }
    }
}
