//! An engine on a fresh root, served on a loopback port, with a `bigquery` client pointed at it:
//! for the crate's own tests that drive the engine as an application does and then look inside
//! it, at its catalog, its commit queues and its files.

use crate::engine::EngineState;
use crate::ids::TableRef;
use crate::{Engine, EngineConfig, TcpEndpoint};
use bigquery::gcloud_sdk::google_cloud_auth::credentials::anonymous;
use bigquery::{BigQueryDatasetId, BigQueryDb, BigQueryDbOptions, BigQueryLocation};
use futures::TryStreamExt;
use iceberg::expr::Predicate;
use iceberg::scan::FileScanTask;
use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::task::JoinHandle;

/// The project every test's client works in.
pub(crate) const PROJECT: &str = "lite-project";

pub(crate) struct TestEngine {
    pub engine: Engine,
    pub db: BigQueryDb,
    root: Arc<TempDir>,
    server: JoinHandle<()>,
}

impl Drop for TestEngine {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl TestEngine {
    /// An engine with the default configuration.
    pub(crate) async fn start() -> Self {
        Self::with_config(|config| config).await
    }

    /// An engine whose configuration `adjust` changes from the defaults.
    pub(crate) async fn with_config(adjust: impl FnOnce(EngineConfig) -> EngineConfig) -> Self {
        let root = Arc::new(TempDir::new().expect("a temporary root"));
        Self::on_root(root, adjust).await
    }

    /// A second engine on this engine's root, as another process opens it, whose configuration
    /// `adjust` changes from the defaults.
    pub(crate) async fn open_on_same_root(
        &self,
        adjust: impl FnOnce(EngineConfig) -> EngineConfig,
    ) -> Self {
        Self::on_root(Arc::clone(&self.root), adjust).await
    }

    async fn on_root(
        root: Arc<TempDir>,
        adjust: impl FnOnce(EngineConfig) -> EngineConfig,
    ) -> Self {
        let url = url::Url::from_directory_path(root.path()).expect("the root is absolute");
        let engine = Engine::open(adjust(EngineConfig::new(url)))
            .await
            .expect("the engine opens");
        let server = engine
            .bind_tcp(TcpEndpoint::new(SocketAddr::from(([127, 0, 0, 1], 0))))
            .await
            .expect("a loopback port binds");
        let endpoint =
            url::Url::parse(&format!("http://{}", server.local_addr())).expect("a loopback URL");
        let server = tokio::spawn(async move {
            server.serve().await.expect("the engine serves");
        });
        let options = BigQueryDbOptions::new(PROJECT.to_string())
            .with_bigquery_api_url(endpoint.clone())
            .with_bigquery_storage_api_url(endpoint);
        let db = BigQueryDb::with_options_auth(options, anonymous::Builder::new().build())
            .await
            .expect("the client connects");
        Self {
            engine,
            db,
            root,
            server,
        }
    }

    pub(crate) fn state(&self) -> &EngineState {
        self.engine.state()
    }

    /// Creates the dataset `dataset` in `location` through the client.
    pub(crate) async fn create_dataset(&self, dataset: &'static str, location: &'static str) {
        self.db
            .fluent()
            .schema()
            .dataset(BigQueryDatasetId::from_static(dataset))
            .create()
            .location(BigQueryLocation::from_static(location))
            .execute()
            .await
            .expect("the dataset is created");
    }

    /// The file tasks the table's current snapshot plans, with `predicate` when set.
    pub(crate) async fn plan_files(
        &self,
        table: &TableRef,
        predicate: Option<Predicate>,
    ) -> Vec<FileScanTask> {
        let loaded = self
            .state()
            .catalog
            .reload(table)
            .await
            .expect("the table loads");
        let Some(snapshot) = loaded.metadata().current_snapshot_id() else {
            return Vec::new();
        };
        let mut scan = loaded.table.scan().snapshot_id(snapshot);
        if let Some(predicate) = predicate {
            scan = scan.with_filter(predicate);
        }
        scan.build()
            .expect("the scan builds")
            .plan_files()
            .await
            .expect("the files plan")
            .try_collect()
            .await
            .expect("every task plans")
    }

    /// The directories of `table` that exist under the root: its catalog pointers and its
    /// Iceberg location.
    pub(crate) fn table_directories(&self, table: &TableRef) -> Vec<PathBuf> {
        let root = self.root.path();
        let parts = [
            table.project.to_string(),
            table.dataset.to_string(),
            table.table.to_string(),
        ];
        [
            root.join("_smallquery")
                .join("catalog")
                .join(parts.iter().collect::<PathBuf>()),
            root.join(parts.iter().collect::<PathBuf>()),
        ]
        .into_iter()
        .filter(|dir| dir.exists())
        .collect()
    }

    /// Fails when the table's current snapshot plans any data file path twice: no commit may add
    /// a path the table already holds.
    pub(crate) async fn assert_no_path_twice(&self, table: &TableRef) {
        let paths: Vec<String> = self
            .plan_files(table, None)
            .await
            .into_iter()
            .map(|task| task.data_file_path)
            .collect();
        let distinct: BTreeSet<&String> = paths.iter().collect();
        assert_eq!(distinct.len(), paths.len(), "{table} plans a path twice");
    }
}

/// The table `table` of the dataset `dataset` in the test project.
pub(crate) fn table_ref(dataset: &str, table: &str) -> TableRef {
    TableRef::parse(PROJECT, dataset, table).expect("a valid table")
}
