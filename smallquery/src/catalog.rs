//! The engine's Iceberg catalog. Its whole state lives in the root:
//!
//! ```text
//! <root>/_smallquery/catalog/<project>/<dataset>.json          a dataset
//! <root>/_smallquery/catalog/<project>/<dataset>/<table>/v…    a table's catalog pointers
//! <root>/<project>/<dataset>/<table>/{metadata,data}/              the Iceberg table location
//! ```
//!
//! A project exists while its catalog directory does, and a table while its newest pointer is
//! not a tombstone. Every change to a table goes through one commit path ("commit.rs"), so
//! several processes can commit to one root safely; jobs, streams and sessions stay per
//! process.

mod commit;
mod datasets;
mod hooks;
mod pointer;

pub(crate) use commit::{Change, EngineCommit};
pub(crate) use datasets::{DatasetEntry, NewDataset};
use hooks::CommitHooks;
pub(crate) use hooks::CommitPoint;
pub(crate) use pointer::PointerVersion;
use pointer::{LivePointer, PointerBody, PointerHead, Tombstone};

use crate::config::EngineConfig;
use crate::error::{EngineError, Resource};
use crate::ids::{DatasetRef, Location, ProjectId, TableId, TableRef};
use crate::runtime::IcebergRuntime;
use crate::storage::{Backend, ListedKind, ObjectPath};
use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::spec::{TableMetadata, TableMetadataBuilder};
use iceberg::table::Table;
use iceberg::{Catalog, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// The settings of the catalog, taken from [`EngineConfig`].
#[derive(Debug, Clone)]
pub(crate) struct CatalogSettings {
    pub pointer_min_age: Duration,
    pub catalog_cache_ttl: Duration,
    pub commit_retry_deadline: Duration,
    pub default_location: Location,
}

impl From<&EngineConfig> for CatalogSettings {
    fn from(config: &EngineConfig) -> Self {
        Self {
            pointer_min_age: config.pointer_min_age,
            catalog_cache_ttl: config.catalog_cache_ttl,
            commit_retry_deadline: config.commit_retry_deadline,
            default_location: config.default_location.clone(),
        }
    }
}

/// A table as one load found it: its live pointer and the metadata that pointer names.
#[derive(Debug, Clone)]
pub(crate) struct LoadedTable {
    pub table: Table,
    head: PointerHead<LivePointer>,
}

impl LoadedTable {
    /// The pointer version, which is the table's etag.
    pub(crate) fn version(&self) -> PointerVersion {
        self.head.version
    }

    pub(crate) fn metadata(&self) -> &TableMetadata {
        self.table.metadata()
    }

    pub(crate) fn metadata_location(&self) -> &str {
        &self.head.body.metadata_location
    }
}

/// What a table name holds.
#[derive(Debug)]
enum NameState {
    /// No pointer was ever created under the name, or every one was deleted.
    Empty,
    /// The newest pointer is a tombstone.
    Dropped(PointerHead<Tombstone>),
    Live(Box<LoadedTable>),
}

/// A loaded table kept for the next statement, trusted only after a probe for a newer commit.
#[derive(Debug)]
struct Cached {
    loaded: LoadedTable,
    /// A time at which the pointer was known to be the newest, no later than the listing or
    /// the create that showed it: the cache's age, and the successor probe's soundness, count
    /// from here.
    observed: Instant,
}

/// The in-process locks: a table's commits run one at a time, and a dataset's tables are
/// created under its read lock and deleted under its write lock. A dataset lock is always taken
/// before a table lock.
///
/// A table's entry lives while someone holds or waits for its lock, and leaves the map with the
/// last of them, so the map holds the tables being committed, not every table the process ever
/// touched, anonymous result tables included.
#[derive(Debug, Default)]
struct Locks {
    tables: Mutex<HashMap<TableRef, Arc<tokio::sync::Mutex<()>>>>,
    datasets: Mutex<HashMap<DatasetRef, Arc<tokio::sync::RwLock<()>>>>,
}

/// A table's lock, held until dropped; a guard dropped while still waiting gives up its place.
struct TableLock<'l> {
    locks: &'l Locks,
    table: TableRef,
    guard: Option<tokio::sync::OwnedMutexGuard<()>>,
    mutex: Option<Arc<tokio::sync::Mutex<()>>>,
}

impl Locks {
    /// Waits for `table`'s lock.
    async fn table(&self, table: &TableRef) -> TableLock<'_> {
        let mutex = {
            let mut tables = self.tables.lock().unwrap_or_else(PoisonError::into_inner);
            Arc::clone(tables.entry(table.clone()).or_default())
        };
        let mut lock = TableLock {
            locks: self,
            table: table.clone(),
            guard: None,
            mutex: Some(Arc::clone(&mutex)),
        };
        lock.guard = Some(mutex.lock_owned().await);
        lock
    }

    fn dataset(&self, dataset: &DatasetRef) -> Arc<tokio::sync::RwLock<()>> {
        let mut datasets = self.datasets.lock().unwrap_or_else(PoisonError::into_inner);
        Arc::clone(datasets.entry(dataset.clone()).or_default())
    }

    #[cfg(test)]
    fn tables_held(&self) -> usize {
        self.tables
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }
}

impl Drop for TableLock<'_> {
    /// Releases the lock, and removes the table's entry when nobody else holds or waits for it:
    /// every holder and waiter has a reference, taken under the map's lock, so a count of one
    /// is the map's own.
    fn drop(&mut self) {
        self.guard.take();
        self.mutex.take();
        let mut tables = self
            .locks
            .tables
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if tables
            .get(&self.table)
            .is_some_and(|mutex| Arc::strong_count(mutex) == 1)
        {
            tables.remove(&self.table);
        }
    }
}

/// The catalog of one root, shared by every request of one engine.
#[derive(Debug)]
pub(crate) struct EngineCatalog {
    backend: Backend,
    file_io: FileIO,
    runtime: Arc<IcebergRuntime>,
    settings: CatalogSettings,
    locks: Locks,
    cache: Mutex<HashMap<TableRef, Cached>>,
    contention: Mutex<HashMap<TableRef, commit::Contention>>,
    hooks: CommitHooks,
}

/// How many times a load starts over when the newest pointer moves under it.
const LOAD_ATTEMPTS: u32 = 100;

impl EngineCatalog {
    pub(crate) fn new(
        backend: Backend,
        runtime: Arc<IcebergRuntime>,
        settings: CatalogSettings,
    ) -> Self {
        Self {
            file_io: backend.file_io(),
            backend,
            runtime,
            settings,
            locks: Locks::default(),
            cache: Mutex::default(),
            contention: Mutex::default(),
            hooks: CommitHooks::default(),
        }
    }

    pub(crate) fn settings(&self) -> &CatalogSettings {
        &self.settings
    }

    pub(crate) fn backend(&self) -> &Backend {
        &self.backend
    }

    /// The points where a test holds this catalog's commits and loads.
    #[cfg(test)]
    pub(crate) fn hooks(&self) -> &CommitHooks {
        &self.hooks
    }

    /// The runtime Iceberg's scans and writers spawn their work on.
    pub(crate) fn iceberg_runtime(&self) -> iceberg::Runtime {
        self.runtime.handle()
    }

    fn catalog_dir() -> ObjectPath {
        ObjectPath::segment("_smallquery").join("catalog")
    }

    fn project_dir(project: &ProjectId) -> ObjectPath {
        Self::catalog_dir().join(project)
    }

    fn dataset_file(dataset: &DatasetRef) -> ObjectPath {
        Self::project_dir(&dataset.project).join(format_args!("{}.json", dataset.dataset))
    }

    fn dataset_dir(dataset: &DatasetRef) -> ObjectPath {
        Self::project_dir(&dataset.project).join(&dataset.dataset)
    }

    fn pointer_dir(table: &TableRef) -> ObjectPath {
        Self::dataset_dir(&table.dataset_ref()).join(&table.table)
    }

    /// The Iceberg table location of `table`: every identity a name ever had shares it, which
    /// is safe since every file Iceberg and the engine write there has a unique name.
    pub(crate) fn table_location(&self, table: &TableRef) -> String {
        self.backend.location(&Self::table_dir(table))
    }

    fn table_dir(table: &TableRef) -> ObjectPath {
        ObjectPath::segment(&table.project)
            .join(&table.dataset)
            .join(&table.table)
    }

    /// The table, from the cache when no newer pointer exists, else from storage.
    ///
    /// A cached entry younger than `catalog_cache_ttl` is returned as it is; one younger than
    /// half of `pointer_min_age` is returned when the successor of its pointer does not exist,
    /// which proves it is still the newest: no name above the newest pointer has ever existed,
    /// and a name is freed only once the pointer above it aged `pointer_min_age`.
    ///
    /// # Errors
    /// [`EngineError::NotFound`] when the table does not exist.
    pub(crate) async fn load(&self, table: &TableRef) -> Result<LoadedTable, EngineError> {
        if let Some((loaded, age)) = self.cached(table) {
            if age < self.settings.catalog_cache_ttl {
                return Ok(loaded);
            }
            if age < self.settings.pointer_min_age / 2 {
                let successor =
                    Self::pointer_dir(table).join(loaded.version().next().object_name());
                if !self.backend.exists(&successor).await? {
                    return Ok(loaded);
                }
            }
        }
        self.reload(table).await
    }

    /// The table, read from storage.
    ///
    /// # Errors
    /// [`EngineError::NotFound`] when the table does not exist.
    pub(crate) async fn reload(&self, table: &TableRef) -> Result<LoadedTable, EngineError> {
        match self.load_name(table).await? {
            NameState::Live(loaded) => Ok(*loaded),
            NameState::Empty | NameState::Dropped(_) => Err(EngineError::NotFound {
                what: Resource::Table(table.clone()),
            }),
        }
    }

    fn cached(&self, table: &TableRef) -> Option<(LoadedTable, Duration)> {
        let cache = self.cache.lock().unwrap_or_else(PoisonError::into_inner);
        cache
            .get(table)
            .map(|cached| (cached.loaded.clone(), cached.observed.elapsed()))
    }

    /// Caches `loaded`, whose pointer was the newest at `observed`.
    fn remember(&self, table: &TableRef, loaded: &LoadedTable, observed: Instant) {
        let mut cache = self.cache.lock().unwrap_or_else(PoisonError::into_inner);
        cache.insert(
            table.clone(),
            Cached {
                loaded: loaded.clone(),
                observed,
            },
        );
    }

    fn forget(&self, table: &TableRef) {
        let mut cache = self.cache.lock().unwrap_or_else(PoisonError::into_inner);
        cache.remove(table);
    }

    /// What the name holds now: the newest pointer by a listing, and the metadata it names.
    ///
    /// # Errors
    /// [`EngineError::MissingMetadata`] when the newest pointer names a missing metadata file
    /// on two listings in a row: a file that fell off the log of a newer commit, or that a
    /// purge deleted after its tombstone, shows a newer pointer on the second.
    async fn load_name(&self, table: &TableRef) -> Result<NameState, EngineError> {
        let dir = Self::pointer_dir(table);
        let mut missing = None;
        for _ in 0..LOAD_ATTEMPTS {
            let observed = Instant::now();
            let newest = self.pointer_versions(&dir).await?.into_iter().max();
            let Some(version) = newest else {
                self.forget(table);
                return Ok(NameState::Empty);
            };
            // Gone between the listing and the read: only a purge or a sweep deletes the newest.
            let Some(object) = self
                .backend
                .read_object(&dir.join(version.object_name()))
                .await?
            else {
                continue;
            };
            let head = PointerHead::read(version, object).map_err(|err| {
                EngineError::invalid(
                    format!("catalog pointer {} of {table}", version.object_name()),
                    err.to_string(),
                )
            })?;
            let PointerHead {
                version,
                identity,
                body,
            } = head;
            let live = match body {
                PointerBody::Live(live) => live,
                PointerBody::Tombstone(tombstone) => {
                    self.forget(table);
                    return Ok(NameState::Dropped(PointerHead {
                        version,
                        identity,
                        body: tombstone,
                    }));
                }
            };
            self.hooks.reach(CommitPoint::PointerRead).await;
            let Some(metadata) = self.read_metadata(&live.metadata_location).await? else {
                if missing.replace(version) == Some(version) {
                    return Err(EngineError::MissingMetadata {
                        what: Resource::Table(table.clone()),
                        pointer_version: version.number(),
                        metadata_location: live.metadata_location,
                    });
                }
                continue;
            };
            let head = PointerHead {
                version,
                identity,
                body: live,
            };
            let loaded = self.loaded_table(table, head, metadata)?;
            self.remember(table, &loaded, observed);
            return Ok(NameState::Live(Box::new(loaded)));
        }
        Err(EngineError::Aborted {
            what: Resource::Table(table.clone()),
            attempts: LOAD_ATTEMPTS,
        })
    }

    /// The versions of the pointers in `dir`, in listing order; staging files and anything else
    /// that is not a pointer are left out.
    async fn pointer_versions(&self, dir: &ObjectPath) -> Result<Vec<PointerVersion>, EngineError> {
        Ok(self
            .backend
            .list(dir)
            .await?
            .iter()
            .filter_map(|listed| PointerVersion::from_object_name(&listed.name))
            .collect())
    }

    /// The metadata file at `location`, or `None` when it does not exist.
    async fn read_metadata(&self, location: &str) -> Result<Option<TableMetadata>, EngineError> {
        match TableMetadata::read_from(&self.file_io, location).await {
            Ok(metadata) => Ok(Some(metadata)),
            Err(err) => {
                let path = self.backend.object_path(location)?;
                if self.backend.exists(&path).await? {
                    Err(err.into())
                } else {
                    Ok(None)
                }
            }
        }
    }

    fn loaded_table(
        &self,
        table: &TableRef,
        head: PointerHead<LivePointer>,
        metadata: TableMetadata,
    ) -> Result<LoadedTable, EngineError> {
        let table = Table::builder()
            .identifier(TableIdent::from(table))
            .metadata(metadata)
            .metadata_location(head.body.metadata_location.clone())
            .file_io(self.file_io.clone())
            .runtime(self.runtime.handle())
            .build()?;
        Ok(LoadedTable { table, head })
    }

    /// Creates `table` with `metadata`, whose location must be [`Self::table_location`]: as the
    /// name's first pointer, or on top of the tombstone of a dropped table.
    ///
    /// # Errors
    /// [`EngineError::NotFound`] for a missing dataset, [`EngineError::AlreadyExists`] when the
    /// table exists, and the commit loop's errors.
    pub(crate) async fn create_table(
        &self,
        table: &TableRef,
        metadata: TableMetadata,
    ) -> Result<LoadedTable, EngineError> {
        let location = self.table_location(table);
        if metadata.location() != location {
            return Err(EngineError::invalid(
                format!("location of {table}"),
                format!(
                    "{:?} is not the table's location {location:?}",
                    metadata.location()
                ),
            ));
        }
        let lock = self.locks.dataset(&table.dataset_ref());
        let _guard = lock.read().await;
        self.dataset(&table.dataset_ref()).await?;
        self.create_pointer(table, commit::NewPointer::Write(metadata))
            .await
    }

    /// Drops `table` with a tombstone and deletes its data and metadata files.
    ///
    /// The pointers below the tombstone stay, naming files that are gone, and leave only
    /// through the pruning: a pointer deleted before its successor aged would let a cached
    /// load's successor probe, or a commit's read-back of its predecessor, find the name free
    /// and take the dropped table for live.
    ///
    /// # Errors
    /// [`EngineError::NotFound`] when the table does not exist, and the commit loop's errors.
    pub(crate) async fn purge(&self, table: &TableRef) -> Result<(), EngineError> {
        let dropped = self.drop_table(table).await?;
        iceberg::drop_table_data(&dropped.table).await?;
        Ok(())
    }

    /// Deletes an anonymous result table whole, its catalog pointers and every file at its
    /// location, where [`EngineCatalog::purge`] keeps the pointers below a tombstone. Nothing
    /// those pointers guard against can happen to such a table: its name carries its job's ID,
    /// so it is never created again, and nothing commits to it once its job has ended.
    ///
    /// # Errors
    /// The storage errors of the deletes.
    pub(crate) async fn purge_anonymous(&self, table: &TableRef) -> Result<(), EngineError> {
        let _lock = self.locks.table(table).await;
        self.backend.delete_dir(&Self::pointer_dir(table)).await?;
        self.forget(table);
        self.backend.delete_dir(&Self::table_dir(table)).await?;
        Ok(())
    }

    /// The tables of `dataset` that exist, by name.
    pub(crate) async fn table_names(
        &self,
        dataset: &DatasetRef,
    ) -> Result<Vec<TableId>, EngineError> {
        Ok(self
            .tables(dataset)
            .await?
            .into_iter()
            .map(|(table, _)| table)
            .collect())
    }

    /// The tables of `dataset` that exist, loaded, by name.
    pub(crate) async fn tables(
        &self,
        dataset: &DatasetRef,
    ) -> Result<Vec<(TableId, LoadedTable)>, EngineError> {
        let mut tables = Vec::new();
        for listed in self.backend.list(&Self::dataset_dir(dataset)).await? {
            let (ListedKind::Directory, Ok(id)) = (listed.kind, listed.name.parse::<TableId>())
            else {
                continue;
            };
            let table = dataset.table(id.clone());
            if let NameState::Live(loaded) = self.load_name(&table).await? {
                tables.push((id, *loaded));
            }
        }
        tables.sort_by(|(a, _), (b, _)| a.cmp(b));
        Ok(tables)
    }
}

/// Milliseconds since the Unix epoch, by this host's clock.
pub(crate) fn now_ms() -> i64 {
    millis(SystemTime::now())
}

/// `time` in milliseconds since the Unix epoch, 0 before it.
pub(crate) fn millis(time: SystemTime) -> i64 {
    time.duration_since(UNIX_EPOCH).map_or(0, |since| {
        i64::try_from(since.as_millis()).unwrap_or(i64::MAX)
    })
}

impl From<&TableRef> for TableIdent {
    fn from(table: &TableRef) -> Self {
        TableIdent::new(
            NamespaceIdent::from(&table.dataset_ref()),
            table.table.to_string(),
        )
    }
}

impl From<&DatasetRef> for NamespaceIdent {
    fn from(dataset: &DatasetRef) -> Self {
        NamespaceIdent::from_vec(vec![
            dataset.project.to_string(),
            dataset.dataset.to_string(),
        ])
        .expect("a namespace of two names is not empty")
    }
}

/// The dataset a two-level namespace `[project, dataset]` names.
impl TryFrom<&NamespaceIdent> for DatasetRef {
    type Error = EngineError;

    fn try_from(namespace: &NamespaceIdent) -> Result<Self, Self::Error> {
        match namespace.as_ref().as_slice() {
            [project, dataset] => Ok(DatasetRef {
                project: project.parse()?,
                dataset: dataset.parse()?,
            }),
            _ => Err(EngineError::invalid(
                format!("namespace {namespace}"),
                "a dataset is the namespace [project, dataset]",
            )),
        }
    }
}

impl TryFrom<&TableIdent> for TableRef {
    type Error = EngineError;

    fn try_from(ident: &TableIdent) -> Result<Self, Self::Error> {
        let dataset = DatasetRef::try_from(ident.namespace())?;
        Ok(dataset.table(ident.name().parse()?))
    }
}

/// A dataset's namespace properties: its catalog entry, a property per field.
impl From<&DatasetEntry> for HashMap<String, String> {
    fn from(entry: &DatasetEntry) -> Self {
        let mut properties = HashMap::from([
            ("location".to_string(), entry.location.to_string()),
            ("etag".to_string(), entry.etag()),
            (
                "creation_time_ms".to_string(),
                entry.creation_time_ms.to_string(),
            ),
            (
                "last_modified_time_ms".to_string(),
                entry.last_modified_time_ms.to_string(),
            ),
        ]);
        if let Some(description) = &entry.description {
            properties.insert("description".to_string(), description.clone());
        }
        if let Some(ms) = entry.default_table_expiration_ms {
            properties.insert("default_table_expiration_ms".to_string(), ms.to_string());
        }
        for (key, value) in &entry.labels {
            properties.insert(format!("label.{key}"), value.clone());
        }
        properties
    }
}

impl EngineCatalog {
    /// A new dataset from namespace properties: `location`, `description` and `label.<key>`.
    fn new_dataset(&self, properties: &HashMap<String, String>) -> Result<NewDataset, EngineError> {
        Ok(NewDataset {
            location: match properties.get("location") {
                Some(location) => location.parse()?,
                None => self.settings.default_location.clone(),
            },
            description: properties.get("description").cloned(),
            labels: properties
                .iter()
                .filter_map(|(key, value)| {
                    key.strip_prefix("label.")
                        .map(|key| (key.to_string(), value.clone()))
                })
                .collect(),
            default_table_expiration_ms: None,
        })
    }
}

fn unsupported(method: &str, detail: &str) -> iceberg::Error {
    iceberg::Error::new(
        iceberg::ErrorKind::FeatureUnsupported,
        format!("{method} is not supported by smallquery's catalog: {detail}"),
    )
}

/// The Iceberg view of the catalog: namespaces are projects and `[project, dataset]` datasets,
/// and every table change goes through the engine's commit path.
#[async_trait]
impl Catalog for EngineCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> iceberg::Result<Vec<NamespaceIdent>> {
        match parent.map(|parent| parent.as_ref().as_slice()) {
            None => Ok(self
                .projects()
                .await?
                .into_iter()
                .map(|project| NamespaceIdent::new(project.to_string()))
                .collect()),
            Some([project]) => {
                let project: ProjectId = project.parse().map_err(iceberg::Error::from)?;
                Ok(self
                    .datasets(&project)
                    .await?
                    .into_iter()
                    .map(|(dataset, _)| {
                        NamespaceIdent::from(&DatasetRef {
                            project: project.clone(),
                            dataset,
                        })
                    })
                    .collect())
            }
            Some(_) => Ok(Vec::new()),
        }
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<Namespace> {
        let dataset = DatasetRef::try_from(namespace)?;
        let entry = self
            .create_dataset(&dataset, self.new_dataset(&properties)?)
            .await?;
        Ok(Namespace::with_properties(
            namespace.clone(),
            HashMap::from(&entry),
        ))
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<Namespace> {
        if let [project] = namespace.as_ref().as_slice() {
            let project: ProjectId = project.parse().map_err(iceberg::Error::from)?;
            if self.datasets(&project).await?.is_empty() {
                return Err(EngineError::NotFound {
                    what: Resource::Project(project),
                }
                .into());
            }
            return Ok(Namespace::new(namespace.clone()));
        }
        let entry = self.dataset(&DatasetRef::try_from(namespace)?).await?;
        Ok(Namespace::with_properties(
            namespace.clone(),
            HashMap::from(&entry),
        ))
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> iceberg::Result<bool> {
        match self.get_namespace(namespace).await {
            Ok(_) => Ok(true),
            Err(err) if err.kind() == iceberg::ErrorKind::NamespaceNotFound => Ok(false),
            Err(err) => Err(err),
        }
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        let dataset = DatasetRef::try_from(namespace)?;
        let new = self.new_dataset(&properties)?;
        self.update_dataset(&dataset, None, |entry| {
            if properties.contains_key("location") && new.location != entry.location {
                return Err(EngineError::invalid(
                    format!("location of {dataset}"),
                    "a dataset's location cannot change",
                ));
            }
            entry.description = new.description;
            entry.labels = new.labels;
            Ok(())
        })
        .await?;
        Ok(())
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<()> {
        Ok(self
            .delete_dataset(&DatasetRef::try_from(namespace)?, false)
            .await?)
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        let dataset = DatasetRef::try_from(namespace)?;
        self.dataset(&dataset).await?;
        Ok(self
            .table_names(&dataset)
            .await?
            .into_iter()
            .map(|table| TableIdent::from(&dataset.table(table)))
            .collect())
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> iceberg::Result<Table> {
        let dataset = DatasetRef::try_from(namespace)?;
        let table = dataset.table(creation.name.parse().map_err(iceberg::Error::from)?);
        let location = self.table_location(&table);
        let creation = match &creation.location {
            None => TableCreation {
                location: Some(location),
                ..creation
            },
            Some(_) => creation,
        };
        let metadata = TableMetadataBuilder::from_table_creation(creation)?
            .build()?
            .metadata;
        Ok(EngineCatalog::create_table(self, &table, metadata)
            .await?
            .table)
    }

    async fn load_table(&self, table: &TableIdent) -> iceberg::Result<Table> {
        Ok(self.load(&TableRef::try_from(table)?).await?.table)
    }

    async fn drop_table(&self, table: &TableIdent) -> iceberg::Result<()> {
        EngineCatalog::drop_table(self, &TableRef::try_from(table)?).await?;
        Ok(())
    }

    async fn purge_table(&self, table: &TableIdent) -> iceberg::Result<()> {
        Ok(self.purge(&TableRef::try_from(table)?).await?)
    }

    async fn table_exists(&self, table: &TableIdent) -> iceberg::Result<bool> {
        match self.load(&TableRef::try_from(table)?).await {
            Ok(_) => Ok(true),
            Err(EngineError::NotFound { .. }) => Ok(false),
            Err(err) => Err(err.into()),
        }
    }

    async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> iceberg::Result<()> {
        Err(unsupported(
            "rename_table",
            "no BigQuery RPC the engine serves renames a table",
        ))
    }

    async fn register_table(
        &self,
        table: &TableIdent,
        metadata_location: String,
    ) -> iceberg::Result<Table> {
        let table = TableRef::try_from(table)?;
        let metadata = self
            .read_metadata(&metadata_location)
            .await?
            .ok_or_else(|| {
                EngineError::invalid(
                    format!("metadata location of {table}"),
                    format!("{metadata_location} does not exist"),
                )
            })?;
        let lock = self.locks.dataset(&table.dataset_ref());
        let _guard = lock.read().await;
        self.dataset(&table.dataset_ref()).await?;
        Ok(self
            .create_pointer(
                &table,
                commit::NewPointer::Register {
                    metadata,
                    location: metadata_location,
                },
            )
            .await?
            .table)
    }

    /// One attempt of the commit, as Iceberg's own transactions expect: a lost race is a
    /// `CatalogCommitConflicts` that is not retryable, and the caller decides what follows.
    async fn update_table(&self, commit: TableCommit) -> iceberg::Result<Table> {
        Ok(self.commit_once(commit).await?.table)
    }
}

#[cfg(test)]
mod tests;
