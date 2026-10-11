//! The commit protocol under the interleavings it must survive: two catalogs on one root stand
//! for two processes, and the commit hooks hold one of them at an exact point while the other
//! proceeds.

use super::hooks::CommitPoint;
use super::*;
use crate::ids::Location;
use crate::services::{Route, Services};
use crate::storage::LocalBackend;
use bigquery::errors::BigQueryError;
use bigquery::gcloud_sdk::google_cloud_auth::credentials::anonymous;
use bigquery::{
    BigQueryDatasetId, BigQueryDbOptions, BigQueryLabels, BigQueryTableId,
    BigQueryTableSchemaBuilder,
};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::TableUpdate;
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use tempfile::TempDir;

const PROJECT: &str = "lite-project";
const DATASET: &str = "shop";
const ORDERS: &str = "orders";

fn settings(pointer_min_age: Duration) -> CatalogSettings {
    CatalogSettings {
        pointer_min_age,
        catalog_cache_ttl: Duration::ZERO,
        commit_retry_deadline: Duration::from_secs(30),
        default_location: Location::from_static("EU"),
    }
}

/// A catalog on `root`, as one engine process opens it.
fn catalog(root: &Path, settings: CatalogSettings) -> Arc<EngineCatalog> {
    let url = url::Url::from_directory_path(root).expect("the temporary root is absolute");
    let backend = Backend::Local(LocalBackend::open(&url).expect("the root opens"));
    let runtime = Arc::new(IcebergRuntime::new(2).expect("a runtime starts"));
    Arc::new(EngineCatalog::new(backend, runtime, settings))
}

fn orders() -> TableRef {
    TableRef {
        project: PROJECT.parse().expect("a valid project"),
        dataset: DATASET.parse().expect("a valid dataset"),
        table: ORDERS.parse().expect("a valid table"),
    }
}

async fn create_dataset(catalog: &EngineCatalog) {
    catalog
        .create_dataset(
            &orders().dataset_ref(),
            NewDataset {
                location: Location::from_static("EU"),
                description: None,
                labels: Default::default(),
                default_table_expiration_ms: None,
            },
        )
        .await
        .expect("the dataset is created");
}

/// The metadata of a new `orders` table with one column.
fn new_orders(catalog: &EngineCatalog) -> TableMetadata {
    let schema = Schema::builder()
        .with_fields(vec![NestedField::required(
            1,
            "order_id",
            Type::Primitive(PrimitiveType::Long),
        )
        .into()])
        .build()
        .expect("the schema is valid");
    let creation = TableCreation::builder()
        .name(ORDERS.to_string())
        .location(catalog.table_location(&orders()))
        .schema(schema)
        .build();
    TableMetadataBuilder::from_table_creation(creation)
        .and_then(TableMetadataBuilder::build)
        .expect("the metadata builds")
        .metadata
}

/// Sets the property `key` through an Iceberg transaction.
async fn set_by_transaction(
    catalog: &EngineCatalog,
    key: &str,
) -> Result<LoadedTable, EngineError> {
    catalog
        .commit(&orders(), |loaded| {
            let transaction = Transaction::new(&loaded.table);
            let action = transaction
                .update_table_properties()
                .set(key.to_string(), "set".to_string());
            Ok(Change::Transaction(action.apply(transaction)?))
        })
        .await
}

/// Sets the property `key` through an engine commit.
async fn set_by_engine(catalog: &EngineCatalog, key: &str) -> Result<LoadedTable, EngineError> {
    catalog
        .commit(&orders(), |_| {
            Ok(Change::Engine(EngineCommit {
                requirements: Vec::new(),
                updates: vec![TableUpdate::SetProperties {
                    updates: HashMap::from([(key.to_string(), "set".to_string())]),
                }],
            }))
        })
        .await
}

/// The pointer versions under `orders`, by a directory listing.
async fn pointer_versions(catalog: &EngineCatalog) -> BTreeSet<u64> {
    catalog
        .backend
        .list(&EngineCatalog::pointer_dir(&orders()))
        .await
        .expect("the pointers list")
        .iter()
        .filter_map(|listed| PointerVersion::from_object_name(&listed.name))
        .map(PointerVersion::number)
        .collect()
}

async fn pointer(catalog: &EngineCatalog, version: u64) -> Option<PointerBody> {
    let path =
        EngineCatalog::pointer_dir(&orders()).join(PointerVersion::new(version).object_name());
    catalog
        .backend
        .read(&path)
        .await
        .expect("the pointer reads")
        .map(|bytes| serde_json::from_slice(&bytes).expect("a pointer parses"))
}

/// Commits `count` property updates through `catalog`, `pause` apart.
async fn commit_many(catalog: &EngineCatalog, prefix: &str, count: usize, pause: Duration) {
    for n in 0..count {
        set_by_engine(catalog, &format!("{prefix}.{n}"))
            .await
            .expect("an update commits");
        tokio::time::sleep(pause).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_catalogs_commit_every_update_once_without_aborting() {
    let root = TempDir::new().expect("a temporary root");
    let first = catalog(root.path(), settings(Duration::from_secs(15 * 60)));
    let second = catalog(root.path(), settings(Duration::from_secs(15 * 60)));
    create_dataset(&first).await;
    first
        .create_table(&orders(), new_orders(&first))
        .await
        .expect("the table is created");

    let by_transaction = tokio::spawn({
        let first = Arc::clone(&first);
        async move {
            for n in 0..1000 {
                set_by_transaction(&first, &format!("first.{n}")).await?;
            }
            Ok::<_, EngineError>(())
        }
    });
    let by_engine = tokio::spawn({
        let second = Arc::clone(&second);
        async move {
            for n in 0..1000 {
                set_by_engine(&second, &format!("second.{n}")).await?;
            }
            Ok::<_, EngineError>(())
        }
    });
    by_transaction
        .await
        .expect("the first writer runs")
        .expect("no commit of the first writer fails");
    by_engine
        .await
        .expect("the second writer runs")
        .expect("no commit of the second writer fails");

    assert_eq!(
        pointer_versions(&first).await,
        (0..=2000).collect::<BTreeSet<u64>>()
    );
    for version in 1..=2000 {
        let Some(PointerBody::Live(live)) = pointer(&first, version).await else {
            panic!("v{version} is a live pointer");
        };
        let Some(PointerBody::Live(below)) = pointer(&first, version - 1).await else {
            panic!("v{} is a live pointer", version - 1);
        };
        assert_eq!(
            live.previous.as_deref(),
            Some(below.metadata_location.as_str()),
            "v{version} was created on v{}",
            version - 1
        );
    }
    let table = first.reload(&orders()).await.expect("the table loads");
    assert_eq!(table.version().number(), 2000);
    let properties = table.metadata().properties();
    for n in 0..1000 {
        assert!(properties.contains_key(&format!("first.{n}")));
        assert!(properties.contains_key(&format!("second.{n}")));
    }
    let metadata_files = std::fs::read_dir(
        root.path()
            .join(PROJECT)
            .join(DATASET)
            .join(ORDERS)
            .join("metadata"),
    )
    .expect("the metadata directory lists")
    .filter_map(Result::ok)
    .filter(|entry| {
        entry
            .file_name()
            .to_string_lossy()
            .ends_with(".metadata.json")
    })
    .count();
    assert_eq!(metadata_files, 101);
}

/// Runs the stale writer's scenario up to its release: `stale` loads `v0` and is held there
/// while `other` commits 105 times over more than `pointer_min_age`, which prunes `v0` and `v1`.
/// Returns the stale writer's commit.
async fn stale_writer(
    stale: &Arc<EngineCatalog>,
    other: &EngineCatalog,
) -> tokio::task::JoinHandle<Result<LoadedTable, EngineError>> {
    create_dataset(other).await;
    other
        .create_table(&orders(), new_orders(other))
        .await
        .expect("the table is created");
    let mut loaded = stale.hooks.hold(CommitPoint::Loaded);
    let commit = tokio::spawn({
        let stale = Arc::clone(stale);
        async move { set_by_engine(&stale, "stale").await }
    });
    loaded.reached().await;
    commit_many(other, "other", 105, Duration::from_millis(30)).await;
    let versions = pointer_versions(other).await;
    assert!(
        !versions.contains(&0) && !versions.contains(&1),
        "v0 and v1 were pruned: {versions:?}"
    );
    loaded.release();
    commit
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_writer_s_pointer_into_a_freed_name_is_lost_and_retried() {
    let root = TempDir::new().expect("a temporary root");
    let min_age = Duration::from_secs(2);
    let stale = catalog(root.path(), settings(min_age));
    let other = catalog(root.path(), settings(min_age));
    let reader = catalog(root.path(), settings(min_age));
    let mut created = stale.hooks.hold(CommitPoint::Created);
    let commit = stale_writer(&stale, &other).await;
    created.reached().await;
    let Some(PointerBody::Live(stray)) = pointer(&other, 1).await else {
        panic!("the stale writer created v1 at the backend");
    };
    let reading = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let reads = tokio::spawn({
        let (reader, reading) = (Arc::clone(&reader), Arc::clone(&reading));
        async move {
            let mut seen = Vec::new();
            while reading.load(std::sync::atomic::Ordering::SeqCst) {
                let loaded = reader.load(&orders()).await.expect("the table loads");
                seen.push((
                    loaded.version().number(),
                    loaded.metadata_location().to_string(),
                ));
            }
            seen
        }
    });
    created.release();

    let landed = commit
        .await
        .expect("the stale writer runs")
        .expect("the stale writer's retry lands");
    reading.store(false, std::sync::atomic::Ordering::SeqCst);
    let seen = reads.await.expect("the reader runs");

    assert_eq!(landed.version().number(), 106);
    assert!(landed.metadata().properties().contains_key("stale"));
    assert!(!seen.is_empty());
    for (version, location) in seen {
        assert!(version >= 105, "a load returned v{version}");
        assert_ne!(
            location, stray.metadata_location,
            "a load returned the stray v1"
        );
    }
    let current = other.reload(&orders()).await.expect("the table loads");
    assert_eq!(current.version().number(), 106);
    assert!(current.metadata().properties().contains_key("stale"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_writer_held_past_half_the_pointer_age_has_an_unknown_outcome() {
    let root = TempDir::new().expect("a temporary root");
    let min_age = Duration::from_secs(2);
    let stale = catalog(root.path(), settings(min_age));
    let other = catalog(root.path(), settings(min_age));
    let mut created = stale.hooks.hold(CommitPoint::Created);
    let commit = stale_writer(&stale, &other).await;
    created.reached().await;
    tokio::time::sleep(min_age / 2 + Duration::from_millis(100)).await;
    created.release();

    let outcome = commit.await.expect("the stale writer runs");
    let Err(EngineError::CommitOutcomeUnknown {
        pointer_version,
        metadata_location,
        ..
    }) = outcome
    else {
        panic!("the outcome is unknown, not {outcome:?}");
    };
    assert_eq!(pointer_version, 1);
    let Some(PointerBody::Live(stray)) = pointer(&other, 1).await else {
        panic!("the stray v1 is kept");
    };
    assert_eq!(stray.metadata_location, metadata_location);
    let stray_metadata = other
        .backend
        .object_path(&metadata_location)
        .expect("the location is under the root");
    assert!(other
        .backend
        .exists(&stray_metadata)
        .await
        .expect("the file stats"));
    let current = other.reload(&orders()).await.expect("the table loads");
    assert_eq!(current.version().number(), 105);
    assert!(!current.metadata().properties().contains_key("stale"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_writer_held_across_drop_and_create_of_its_table_fails_with_not_found() {
    let root = TempDir::new().expect("a temporary root");
    let writer = catalog(root.path(), settings(Duration::from_secs(15 * 60)));
    let other = catalog(root.path(), settings(Duration::from_secs(15 * 60)));
    create_dataset(&other).await;
    other
        .create_table(&orders(), new_orders(&other))
        .await
        .expect("the table is created");
    let mut loaded = writer.hooks.hold(CommitPoint::Loaded);
    let commit = tokio::spawn({
        let writer = Arc::clone(&writer);
        async move { set_by_engine(&writer, "held").await }
    });
    loaded.reached().await;
    other.purge(&orders()).await.expect("the table is dropped");
    let recreated = other
        .create_table(&orders(), new_orders(&other))
        .await
        .expect("the table is created again");
    loaded.release();

    let outcome = commit.await.expect("the writer runs");
    assert!(
        matches!(outcome, Err(EngineError::NotFound { .. })),
        "{outcome:?}"
    );
    let current = other.reload(&orders()).await.expect("the table loads");
    assert_eq!(current.version().number(), 2);
    assert_eq!(current.metadata().uuid(), recreated.metadata().uuid());
    assert!(!current.metadata().properties().contains_key("held"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_create_held_across_create_drop_and_purge_lands_on_the_tombstone() {
    let root = TempDir::new().expect("a temporary root");
    let creator = catalog(root.path(), settings(Duration::from_secs(15 * 60)));
    let other = catalog(root.path(), settings(Duration::from_secs(15 * 60)));
    create_dataset(&other).await;
    let metadata = new_orders(&creator);
    let uuid = metadata.uuid();
    let mut loaded = creator.hooks.hold(CommitPoint::Loaded);
    let create = tokio::spawn({
        let creator = Arc::clone(&creator);
        async move { creator.create_table(&orders(), metadata).await }
    });
    loaded.reached().await;
    other
        .create_table(&orders(), new_orders(&other))
        .await
        .expect("the other catalog creates the table");
    other.purge(&orders()).await.expect("the table is dropped");
    loaded.release();

    let created = create
        .await
        .expect("the creator runs")
        .expect("the create lands");
    assert_eq!(created.version().number(), 2);
    assert_eq!(created.metadata().uuid(), uuid);
    assert!(matches!(
        pointer(&other, 1).await,
        Some(PointerBody::Tombstone(_))
    ));
    let current = other.reload(&orders()).await.expect("the table loads");
    assert_eq!(current.version().number(), 2);
    assert_eq!(current.metadata().uuid(), uuid);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pointers_are_pruned_bottom_up_once_their_successor_aged() {
    let root = TempDir::new().expect("a temporary root");
    let catalog = catalog(root.path(), settings(Duration::from_secs(1)));
    create_dataset(&catalog).await;
    catalog
        .create_table(&orders(), new_orders(&catalog))
        .await
        .expect("the table is created");
    commit_many(&catalog, "early", 4, Duration::ZERO).await;
    assert_eq!(pointer_versions(&catalog).await, (0..=4).collect());

    tokio::time::sleep(Duration::from_millis(1200)).await;
    set_by_engine(&catalog, "late")
        .await
        .expect("an update commits");
    assert_eq!(pointer_versions(&catalog).await, BTreeSet::from([4, 5]));

    set_by_engine(&catalog, "later")
        .await
        .expect("an update commits");
    assert_eq!(pointer_versions(&catalog).await, BTreeSet::from([4, 5, 6]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_create_dropped_before_its_read_back_landed_and_stays_dropped() {
    let root = TempDir::new().expect("a temporary root");
    let creator = catalog(root.path(), settings(Duration::from_secs(15 * 60)));
    let other = catalog(root.path(), settings(Duration::from_secs(15 * 60)));
    create_dataset(&other).await;
    let metadata = new_orders(&creator);
    let mut created = creator.hooks.hold(CommitPoint::Created);
    let create = tokio::spawn({
        let creator = Arc::clone(&creator);
        async move { creator.create_table(&orders(), metadata).await }
    });
    created.reached().await;
    other
        .drop_table(&orders())
        .await
        .expect("the other catalog drops the new table");
    created.release();

    let landed = create
        .await
        .expect("the creator runs")
        .expect("the create landed");
    assert_eq!(landed.version().number(), 0);
    assert_eq!(pointer_versions(&other).await, BTreeSet::from([0, 1]));
    for catalog in [&creator, &other] {
        let outcome = catalog.load(&orders()).await;
        assert!(
            matches!(outcome, Err(EngineError::NotFound { .. })),
            "{outcome:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_create_purged_before_its_read_back_landed_and_stays_dropped() {
    let root = TempDir::new().expect("a temporary root");
    let creator = catalog(root.path(), settings(Duration::from_secs(15 * 60)));
    let other = catalog(root.path(), settings(Duration::from_secs(15 * 60)));
    create_dataset(&other).await;
    let metadata = new_orders(&creator);
    let mut created = creator.hooks.hold(CommitPoint::Created);
    let create = tokio::spawn({
        let creator = Arc::clone(&creator);
        async move { creator.create_table(&orders(), metadata).await }
    });
    created.reached().await;
    other
        .purge(&orders())
        .await
        .expect("the other catalog purges the new table");
    created.release();

    let landed = create
        .await
        .expect("the creator runs")
        .expect("the create landed");
    assert_eq!(landed.version().number(), 0);
    assert_eq!(pointer_versions(&other).await, BTreeSet::from([0, 1]));
    let outcome = other.reload(&orders()).await;
    assert!(
        matches!(outcome, Err(EngineError::NotFound { .. })),
        "{outcome:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cached_table_is_not_served_once_another_catalog_purged_it() {
    let root = TempDir::new().expect("a temporary root");
    let reader = catalog(root.path(), settings(Duration::from_secs(15 * 60)));
    let other = catalog(root.path(), settings(Duration::from_secs(15 * 60)));
    create_dataset(&other).await;
    other
        .create_table(&orders(), new_orders(&other))
        .await
        .expect("the table is created");
    let cached = reader.load(&orders()).await.expect("the table loads");
    assert_eq!(cached.version().number(), 0);
    set_by_engine(&other, "late")
        .await
        .expect("an update commits");
    other.purge(&orders()).await.expect("the table is purged");

    let outcome = reader.load(&orders()).await;
    assert!(
        matches!(outcome, Err(EngineError::NotFound { .. })),
        "{outcome:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_load_whose_pointer_s_metadata_is_purged_under_it_finds_no_table() {
    let root = TempDir::new().expect("a temporary root");
    let reader = catalog(root.path(), settings(Duration::from_secs(15 * 60)));
    let other = catalog(root.path(), settings(Duration::from_secs(15 * 60)));
    create_dataset(&other).await;
    other
        .create_table(&orders(), new_orders(&other))
        .await
        .expect("the table is created");
    let mut read = reader.hooks.hold(CommitPoint::PointerRead);
    let load = tokio::spawn({
        let reader = Arc::clone(&reader);
        async move { reader.reload(&orders()).await }
    });
    read.reached().await;
    other.purge(&orders()).await.expect("the table is purged");
    read.release();

    let outcome = load.await.expect("the reader runs");
    let Err(EngineError::NotFound {
        what: Resource::Table(missing),
    }) = outcome
    else {
        panic!("the table is not found, not {outcome:?}");
    };
    assert_eq!(missing, orders());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cache_entry_ages_from_before_the_load_that_filled_it() {
    let root = TempDir::new().expect("a temporary root");
    let min_age = Duration::from_secs(2);
    let reader = catalog(root.path(), settings(min_age));
    let other = catalog(root.path(), settings(min_age));
    create_dataset(&other).await;
    other
        .create_table(&orders(), new_orders(&other))
        .await
        .expect("the table is created");
    let mut read = reader.hooks.hold(CommitPoint::PointerRead);
    let first = tokio::spawn({
        let reader = Arc::clone(&reader);
        async move { reader.load(&orders()).await }
    });
    read.reached().await;
    commit_many(&other, "early", 2, Duration::ZERO).await;
    tokio::time::sleep(min_age + Duration::from_millis(100)).await;
    set_by_engine(&other, "late")
        .await
        .expect("an update commits");
    assert_eq!(pointer_versions(&other).await, BTreeSet::from([2, 3]));
    read.release();
    let first = first
        .await
        .expect("the reader runs")
        .expect("the table loads");
    assert_eq!(first.version().number(), 0);

    let next = reader.load(&orders()).await.expect("the table loads");
    assert_eq!(next.version().number(), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_commit_whose_read_back_fails_has_an_unknown_outcome() {
    let root = TempDir::new().expect("a temporary root");
    let writer = catalog(root.path(), settings(Duration::from_secs(15 * 60)));
    create_dataset(&writer).await;
    writer
        .create_table(&orders(), new_orders(&writer))
        .await
        .expect("the table is created");
    let mut created = writer.hooks.hold(CommitPoint::Created);
    let commit = tokio::spawn({
        let writer = Arc::clone(&writer);
        async move { set_by_engine(&writer, "unread").await }
    });
    created.reached().await;
    let first = root
        .path()
        .join("_smallquery/catalog")
        .join(PROJECT)
        .join(DATASET)
        .join(ORDERS)
        .join(PointerVersion::FIRST.object_name());
    std::fs::remove_file(&first).expect("v0 is removed");
    std::fs::create_dir(&first).expect("a directory unreadable as v0 takes its name");
    created.release();

    let outcome = commit.await.expect("the writer runs");
    let Err(EngineError::CommitOutcomeUnknown {
        pointer_version, ..
    }) = outcome
    else {
        panic!("the outcome is unknown, not {outcome:?}");
    };
    assert_eq!(pointer_version, 1);
}

/// `catalog` served on a loopback port, and the number of `PatchTable` calls that reached it.
async fn serve_counting_patches(
    catalog: &Arc<EngineCatalog>,
) -> (
    std::net::SocketAddr,
    Arc<AtomicUsize>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("a loopback port binds");
    let addr = listener.local_addr().expect("the listener has an address");
    let patches = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&patches);
    let count = tower::util::MapRequestLayer::new(
        move |request: tonic::codegen::http::Request<tonic::body::Body>| {
            if request.uri().path().ends_with("/PatchTable") {
                counter.fetch_add(1, Ordering::SeqCst);
            }
            request
        },
    );
    let state = crate::engine::EngineState::new(
        &crate::EngineConfig::new(url::Url::parse("file:///unused").expect("a file URL")),
        Arc::clone(catalog),
    )
    .expect("the engine state builds");
    let routes = Services::new(Arc::new(state), Route::Tcp).routes();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .layer(count)
            .add_routes(routes)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .expect("the catalog serves");
    });
    (addr, patches, server)
}

/// `orders` as the client declares it, with a label when `labelled`.
fn declared_orders(db: &bigquery::BigQueryDb, labelled: bool) -> BigQueryTableSchemaBuilder<'_> {
    let labels: BigQueryLabels = if labelled {
        [("stage", "test")].into_iter().collect()
    } else {
        BigQueryLabels::default()
    };
    db.fluent()
        .schema()
        .table(BigQueryDatasetId::from_static(DATASET).table(BigQueryTableId::from_static(ORDERS)))
        .columns(|columns| columns.fields([columns.field("order_id").int64().required()]))
        .labels(labels)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_sends_a_commit_with_an_unknown_outcome_once() {
    let root = TempDir::new().expect("a temporary root");
    let catalog = catalog(root.path(), settings(Duration::from_secs(15 * 60)));
    create_dataset(&catalog).await;
    let (addr, patches, server) = serve_counting_patches(&catalog).await;
    let endpoint = url::Url::parse(&format!("http://{addr}")).expect("a loopback URL");
    let options = BigQueryDbOptions::new(PROJECT.to_string())
        .with_bigquery_api_url(endpoint.clone())
        .with_bigquery_storage_api_url(endpoint);
    let db = bigquery::BigQueryDb::with_options_auth(options, anonymous::Builder::new().build())
        .await
        .expect("the client connects");
    declared_orders(&db, false)
        .sync()
        .await
        .expect("the table is created");

    let mut created = catalog.hooks.hold(CommitPoint::Created);
    let patch = tokio::spawn(async move { declared_orders(&db, true).sync().await });
    created.reached().await;
    let first = root
        .path()
        .join("_smallquery/catalog")
        .join(PROJECT)
        .join(DATASET)
        .join(ORDERS)
        .join(PointerVersion::FIRST.object_name());
    std::fs::remove_file(&first).expect("v0 is removed");
    std::fs::create_dir(&first).expect("a directory unreadable as v0 takes its name");
    created.release();

    let outcome = patch.await.expect("the sync runs");
    server.abort();
    assert_eq!(patches.load(Ordering::SeqCst), 1);
    let Err(BigQueryError::DatabaseError(error)) = outcome else {
        panic!("the sync fails with a database error, not {outcome:?}");
    };
    assert_eq!(error.public.code, format!("{:?}", tonic::Code::Unknown));
    assert!(!error.retry_possible);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_newest_pointer_whose_metadata_is_missing_fails_the_load() {
    let root = TempDir::new().expect("a temporary root");
    let catalog = catalog(root.path(), settings(Duration::from_secs(15 * 60)));
    create_dataset(&catalog).await;
    let created = catalog
        .create_table(&orders(), new_orders(&catalog))
        .await
        .expect("the table is created");
    let metadata = catalog
        .backend
        .object_path(created.metadata_location())
        .expect("the location is under the root");
    catalog
        .backend
        .delete(&metadata)
        .await
        .expect("the metadata file is deleted");

    let outcome = catalog.reload(&orders()).await;
    let Err(EngineError::MissingMetadata {
        what: Resource::Table(table),
        pointer_version,
        metadata_location,
    }) = outcome
    else {
        panic!("the metadata is missing, not {outcome:?}");
    };
    assert_eq!(table, orders());
    assert_eq!(pointer_version, 0);
    assert_eq!(metadata_location, created.metadata_location());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_requirement_unmet_by_the_stored_table_fails_the_commit() {
    let root = TempDir::new().expect("a temporary root");
    let catalog = catalog(
        root.path(),
        CatalogSettings {
            commit_retry_deadline: Duration::from_secs(2),
            ..settings(Duration::from_secs(15 * 60))
        },
    );
    create_dataset(&catalog).await;
    catalog
        .create_table(&orders(), new_orders(&catalog))
        .await
        .expect("the table is created");
    let another_table = uuid::Uuid::new_v4();
    for updates in [
        Vec::new(),
        vec![TableUpdate::SetProperties {
            updates: HashMap::from([("unmet".to_string(), "set".to_string())]),
        }],
    ] {
        let outcome = catalog
            .commit(&orders(), |_| {
                Ok(Change::Engine(EngineCommit {
                    requirements: vec![iceberg::TableRequirement::UuidMatch {
                        uuid: another_table,
                    }],
                    updates: updates.clone(),
                }))
            })
            .await;
        assert!(
            matches!(outcome, Err(EngineError::Precondition { .. })),
            "{outcome:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_table_lock_map_holds_only_the_tables_being_committed() {
    let root = TempDir::new().expect("a temporary root");
    let catalog = catalog(root.path(), settings(Duration::from_secs(15 * 60)));
    create_dataset(&catalog).await;
    catalog
        .create_table(&orders(), new_orders(&catalog))
        .await
        .expect("the table is created");
    let mut writers = tokio::task::JoinSet::new();
    for n in 0..20 {
        let catalog = Arc::clone(&catalog);
        writers.spawn(async move { set_by_engine(&catalog, &format!("concurrent.{n}")).await });
    }
    while let Some(written) = writers.join_next().await {
        written
            .expect("the writer runs")
            .expect("the update commits");
    }
    for n in 0..50 {
        let table = orders()
            .dataset_ref()
            .table(format!("result_{n}").parse().expect("a valid table ID"));
        let mut metadata = new_orders(&catalog)
            .into_builder(None)
            .set_location(catalog.table_location(&table))
            .build()
            .expect("the metadata builds")
            .metadata;
        metadata = metadata
            .into_builder(None)
            .assign_uuid(uuid::Uuid::new_v4())
            .build()
            .expect("the metadata builds")
            .metadata;
        catalog
            .create_table(&table, metadata)
            .await
            .expect("the table is created");
        catalog.purge(&table).await.expect("the table is purged");
    }
    assert_eq!(catalog.locks.tables_held(), 0);
}
