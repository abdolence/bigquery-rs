//! The write path through the `bigquery` client: rows of every type written with
//! `insert().objects()` read back equal, the layout of a partitioned table's files, and group
//! commit under concurrent requests.

use crate::catalog::CommitPoint;
use crate::testkit::{table_ref, TestEngine};
use bigquery::*;
use iceberg::expr::Reference;
use iceberg::spec::Datum;
use serde::{Deserialize, Serialize};

const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");
const TICKS: BigQueryTableId = BigQueryTableId::from_static("ticks");

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Coords {
    lat: Option<f64>,
    lon: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Shipping {
    city: Option<String>,
    zip: Option<String>,
    coords: Option<Coords>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Line {
    sku: String,
    quantity: Option<i64>,
}

/// A row of `orders`, one field per column type the engine stores.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Order {
    order_id: i64,
    customer: Option<String>,
    note: Option<String>,
    price: Option<f64>,
    total: Option<String>,
    amount: Option<String>,
    paid: Option<bool>,
    payload: Option<Vec<u8>>,
    ship_date: Option<BigQueryDate>,
    ship_time: Option<BigQueryTime>,
    local_at: Option<BigQueryDateTime>,
    placed_at: BigQueryTimestamp,
    area: Option<String>,
    attributes: Option<serde_json::Value>,
    window: Option<BigQueryRange<BigQueryDate>>,
    local_window: Option<BigQueryRange<BigQueryDateTime>>,
    active: Option<BigQueryRange<BigQueryTimestamp>>,
    shipping: Option<Shipping>,
    tags: Vec<String>,
    lines: Vec<Line>,
    scores: Vec<i64>,
}

/// `orders` with a column of every type `Order` holds, partitioned by day on `placed_at` when
/// `partitioned`.
fn declare_orders(db: &BigQueryDb, partitioned: bool) -> BigQueryTableSchemaBuilder<'_> {
    let declared = db
        .fluent()
        .schema()
        .table(SHOP.table(ORDERS))
        .columns(|columns| {
            columns.fields([
                columns.field("order_id").int64().required(),
                columns.field("customer").string(),
                columns.field("note").string_with_max_length(64),
                columns.field("price").float64(),
                columns.field("total").numeric(),
                columns.field("amount").numeric_with(10, 2),
                columns.field("paid").bool(),
                columns.field("payload").bytes(),
                columns.field("ship_date").date(),
                columns.field("ship_time").time(),
                columns.field("local_at").datetime(),
                columns.field("placed_at").timestamp().required(),
                columns.field("area").geography(),
                columns.field("attributes").json(),
                columns
                    .field("window")
                    .range(BigQueryRangeElementType::Date),
                columns
                    .field("local_window")
                    .range(BigQueryRangeElementType::DateTime),
                columns
                    .field("active")
                    .range(BigQueryRangeElementType::Timestamp),
                columns.field("shipping").record(|shipping| {
                    shipping.fields([
                        shipping.field("city").string(),
                        shipping.field("zip").string_with_max_length(10),
                        shipping.field("coords").record(|coords| {
                            coords.fields([
                                coords.field("lat").float64(),
                                coords.field("lon").float64(),
                            ])
                        }),
                    ])
                }),
                columns.field("tags").string().repeated(),
                columns.field("lines").repeated().record(|lines| {
                    lines.fields([
                        lines.field("sku").string().required(),
                        lines.field("quantity").int64(),
                    ])
                }),
                columns.field("scores").int64().repeated(),
            ])
        });
    if partitioned {
        declared.partition_by_day("placed_at")
    } else {
        declared
    }
}

fn timestamp(micros: i64) -> BigQueryTimestamp {
    BigQueryTimestamp(jiff::Timestamp::from_microsecond(micros).expect("in range"))
}

fn date(year: i16, month: i8, day: i8) -> BigQueryDate {
    BigQueryDate(jiff::civil::date(year, month, day))
}

fn datetime(year: i16, month: i8, day: i8, hour: i8) -> BigQueryDateTime {
    BigQueryDateTime(jiff::civil::date(year, month, day).at(hour, 30, 15, 250_000_000))
}

/// The midnight of 2026-10-07 UTC, in microseconds.
const OCTOBER_7: i64 = 1_791_331_200_000_000;
const DAY: i64 = 86_400_000_000;

/// Four orders on three days: every value of each type, NULLs and empty arrays included.
fn orders() -> Vec<Order> {
    let full = Order {
        order_id: 1,
        customer: Some("Åsa Lindström".to_string()),
        note: Some("leave at the door".to_string()),
        price: Some(12.5),
        total: Some("-1234567.123456789".to_string()),
        amount: Some("99.95".to_string()),
        paid: Some(true),
        payload: Some(vec![0, 1, 2, 255]),
        ship_date: Some(date(2026, 10, 9)),
        ship_time: Some(BigQueryTime(jiff::civil::time(13, 45, 30, 123_456_000))),
        local_at: Some(datetime(2026, 10, 7, 9)),
        placed_at: timestamp(OCTOBER_7 + 3_600_000_000),
        area: Some("POINT(18.07 59.33)".to_string()),
        attributes: Some(serde_json::json!({"gift": true, "tags": ["a", "b"], "n": 1.5})),
        window: Some(BigQueryRange {
            start: Some(date(2026, 1, 1)),
            end: None,
        }),
        local_window: Some(BigQueryRange {
            start: Some(datetime(2026, 1, 1, 0)),
            end: Some(datetime(2026, 2, 1, 0)),
        }),
        active: Some(BigQueryRange {
            start: None,
            end: Some(timestamp(OCTOBER_7)),
        }),
        shipping: Some(Shipping {
            city: Some("Uppsala".to_string()),
            zip: Some("75310".to_string()),
            coords: Some(Coords {
                lat: Some(59.86),
                lon: Some(17.64),
            }),
        }),
        tags: vec!["new".to_string(), "priority".to_string()],
        lines: vec![
            Line {
                sku: "A-1".to_string(),
                quantity: Some(2),
            },
            Line {
                sku: "B-2".to_string(),
                quantity: None,
            },
        ],
        scores: vec![i64::MIN, 0, i64::MAX],
    };
    let sparse = Order {
        order_id: 2,
        customer: None,
        note: None,
        price: Some(f64::INFINITY),
        total: None,
        amount: Some("-0.5".to_string()),
        paid: None,
        payload: None,
        ship_date: None,
        ship_time: None,
        local_at: None,
        placed_at: timestamp(OCTOBER_7 + DAY),
        area: None,
        attributes: None,
        window: None,
        local_window: None,
        active: None,
        shipping: Some(Shipping {
            city: None,
            zip: None,
            coords: None,
        }),
        tags: Vec::new(),
        lines: Vec::new(),
        scores: Vec::new(),
    };
    let third = Order {
        order_id: 3,
        customer: Some("Björn".to_string()),
        price: Some(-0.0),
        total: Some("0.000000001".to_string()),
        shipping: None,
        attributes: Some(serde_json::json!([1, "two", null])),
        placed_at: timestamp(OCTOBER_7 + DAY + 7),
        ..sparse.clone()
    };
    let fourth = Order {
        order_id: 4,
        placed_at: timestamp(OCTOBER_7 + 2 * DAY + 1),
        ..full.clone()
    };
    vec![full, sparse, third, fourth]
}

/// Writes `rows` into `shop.orders` with one `insert().objects()`.
async fn insert(engine: &TestEngine, rows: &[Order]) -> BigQueryWriteSummary {
    engine
        .db
        .fluent()
        .insert()
        .into(SHOP.table(ORDERS))
        .objects(rows)
        .execute()
        .await
        .expect("the rows are written")
}

/// Every order through Storage Read and through a query, each sorted by ID.
async fn read_back(engine: &TestEngine) -> (Vec<Order>, Vec<Order>) {
    let mut scanned: Vec<Order> = engine
        .db
        .fluent()
        .select()
        .from(SHOP.table(ORDERS))
        .obj()
        .query()
        .await
        .expect("the table reads");
    scanned.sort_by_key(|order| order.order_id);
    let queried: Vec<Order> = engine
        .db
        .fluent()
        .query("SELECT * FROM shop.orders ORDER BY order_id")
        .obj()
        .query()
        .await
        .expect("the query runs");
    (scanned, queried)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_type_round_trips_through_storage_write_read_and_a_query() {
    let engine = TestEngine::start().await;
    engine.create_dataset("shop", "EU").await;
    declare_orders(&engine.db, false)
        .sync()
        .await
        .expect("the table is created");
    let written = orders();
    let summary = insert(&engine, &written).await;
    assert_eq!(summary.rows_written, 4);

    let (scanned, queried) = read_back(&engine).await;
    assert_eq!(scanned, written);
    assert_eq!(queried, written);
    engine
        .assert_no_path_twice(&table_ref("shop", "orders"))
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_day_partitioned_table_stores_a_file_per_day_and_prunes_by_day() {
    let engine = TestEngine::start().await;
    engine.create_dataset("shop", "EU").await;
    declare_orders(&engine.db, true)
        .sync()
        .await
        .expect("the table is created");
    let written = orders();
    insert(&engine, &written).await;

    let (scanned, queried) = read_back(&engine).await;
    assert_eq!(scanned, written);
    assert_eq!(queried, written);

    let orders = table_ref("shop", "orders");
    let mut directories: Vec<String> = engine
        .plan_files(&orders, None)
        .await
        .into_iter()
        .map(|task| {
            let path = std::path::Path::new(&task.data_file_path);
            let directory = path.parent().expect("a file has a directory");
            let data = directory
                .parent()
                .expect("a partition has a data directory");
            assert!(data.ends_with("data"), "{}", task.data_file_path);
            directory
                .file_name()
                .expect("a partition directory has a name")
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    directories.sort();
    assert_eq!(
        directories,
        [
            "placed_at_day=2026-10-07",
            "placed_at_day=2026-10-08",
            "placed_at_day=2026-10-09",
        ]
    );

    let second_day = Reference::new("placed_at")
        .greater_than_or_equal_to(Datum::timestamptz_micros(OCTOBER_7 + DAY))
        .and(Reference::new("placed_at").less_than(Datum::timestamptz_micros(OCTOBER_7 + 2 * DAY)));
    let pruned = engine.plan_files(&orders, Some(second_day)).await;
    assert_eq!(pruned.len(), 1);
    assert!(pruned[0]
        .data_file_path
        .contains("/data/placed_at_day=2026-10-08/"));
    engine.assert_no_path_twice(&orders).await;
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Tick {
    request: i64,
    n: i64,
}

/// The requests the concurrent-writes gate sends at once.
const REQUESTS: i64 = 64;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_default_stream_requests_share_commits_and_land_once() {
    let engine = TestEngine::start().await;
    engine.create_dataset("shop", "EU").await;
    engine
        .db
        .fluent()
        .schema()
        .table(SHOP.table(TICKS))
        .columns(|columns| {
            columns.fields([
                columns.field("request").int64().required(),
                columns.field("n").int64().required(),
            ])
        })
        .sync()
        .await
        .expect("the table is created");
    let table = table_ref("shop", "ticks");

    // The first group's commit is held after it loaded the table, until every request has
    // written its files and joined a queue: whatever did not make the first group lands in one
    // more commit.
    let mut first = engine.state().catalog.hooks().hold(CommitPoint::Loaded);
    let started = std::time::Instant::now();
    let mut requests = tokio::task::JoinSet::new();
    for request in 0..REQUESTS {
        let db = engine.db.clone();
        requests.spawn(async move {
            let rows: Vec<Tick> = (0..10).map(|n| Tick { request, n }).collect();
            db.fluent()
                .insert()
                .into(SHOP.table(TICKS))
                .objects(&rows)
                .execute()
                .await
        });
    }
    first.reached().await;
    let groups = engine.state().writes.groups().clone();
    while groups.joined() < usize::try_from(REQUESTS).expect("fits") {
        tokio::task::yield_now().await;
    }
    first.release();
    while let Some(written) = requests.join_next().await {
        let summary = written
            .expect("the request runs")
            .expect("the rows are written");
        assert_eq!(summary.rows_written, 10);
    }
    let elapsed = started.elapsed();

    let loaded = engine
        .state()
        .catalog
        .reload(&table)
        .await
        .expect("the table loads");
    let commits = loaded.metadata().snapshots().count();
    assert!(
        commits <= 2,
        "the held group and the rest, not {commits} commits"
    );
    let mut read: Vec<Tick> = engine
        .db
        .fluent()
        .select()
        .from(SHOP.table(TICKS))
        .obj()
        .query()
        .await
        .expect("the table reads");
    read.sort_by_key(|tick| (tick.request, tick.n));
    let expected: Vec<Tick> = (0..REQUESTS)
        .flat_map(|request| (0..10).map(move |n| Tick { request, n }))
        .collect();
    assert_eq!(read, expected);
    assert_eq!(groups.running(), 0, "no commit queue outlives its commits");
    engine.assert_no_path_twice(&table).await;
    eprintln!(
        "{REQUESTS} held default-stream requests landed in {commits} commits in {} ms",
        elapsed.as_millis()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_requests_without_a_hold_land_in_fewer_commits_than_requests() {
    let engine = TestEngine::start().await;
    engine.create_dataset("shop", "EU").await;
    engine
        .db
        .fluent()
        .schema()
        .table(SHOP.table(TICKS))
        .columns(|columns| {
            columns.fields([
                columns.field("request").int64().required(),
                columns.field("n").int64().required(),
            ])
        })
        .sync()
        .await
        .expect("the table is created");
    let started = std::time::Instant::now();
    let mut requests = tokio::task::JoinSet::new();
    for request in 0..REQUESTS {
        let db = engine.db.clone();
        requests.spawn(async move {
            db.fluent()
                .insert()
                .into(SHOP.table(TICKS))
                .objects(&[Tick { request, n: 0 }])
                .execute()
                .await
        });
    }
    while let Some(written) = requests.join_next().await {
        written
            .expect("the request runs")
            .expect("the row is written");
    }
    let elapsed = started.elapsed();
    let table = table_ref("shop", "ticks");
    let loaded = engine
        .state()
        .catalog
        .reload(&table)
        .await
        .expect("the table loads");
    let commits = loaded.metadata().snapshots().count();
    eprintln!(
        "{REQUESTS} unheld default-stream requests landed in {commits} commits in {} ms, {:.1} \
         commits per second",
        elapsed.as_millis(),
        commits as f64 / elapsed.as_secs_f64()
    );
    assert!(commits < 64, "{commits} commits");
    engine.assert_no_path_twice(&table).await;
}
