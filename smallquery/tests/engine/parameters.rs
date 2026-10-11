//! Query parameters of every type the `bigquery` client sends, named and positional, read back
//! through the client's row decoding.

use super::support::{client, serve};
use bigquery::errors::BigQueryError;
use bigquery::jiff::civil::{date, time};
use bigquery::jiff::Timestamp;
use bigquery::*;
use serde::{Deserialize, Serialize};

const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Line {
    sku: String,
    quantity: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Coords {
    lat: f64,
    lon: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Shipping {
    city: String,
    coords: Coords,
}

/// One value of every parameter type, each column named after its parameter.
#[derive(Debug, Clone, PartialEq, Deserialize)]
struct EveryType {
    int64: i64,
    float64: f64,
    numeric: BigQueryDecimal<String>,
    bool: bool,
    string: String,
    bytes: Vec<u8>,
    date: BigQueryDate,
    time: BigQueryTime,
    datetime: BigQueryDateTime,
    timestamp: BigQueryTimestamp,
    geography: String,
    json: BigQueryJson<serde_json::Value>,
    interval: BigQueryInterval,
    date_range: BigQueryRange<BigQueryDate>,
    datetime_range: BigQueryRange<BigQueryDateTime>,
    timestamp_range: BigQueryRange<BigQueryTimestamp>,
    int64_array: Vec<i64>,
    string_array: Vec<String>,
    lines: Vec<Line>,
    shipping: Shipping,
}

/// The columns of [`EveryType`], in the order [`Placeholders::bind_every_type`] binds them.
const EVERY_TYPE: [&str; 20] = [
    "int64",
    "float64",
    "numeric",
    "bool",
    "string",
    "bytes",
    "date",
    "time",
    "datetime",
    "timestamp",
    "geography",
    "json",
    "interval",
    "date_range",
    "datetime_range",
    "timestamp_range",
    "int64_array",
    "string_array",
    "lines",
    "shipping",
];

/// [`EveryType`] with every column NULL; an ARRAY parameter that is NULL reads as empty, as
/// BigQuery returns it.
#[derive(Debug, Clone, PartialEq, Deserialize)]
struct EveryTypeNull {
    int64: Option<i64>,
    float64: Option<f64>,
    numeric: Option<BigQueryDecimal<String>>,
    bool: Option<bool>,
    string: Option<String>,
    bytes: Option<Vec<u8>>,
    date: Option<BigQueryDate>,
    time: Option<BigQueryTime>,
    datetime: Option<BigQueryDateTime>,
    timestamp: Option<BigQueryTimestamp>,
    geography: Option<String>,
    json: Option<BigQueryJson<serde_json::Value>>,
    interval: Option<BigQueryInterval>,
    date_range: Option<BigQueryRange<BigQueryDate>>,
    datetime_range: Option<BigQueryRange<BigQueryDateTime>>,
    timestamp_range: Option<BigQueryRange<BigQueryTimestamp>>,
    int64_array: Vec<i64>,
    string_array: Vec<String>,
    lines: Vec<Line>,
    shipping: Option<Shipping>,
}

fn every_type() -> EveryType {
    let placed = Timestamp::from_microsecond(1_760_000_000_123_456).expect("a valid instant");
    EveryType {
        int64: -42,
        float64: 1.25e-3,
        numeric: BigQueryDecimal("12345.678901234".to_string()),
        bool: true,
        string: "naïve café".to_string(),
        bytes: vec![0, 1, 254, 255],
        date: BigQueryDate(date(2026, 10, 11)),
        time: BigQueryTime(time(23, 59, 58, 123_456_000)),
        datetime: BigQueryDateTime(date(2026, 10, 11).at(8, 30, 0, 250_000_000)),
        timestamp: BigQueryTimestamp(placed),
        geography: "POINT(1 2)".to_string(),
        json: BigQueryJson(serde_json::json!({"a": [1, 2], "b": null})),
        interval: BigQueryInterval {
            months: -14,
            days: 3,
            nanos: 4_000_500_000,
        },
        date_range: BigQueryRange {
            start: Some(BigQueryDate(date(2026, 1, 1))),
            end: None,
        },
        datetime_range: BigQueryRange {
            start: Some(BigQueryDateTime(date(2026, 1, 1).at(0, 0, 0, 0))),
            end: Some(BigQueryDateTime(date(2026, 2, 1).at(12, 0, 0, 0))),
        },
        timestamp_range: BigQueryRange {
            start: None,
            end: Some(BigQueryTimestamp(placed)),
        },
        int64_array: vec![1, -2, 3],
        string_array: vec!["a".to_string(), String::new()],
        lines: vec![
            Line {
                sku: "pen".to_string(),
                quantity: 2,
            },
            Line {
                sku: "ink".to_string(),
                quantity: 1,
            },
        ],
        shipping: Shipping {
            city: "Paris".to_string(),
            coords: Coords {
                lat: 48.85,
                lon: 2.35,
            },
        },
    }
}

/// The STRUCT type of a [`Line`], for a NULL that cannot be inferred.
fn line_type() -> BigQueryFieldType {
    BigQueryFieldType::Struct(vec![
        field("sku", BigQueryFieldType::String { max_length: None }),
        field("quantity", BigQueryFieldType::Int64),
    ])
}

/// The STRUCT type of a [`Shipping`], for a NULL that cannot be inferred.
fn shipping_type() -> BigQueryFieldType {
    BigQueryFieldType::Struct(vec![
        field("city", BigQueryFieldType::String { max_length: None }),
        field(
            "coords",
            BigQueryFieldType::Struct(vec![
                field("lat", BigQueryFieldType::Float64),
                field("lon", BigQueryFieldType::Float64),
            ]),
        ),
    ])
}

fn field(name: &str, field_type: BigQueryFieldType) -> BigQueryFieldSchema {
    BigQueryFieldSchema {
        name: name.to_string(),
        field_type,
        mode: BigQueryFieldMode::Nullable,
        description: None,
        default_value_expression: None,
    }
}

type Query<'a> = BigQueryQueryBuilder<'a, BigQueryDb>;

/// How a statement names its parameters.
#[derive(Debug, Clone, Copy)]
enum Placeholders {
    /// `@name`, bound by name.
    Named,
    /// `?`, bound in order.
    Positional,
}

impl Placeholders {
    /// `SELECT` of one parameter per column, each aliased to the column's name.
    fn select(self, columns: &[&str]) -> String {
        let list = columns
            .iter()
            .map(|column| match self {
                Placeholders::Named => format!("@{column} AS {column}"),
                Placeholders::Positional => format!("? AS {column}"),
            })
            .collect::<Vec<_>>()
            .join(", ");
        format!("SELECT {list}")
    }

    fn typed<'a, V: Serialize>(
        self,
        query: Query<'a>,
        name: &str,
        param_type: impl Into<BigQueryParamType>,
        value: V,
    ) -> Query<'a> {
        match self {
            Placeholders::Named => query.param_as(name, param_type, value),
            Placeholders::Positional => query.positional_param_as(param_type, value),
        }
    }

    fn inferred<'a, V: Serialize>(self, query: Query<'a>, name: &str, value: V) -> Query<'a> {
        match self {
            Placeholders::Named => query.param(name, value),
            Placeholders::Positional => query.positional_param(value),
        }
    }

    /// Binds `values` in the order of [`EVERY_TYPE`].
    fn bind_every_type<'a>(self, query: Query<'a>, values: &EveryType) -> Query<'a> {
        let query = self.inferred(query, "int64", values.int64);
        let query = self.inferred(query, "float64", values.float64);
        let query = self.inferred(query, "numeric", &values.numeric);
        let query = self.inferred(query, "bool", values.bool);
        let query = self.inferred(query, "string", &values.string);
        let query = self.typed(
            query,
            "bytes",
            BigQueryFieldType::Bytes { max_length: None },
            &values.bytes,
        );
        let query = self.inferred(query, "date", values.date);
        let query = self.inferred(query, "time", values.time);
        let query = self.inferred(query, "datetime", values.datetime);
        let query = self.inferred(query, "timestamp", values.timestamp);
        let query = self.typed(
            query,
            "geography",
            BigQueryFieldType::Geography,
            &values.geography,
        );
        let query = self.inferred(query, "json", &values.json);
        let query = self.inferred(query, "interval", values.interval);
        let query = self.inferred(query, "date_range", values.date_range);
        let query = self.inferred(query, "datetime_range", values.datetime_range);
        let query = self.inferred(query, "timestamp_range", values.timestamp_range);
        let query = self.inferred(query, "int64_array", &values.int64_array);
        let query = self.inferred(query, "string_array", &values.string_array);
        let query = self.inferred(query, "lines", &values.lines);
        self.inferred(query, "shipping", &values.shipping)
    }

    /// Binds a NULL of every type in the order of [`EVERY_TYPE`].
    fn bind_every_null<'a>(self, query: Query<'a>) -> Query<'a> {
        use BigQueryFieldType as Type;
        let string = Type::String { max_length: None };
        let scalars: [(&str, Type); 16] = [
            ("int64", Type::Int64),
            ("float64", Type::Float64),
            ("numeric", Type::Numeric(None)),
            ("bool", Type::Bool),
            ("string", string.clone()),
            ("bytes", Type::Bytes { max_length: None }),
            ("date", Type::Date),
            ("time", Type::Time),
            ("datetime", Type::DateTime),
            ("timestamp", Type::Timestamp),
            ("geography", Type::Geography),
            ("json", Type::Json),
            ("interval", Type::Interval),
            ("date_range", Type::Range(BigQueryRangeElementType::Date)),
            (
                "datetime_range",
                Type::Range(BigQueryRangeElementType::DateTime),
            ),
            (
                "timestamp_range",
                Type::Range(BigQueryRangeElementType::Timestamp),
            ),
        ];
        let query = scalars
            .into_iter()
            .fold(query, |query, (name, param_type)| {
                self.typed(query, name, param_type, None::<()>)
            });
        let query = self.typed(
            query,
            "int64_array",
            BigQueryParamType::array_of(Type::Int64),
            None::<()>,
        );
        let query = self.typed(
            query,
            "string_array",
            BigQueryParamType::array_of(string),
            None::<()>,
        );
        let query = self.typed(
            query,
            "lines",
            BigQueryParamType::array_of(line_type()),
            None::<()>,
        );
        self.typed(query, "shipping", shipping_type(), None::<()>)
    }
}

const STYLES: [Placeholders; 2] = [Placeholders::Named, Placeholders::Positional];

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_parameter_of_every_type_reads_back_as_sent() -> BigQueryResult<()> {
    let served = serve().await;
    let db = client(served.addr).await;
    let sent = every_type();
    for style in STYLES {
        let query = db.fluent().query(style.select(&EVERY_TYPE));
        let read: Vec<EveryType> = style.bind_every_type(query, &sent).obj().query().await?;
        assert_eq!(read, vec![sent.clone()], "{style:?}");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_null_parameter_of_every_type_reads_back_as_null() -> BigQueryResult<()> {
    let served = serve().await;
    let db = client(served.addr).await;
    for style in STYLES {
        let query = db.fluent().query(style.select(&EVERY_TYPE));
        let read: Vec<EveryTypeNull> = style.bind_every_null(query).obj().query().await?;
        assert_eq!(
            read,
            vec![EveryTypeNull {
                int64: None,
                float64: None,
                numeric: None,
                bool: None,
                string: None,
                bytes: None,
                date: None,
                time: None,
                datetime: None,
                timestamp: None,
                geography: None,
                json: None,
                interval: None,
                date_range: None,
                datetime_range: None,
                timestamp_range: None,
                int64_array: Vec::new(),
                string_array: Vec::new(),
                lines: Vec::new(),
                shipping: None,
            }],
            "{style:?}"
        );
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
struct Floats {
    nan: f64,
    infinity: f64,
    negative_infinity: f64,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn float64_parameters_keep_nan_and_the_infinities() -> BigQueryResult<()> {
    let served = serve().await;
    let db = client(served.addr).await;
    for style in STYLES {
        let query = db
            .fluent()
            .query(style.select(&["nan", "infinity", "negative_infinity"]));
        let query = style.inferred(query, "nan", f64::NAN);
        let query = style.inferred(query, "infinity", f64::INFINITY);
        let query = style.inferred(query, "negative_infinity", f64::NEG_INFINITY);
        let read: Vec<Floats> = query.obj().query().await?;
        assert_eq!(read.len(), 1, "{style:?}");
        assert!(read[0].nan.is_nan(), "{style:?}: {:?}", read[0]);
        assert_eq!(read[0].infinity, f64::INFINITY, "{style:?}");
        assert_eq!(read[0].negative_infinity, f64::NEG_INFINITY, "{style:?}");
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize)]
struct Order {
    order_id: i64,
    customer: String,
    ship_date: BigQueryDate,
    placed_at: BigQueryTimestamp,
    total: BigQueryDecimal<String>,
}

#[derive(Debug, PartialEq, Deserialize)]
struct OrderId {
    order_id: i64,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_parameter_filters_a_table_column_of_its_type() -> BigQueryResult<()> {
    let served = serve().await;
    let db = client(served.addr).await;
    db.fluent()
        .schema()
        .dataset(SHOP)
        .create()
        .execute()
        .await?;
    db.fluent()
        .schema()
        .table(SHOP.table(ORDERS))
        .columns(|columns| {
            columns.fields([
                columns.field("order_id").int64().required(),
                columns.field("customer").string(),
                columns.field("ship_date").date(),
                columns.field("placed_at").timestamp(),
                columns.field("total").numeric(),
            ])
        })
        .sync()
        .await?;
    let orders: Vec<Order> = [(1, 2), (2, 3), (3, 4)]
        .into_iter()
        .map(|(order_id, ship_day)| Order {
            order_id,
            customer: format!("customer-{order_id}"),
            ship_date: BigQueryDate(date(2026, 10, ship_day)),
            placed_at: BigQueryTimestamp(
                Timestamp::from_second(1_760_000_000 + order_id).expect("a valid instant"),
            ),
            total: BigQueryDecimal(format!("{order_id}.5")),
        })
        .collect();
    db.fluent()
        .insert()
        .into(SHOP.table(ORDERS))
        .objects(orders.clone())
        .execute()
        .await?;

    let second = &orders[1];
    let filters = [
        (
            "order_id",
            db.fluent()
                .query("SELECT order_id FROM shop.orders WHERE order_id = @order_id")
                .param("order_id", second.order_id),
        ),
        (
            "customer",
            db.fluent()
                .query("SELECT order_id FROM shop.orders WHERE customer = @customer")
                .param("customer", &second.customer),
        ),
        (
            "ship_date",
            db.fluent()
                .query("SELECT order_id FROM shop.orders WHERE ship_date = @ship_date")
                .param("ship_date", second.ship_date),
        ),
        (
            "placed_at",
            db.fluent()
                .query("SELECT order_id FROM shop.orders WHERE placed_at = @placed_at")
                .param("placed_at", second.placed_at),
        ),
        (
            "total",
            db.fluent()
                .query("SELECT order_id FROM shop.orders WHERE total = @total")
                .param("total", &second.total),
        ),
    ];
    for (column, query) in filters {
        let read: Vec<OrderId> = query.obj().query().await?;
        assert_eq!(
            read,
            vec![OrderId {
                order_id: second.order_id
            }],
            "{column}"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_placeholder_without_a_parameter_is_invalid_argument() -> BigQueryResult<()> {
    let served = serve().await;
    let db = client(served.addr).await;
    let refused = db
        .fluent()
        .query("SELECT @present AS present, @missing AS missing")
        .param("present", 1)
        .execute()
        .await;
    assert!(
        matches!(&refused, Err(BigQueryError::DatabaseError(err)) if err.public.code == "InvalidArgument"),
        "{refused:?}"
    );
    Ok(())
}

#[derive(Debug, PartialEq, Deserialize)]
struct Used {
    used: i64,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_parameter_the_statement_never_names_is_accepted() -> BigQueryResult<()> {
    let served = serve().await;
    let db = client(served.addr).await;
    let read: Vec<Used> = db
        .fluent()
        .query("SELECT @used AS used")
        .param("used", 7)
        .param("unused", "spare")
        .obj()
        .query()
        .await?;
    assert_eq!(read, vec![Used { used: 7 }]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bignumeric_parameter_is_unimplemented() -> BigQueryResult<()> {
    let served = serve().await;
    let db = client(served.addr).await;
    let refused = db
        .fluent()
        .query("SELECT @big AS big")
        .param_as(
            "big",
            BigQueryFieldType::BigNumeric(None),
            BigQueryDecimal("1.5".to_string()),
        )
        .execute()
        .await;
    assert!(
        matches!(&refused, Err(BigQueryError::DatabaseError(err)) if err.public.code == "Unimplemented"),
        "{refused:?}"
    );
    Ok(())
}
