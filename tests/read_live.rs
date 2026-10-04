//! Live table reads against BigQuery, on tables of a few rows in a scratch dataset. They run
//! only with `GCP_PROJECT` set.

use bigquery::arrow_schema::DataType;
use bigquery::*;
use futures::{StreamExt, TryStreamExt};
use gcloud_sdk::google::cloud::bigquery::storage::v1 as storage;
use serde::Deserialize;
use std::collections::BTreeSet;

mod common;
mod read_common;
use common::{with_scratch, Scratch, TestResult};
use read_common::run_sql;

/// One column per type and mode: `n_<type>` NULLABLE, `r_<type>` REQUIRED, `a_<type>`
/// REPEATED, with the NULLABLE value, the REQUIRED value, the array, and the GoogleSQL type.
const TYPES: &[(&str, &str, &str, &str, &str)] = &[
    ("int64", "INT64", "42", "-9223372036854775807 - 1", "[1, -2]"),
    ("float64", "FLOAT64", "1.5", "CAST('-inf' AS FLOAT64)", "[0.25, 1e308]"),
    (
        "numeric",
        "NUMERIC",
        "NUMERIC '123.450'",
        "NUMERIC '-0.000000001'",
        "[NUMERIC '0', NUMERIC '99999999999999999999999999999.999999999']",
    ),
    (
        "bignumeric",
        "BIGNUMERIC",
        "BIGNUMERIC '1.5'",
        "BIGNUMERIC '-578960446186580977117854925043439539266.34992332820282019728792003956564819968'",
        "[BIGNUMERIC '0.00000000000000000000000000000000000001']",
    ),
    ("bool", "BOOL", "TRUE", "FALSE", "[TRUE, FALSE]"),
    ("string", "STRING", "'Åsa Öberg'", "''", "['Linnéa', 'Göteborg']"),
    ("bytes", "BYTES", "b'\\x00\\xff'", "b''", "[b'a']"),
    ("date", "DATE", "DATE '2024-02-29'", "DATE '0001-01-01'", "[DATE '9999-12-31']"),
    (
        "time",
        "TIME",
        "TIME '12:34:56.789012'",
        "TIME '00:00:00'",
        "[TIME '23:59:59.999999']",
    ),
    (
        "datetime",
        "DATETIME",
        "DATETIME '2024-02-29 12:34:56.789012'",
        "DATETIME '0001-01-01 00:00:00'",
        "[DATETIME '9999-12-31 23:59:59.999999']",
    ),
    (
        "timestamp",
        "TIMESTAMP",
        "TIMESTAMP '2024-02-29 12:34:56.789012+00'",
        "TIMESTAMP '1969-07-20 20:17:40 UTC'",
        "[TIMESTAMP '0001-01-01 00:00:00 UTC']",
    ),
    (
        "geography",
        "GEOGRAPHY",
        "ST_GEOGPOINT(18.07, 59.33)",
        "ST_GEOGPOINT(0, 0)",
        "[ST_GEOGPOINT(1, 2)]",
    ),
    (
        "json",
        "JSON",
        "JSON '{\"stad\": \"Malmö\"}'",
        "JSON '[1, 2]'",
        "[JSON 'null']",
    ),
    (
        "interval",
        "INTERVAL",
        "INTERVAL '1-2 3 4:5:6.789' YEAR TO SECOND",
        "INTERVAL -3 DAY",
        "[INTERVAL 10 HOUR]",
    ),
    (
        "range_date",
        "RANGE<DATE>",
        "RANGE<DATE> '[2024-01-01, UNBOUNDED)'",
        "RANGE<DATE> '[2024-01-01, 2024-02-01)'",
        "[RANGE<DATE> '[UNBOUNDED, 2021-01-01)']",
    ),
    (
        "range_datetime",
        "RANGE<DATETIME>",
        "RANGE<DATETIME> '[2024-01-01 10:00:00, UNBOUNDED)'",
        "RANGE<DATETIME> '[2024-01-01 10:00:00, 2024-01-02 10:00:00)'",
        "[RANGE<DATETIME> '[2020-01-01 00:00:00, 2021-01-01 00:00:00)']",
    ),
    (
        "range_timestamp",
        "RANGE<TIMESTAMP>",
        "RANGE<TIMESTAMP> '[UNBOUNDED, 2024-01-01 00:00:00 UTC)'",
        "RANGE<TIMESTAMP> '[2024-01-01 00:00:00 UTC, 2024-01-02 00:00:00 UTC)'",
        "[RANGE<TIMESTAMP> '[2020-01-01 00:00:00 UTC, UNBOUNDED)']",
    ),
    (
        "struct",
        "STRUCT<a INT64, b STRING>",
        "STRUCT(1 AS a, 'Ö' AS b)",
        "STRUCT(2 AS a, CAST(NULL AS STRING) AS b)",
        "[STRUCT(3 AS a, 'y' AS b)]",
    ),
];

/// `t_all`: row 1 holds every value, row 2 NULLs and empty arrays. Two more columns pin
/// BigQuery behaviour: a parameterised NUMERIC, which arrives at its own precision and scale,
/// and a REQUIRED RANGE with an unbounded end.
fn all_types_sql() -> String {
    let mut columns = vec!["id INT64 NOT NULL".to_string()];
    let mut first = vec!["1 AS id".to_string()];
    let mut second = vec!["2".to_string()];
    for (name, ty, n, r, a) in TYPES {
        columns.push(format!("n_{name} {ty}"));
        columns.push(format!("r_{name} {ty} NOT NULL"));
        columns.push(format!("a_{name} ARRAY<{ty}>"));
        first.extend([n.to_string(), r.to_string(), a.to_string()]);
        second.extend([
            format!("CAST(NULL AS {ty})"),
            r.to_string(),
            format!("ARRAY<{ty}>[]"),
        ]);
    }
    columns.push("p_numeric NUMERIC(10, 2)".into());
    first.push("NUMERIC '12345678.91'".into());
    second.push("CAST(NULL AS NUMERIC)".into());
    columns.push("rr_range RANGE<DATE> NOT NULL".into());
    first.push("RANGE<DATE> '[UNBOUNDED, 2024-01-01)'".into());
    second.push("RANGE<DATE> '[2024-01-01, UNBOUNDED)'".into());
    format!(
        "CREATE TABLE t_all ({}) AS SELECT {} UNION ALL SELECT {}",
        columns.join(", "),
        first.join(", "),
        second.join(", ")
    )
}

#[derive(Deserialize, Debug, PartialEq)]
struct Pair {
    a: Option<i64>,
    b: Option<String>,
}

#[derive(Deserialize, Debug, PartialEq)]
struct AllTypes {
    id: i64,
    n_int64: Option<i64>,
    r_int64: i64,
    a_int64: Vec<i64>,
    n_float64: Option<f64>,
    r_float64: f64,
    a_float64: Vec<f64>,
    n_numeric: Option<String>,
    r_numeric: String,
    a_numeric: Vec<String>,
    n_bignumeric: Option<String>,
    r_bignumeric: String,
    a_bignumeric: Vec<String>,
    n_bool: Option<bool>,
    r_bool: bool,
    a_bool: Vec<bool>,
    n_string: Option<String>,
    r_string: String,
    a_string: Vec<String>,
    n_bytes: Option<Vec<u8>>,
    r_bytes: Vec<u8>,
    a_bytes: Vec<Vec<u8>>,
    n_date: Option<jiff::civil::Date>,
    r_date: jiff::civil::Date,
    a_date: Vec<jiff::civil::Date>,
    n_time: Option<jiff::civil::Time>,
    r_time: jiff::civil::Time,
    a_time: Vec<jiff::civil::Time>,
    n_datetime: Option<jiff::civil::DateTime>,
    r_datetime: jiff::civil::DateTime,
    a_datetime: Vec<jiff::civil::DateTime>,
    n_timestamp: Option<jiff::Timestamp>,
    r_timestamp: jiff::Timestamp,
    a_timestamp: Vec<jiff::Timestamp>,
    n_geography: Option<String>,
    r_geography: String,
    a_geography: Vec<String>,
    n_json: Option<BigQueryJson<serde_json::Value>>,
    r_json: BigQueryJson<serde_json::Value>,
    a_json: Vec<BigQueryJson<serde_json::Value>>,
    n_interval: Option<BigQueryInterval>,
    r_interval: BigQueryInterval,
    a_interval: Vec<BigQueryInterval>,
    n_range_date: Option<BigQueryRange<jiff::civil::Date>>,
    r_range_date: BigQueryRange<jiff::civil::Date>,
    a_range_date: Vec<BigQueryRange<jiff::civil::Date>>,
    n_range_datetime: Option<BigQueryRange<jiff::civil::DateTime>>,
    r_range_datetime: BigQueryRange<jiff::civil::DateTime>,
    a_range_datetime: Vec<BigQueryRange<jiff::civil::DateTime>>,
    n_range_timestamp: Option<BigQueryRange<jiff::Timestamp>>,
    r_range_timestamp: BigQueryRange<jiff::Timestamp>,
    a_range_timestamp: Vec<BigQueryRange<jiff::Timestamp>>,
    n_struct: Option<Pair>,
    r_struct: Pair,
    a_struct: Vec<Pair>,
    p_numeric: Option<String>,
    rr_range: BigQueryRange<jiff::civil::Date>,
}

fn ts(s: &str) -> jiff::Timestamp {
    s.parse().expect("a valid test timestamp")
}

fn dt(s: &str) -> jiff::civil::DateTime {
    s.parse().expect("a valid test datetime")
}

fn d(y: i16, m: i8, day: i8) -> jiff::civil::Date {
    jiff::civil::date(y, m, day)
}

fn range<T>(start: Option<T>, end: Option<T>) -> BigQueryRange<T> {
    BigQueryRange { start, end }
}

fn expected(id: i64) -> AllTypes {
    let full = id == 1;
    let some = |v| if full { Some(v) } else { None };
    let arr = |v: Vec<_>| if full { v } else { Vec::new() };
    AllTypes {
        id,
        n_int64: if full { Some(42) } else { None },
        r_int64: i64::MIN,
        a_int64: if full { vec![1, -2] } else { vec![] },
        n_float64: if full { Some(1.5) } else { None },
        r_float64: f64::NEG_INFINITY,
        a_float64: if full { vec![0.25, 1e308] } else { vec![] },
        n_numeric: some("123.45".to_string()),
        r_numeric: "-0.000000001".into(),
        a_numeric: arr(vec![
            "0".to_string(),
            "99999999999999999999999999999.999999999".into(),
        ]),
        n_bignumeric: some("1.5".to_string()),
        r_bignumeric:
            "-578960446186580977117854925043439539266.34992332820282019728792003956564819968".into(),
        a_bignumeric: arr(vec!["0.00000000000000000000000000000000000001".to_string()]),
        n_bool: if full { Some(true) } else { None },
        r_bool: false,
        a_bool: if full { vec![true, false] } else { vec![] },
        n_string: some("Åsa Öberg".to_string()),
        r_string: String::new(),
        a_string: arr(vec!["Linnéa".to_string(), "Göteborg".into()]),
        n_bytes: if full { Some(vec![0, 255]) } else { None },
        r_bytes: vec![],
        a_bytes: if full { vec![b"a".to_vec()] } else { vec![] },
        n_date: if full { Some(d(2024, 2, 29)) } else { None },
        r_date: d(1, 1, 1),
        a_date: if full { vec![d(9999, 12, 31)] } else { vec![] },
        n_time: if full {
            Some(jiff::civil::time(12, 34, 56, 789_012_000))
        } else {
            None
        },
        r_time: jiff::civil::time(0, 0, 0, 0),
        a_time: if full {
            vec![jiff::civil::time(23, 59, 59, 999_999_000)]
        } else {
            vec![]
        },
        n_datetime: if full {
            Some(dt("2024-02-29T12:34:56.789012"))
        } else {
            None
        },
        r_datetime: dt("0001-01-01T00:00:00"),
        a_datetime: if full {
            vec![dt("9999-12-31T23:59:59.999999")]
        } else {
            vec![]
        },
        n_timestamp: if full {
            Some(ts("2024-02-29T12:34:56.789012Z"))
        } else {
            None
        },
        r_timestamp: ts("1969-07-20T20:17:40Z"),
        a_timestamp: if full {
            vec![ts("0001-01-01T00:00:00Z")]
        } else {
            vec![]
        },
        n_geography: some("POINT(18.07 59.33)".to_string()),
        r_geography: "POINT(0 0)".into(),
        a_geography: arr(vec!["POINT(1 2)".to_string()]),
        n_json: if full {
            Some(BigQueryJson(serde_json::json!({"stad": "Malmö"})))
        } else {
            None
        },
        r_json: BigQueryJson(serde_json::json!([1, 2])),
        a_json: if full {
            vec![BigQueryJson(serde_json::Value::Null)]
        } else {
            vec![]
        },
        n_interval: if full {
            Some(BigQueryInterval {
                months: 14,
                days: 3,
                nanos: 14_706_789_000_000,
            })
        } else {
            None
        },
        r_interval: BigQueryInterval {
            months: 0,
            days: -3,
            nanos: 0,
        },
        a_interval: if full {
            vec![BigQueryInterval {
                months: 0,
                days: 0,
                nanos: 36_000_000_000_000,
            }]
        } else {
            vec![]
        },
        n_range_date: if full {
            Some(range(Some(d(2024, 1, 1)), None))
        } else {
            None
        },
        r_range_date: range(Some(d(2024, 1, 1)), Some(d(2024, 2, 1))),
        a_range_date: if full {
            vec![range(None, Some(d(2021, 1, 1)))]
        } else {
            vec![]
        },
        n_range_datetime: if full {
            Some(range(Some(dt("2024-01-01T10:00:00")), None))
        } else {
            None
        },
        r_range_datetime: range(
            Some(dt("2024-01-01T10:00:00")),
            Some(dt("2024-01-02T10:00:00")),
        ),
        a_range_datetime: if full {
            vec![range(
                Some(dt("2020-01-01T00:00:00")),
                Some(dt("2021-01-01T00:00:00")),
            )]
        } else {
            vec![]
        },
        n_range_timestamp: if full {
            Some(range(None, Some(ts("2024-01-01T00:00:00Z"))))
        } else {
            None
        },
        r_range_timestamp: range(
            Some(ts("2024-01-01T00:00:00Z")),
            Some(ts("2024-01-02T00:00:00Z")),
        ),
        a_range_timestamp: if full {
            vec![range(Some(ts("2020-01-01T00:00:00Z")), None)]
        } else {
            vec![]
        },
        n_struct: if full {
            Some(Pair {
                a: Some(1),
                b: Some("Ö".into()),
            })
        } else {
            None
        },
        r_struct: Pair {
            a: Some(2),
            b: None,
        },
        a_struct: if full {
            vec![Pair {
                a: Some(3),
                b: Some("y".into()),
            }]
        } else {
            vec![]
        },
        p_numeric: some("12345678.91".to_string()),
        // A REQUIRED RANGE carries no validity for its ends, so BigQuery's unbounded start
        // arrives as 1970-01-01.
        rr_range: if full {
            range(Some(d(1970, 1, 1)), Some(d(2024, 1, 1)))
        } else {
            range(Some(d(2024, 1, 1)), Some(d(1970, 1, 1)))
        },
    }
}

#[tokio::test]
async fn read_every_type_and_mode() -> TestResult {
    with_scratch("read_every_type_and_mode", async |s: &Scratch| {
        run_sql(s, &all_types_sql()).await?;
        let table = s.dataset.table(BigQueryTableId::from_static("t_all"));
        let mut rows: Vec<AllTypes> =
            s.db.fluent()
                .select()
                .from(table.clone())
                .obj()
                .query()
                .await?;
        rows.sort_by_key(|r| r.id);
        assert_eq!(rows, [expected(1), expected(2)]);

        let batches: Vec<_> =
            s.db.fluent()
                .select()
                .fields(["p_numeric"])
                .from(table)
                .record_batches()
                .await?
                .try_collect()
                .await?;
        let p_type = batches[0].schema().field(0).data_type().clone();
        eprintln!("LIVE NUMERIC(10, 2) arrives as {p_type}");
        assert_eq!(p_type, DataType::Decimal128(10, 2));
        Ok(())
    })
    .await
}

/// `people`: twelve rows of Swedish names, with counties and birth years.
const PEOPLE_SQL: &str = "CREATE TABLE people AS
    SELECT id, name, county, 1990 + id * 3 AS year
    FROM UNNEST([
        STRUCT(1 AS id, 'Åsa' AS name, 'Skåne' AS county),
        (2, 'Björn', 'Stockholm'), (3, 'Linnéa', 'Västra Götaland'), (4, 'Örjan', 'Örebro'),
        (5, 'Märta', 'Gävleborg'), (6, 'Göran', 'Jönköping'), (7, 'Saga', 'Uppsala'),
        (8, 'Håkan', 'Västerbotten'), (9, 'Elsa', 'Dalarna'), (10, 'Sören', 'Kalmar'),
        (11, 'Maja', 'Halland'), (12, 'Åke', 'Norrbotten')
    ])";

#[derive(Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Person {
    id: i64,
    name: String,
}

#[tokio::test]
async fn read_projection_and_row_restriction() -> TestResult {
    with_scratch(
        "read_projection_and_row_restriction",
        async |s: &Scratch| {
            run_sql(s, PEOPLE_SQL).await?;
            let table = s.dataset.table(BigQueryTableId::from_static("people"));

            let recent: BTreeSet<Person> =
                s.db.fluent()
                    .select()
                    .from(table.clone())
                    .filter(|f| {
                        f.for_all([
                            f.field("year").ge(2010),
                            f.field("county").neq("Norrbotten"),
                        ])
                    })
                    .obj::<Person>()
                    .stream_query()
                    .await?
                    .collect()
                    .await;
            let names: Vec<&str> = recent.iter().map(|p| p.name.as_str()).collect();
            assert_eq!(names, ["Saga", "Håkan", "Elsa", "Sören", "Maja"]);

            let batches: Vec<_> =
                s.db.fluent()
                    .select()
                    .fields(["name", "id"])
                    .from(table.clone())
                    .filter_sql("id <= 2")
                    .record_batches()
                    .await?
                    .try_collect()
                    .await?;
            let schema = batches[0].schema();
            let columns: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
            assert_eq!(
                columns,
                ["id", "name"],
                "the session's column order is the table's"
            );
            assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);

            let missing =
                s.db.fluent()
                    .select()
                    .fields(["id", "no_such_column"])
                    .from(table)
                    .record_batches()
                    .await;
            assert!(
                matches!(missing, Err(errors::BigQueryError::SchemaMismatchError(_))),
                "{:?}",
                missing.map(|_| ())
            );
            Ok(())
        },
    )
    .await
}

#[tokio::test]
async fn read_streams_merge() -> TestResult {
    with_scratch("read_streams_merge", async |s: &Scratch| {
        // Separate statements leave separate storage files, which the session can split.
        run_sql(s, "CREATE TABLE parts (id INT64, county STRING)").await?;
        for part in 0..4 {
            run_sql(s, &format!(
                "INSERT INTO parts SELECT {part} * 10 + n, 'Gotland' FROM UNNEST(GENERATE_ARRAY(1, 10)) AS n"
            ))
            .await?;
        }
        let options = BigQueryReadOptions::new()
            .with_max_stream_count(4)
            .with_preferred_min_stream_count(4);
        let session = s
            .db
            .read_client()
            .create_read_session(storage::CreateReadSessionRequest {
                parent: format!("projects/{}", s.project),
                read_session: Some(storage::ReadSession {
                    table: format!(
                        "projects/{}/datasets/{}/tables/parts",
                        s.project, s.dataset
                    ),
                    data_format: storage::DataFormat::Arrow.into(),
                    ..Default::default()
                }),
                max_stream_count: 4,
                preferred_min_stream_count: 4,
            })
            .await
            .map_err(errors::BigQueryError::from)?
            .into_inner();
        eprintln!("LIVE streams for 40 rows in 4 inserts: {}", session.streams.len());

        #[derive(Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord)]
        struct Part {
            id: i64,
        }
        let ids: Vec<i64> = s
            .db
            .fluent()
            .select()
            .from(s.dataset.table(BigQueryTableId::from_static("parts")))
            .options(options)
            .obj::<Part>()
            .stream_query_with_errors()
            .await?
            .map_ok(|p| p.id)
            .try_collect::<Vec<_>>()
            .await?;
        let mut sorted = ids.clone();
        sorted.sort_unstable();
        let expected: Vec<i64> = (0..4)
            .flat_map(|part| (1..=10).map(move |n| part * 10 + n))
            .collect();
        assert_eq!(sorted, expected, "every row arrives once");
        Ok(())
    })
    .await
}
