//! Rust values written to BigQuery and read back through Storage Read into the same Rust
//! types: once as GoogleSQL literals inserted with DML, and once through the crate's own
//! writer.

use base64::Engine;
use bigquery::*;
use serde::{Deserialize, Serialize};

mod common;
mod read_common;
use common::TestResult;
use read_common::{with_scratch, with_scratch_prefixed, Scratch};

#[derive(Deserialize, Debug, PartialEq, Clone)]
struct Home {
    county: String,
    zip: Option<i64>,
}

#[derive(Deserialize, Debug, PartialEq, Clone)]
struct Resident {
    id: i64,
    name: String,
    born: jiff::civil::Date,
    seen: jiff::Timestamp,
    seen_fast: BigQueryTimestamp,
    wakes: BigQueryTime,
    balance: String,
    photo: Vec<u8>,
    tags: Vec<String>,
    home: Option<Home>,
}

fn residents() -> Vec<Resident> {
    let names = [
        "Åsa Lindström",
        "Björn Ö'Hara",
        "Linnéa Ågren",
        "Örjan Näslund",
        "Märta Sjöberg",
        "Göran Ekström",
        "Saga Bäckström",
        "Håkan Åberg",
    ];
    names
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let i = i as i64;
            let seen =
                jiff::Timestamp::from_microsecond(1_700_000_000_123_456 + i * 86_400_000_000)
                    .expect("in range");
            Resident {
                id: i,
                name: name.to_string(),
                born: jiff::civil::date(1950 + i as i16 * 7, 1 + i as i8, 1 + 3 * i as i8),
                seen,
                seen_fast: BigQueryTimestamp(seen),
                wakes: BigQueryTime(jiff::civil::time(5 + i as i8, 30, 0, 250_000_000)),
                // Canonical NUMERIC text: no trailing fractional zeros.
                balance: format!("{}.{:02}", 1000 * i - 3500, i * 6 + 1),
                photo: (0..i as u8 * 3).collect(),
                tags: (0..i % 3).map(|t| format!("län{t}")).collect(),
                home: (i % 4 != 3).then(|| Home {
                    county: ["Skåne", "Uppsala", "Gotland"][(i % 3) as usize].into(),
                    zip: (i % 2 == 0).then_some(10_000 + i),
                }),
            }
        })
        .collect()
}

fn string_literal(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
}

fn timestamp_literal(t: jiff::Timestamp) -> String {
    format!("TIMESTAMP '{}'", t.strftime("%Y-%m-%d %H:%M:%S%.6f+00"))
}

fn row_literal(r: &Resident) -> String {
    let tags: Vec<String> = r.tags.iter().map(|t| string_literal(t)).collect();
    let home = match &r.home {
        Some(h) => format!(
            "STRUCT({} AS county, {} AS zip)",
            string_literal(&h.county),
            h.zip
                .map_or("CAST(NULL AS INT64)".into(), |z| z.to_string())
        ),
        None => "NULL".into(),
    };
    format!(
        "({}, {}, DATE '{}', {}, {}, TIME '{}', NUMERIC '{}', FROM_BASE64('{}'), ARRAY<STRING>[{}], {})",
        r.id,
        string_literal(&r.name),
        r.born,
        timestamp_literal(r.seen),
        timestamp_literal(r.seen_fast.0),
        r.wakes.0,
        r.balance,
        base64::engine::general_purpose::STANDARD.encode(&r.photo),
        tags.join(", "),
        home
    )
}

#[tokio::test]
async fn round_trip_every_type_through_dml() -> TestResult {
    with_scratch("round_trip_every_type_through_dml", async |s: &Scratch| {
        s.sql(
            "CREATE TABLE residents (id INT64 NOT NULL, name STRING NOT NULL, born DATE NOT NULL, \
             seen TIMESTAMP NOT NULL, seen_fast TIMESTAMP NOT NULL, wakes TIME NOT NULL, \
             balance NUMERIC NOT NULL, photo BYTES NOT NULL, tags ARRAY<STRING>, \
             home STRUCT<county STRING, zip INT64>)",
        )
        .await?;
        let expected = residents();
        let values: Vec<String> = expected.iter().map(row_literal).collect();
        s.sql(&format!(
            "INSERT INTO residents VALUES {}",
            values.join(", ")
        ))
        .await?;
        let mut got: Vec<Resident> =
            s.db.fluent()
                .select()
                .from((s.dataset.as_str(), "residents"))
                .obj()
                .query()
                .await?;
        got.sort_by_key(|r| r.id);
        assert_eq!(got, expected);
        Ok(())
    })
    .await
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
struct Pair {
    a: Option<i64>,
    b: String,
}

type Date = jiff::civil::Date;
type DateTime = jiff::civil::DateTime;
type Time = jiff::civil::Time;
type Timestamp = jiff::Timestamp;
type Json = serde_json::Value;

/// One column per type and mode: `n_` NULLABLE, `r_` REQUIRED, `a_` REPEATED.
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
struct EveryType {
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
    n_date: Option<Date>,
    r_date: Date,
    a_date: Vec<Date>,
    n_time: Option<Time>,
    r_time: Time,
    a_time: Vec<Time>,
    n_datetime: Option<DateTime>,
    r_datetime: DateTime,
    a_datetime: Vec<DateTime>,
    n_timestamp: Option<Timestamp>,
    r_timestamp: Timestamp,
    a_timestamp: Vec<Timestamp>,
    n_geography: Option<String>,
    r_geography: String,
    a_geography: Vec<String>,
    n_json: Option<Json>,
    r_json: Json,
    a_json: Vec<Json>,
    n_interval: Option<BigQueryInterval>,
    r_interval: BigQueryInterval,
    a_interval: Vec<BigQueryInterval>,
    n_range_date: Option<BigQueryRange<Date>>,
    r_range_date: BigQueryRange<Date>,
    a_range_date: Vec<BigQueryRange<Date>>,
    n_range_datetime: Option<BigQueryRange<DateTime>>,
    r_range_datetime: BigQueryRange<DateTime>,
    a_range_datetime: Vec<BigQueryRange<DateTime>>,
    n_range_timestamp: Option<BigQueryRange<Timestamp>>,
    r_range_timestamp: BigQueryRange<Timestamp>,
    a_range_timestamp: Vec<BigQueryRange<Timestamp>>,
    n_struct: Option<Pair>,
    r_struct: Pair,
    a_struct: Vec<Pair>,
}

/// The GoogleSQL type of each `<mode>_<name>` column of [`EveryType`].
const EVERY_TYPE_COLUMNS: &[(&str, &str)] = &[
    ("int64", "INT64"),
    ("float64", "FLOAT64"),
    ("numeric", "NUMERIC"),
    ("bignumeric", "BIGNUMERIC"),
    ("bool", "BOOL"),
    ("string", "STRING"),
    ("bytes", "BYTES"),
    ("date", "DATE"),
    ("time", "TIME"),
    ("datetime", "DATETIME"),
    ("timestamp", "TIMESTAMP"),
    ("geography", "GEOGRAPHY"),
    ("json", "JSON"),
    ("interval", "INTERVAL"),
    ("range_date", "RANGE<DATE>"),
    ("range_datetime", "RANGE<DATETIME>"),
    ("range_timestamp", "RANGE<TIMESTAMP>"),
    ("struct", "STRUCT<a INT64, b STRING>"),
];

fn every_type_table_sql() -> String {
    let mut columns = vec!["id INT64 NOT NULL".to_string()];
    for (name, ty) in EVERY_TYPE_COLUMNS {
        columns.push(format!("n_{name} {ty}"));
        columns.push(format!("r_{name} {ty} NOT NULL"));
        columns.push(format!("a_{name} ARRAY<{ty}>"));
    }
    format!("CREATE TABLE every_type ({})", columns.join(", "))
}

fn bounded<T>(start: T, end: T) -> BigQueryRange<T> {
    BigQueryRange {
        start: Some(start),
        end: Some(end),
    }
}

fn opt<T>(keep: bool, v: T) -> Option<T> {
    keep.then_some(v)
}

fn arr<T>(len: i64, f: impl Fn(i64) -> T) -> Vec<T> {
    (0..len).map(f).collect()
}

/// Twelve rows: every third has NULLs, and arrays of zero to two elements. Values are those
/// BigQuery gives back as written: canonical NUMERIC and WKT text, JSON without floats, and
/// REQUIRED RANGEs with both ends, since an unbounded end of a REQUIRED RANGE reads back as
/// the epoch.
fn every_type_rows() -> Vec<EveryType> {
    let names = ["Åsa", "Björn", "Linnéa", "Örjan", "Märta", "Göran"];
    (0..12)
        .map(|id| {
            let i = id as usize;
            let keep = id % 3 != 0;
            let len = id % 3;
            let date =
                |k: i64| jiff::civil::date(1950 + (id + k) as i16 * 5, 1 + (k % 12) as i8, 28);
            let time = |k: i64| jiff::civil::time((id + k) as i8, 30, 15, (k as i32) * 1_000);
            let datetime = |k: i64| date(k).at((id + k) as i8, 0, 1, 250_000_000);
            let timestamp = |k: i64| {
                jiff::Timestamp::from_microsecond(
                    1_700_000_000_123_456 + (id * 10 + k) * 86_400_000_000,
                )
                .expect("in range")
            };
            let interval = |k: i64| BigQueryInterval {
                months: (id + k) as i32,
                days: -(k as i32),
                nanos: (id * 3_600 + k) * 1_000_000_000 + 5_000,
            };
            let pair = |k: i64| Pair {
                a: (k % 2 == 0).then_some(id * 10 + k),
                b: names[(i + k as usize) % names.len()].into(),
            };
            EveryType {
                id,
                n_int64: opt(keep, id * 1_000),
                r_int64: -id,
                a_int64: arr(len, |k| id + k),
                n_float64: opt(keep, id as f64 * 0.25),
                r_float64: -(id as f64) * 1.5,
                a_float64: arr(len, |k| k as f64 / 8.0),
                n_numeric: opt(keep, format!("{id}.5")),
                r_numeric: format!("-{id}.000000001"),
                a_numeric: arr(len, |k| format!("{}", id * 100 + k)),
                n_bignumeric: opt(keep, format!("{id}.00000000000000000000000000000000000001")),
                r_bignumeric: format!("{}", id * 7),
                a_bignumeric: arr(len, |k| format!("-{k}.25")),
                n_bool: opt(keep, id % 2 == 0),
                r_bool: id % 2 == 1,
                a_bool: arr(len, |k| k == 0),
                n_string: opt(keep, names[i % names.len()].to_string()),
                r_string: format!("rad {id}"),
                a_string: arr(len, |k| format!("{}{k}", names[i % names.len()])),
                n_bytes: opt(keep, vec![id as u8, 255]),
                r_bytes: (0..id as u8).collect(),
                a_bytes: arr(len, |k| vec![k as u8; k as usize]),
                n_date: opt(keep, date(0)),
                r_date: date(1),
                a_date: arr(len, date),
                n_time: opt(keep, time(0)),
                r_time: time(1),
                a_time: arr(len, time),
                n_datetime: opt(keep, datetime(0)),
                r_datetime: datetime(1),
                a_datetime: arr(len, datetime),
                n_timestamp: opt(keep, timestamp(0)),
                r_timestamp: timestamp(1),
                a_timestamp: arr(len, timestamp),
                n_geography: opt(keep, format!("POINT({id} 59)")),
                r_geography: format!("POINT(18 {id})"),
                a_geography: arr(len, |k| format!("POINT({k} {id})")),
                n_json: opt(
                    keep,
                    serde_json::json!({"stad": names[i % names.len()], "n": id}),
                ),
                r_json: serde_json::json!([id, null, true]),
                a_json: arr(len, |k| serde_json::json!({"k": k})),
                n_interval: opt(keep, interval(0)),
                r_interval: interval(1),
                a_interval: arr(len, interval),
                n_range_date: opt(
                    keep,
                    BigQueryRange {
                        start: Some(date(0)),
                        end: None,
                    },
                ),
                r_range_date: bounded(date(0), date(1)),
                a_range_date: arr(len, |k| BigQueryRange {
                    start: None,
                    end: Some(date(k)),
                }),
                n_range_datetime: opt(
                    keep,
                    BigQueryRange {
                        start: None,
                        end: Some(datetime(0)),
                    },
                ),
                r_range_datetime: bounded(datetime(0), datetime(1)),
                a_range_datetime: arr(len, |k| bounded(datetime(k), datetime(k + 1))),
                n_range_timestamp: opt(
                    keep,
                    BigQueryRange {
                        start: Some(timestamp(0)),
                        end: None,
                    },
                ),
                r_range_timestamp: bounded(timestamp(0), timestamp(1)),
                a_range_timestamp: arr(len, |k| bounded(timestamp(k), timestamp(k + 1))),
                n_struct: opt(keep, pair(0)),
                r_struct: pair(1),
                a_struct: arr(len, pair),
            }
        })
        .collect()
}

#[tokio::test]
async fn round_trip_every_type_and_mode_through_the_writer() -> TestResult {
    with_scratch_prefixed(
        "bqp4b",
        "round_trip_every_type_and_mode_through_the_writer",
        async |s: &Scratch| {
            s.sql(&every_type_table_sql()).await?;
            let expected = every_type_rows();
            let summary =
                s.db.fluent()
                    .insert()
                    .into((s.project.as_str(), s.dataset.as_str(), "every_type"))
                    .objects(&expected)
                    .execute()
                    .await?;
            assert_eq!(summary.rows_written, expected.len() as u64);
            let mut got: Vec<EveryType> =
                s.db.fluent()
                    .select()
                    .from((s.dataset.as_str(), "every_type"))
                    .obj()
                    .query()
                    .await?;
            got.sort_by_key(|r| r.id);
            assert_eq!(got, expected);
            Ok(())
        },
    )
    .await
}
