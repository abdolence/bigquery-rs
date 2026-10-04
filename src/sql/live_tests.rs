//! BigQuery parses every rendered literal back to the value it was rendered from. A handful of
//! constant queries cover every kind, so they bill nothing. They run only with `GCP_PROJECT`
//! set, and live here rather than under `tests/` because the renderer is crate-private.

use super::tests::{every_char_class, injection_corpus};
use super::{quote_identifier, SqlLiteral};
use crate::query::{infer_param, ParamLabel};
use crate::{
    BigQueryDate, BigQueryDb, BigQueryDecimal, BigQueryInterval, BigQueryJson, BigQueryRange,
    BigQueryResult,
};
use gcloud_sdk::google::cloud::bigquery::v2 as bq;
use serde::{Deserialize, Serialize};

const RUN_LABEL: &str = "bqp4c_run";

/// The corpus values a single query can carry: the 1 MiB entry is above the 1,024K
/// characters an unresolved GoogleSQL query may have, so a 100 KB run of quotes stands in for
/// it.
fn live_corpus() -> Vec<String> {
    injection_corpus()
        .into_iter()
        .filter(|v| v.len() < 1000)
        .chain(["'".repeat(100_000), every_char_class()])
        .collect()
}

fn infer_literal<V: Serialize>(value: V) -> SqlLiteral {
    let param = infer_param(ParamLabel::Positional(0), &value).expect("an inferable value");
    SqlLiteral::try_from((
        &param.parameter_type.expect("a type"),
        &param.parameter_value.expect("a value"),
    ))
    .expect("a literal")
}

/// `SELECT i, v FROM UNNEST([...])` over `values`, numbered from 0.
fn numbered_rows(values: impl IntoIterator<Item = String>) -> String {
    let rows: Vec<String> = values
        .into_iter()
        .enumerate()
        .map(|(i, v)| format!("STRUCT({i} AS i, {v} AS v)"))
        .collect();
    format!("SELECT i, v FROM UNNEST([{}]) ORDER BY i", rows.join(", "))
}

struct Live {
    db: BigQueryDb,
    project: String,
    run: String,
    started_ms: u64,
}

impl Live {
    async fn rows<T>(&self, sql: &str) -> BigQueryResult<Vec<T>>
    where
        T: serde::de::DeserializeOwned + Send + 'static,
    {
        self.db
            .fluent()
            .query(sql)
            .label(RUN_LABEL, self.run.clone())
            .obj::<T>()
            .query()
            .await
    }

    /// The bytes billed by this run's jobs, from `ListJobs`, which bills nothing.
    async fn bytes_billed(&self) -> BigQueryResult<i64> {
        let mut billed = 0;
        let mut page_token = String::new();
        loop {
            let page = self
                .db
                .job_client()
                .list_jobs(bq::ListJobsRequest {
                    project_id: self.project.clone(),
                    min_creation_time: self.started_ms,
                    projection: bq::list_jobs_request::Projection::Full.into(),
                    page_token: page_token.clone(),
                    ..Default::default()
                })
                .await?
                .into_inner();
            for job in page.jobs {
                let ours = job
                    .configuration
                    .as_ref()
                    .and_then(|c| c.labels.get(RUN_LABEL))
                    .is_some_and(|run| *run == self.run);
                if ours {
                    billed += job
                        .statistics
                        .and_then(|s| s.query)
                        .and_then(|q| q.total_bytes_billed)
                        .unwrap_or(0);
                }
            }
            if page.next_page_token.is_empty() {
                return Ok(billed);
            }
            page_token = page.next_page_token;
        }
    }
}

#[derive(Deserialize, Debug, PartialEq)]
struct Row<T> {
    i: i64,
    v: T,
}

#[derive(Deserialize, Debug)]
struct Scalars {
    min: i64,
    max: i64,
    nan: f64,
    inf: f64,
    neg_inf: f64,
    neg_zero: f64,
    tiny: f64,
    huge: f64,
    yes: bool,
    no: bool,
    numeric: String,
    bignumeric: String,
    date: jiff::civil::Date,
    time: jiff::civil::Time,
    datetime: jiff::civil::DateTime,
    timestamp: jiff::Timestamp,
    interval: BigQueryInterval,
    range: BigQueryRange<BigQueryDate>,
    array: Vec<String>,
    st: Pair,
    null: Option<i64>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Pair {
    a: i64,
    b: String,
}

async fn parse_back(live: &Live) -> Result<(), Box<dyn std::error::Error>> {
    let corpus = live_corpus();

    let strings: Vec<Row<String>> = live
        .rows(&numbered_rows(
            corpus.iter().map(|v| SqlLiteral::string(v).to_string()),
        ))
        .await?;
    assert_eq!(strings.len(), corpus.len());
    for (row, value) in strings.iter().zip(&corpus) {
        assert_eq!(&row.v, value, "STRING {}", row.i);
    }

    let mut blobs: Vec<Vec<u8>> = corpus.iter().map(|v| v.clone().into_bytes()).collect();
    blobs.push((0..=255).collect());
    let bytes: Vec<Row<Vec<u8>>> = live
        .rows(&numbered_rows(
            blobs.iter().map(|v| SqlLiteral::bytes(v).to_string()),
        ))
        .await?;
    assert_eq!(bytes.len(), blobs.len());
    for (row, value) in bytes.iter().zip(&blobs) {
        assert_eq!(&row.v, value, "BYTES {}", row.i);
    }

    #[derive(Serialize)]
    struct Doc<'a> {
        v: &'a str,
    }
    let json: Vec<Row<serde_json::Value>> = live
        .rows(&numbered_rows(
            corpus
                .iter()
                .map(|v| infer_literal(BigQueryJson(Doc { v })).to_string()),
        ))
        .await?;
    assert_eq!(json.len(), corpus.len());
    for (row, value) in json.iter().zip(&corpus) {
        assert_eq!(row.v["v"], value.as_str(), "JSON {}", row.i);
    }

    // A quoted identifier as a STRUCT field name, which TO_JSON_STRING writes back as a JSON
    // key.
    let names: Vec<&String> = corpus.iter().filter(|v| !v.is_empty()).collect();
    let keys: Vec<Row<String>> = live
        .rows(&numbered_rows(names.iter().map(|name| {
            format!("TO_JSON_STRING(STRUCT(1 AS {}))", quote_identifier(name))
        })))
        .await?;
    assert_eq!(keys.len(), names.len());
    for (row, name) in keys.iter().zip(&names) {
        let doc: serde_json::Map<String, serde_json::Value> = serde_json::from_str(&row.v)?;
        let key: Vec<&String> = doc.keys().collect();
        assert_eq!(key, [*name], "identifier {}", row.i);
    }

    let ts: jiff::Timestamp = "9999-12-30T12:34:56.999999Z".parse()?;
    let date = jiff::civil::date(1, 1, 1);
    let time = jiff::civil::time(23, 59, 59, 999_999_000);
    let interval = BigQueryInterval {
        months: -14,
        days: 3,
        nanos: 3_723_000_001_000,
    };
    let range = BigQueryRange {
        start: Some(BigQueryDate(jiff::civil::date(2024, 2, 29))),
        end: None,
    };
    let numeric = "-99999999999999999999999999999.999999999";
    let bignumeric = "0.00000000000000000000000000000000000001";
    let columns: Vec<(SqlLiteral, &str)> = vec![
        (SqlLiteral::int64(i64::MIN), "min"),
        (SqlLiteral::int64(i64::MAX), "max"),
        (SqlLiteral::float64(f64::NAN), "nan"),
        (SqlLiteral::float64(f64::INFINITY), "inf"),
        (SqlLiteral::float64(f64::NEG_INFINITY), "neg_inf"),
        (SqlLiteral::float64(-0.0), "neg_zero"),
        (SqlLiteral::float64(5e-324), "tiny"),
        (SqlLiteral::float64(f64::MAX), "huge"),
        (SqlLiteral::bool(true), "yes"),
        (SqlLiteral::bool(false), "no"),
        (infer_literal(BigQueryDecimal(numeric)), "numeric"),
        (infer_literal(BigQueryDecimal(bignumeric)), "bignumeric"),
        (infer_literal(BigQueryDate(date)), "date"),
        (infer_literal(crate::BigQueryTime(time)), "time"),
        (
            infer_literal(crate::BigQueryDateTime(date.to_datetime(time))),
            "datetime",
        ),
        (infer_literal(crate::BigQueryTimestamp(ts)), "timestamp"),
        (infer_literal(interval), "interval"),
        (infer_literal(range), "range"),
        (infer_literal(["'; DROP TABLE x; --", "\\'"]), "array"),
        (
            infer_literal(Pair {
                a: -1,
                b: "' OR '1'='1".into(),
            }),
            "st",
        ),
        (SqlLiteral::null(), "null"),
    ];
    let select: Vec<String> = columns
        .iter()
        .map(|(literal, name)| format!("{literal} AS {}", quote_identifier(name)))
        .collect();
    let scalars: Vec<Scalars> = live.rows(&format!("SELECT {}", select.join(", "))).await?;
    let [s] = &scalars[..] else {
        panic!("one row, got {scalars:?}");
    };
    assert_eq!((s.min, s.max), (i64::MIN, i64::MAX));
    assert!(s.nan.is_nan());
    assert_eq!((s.inf, s.neg_inf), (f64::INFINITY, f64::NEG_INFINITY));
    assert!(s.neg_zero == 0.0 && s.neg_zero.is_sign_negative());
    assert_eq!((s.tiny, s.huge), (5e-324, f64::MAX));
    assert_eq!((s.yes, s.no), (true, false));
    assert_eq!(s.numeric, numeric);
    assert_eq!(s.bignumeric, bignumeric);
    assert_eq!((s.date, s.time), (date, time));
    assert_eq!(s.datetime, date.to_datetime(time));
    assert_eq!(s.timestamp, ts);
    assert_eq!(s.interval, interval);
    assert_eq!(s.range, range);
    assert_eq!(s.array, ["'; DROP TABLE x; --", "\\'"]);
    assert_eq!(
        s.st,
        Pair {
            a: -1,
            b: "' OR '1'='1".into()
        }
    );
    assert_eq!(s.null, None);
    Ok(())
}

#[tokio::test]
async fn every_literal_kind_parses_back_to_its_value() -> Result<(), Box<dyn std::error::Error>> {
    let Ok(project) = std::env::var("GCP_PROJECT") else {
        eprintln!("GCP_PROJECT is not set, skipping the live test");
        return Ok(());
    };
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?;
    let live = Live {
        db: BigQueryDb::new(&project).await?,
        project,
        run: format!("bqp4c_{}_{}", now.as_secs(), now.subsec_nanos()),
        started_ms: u64::try_from(now.as_millis())?,
    };
    let result = parse_back(&live).await;
    match live.bytes_billed().await {
        Ok(billed) => eprintln!("LIVE bytes billed every_literal_kind_parses_back: {billed}"),
        Err(err) => {
            eprintln!("LIVE bytes billed every_literal_kind_parses_back: not reported ({err})")
        }
    }
    result
}
