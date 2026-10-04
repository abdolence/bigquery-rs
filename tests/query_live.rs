//! Live queries against BigQuery, on generated rows and a scratch table of a few rows. They run
//! only with `GCP_PROJECT` set.

use bigquery::*;
use futures::TryStreamExt;
use gcloud_sdk::google::cloud::bigquery::v2 as bq;
use serde::{Deserialize, Serialize};

#[path = "support/common.rs"]
mod common;
use common::{with_scratch, Scratch, TestResult, RUN_LABEL};

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Pair {
    a: i64,
    b: String,
}

#[derive(Deserialize, Debug, PartialEq)]
struct Echo {
    i: i64,
    f: f64,
    s: String,
    b: bool,
    bytes: Vec<u8>,
    num: String,
    big: String,
    d: jiff::civil::Date,
    t: jiff::civil::Time,
    dt: jiff::civil::DateTime,
    ts: jiff::Timestamp,
    ts_plain: jiff::Timestamp,
    geo: String,
    j: BigQueryJson<serde_json::Value>,
    iv: BigQueryInterval,
    r: BigQueryRange<BigQueryDate>,
    arr: Vec<i64>,
    st: Pair,
    null_i: Option<i64>,
    null_ts: Option<jiff::Timestamp>,
}

#[tokio::test]
async fn named_parameters_of_every_kind_round_trip() -> TestResult {
    with_scratch("named_parameters_of_every_kind_round_trip", async |l| {
        let ts: jiff::Timestamp = "2026-10-04T12:34:56.123456Z".parse()?;
        let d = jiff::civil::date(2024, 2, 29);
        let t = jiff::civil::time(23, 59, 59, 999_999_000);
        let iv = BigQueryInterval {
            months: 14,
            days: -3,
            nanos: 3_723_500_000_000,
        };
        let columns = [
            "i", "f", "s", "b", "bytes", "num", "big", "d", "t", "dt", "ts", "ts_plain", "geo",
            "j", "iv", "r", "arr", "st", "null_i", "null_ts",
        ];
        let sql = format!(
            "SELECT {}",
            columns
                .iter()
                .map(|c| format!("@{c} AS {c}"))
                .collect::<Vec<_>>()
                .join(", ")
        );
        let rows: Vec<Echo> =
            l.db.fluent()
                .query(sql)
                .label(RUN_LABEL, l.dataset.as_str())
                .param("i", -42)
                .param("f", 1.5)
                .param("s", "Åsa Öberg")
                .param("b", true)
                .param("bytes", serde_bytes::ByteBuf::from(vec![0u8, 255]))
                .param("num", BigQueryDecimal("123.450"))
                .param(
                    "big",
                    BigQueryDecimal("0.00000000000000000000000000000000000001"),
                )
                .param("d", BigQueryDate(d))
                .param("t", BigQueryTime(t))
                .param("dt", BigQueryDateTime(d.to_datetime(t)))
                .param("ts", BigQueryTimestamp(ts))
                .param_as("ts_plain", BigQueryFieldType::Timestamp, ts)
                .param_as("geo", BigQueryFieldType::Geography, "POINT(1 2)")
                .param("j", BigQueryJson(serde_json::json!({"stad": "Malmö"})))
                .param("iv", iv)
                .param(
                    "r",
                    BigQueryRange {
                        start: Some(BigQueryDate(d)),
                        end: None,
                    },
                )
                .param("arr", vec![1i64, 2, 3])
                .param(
                    "st",
                    Pair {
                        a: 7,
                        b: "z".into(),
                    },
                )
                .param_as("null_i", BigQueryFieldType::Int64, None::<i64>)
                .param_as(
                    "null_ts",
                    BigQueryFieldType::Timestamp,
                    None::<jiff::Timestamp>,
                )
                .obj::<Echo>()
                .query()
                .await?;
        assert_eq!(
            rows,
            [Echo {
                i: -42,
                f: 1.5,
                s: "Åsa Öberg".into(),
                b: true,
                bytes: vec![0, 255],
                num: "123.45".into(),
                big: "0.00000000000000000000000000000000000001".into(),
                d,
                t,
                dt: d.to_datetime(t),
                ts,
                ts_plain: ts,
                geo: "POINT(1 2)".into(),
                j: BigQueryJson(serde_json::json!({"stad": "Malmö"})),
                iv,
                r: BigQueryRange {
                    start: Some(BigQueryDate(d)),
                    end: None,
                },
                arr: vec![1, 2, 3],
                st: Pair {
                    a: 7,
                    b: "z".into()
                },
                null_i: None,
                null_ts: None,
            }]
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn result_over_the_inline_limit_is_read_through_storage_read() -> TestResult {
    with_scratch(
        "result_over_the_inline_limit_is_read_through_storage_read",
        async |l| {
            #[derive(Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord)]
            struct Row {
                x: i64,
                s: String,
            }
            let mut rows: Vec<Row> =
                l.db.fluent()
                    .query(
                        "SELECT x, CONCAT('r', CAST(x AS STRING)) AS s \
                     FROM UNNEST(GENERATE_ARRAY(1, 50)) AS x",
                    )
                    .label(RUN_LABEL, l.dataset.as_str())
                    .inline_rows_limit(10)
                    .obj::<Row>()
                    .query()
                    .await?;
            rows.sort();
            let expected: Vec<Row> = (1..=50)
                .map(|x| Row {
                    x,
                    s: format!("r{x}"),
                })
                .collect();
            assert_eq!(rows, expected);

            let batches: Vec<arrow_array::RecordBatch> =
                l.db.fluent()
                    .query("SELECT x FROM UNNEST(GENERATE_ARRAY(1, 30)) AS x")
                    .label(RUN_LABEL, l.dataset.as_str())
                    .inline_rows_limit(5)
                    .record_batches()
                    .await?
                    .try_collect()
                    .await?;
            assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 30);
            Ok(())
        },
    )
    .await
}

#[tokio::test]
async fn stats_report_what_a_query_job_used() -> TestResult {
    with_scratch("stats_report_what_a_query_job_used", async |l| {
        #[derive(Deserialize)]
        struct Row {
            x: i64,
        }
        // Generated rows read no table, so they process and bill 0 bytes; the cache is off
        // so that the job runs and uses slots.
        let sql = "SELECT x FROM UNNEST(GENERATE_ARRAY(1, 1000)) AS x";
        let (rows, inline): (Vec<Row>, _) =
            l.db.fluent()
                .query(sql)
                .label(RUN_LABEL, l.dataset.as_str())
                .use_query_cache(false)
                .obj::<Row>()
                .query_with_stats()
                .await?;
        eprintln!("LIVE stats inline: {inline:?}");
        assert_eq!(rows.iter().map(|r| r.x).sum::<i64>(), 500_500);
        assert_eq!(
            inline.job, None,
            "a short query runs without a job: {inline:?}"
        );
        assert!(inline.query_id.is_some(), "{inline:?}");
        assert_eq!(inline.statement_type, Some(BigQueryStatementType::Select));
        assert_eq!(inline.total_rows, Some(1000));
        assert_eq!(inline.total_bytes_processed, Some(0));
        assert_eq!(inline.total_bytes_billed, Some(0));
        assert_eq!(inline.cache_hit, Some(false));
        assert!(inline.total_slot_ms.is_some(), "{inline:?}");

        let (rows, read) =
            l.db.fluent()
                .query(sql)
                .label(RUN_LABEL, l.dataset.as_str())
                .use_query_cache(false)
                .inline_rows_limit(10)
                .obj::<Row>()
                .stream_query_with_stats()
                .await?;
        let rows: Vec<Row> = rows.try_collect().await?;
        eprintln!("LIVE stats storage read: {read:?}");
        assert_eq!(rows.iter().map(|r| r.x).sum::<i64>(), 500_500);
        assert!(
            read.job.is_some(),
            "a result over the inline limit has a job: {read:?}"
        );
        assert_eq!(read.statement_type, Some(BigQueryStatementType::Select));
        assert_eq!(read.total_rows, Some(1000));
        assert_eq!(read.total_bytes_processed, Some(0));
        assert_eq!(read.total_bytes_billed, Some(0));
        assert_eq!(read.cache_hit, Some(false));
        assert!(read.total_slot_ms.is_some(), "{read:?}");
        Ok(())
    })
    .await
}

/// `sql` with the scratch dataset as its default dataset, labelled with the run.
fn scratch_query<'a>(l: &'a Scratch, sql: &str) -> BigQueryQueryBuilder<'a, BigQueryDb> {
    l.db.fluent()
        .query(sql.to_string())
        .default_dataset(l.dataset.clone())
        .label(RUN_LABEL, l.dataset.as_str())
}

#[tokio::test]
async fn dml_counts_labels_and_dry_run_on_a_scratch_table() -> TestResult {
    with_scratch(
        "dml_counts_labels_and_dry_run_on_a_scratch_table",
        async |l| {
            let created = scratch_query(l, "CREATE TABLE t (id INT64, name STRING)")
                .execute()
                .await?;
            assert_eq!(
                created.statement_type,
                Some(BigQueryStatementType::CreateTable)
            );

            let inserted =
                scratch_query(l, "INSERT t (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c')")
                    .execute()
                    .await?;
            assert_eq!(inserted.statement_type, Some(BigQueryStatementType::Insert));
            assert_eq!(inserted.num_dml_affected_rows, Some(3));
            assert_eq!(
                inserted.dml_stats,
                Some(BigQueryDmlStats {
                    inserted: 3,
                    updated: 0,
                    deleted: 0
                })
            );

            let updated = scratch_query(l, "UPDATE t SET name = @name WHERE id <= @max")
                .param("name", "z")
                .param("max", 2)
                .execute()
                .await?;
            assert_eq!(updated.num_dml_affected_rows, Some(2));
            assert_eq!(updated.dml_stats.map(|s| s.updated), Some(2));

            let deleted = scratch_query(l, "DELETE t WHERE id = 3").execute().await?;
            assert_eq!(deleted.dml_stats.map(|s| s.deleted), Some(1));

            let job = deleted.job.ok_or("a DML statement runs as a job")?;
            let details =
                l.db.job_client()
                    .get_job(bq::GetJobRequest {
                        project_id: job.project_id.clone(),
                        job_id: job.job_id.to_string(),
                        location: job
                            .location
                            .as_ref()
                            .map(ToString::to_string)
                            .unwrap_or_default(),
                    })
                    .await
                    .map_err(errors::BigQueryError::from)?
                    .into_inner();
            let labels = details.configuration.map(|c| c.labels).unwrap_or_default();
            assert_eq!(
                labels.get(RUN_LABEL).map(String::as_str),
                Some(l.dataset.as_str())
            );

            let estimate = scratch_query(l, "SELECT id, name FROM t").dry_run().await?;
            assert!(
                estimate.total_bytes_processed.is_some_and(|b| b > 0),
                "{estimate:?}"
            );
            let columns: Vec<String> = estimate
                .schema
                .map(|s| s.fields.into_iter().map(|f| f.name).collect())
                .unwrap_or_default();
            assert_eq!(columns, ["id", "name"]);

            #[derive(Deserialize, Debug, PartialEq)]
            struct Row {
                id: i64,
                name: String,
            }
            let rows: Vec<Row> = scratch_query(l, "SELECT id, name FROM t ORDER BY id")
                .obj::<Row>()
                .query()
                .await?;
            assert_eq!(
                rows,
                [
                    Row {
                        id: 1,
                        name: "z".into()
                    },
                    Row {
                        id: 2,
                        name: "z".into()
                    }
                ]
            );
            Ok(())
        },
    )
    .await
}

#[tokio::test]
async fn injection_payloads_stay_data() -> TestResult {
    with_scratch("injection_payloads_stay_data", async |l| {
        scratch_query(l, "CREATE TABLE people (name STRING)")
            .execute()
            .await?;
        scratch_query(
            l,
            "INSERT people (name) VALUES ('Åsa'), ('Linnéa'), ('Olle')",
        )
        .execute()
        .await?;

        #[derive(Deserialize, Debug, PartialEq)]
        struct Echo {
            v: String,
        }
        #[derive(Deserialize, Debug, PartialEq)]
        struct Person {
            name: String,
        }
        #[derive(Deserialize, Debug, PartialEq)]
        struct Count {
            n: i64,
        }
        for payload in [
            "x'; DROP TABLE people; --",
            "' OR '1'='1",
            "Åsa' OR TRUE --",
            "\\'; DELETE people WHERE TRUE; --",
        ] {
            let echoed: Vec<Echo> = scratch_query(l, "SELECT @v AS v")
                .param("v", payload)
                .obj::<Echo>()
                .query()
                .await?;
            assert_eq!(echoed, [Echo { v: payload.into() }], "{payload:?}");
            let matched: Vec<Person> = scratch_query(l, "SELECT name FROM people WHERE name = @v")
                .param("v", payload)
                .obj::<Person>()
                .query()
                .await?;
            assert_eq!(matched, [], "{payload:?}");
        }
        let count: Vec<Count> = scratch_query(l, "SELECT COUNT(*) AS n FROM people")
            .obj::<Count>()
            .query()
            .await?;
        assert_eq!(count, [Count { n: 3 }], "the table is intact");
        Ok(())
    })
    .await
}
