//! Live queries against BigQuery, on generated rows and tables of a few rows in the CI dataset.
//! They run only with `GCP_PROJECT` set.

use bigquery::*;
use futures::TryStreamExt;
use gcloud_sdk::google::cloud::bigquery::v2 as bq;
use serde::{Deserialize, Serialize};

#[path = "support/common.rs"]
mod common;
use common::{with_scratch, Scratch, TestResult, CI_LOCATION, RUN_LABEL};

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Pair {
    number: i64,
    label: String,
}

#[derive(Deserialize, Debug, PartialEq)]
struct Echo {
    integer: i64,
    float: f64,
    text: String,
    flag: bool,
    bytes: Vec<u8>,
    numeric: String,
    bignumeric: String,
    date: jiff::civil::Date,
    time: jiff::civil::Time,
    datetime: jiff::civil::DateTime,
    timestamp: jiff::Timestamp,
    plain_timestamp: jiff::Timestamp,
    geography: String,
    document: BigQueryJson<serde_json::Value>,
    period: BigQueryInterval,
    date_range: BigQueryRange<BigQueryDate>,
    integers: Vec<i64>,
    pair: Pair,
    null_integer: Option<i64>,
    null_timestamp: Option<jiff::Timestamp>,
}

#[tokio::test]
async fn named_parameters_of_every_kind_round_trip() -> TestResult {
    with_scratch(
        "named_parameters_of_every_kind_round_trip",
        async |scratch| {
            let timestamp: jiff::Timestamp = "2026-10-04T12:34:56.123456Z".parse()?;
            let date = jiff::civil::date(2024, 2, 29);
            let time = jiff::civil::time(23, 59, 59, 999_999_000);
            let interval = BigQueryInterval {
                months: 14,
                days: -3,
                nanos: 3_723_500_000_000,
            };
            let columns = [
                "integer",
                "float",
                "text",
                "flag",
                "bytes",
                "numeric",
                "bignumeric",
                "date",
                "time",
                "datetime",
                "timestamp",
                "plain_timestamp",
                "geography",
                "document",
                "period",
                "date_range",
                "integers",
                "pair",
                "null_integer",
                "null_timestamp",
            ];
            let sql = format!(
                "SELECT {}",
                columns
                    .iter()
                    .map(|column| format!("@{column} AS {column}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            let rows: Vec<Echo> = scratch
                .db
                .fluent()
                .query(sql)
                .label(RUN_LABEL, scratch.run.as_str())
                .param("integer", -42)
                .param("float", 1.5)
                .param("text", "Åsa Öberg")
                .param("flag", true)
                .param("bytes", serde_bytes::ByteBuf::from(vec![0u8, 255]))
                .param("numeric", BigQueryDecimal("123.450"))
                .param(
                    "bignumeric",
                    BigQueryDecimal("0.00000000000000000000000000000000000001"),
                )
                .param("date", BigQueryDate(date))
                .param("time", BigQueryTime(time))
                .param("datetime", BigQueryDateTime(date.to_datetime(time)))
                .param("timestamp", BigQueryTimestamp(timestamp))
                .param_as("plain_timestamp", BigQueryFieldType::Timestamp, timestamp)
                .param_as("geography", BigQueryFieldType::Geography, "POINT(1 2)")
                .param(
                    "document",
                    BigQueryJson(serde_json::json!({"stad": "Malmö"})),
                )
                .param("period", interval)
                .param(
                    "date_range",
                    BigQueryRange {
                        start: Some(BigQueryDate(date)),
                        end: None,
                    },
                )
                .param("integers", vec![1i64, 2, 3])
                .param(
                    "pair",
                    Pair {
                        number: 7,
                        label: "z".into(),
                    },
                )
                .param_as("null_integer", BigQueryFieldType::Int64, None::<i64>)
                .param_as(
                    "null_timestamp",
                    BigQueryFieldType::Timestamp,
                    None::<jiff::Timestamp>,
                )
                .obj::<Echo>()
                .query()
                .await?;
            assert_eq!(
                rows,
                [Echo {
                    integer: -42,
                    float: 1.5,
                    text: "Åsa Öberg".into(),
                    flag: true,
                    bytes: vec![0, 255],
                    numeric: "123.45".into(),
                    bignumeric: "0.00000000000000000000000000000000000001".into(),
                    date,
                    time,
                    datetime: date.to_datetime(time),
                    timestamp,
                    plain_timestamp: timestamp,
                    geography: "POINT(1 2)".into(),
                    document: BigQueryJson(serde_json::json!({"stad": "Malmö"})),
                    period: interval,
                    date_range: BigQueryRange {
                        start: Some(BigQueryDate(date)),
                        end: None,
                    },
                    integers: vec![1, 2, 3],
                    pair: Pair {
                        number: 7,
                        label: "z".into()
                    },
                    null_integer: None,
                    null_timestamp: None,
                }]
            );
            Ok(())
        },
    )
    .await
}

#[tokio::test]
async fn result_over_the_inline_limit_is_read_through_storage_read() -> TestResult {
    with_scratch(
        "result_over_the_inline_limit_is_read_through_storage_read",
        async |scratch| {
            #[derive(Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord)]
            struct Row {
                number: i64,
                label: String,
            }
            let mut rows: Vec<Row> = scratch
                .db
                .fluent()
                .query(
                    "SELECT number, CONCAT('r', CAST(number AS STRING)) AS label \
                     FROM UNNEST(GENERATE_ARRAY(1, 50)) AS number",
                )
                .label(RUN_LABEL, scratch.run.as_str())
                .inline_rows_limit(10)
                .obj::<Row>()
                .query()
                .await?;
            rows.sort();
            let expected: Vec<Row> = (1..=50)
                .map(|number| Row {
                    number,
                    label: format!("r{number}"),
                })
                .collect();
            assert_eq!(rows, expected);

            let batches: Vec<arrow_array::RecordBatch> = scratch
                .db
                .fluent()
                .query("SELECT number FROM UNNEST(GENERATE_ARRAY(1, 30)) AS number")
                .label(RUN_LABEL, scratch.run.as_str())
                .inline_rows_limit(5)
                .record_batches()
                .await?
                .try_collect()
                .await?;
            assert_eq!(
                batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                30
            );
            Ok(())
        },
    )
    .await
}

#[tokio::test]
async fn stats_report_what_a_query_job_used() -> TestResult {
    with_scratch("stats_report_what_a_query_job_used", async |scratch| {
        #[derive(Deserialize)]
        struct Row {
            number: i64,
        }
        // Generated rows read no table, so they process and bill 0 bytes; the cache is off
        // so that the job runs and uses slots.
        let sql = "SELECT number FROM UNNEST(GENERATE_ARRAY(1, 1000)) AS number";
        let (rows, inline): (Vec<Row>, _) = scratch
            .db
            .fluent()
            .query(sql)
            .label(RUN_LABEL, scratch.run.as_str())
            .use_query_cache(false)
            .obj::<Row>()
            .query_with_stats()
            .await?;
        eprintln!("LIVE stats inline: {inline:?}");
        assert_eq!(rows.iter().map(|row| row.number).sum::<i64>(), 500_500);
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

        let (rows, read) = scratch
            .db
            .fluent()
            .query(sql)
            .label(RUN_LABEL, scratch.run.as_str())
            .use_query_cache(false)
            .inline_rows_limit(10)
            .obj::<Row>()
            .stream_query_with_stats()
            .await?;
        let rows: Vec<Row> = rows.try_collect().await?;
        eprintln!("LIVE stats storage read: {read:?}");
        assert_eq!(rows.iter().map(|row| row.number).sum::<i64>(), 500_500);
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

/// `sql` in the CI dataset's location, labelled with the run.
fn scratch_query(scratch: &Scratch, sql: String) -> BigQueryQueryBuilder<'_, BigQueryDb> {
    scratch
        .db
        .fluent()
        .query(sql)
        .location(CI_LOCATION)
        .label(RUN_LABEL, scratch.run.as_str())
}

#[tokio::test]
async fn dml_counts_labels_and_dry_run_on_a_scratch_table() -> TestResult {
    with_scratch(
        "dml_counts_labels_and_dry_run_on_a_scratch_table",
        async |scratch| {
            let table = scratch.table_sql("t");
            let created = scratch_query(
                scratch,
                format!("CREATE TABLE {table} (id INT64, name STRING)"),
            )
            .execute()
            .await?;
            assert_eq!(
                created.statement_type,
                Some(BigQueryStatementType::CreateTable)
            );

            let inserted = scratch_query(
                scratch,
                format!("INSERT {table} (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c')"),
            )
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

            let updated = scratch_query(
                scratch,
                format!("UPDATE {table} SET name = @name WHERE id <= @max"),
            )
            .param("name", "z")
            .param("max", 2)
            .execute()
            .await?;
            assert_eq!(updated.num_dml_affected_rows, Some(2));
            assert_eq!(updated.dml_stats.map(|stats| stats.updated), Some(2));

            let deleted = scratch_query(scratch, format!("DELETE {table} WHERE id = 3"))
                .execute()
                .await?;
            assert_eq!(deleted.dml_stats.map(|stats| stats.deleted), Some(1));

            let job = deleted.job.ok_or("a DML statement runs as a job")?;
            let details = scratch
                .db
                .job_client()
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
            let labels = details
                .configuration
                .map(|configuration| configuration.labels)
                .unwrap_or_default();
            assert_eq!(
                labels.get(RUN_LABEL).map(String::as_str),
                Some(scratch.run.as_str())
            );

            let estimate = scratch_query(scratch, format!("SELECT id, name FROM {table}"))
                .dry_run()
                .await?;
            assert!(
                estimate
                    .total_bytes_processed
                    .is_some_and(|bytes| bytes > 0),
                "{estimate:?}"
            );
            let columns: Vec<String> = estimate
                .schema
                .map(|schema| schema.fields.into_iter().map(|field| field.name).collect())
                .unwrap_or_default();
            assert_eq!(columns, ["id", "name"]);

            #[derive(Deserialize, Debug, PartialEq)]
            struct Row {
                id: i64,
                name: String,
            }
            let rows: Vec<Row> =
                scratch_query(scratch, format!("SELECT id, name FROM {table} ORDER BY id"))
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
    with_scratch("injection_payloads_stay_data", async |scratch| {
        let people = scratch.table_sql("people");
        scratch_query(scratch, format!("CREATE TABLE {people} (name STRING)"))
            .execute()
            .await?;
        scratch_query(
            scratch,
            format!("INSERT {people} (name) VALUES ('Åsa'), ('Linnéa'), ('Olle')"),
        )
        .execute()
        .await?;

        #[derive(Deserialize, Debug, PartialEq)]
        struct Echo {
            value: String,
        }
        #[derive(Deserialize, Debug, PartialEq)]
        struct Person {
            name: String,
        }
        #[derive(Deserialize, Debug, PartialEq)]
        struct Count {
            people: i64,
        }
        for payload in [
            "x'; DROP TABLE people; --",
            "' OR '1'='1",
            "Åsa' OR TRUE --",
            "\\'; DELETE people WHERE TRUE; --",
        ] {
            let echoed: Vec<Echo> = scratch_query(scratch, "SELECT @value AS value".to_string())
                .param("value", payload)
                .obj::<Echo>()
                .query()
                .await?;
            assert_eq!(
                echoed,
                [Echo {
                    value: payload.into()
                }],
                "{payload:?}"
            );
            let matched: Vec<Person> = scratch_query(
                scratch,
                format!("SELECT name FROM {people} WHERE name = @value"),
            )
            .param("value", payload)
            .obj::<Person>()
            .query()
            .await?;
            assert_eq!(matched, [], "{payload:?}");
        }
        let count: Vec<Count> =
            scratch_query(scratch, format!("SELECT COUNT(*) AS people FROM {people}"))
                .obj::<Count>()
                .query()
                .await?;
        assert_eq!(count, [Count { people: 3 }], "the table is intact");
        Ok(())
    })
    .await
}
