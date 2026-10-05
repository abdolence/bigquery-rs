use crate::db::fake::events::CapturedEvents;
use crate::db::fake::query::{
    done_job, encode_ipc, incomplete_response, inline_response, job_reference,
};
use crate::db::fake::read::FakeReadTable;
use crate::db::fake::spans::{bigquery_fields, CapturedSpans};
use crate::db::fake::{FakeBigQuery, FakeCall};
use crate::errors::{BigQueryCodecErrorKind, BigQueryError};
use crate::{
    BigQueryDatasetId, BigQueryDatasetRef, BigQueryDmlStats, BigQueryJobId, BigQueryJobRef,
    BigQueryJobStats, BigQueryLocation, BigQueryQueryId, BigQueryQueryOutcome, BigQueryRequestId,
    BigQueryResult, BigQueryStatementType,
};
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::{StreamExt, TryStreamExt};
use gcloud_sdk::google::cloud::bigquery::v2::{
    query_response, ArrowRecordBatch, DmlStats, ErrorProto, GetQueryResultsResponse,
    JobCancelResponse, JobStatistics, JobStatistics2, JobStatus, PostQueryRequest, QueryResponse,
    TableFieldSchema, TableSchema,
};
use gcloud_sdk::tonic::Code;
use serde::Deserialize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");

/// `id, name`, with `ids` as the ids and `Åsa <id>` as the names.
fn people(ids: &[i64]) -> RecordBatch {
    let names: Vec<Option<String>> = ids.iter().map(|id| Some(format!("Åsa {id}"))).collect();
    people_named(ids, names)
}

fn people_named(ids: &[i64], names: Vec<Option<String>>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef,
            Arc::new(StringArray::from(names)),
        ],
    )
    .expect("a valid batch")
}

#[derive(Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Person {
    id: i64,
    name: String,
}

fn person(id: i64) -> Person {
    Person {
        id,
        name: format!("Åsa {id}"),
    }
}

/// Answers the Storage Read calls with `table` as the destination table's content.
async fn storage_read(mut call: FakeCall, table: &FakeReadTable) {
    match call.method() {
        "GetTable" => call.get_table(table).await,
        "CreateReadSession" => {
            let request = call.session_request().await;
            call.open_session(&request, table);
        }
        "ReadRows" => {
            let request = call.read_rows_request().await;
            let index: usize = request.read_stream[1..].parse().expect("s<n>");
            let batches: Vec<(Vec<u8>, i64)> = table.streams[index]
                .iter()
                .map(|batch| (encode_ipc(batch).1, batch.num_rows() as i64))
                .collect();
            call.send_batches(&batches);
            call.finish();
        }
        other => panic!("unexpected call {other}"),
    }
}

async fn rows(fake: &FakeBigQuery, sql: &str) -> BigQueryResult<Vec<Person>> {
    let mut rows: Vec<Person> = tokio::time::timeout(
        Duration::from_secs(20),
        fake.db.fluent().query(sql).obj::<Person>().query(),
    )
    .await
    .expect("the query ends")?;
    rows.sort();
    Ok(rows)
}

#[tokio::test]
async fn complete_first_response_is_decoded_inline() -> BigQueryResult<()> {
    let fake = FakeBigQuery::start(|mut call: FakeCall| async move {
        call.query_request().await;
        call.reply(&inline_response(&people(&[1, 2]), 2));
    })
    .await;
    assert_eq!(rows(&fake, "SELECT 1").await?, [person(1), person(2)]);
    let batches: Vec<RecordBatch> = fake
        .db
        .fluent()
        .query("SELECT 2")
        .record_batches()
        .await?
        .try_collect()
        .await?;
    assert_eq!(batches, [people(&[1, 2])]);
    assert_eq!(fake.calls(), ["Query SELECT 1", "Query SELECT 2"]);
    Ok(())
}

#[tokio::test]
async fn empty_result_is_an_empty_stream() -> BigQueryResult<()> {
    let fake = FakeBigQuery::start(|mut call: FakeCall| async move {
        call.query_request().await;
        let mut response = inline_response(&people(&[]), 0);
        response.results = None;
        call.reply(&response);
    })
    .await;
    assert_eq!(rows(&fake, "SELECT 1 LIMIT 0").await?, []);
    assert_eq!(fake.calls(), ["Query SELECT 1 LIMIT 0"]);
    Ok(())
}

#[tokio::test]
async fn zero_rows_sent_as_an_empty_batch_message_are_an_empty_stream() -> BigQueryResult<()> {
    let fake = FakeBigQuery::start(|mut call: FakeCall| async move {
        call.query_request().await;
        let mut response = inline_response(&people(&[]), 0);
        response.results = Some(query_response::Results::ArrowRecordBatch(
            ArrowRecordBatch {
                serialized_record_batch: Vec::new(),
            },
        ));
        call.reply(&response);
    })
    .await;
    assert_eq!(rows(&fake, "SELECT 1 LIMIT 0").await?, []);
    assert_eq!(fake.calls(), ["Query SELECT 1 LIMIT 0"]);
    Ok(())
}

#[tokio::test]
async fn query_request_carries_the_routing_settings() -> BigQueryResult<()> {
    let fake = FakeBigQuery::start(|mut call: FakeCall| async move {
        let request = call.query_request().await;
        let sent = request.query_request.unwrap_or_default();
        call.log(format!(
            "format={:?} legacy={:?} int64_timestamp={:?} timeout_ms={:?} location={:?} \
             job_creation_mode={} dry_run={} request_id_len={}",
            sent.query_results_format(),
            sent.use_legacy_sql,
            sent.format_options
                .map(|options| options.use_int64_timestamp),
            sent.timeout_ms,
            sent.location,
            sent.job_creation_mode,
            sent.dry_run,
            sent.request_id.len(),
        ));
        call.reply(&inline_response(&people(&[1]), 1));
    })
    .await;
    rows(&fake, "SELECT 1").await?;
    assert_eq!(
        fake.calls(),
        [
            "Query SELECT 1",
            "format=Arrow legacy=Some(false) int64_timestamp=Some(true) timeout_ms=Some(10000) \
             location=\"\" job_creation_mode=2 dry_run=false request_id_len=32"
        ]
    );
    Ok(())
}

#[tokio::test]
async fn builder_settings_reach_the_query_request() -> BigQueryResult<()> {
    let fake = FakeBigQuery::start(|mut call: FakeCall| async move {
        let request = call.query_request().await;
        let sent = request.query_request.unwrap_or_default();
        let mut labels: Vec<_> = sent.labels.into_iter().collect();
        labels.sort();
        call.log(format!(
            "params={} mode={} location={} dataset={:?} labels={labels:?} max_bytes={:?} \
             cache={:?} timeout_ms={:?} job_timeout_ms={:?} request_id={} max_results={:?}",
            sent.query_parameters.len(),
            sent.parameter_mode,
            sent.location,
            sent.default_dataset
                .map(|dataset| format!("{}.{}", dataset.project_id, dataset.dataset_id)),
            sent.maximum_bytes_billed,
            sent.use_query_cache,
            sent.timeout_ms,
            sent.job_timeout_ms,
            sent.request_id,
            sent.max_results,
        ));
        call.reply(&inline_response(&people(&[1]), 1));
    })
    .await;
    fake.db
        .fluent()
        .query("SELECT @a")
        .param("a", 1)
        .location(BigQueryLocation::from_static("EU"))
        .default_dataset(BigQueryDatasetRef::new("other", SHOP)?)
        .label("team", "data")
        .maximum_bytes_billed(10)
        .use_query_cache(false)
        .timeout(Duration::from_millis(1500))
        .job_timeout(Duration::from_secs(60))
        .request_id(BigQueryRequestId::new("req-1")?)
        .inline_rows_limit(100)
        .obj::<Person>()
        .query()
        .await?;
    fake.db
        .fluent()
        .query("SELECT ?")
        .positional_param(1)
        .default_dataset(SHOP)
        .request_id(BigQueryRequestId::new("req-2")?)
        .execute()
        .await?;
    assert_eq!(
        fake.calls(),
        [
            "Query SELECT @a",
            "params=1 mode=NAMED location=EU dataset=Some(\"other.shop\") \
             labels=[(\"team\", \"data\")] max_bytes=Some(10) cache=Some(false) \
             timeout_ms=Some(1500) job_timeout_ms=Some(60000) request_id=req-1 \
             max_results=Some(100)",
            "Query SELECT ?",
            "params=1 mode=POSITIONAL location= dataset=Some(\"fake-project.shop\") labels=[] \
             max_bytes=None cache=None timeout_ms=Some(10000) job_timeout_ms=None \
             request_id=req-2 max_results=Some(0)",
        ]
    );
    Ok(())
}

#[tokio::test]
async fn mixed_parameter_modes_fail_before_sending() {
    let fake = FakeBigQuery::start(|call: FakeCall| async move {
        panic!("nothing is sent, got {}", call.method());
    })
    .await;
    let result = fake
        .db
        .fluent()
        .query("SELECT @a, ?")
        .param("a", 1)
        .positional_param(2)
        .execute()
        .await;
    assert!(
        matches!(result, Err(BigQueryError::InvalidParametersError(_))),
        "{result:?}"
    );
    assert!(fake.calls().is_empty());
}

#[tokio::test]
async fn retried_query_keeps_its_request_id_and_requires_a_job() -> BigQueryResult<()> {
    let attempts = Arc::new(AtomicUsize::new(0));
    let fake = FakeBigQuery::start(move |mut call: FakeCall| {
        let attempts = attempts.clone();
        async move {
            let request = call.query_request().await;
            let sent = request.query_request.unwrap_or_default();
            call.log(format!(
                "{} {:?}",
                sent.request_id,
                sent.job_creation_mode()
            ));
            if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                call.fail(Code::Unavailable, "backend went away");
            } else {
                call.reply(&inline_response(&people(&[1]), 1));
            }
        }
    })
    .await;
    rows(&fake, "SELECT 1").await?;
    rows(&fake, "SELECT 1").await?;
    let calls = fake.calls();
    let sent: Vec<(&str, &str)> = calls
        .iter()
        .skip(1)
        .step_by(2)
        .filter_map(|line| line.split_once(' '))
        .collect();
    assert_eq!(sent.len(), 3, "{calls:?}");
    assert_eq!(sent[0].0, sent[1].0, "a retry repeats the id");
    assert_ne!(
        sent[1].0, sent[2].0,
        "another terminal call sends another id"
    );
    assert_eq!(
        sent.iter().map(|(_, mode)| *mode).collect::<Vec<_>>(),
        [
            "JobCreationOptional",
            "JobCreationRequired",
            "JobCreationOptional"
        ],
        "a retry requires a job, so that BigQuery replays the first attempt's job"
    );
    Ok(())
}

#[tokio::test]
async fn page_token_reads_the_destination_table_through_storage_read() -> BigQueryResult<()> {
    let table = Arc::new(FakeReadTable::new(vec![vec![
        people(&[1, 2]),
        people(&[3]),
    ]]));
    let fake = FakeBigQuery::start(move |mut call: FakeCall| {
        let table = table.clone();
        async move {
            match call.method() {
                "Query" => {
                    call.query_request().await;
                    let mut response = inline_response(&people(&[1]), 3);
                    response.page_token = "page-2".into();
                    call.reply(&response);
                }
                "GetJob" => {
                    call.get_job_request().await;
                    call.reply(&done_job("_anon", "anon1"));
                }
                _ => storage_read(call, &table).await,
            }
        }
    })
    .await;
    assert_eq!(
        rows(&fake, "SELECT big").await?,
        [person(1), person(2), person(3)]
    );
    assert_eq!(
        fake.calls(),
        [
            "Query SELECT big",
            "GetJob job1 at US",
            "GetTable _anon.anon1",
            "CreateReadSession [id,name]",
            "ReadRows s0 at 0"
        ]
    );
    Ok(())
}

#[tokio::test]
async fn inline_result_short_of_total_rows_reads_the_destination_table() -> BigQueryResult<()> {
    let table = Arc::new(FakeReadTable::new(vec![vec![people(&[1, 2])]]));
    let fake = FakeBigQuery::start(move |mut call: FakeCall| {
        let table = table.clone();
        async move {
            match call.method() {
                "Query" => {
                    call.query_request().await;
                    call.reply(&inline_response(&people(&[1]), 2));
                }
                "GetJob" => {
                    call.get_job_request().await;
                    call.reply(&done_job("_anon", "anon1"));
                }
                _ => storage_read(call, &table).await,
            }
        }
    })
    .await;
    let batches: Vec<RecordBatch> = fake
        .db
        .fluent()
        .query("SELECT big")
        .record_batches()
        .await?
        .try_collect()
        .await?;
    assert_eq!(batches, [people(&[1, 2])]);
    assert_eq!(
        fake.calls(),
        [
            "Query SELECT big",
            "GetJob job1 at US",
            "CreateReadSession []",
            "ReadRows s0 at 0"
        ]
    );
    Ok(())
}

#[tokio::test]
async fn incomplete_job_is_polled_until_complete() -> BigQueryResult<()> {
    let table = Arc::new(FakeReadTable::new(vec![vec![people(&[1])]]));
    let polls = Arc::new(AtomicUsize::new(0));
    let fake = FakeBigQuery::start(move |mut call: FakeCall| {
        let (table, polls) = (table.clone(), polls.clone());
        async move {
            match call.method() {
                "Query" => {
                    call.query_request().await;
                    call.reply(&incomplete_response());
                }
                "GetQueryResults" => {
                    call.query_results_request().await;
                    let complete = polls.fetch_add(1, Ordering::SeqCst) > 0;
                    call.reply(&GetQueryResultsResponse {
                        job_reference: Some(job_reference()),
                        job_complete: Some(complete),
                        total_rows: complete.then_some(1),
                        ..Default::default()
                    });
                }
                "GetJob" => {
                    call.get_job_request().await;
                    call.reply(&done_job("_anon", "anon1"));
                }
                _ => storage_read(call, &table).await,
            }
        }
    })
    .await;
    assert_eq!(rows(&fake, "SELECT slow").await?, [person(1)]);
    assert_eq!(
        fake.calls(),
        [
            "Query SELECT slow",
            "GetQueryResults job1 at US max_results=Some(0)",
            "GetQueryResults job1 at US max_results=Some(0)",
            "GetJob job1 at US",
            "GetTable _anon.anon1",
            "CreateReadSession [id,name]",
            "ReadRows s0 at 0"
        ]
    );
    Ok(())
}

#[tokio::test]
async fn dml_reports_counts_and_has_no_rows() -> BigQueryResult<()> {
    let fake = FakeBigQuery::start(|mut call: FakeCall| async move {
        call.query_request().await;
        call.reply(&QueryResponse {
            job_reference: Some(job_reference()),
            job_complete: Some(true),
            statement_type: "UPDATE".into(),
            num_dml_affected_rows: Some(2),
            dml_stats: Some(DmlStats {
                updated_row_count: Some(2),
                ..Default::default()
            }),
            total_bytes_processed: Some(33),
            total_bytes_billed: Some(10_485_760),
            total_slot_ms: Some(12),
            cache_hit: Some(false),
            ..Default::default()
        });
    })
    .await;
    let outcome = fake.db.fluent().query("UPDATE t").execute().await?;
    assert_eq!(
        outcome,
        BigQueryQueryOutcome {
            query_id: None,
            job: Some(BigQueryJobRef {
                project_id: "fake-project".into(),
                job_id: BigQueryJobId::new("job1").expect("a job ID"),
                location: Some(BigQueryLocation::from_static("US")),
            }),
            statement_type: Some(BigQueryStatementType::Update),
            num_dml_affected_rows: Some(2),
            dml_stats: Some(BigQueryDmlStats {
                inserted: 0,
                updated: 2,
                deleted: 0,
            }),
            total_rows: None,
            total_bytes_processed: Some(33),
            total_bytes_billed: Some(10_485_760),
            total_slot_ms: Some(12),
            cache_hit: Some(false),
        }
    );
    assert_eq!(rows(&fake, "UPDATE t").await?, []);
    assert_eq!(fake.calls(), ["Query UPDATE t", "Query UPDATE t"]);
    Ok(())
}

#[tokio::test]
async fn polled_dml_reports_counts_from_the_job() -> BigQueryResult<()> {
    let fake = FakeBigQuery::start(|mut call: FakeCall| async move {
        match call.method() {
            "Query" => {
                call.query_request().await;
                call.reply(&incomplete_response());
            }
            "GetQueryResults" => {
                call.query_results_request().await;
                call.reply(&GetQueryResultsResponse {
                    job_reference: Some(job_reference()),
                    job_complete: Some(true),
                    num_dml_affected_rows: Some(3),
                    total_bytes_processed: Some(0),
                    cache_hit: Some(false),
                    ..Default::default()
                });
            }
            "GetJob" => {
                call.get_job_request().await;
                let mut job = done_job("shop", "orders");
                job.statistics = Some(JobStatistics {
                    query: Some(JobStatistics2 {
                        statement_type: "INSERT".into(),
                        num_dml_affected_rows: Some(3),
                        dml_stats: Some(DmlStats {
                            inserted_row_count: Some(3),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                });
                call.reply(&job);
            }
            other => panic!("unexpected call {other}"),
        }
    })
    .await;
    let outcome = fake.db.fluent().query("INSERT t").execute().await?;
    assert_eq!(outcome.statement_type, Some(BigQueryStatementType::Insert));
    assert_eq!(outcome.num_dml_affected_rows, Some(3));
    assert_eq!(
        outcome.dml_stats,
        Some(BigQueryDmlStats {
            inserted: 3,
            updated: 0,
            deleted: 0
        })
    );
    assert_eq!(
        fake.calls(),
        [
            "Query INSERT t",
            "GetQueryResults job1 at US max_results=Some(0)",
            "GetJob job1 at US"
        ]
    );
    Ok(())
}

#[tokio::test]
async fn required_job_creation_reaches_the_query_request() -> BigQueryResult<()> {
    let fake = FakeBigQuery::start(|mut call: FakeCall| async move {
        let request = call.query_request().await;
        let sent = request.query_request.unwrap_or_default();
        call.log(format!("job_creation_mode={:?}", sent.job_creation_mode()));
        call.reply(&inline_response(&people(&[1]), 1));
    })
    .await;
    fake.db
        .fluent()
        .query("SELECT 1")
        .job_creation_required()
        .obj::<Person>()
        .query()
        .await?;
    assert_eq!(
        fake.calls(),
        ["Query SELECT 1", "job_creation_mode=JobCreationRequired"]
    );
    Ok(())
}

/// A short query's response: the whole result inline, a query ID, and no job.
fn job_less_response(batch: &RecordBatch, total_rows: u64) -> QueryResponse {
    QueryResponse {
        job_reference: None,
        query_id: "query-1".into(),
        location: "US".into(),
        ..inline_response(batch, total_rows)
    }
}

#[tokio::test]
async fn job_less_result_is_decoded_inline_with_its_query_id() -> BigQueryResult<()> {
    let (spans, _guard) = CapturedSpans::capture();
    let fake = FakeBigQuery::start(|mut call: FakeCall| async move {
        call.query_request().await;
        let mut response = job_less_response(&people(&[1, 2]), 2);
        response.total_bytes_processed = Some(0);
        response.total_slot_ms = Some(3);
        call.reply(&response);
    })
    .await;
    let (rows, stats) = fake
        .db
        .fluent()
        .query("SELECT 1")
        .obj::<Person>()
        .query_with_stats()
        .await?;
    assert_eq!(rows, [person(1), person(2)]);
    assert_eq!(stats.job, None);
    assert_eq!(
        stats.query_id.as_ref().map(BigQueryQueryId::as_str),
        Some("query-1")
    );
    assert_eq!(stats.total_slot_ms, Some(3));
    assert_eq!(fake.calls(), ["Query SELECT 1"]);
    assert_eq!(
        spans.only("BigQuery Query"),
        bigquery_fields(&[
            ("sql_len", "8"),
            ("query_id", "query-1"),
            ("location", "US"),
            ("statement_type", "SELECT"),
            ("bytes_processed", "0"),
            ("slot_ms", "3"),
            ("total_rows", "2"),
            ("route", "inline"),
        ])
    );
    Ok(())
}

#[tokio::test]
async fn query_given_a_job_anyway_waits_for_it_and_reads_its_table() -> BigQueryResult<()> {
    let table = Arc::new(FakeReadTable::new(vec![vec![people(&[1])]]));
    let fake = FakeBigQuery::start(move |mut call: FakeCall| {
        let table = table.clone();
        async move {
            match call.method() {
                "Query" => {
                    call.query_request().await;
                    call.reply(&QueryResponse {
                        query_id: "query-1".into(),
                        ..incomplete_response()
                    });
                }
                "GetQueryResults" => {
                    call.query_results_request().await;
                    call.reply(&GetQueryResultsResponse {
                        job_reference: Some(job_reference()),
                        job_complete: Some(true),
                        total_rows: Some(1),
                        ..Default::default()
                    });
                }
                "GetJob" => {
                    call.get_job_request().await;
                    call.reply(&done_job("_anon", "anon1"));
                }
                _ => storage_read(call, &table).await,
            }
        }
    })
    .await;
    let (rows, stats) = fake
        .db
        .fluent()
        .query("SELECT slow")
        .obj::<Person>()
        .query_with_stats()
        .await?;
    assert_eq!(rows, [person(1)]);
    assert_eq!(stats.job, Some(job_reference().into()));
    assert_eq!(
        stats.query_id.as_ref().map(BigQueryQueryId::as_str),
        Some("query-1")
    );
    assert_eq!(
        fake.calls(),
        [
            "Query SELECT slow",
            "GetQueryResults job1 at US max_results=Some(0)",
            "GetJob job1 at US",
            "GetTable _anon.anon1",
            "CreateReadSession [id,name]",
            "ReadRows s0 at 0"
        ]
    );
    Ok(())
}

#[tokio::test]
async fn job_less_dml_reports_its_counts_and_query_id() -> BigQueryResult<()> {
    let fake = FakeBigQuery::start(|mut call: FakeCall| async move {
        let request = call.query_request().await;
        let sent = request.query_request.unwrap_or_default();
        call.log(format!("job_creation_mode={:?}", sent.job_creation_mode()));
        call.reply(&QueryResponse {
            query_id: "query-1".into(),
            job_complete: Some(true),
            statement_type: "DELETE".into(),
            num_dml_affected_rows: Some(1),
            dml_stats: Some(DmlStats {
                deleted_row_count: Some(1),
                ..Default::default()
            }),
            ..Default::default()
        });
    })
    .await;
    let outcome = fake.db.fluent().query("DELETE t").execute().await?;
    assert_eq!(outcome.job, None);
    assert_eq!(
        outcome.query_id.as_ref().map(BigQueryQueryId::as_str),
        Some("query-1")
    );
    assert_eq!(outcome.num_dml_affected_rows, Some(1));
    assert_eq!(outcome.dml_stats.map(|stats| stats.deleted), Some(1));
    assert_eq!(
        fake.calls(),
        ["Query DELETE t", "job_creation_mode=JobCreationOptional"]
    );
    Ok(())
}

#[tokio::test]
async fn inline_result_records_the_response_figures() -> BigQueryResult<()> {
    let (spans, _guard) = CapturedSpans::capture();
    let fake = FakeBigQuery::start(|mut call: FakeCall| async move {
        call.query_request().await;
        let mut response = inline_response(&people(&[1, 2]), 2);
        response.location = "US".into();
        response.total_bytes_processed = Some(100);
        response.total_bytes_billed = Some(10_485_760);
        response.total_slot_ms = Some(7);
        response.cache_hit = Some(false);
        call.reply(&response);
    })
    .await;
    let (rows, stats) = fake
        .db
        .fluent()
        .query("SELECT 1")
        .obj::<Person>()
        .query_with_stats()
        .await?;
    assert_eq!(rows, [person(1), person(2)]);
    assert_eq!(
        stats,
        BigQueryJobStats {
            job: Some(job_reference().into()),
            query_id: None,
            statement_type: Some(BigQueryStatementType::Select),
            total_rows: Some(2),
            total_bytes_processed: Some(100),
            total_bytes_billed: Some(10_485_760),
            total_slot_ms: Some(7),
            cache_hit: Some(false),
            num_dml_affected_rows: None,
            dml_stats: None,
        }
    );
    assert_eq!(
        spans.only("BigQuery Query"),
        bigquery_fields(&[
            ("sql_len", "8"),
            ("job_id", "job1"),
            ("location", "US"),
            ("statement_type", "SELECT"),
            ("bytes_processed", "100"),
            ("bytes_billed", "10485760"),
            ("slot_ms", "7"),
            ("cache_hit", "false"),
            ("total_rows", "2"),
            ("route", "inline"),
        ])
    );
    Ok(())
}

#[tokio::test]
async fn storage_read_result_takes_the_figures_of_its_job() -> BigQueryResult<()> {
    let (spans, _guard) = CapturedSpans::capture();
    let table = Arc::new(FakeReadTable::new(vec![vec![people(&[1, 2, 3])]]));
    let fake = FakeBigQuery::start(move |mut call: FakeCall| {
        let table = table.clone();
        async move {
            match call.method() {
                "Query" => {
                    call.query_request().await;
                    let mut response = inline_response(&people(&[1]), 3);
                    response.page_token = "page-2".into();
                    response.total_bytes_processed = Some(500);
                    call.reply(&response);
                }
                "GetJob" => {
                    call.get_job_request().await;
                    let mut job = done_job("_anon", "anon1");
                    job.statistics = Some(JobStatistics {
                        total_bytes_processed: Some(500),
                        total_slot_ms: Some(40),
                        query: Some(JobStatistics2 {
                            statement_type: "SELECT".into(),
                            total_bytes_processed: Some(500),
                            total_bytes_billed: Some(10_485_760),
                            total_slot_ms: Some(40),
                            cache_hit: Some(false),
                            ..Default::default()
                        }),
                        ..Default::default()
                    });
                    call.reply(&job);
                }
                _ => storage_read(call, &table).await,
            }
        }
    })
    .await;
    let (rows, stats) = fake
        .db
        .fluent()
        .query("SELECT big")
        .obj::<Person>()
        .stream_query_with_stats()
        .await?;
    assert_eq!(
        stats,
        BigQueryJobStats {
            job: Some(job_reference().into()),
            query_id: None,
            statement_type: Some(BigQueryStatementType::Select),
            total_rows: Some(3),
            total_bytes_processed: Some(500),
            total_bytes_billed: Some(10_485_760),
            total_slot_ms: Some(40),
            cache_hit: Some(false),
            num_dml_affected_rows: None,
            dml_stats: None,
        }
    );
    let mut rows: Vec<Person> = tokio::time::timeout(Duration::from_secs(20), rows.try_collect())
        .await
        .expect("the rows end")?;
    rows.sort();
    assert_eq!(rows, [person(1), person(2), person(3)]);
    assert_eq!(
        spans.only("BigQuery Query"),
        bigquery_fields(&[
            ("sql_len", "10"),
            ("job_id", "job1"),
            ("location", "US"),
            ("statement_type", "SELECT"),
            ("bytes_processed", "500"),
            ("bytes_billed", "10485760"),
            ("slot_ms", "40"),
            ("cache_hit", "false"),
            ("total_rows", "3"),
            ("route", "storage_read"),
        ])
    );
    Ok(())
}

#[tokio::test]
async fn polled_statement_records_the_figures_of_its_results_and_job() -> BigQueryResult<()> {
    let (spans, _guard) = CapturedSpans::capture();
    let fake = FakeBigQuery::start(|mut call: FakeCall| async move {
        match call.method() {
            "Query" => {
                call.query_request().await;
                call.reply(&incomplete_response());
            }
            "GetQueryResults" => {
                call.query_results_request().await;
                call.reply(&GetQueryResultsResponse {
                    job_reference: Some(job_reference()),
                    job_complete: Some(true),
                    num_dml_affected_rows: Some(3),
                    total_bytes_processed: Some(77),
                    cache_hit: Some(false),
                    ..Default::default()
                });
            }
            "GetJob" => {
                call.get_job_request().await;
                let mut job = done_job("shop", "orders");
                job.configuration = None;
                job.statistics = Some(JobStatistics {
                    total_slot_ms: Some(9),
                    query: Some(JobStatistics2 {
                        statement_type: "INSERT".into(),
                        total_bytes_billed: Some(10_485_760),
                        total_slot_ms: Some(9),
                        num_dml_affected_rows: Some(3),
                        dml_stats: Some(DmlStats {
                            inserted_row_count: Some(3),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                });
                call.reply(&job);
            }
            other => panic!("unexpected call {other}"),
        }
    })
    .await;
    let (rows, stats) = fake
        .db
        .fluent()
        .query("INSERT t")
        .obj::<Person>()
        .query_with_stats()
        .await?;
    assert_eq!(rows, []);
    assert_eq!(
        stats,
        BigQueryJobStats {
            job: Some(job_reference().into()),
            query_id: None,
            statement_type: Some(BigQueryStatementType::Insert),
            total_rows: None,
            total_bytes_processed: Some(77),
            total_bytes_billed: Some(10_485_760),
            total_slot_ms: Some(9),
            cache_hit: Some(false),
            num_dml_affected_rows: Some(3),
            dml_stats: Some(BigQueryDmlStats {
                inserted: 3,
                updated: 0,
                deleted: 0,
            }),
        }
    );
    assert_eq!(
        spans.only("BigQuery Query"),
        bigquery_fields(&[
            ("sql_len", "8"),
            ("job_id", "job1"),
            ("location", "US"),
            ("statement_type", "INSERT"),
            ("bytes_processed", "77"),
            ("bytes_billed", "10485760"),
            ("slot_ms", "9"),
            ("cache_hit", "false"),
            ("dml_rows", "3"),
            ("route", "none"),
        ])
    );
    Ok(())
}

#[tokio::test]
async fn failed_query_is_the_status_of_the_query_call() {
    let fake = FakeBigQuery::start(|mut call: FakeCall| async move {
        call.query_request().await;
        call.fail(
            Code::InvalidArgument,
            "Syntax error: Unexpected identifier \"SELEC\" at [1:1]",
        );
    })
    .await;
    match fake.db.fluent().query("SELEC 1").execute().await {
        Err(BigQueryError::DatabaseError(err)) => {
            assert!(err.details.contains("SELEC"), "{err}");
            assert!(!err.retry_possible);
        }
        other => panic!("expected a database error, got {other:?}"),
    }
    assert_eq!(fake.calls(), ["Query SELEC 1"]);
}

#[tokio::test]
async fn job_error_result_is_a_job_error() {
    let fake = FakeBigQuery::start(|mut call: FakeCall| async move {
        match call.method() {
            "Query" => {
                call.query_request().await;
                call.reply(&incomplete_response());
            }
            "GetQueryResults" => {
                call.query_results_request().await;
                call.reply(&GetQueryResultsResponse {
                    job_complete: Some(true),
                    ..Default::default()
                });
            }
            "GetJob" => {
                call.get_job_request().await;
                let failure = ErrorProto {
                    reason: "invalidQuery".into(),
                    location: "query".into(),
                    message: "boom".into(),
                    ..Default::default()
                };
                let mut job = done_job("shop", "orders");
                job.status = Some(JobStatus {
                    state: "DONE".into(),
                    error_result: Some(failure.clone()),
                    errors: vec![failure],
                });
                call.reply(&job);
            }
            other => panic!("unexpected call {other}"),
        }
    })
    .await;
    match fake
        .db
        .fluent()
        .query("SELECT ERROR('boom')")
        .execute()
        .await
    {
        Err(BigQueryError::JobError(err)) => {
            assert_eq!(err.public.code, "invalidQuery");
            assert_eq!(
                err.job.map(|job| job.job_id),
                Some(BigQueryJobId::new("job1").expect("a job ID"))
            );
            assert!(err.details.contains("boom"), "{}", err.details);
            assert_eq!(err.errors.len(), 1);
            assert_eq!(err.errors[0].reason, "invalidQuery");
        }
        other => panic!("expected a job error, got {other:?}"),
    }
}

#[tokio::test]
async fn dry_run_reports_bytes_and_schema() -> BigQueryResult<()> {
    let fake = FakeBigQuery::start(|mut call: FakeCall| async move {
        let request = call.query_request().await;
        let dry_run = request
            .query_request
            .map(|sent| sent.dry_run)
            .unwrap_or_default();
        call.log(format!("dry_run={dry_run}"));
        call.reply(&QueryResponse {
            total_bytes_processed: Some(1234),
            schema: Some(TableSchema {
                fields: vec![TableFieldSchema {
                    name: "n".into(),
                    r#type: "INTEGER".into(),
                    mode: "NULLABLE".into(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        });
    })
    .await;
    let result = fake.db.fluent().query("SELECT n FROM t").dry_run().await?;
    assert_eq!(result.total_bytes_processed, Some(1234));
    let fields: Vec<(String, String)> = result
        .schema
        .map(|schema| {
            schema
                .fields
                .into_iter()
                .map(|field| (field.name, field.field_type.to_string()))
                .collect()
        })
        .unwrap_or_default();
    assert_eq!(fields, [("n".to_string(), "INT64".to_string())]);
    assert_eq!(fake.calls(), ["Query SELECT n FROM t", "dry_run=true"]);
    Ok(())
}

#[tokio::test]
async fn cancel_job_sends_the_job_and_its_location() -> BigQueryResult<()> {
    let fake = FakeBigQuery::start(|mut call: FakeCall| async move {
        call.cancel_job_request().await;
        call.reply(&JobCancelResponse::default());
    })
    .await;
    fake.db
        .cancel_job(&BigQueryJobRef {
            project_id: "fake-project".into(),
            job_id: BigQueryJobId::new("job1").expect("a job ID"),
            location: Some(BigQueryLocation::from_static("EU")),
        })
        .await?;
    assert_eq!(fake.calls(), ["CancelJob job1 at EU"]);
    Ok(())
}

#[tokio::test]
async fn base_variant_skips_rows_that_fail_to_decode() -> BigQueryResult<()> {
    let fake = FakeBigQuery::start(|mut call: FakeCall| async move {
        call.query_request().await;
        let batch = people_named(&[1, 2], vec![None, Some("Åsa 2".into())]);
        call.reply(&inline_response(&batch, 2));
    })
    .await;
    let skipped: Vec<Person> = fake
        .db
        .fluent()
        .query("SELECT 1")
        .obj::<Person>()
        .stream_query()
        .await?
        .collect()
        .await;
    assert_eq!(skipped, [person(2)]);
    let with_errors: Vec<BigQueryResult<Person>> = fake
        .db
        .fluent()
        .query("SELECT 1")
        .obj::<Person>()
        .stream_query_with_errors()
        .await?
        .collect()
        .await;
    match &with_errors[..] {
        [Err(BigQueryError::DeserializeError(err)), Ok(second)] => {
            assert_eq!(err.kind, BigQueryCodecErrorKind::NullForNonOption);
            assert_eq!(err.row, Some(0));
            assert_eq!(second, &person(2));
        }
        other => panic!("expected a row error then a row, got {other:?}"),
    }
    Ok(())
}

#[tokio::test]
async fn invalid_parameter_name_fails_before_sending() {
    let fake = FakeBigQuery::start(|call: FakeCall| async move {
        panic!("nothing is sent, got {}", call.method());
    })
    .await;
    for name in ["", "a b", "x; DROP TABLE t; --", "`v`", "v'"] {
        let result = fake
            .db
            .fluent()
            .query("SELECT 1")
            .param(name, 1)
            .execute()
            .await;
        assert!(
            matches!(result, Err(BigQueryError::InvalidParametersError(_))),
            "{name:?}: {result:?}"
        );
    }
    assert!(fake.calls().is_empty());
}

#[derive(serde::Serialize)]
struct Holder<'a> {
    text: &'a str,
}

#[derive(serde::Serialize)]
struct AllForms<'a> {
    text: &'a str,
    texts: Vec<&'a str>,
    record: Holder<'a>,
    document: crate::BigQueryJson<Holder<'a>>,
}

/// The payload as each of the four parameters carries it: the STRING text, the ARRAY
/// element, the STRUCT field and the JSON document's field.
fn carried_values(request: &PostQueryRequest) -> Vec<String> {
    let sent = request.query_request.clone().unwrap_or_default();
    let value = |index: usize| {
        sent.query_parameters[index]
            .parameter_value
            .clone()
            .unwrap_or_default()
    };
    let json: serde_json::Value =
        serde_json::from_str(&value(3).value.unwrap_or_default()).expect("JSON text");
    vec![
        value(0).value.unwrap_or_default(),
        value(1).array_values[0].value.clone().unwrap_or_default(),
        value(2).struct_values["text"]
            .value
            .clone()
            .unwrap_or_default(),
        json["text"].as_str().unwrap_or_default().to_string(),
    ]
}

#[tokio::test]
async fn sql_text_is_sent_as_given_and_values_only_as_parameters() -> BigQueryResult<()> {
    use crate::{BigQueryFieldMode, BigQueryFieldSchema, BigQueryFieldType, BigQueryParamType};
    let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen = requests.clone();
    let fake = FakeBigQuery::start(move |mut call: FakeCall| {
        let seen = seen.clone();
        async move {
            let request: PostQueryRequest = call.next_request().await.expect("a Query request");
            seen.lock().expect("not poisoned").push(request);
            call.reply(&QueryResponse {
                job_reference: Some(job_reference()),
                job_complete: Some(true),
                ..Default::default()
            });
        }
    })
    .await;
    let holder = BigQueryFieldType::Struct(vec![BigQueryFieldSchema {
        name: "text".into(),
        field_type: BigQueryFieldType::String { max_length: None },
        mode: BigQueryFieldMode::Nullable,
        description: None,
        default_value_expression: None,
    }]);
    let named = "SELECT @text, @texts, @record.text, JSON_VALUE(@document, '$.text') -- @text ?";
    let positional = "SELECT ?, ?, ?.text, JSON_VALUE(?, '$.text') /* ? */";
    let corpus = crate::sql::tests::injection_corpus();
    for payload in &corpus {
        let payload = payload.as_str();
        let all = AllForms {
            text: payload,
            texts: vec![payload],
            record: Holder { text: payload },
            document: crate::BigQueryJson(Holder { text: payload }),
        };
        let forms = [
            (
                "param",
                named,
                fake.db
                    .fluent()
                    .query(named)
                    .param("text", all.text)
                    .param("texts", &all.texts)
                    .param("record", &all.record)
                    .param("document", &all.document),
            ),
            (
                "param_as",
                named,
                fake.db
                    .fluent()
                    .query(named)
                    .param_as(
                        "text",
                        BigQueryFieldType::String { max_length: None },
                        all.text,
                    )
                    .param_as(
                        "texts",
                        BigQueryParamType::array_of(BigQueryFieldType::String { max_length: None }),
                        &all.texts,
                    )
                    .param_as("record", holder.clone(), &all.record)
                    .param_as("document", BigQueryFieldType::Json, &all.document),
            ),
            ("params", named, fake.db.fluent().query(named).params(&all)),
            (
                "positional_param",
                positional,
                fake.db
                    .fluent()
                    .query(positional)
                    .positional_param(all.text)
                    .positional_param(&all.texts)
                    .positional_param(&all.record)
                    .positional_param(&all.document),
            ),
            (
                "positional_param_as",
                positional,
                fake.db
                    .fluent()
                    .query(positional)
                    .positional_param_as(BigQueryFieldType::String { max_length: None }, all.text)
                    .positional_param_as(
                        BigQueryParamType::array_of(BigQueryFieldType::String { max_length: None }),
                        &all.texts,
                    )
                    .positional_param_as(holder.clone(), &all.record)
                    .positional_param_as(BigQueryFieldType::Json, &all.document),
            ),
        ];
        for (form, sql, builder) in forms {
            builder.execute().await?;
            let request = requests
                .lock()
                .expect("not poisoned")
                .pop()
                .expect("one request per call");
            let what = format!("{form} with {:?}", &payload[..payload.len().min(40)]);
            let sent = request.query_request.clone().unwrap_or_default();
            assert_eq!(
                sent.query.as_bytes(),
                sql.as_bytes(),
                "{what}: the SQL text"
            );
            assert_eq!(sent.query_parameters.len(), 4, "{what}");
            assert_eq!(carried_values(&request), [payload; 4], "{what}: the values");
        }
    }
    Ok(())
}

#[tokio::test]
async fn a_skipped_row_log_line_names_the_row_and_field_and_not_the_cell() -> BigQueryResult<()> {
    #[derive(Deserialize, Debug)]
    enum Plan {
        Basic,
    }
    #[derive(Deserialize, Debug)]
    struct Subscription {
        #[allow(dead_code, reason = "the row only has to decode")]
        name: Plan,
    }
    const CELL: &str = "s3cr3t-cell";
    let fake = FakeBigQuery::start(|mut call: FakeCall| async move {
        call.query_request().await;
        let names = vec![Some("Basic".to_string()), Some(CELL.to_string())];
        call.reply(&inline_response(&people_named(&[1, 2], names), 2));
    })
    .await;

    let mut with_errors = fake
        .db
        .fluent()
        .query("SELECT 1")
        .obj::<Subscription>()
        .stream_query_with_errors()
        .await?;
    assert!(with_errors.next().await.expect("the first row").is_ok());
    let err = match with_errors.next().await.expect("the second row") {
        Err(BigQueryError::DeserializeError(err)) => err,
        other => panic!("expected a DeserializeError, got {other:?}"),
    };
    assert!(err.message.contains(CELL), "{err}");

    let (events, _guard) = CapturedEvents::capture();
    let rows: Vec<Subscription> = fake
        .db
        .fluent()
        .query("SELECT 1")
        .obj::<Subscription>()
        .stream_query()
        .await?
        .collect()
        .await;
    assert_eq!(rows.len(), 1);
    let logged = events.at(tracing::Level::ERROR);
    assert_eq!(logged.len(), 1, "{logged:?}");
    let fields = &logged[0];
    assert_eq!(
        fields.get("kind").map(String::as_str),
        Some(err.kind.code())
    );
    assert_eq!(
        fields.get("row"),
        Some(&err.row.expect("a decoded row has an index").to_string())
    );
    assert_eq!(fields.get("path"), Some(&err.path));
    assert!(
        fields.values().all(|value| !value.contains(CELL)),
        "{fields:?}"
    );
    Ok(())
}
