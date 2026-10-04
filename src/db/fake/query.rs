//! Fake answers for the query RPCs: `Query`, `GetQueryResults`, `GetJob` and `CancelJob`.

use super::FakeCall;
use arrow_array::RecordBatch;
use arrow_ipc::writer::StreamWriter;
use gcloud_sdk::google::cloud::bigquery::v2::{
    query_response, ArrowRecordBatch, ArrowSchema, CancelJobRequest, GetJobRequest,
    GetQueryResultsRequest, Job, JobReference, PostQueryRequest, QueryResponse,
};

/// The job every fake query runs as.
pub(crate) fn job_reference() -> JobReference {
    JobReference {
        project_id: "fake-project".into(),
        job_id: "job1".into(),
        location: Some("US".into()),
    }
}

/// Reads a `Query` request and logs it as `Query <sql>`.
pub(crate) async fn query_request(call: &mut FakeCall) -> PostQueryRequest {
    let request: PostQueryRequest = call.next_request().await.expect("a Query request");
    let sql = request
        .query_request
        .as_ref()
        .map(|q| q.query.clone())
        .unwrap_or_default();
    call.log(format!("Query {sql}"));
    request
}

/// Reads a `GetQueryResults` request and logs it as
/// `GetQueryResults job1 at US max_results=Some(0)`.
pub(crate) async fn query_results_request(call: &mut FakeCall) -> GetQueryResultsRequest {
    let request: GetQueryResultsRequest = call
        .next_request()
        .await
        .expect("a GetQueryResults request");
    call.log(format!(
        "GetQueryResults {} at {} max_results={:?}",
        request.job_id, request.location, request.max_results
    ));
    request
}

/// Reads a `GetJob` request and logs it as `GetJob job1 at US`.
pub(crate) async fn get_job_request(call: &mut FakeCall) -> GetJobRequest {
    let request: GetJobRequest = call.next_request().await.expect("a GetJob request");
    call.log(format!("GetJob {} at {}", request.job_id, request.location));
    request
}

/// Reads a `CancelJob` request and logs it as `CancelJob job1 at EU`.
pub(crate) async fn cancel_job_request(call: &mut FakeCall) -> CancelJobRequest {
    let request: CancelJobRequest = call.next_request().await.expect("a CancelJob request");
    call.log(format!(
        "CancelJob {} at {}",
        request.job_id, request.location
    ));
    request
}

/// The IPC schema message of `batch` and its record batch message, uncompressed, as the inline
/// result of a `Query` response carries them.
pub(crate) fn encode_ipc(batch: &RecordBatch) -> (Vec<u8>, Vec<u8>) {
    let mut writer = StreamWriter::try_new(Vec::new(), &batch.schema()).expect("an IPC writer");
    let schema = writer.get_ref().clone();
    writer.write(batch).expect("the batch encodes");
    let message = writer.get_ref()[schema.len()..].to_vec();
    (schema, message)
}

/// A complete `Query` response carrying `batch` inline, of a result of `total_rows` rows.
pub(crate) fn inline_response(batch: &RecordBatch, total_rows: u64) -> QueryResponse {
    let (schema, message) = encode_ipc(batch);
    QueryResponse {
        job_reference: Some(job_reference()),
        job_complete: Some(true),
        total_rows: Some(total_rows),
        statement_type: "SELECT".into(),
        results_schema: Some(query_response::ResultsSchema::ArrowSchema(ArrowSchema {
            serialized_schema: schema,
        })),
        results: Some(query_response::Results::ArrowRecordBatch(
            ArrowRecordBatch {
                serialized_record_batch: message,
            },
        )),
        ..Default::default()
    }
}

/// A `Query` response for a job still running.
pub(crate) fn incomplete_response() -> QueryResponse {
    QueryResponse {
        job_reference: Some(job_reference()),
        job_complete: Some(false),
        ..Default::default()
    }
}

/// The fake job, done, with its result in `ds.table`.
pub(crate) fn done_job(dataset: &str, table: &str) -> Job {
    use gcloud_sdk::google::cloud::bigquery::v2::{
        JobConfiguration, JobConfigurationQuery, JobStatus, TableReference,
    };
    Job {
        job_reference: Some(job_reference()),
        configuration: Some(JobConfiguration {
            query: Some(JobConfigurationQuery {
                destination_table: Some(TableReference {
                    project_id: "fake-project".into(),
                    dataset_id: dataset.into(),
                    table_id: table.into(),
                }),
                ..Default::default()
            }),
            ..Default::default()
        }),
        status: Some(JobStatus {
            state: "DONE".into(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::fake::read::{
        get_table, open_session, read_rows_request, send_batches, session_request, FakeReadTable,
    };
    use crate::db::fake::spans::{bigquery_fields, CapturedSpans};
    use crate::db::fake::FakeBigQuery;
    use crate::errors::{BigQueryCodecErrorKind, BigQueryError};
    use crate::{
        BigQueryDatasetId, BigQueryDatasetRef, BigQueryDmlStats, BigQueryJobId, BigQueryJobRef,
        BigQueryJobStats, BigQueryLocation, BigQueryQueryOutcome, BigQueryRequestId,
        BigQueryResult, BigQueryStatementType,
    };
    use arrow_array::{ArrayRef, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use futures::{StreamExt, TryStreamExt};
    use gcloud_sdk::google::cloud::bigquery::v2::{
        DmlStats, ErrorProto, GetQueryResultsResponse, JobCancelResponse, JobStatistics,
        JobStatistics2, JobStatus, TableFieldSchema, TableSchema,
    };
    use gcloud_sdk::tonic::Code;
    use serde::Deserialize;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    const DS: BigQueryDatasetId = BigQueryDatasetId::from_static("ds");

    /// `id, name`, with `ids` as the ids and `Åsa <id>` as the names.
    fn people(ids: &[i64]) -> RecordBatch {
        let names: Vec<Option<String>> = ids.iter().map(|i| Some(format!("Åsa {i}"))).collect();
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
            "GetTable" => get_table(call, table).await,
            "CreateReadSession" => {
                let request = session_request(&mut call).await;
                open_session(call, &request, table);
            }
            "ReadRows" => {
                let request = read_rows_request(&mut call).await;
                let index: usize = request.read_stream[1..].parse().expect("s<n>");
                let batches: Vec<(Vec<u8>, i64)> = table.streams[index]
                    .iter()
                    .map(|b| (encode_ipc(b).1, b.num_rows() as i64))
                    .collect();
                send_batches(&mut call, &batches);
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
            query_request(&mut call).await;
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
            query_request(&mut call).await;
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
            query_request(&mut call).await;
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
            let request = query_request(&mut call).await;
            let q = request.query_request.unwrap_or_default();
            call.log(format!(
                "format={:?} legacy={:?} int64_timestamp={:?} timeout_ms={:?} location={:?} \
                 job_creation_mode={} dry_run={} request_id_len={}",
                q.query_results_format(),
                q.use_legacy_sql,
                q.format_options.map(|f| f.use_int64_timestamp),
                q.timeout_ms,
                q.location,
                q.job_creation_mode,
                q.dry_run,
                q.request_id.len(),
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
                 location=\"\" job_creation_mode=0 dry_run=false request_id_len=32"
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn builder_settings_reach_the_query_request() -> BigQueryResult<()> {
        let fake = FakeBigQuery::start(|mut call: FakeCall| async move {
            let request = query_request(&mut call).await;
            let q = request.query_request.unwrap_or_default();
            let mut labels: Vec<_> = q.labels.into_iter().collect();
            labels.sort();
            call.log(format!(
                "params={} mode={} location={} dataset={:?} labels={labels:?} max_bytes={:?} \
                 cache={:?} timeout_ms={:?} job_timeout_ms={:?} request_id={} max_results={:?}",
                q.query_parameters.len(),
                q.parameter_mode,
                q.location,
                q.default_dataset
                    .map(|d| format!("{}.{}", d.project_id, d.dataset_id)),
                q.maximum_bytes_billed,
                q.use_query_cache,
                q.timeout_ms,
                q.job_timeout_ms,
                q.request_id,
                q.max_results,
            ));
            call.reply(&inline_response(&people(&[1]), 1));
        })
        .await;
        fake.db
            .fluent()
            .query("SELECT @a")
            .param("a", 1)
            .location(BigQueryLocation::from_static("EU"))
            .default_dataset(BigQueryDatasetRef::new("other", DS)?)
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
            .default_dataset(DS)
            .request_id(BigQueryRequestId::new("req-2")?)
            .execute()
            .await?;
        assert_eq!(
            fake.calls(),
            [
                "Query SELECT @a",
                "params=1 mode=NAMED location=EU dataset=Some(\"other.ds\") \
                 labels=[(\"team\", \"data\")] max_bytes=Some(10) cache=Some(false) \
                 timeout_ms=Some(1500) job_timeout_ms=Some(60000) request_id=req-1 \
                 max_results=Some(100)",
                "Query SELECT ?",
                "params=1 mode=POSITIONAL location= dataset=Some(\"fake-project.ds\") labels=[] \
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
    async fn retried_query_keeps_its_request_id() -> BigQueryResult<()> {
        let attempts = Arc::new(AtomicUsize::new(0));
        let fake = FakeBigQuery::start(move |mut call: FakeCall| {
            let attempts = attempts.clone();
            async move {
                let request = query_request(&mut call).await;
                let id = request.query_request.unwrap_or_default().request_id;
                call.log(id);
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
        let ids: Vec<&String> = calls.iter().skip(1).step_by(2).collect();
        assert_eq!(ids.len(), 3, "{calls:?}");
        assert_eq!(ids[0], ids[1], "a retry repeats the id");
        assert_ne!(ids[1], ids[2], "another terminal call sends another id");
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
                        query_request(&mut call).await;
                        let mut response = inline_response(&people(&[1]), 3);
                        response.page_token = "page-2".into();
                        call.reply(&response);
                    }
                    "GetJob" => {
                        get_job_request(&mut call).await;
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
                        query_request(&mut call).await;
                        call.reply(&inline_response(&people(&[1]), 2));
                    }
                    "GetJob" => {
                        get_job_request(&mut call).await;
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
                        query_request(&mut call).await;
                        call.reply(&incomplete_response());
                    }
                    "GetQueryResults" => {
                        query_results_request(&mut call).await;
                        let complete = polls.fetch_add(1, Ordering::SeqCst) > 0;
                        call.reply(&GetQueryResultsResponse {
                            job_reference: Some(job_reference()),
                            job_complete: Some(complete),
                            total_rows: complete.then_some(1),
                            ..Default::default()
                        });
                    }
                    "GetJob" => {
                        get_job_request(&mut call).await;
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
            query_request(&mut call).await;
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
                    query_request(&mut call).await;
                    call.reply(&incomplete_response());
                }
                "GetQueryResults" => {
                    query_results_request(&mut call).await;
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
                    get_job_request(&mut call).await;
                    let mut job = done_job("ds", "t");
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
    async fn inline_result_records_the_response_figures() -> BigQueryResult<()> {
        let (spans, _guard) = CapturedSpans::capture();
        let fake = FakeBigQuery::start(|mut call: FakeCall| async move {
            query_request(&mut call).await;
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
                        query_request(&mut call).await;
                        let mut response = inline_response(&people(&[1]), 3);
                        response.page_token = "page-2".into();
                        response.total_bytes_processed = Some(500);
                        call.reply(&response);
                    }
                    "GetJob" => {
                        get_job_request(&mut call).await;
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
        let mut rows: Vec<Person> =
            tokio::time::timeout(Duration::from_secs(20), rows.try_collect())
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
                    query_request(&mut call).await;
                    call.reply(&incomplete_response());
                }
                "GetQueryResults" => {
                    query_results_request(&mut call).await;
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
                    get_job_request(&mut call).await;
                    let mut job = done_job("ds", "t");
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
            query_request(&mut call).await;
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
                    query_request(&mut call).await;
                    call.reply(&incomplete_response());
                }
                "GetQueryResults" => {
                    query_results_request(&mut call).await;
                    call.reply(&GetQueryResultsResponse {
                        job_complete: Some(true),
                        ..Default::default()
                    });
                }
                "GetJob" => {
                    get_job_request(&mut call).await;
                    let failure = ErrorProto {
                        reason: "invalidQuery".into(),
                        location: "query".into(),
                        message: "boom".into(),
                        ..Default::default()
                    };
                    let mut job = done_job("ds", "t");
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
                    err.job.map(|j| j.job_id.to_string()).as_deref(),
                    Some("job1")
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
            let request = query_request(&mut call).await;
            let dry_run = request.query_request.map(|q| q.dry_run).unwrap_or_default();
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
            .map(|s| {
                s.fields
                    .into_iter()
                    .map(|f| (f.name, f.field_type.to_string()))
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
            cancel_job_request(&mut call).await;
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
            query_request(&mut call).await;
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
        v: &'a str,
    }

    #[derive(serde::Serialize)]
    struct AllForms<'a> {
        s: &'a str,
        arr: Vec<&'a str>,
        st: Holder<'a>,
        j: crate::BigQueryJson<Holder<'a>>,
    }

    /// The payload as each of the four parameters carries it: the STRING text, the ARRAY
    /// element, the STRUCT field and the JSON document's field.
    fn carried_values(request: &PostQueryRequest) -> Vec<String> {
        let q = request.query_request.clone().unwrap_or_default();
        let value = |i: usize| {
            q.query_parameters[i]
                .parameter_value
                .clone()
                .unwrap_or_default()
        };
        let json: serde_json::Value =
            serde_json::from_str(&value(3).value.unwrap_or_default()).expect("JSON text");
        vec![
            value(0).value.unwrap_or_default(),
            value(1).array_values[0].value.clone().unwrap_or_default(),
            value(2).struct_values["v"]
                .value
                .clone()
                .unwrap_or_default(),
            json["v"].as_str().unwrap_or_default().to_string(),
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
        let string = || BigQueryFieldType::String { max_length: None };
        let holder = BigQueryFieldType::Struct(vec![BigQueryFieldSchema {
            name: "v".into(),
            field_type: string(),
            mode: BigQueryFieldMode::Nullable,
            description: None,
            default_value_expression: None,
        }]);
        let named = "SELECT @s, @arr, @st.v, JSON_VALUE(@j, '$.v') -- @s ?";
        let positional = "SELECT ?, ?, ?.v, JSON_VALUE(?, '$.v') /* ? */";
        let corpus = crate::sql::tests::injection_corpus();
        for payload in &corpus {
            let p = payload.as_str();
            let all = AllForms {
                s: p,
                arr: vec![p],
                st: Holder { v: p },
                j: crate::BigQueryJson(Holder { v: p }),
            };
            let q = || fake.db.fluent();
            let forms = [
                (
                    "param",
                    named,
                    q().query(named)
                        .param("s", all.s)
                        .param("arr", &all.arr)
                        .param("st", &all.st)
                        .param("j", &all.j),
                ),
                (
                    "param_as",
                    named,
                    q().query(named)
                        .param_as("s", string(), all.s)
                        .param_as("arr", BigQueryParamType::array_of(string()), &all.arr)
                        .param_as("st", holder.clone(), &all.st)
                        .param_as("j", BigQueryFieldType::Json, &all.j),
                ),
                ("params", named, q().query(named).params(&all)),
                (
                    "positional_param",
                    positional,
                    q().query(positional)
                        .positional_param(all.s)
                        .positional_param(&all.arr)
                        .positional_param(&all.st)
                        .positional_param(&all.j),
                ),
                (
                    "positional_param_as",
                    positional,
                    q().query(positional)
                        .positional_param_as(string(), all.s)
                        .positional_param_as(BigQueryParamType::array_of(string()), &all.arr)
                        .positional_param_as(holder.clone(), &all.st)
                        .positional_param_as(BigQueryFieldType::Json, &all.j),
                ),
            ];
            for (form, sql, builder) in forms {
                builder.execute().await?;
                let request = requests
                    .lock()
                    .expect("not poisoned")
                    .pop()
                    .expect("one request per call");
                let what = format!("{form} with {:?}", &p[..p.len().min(40)]);
                let q = request.query_request.clone().unwrap_or_default();
                assert_eq!(q.query.as_bytes(), sql.as_bytes(), "{what}: the SQL text");
                assert_eq!(q.query_parameters.len(), 4, "{what}");
                assert_eq!(carried_values(&request), [p; 4], "{what}: the values");
            }
        }
        Ok(())
    }
}
