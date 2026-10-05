use crate::db::fake::spans::CapturedSpans;
use crate::db::fake::write::{
    ack, column, describe, ids, in_band, row_errors, schema, ArrowConnection, CREATED_STREAM,
    DEFAULT_STREAM,
};
use crate::db::fake::{FakeBigQuery, FakeCall};
use crate::errors::{BigQueryCodecErrorKind, BigQueryError};
use crate::BigQueryWriteStreamName;
use crate::{BigQueryStreamingWriteOptions, BigQueryWriteMode, BigQueryWriteResponse};
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::StreamExt;
use gcloud_sdk::google::cloud::bigquery::storage::v1::storage_error::StorageErrorCode;
use gcloud_sdk::google::cloud::bigquery::storage::v1::table_field_schema::{Mode, Type};
use gcloud_sdk::google::cloud::bigquery::storage::v1::{
    AppendRowsRequest, AppendRowsResponse, TableSchema,
};
use gcloud_sdk::prost::Message;
use gcloud_sdk::tonic::Code;
use serde::Serialize;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::db::fake::{ORDERS, SHOP};

#[derive(Serialize)]
struct Row {
    id: i64,
    name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    extra: Option<String>,
}

fn row(id: i64) -> Row {
    Row {
        id,
        name: Some(format!("n{id}")),
        extra: None,
    }
}

fn options() -> BigQueryStreamingWriteOptions {
    BigQueryStreamingWriteOptions::new().with_max_batch_rows(1)
}

async fn within<F: Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(20), future)
        .await
        .expect("the writer answers in time")
}

/// The log lines of connection `connection`.
fn on_connection(calls: &[String], connection: usize) -> Vec<String> {
    let prefix = format!("c{connection} ");
    calls
        .iter()
        .filter(|line| line.starts_with(&prefix))
        .cloned()
        .collect()
}

fn count(calls: &[String], prefix: &str) -> usize {
    calls.iter().filter(|line| line.starts_with(prefix)).count()
}

/// A committed stream as BigQuery keeps it: an append lands only at the stream's end.
#[derive(Default)]
struct StreamEnd(Mutex<i64>);

impl StreamEnd {
    fn answer(&self, request: &AppendRowsRequest) -> AppendRowsResponse {
        self.answer_rows(request, ids(request).len())
    }

    /// Answers `request`, which holds `rows` rows.
    fn answer_rows(&self, request: &AppendRowsRequest, rows: usize) -> AppendRowsResponse {
        let mut end = self.0.lock().expect("the lock is never poisoned");
        let offset = request
            .offset
            .expect("a committed stream request has an offset");
        let rows = rows as i64;
        if offset == *end {
            *end += rows;
            ack(Some(offset))
        } else if offset < *end {
            in_band(
                Code::AlreadyExists,
                Some(StorageErrorCode::OffsetAlreadyExists),
                &format!("The offset is within stream, expected offset {end}, received {offset}"),
            )
        } else {
            in_band(
                Code::OutOfRange,
                Some(StorageErrorCode::OffsetOutOfRange),
                &format!("The offset is beyond stream, expected offset {end}, received {offset}"),
            )
        }
    }
}

async fn collect(
    responses: futures::stream::BoxStream<'_, crate::BigQueryResult<BigQueryWriteResponse>>,
) -> Vec<crate::BigQueryResult<BigQueryWriteResponse>> {
    within(responses.collect::<Vec<_>>()).await
}

#[tokio::test]
async fn default_mode_resends_unacked_batches_after_reconnect() {
    let connections = Arc::new(AtomicUsize::new(0));
    let fake = FakeBigQuery::start(move |call| {
        let connections = connections.clone();
        async move {
            let Some(mut call) = call.answer_unary(schema(&[])).await else {
                return;
            };
            let connection = connections.fetch_add(1, Ordering::SeqCst);
            let mut requests_seen = 0;
            while let Some(request) = call.next_request::<AppendRowsRequest>().await {
                call.log(describe(connection, &request));
                if connection == 0 && requests_seen == 1 {
                    // The second batch's answer is lost with the connection.
                    call.drop_connection().await;
                    return;
                }
                call.send(&ack(None));
                requests_seen += 1;
            }
            call.finish();
        }
    })
    .await;
    let (mut writer, responses) = fake
        .db
        .create_streaming_writer_with_options::<Row>(SHOP.table(ORDERS), options())
        .await
        .expect("the writer opens");
    for id in 0..3 {
        within(writer.write(&row(id)))
            .await
            .expect("the row is written");
    }
    let summary = within(writer.finish()).await.expect("the writer finishes");
    assert_eq!(
        (summary.rows_written, summary.rows_failed, summary.batches),
        (3, 0, 3)
    );
    assert_eq!(summary.stream, None);
    let responses = collect(responses).await;
    let indexes: Vec<u64> = responses
        .iter()
        .map(|response| {
            response
                .as_ref()
                .map(|acknowledged| acknowledged.batch_index)
                .expect("every batch is written")
        })
        .collect();
    assert_eq!(indexes, [0, 1, 2]);
    let calls = fake.calls();
    assert_eq!(
        &on_connection(&calls, 0)[..2],
        ["c0 append [0] schema=id,name", "c0 append [1]"]
    );
    // Batch 0's answer can be lost with the connection too, in which case it is sent again.
    let resent = on_connection(&calls, 1);
    assert!(resent[0].ends_with(" schema=id,name"), "{resent:?}");
    assert_eq!(
        resent[resent.len() - 2..]
            .iter()
            .map(|line| line.replace(" schema=id,name", ""))
            .collect::<Vec<_>>(),
        ["c1 append [1]", "c1 append [2]"]
    );
}

#[tokio::test]
async fn committed_mode_counts_offset_already_exists_as_written() {
    let connections = Arc::new(AtomicUsize::new(0));
    let end = Arc::new(StreamEnd::default());
    let fake = FakeBigQuery::start(move |call| {
        let connections = connections.clone();
        let end = end.clone();
        async move {
            let Some(mut call) = call.answer_unary(schema(&[])).await else {
                return;
            };
            let connection = connections.fetch_add(1, Ordering::SeqCst);
            while let Some(request) = call.next_request::<AppendRowsRequest>().await {
                call.log(describe(connection, &request));
                let answer = end.answer(&request);
                if connection == 0 && request.offset == Some(1) {
                    // Written, but the answer is lost with the connection.
                    call.drop_connection().await;
                    return;
                }
                call.send(&answer);
            }
            call.finish();
        }
    })
    .await;
    let (mut writer, responses) = fake
        .db
        .create_streaming_writer_with_options::<Row>(
            SHOP.table(ORDERS),
            options().with_mode(BigQueryWriteMode::Committed),
        )
        .await
        .expect("the writer opens");
    let created = BigQueryWriteStreamName::reported(CREATED_STREAM.to_string());
    assert_eq!(writer.stream_name(), Some(&created));
    for id in 0..3 {
        within(writer.write(&row(id)))
            .await
            .expect("the row is written");
    }
    let summary = within(writer.finish()).await.expect("the writer finishes");
    assert_eq!((summary.rows_written, summary.rows_failed), (3, 0));
    assert_eq!(summary.stream, Some(created));
    let offsets: Vec<Option<i64>> = collect(responses)
        .await
        .into_iter()
        .map(|response| {
            response
                .map(|acknowledged| acknowledged.offset)
                .expect("every batch is written")
        })
        .collect();
    assert_eq!(offsets, [Some(0), Some(1), Some(2)]);
    let calls = fake.calls();
    // Batch 0's answer can be lost with the connection too; resent, it is already there.
    let resent: Vec<String> = on_connection(&calls, 1)
        .iter()
        .map(|line| line.replace(" schema=id,name", ""))
        .collect();
    assert!(
        resent == ["c1 append @1 [1]", "c1 append @2 [2]"]
            || resent == ["c1 append @0 [0]", "c1 append @1 [1]", "c1 append @2 [2]"],
        "{resent:?}"
    );
    assert_eq!(calls[0], "CreateWriteStream COMMITTED");
    assert_eq!(
        calls.last().map(String::as_str),
        Some(format!("FinalizeWriteStream {CREATED_STREAM}").as_str())
    );
}

/// The two answers the proto describes for a request behind a failed one.
#[derive(Clone, Copy, Debug)]
enum BehindAFailure {
    OutOfRange,
    Aborted,
}

#[tokio::test]
async fn committed_mode_resequences_after_row_errors() {
    for behind in [BehindAFailure::OutOfRange, BehindAFailure::Aborted] {
        let end = Arc::new(StreamEnd::default());
        let fake = FakeBigQuery::start(move |call| {
            let end = end.clone();
            async move {
                let Some(mut call) = call.answer_unary(schema(&[])).await else {
                    return;
                };
                while let Some(request) = call.next_request::<AppendRowsRequest>().await {
                    call.log(describe(0, &request));
                    let rows = ids(&request);
                    let Some(bad) = rows.iter().position(|&id| id == 3) else {
                        call.send(&end.answer(&request));
                        continue;
                    };
                    // Answer only once the next batch is in flight behind this one.
                    let next = call.next_request::<AppendRowsRequest>().await;
                    call.send(&row_errors(&[bad as i64]));
                    if let Some(next) = next {
                        call.log(describe(0, &next));
                        call.send(&match behind {
                            BehindAFailure::OutOfRange => end.answer(&next),
                            BehindAFailure::Aborted => in_band(
                                Code::Aborted,
                                None,
                                "Request processing is aborted because of prior failures.",
                            ),
                        });
                    }
                }
                call.finish();
            }
        })
        .await;
        let (mut writer, responses) = fake
            .db
            .create_streaming_writer_with_options::<Row>(
                SHOP.table(ORDERS),
                BigQueryStreamingWriteOptions::new()
                    .with_mode(BigQueryWriteMode::Committed)
                    .with_max_batch_rows(2),
            )
            .await
            .expect("the writer opens");
        let rows: Vec<Row> = (0..6).map(row).collect();
        within(writer.write_all(&rows))
            .await
            .expect("rows are written");
        let summary = within(writer.finish()).await.expect("the writer finishes");
        assert_eq!(
            (summary.rows_written, summary.rows_failed),
            (4, 2),
            "{behind:?}"
        );
        let responses = collect(responses).await;
        assert_eq!(responses.len(), 3, "{behind:?}");
        assert_eq!(
            responses[0].as_ref().map(|r| r.offset).ok(),
            Some(Some(0)),
            "{behind:?}"
        );
        match &responses[1] {
            Err(BigQueryError::RowErrors(err)) => {
                assert_eq!((err.batch_index, err.first_row, err.row_count), (1, 2, 2));
                assert_eq!(err.errors.iter().map(|e| e.row).collect::<Vec<_>>(), [3]);
            }
            other => panic!("{behind:?}: batch 1 must fail with its row errors, got {other:?}"),
        }
        assert_eq!(
            responses[2]
                .as_ref()
                .map(|r| (r.batch_index, r.offset))
                .ok(),
            Some((2, Some(2))),
            "{behind:?}"
        );
        assert_eq!(
            on_connection(&fake.calls(), 0),
            [
                "c0 append @0 [0, 1] schema=id,name",
                "c0 append @2 [2, 3]",
                "c0 append @4 [4, 5]",
                "c0 append @2 [4, 5]"
            ],
            "{behind:?}"
        );
    }
}

#[tokio::test]
async fn pending_mode_commits_only_without_failed_batches() {
    for fail in [false, true] {
        let fake = FakeBigQuery::start(move |call| async move {
            let Some(mut call) = call.answer_unary(schema(&[])).await else {
                return;
            };
            let end = StreamEnd::default();
            while let Some(request) = call.next_request::<AppendRowsRequest>().await {
                call.log(describe(0, &request));
                if fail && ids(&request) == [1] {
                    call.send(&row_errors(&[0]));
                } else {
                    call.send(&end.answer(&request));
                }
            }
            call.finish();
        })
        .await;
        let (mut writer, _responses) = fake
            .db
            .create_streaming_writer_with_options::<Row>(
                SHOP.table(ORDERS),
                options().with_mode(BigQueryWriteMode::Pending),
            )
            .await
            .expect("the writer opens");
        for id in 0..3 {
            within(writer.write(&row(id)))
                .await
                .expect("the row is written");
        }
        let result = within(writer.finish()).await;
        let calls = fake.calls();
        let committed = count(&calls, "BatchCommitWriteStreams");
        if fail {
            match result {
                Err(BigQueryError::WriteStreamError(err)) => {
                    assert_eq!(err.public.code, "NOT_COMMITTED");
                }
                other => panic!("a failed batch must leave the stream uncommitted: {other:?}"),
            }
            assert_eq!(committed, 0, "{calls:?}");
        } else {
            let summary = result.expect("the writer finishes");
            assert_eq!(
                summary.commit_time,
                Some(jiff::Timestamp::from_second(1_791_000_000).expect("valid"))
            );
            assert_eq!(summary.rows_written, 3);
            assert_eq!(calls[0], "CreateWriteStream PENDING");
            assert_eq!(
                &calls[calls.len() - 2..],
                [
                    format!("FinalizeWriteStream {CREATED_STREAM}"),
                    format!("BatchCommitWriteStreams [\"{CREATED_STREAM}\"]")
                ]
            );
        }
    }
}

#[tokio::test]
async fn row_errors_carry_write_order_indexes() {
    let fake = FakeBigQuery::start(|call| async move {
        let Some(mut call) = call.answer_unary(schema(&[])).await else {
            return;
        };
        while let Some(request) = call.next_request::<AppendRowsRequest>().await {
            call.log(describe(0, &request));
            match ids(&request).iter().position(|&id| id == 4) {
                Some(index) => call.send(&row_errors(&[index as i64])),
                None => call.send(&ack(None)),
            }
        }
        call.finish();
    })
    .await;
    let (mut writer, responses) = fake
        .db
        .create_streaming_writer_with_options::<Row>(
            SHOP.table(ORDERS),
            BigQueryStreamingWriteOptions::new().with_max_batch_rows(3),
        )
        .await
        .expect("the writer opens");
    let rows: Vec<Row> = (0..9).map(row).collect();
    within(writer.write_all(&rows))
        .await
        .expect("rows are written");
    let summary = within(writer.finish()).await.expect("the writer finishes");
    assert_eq!((summary.rows_written, summary.rows_failed), (6, 3));
    let responses = collect(responses).await;
    assert!(
        responses[0].is_ok() && responses[2].is_ok(),
        "{responses:?}"
    );
    match &responses[1] {
        Err(BigQueryError::RowErrors(err)) => {
            assert_eq!((err.batch_index, err.first_row, err.row_count), (1, 3, 3));
            assert_eq!(err.errors.len(), 1);
            assert_eq!(err.errors[0].row, 4);
            assert_eq!(err.errors[0].code, "FIELDS_ERROR");
        }
        other => panic!("batch 1 must fail with its row errors, got {other:?}"),
    }
}

#[tokio::test]
async fn updated_schema_switches_plan_at_a_batch_boundary() {
    let fake = FakeBigQuery::start(|call| async move {
        let Some(mut call) = call.answer_unary(schema(&[])).await else {
            return;
        };
        let mut first = true;
        while let Some(request) = call.next_request::<AppendRowsRequest>().await {
            call.log(describe(0, &request));
            let mut answer = ack(None);
            if first {
                answer.updated_schema =
                    Some(schema(&[column("extra", Type::String, Mode::Nullable)]));
                first = false;
            }
            call.send(&answer);
        }
        call.finish();
    })
    .await;
    let (mut writer, _responses) = fake
        .db
        .create_streaming_writer_with_options::<Row>(
            SHOP.table(ORDERS),
            BigQueryStreamingWriteOptions::new(),
        )
        .await
        .expect("the writer opens");
    within(writer.write(&row(0)))
        .await
        .expect("the row is written");
    within(writer.write(&row(1)))
        .await
        .expect("the row is written");
    within(writer.flush())
        .await
        .expect("the batch is acknowledged");
    within(writer.write(&row(2)))
        .await
        .expect("the row is written");
    let with_extra = Row {
        extra: Some("x".into()),
        ..row(3)
    };
    within(writer.write(&with_extra))
        .await
        .expect("the new column is known");
    let summary = within(writer.finish()).await.expect("the writer finishes");
    assert_eq!(summary.rows_written, 4);
    let calls = fake.calls();
    assert_eq!(
        on_connection(&calls, 0),
        [
            "c0 append [0, 1] schema=id,name",
            "c0 append [2, 3] schema=id,name,extra"
        ]
    );
    assert_eq!(count(&calls, "GetWriteStream"), 1, "{calls:?}");
}

#[tokio::test]
async fn write_stream_is_sent_on_every_request() {
    let fake = FakeBigQuery::start(|call| async move {
        let Some(mut call) = call.answer_unary(schema(&[])).await else {
            return;
        };
        while let Some(request) = call.next_request::<AppendRowsRequest>().await {
            call.log(format!(
                "{} to {}",
                describe(0, &request),
                request.write_stream
            ));
            call.send(&ack(None));
        }
        call.finish();
    })
    .await;
    let (mut writer, _responses) = fake
        .db
        .create_streaming_writer_with_options::<Row>(SHOP.table(ORDERS), options())
        .await
        .expect("the writer opens");
    for id in 0..3 {
        within(writer.write(&row(id)))
            .await
            .expect("the row is written");
    }
    within(writer.finish()).await.expect("the writer finishes");
    assert_eq!(
        on_connection(&fake.calls(), 0),
        [
            format!("c0 append [0] schema=id,name to {DEFAULT_STREAM}"),
            format!("c0 append [1] to {DEFAULT_STREAM}"),
            format!("c0 append [2] to {DEFAULT_STREAM}"),
        ]
    );
}

#[tokio::test]
async fn unknown_field_refreshes_schema_once_then_fails() {
    let server_schema = Arc::new(Mutex::new(schema(&[])));
    let shared_schema = server_schema.clone();
    let fake = FakeBigQuery::start(move |call| {
        let schema = shared_schema
            .lock()
            .expect("the lock is never poisoned")
            .clone();
        async move {
            let Some(mut call) = call.answer_unary(schema).await else {
                return;
            };
            while let Some(request) = call.next_request::<AppendRowsRequest>().await {
                call.log(describe(0, &request));
                call.send(&ack(None));
            }
            call.finish();
        }
    })
    .await;
    let (mut writer, _responses) = fake
        .db
        .create_streaming_writer_with_options::<Row>(
            SHOP.table(ORDERS),
            BigQueryStreamingWriteOptions::new()
                .with_schema_refresh_interval(Duration::from_millis(300)),
        )
        .await
        .expect("the writer opens");
    let with_extra = |id| Row {
        extra: Some("x".into()),
        ..row(id)
    };
    for attempt in 0..2 {
        match within(writer.write(&with_extra(0))).await {
            Err(BigQueryError::SerializeError(err)) => {
                assert_eq!(err.kind, BigQueryCodecErrorKind::UnknownField, "{attempt}");
                assert_eq!(
                    (err.path.as_str(), err.row),
                    ("extra", Some(0)),
                    "{attempt}"
                );
            }
            other => panic!("attempt {attempt}: expected UnknownField, got {other:?}"),
        }
    }
    assert_eq!(count(&fake.calls(), "GetWriteStream"), 2);

    *server_schema.lock().expect("the lock is never poisoned") =
        schema(&[column("extra", Type::String, Mode::Nullable)]);
    tokio::time::sleep(Duration::from_millis(350)).await;
    within(writer.write(&with_extra(0)))
        .await
        .expect("the refreshed schema has the column");
    within(writer.finish()).await.expect("the writer finishes");
    let calls = fake.calls();
    assert_eq!(count(&calls, "GetWriteStream"), 3);
    assert_eq!(
        on_connection(&calls, 0),
        ["c0 append [0] schema=id,name,extra"]
    );
}

#[tokio::test]
async fn relaxed_mode_reconnects() {
    let server_schema = Arc::new(Mutex::new(TableSchema {
        fields: vec![
            column("id", Type::Int64, Mode::Required),
            column("name", Type::String, Mode::Required),
        ],
    }));
    let shared_schema = server_schema.clone();
    let connections = Arc::new(AtomicUsize::new(0));
    let fake = FakeBigQuery::start(move |call| {
        let schema = shared_schema
            .lock()
            .expect("the lock is never poisoned")
            .clone();
        let connections = connections.clone();
        async move {
            let Some(mut call) = call.answer_unary(schema).await else {
                return;
            };
            let connection = connections.fetch_add(1, Ordering::SeqCst);
            while let Some(request) = call.next_request::<AppendRowsRequest>().await {
                call.log(describe(connection, &request));
                call.send(&ack(None));
            }
            call.finish();
        }
    })
    .await;
    let (mut writer, _responses) = fake
        .db
        .create_streaming_writer_with_options::<Row>(
            SHOP.table(ORDERS),
            BigQueryStreamingWriteOptions::new(),
        )
        .await
        .expect("the writer opens");
    within(writer.write(&row(0)))
        .await
        .expect("the row is written");
    within(writer.flush())
        .await
        .expect("the batch is acknowledged");

    *server_schema.lock().expect("the lock is never poisoned") = schema(&[]);
    let no_name = Row {
        name: None,
        ..row(1)
    };
    within(writer.write(&no_name))
        .await
        .expect("the refreshed schema relaxed the column");
    within(writer.finish()).await.expect("the writer finishes");
    let calls = fake.calls();
    assert_eq!(on_connection(&calls, 0), ["c0 append [0] schema=id,name"]);
    assert_eq!(on_connection(&calls, 1), ["c1 append [1] schema=id,name"]);
}

#[tokio::test]
async fn write_waits_at_the_inflight_limit() {
    let permits = Arc::new(tokio::sync::Semaphore::new(0));
    let server_permits = permits.clone();
    let fake = FakeBigQuery::start(move |call| {
        let permits = server_permits.clone();
        async move {
            let Some(mut call) = call.answer_unary(schema(&[])).await else {
                return;
            };
            while let Some(request) = call.next_request::<AppendRowsRequest>().await {
                call.log(describe(0, &request));
                let Ok(permit) = permits.acquire().await else {
                    return;
                };
                permit.forget();
                call.send(&ack(None));
            }
            call.finish();
        }
    })
    .await;
    let (mut writer, _responses) = fake
        .db
        .create_streaming_writer_with_options::<Row>(
            SHOP.table(ORDERS),
            options().with_max_inflight_requests(2),
        )
        .await
        .expect("the writer opens");
    within(writer.write(&row(0))).await.expect("in flight: 1");
    within(writer.write(&row(1))).await.expect("in flight: 2");
    let third = tokio::time::timeout(Duration::from_millis(500), writer.write(&row(2))).await;
    assert!(
        third.is_err(),
        "the third batch must wait for an acknowledgement"
    );
    permits.add_permits(3);
    let summary = within(writer.finish()).await.expect("the writer finishes");
    assert_eq!(summary.rows_written, 3);
    assert_eq!(on_connection(&fake.calls(), 0).len(), 3);
}

#[tokio::test]
async fn idle_batch_is_flushed_after_the_delay() {
    let fake = FakeBigQuery::start(|call| async move {
        let Some(mut call) = call.answer_unary(schema(&[])).await else {
            return;
        };
        while let Some(request) = call.next_request::<AppendRowsRequest>().await {
            call.log(describe(0, &request));
            call.send(&ack(None));
        }
        call.finish();
    })
    .await;
    let (mut writer, _responses) = fake
        .db
        .create_streaming_writer_with_options::<Row>(
            SHOP.table(ORDERS),
            BigQueryStreamingWriteOptions::new().with_max_batch_delay(Duration::from_millis(50)),
        )
        .await
        .expect("the writer opens");
    within(writer.write(&row(0)))
        .await
        .expect("the row is written");
    within(fake.wait_for_calls(2)).await;
    assert_eq!(
        on_connection(&fake.calls(), 0),
        ["c0 append [0] schema=id,name"]
    );
    within(writer.finish()).await.expect("the writer finishes");
}

#[tokio::test]
async fn oversize_stream_status_is_not_retried() {
    let fake = FakeBigQuery::start(|call| async move {
        let Some(mut call) = call.answer_unary(schema(&[])).await else {
            return;
        };
        if let Some(request) = call.next_request::<AppendRowsRequest>().await {
            call.log(describe(0, &request));
        }
        call.fail(
            Code::InvalidArgument,
            "Request contains an invalid argument.",
        );
    })
    .await;
    let (mut writer, responses) = fake
        .db
        .create_streaming_writer_with_options::<Row>(SHOP.table(ORDERS), options())
        .await
        .expect("the writer opens");
    within(writer.write(&row(0)))
        .await
        .expect("the row is queued");
    let flushed = within(writer.flush()).await;
    assert!(
        matches!(&flushed, Err(BigQueryError::DatabaseError(e)) if !e.retry_possible),
        "{flushed:?}"
    );
    let again = within(writer.write(&row(1))).await;
    assert!(
        matches!(again, Err(BigQueryError::DatabaseError(_))),
        "{again:?}"
    );
    assert!(within(writer.finish()).await.is_err());
    let responses = collect(responses).await;
    assert_eq!(responses.len(), 1);
    assert!(responses[0].is_err());
    assert_eq!(count(&fake.calls(), "c0 append"), 1);
}

/// Collects what a `tracing` subscriber writes.
#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .expect("the lock is never poisoned")
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Captured {
    type Writer = Captured;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

#[tokio::test]
async fn drop_without_finish_warns() {
    let fake = FakeBigQuery::start(|call| async move {
        let Some(mut call) = call.answer_unary(schema(&[])).await else {
            return;
        };
        while let Some(_request) = call.next_request::<AppendRowsRequest>().await {
            call.send(&ack(None));
        }
        call.finish();
    })
    .await;
    let captured = Captured::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(captured.clone())
        .with_ansi(false)
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    let (mut writer, _responses) = fake
        .db
        .create_streaming_writer::<Row>(SHOP.table(ORDERS))
        .await
        .expect("the writer opens");
    within(writer.write(&row(0)))
        .await
        .expect("the row is written");
    drop(writer);
    let logged = String::from_utf8(
        captured
            .0
            .lock()
            .expect("the lock is never poisoned")
            .clone(),
    )
    .expect("the log is text");
    assert!(
        logged.contains("WARN") && logged.contains("dropped without finish()"),
        "{logged}"
    );
}

#[tokio::test]
async fn write_span_and_summary_record_what_was_sent() {
    let (spans, _guard) = CapturedSpans::capture();
    let sent_bytes = Arc::new(AtomicUsize::new(0));
    let counted = sent_bytes.clone();
    let fake = FakeBigQuery::start(move |call| {
        let counted = counted.clone();
        async move {
            let Some(mut call) = call.answer_unary(schema(&[])).await else {
                return;
            };
            let mut failed_once = false;
            while let Some(request) = call.next_request::<AppendRowsRequest>().await {
                counted.fetch_add(request.encoded_len(), Ordering::SeqCst);
                if ids(&request) == [1] && !failed_once {
                    failed_once = true;
                    call.send(&in_band(Code::Internal, None, "try again"));
                } else {
                    call.send(&ack(None));
                }
            }
            call.finish();
        }
    })
    .await;
    let (mut writer, _responses) = fake
        .db
        .create_streaming_writer_with_options::<Row>(SHOP.table(ORDERS), options())
        .await
        .expect("the writer opens");
    for id in 0..3 {
        within(writer.write(&row(id)))
            .await
            .expect("the row is written");
    }
    let summary = within(writer.finish()).await.expect("the writer finishes");
    let bytes_sent = sent_bytes.load(Ordering::SeqCst);
    assert!(bytes_sent > 0);
    assert_eq!(summary.bytes_sent, bytes_sent as u64);
    assert_eq!(
        spans.only("BigQuery streaming write"),
        crate::db::fake::spans::bigquery_fields(&[
            ("table", "shop.orders"),
            ("write_mode", "Default"),
            ("rows_appended", "3"),
            ("bytes_sent", &bytes_sent.to_string()),
            ("appends", "4"),
            ("retries", "1"),
        ])
    );
}

#[tokio::test]
async fn dropped_writer_records_what_was_sent() {
    let (spans, _guard) = CapturedSpans::capture();
    let fake = FakeBigQuery::start(|call| async move {
        let Some(mut call) = call.answer_unary(schema(&[])).await else {
            return;
        };
        while let Some(_request) = call.next_request::<AppendRowsRequest>().await {
            call.send(&ack(None));
        }
        call.finish();
    })
    .await;
    let (mut writer, _responses) = fake
        .db
        .create_streaming_writer::<Row>(SHOP.table(ORDERS))
        .await
        .expect("the writer opens");
    within(writer.write(&row(0)))
        .await
        .expect("the row is written");
    within(writer.flush())
        .await
        .expect("the row is acknowledged");
    drop(writer);
    let fields = spans
        .wait_for("BigQuery streaming write", "/bigquery/appends")
        .await;
    assert_eq!(fields["/bigquery/appends"], "1");
    assert_eq!(fields["/bigquery/rows_appended"], "1");
}

#[tokio::test]
async fn an_upsert_outside_the_default_stream_is_refused_before_any_call() {
    let fake = FakeBigQuery::start(|call: FakeCall| async move {
        panic!("unexpected call {}", call.method());
    })
    .await;
    let result = fake
        .db
        .fluent()
        .insert()
        .into(SHOP.table(ORDERS))
        .objects(&[row(1)])
        .upsert()
        .exactly_once()
        .execute()
        .await;
    match result {
        Err(BigQueryError::InvalidParametersError(err)) => {
            assert_eq!(err.public.field, "mode", "{err}");
        }
        other => panic!("expected the mode to be refused, got {other:?}"),
    }
    assert!(fake.calls().is_empty(), "{:?}", fake.calls());
}

/// A fake whose `AppendRows` keeps a committed or buffered stream's offsets.
async fn stream_with_offsets() -> FakeBigQuery {
    FakeBigQuery::start(|call| async move {
        let Some(mut call) = call.answer_unary(schema(&[])).await else {
            return;
        };
        let end = StreamEnd::default();
        while let Some(request) = call.next_request::<AppendRowsRequest>().await {
            call.log(describe(0, &request));
            call.send(&end.answer(&request));
        }
        call.finish();
    })
    .await
}

#[tokio::test]
async fn buffered_mode_flushes_what_is_written_and_finish_flushes_before_finalizing() {
    let fake = stream_with_offsets().await;
    let (mut writer, _responses) = fake
        .db
        .create_streaming_writer_with_options::<Row>(
            SHOP.table(ORDERS),
            options().with_mode(BigQueryWriteMode::Buffered),
        )
        .await
        .expect("the writer opens");
    assert_eq!(
        within(writer.flush_rows()).await.expect("nothing to flush"),
        None
    );
    for id in 0..2 {
        within(writer.write(&row(id)))
            .await
            .expect("the row is written");
    }
    assert_eq!(
        within(writer.flush_rows()).await.expect("the rows flush"),
        Some(1)
    );
    within(writer.write(&row(2)))
        .await
        .expect("the row is written");
    let summary = within(writer.finish()).await.expect("the writer finishes");
    assert_eq!(summary.rows_written, 3);
    assert_eq!(
        summary.stream,
        Some(BigQueryWriteStreamName::reported(CREATED_STREAM.into()))
    );
    assert_eq!(
        fake.calls(),
        [
            "CreateWriteStream BUFFERED".to_string(),
            "c0 append @0 [0] schema=id,name".to_string(),
            "c0 append @1 [1]".to_string(),
            format!("FlushRows {CREATED_STREAM} @1"),
            "c0 append @2 [2]".to_string(),
            format!("FlushRows {CREATED_STREAM} @2"),
            format!("FinalizeWriteStream {CREATED_STREAM}"),
        ]
    );
}

#[tokio::test]
async fn flush_rows_to_flushes_up_to_the_given_offset() {
    let fake = stream_with_offsets().await;
    let (mut writer, _responses) = fake
        .db
        .create_streaming_writer_with_options::<Row>(
            SHOP.table(ORDERS),
            options().with_mode(BigQueryWriteMode::Buffered),
        )
        .await
        .expect("the writer opens");
    within(writer.write_all(&[row(0), row(1), row(2)]))
        .await
        .expect("the rows are written");
    within(writer.flush()).await.expect("the batches are sent");
    within(writer.flush_rows_to(0))
        .await
        .expect("the first row flushes");
    let calls = fake.calls();
    assert_eq!(
        calls.last(),
        Some(&format!("FlushRows {CREATED_STREAM} @0")),
        "{calls:?}"
    );
    within(writer.finish()).await.expect("the writer finishes");
}

#[tokio::test]
async fn buffered_mode_flushes_only_the_rows_written_around_a_failed_batch() {
    let fake = FakeBigQuery::start(|call| async move {
        let Some(mut call) = call.answer_unary(schema(&[])).await else {
            return;
        };
        let end = StreamEnd::default();
        while let Some(request) = call.next_request::<AppendRowsRequest>().await {
            call.log(describe(0, &request));
            if ids(&request) == [1] {
                call.send(&row_errors(&[0]));
            } else {
                call.send(&end.answer(&request));
            }
        }
        call.finish();
    })
    .await;
    let (mut writer, _responses) = fake
        .db
        .create_streaming_writer_with_options::<Row>(
            SHOP.table(ORDERS),
            options().with_mode(BigQueryWriteMode::Buffered),
        )
        .await
        .expect("the writer opens");
    within(writer.write_all(&[row(0), row(1), row(2)]))
        .await
        .expect("the rows are written");
    within(writer.flush_rows())
        .await
        .expect("the written rows flush");
    let summary = within(writer.finish()).await.expect("the writer finishes");
    assert_eq!((summary.rows_written, summary.rows_failed), (2, 1));
    let calls = fake.calls();
    let unary: Vec<&String> = calls
        .iter()
        .filter(|line| !line.starts_with("c0 "))
        .collect();
    assert_eq!(
        unary,
        [
            "CreateWriteStream BUFFERED",
            &format!("FlushRows {CREATED_STREAM} @1"),
            &format!("FlushRows {CREATED_STREAM} @1"),
            &format!("FinalizeWriteStream {CREATED_STREAM}"),
        ],
        "{calls:?}"
    );
}

#[tokio::test]
async fn flush_rows_to_reaches_acknowledged_rows_after_the_writer_failed() {
    let fake = FakeBigQuery::start(|call| async move {
        let Some(mut call) = call.answer_unary(schema(&[])).await else {
            return;
        };
        let end = StreamEnd::default();
        if let Some(request) = call.next_request::<AppendRowsRequest>().await {
            call.log(describe(0, &request));
            call.send(&end.answer(&request));
        }
        if let Some(request) = call.next_request::<AppendRowsRequest>().await {
            call.log(describe(0, &request));
        }
        call.fail(
            Code::InvalidArgument,
            "Request contains an invalid argument.",
        );
    })
    .await;
    let (mut writer, _responses) = fake
        .db
        .create_streaming_writer_with_options::<Row>(
            SHOP.table(ORDERS),
            options().with_mode(BigQueryWriteMode::Buffered),
        )
        .await
        .expect("the writer opens");
    within(writer.write(&row(0)))
        .await
        .expect("the row is written");
    within(writer.flush())
        .await
        .expect("the first row is acknowledged");
    within(writer.write(&row(1)))
        .await
        .expect("the row is queued");
    assert!(within(writer.flush()).await.is_err(), "the writer fails");
    within(writer.flush_rows_to(0))
        .await
        .expect("the acknowledged row flushes");
    let calls = fake.calls();
    assert_eq!(
        calls.last(),
        Some(&format!("FlushRows {CREATED_STREAM} @0")),
        "{calls:?}"
    );
}

#[tokio::test]
async fn flushing_rows_outside_buffered_mode_is_refused_before_any_call() {
    for mode in [
        BigQueryWriteMode::Default,
        BigQueryWriteMode::Committed,
        BigQueryWriteMode::Pending,
    ] {
        let fake = stream_with_offsets().await;
        let (mut writer, _responses) = fake
            .db
            .create_streaming_writer_with_options::<Row>(
                SHOP.table(ORDERS),
                options().with_mode(mode),
            )
            .await
            .expect("the writer opens");
        for result in [
            within(writer.flush_rows()).await.map(|_| ()),
            within(writer.flush_rows_to(0)).await.map(|_| ()),
        ] {
            match result {
                Err(BigQueryError::InvalidParametersError(err)) => {
                    assert_eq!(err.public.field, "mode", "{mode:?}");
                }
                other => panic!("{mode:?} must refuse a flush, got {other:?}"),
            }
        }
        within(writer.finish()).await.expect("the writer finishes");
        assert_eq!(count(&fake.calls(), "FlushRows"), 0, "{mode:?}");
    }
}

#[tokio::test]
async fn a_buffered_insert_flushes_every_row_before_finalizing() {
    let fake = stream_with_offsets().await;
    let summary = within(
        fake.db
            .fluent()
            .insert()
            .into(SHOP.table(ORDERS))
            .objects(&[row(0), row(1)])
            .options(options())
            .buffered()
            .execute(),
    )
    .await
    .expect("the insert runs");
    assert_eq!(summary.rows_written, 2);
    let calls = fake.calls();
    assert_eq!(calls[0], "CreateWriteStream BUFFERED");
    assert_eq!(
        &calls[calls.len() - 2..],
        [
            format!("FlushRows {CREATED_STREAM} @1"),
            format!("FinalizeWriteStream {CREATED_STREAM}"),
        ]
    );
}

/// `id INT64 NOT NULL, name STRING` with `ids`, as the Arrow side of [`Row`].
fn record_batch(ids: std::ops::Range<i64>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let names: StringArray = ids.clone().map(|id| Some(format!("n{id}"))).collect();
    RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from_iter_values(ids)), Arc::new(names)],
    )
    .expect("the columns fit the schema")
}

/// A fake whose `AppendRows` reads Arrow rows, keeping a stream's offsets when requests have
/// them, and logs each request on its connection.
async fn arrow_stream() -> FakeBigQuery {
    let connections = Arc::new(AtomicUsize::new(0));
    FakeBigQuery::start(move |call| {
        let connections = connections.clone();
        async move {
            let Some(mut call) = call.answer_unary(schema(&[])).await else {
                return;
            };
            let connection = connections.fetch_add(1, Ordering::SeqCst);
            let mut arrow = ArrowConnection::default();
            let end = StreamEnd::default();
            while let Some(request) = call.next_request::<AppendRowsRequest>().await {
                let (line, rows) = arrow.describe(connection, &request);
                call.log(line);
                match request.offset {
                    Some(_) => call.send(&end.answer_rows(&request, rows)),
                    None => call.send(&ack(None)),
                }
            }
            call.finish();
        }
    })
    .await
}

#[tokio::test]
async fn record_batches_carry_their_schema_on_the_first_request_only() {
    let fake = arrow_stream().await;
    let (mut writer, responses) = fake
        .db
        .create_record_batch_writer(SHOP.table(ORDERS))
        .await
        .expect("the writer opens");
    within(writer.write_batch(&record_batch(0..2)))
        .await
        .expect("the batch is written");
    within(writer.write_batch(&record_batch(2..3)))
        .await
        .expect("the batch is written");
    let summary = within(writer.finish()).await.expect("the writer finishes");
    assert_eq!((summary.rows_written, summary.batches), (3, 2));
    let first_rows: Vec<u64> = collect(responses)
        .await
        .into_iter()
        .map(|response| response.expect("every batch is written").first_row)
        .collect();
    assert_eq!(first_rows, [0, 2]);
    assert_eq!(
        fake.calls(),
        [
            format!("GetWriteStream {DEFAULT_STREAM}"),
            "c0 append [0, 1] arrow=id,name".to_string(),
            "c0 append [2]".to_string(),
        ]
    );
}

#[tokio::test]
async fn a_record_batch_over_the_request_cap_goes_as_slices_at_their_offsets() {
    let fake = arrow_stream().await;
    let (mut writer, _responses) = fake
        .db
        .create_record_batch_writer_with_options(
            SHOP.table(ORDERS),
            BigQueryStreamingWriteOptions::new()
                .with_mode(BigQueryWriteMode::Committed)
                .with_max_request_bytes(4_000),
        )
        .await
        .expect("the writer opens");
    within(writer.write_batch(&record_batch(0..500)))
        .await
        .expect("the batch is written");
    let summary = within(writer.finish()).await.expect("the writer finishes");
    assert_eq!(summary.rows_written, 500);
    assert!(summary.batches > 1, "{summary:?}");
    let appends: Vec<String> = fake
        .calls()
        .into_iter()
        .filter(|line| line.starts_with("c0 append"))
        .collect();
    let mut next_id = 0;
    for append in &appends {
        let offset: i64 = append["c0 append @".len()..]
            .split(' ')
            .next()
            .and_then(|offset| offset.parse().ok())
            .expect("every request has an offset");
        assert_eq!(offset, next_id, "{appends:?}");
        let ids = &append[append.find('[').expect("ids")..=append.find(']').expect("ids")];
        let first: i64 = ids[1..]
            .split([',', ']'])
            .next()
            .and_then(|id| id.parse().ok())
            .expect("a slice has rows");
        assert_eq!(first, next_id, "{appends:?}");
        next_id += ids.split(',').count() as i64;
    }
    assert_eq!(next_id, 500);
}

#[tokio::test]
async fn a_new_arrow_schema_is_sent_on_a_new_connection() {
    let fake = arrow_stream().await;
    let (mut writer, _responses) = fake
        .db
        .create_record_batch_writer(SHOP.table(ORDERS))
        .await
        .expect("the writer opens");
    within(writer.write_batch(&record_batch(0..1)))
        .await
        .expect("the batch is written");
    let narrower = record_batch(1..2)
        .project(&[0])
        .expect("the batch has an `id` column");
    within(writer.write_batch(&narrower))
        .await
        .expect("the batch is written");
    within(writer.finish()).await.expect("the writer finishes");
    assert_eq!(
        fake.calls()[1..],
        [
            "c0 append [0] arrow=id,name".to_string(),
            "c1 append [1] arrow=id".to_string(),
        ]
    );
}

#[tokio::test]
async fn a_buffered_record_batch_writer_flushes_what_is_written() {
    let fake = arrow_stream().await;
    let (mut writer, _responses) = fake
        .db
        .create_record_batch_writer_with_options(
            SHOP.table(ORDERS),
            BigQueryStreamingWriteOptions::new().with_mode(BigQueryWriteMode::Buffered),
        )
        .await
        .expect("the writer opens");
    within(writer.write_batch(&record_batch(0..3)))
        .await
        .expect("the batch is written");
    assert_eq!(
        within(writer.flush_rows()).await.expect("the rows flush"),
        Some(2)
    );
    within(writer.write_batch(&record_batch(3..4)))
        .await
        .expect("the batch is written");
    within(writer.finish()).await.expect("the writer finishes");
    assert_eq!(
        fake.calls(),
        [
            "CreateWriteStream BUFFERED".to_string(),
            "c0 append @0 [0, 1, 2] arrow=id,name".to_string(),
            format!("FlushRows {CREATED_STREAM} @2"),
            "c0 append @3 [3]".to_string(),
            format!("FlushRows {CREATED_STREAM} @3"),
            format!("FinalizeWriteStream {CREATED_STREAM}"),
        ]
    );
}

#[tokio::test]
async fn a_record_batch_insert_closes_the_stream_of_every_mode() {
    let modes = [
        (BigQueryWriteMode::Default, vec![]),
        (
            BigQueryWriteMode::Committed,
            vec![format!("FinalizeWriteStream {CREATED_STREAM}")],
        ),
        (
            BigQueryWriteMode::Pending,
            vec![
                format!("FinalizeWriteStream {CREATED_STREAM}"),
                format!("BatchCommitWriteStreams [\"{CREATED_STREAM}\"]"),
            ],
        ),
        (
            BigQueryWriteMode::Buffered,
            vec![
                format!("FlushRows {CREATED_STREAM} @3"),
                format!("FinalizeWriteStream {CREATED_STREAM}"),
            ],
        ),
    ];
    for (mode, closing) in modes {
        let fake = arrow_stream().await;
        let insert = fake
            .db
            .fluent()
            .insert()
            .into(SHOP.table(ORDERS))
            .record_batches([record_batch(0..2), record_batch(2..4)]);
        let insert = match mode {
            BigQueryWriteMode::Committed => insert.exactly_once(),
            BigQueryWriteMode::Pending => insert.atomic(),
            BigQueryWriteMode::Buffered => insert.buffered(),
            _ => insert,
        };
        let summary = within(insert.execute()).await.expect("the insert runs");
        assert_eq!(summary.rows_written, 4, "{mode:?}");
        let calls = fake.calls();
        let appends = count(&calls, "c0 append");
        assert_eq!(appends, 2, "{mode:?}: {calls:?}");
        assert_eq!(calls[1 + appends..], closing, "{mode:?}");
    }
}

#[tokio::test]
async fn a_row_too_large_for_a_request_is_named_by_its_write_order_index() {
    let fake = arrow_stream().await;
    let (mut writer, _responses) = fake
        .db
        .create_record_batch_writer_with_options(
            SHOP.table(ORDERS),
            BigQueryStreamingWriteOptions::new().with_max_request_bytes(2_000),
        )
        .await
        .expect("the writer opens");
    within(writer.write_batch(&record_batch(0..2)))
        .await
        .expect("the batch is written");
    let schema = record_batch(0..0).schema();
    let large = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![2, 3])),
            Arc::new(StringArray::from(vec!["n2".to_string(), "x".repeat(4_000)])),
        ],
    )
    .expect("the columns fit the schema");
    match within(writer.write_batch(&large)).await {
        Err(BigQueryError::SerializeError(err)) => {
            assert_eq!(err.kind, BigQueryCodecErrorKind::RowTooLarge);
            assert_eq!(err.row, Some(3));
        }
        other => panic!("the large row must fail before it is sent: {other:?}"),
    }
    let summary = within(writer.finish()).await.expect("the writer finishes");
    assert_eq!(summary.rows_written, 3);
}
