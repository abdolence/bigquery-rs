use crate::db::fake::read::{requested_compression, FakeReadTable};
use crate::db::fake::spans::{bigquery_fields, CapturedSpans};
use crate::db::fake::{FakeBigQuery, FakeCall};
use crate::errors::{BigQueryCodecErrorKind, BigQueryError};
use crate::BigQueryResult;
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::StreamExt;
use gcloud_sdk::google::cloud::bigquery::storage::v1::arrow_serialization_options::CompressionCodec;
use gcloud_sdk::google::cloud::bigquery::storage::v1::{
    read_rows_response, read_session, ArrowRecordBatch, ArrowSchema, ReadRowsResponse, ReadSession,
    ReadStream, ThrottleState,
};
use gcloud_sdk::tonic::Code;
use serde::Deserialize;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::db::fake::{ORDERS, SHOP};

/// `id, name, extra`, with `ids` as the ids.
fn people(ids: &[i64]) -> RecordBatch {
    let names: Vec<String> = ids.iter().map(|i| format!("Åsa {i}")).collect();
    let extras: Vec<String> = ids.iter().map(|i| format!("x{i}")).collect();
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("extra", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef,
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(extras)),
        ],
    )
    .expect("a valid batch")
}

#[derive(Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Person {
    id: i64,
    name: String,
    nickname: Option<String>,
}

fn person(id: i64) -> Person {
    Person {
        id,
        name: format!("Åsa {id}"),
        nickname: None,
    }
}

/// A fake serving `table` on every call: `GetTable`, `CreateReadSession` and `ReadRows`
/// read from the start of each stream to its end.
async fn serve(table: FakeReadTable) -> FakeBigQuery {
    let table = Arc::new(table);
    FakeBigQuery::start(move |mut call: FakeCall| {
        let table = table.clone();
        async move {
            match call.method() {
                "GetTable" => call.get_table(&table).await,
                "CreateReadSession" => {
                    let request = call.session_request().await;
                    call.open_session(&request, &table);
                }
                "ReadRows" => {
                    let request = call.read_rows_request().await;
                    let (_, streams) = table.encode(CompressionCodec::Lz4Frame);
                    let index: usize = request.read_stream[1..].parse().expect("s<n>");
                    let batches: Vec<(Vec<u8>, i64)> = streams[index]
                        .iter()
                        .cloned()
                        .zip(table.streams[index].iter().map(|b| b.num_rows() as i64))
                        .collect();
                    call.send_batches(&batches);
                    call.finish();
                }
                other => panic!("unexpected call {other}"),
            }
        }
    })
    .await
}

async fn collect_with_errors(fake: &FakeBigQuery) -> BigQueryResult<Vec<BigQueryResult<Person>>> {
    let stream = fake
        .db
        .fluent()
        .select()
        .from(SHOP.table(ORDERS))
        .obj::<Person>()
        .stream_query_with_errors()
        .await?;
    Ok(
        tokio::time::timeout(Duration::from_secs(20), stream.collect::<Vec<_>>())
            .await
            .expect("the merged stream ends"),
    )
}

#[tokio::test]
async fn projection_is_derived_from_struct_fields_and_table_columns() -> BigQueryResult<()> {
    let fake = serve(FakeReadTable::new(vec![vec![people(&[1, 2])]])).await;
    let rows = fake
        .db
        .fluent()
        .select()
        .from(SHOP.table(ORDERS))
        .obj::<Person>()
        .query()
        .await?;
    assert_eq!(rows, [person(1), person(2)]);
    assert_eq!(
        fake.calls(),
        [
            "GetTable shop.orders",
            "CreateReadSession [id,name]",
            "ReadRows s0 at 0"
        ]
    );
    Ok(())
}

#[tokio::test]
async fn projection_falls_back_when_selected_fields_lag() -> BigQueryResult<()> {
    let table = Arc::new(FakeReadTable::new(vec![vec![people(&[1])]]));
    let fake = FakeBigQuery::start(move |mut call: FakeCall| {
        let table = table.clone();
        async move {
            match call.method() {
                "GetTable" => call.get_table(&table).await,
                "CreateReadSession" => {
                    let request = call.session_request().await;
                    let selected = &request
                        .read_session
                        .as_ref()
                        .and_then(|s| s.read_options.as_ref())
                        .map(|o| o.selected_fields.clone())
                        .unwrap_or_default();
                    if selected.is_empty() {
                        call.open_session(&request, &table);
                    } else {
                        call.fail(
                            Code::InvalidArgument,
                            "request failed: The following selected fields do not exist \
                             in the table schema: name",
                        );
                    }
                }
                "ReadRows" => {
                    call.read_rows_request().await;
                    let (_, streams) = table.encode(CompressionCodec::Lz4Frame);
                    call.send_batches(&[(streams[0][0].clone(), 1)]);
                    call.finish();
                }
                other => panic!("unexpected call {other}"),
            }
        }
    })
    .await;
    let rows = fake
        .db
        .fluent()
        .select()
        .from(SHOP.table(ORDERS))
        .obj::<Person>()
        .query()
        .await?;
    assert_eq!(rows, [person(1)]);
    assert_eq!(
        fake.calls(),
        [
            "GetTable shop.orders",
            "CreateReadSession [id,name]",
            "CreateReadSession []",
            "ReadRows s0 at 0"
        ]
    );
    Ok(())
}

#[tokio::test]
async fn explicit_fields_error_is_schema_mismatch() {
    let fake = FakeBigQuery::start(|mut call: FakeCall| async move {
        call.session_request().await;
        call.fail(
            Code::InvalidArgument,
            "request failed: The following selected fields do not exist in the table \
             schema: nope",
        );
    })
    .await;
    let result = fake
        .db
        .fluent()
        .select()
        .fields(["id", "nope"])
        .from(SHOP.table(ORDERS))
        .obj::<Person>()
        .query()
        .await;
    match result {
        Err(BigQueryError::SchemaMismatchError(err)) => {
            assert_eq!(err.table, SHOP.table(ORDERS));
            assert!(err.details.contains("nope"), "{}", err.details);
        }
        other => panic!("expected a schema mismatch, got {other:?}"),
    }
    assert_eq!(fake.calls(), ["CreateReadSession [id,nope]"]);
}

#[tokio::test]
async fn stream_resumes_at_row_offset_after_retryable_error() -> BigQueryResult<()> {
    let table = Arc::new(FakeReadTable::new(vec![vec![
        people(&[1, 2]),
        people(&[3]),
    ]]));
    let attempts = Arc::new(AtomicUsize::new(0));
    let fake = FakeBigQuery::start(move |mut call: FakeCall| {
        let (table, attempts) = (table.clone(), attempts.clone());
        async move {
            match call.method() {
                "GetTable" => call.get_table(&table).await,
                "CreateReadSession" => {
                    let request = call.session_request().await;
                    call.open_session(&request, &table);
                }
                "ReadRows" => {
                    let request = call.read_rows_request().await;
                    let (_, streams) = table.encode(CompressionCodec::Lz4Frame);
                    if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                        call.send_batches(&[(streams[0][0].clone(), 2)]);
                        call.fail(Code::Unavailable, "backend went away");
                    } else {
                        assert_eq!(request.offset, 2);
                        call.send_batches(&[(streams[0][1].clone(), 1)]);
                        call.finish();
                    }
                }
                other => panic!("unexpected call {other}"),
            }
        }
    })
    .await;
    let rows: Vec<Person> = collect_with_errors(&fake)
        .await?
        .into_iter()
        .collect::<BigQueryResult<_>>()?;
    assert_eq!(rows, [person(1), person(2), person(3)]);
    assert_eq!(
        fake.calls(),
        [
            "GetTable shop.orders",
            "CreateReadSession [id,name]",
            "ReadRows s0 at 0",
            "ReadRows s0 at 2"
        ]
    );
    Ok(())
}

#[tokio::test]
async fn stream_resumes_at_row_offset_after_lost_connection() -> BigQueryResult<()> {
    let table = Arc::new(FakeReadTable::new(vec![vec![
        people(&[1, 2]),
        people(&[3]),
    ]]));
    let attempts = Arc::new(AtomicUsize::new(0));
    let fake = FakeBigQuery::start(move |mut call: FakeCall| {
        let (table, attempts) = (table.clone(), attempts.clone());
        async move {
            match call.method() {
                "GetTable" => call.get_table(&table).await,
                "CreateReadSession" => {
                    let request = call.session_request().await;
                    call.open_session(&request, &table);
                }
                "ReadRows" => {
                    let request = call.read_rows_request().await;
                    let (_, streams) = table.encode(CompressionCodec::Lz4Frame);
                    if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                        call.send_batches(&[(streams[0][0].clone(), 2)]);
                        // Lets the batch reach the client before the connection closes.
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        call.drop_connection().await;
                    } else {
                        assert_eq!(request.offset, 2);
                        call.send_batches(&[(streams[0][1].clone(), 1)]);
                        call.finish();
                    }
                }
                other => panic!("unexpected call {other}"),
            }
        }
    })
    .await;
    let rows: Vec<Person> = collect_with_errors(&fake)
        .await?
        .into_iter()
        .collect::<BigQueryResult<_>>()?;
    assert_eq!(rows, [person(1), person(2), person(3)]);
    Ok(())
}

/// Stream `s0` sends one batch and fails with `code` and `message`; `s1` sends one batch
/// and then never ends.
async fn failing_stream(code: Code, message: &'static str) -> FakeBigQuery {
    let table = Arc::new(FakeReadTable::new(vec![
        vec![people(&[1])],
        vec![people(&[2])],
    ]));
    FakeBigQuery::start_with_max_retries(2, move |mut call: FakeCall| {
        let table = table.clone();
        async move {
            match call.method() {
                "GetTable" => call.get_table(&table).await,
                "CreateReadSession" => {
                    let request = call.session_request().await;
                    call.open_session(&request, &table);
                }
                "ReadRows" => {
                    let request = call.read_rows_request().await;
                    let (_, streams) = table.encode(CompressionCodec::Lz4Frame);
                    if request.read_stream == "s0" {
                        call.send_batches(&[(streams[0][0].clone(), 1)]);
                        call.fail(code, message);
                    } else {
                        call.send_batches(&[(streams[1][0].clone(), 1)]);
                        call.hang().await;
                    }
                }
                other => panic!("unexpected call {other}"),
            }
        }
    })
    .await
}

#[tokio::test]
async fn non_retryable_stream_error_ends_the_merged_stream() -> BigQueryResult<()> {
    let fake = failing_stream(Code::PermissionDenied, "no access to the table").await;
    let items = collect_with_errors(&fake).await?;
    let (last, rows) = items.split_last().expect("at least the error");
    assert!(
        matches!(last, Err(BigQueryError::DatabaseError(e)) if e.public.code == "PermissionDenied"),
        "{last:?}"
    );
    assert!(rows.iter().all(Result::is_ok), "{rows:?}");
    let s0_reads = fake
        .calls()
        .iter()
        .filter(|c| *c == "ReadRows s0 at 0")
        .count();
    assert_eq!(s0_reads, 1);
    Ok(())
}

#[tokio::test]
async fn interval_overflow_is_not_retried() -> BigQueryResult<()> {
    let fake = failing_stream(
        Code::Internal,
        "Operation was attempted past the valid range. Out of range conversion for \
         microseconds value: 316224000000000000 to nanoseconds",
    )
    .await;
    let items = collect_with_errors(&fake).await?;
    match items.last() {
        Some(Err(BigQueryError::DatabaseError(e))) => {
            assert!(!e.retry_possible);
        }
        other => panic!("expected the overflow error last, got {other:?}"),
    }
    let s0_reads = fake
        .calls()
        .iter()
        .filter(|c| c.starts_with("ReadRows s0"))
        .count();
    assert_eq!(s0_reads, 1);
    Ok(())
}

/// A stream whose middle row has a NULL `name`, which `Person` cannot hold.
async fn null_name_table() -> FakeBigQuery {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("Åsa 1"), None, Some("Åsa 3")])),
        ],
    )
    .expect("a valid batch");
    serve(FakeReadTable::new(vec![vec![batch]])).await
}

#[tokio::test]
async fn row_decode_error_does_not_end_the_stream() -> BigQueryResult<()> {
    let fake = null_name_table().await;
    let items = collect_with_errors(&fake).await?;
    assert_eq!(items.len(), 3, "{items:?}");
    assert_eq!(items[0].as_ref().ok(), Some(&person(1)));
    match &items[1] {
        Err(BigQueryError::DeserializeError(e)) => {
            assert_eq!(
                (e.row, e.path.as_str(), e.kind),
                (Some(1), "name", BigQueryCodecErrorKind::NullForNonOption)
            );
        }
        other => panic!("expected the NULL row to fail, got {other:?}"),
    }
    assert_eq!(items[2].as_ref().ok(), Some(&person(3)));
    Ok(())
}

#[tokio::test]
async fn base_variant_skips_errors() -> BigQueryResult<()> {
    let fake = null_name_table().await;
    let rows: Vec<Person> = fake
        .db
        .fluent()
        .select()
        .from(SHOP.table(ORDERS))
        .obj::<Person>()
        .stream_query()
        .await?
        .collect()
        .await;
    assert_eq!(rows, [person(1), person(3)]);
    Ok(())
}

#[tokio::test]
async fn streams_merge_into_one() -> BigQueryResult<()> {
    let fake = serve(FakeReadTable::new(vec![
        vec![people(&[1, 2]), people(&[3])],
        vec![people(&[4])],
        vec![people(&[5, 6])],
    ]))
    .await;
    let rows: BTreeSet<Person> = collect_with_errors(&fake)
        .await?
        .into_iter()
        .collect::<BigQueryResult<_>>()?;
    assert_eq!(rows, (1..=6).map(person).collect::<BTreeSet<_>>());

    let batches: Vec<RecordBatch> = fake
        .db
        .fluent()
        .select()
        .from(SHOP.table(ORDERS))
        .record_batches()
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<BigQueryResult<_>>()?;
    let mut ids: Vec<i64> = batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id is INT64")
                .values()
                .to_vec()
        })
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, [1, 2, 3, 4, 5, 6]);
    assert_eq!(
        batches[0].num_columns(),
        3,
        "record batches read every column"
    );
    Ok(())
}

fn is_unexpected_response(err: &BigQueryError) -> bool {
    matches!(err, BigQueryError::SystemError(e) if e.public.code == "UNEXPECTED_RESPONSE")
}

#[tokio::test]
async fn a_sample_percentage_is_left_for_bigquery_to_check() {
    let fake = FakeBigQuery::start(|mut call: FakeCall| async move {
        let request = call.session_request().await;
        let sample = request
            .read_session
            .and_then(|s| s.read_options)
            .and_then(|o| o.sample_percentage);
        assert_eq!(sample, Some(150.0));
        call.fail(
            Code::InvalidArgument,
            "sample_percentage must be in (0, 100]",
        );
    })
    .await;
    let result = fake
        .db
        .fluent()
        .select()
        .from(SHOP.table(ORDERS))
        .sample_percentage(150.0)
        .record_batches()
        .await;
    match result {
        Err(err) => assert!(err.has_code(Code::InvalidArgument), "{err:?}"),
        Ok(_) => panic!("BigQuery refused the session"),
    }
    assert_eq!(fake.calls(), ["CreateReadSession []"]);
}

#[tokio::test]
async fn an_avro_session_is_an_unexpected_response() {
    let fake = FakeBigQuery::start(|mut call: FakeCall| async move {
        call.session_request().await;
        call.reply(&ReadSession {
            schema: Some(read_session::Schema::AvroSchema(Default::default())),
            ..Default::default()
        });
    })
    .await;
    let result = fake
        .db
        .fluent()
        .select()
        .from(SHOP.table(ORDERS))
        .record_batches()
        .await;
    match result {
        Err(err) => assert!(is_unexpected_response(&err), "{err:?}"),
        Ok(_) => panic!("an Avro session must be refused"),
    }
}

#[tokio::test]
async fn avro_rows_are_an_unexpected_response() -> BigQueryResult<()> {
    let table = Arc::new(FakeReadTable::new(vec![vec![people(&[1])]]));
    let fake = FakeBigQuery::start(move |mut call: FakeCall| {
        let table = table.clone();
        async move {
            match call.method() {
                "CreateReadSession" => {
                    let request = call.session_request().await;
                    call.open_session(&request, &table);
                }
                "ReadRows" => {
                    call.read_rows_request().await;
                    call.send(&ReadRowsResponse {
                        row_count: 1,
                        rows: Some(read_rows_response::Rows::AvroRows(Default::default())),
                        ..Default::default()
                    });
                    call.finish();
                }
                other => panic!("unexpected call {other}"),
            }
        }
    })
    .await;
    let items = fake
        .db
        .fluent()
        .select()
        .from(SHOP.table(ORDERS))
        .record_batches()
        .await?
        .collect::<Vec<_>>()
        .await;
    let err = items
        .into_iter()
        .find_map(Result::err)
        .expect("Avro rows must end the stream with an error");
    assert!(is_unexpected_response(&err), "{err:?}");
    Ok(())
}

#[tokio::test]
async fn empty_session_is_an_empty_stream() -> BigQueryResult<()> {
    let fake = FakeBigQuery::start(|mut call: FakeCall| async move {
        match call.method() {
            "CreateReadSession" => {
                call.session_request().await;
                call.reply(&ReadSession::default());
            }
            other => panic!("unexpected call {other}"),
        }
    })
    .await;
    let batches = fake
        .db
        .fluent()
        .select()
        .from(SHOP.table(ORDERS))
        .record_batches()
        .await?
        .collect::<Vec<_>>()
        .await;
    assert!(batches.is_empty(), "{batches:?}");
    assert_eq!(fake.calls(), ["CreateReadSession []"]);
    Ok(())
}

/// A fake whose session estimates 4096 bytes and 3 rows over two streams, `s0` sending
/// `[1, 2]` then `[3]` at 10% throttling and `s1` sending `[4]` at 25%, each response
/// reporting 100 uncompressed bytes per row.
async fn serve_with_figures() -> FakeBigQuery {
    let table = Arc::new(FakeReadTable::new(vec![
        vec![people(&[1, 2]), people(&[3])],
        vec![people(&[4])],
    ]));
    FakeBigQuery::start(move |mut call: FakeCall| {
        let table = table.clone();
        async move {
            match call.method() {
                "CreateReadSession" => {
                    let request = call.session_request().await;
                    let (schema, _) = table.encode(requested_compression(&request));
                    call.reply(&ReadSession {
                        name: "session".into(),
                        streams: ["s0", "s1"]
                            .map(|name| ReadStream { name: name.into() })
                            .to_vec(),
                        schema: Some(read_session::Schema::ArrowSchema(ArrowSchema {
                            serialized_schema: schema,
                        })),
                        estimated_total_bytes_scanned: 4096,
                        estimated_row_count: 3,
                        ..Default::default()
                    });
                }
                "ReadRows" => {
                    let request = call.read_rows_request().await;
                    let index: usize = request.read_stream[1..].parse().expect("s<n>");
                    let (_, streams) = table.encode(CompressionCodec::CompressionUnspecified);
                    for (bytes, batch) in streams[index].iter().zip(&table.streams[index]) {
                        let rows = batch.num_rows() as i64;
                        call.send(&ReadRowsResponse {
                            row_count: rows,
                            uncompressed_byte_size: Some(100 * rows),
                            throttle_state: Some(ThrottleState {
                                throttle_percent: [10, 25][index],
                            }),
                            rows: Some(read_rows_response::Rows::ArrowRecordBatch(
                                ArrowRecordBatch {
                                    serialized_record_batch: bytes.clone(),
                                    ..Default::default()
                                },
                            )),
                            ..Default::default()
                        });
                    }
                    call.finish();
                }
                other => panic!("unexpected call {other}"),
            }
        }
    })
    .await
}

#[tokio::test]
async fn read_span_records_the_session_estimates_and_what_was_read() -> BigQueryResult<()> {
    let (spans, _guard) = CapturedSpans::capture();
    let fake = serve_with_figures().await;
    let batches = fake
        .db
        .fluent()
        .select()
        .from(SHOP.table(ORDERS))
        .record_batches()
        .await?;
    let batches: Vec<RecordBatch> =
        tokio::time::timeout(Duration::from_secs(20), batches.collect::<Vec<_>>())
            .await
            .expect("the merged stream ends")
            .into_iter()
            .collect::<BigQueryResult<_>>()?;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 4);
    assert_eq!(
        spans.only("BigQuery Read"),
        bigquery_fields(&[
            ("table", "shop.orders"),
            ("streams", "2"),
            ("estimated_bytes_scanned", "4096"),
            ("estimated_rows", "3"),
            ("rows_read", "4"),
            ("bytes_read", "400"),
            ("throttle_percent", "25"),
        ])
    );
    Ok(())
}

#[tokio::test]
async fn read_dropped_early_records_what_it_read() -> BigQueryResult<()> {
    let (spans, _guard) = CapturedSpans::capture();
    let fake = serve_with_figures().await;
    let mut batches = fake
        .db
        .fluent()
        .select()
        .from(SHOP.table(ORDERS))
        .record_batches()
        .await?;
    let first = tokio::time::timeout(Duration::from_secs(20), batches.next())
        .await
        .expect("a first batch arrives")
        .expect("the stream has a batch")?;
    drop(batches);
    let fields = spans.only("BigQuery Read");
    let rows_read: usize = fields
        .get("/bigquery/rows_read")
        .expect("rows_read is recorded on drop")
        .parse()
        .expect("rows_read is a count");
    assert!(
        (first.num_rows()..=4).contains(&rows_read),
        "{rows_read} rows read after a first batch of {}",
        first.num_rows()
    );
    Ok(())
}
