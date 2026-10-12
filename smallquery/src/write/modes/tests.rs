//! Committed, pending and buffered write streams through the `bigquery` client and the raw
//! Storage Write API: each row once per offset, all of a batch commit or none of it, rows
//! readable up to the flushed offset, and the stream state a restarted or a second engine
//! finds on storage and in the table.

use crate::catalog::CommitPoint;
use crate::testkit::{table_ref, TestEngine, PROJECT};
use crate::write::tests::{create_ticks, declare_orders, orders, Order, Tick, ORDERS, SHOP, TICKS};
use bigquery::errors::BigQueryError;
use bigquery::*;
use gcloud_sdk::google::cloud::bigquery::storage::v1 as storage;
use gcloud_sdk::prost::Message;
use gcloud_sdk::prost_types::{field_descriptor_proto, DescriptorProto, FieldDescriptorProto};
use std::collections::BTreeSet;
use std::sync::atomic::Ordering;
use std::time::Duration;
use storage::append_rows_response::Response;
use storage::storage_error::StorageErrorCode;
use tonic::Code;

const TICKS_PATH: &str = "projects/lite-project/datasets/shop/tables/ticks";

fn options(mode: BigQueryWriteMode, rows_per_request: usize) -> BigQueryStreamingWriteOptions {
    BigQueryStreamingWriteOptions::new()
        .with_mode(mode)
        .with_max_batch_rows(rows_per_request)
}

/// The ticks `0..count` of `request`.
fn ticks(request: i64, count: i64) -> Vec<Tick> {
    (0..count).map(|n| Tick { request, n }).collect()
}

/// Every row of `shop.ticks` through Storage Read, sorted.
async fn read_ticks(engine: &TestEngine) -> Vec<Tick> {
    let mut read: Vec<Tick> = engine
        .db
        .fluent()
        .select()
        .from(SHOP.table(TICKS))
        .obj()
        .query()
        .await
        .expect("the table reads");
    read.sort_by_key(|tick| (tick.request, tick.n));
    read
}

async fn snapshot_count(engine: &TestEngine) -> usize {
    engine
        .state()
        .catalog
        .reload(&table_ref("shop", "ticks"))
        .await
        .expect("the table loads")
        .metadata()
        .snapshots()
        .count()
}

/// A writer of ticks on `engine` in `mode`, `rows_per_request` rows to a request.
async fn tick_writer(
    engine: &TestEngine,
    mode: BigQueryWriteMode,
    rows_per_request: usize,
) -> BigQueryStreamingWriter<Tick> {
    let (writer, _responses) = engine
        .db
        .create_streaming_writer_with_options::<Tick>(
            SHOP.table(TICKS),
            options(mode, rows_per_request),
        )
        .await
        .expect("the writer opens");
    writer
}

/// A stream of `shop.ticks` of `kind`, created through the raw API.
async fn create_stream(engine: &TestEngine, kind: storage::write_stream::Type) -> String {
    engine
        .write_client()
        .create_write_stream(storage::CreateWriteStreamRequest {
            parent: TICKS_PATH.to_string(),
            write_stream: Some(storage::WriteStream {
                r#type: kind.into(),
                ..Default::default()
            }),
        })
        .await
        .expect("the stream is created")
        .into_inner()
        .name
}

/// The proto descriptor of a tick row, with the extra string fields `extra` after its own.
fn tick_descriptor(extra: &[&str]) -> DescriptorProto {
    let field =
        |name: &str, number: i32, kind: field_descriptor_proto::Type| FieldDescriptorProto {
            name: Some(name.to_string()),
            number: Some(number),
            label: Some(field_descriptor_proto::Label::Optional.into()),
            r#type: Some(kind.into()),
            ..Default::default()
        };
    let mut fields = vec![
        field("request", 1, field_descriptor_proto::Type::Int64),
        field("n", 2, field_descriptor_proto::Type::Int64),
    ];
    for (number, name) in (3..).zip(extra) {
        fields.push(field(name, number, field_descriptor_proto::Type::String));
    }
    DescriptorProto {
        name: Some("Tick".to_string()),
        field: fields,
        ..Default::default()
    }
}

/// An append of `rows` to `stream` at `offset`, with the writer schema `descriptor`.
fn append_request(
    stream: &str,
    offset: Option<i64>,
    descriptor: DescriptorProto,
    rows: &[Tick],
) -> storage::AppendRowsRequest {
    let serialized_rows = rows
        .iter()
        .map(|tick| {
            let mut row = Vec::new();
            gcloud_sdk::prost::encoding::int64::encode(1, &tick.request, &mut row);
            gcloud_sdk::prost::encoding::int64::encode(2, &tick.n, &mut row);
            row
        })
        .collect();
    storage::AppendRowsRequest {
        write_stream: stream.to_string(),
        offset,
        rows: Some(storage::append_rows_request::Rows::ProtoRows(
            storage::append_rows_request::ProtoData {
                writer_schema: Some(storage::ProtoSchema {
                    proto_descriptor: Some(descriptor),
                }),
                rows: Some(storage::ProtoRows { serialized_rows }),
            },
        )),
        ..Default::default()
    }
}

/// The engine's answers to `requests`, sent on one connection.
async fn append(
    engine: &TestEngine,
    requests: Vec<storage::AppendRowsRequest>,
) -> Vec<storage::AppendRowsResponse> {
    let mut answers = engine
        .write_client()
        .append_rows(tokio_stream::iter(requests))
        .await
        .expect("the connection opens")
        .into_inner();
    let mut responses = Vec::new();
    while let Some(response) = answers.message().await.expect("the connection answers") {
        responses.push(response);
    }
    responses
}

/// The in-band error of `response`: its code and the `StorageError` code it carries.
fn in_band_error(response: &storage::AppendRowsResponse) -> (Code, Option<StorageErrorCode>) {
    let Some(Response::Error(status)) = &response.response else {
        panic!("an error, not {response:?}");
    };
    let storage = status
        .details
        .iter()
        .map(|any| storage::StorageError::decode(any.value.as_slice()).expect("a StorageError"))
        .map(|error| StorageErrorCode::try_from(error.code).expect("a known code"))
        .next();
    (Code::from(status.code), storage)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_exactly_once_insert_lands_every_request_once() {
    let engine = TestEngine::start().await;
    create_ticks(&engine).await;
    let written = ticks(1, 10);

    let summary = engine
        .db
        .fluent()
        .insert()
        .into(SHOP.table(TICKS))
        .objects(&written)
        .options(BigQueryStreamingWriteOptions::new().with_max_batch_rows(3))
        .exactly_once()
        .execute()
        .await
        .expect("the rows are written");

    assert_eq!(summary.rows_written, 10);
    assert_eq!(read_ticks(&engine).await, written);
    let table = table_ref("shop", "ticks");
    assert_eq!(engine.data_files(&table).len(), 4, "a file per request");
    engine.assert_no_path_twice(&table).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_sent_again_after_its_response_was_lost_lands_once() {
    let engine = TestEngine::start().await;
    create_ticks(&engine).await;
    engine
        .state()
        .writes
        .lose_response
        .store(true, Ordering::SeqCst);
    let written = ticks(1, 10);

    let summary = engine
        .db
        .fluent()
        .insert()
        .into(SHOP.table(TICKS))
        .objects(&written)
        .options(BigQueryStreamingWriteOptions::new().with_max_batch_rows(3))
        .exactly_once()
        .execute()
        .await
        .expect("the rows are written");

    assert!(
        !engine.state().writes.lose_response.load(Ordering::SeqCst),
        "a response was lost"
    );
    assert_eq!((summary.rows_written, summary.rows_failed), (10, 0));
    assert_eq!(read_ticks(&engine).await, written);
    let table = table_ref("shop", "ticks");
    assert_eq!(
        engine.data_files(&table).len(),
        4,
        "the request sent again wrote no file"
    );
    engine.assert_no_path_twice(&table).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_offset_already_written_or_past_the_end_is_refused_in_band() {
    let engine = TestEngine::start().await;
    create_ticks(&engine).await;
    let stream = create_stream(&engine, storage::write_stream::Type::Committed).await;
    let rows = ticks(1, 2);

    let responses = append(
        &engine,
        vec![
            append_request(&stream, Some(0), tick_descriptor(&[]), &rows),
            append_request(&stream, Some(0), tick_descriptor(&[]), &rows),
            append_request(&stream, Some(5), tick_descriptor(&[]), &rows),
        ],
    )
    .await;

    assert_eq!(
        responses[0].response,
        Some(Response::AppendResult(
            storage::append_rows_response::AppendResult { offset: Some(0) }
        ))
    );
    assert_eq!(
        in_band_error(&responses[1]),
        (
            Code::AlreadyExists,
            Some(StorageErrorCode::OffsetAlreadyExists)
        )
    );
    assert_eq!(
        in_band_error(&responses[2]),
        (Code::OutOfRange, Some(StorageErrorCode::OffsetOutOfRange))
    );
    assert_eq!(read_ticks(&engine).await, rows);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_change_batch_on_a_created_stream_is_an_invalid_argument() {
    let engine = TestEngine::start().await;
    create_ticks(&engine).await;
    let stream = create_stream(&engine, storage::write_stream::Type::Committed).await;

    let responses = append(
        &engine,
        vec![append_request(
            &stream,
            Some(0),
            tick_descriptor(&["_CHANGE_TYPE"]),
            &ticks(1, 1),
        )],
    )
    .await;

    assert_eq!(in_band_error(&responses[0]), (Code::InvalidArgument, None));
    assert_eq!(read_ticks(&engine).await, Vec::new());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn atomic_streams_show_their_rows_together_at_one_commit() {
    let engine = TestEngine::start().await;
    create_ticks(&engine).await;
    let inserted = engine
        .db
        .fluent()
        .insert()
        .into(SHOP.table(TICKS))
        .objects(&ticks(1, 5))
        .options(BigQueryStreamingWriteOptions::new().with_max_batch_rows(2))
        .atomic()
        .execute()
        .await
        .expect("the rows are written");
    assert_eq!(inserted.rows_written, 5);
    assert!(inserted.commit_time.is_some());
    assert_eq!(read_ticks(&engine).await, ticks(1, 5));

    let mut first = tick_writer(&engine, BigQueryWriteMode::Pending, 2).await;
    first
        .write_all(&ticks(2, 4))
        .await
        .expect("rows are queued");
    let first = first.finalize().await.expect("the stream is finalized");
    let mut second = tick_writer(&engine, BigQueryWriteMode::Pending, 4).await;
    second
        .write_all(&ticks(3, 6))
        .await
        .expect("rows are queued");
    let second = second.finalize().await.expect("the stream is finalized");
    assert_eq!((first.row_count, second.row_count), (4, 6));
    assert_eq!(
        read_ticks(&engine).await,
        ticks(1, 5),
        "an uncommitted stream shows no row"
    );

    let before = snapshot_count(&engine).await;
    engine
        .db
        .commit_write_streams(vec![first.clone(), second])
        .await
        .expect("the streams commit");
    assert_eq!(snapshot_count(&engine).await, before + 1);
    let expected: Vec<Tick> = [ticks(1, 5), ticks(2, 4), ticks(3, 6)].concat();
    assert_eq!(read_ticks(&engine).await, expected);

    let again = engine.db.commit_write_streams(vec![first]).await;
    assert!(
        matches!(&again, Err(BigQueryError::WriteStreamError(err))
            if err.public.code == "STREAM_ALREADY_COMMITTED"),
        "{again:?}"
    );
    assert_eq!(snapshot_count(&engine).await, before + 1);
    assert_eq!(read_ticks(&engine).await, expected);
    engine
        .assert_no_path_twice(&table_ref("shop", "ticks"))
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_flush_inside_a_request_makes_exactly_the_rows_to_its_offset_readable() {
    let engine = TestEngine::start().await;
    create_ticks(&engine).await;
    let inserted = engine
        .db
        .fluent()
        .insert()
        .into(SHOP.table(TICKS))
        .objects(&ticks(1, 7))
        .options(BigQueryStreamingWriteOptions::new().with_max_batch_rows(3))
        .buffered()
        .execute()
        .await
        .expect("the rows are written");
    assert_eq!(inserted.rows_written, 7);
    assert_eq!(read_ticks(&engine).await, ticks(1, 7));

    let mut writer = tick_writer(&engine, BigQueryWriteMode::Buffered, 4).await;
    writer
        .write_all(&ticks(2, 12))
        .await
        .expect("rows are queued");
    writer.flush().await.expect("every request is answered");
    assert_eq!(read_ticks(&engine).await, ticks(1, 7));

    assert_eq!(writer.flush_rows_to(5).await.expect("the rows flush"), 5);
    assert_eq!(
        read_ticks(&engine).await,
        [ticks(1, 7), ticks(2, 6)].concat()
    );
    let before = snapshot_count(&engine).await;
    assert_eq!(writer.flush_rows_to(5).await.expect("a flush again"), 5);
    assert_eq!(writer.flush_rows_to(2).await.expect("a lower flush"), 2);
    assert_eq!(
        snapshot_count(&engine).await,
        before,
        "nothing commits twice"
    );

    writer.finish().await.expect("the writer finishes");
    assert_eq!(
        read_ticks(&engine).await,
        [ticks(1, 7), ticks(2, 12)].concat()
    );
    engine
        .assert_no_path_twice(&table_ref("shop", "ticks"))
        .await;
}

/// The day directories of the files the current snapshot of `shop.orders` plans, sorted.
async fn planned_days(engine: &TestEngine) -> Vec<String> {
    let mut days: Vec<String> = engine
        .plan_files(&table_ref("shop", "orders"), None, None)
        .await
        .into_iter()
        .map(|task| {
            std::path::Path::new(&task.data_file_path)
                .parent()
                .and_then(std::path::Path::file_name)
                .expect("a file in a partition directory")
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    days.sort();
    days
}

async fn read_orders(engine: &TestEngine) -> Vec<Order> {
    let mut read: Vec<Order> = engine
        .db
        .fluent()
        .select()
        .from(SHOP.table(ORDERS))
        .obj()
        .query()
        .await
        .expect("the table reads");
    read.sort_by_key(|order| order.order_id);
    read
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_flush_inside_a_request_on_a_partitioned_table_keeps_each_row_in_its_day() {
    let engine = TestEngine::start().await;
    engine.create_dataset("shop", "EU").await;
    declare_orders(&engine.db, true)
        .sync()
        .await
        .expect("the table is created");
    let written = orders();
    let (mut writer, _responses) = engine
        .db
        .create_streaming_writer_with_options::<Order>(
            SHOP.table(ORDERS),
            options(BigQueryWriteMode::Buffered, 4),
        )
        .await
        .expect("the writer opens");
    writer.write_all(&written).await.expect("rows are queued");
    writer.flush().await.expect("the request is answered");

    assert_eq!(writer.flush_rows_to(1).await.expect("the rows flush"), 1);
    assert_eq!(read_orders(&engine).await, written[..2]);
    assert_eq!(
        planned_days(&engine).await,
        ["placed_at_day=2026-10-07", "placed_at_day=2026-10-08"]
    );

    writer.finish().await.expect("the writer finishes");
    assert_eq!(read_orders(&engine).await, written);
    assert_eq!(
        planned_days(&engine).await,
        [
            "placed_at_day=2026-10-07",
            "placed_at_day=2026-10-08",
            "placed_at_day=2026-10-08",
            "placed_at_day=2026-10-09",
        ]
    );
    engine
        .assert_no_path_twice(&table_ref("shop", "orders"))
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn eight_writers_of_every_mode_on_one_table_end_with_the_exact_sum() {
    let engine = TestEngine::start().await;
    create_ticks(&engine).await;
    let modes = [
        BigQueryWriteMode::Default,
        BigQueryWriteMode::Committed,
        BigQueryWriteMode::Pending,
        BigQueryWriteMode::Buffered,
    ];

    let mut writers = tokio::task::JoinSet::new();
    for (writer, mode) in (0..8).zip(modes.iter().cycle()) {
        let db = engine.db.clone();
        let mode = *mode;
        writers.spawn(async move {
            let (mut writer_of, _responses) = db
                .create_streaming_writer_with_options::<Tick>(SHOP.table(TICKS), options(mode, 10))
                .await?;
            writer_of.write_all(&ticks(writer, 50)).await?;
            writer_of.finish().await
        });
    }
    while let Some(finished) = writers.join_next().await {
        let summary = finished
            .expect("the writer runs")
            .expect("the writer finishes");
        assert_eq!((summary.rows_written, summary.rows_failed), (50, 0));
    }

    let read = read_ticks(&engine).await;
    assert_eq!(read.len(), 400);
    assert_eq!(read.iter().map(|tick| tick.n).sum::<i64>(), 8 * 1225);
    assert_eq!(
        read,
        (0..8)
            .flat_map(|writer| ticks(writer, 50))
            .collect::<Vec<_>>()
    );
    engine
        .assert_no_path_twice(&table_ref("shop", "ticks"))
        .await;
}

/// Appends `rows` to `stream` at `offset` on a connection of its own, and fails unless the
/// engine acknowledges them.
async fn acknowledged(engine: &TestEngine, stream: &str, offset: i64, rows: &[Tick]) {
    let responses = append(
        engine,
        vec![append_request(
            stream,
            Some(offset),
            tick_descriptor(&[]),
            rows,
        )],
    )
    .await;
    assert_eq!(
        responses[0].response,
        Some(Response::AppendResult(
            storage::append_rows_response::AppendResult {
                offset: Some(offset)
            }
        )),
        "the rows at offset {offset} are acknowledged"
    );
}

/// An engine on `engine`'s root whose stream TTL has passed for every stream already created,
/// once it has dropped them.
async fn sweeper_of(engine: &TestEngine) -> TestEngine {
    tokio::time::sleep(Duration::from_millis(5)).await;
    let sweeper = engine
        .open_on_same_root(|mut config| {
            config.stream_ttl = Duration::from_millis(1);
            config
        })
        .await;
    sweeper
        .state()
        .writes
        .sweep_streams()
        .await
        .expect("the expired streams are dropped");
    sweeper
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stream_another_engine_expired_reports_no_commit_on_its_creator() {
    let creator = TestEngine::start().await;
    create_ticks(&creator).await;
    let pending = create_stream(&creator, storage::write_stream::Type::Pending).await;
    let buffered = create_stream(&creator, storage::write_stream::Type::Buffered).await;
    for stream in [&pending, &buffered] {
        acknowledged(&creator, stream, 0, &ticks(1, 3)).await;
        acknowledged(&creator, stream, 3, &ticks(2, 3)).await;
    }
    let _sweeper = sweeper_of(&creator).await;

    let mut client = creator.write_client();
    let finalized = client
        .finalize_write_stream(storage::FinalizeWriteStreamRequest {
            name: pending.clone(),
        })
        .await;
    assert_eq!(
        finalized
            .expect_err("an expired stream finalizes nothing")
            .code(),
        Code::NotFound
    );
    let committed = client
        .batch_commit_write_streams(storage::BatchCommitWriteStreamsRequest {
            parent: TICKS_PATH.to_string(),
            write_streams: vec![pending],
        })
        .await
        .expect("the batch commit answers")
        .into_inner();
    assert_eq!(committed.commit_time, None);
    assert_eq!(
        committed
            .stream_errors
            .iter()
            .map(storage::StorageError::code)
            .collect::<Vec<_>>(),
        [StorageErrorCode::StreamNotFound]
    );
    let flushed = client
        .flush_rows(storage::FlushRowsRequest {
            write_stream: buffered,
            offset: Some(4),
        })
        .await;
    assert_eq!(
        flushed
            .expect_err("an expired stream flushes nothing")
            .code(),
        Code::NotFound
    );
    assert_eq!(read_ticks(&creator).await, Vec::new());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_flush_held_while_another_engine_expires_its_stream_lands_nothing() {
    let creator = TestEngine::start().await;
    create_ticks(&creator).await;
    let stream = create_stream(&creator, storage::write_stream::Type::Buffered).await;
    acknowledged(&creator, &stream, 0, &ticks(1, 3)).await;
    acknowledged(&creator, &stream, 3, &ticks(2, 3)).await;

    let mut held = creator.state().catalog.hooks().hold(CommitPoint::Loaded);
    let mut client = creator.write_client();
    let flush = tokio::spawn({
        let stream = stream.clone();
        async move {
            client
                .flush_rows(storage::FlushRowsRequest {
                    write_stream: stream,
                    offset: Some(4),
                })
                .await
        }
    });
    held.reached().await;
    let _sweeper = sweeper_of(&creator).await;
    held.release();

    let flushed = flush.await.expect("the flush runs");
    assert_eq!(
        flushed
            .expect_err("the flush lands nothing of an expired stream")
            .code(),
        Code::NotFound
    );
    assert_eq!(read_ticks(&creator).await, Vec::new());
    creator
        .assert_no_path_twice(&table_ref("shop", "ticks"))
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_engine_that_did_not_create_a_stream_acknowledges_none_of_its_rows() {
    let creator = TestEngine::start().await;
    create_ticks(&creator).await;
    let stream = create_stream(&creator, storage::write_stream::Type::Pending).await;
    acknowledged(&creator, &stream, 0, &ticks(1, 3)).await;
    let other = creator.open_on_same_root(|config| config).await;
    other
        .write_client()
        .get_write_stream(storage::GetWriteStreamRequest {
            name: stream.clone(),
            ..Default::default()
        })
        .await
        .expect("the other engine describes the stream");

    let mut client = creator.write_client();
    client
        .finalize_write_stream(storage::FinalizeWriteStreamRequest {
            name: stream.clone(),
        })
        .await
        .expect("the creator finalizes the stream");
    let committed = client
        .batch_commit_write_streams(storage::BatchCommitWriteStreamsRequest {
            parent: TICKS_PATH.to_string(),
            write_streams: vec![stream.clone()],
        })
        .await
        .expect("the batch commit answers")
        .into_inner();
    assert_eq!(committed.stream_errors, Vec::new());

    let late = append(
        &other,
        vec![append_request(
            &stream,
            Some(3),
            tick_descriptor(&[]),
            &ticks(2, 3),
        )],
    )
    .await;
    assert_eq!(
        in_band_error(&late[0]),
        (Code::NotFound, Some(StorageErrorCode::StreamNotFound))
    );
    assert_eq!(read_ticks(&other).await, ticks(1, 3));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_engine_reports_when_a_pending_stream_was_committed() {
    let engine = TestEngine::start().await;
    create_ticks(&engine).await;
    let stream = create_stream(&engine, storage::write_stream::Type::Pending).await;
    acknowledged(&engine, &stream, 0, &ticks(1, 3)).await;
    let mut client = engine.write_client();
    client
        .finalize_write_stream(storage::FinalizeWriteStreamRequest {
            name: stream.clone(),
        })
        .await
        .expect("the stream is finalized");
    let committed = client
        .batch_commit_write_streams(storage::BatchCommitWriteStreamsRequest {
            parent: TICKS_PATH.to_string(),
            write_streams: vec![stream.clone()],
        })
        .await
        .expect("the batch commit answers")
        .into_inner();
    let commit_time = committed.commit_time.expect("the commit reports its time");

    let restarted = engine.restart(|config| config).await;
    let described = restarted
        .write_client()
        .get_write_stream(storage::GetWriteStreamRequest {
            name: stream,
            ..Default::default()
        })
        .await
        .expect("the stream is described")
        .into_inner();
    assert_eq!(described.commit_time, Some(commit_time));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streams_that_keep_committing_outlive_their_ttl_from_creation() {
    let ttl = Duration::from_secs(2);
    let engine = TestEngine::with_config(|mut config| {
        config.stream_ttl = ttl;
        config
    })
    .await;
    create_ticks(&engine).await;
    let committed = create_stream(&engine, storage::write_stream::Type::Committed).await;
    let buffered = create_stream(&engine, storage::write_stream::Type::Buffered).await;
    let mut client = engine.write_client();
    for request in 0..3 {
        if request > 0 {
            tokio::time::sleep(ttl * 3 / 5).await;
        }
        if request == 2 {
            engine
                .state()
                .writes
                .sweep_streams()
                .await
                .expect("the expired streams are dropped");
        }
        for stream in [&committed, &buffered] {
            acknowledged(&engine, stream, request, &ticks(request, 1)).await;
        }
        client
            .flush_rows(storage::FlushRowsRequest {
                write_stream: buffered.clone(),
                offset: Some(request),
            })
            .await
            .expect("the buffered stream flushes");
    }
    assert_eq!(read_ticks(&engine).await.len(), 6);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn creating_a_stream_does_not_wait_for_the_drop_of_expired_ones() {
    let creator = TestEngine::start().await;
    create_ticks(&creator).await;
    let sweeper = creator
        .open_on_same_root(|mut config| {
            config.stream_ttl = Duration::from_millis(1);
            config
        })
        .await;
    let expired = create_stream(&creator, storage::write_stream::Type::Pending).await;
    acknowledged(&creator, &expired, 0, &ticks(1, 3)).await;
    tokio::time::sleep(Duration::from_millis(5)).await;

    let mut held = sweeper.state().catalog.hooks().hold(CommitPoint::Loaded);
    let created = tokio::time::timeout(
        Duration::from_secs(10),
        create_stream(&sweeper, storage::write_stream::Type::Committed),
    )
    .await;
    assert!(created.is_ok(), "the stream is created while a drop waits");
    held.reached().await;
    held.release();
    sweeper
        .state()
        .writes
        .sweep_streams()
        .await
        .expect("the expired streams are dropped");
    assert_eq!(creator.stream_entries(), Vec::<std::path::PathBuf>::new());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn another_engine_commits_a_pending_stream_and_the_first_cannot_commit_it_again() {
    let first = TestEngine::start().await;
    create_ticks(&first).await;
    let mut writer = tick_writer(&first, BigQueryWriteMode::Pending, 3).await;
    writer
        .write_all(&ticks(1, 6))
        .await
        .expect("rows are queued");
    let finalized = writer.finalize().await.expect("the stream is finalized");

    let second = first.open_on_same_root(|config| config).await;
    second
        .db
        .commit_write_streams(vec![finalized.clone()])
        .await
        .expect("the second engine commits the stream");
    assert_eq!(read_ticks(&second).await, ticks(1, 6));

    let again = first.db.commit_write_streams(vec![finalized]).await;
    assert!(
        matches!(&again, Err(BigQueryError::WriteStreamError(err))
            if err.public.code == "STREAM_ALREADY_COMMITTED"),
        "{again:?}"
    );
    assert_eq!(read_ticks(&first).await, ticks(1, 6));
    second
        .assert_no_path_twice(&table_ref("shop", "ticks"))
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batch_commit_that_dies_before_its_pointer_changes_nothing_and_its_files_expire() {
    let engine = TestEngine::start().await;
    create_ticks(&engine).await;
    let table = table_ref("shop", "ticks");
    let mut writer = tick_writer(&engine, BigQueryWriteMode::Pending, 3).await;
    writer
        .write_all(&ticks(1, 6))
        .await
        .expect("rows are queued");
    let finalized = writer.finalize().await.expect("the stream is finalized");
    assert_eq!(
        engine.data_files(&table).len(),
        2,
        "a staged file per request"
    );
    let version = |engine: &TestEngine| {
        let catalog = engine.state().catalog.clone();
        let table = table.clone();
        async move {
            catalog
                .reload(&table)
                .await
                .expect("the table loads")
                .version()
        }
    };
    let before = version(&engine).await;

    engine.state().catalog.hooks().panic_at(CommitPoint::Loaded);
    let died = engine
        .write_client()
        .batch_commit_write_streams(storage::BatchCommitWriteStreamsRequest {
            parent: TICKS_PATH.to_string(),
            write_streams: vec![finalized.name.to_string()],
        })
        .await;
    assert_eq!(
        died.expect_err("the commit died").code(),
        Code::Internal,
        "the commit task ended without an outcome"
    );

    let restarted = engine.restart(|config| config).await;
    assert_eq!(version(&restarted).await, before);
    assert_eq!(read_ticks(&restarted).await, Vec::new());
    assert_eq!(
        restarted.data_files(&table).len(),
        2,
        "a stream within its TTL keeps its files"
    );

    let expired = restarted
        .restart(|mut config| {
            config.stream_ttl = Duration::from_millis(1);
            config
        })
        .await;
    expired
        .state()
        .writes
        .sweep_streams()
        .await
        .expect("the expired streams are dropped");
    assert_eq!(read_ticks(&expired).await, Vec::new());
    assert_eq!(expired.data_files(&table), BTreeSet::new());
    assert_eq!(expired.stream_entries(), Vec::<std::path::PathBuf>::new());
    let loaded = expired
        .state()
        .catalog
        .reload(&table)
        .await
        .expect("the table loads");
    assert!(
        !loaded
            .metadata()
            .properties()
            .keys()
            .any(|key| key.starts_with("smallquery.stream.")),
        "the stream's state left the table"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_flush_that_dies_after_its_pointer_leaves_its_rows_to_the_drop_of_its_stream() {
    let engine = TestEngine::start().await;
    create_ticks(&engine).await;
    let table = table_ref("shop", "ticks");
    let mut writer = tick_writer(&engine, BigQueryWriteMode::Buffered, 4).await;
    writer
        .write_all(&ticks(1, 12))
        .await
        .expect("rows are queued");
    writer.flush().await.expect("every request is answered");
    let stream = writer
        .stream_name()
        .expect("a buffered writer has a stream")
        .to_string();

    engine
        .state()
        .catalog
        .hooks()
        .panic_at(CommitPoint::Created);
    let died = engine
        .write_client()
        .flush_rows(storage::FlushRowsRequest {
            write_stream: stream.clone(),
            offset: Some(5),
        })
        .await;
    assert_eq!(
        died.expect_err("the flush died").code(),
        Code::Internal,
        "the commit task ended after its pointer, before the flush cleaned up"
    );
    drop(writer);

    let restarted = engine.restart(|config| config).await;
    assert_eq!(
        read_ticks(&restarted).await,
        ticks(1, 6),
        "the flush landed"
    );
    let refused = restarted
        .write_client()
        .flush_rows(storage::FlushRowsRequest {
            write_stream: stream,
            offset: Some(11),
        })
        .await;
    assert_eq!(
        refused.expect_err("only the creator flushes").code(),
        Code::NotFound
    );

    let expired = restarted
        .restart(|mut config| {
            config.stream_ttl = Duration::from_millis(1);
            config
        })
        .await;
    expired
        .state()
        .writes
        .sweep_streams()
        .await
        .expect("the expired streams are dropped");
    assert_eq!(read_ticks(&expired).await, ticks(1, 6));
    assert_eq!(
        expired.data_files(&table).len(),
        2,
        "the committed files stay, and the split original and the unflushed files go"
    );
    assert_eq!(expired.stream_entries(), Vec::<std::path::PathBuf>::new());
    expired.assert_no_path_twice(&table).await;
}

/// The pointer versions under `shop.ticks`, each with the metadata location it names and the
/// one its predecessor named.
fn pointers(engine: &TestEngine) -> Vec<(u64, String, Option<String>)> {
    let directory = engine
        .root()
        .join("_smallquery")
        .join("catalog")
        .join(PROJECT)
        .join("shop")
        .join("ticks");
    let mut pointers: Vec<(u64, String, Option<String>)> = std::fs::read_dir(directory)
        .expect("the pointers list")
        .map(|entry| {
            let path = entry.expect("the directory lists").path();
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .expect("a UTF-8 name");
            let version = name
                .strip_prefix('v')
                .and_then(|digits| digits.parse().ok())
                .expect("a pointer name");
            let body: serde_json::Value =
                serde_json::from_slice(&std::fs::read(&path).expect("the pointer reads"))
                    .expect("a pointer parses");
            let location = body["metadata_location"]
                .as_str()
                .expect("a live pointer")
                .to_string();
            let previous = body["previous"].as_str().map(str::to_string);
            (version, location, previous)
        })
        .collect();
    pointers.sort();
    pointers
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn two_engines_append_a_thousand_requests_each_and_create_every_version_once() {
    let first = TestEngine::start().await;
    create_ticks(&first).await;
    let second = first.open_on_same_root(|config| config).await;

    let mut writers = tokio::task::JoinSet::new();
    for (engine, db) in [(0, first.db.clone()), (1, second.db.clone())] {
        writers.spawn(async move {
            let (mut writer, _responses) = db
                .create_streaming_writer_with_options::<Tick>(
                    SHOP.table(TICKS),
                    options(BigQueryWriteMode::Committed, 1),
                )
                .await?;
            writer.write_all(&ticks(engine, 1000)).await?;
            writer.finish().await
        });
    }
    while let Some(finished) = writers.join_next().await {
        let summary = finished
            .expect("the writer runs")
            .expect("the writer finishes");
        assert_eq!((summary.rows_written, summary.rows_failed), (1000, 0));
    }

    let pointers = pointers(&first);
    assert_eq!(
        pointers
            .iter()
            .map(|(version, ..)| *version)
            .collect::<Vec<_>>(),
        (0..=2000).collect::<Vec<u64>>()
    );
    for (below, (version, _, previous)) in pointers.iter().zip(&pointers[1..]) {
        assert_eq!(
            previous.as_ref(),
            Some(&below.1),
            "v{version} was created on its predecessor"
        );
    }
    let table = table_ref("shop", "ticks");
    assert_eq!(
        first.data_files(&table).len(),
        2000,
        "no request was answered with an error the client sends again"
    );
    assert_eq!(
        read_ticks(&first).await,
        [ticks(0, 1000), ticks(1, 1000)].concat()
    );
    first.assert_no_path_twice(&table).await;
}
