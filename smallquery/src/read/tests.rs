//! Storage Read through the `bigquery` client and gcloud-sdk's generated read client: resumed
//! streams, compressed IPC, typed reads and row restrictions.

use super::StreamName;
use crate::testkit::{table_ref, TestEngine, PROJECT};
use arrow::array::{AsArray, RecordBatch};
use arrow::buffer::Buffer;
use arrow::datatypes::Int64Type;
use arrow::ipc::reader::StreamDecoder;
use bigquery::errors::BigQueryError;
use bigquery::*;
use gcloud_sdk::google::cloud::bigquery::storage::v1 as storage;
use serde::{Deserialize, Serialize};
use tonic::Code;
use uuid::Uuid;

const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
const EVENTS: BigQueryTableId = BigQueryTableId::from_static("events");

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Event {
    id: i64,
    kind: String,
    at: BigQueryTimestamp,
    detail: Option<serde_json::Value>,
}

/// `shop.events`, written in `requests` requests of `rows` rows each, so that it holds one data
/// file per request.
async fn events(engine: &TestEngine, requests: i64, rows: i64) -> Vec<Event> {
    engine.create_dataset("shop", "EU").await;
    engine
        .db
        .fluent()
        .schema()
        .table(SHOP.table(EVENTS))
        .columns(|columns| {
            columns.fields([
                columns.field("id").int64().required(),
                columns.field("kind").string().required(),
                columns.field("at").timestamp().required(),
                columns.field("detail").json(),
            ])
        })
        .sync()
        .await
        .expect("the table is created");
    let mut written = Vec::new();
    for request in 0..requests {
        let batch: Vec<Event> = (0..rows)
            .map(|n| {
                let id = request * rows + n;
                Event {
                    id,
                    kind: ["click", "view", "buy"][usize::try_from(id % 3).expect("fits")]
                        .to_string(),
                    at: BigQueryTimestamp(
                        jiff::Timestamp::from_second(1_791_000_000 + id).expect("in range"),
                    ),
                    detail: (id % 2 == 0).then(|| serde_json::json!({"id": id})),
                }
            })
            .collect();
        engine
            .db
            .fluent()
            .insert()
            .into(SHOP.table(EVENTS))
            .objects(&batch)
            .execute()
            .await
            .expect("the rows are written");
        written.extend(batch);
    }
    written
}

/// The IDs a raw `ReadRows` of `stream` from `offset` returns, decoded with the session's
/// schema message.
async fn read_ids(engine: &TestEngine, schema: &[u8], stream: &str, offset: i64) -> Vec<i64> {
    let mut responses = engine
        .db
        .read_client()
        .read_rows(storage::ReadRowsRequest {
            read_stream: stream.to_string(),
            offset,
            ..Default::default()
        })
        .await
        .expect("the stream opens")
        .into_inner();
    let mut decoder = StreamDecoder::new();
    let mut schema = Buffer::from(schema.to_vec());
    while !schema.is_empty() {
        assert!(decoder
            .decode(&mut schema)
            .expect("the schema decodes")
            .is_none());
    }
    let mut ids = Vec::new();
    while let Some(response) = responses.message().await.expect("the stream reads") {
        let Some(storage::read_rows_response::Rows::ArrowRecordBatch(rows)) = response.rows else {
            panic!("an Arrow session sends Arrow rows");
        };
        let mut message = Buffer::from(rows.serialized_record_batch);
        let mut batch: Option<RecordBatch> = None;
        while !message.is_empty() {
            if let Some(decoded) = decoder.decode(&mut message).expect("the batch decodes") {
                batch = Some(decoded);
            }
        }
        let batch = batch.expect("a message holds a batch");
        assert_eq!(
            i64::try_from(batch.num_rows()).expect("fits"),
            response.row_count
        );
        ids.extend(batch.column(0).as_primitive::<Int64Type>().values().iter());
    }
    ids
}

/// A session of one stream over `events`' `id` column, without compression.
async fn one_stream_session(engine: &TestEngine) -> storage::ReadSession {
    one_stream_session_of(
        engine,
        "id",
        storage::arrow_serialization_options::CompressionCodec::CompressionUnspecified,
    )
    .await
}

/// A session of one stream over one column of `events`, its buffers compressed with `codec`.
async fn one_stream_session_of(
    engine: &TestEngine,
    column: &str,
    codec: storage::arrow_serialization_options::CompressionCodec,
) -> storage::ReadSession {
    engine
        .db
        .read_client()
        .create_read_session(storage::CreateReadSessionRequest {
            parent: format!("projects/{PROJECT}"),
            read_session: Some(storage::ReadSession {
                table: format!("projects/{PROJECT}/datasets/shop/tables/events"),
                data_format: storage::DataFormat::Arrow.into(),
                read_options: Some(storage::read_session::TableReadOptions {
                    selected_fields: vec![column.to_string()],
                    output_format_serialization_options: Some(
                        storage::read_session::table_read_options::OutputFormatSerializationOptions::ArrowSerializationOptions(
                            storage::ArrowSerializationOptions {
                                buffer_compression: codec.into(),
                                ..Default::default()
                            },
                        ),
                    ),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            max_stream_count: 1,
            ..Default::default()
        })
        .await
        .expect("the session opens")
        .into_inner()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_read_resumed_at_an_offset_returns_the_same_rows() {
    let engine = TestEngine::start().await;
    let written = events(&engine, 3, 5_000).await;
    let session = one_stream_session(&engine).await;
    let Some(storage::read_session::Schema::ArrowSchema(schema)) = &session.schema else {
        panic!("an Arrow session has an Arrow schema");
    };
    let [stream] = session.streams.as_slice() else {
        panic!("one stream was asked for, not {}", session.streams.len());
    };
    let whole = read_ids(&engine, &schema.serialized_schema, &stream.name, 0).await;
    let mut sorted = whole.clone();
    sorted.sort_unstable();
    assert_eq!(
        sorted,
        written.iter().map(|event| event.id).collect::<Vec<_>>()
    );

    for offset in [5_000, 7_123, 14_999, 15_000] {
        let resumed = read_ids(&engine, &schema.serialized_schema, &stream.name, offset).await;
        let start = usize::try_from(offset).expect("fits");
        assert_eq!(resumed, whole[start..], "resumed at {offset}");
    }
    let past = engine
        .db
        .read_client()
        .read_rows(storage::ReadRowsRequest {
            read_stream: stream.name.clone(),
            offset: 15_001,
            ..Default::default()
        })
        .await
        .expect("the stream opens")
        .into_inner()
        .message()
        .await
        .expect_err("an offset past the end fails the stream");
    assert_eq!(past.code(), Code::OutOfRange);
    engine
        .assert_no_path_twice(&table_ref("shop", "events"))
        .await;
}

/// The bytes of every record batch message of the one stream of `session`.
async fn message_bytes(engine: &TestEngine, session: &storage::ReadSession) -> usize {
    let mut responses = engine
        .db
        .read_client()
        .read_rows(storage::ReadRowsRequest {
            read_stream: session.streams[0].name.clone(),
            ..Default::default()
        })
        .await
        .expect("the stream opens")
        .into_inner();
    let mut bytes = 0;
    while let Some(response) = responses.message().await.expect("the stream reads") {
        if let Some(storage::read_rows_response::Rows::ArrowRecordBatch(rows)) = response.rows {
            bytes += rows.serialized_record_batch.len();
        }
    }
    bytes
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lz4_and_zstd_streams_decode_in_the_client() {
    use storage::arrow_serialization_options::CompressionCodec;
    let engine = TestEngine::start().await;
    let written = events(&engine, 2, 3_000).await;
    let plain =
        one_stream_session_of(&engine, "kind", CompressionCodec::CompressionUnspecified).await;
    let plain = message_bytes(&engine, &plain).await;
    for codec in [CompressionCodec::Lz4Frame, CompressionCodec::Zstd] {
        let compressed = one_stream_session_of(&engine, "kind", codec).await;
        let compressed = message_bytes(&engine, &compressed).await;
        assert!(
            compressed < plain,
            "{codec:?}: {compressed} bytes against {plain} plain"
        );
    }
    for compression in [
        BigQueryReadCompression::None,
        BigQueryReadCompression::Lz4,
        BigQueryReadCompression::Zstd,
    ] {
        let mut read: Vec<Event> = engine
            .db
            .fluent()
            .select()
            .from(SHOP.table(EVENTS))
            .options(BigQueryReadOptions::new().with_compression(compression))
            .obj()
            .query()
            .await
            .expect("the table reads");
        read.sort_by_key(|event| event.id);
        assert_eq!(read, written, "{compression:?}");

        let mut queried: Vec<Event> = engine
            .db
            .fluent()
            .query("SELECT * FROM shop.events WHERE id < 100")
            .read_options(BigQueryReadOptions::new().with_compression(compression))
            .obj()
            .query()
            .await
            .expect("the query runs");
        queried.sort_by_key(|event| event.id);
        assert_eq!(queried, written[..100], "{compression:?}");
    }
    engine
        .assert_no_path_twice(&table_ref("shop", "events"))
        .await;
}

/// The two columns of `events` a typed read projects.
#[derive(Debug, Clone, PartialEq, Deserialize)]
struct EventKind {
    id: i64,
    kind: String,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_typed_read_projects_the_fields_of_its_type_and_restricts_rows() {
    let engine = TestEngine::start().await;
    let written = events(&engine, 1, 300).await;
    let mut kinds: Vec<EventKind> = engine
        .db
        .fluent()
        .select()
        .from(SHOP.table(EVENTS))
        .filter_sql("kind = 'buy' AND id >= 30")
        .obj()
        .query()
        .await
        .expect("the table reads");
    kinds.sort_by_key(|kind| kind.id);
    let expected: Vec<EventKind> = written
        .iter()
        .filter(|event| event.kind == "buy" && event.id >= 30)
        .map(|event| EventKind {
            id: event.id,
            kind: event.kind.clone(),
        })
        .collect();
    assert_eq!(kinds, expected);

    let unknown = engine
        .db
        .fluent()
        .select()
        .from(SHOP.table(EVENTS))
        .filter_sql("missing = 1")
        .obj::<EventKind>()
        .query()
        .await;
    let Err(BigQueryError::DatabaseError(error)) = unknown else {
        panic!("a restriction on a missing column fails, not {unknown:?}");
    };
    assert_eq!(error.public.code, format!("{:?}", Code::InvalidArgument));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_table_without_rows_reads_as_empty() {
    let engine = TestEngine::start().await;
    events(&engine, 0, 0).await;
    let read: Vec<Event> = engine
        .db
        .fluent()
        .select()
        .from(SHOP.table(EVENTS))
        .obj()
        .query()
        .await
        .expect("the table reads");
    assert!(read.is_empty());
}

#[test]
fn a_stream_name_holds_its_session_and_index() {
    let session = Uuid::new_v4();
    let name: StreamName =
        format!("projects/lite-project/locations/eu/sessions/{session}/streams/3")
            .parse()
            .expect("a stream name");
    assert_eq!((name.session, name.index), (session, 3));
    assert!(matches!(
        "projects/p/sessions/x".parse::<StreamName>(),
        Err(crate::error::EngineError::NotFound { .. })
    ));
}
