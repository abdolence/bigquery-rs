//! Fake answers for the Storage Read RPCs: `CreateReadSession` and `ReadRows`, plus the
//! `GetTable` a typed read sends for its projection.

use super::wire::IpcMessages;
use super::FakeCall;
use arrow_array::RecordBatch;
use gcloud_sdk::google::cloud::bigquery::storage::v1::arrow_serialization_options::CompressionCodec;
use gcloud_sdk::google::cloud::bigquery::storage::v1::read_session::table_read_options::OutputFormatSerializationOptions;
use gcloud_sdk::google::cloud::bigquery::storage::v1::{
    read_rows_response, read_session, ArrowRecordBatch, ArrowSchema, CreateReadSessionRequest,
    ReadRowsRequest, ReadRowsResponse, ReadSession, ReadStream,
};
use gcloud_sdk::google::cloud::bigquery::v2::{
    GetTableRequest, Table, TableFieldSchema, TableSchema,
};

/// One table as the fake serves it: the Arrow schema and, per read stream, its batches.
#[derive(Clone)]
pub(crate) struct FakeReadTable {
    pub schema: arrow_schema::SchemaRef,
    pub streams: Vec<Vec<RecordBatch>>,
}

impl FakeReadTable {
    /// A table whose read session has one stream per entry of `streams`.
    pub fn new(streams: Vec<Vec<RecordBatch>>) -> Self {
        let schema = streams
            .iter()
            .flatten()
            .next()
            .expect("a fake table has at least one batch")
            .schema();
        Self { schema, streams }
    }

    /// The IPC schema message and each stream's IPC record batch messages, encoded as a session
    /// with `compression` sends them.
    pub(crate) fn encode(&self, compression: CompressionCodec) -> (Vec<u8>, Vec<Vec<Vec<u8>>>) {
        let compression = compression.into();
        let mut schema = Vec::new();
        let streams = self
            .streams
            .iter()
            .map(|batches| {
                let messages = IpcMessages::encode(&self.schema, batches, compression)
                    .expect("the batches encode");
                schema = messages.schema;
                messages.batches
            })
            .collect();
        (schema, streams)
    }

    /// The table's columns as `GetTable` lists them.
    fn table_schema(&self) -> TableSchema {
        TableSchema {
            fields: self
                .schema
                .fields()
                .iter()
                .map(|f| TableFieldSchema {
                    name: f.name().clone(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }
}

impl FakeCall {
    /// Answers `GetTable` with the table's columns, logging `GetTable ds.t`.
    pub(crate) async fn get_table(mut self, table: &FakeReadTable) {
        let request: GetTableRequest = self.next_request().await.expect("a GetTable request");
        self.log(format!(
            "GetTable {}.{}",
            request.dataset_id, request.table_id
        ));
        self.reply(&Table {
            schema: Some(table.table_schema()),
            ..Default::default()
        });
    }

    /// Reads a `CreateReadSession` request and logs it as `CreateReadSession [selected fields]`.
    pub(crate) async fn session_request(&mut self) -> CreateReadSessionRequest {
        let request: CreateReadSessionRequest = self
            .next_request()
            .await
            .expect("a CreateReadSession request");
        let selected = request
            .read_session
            .as_ref()
            .and_then(|s| s.read_options.as_ref())
            .map(|o| o.selected_fields.join(","))
            .unwrap_or_default();
        self.log(format!("CreateReadSession [{selected}]"));
        request
    }

    /// Answers a `CreateReadSession` request with the table's schema and streams `s0`, `s1`, ...
    /// in the compression it asked for. Returns each stream's encoded batches for `ReadRows`.
    pub(crate) fn open_session(
        self,
        request: &CreateReadSessionRequest,
        table: &FakeReadTable,
    ) -> Vec<Vec<Vec<u8>>> {
        let (schema, streams) = table.encode(requested_compression(request));
        self.reply(&ReadSession {
            name: "session".into(),
            streams: (0..streams.len())
                .map(|i| ReadStream {
                    name: format!("s{i}"),
                })
                .collect(),
            schema: Some(read_session::Schema::ArrowSchema(ArrowSchema {
                serialized_schema: schema,
            })),
            ..Default::default()
        });
        streams
    }

    /// Reads a `ReadRows` request and logs it as `ReadRows s0 at 2`.
    pub(crate) async fn read_rows_request(&mut self) -> ReadRowsRequest {
        let request: ReadRowsRequest = self.next_request().await.expect("a ReadRows request");
        self.log(format!(
            "ReadRows {} at {}",
            request.read_stream, request.offset
        ));
        request
    }

    /// Sends one `ReadRowsResponse` per encoded batch, with its row count.
    pub(crate) fn send_batches(&mut self, batches: &[(Vec<u8>, i64)]) {
        for (bytes, row_count) in batches {
            self.send(&ReadRowsResponse {
                row_count: *row_count,
                rows: Some(read_rows_response::Rows::ArrowRecordBatch(
                    ArrowRecordBatch {
                        serialized_record_batch: bytes.clone(),
                        ..Default::default()
                    },
                )),
                ..Default::default()
            });
        }
    }
}

pub(crate) fn requested_compression(request: &CreateReadSessionRequest) -> CompressionCodec {
    let options = request
        .read_session
        .as_ref()
        .and_then(|s| s.read_options.as_ref())
        .and_then(|o| o.output_format_serialization_options.as_ref());
    match options {
        Some(OutputFormatSerializationOptions::ArrowSerializationOptions(arrow)) => {
            arrow.buffer_compression()
        }
        _ => CompressionCodec::CompressionUnspecified,
    }
}
