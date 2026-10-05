//! Fake answers for the Storage Write RPCs: `AppendRows`, `GetWriteStream`,
//! `CreateWriteStream`, `FlushRows`, `FinalizeWriteStream` and `BatchCommitWriteStreams`.

use super::FakeCall;
use gcloud_sdk::google::cloud::bigquery::storage::v1::append_rows_request::Rows;
use gcloud_sdk::google::cloud::bigquery::storage::v1::append_rows_response::{
    AppendResult, Response,
};
use gcloud_sdk::google::cloud::bigquery::storage::v1::storage_error::StorageErrorCode;
use gcloud_sdk::google::cloud::bigquery::storage::v1::table_field_schema::{Mode, Type};
use gcloud_sdk::google::cloud::bigquery::storage::v1::{
    AppendRowsRequest, AppendRowsResponse, BatchCommitWriteStreamsRequest,
    BatchCommitWriteStreamsResponse, CreateWriteStreamRequest, FinalizeWriteStreamRequest,
    FinalizeWriteStreamResponse, FlushRowsRequest, FlushRowsResponse, GetWriteStreamRequest,
    RowError, StorageError, TableFieldSchema, TableSchema, WriteStream,
};
use gcloud_sdk::prost::encoding::decode_varint;
use gcloud_sdk::prost::Message;
use gcloud_sdk::tonic::Code;

pub(crate) const DEFAULT_STREAM: &str =
    "projects/fake-project/datasets/shop/tables/orders/streams/_default";
pub(crate) const CREATED_STREAM: &str =
    "projects/fake-project/datasets/shop/tables/orders/streams/s1";

pub(crate) fn column(name: &str, r#type: Type, mode: Mode) -> TableFieldSchema {
    TableFieldSchema {
        name: name.into(),
        r#type: r#type.into(),
        mode: mode.into(),
        ..Default::default()
    }
}

/// `id INT64 REQUIRED, name STRING`, plus `extra`.
pub(crate) fn schema(extra: &[TableFieldSchema]) -> TableSchema {
    let mut fields = vec![
        column("id", Type::Int64, Mode::Required),
        column("name", Type::String, Mode::Nullable),
    ];
    fields.extend_from_slice(extra);
    TableSchema { fields }
}

impl FakeCall {
    /// Answers a unary write RPC with `schema`, logging it, and hands back an `AppendRows` self
    /// for the test to answer itself.
    pub(crate) async fn answer_unary(mut self, schema: TableSchema) -> Option<Self> {
        match self.method() {
            "AppendRows" => return Some(self),
            "GetWriteStream" => {
                let request: Option<GetWriteStreamRequest> = self.next_request().await;
                let name = request.map(|r| r.name).unwrap_or_default();
                self.log(format!("GetWriteStream {name}"));
                self.reply(&WriteStream {
                    name,
                    table_schema: Some(schema),
                    ..Default::default()
                });
            }
            "CreateWriteStream" => {
                let request: Option<CreateWriteStreamRequest> = self.next_request().await;
                let kind = request
                    .and_then(|r| r.write_stream)
                    .map(|s| s.r#type().as_str_name().to_string())
                    .unwrap_or_default();
                self.log(format!("CreateWriteStream {kind}"));
                self.reply(&WriteStream {
                    name: CREATED_STREAM.into(),
                    table_schema: Some(schema),
                    ..Default::default()
                });
            }
            "FlushRows" => {
                let request: Option<FlushRowsRequest> = self.next_request().await;
                let (stream, offset) = request
                    .map(|r| (r.write_stream, r.offset))
                    .unwrap_or_default();
                let shown = offset.map(|o| o.to_string()).unwrap_or_default();
                self.log(format!("FlushRows {stream} @{shown}"));
                self.reply(&FlushRowsResponse {
                    offset: offset.unwrap_or_default(),
                });
            }
            "FinalizeWriteStream" => {
                let request: Option<FinalizeWriteStreamRequest> = self.next_request().await;
                self.log(format!(
                    "FinalizeWriteStream {}",
                    request.map(|r| r.name).unwrap_or_default()
                ));
                self.reply(&FinalizeWriteStreamResponse { row_count: 42 });
            }
            "BatchCommitWriteStreams" => {
                let request: Option<BatchCommitWriteStreamsRequest> = self.next_request().await;
                self.log(format!(
                    "BatchCommitWriteStreams {:?}",
                    request.map(|r| r.write_streams).unwrap_or_default()
                ));
                self.reply(&BatchCommitWriteStreamsResponse {
                    commit_time: Some(gcloud_sdk::prost_types::Timestamp {
                        seconds: 1_791_000_000,
                        nanos: 0,
                    }),
                    stream_errors: Vec::new(),
                });
            }
            other => {
                let other = other.to_string();
                self.fail(Code::Unimplemented, &other);
            }
        }
        None
    }
}

/// The `id` of each row in `request`: the varint after the key of field 1, which the test
/// rows write first.
pub(crate) fn ids(request: &AppendRowsRequest) -> Vec<i64> {
    let Some(Rows::ProtoRows(data)) = &request.rows else {
        return Vec::new();
    };
    data.rows
        .as_ref()
        .map(|rows| {
            rows.serialized_rows
                .iter()
                .map(|row| {
                    assert_eq!(
                        row.first(),
                        Some(&0x08),
                        "rows start with field 1, a varint"
                    );
                    decode_varint(&mut &row[1..]).expect("a varint id") as i64
                })
                .collect()
        })
        .unwrap_or_default()
}

/// The field names of the writer schema a request carries, if it carries one.
pub(crate) fn schema_fields(request: &AppendRowsRequest) -> Option<Vec<String>> {
    let Some(Rows::ProtoRows(data)) = &request.rows else {
        return None;
    };
    data.writer_schema
        .as_ref()
        .and_then(|s| s.proto_descriptor.as_ref())
        .map(|d| d.field.iter().map(|f| f.name().to_string()).collect())
}

/// One line per request: connection, offset, ids, and the writer schema's columns if sent.
pub(crate) fn describe(connection: usize, request: &AppendRowsRequest) -> String {
    let offset = request.offset.map(|o| format!(" @{o}")).unwrap_or_default();
    let schema = schema_fields(request)
        .map(|f| format!(" schema={}", f.join(",")))
        .unwrap_or_default();
    format!("c{connection} append{offset} {:?}{schema}", ids(request))
}

pub(crate) fn ack(offset: Option<i64>) -> AppendRowsResponse {
    AppendRowsResponse {
        response: Some(Response::AppendResult(AppendResult { offset })),
        ..Default::default()
    }
}

pub(crate) fn in_band(
    code: Code,
    storage: Option<StorageErrorCode>,
    message: &str,
) -> AppendRowsResponse {
    let details = storage
        .map(|storage| gcloud_sdk::prost_types::Any {
            type_url: "type.googleapis.com/google.cloud.bigquery.storage.v1.StorageError".into(),
            value: StorageError {
                code: storage.into(),
                entity: CREATED_STREAM.into(),
                error_message: message.into(),
            }
            .encode_to_vec(),
        })
        .into_iter()
        .collect();
    AppendRowsResponse {
        response: Some(Response::Error(gcloud_sdk::google::rpc::Status {
            code: code as i32,
            message: message.into(),
            details,
        })),
        ..Default::default()
    }
}

pub(crate) fn row_errors(indexes: &[i64]) -> AppendRowsResponse {
    AppendRowsResponse {
        row_errors: indexes
            .iter()
            .map(|&index| RowError {
                index,
                code: 1,
                message: format!("Field value of req_col cannot be empty. ({index})"),
            })
            .collect(),
        ..in_band(
            Code::InvalidArgument,
            None,
            "Errors found while processing rows.",
        )
    }
}
