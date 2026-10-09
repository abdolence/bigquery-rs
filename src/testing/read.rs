//! The Storage Read RPCs: `CreateReadSession` and `ReadRows`.
//!
//! A session is served a read rule's rows when one matches its table and row restriction, and
//! the table's own rows otherwise, projected to its selected fields and spread over the
//! table's read streams. The fake evaluates no filter, so a session with a row restriction
//! needs a rule, and one with a sample percentage or a snapshot time is served the current
//! rows.

use crate::db::fake::wire::{IpcCompression, IpcMessages};
use crate::db::fake::FakeCall;
use crate::testing::rules::BigQueryFakeRpc;
use crate::testing::server::FakeShared;
use crate::testing::state::{fit_to_layout, FakeReadStream, TableKey, DEFAULT_LOCATION};
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use gcloud_sdk::google::cloud::bigquery::storage::v1::read_session::table_read_options::OutputFormatSerializationOptions;
use gcloud_sdk::google::cloud::bigquery::storage::v1::read_session::{self, TableReadOptions};
use gcloud_sdk::google::cloud::bigquery::storage::v1::{
    read_rows_response, ArrowRecordBatch, ArrowSchema, CreateReadSessionRequest, DataFormat,
    ReadRowsRequest, ReadRowsResponse, ReadSession, ReadStream,
};
use gcloud_sdk::tonic::Code;
use std::sync::Arc;

/// How BigQuery's `CreateReadSession` starts its `InvalidArgument` for a `selected_fields`
/// entry the table does not have. The client tells that error apart by the words "selected
/// fields do not exist" in it.
const SELECTED_FIELDS_DO_NOT_EXIST: &str =
    "The following selected fields do not exist in the table schema";

impl FakeShared {
    pub(super) async fn serve_read(&self, call: FakeCall) {
        match call.method() {
            "CreateReadSession" => self.create_read_session(call).await,
            "ReadRows" => self.read_rows(call).await,
            method => {
                let described = method.to_string();
                self.unmatched(call, &described, &[]);
            }
        }
    }

    /// Answers `CreateReadSession` with a session over the rows of a read rule or of the
    /// table, and records its streams for `ReadRows`.
    async fn create_read_session(&self, call: FakeCall) {
        let Some((call, request)) = self.first_request::<CreateReadSessionRequest>(call).await
        else {
            return;
        };
        let session = request.read_session.unwrap_or_default();
        let key = match TableKey::from_path(&session.table) {
            Ok(key) => key,
            Err(err) => {
                let described = format!("CreateReadSession of an invalid table: {err}");
                self.unmatched(call, &described, &[]);
                return;
            }
        };
        let rpc = BigQueryFakeRpc::CreateReadSession;
        let Some(call) = self.unfaulted(call, rpc, Some(&key)).await else {
            return;
        };
        let options = session.read_options.clone().unwrap_or_default();
        if session.data_format() != DataFormat::Arrow {
            let described = format!("CreateReadSession of {key} in {:?}", session.data_format());
            self.unmatched(call, &described, &[]);
            return;
        }
        let table = self.state().tables.get(&key).map(|table| {
            (
                table.arrow_schema.clone(),
                table.batches.clone(),
                table.read_streams,
            )
        });
        let Some((arrow_schema, table_batches, read_streams)) = table else {
            let status = key.not_found();
            call.fail(status.code(), status.message());
            return;
        };
        let columns = match SelectedColumns::of(&arrow_schema, &options.selected_fields) {
            SelectedColumns::Indexes(columns) => columns,
            SelectedColumns::Unknown(names) => {
                let message = format!("{SELECTED_FIELDS_DO_NOT_EXIST}: {}", names.join(", "));
                call.fail(Code::InvalidArgument, &message);
                return;
            }
            SelectedColumns::Nested(name) => {
                let described =
                    format!("CreateReadSession of {key} selecting the sub-field {name:?}");
                self.unmatched(call, &described, &[]);
                return;
            }
        };
        let restriction = Some(options.row_restriction.as_str()).filter(|r| !r.is_empty());
        let ruled = self.rules().answer_read(&key, restriction);
        let batches = match (ruled, restriction) {
            (Some(rows), _) => match fit_to_layout(&rows, &arrow_schema) {
                Ok(rows) => vec![rows],
                Err(err) => {
                    self.internal(call, &format!("the read rule's rows of {key}: {err}"));
                    return;
                }
            },
            (None, None) => table_batches,
            (None, Some(restriction)) => {
                let described = format!("CreateReadSession of {key} where {restriction:?}");
                let rules = self.rules().describe_reads();
                self.unmatched(call, &described, &rules);
                return;
            }
        };
        let projected = batches
            .iter()
            .map(|batch| batch.project(&columns))
            .collect::<Result<Vec<_>, _>>()
            .and_then(|batches| Ok((Arc::new(arrow_schema.project(&columns)?), batches)));
        let (arrow_schema, batches) = match projected {
            Ok(projected) => projected,
            Err(err) => {
                self.internal(call, &format!("the rows of {key} do not project: {err}"));
                return;
            }
        };
        let compression = requested_compression(&options);
        let schema_message = match IpcMessages::encode(&arrow_schema, &[], compression) {
            Ok(messages) => messages.schema,
            Err(err) => {
                self.internal(call, &format!("the schema of {key} does not encode: {err}"));
                return;
            }
        };
        let stream_count = usize::try_from(request.max_stream_count)
            .ok()
            .filter(|&max| max > 0)
            .map_or(read_streams.get(), |max| max.min(read_streams.get()));
        let row_count: usize = batches.iter().map(RecordBatch::num_rows).sum();
        let mut state = self.state();
        let name = format!(
            "projects/{}/locations/{DEFAULT_LOCATION}/sessions/fake-session-{}",
            key.dataset.project,
            state.next_id()
        );
        let streams = (0..stream_count)
            .map(|index| {
                let stream = ReadStream {
                    name: format!("{name}/streams/{index}"),
                };
                let rows = rows_between(
                    &batches,
                    index * row_count / stream_count,
                    (index + 1) * row_count / stream_count,
                );
                state.read_streams.insert(
                    stream.name.clone(),
                    FakeReadStream {
                        table: key.clone(),
                        arrow_schema: arrow_schema.clone(),
                        batches: rows,
                        compression,
                    },
                );
                stream
            })
            .collect();
        drop(state);
        call.reply(&ReadSession {
            name,
            streams,
            estimated_row_count: i64::try_from(row_count).unwrap_or(i64::MAX),
            schema: Some(read_session::Schema::ArrowSchema(ArrowSchema {
                serialized_schema: schema_message,
            })),
            ..session
        });
    }

    /// Answers `ReadRows` with one response per batch of the stream, from the request's
    /// offset on.
    async fn read_rows(&self, call: FakeCall) {
        let Some((call, request)) = self.first_request::<ReadRowsRequest>(call).await else {
            return;
        };
        let stream = self.state().read_streams.get(&request.read_stream).cloned();
        let table = stream.as_ref().map(|stream| &stream.table);
        let Some(mut call) = self.unfaulted(call, BigQueryFakeRpc::ReadRows, table).await else {
            return;
        };
        let Some(stream) = stream else {
            let described = format!("ReadRows of the unknown stream {:?}", request.read_stream);
            self.unmatched(call, &described, &[]);
            return;
        };
        let row_count: usize = stream.batches.iter().map(RecordBatch::num_rows).sum();
        let Some(offset) = usize::try_from(request.offset)
            .ok()
            .filter(|&offset| offset <= row_count)
        else {
            let described = format!(
                "ReadRows of {} at offset {} of {row_count} rows",
                request.read_stream, request.offset
            );
            self.unmatched(call, &described, &[]);
            return;
        };
        let batches = rows_between(&stream.batches, offset, row_count);
        let messages = match IpcMessages::encode(&stream.arrow_schema, &batches, stream.compression)
        {
            Ok(messages) => messages,
            Err(err) => {
                let failure = format!("the rows of {} do not encode: {err}", stream.table);
                self.internal(call, &failure);
                return;
            }
        };
        for (batch, message) in batches.iter().zip(messages.batches) {
            call.send(&ReadRowsResponse {
                row_count: i64::try_from(batch.num_rows()).unwrap_or(i64::MAX),
                rows: Some(read_rows_response::Rows::ArrowRecordBatch(
                    ArrowRecordBatch {
                        serialized_record_batch: message,
                        ..Default::default()
                    },
                )),
                ..Default::default()
            });
        }
        call.finish();
    }
}

/// The columns a session's `selected_fields` names, ignoring case as BigQuery does.
enum SelectedColumns {
    /// Their indexes in table order, as BigQuery sends them whatever order they were named in;
    /// every column when none is named.
    Indexes(Vec<usize>),
    /// The names the table does not have.
    Unknown(Vec<String>),
    /// A STRUCT sub-field such as `rec.field`, which the fake does not project.
    Nested(String),
}

impl SelectedColumns {
    fn of(schema: &Schema, selected: &[String]) -> Self {
        if selected.is_empty() {
            return Self::Indexes((0..schema.fields().len()).collect());
        }
        let has_column = |name: &str| {
            schema
                .fields()
                .iter()
                .any(|field| field.name().eq_ignore_ascii_case(name))
        };
        let unknown: Vec<String> = selected
            .iter()
            .filter(|name| !has_column(name))
            .cloned()
            .collect();
        if let Some(nested) = unknown.iter().find(|name| {
            name.split_once('.')
                .is_some_and(|(column, _)| has_column(column))
        }) {
            return Self::Nested(nested.clone());
        }
        if !unknown.is_empty() {
            return Self::Unknown(unknown);
        }
        Self::Indexes(
            (0..schema.fields().len())
                .filter(|&index| {
                    let column = schema.field(index).name();
                    selected
                        .iter()
                        .any(|name| name.eq_ignore_ascii_case(column))
                })
                .collect(),
        )
    }
}

/// The buffer compression a session's Arrow options ask for.
fn requested_compression(options: &TableReadOptions) -> IpcCompression {
    match &options.output_format_serialization_options {
        Some(OutputFormatSerializationOptions::ArrowSerializationOptions(arrow)) => {
            arrow.buffer_compression().into()
        }
        _ => IpcCompression::default(),
    }
}

/// The rows from `start` up to `end` of `batches` taken as one sequence, as slices of them.
fn rows_between(batches: &[RecordBatch], start: usize, end: usize) -> Vec<RecordBatch> {
    let mut first_row = 0;
    batches
        .iter()
        .filter_map(|batch| {
            let (from, to) = (first_row, first_row + batch.num_rows());
            first_row = to;
            let (low, high) = (start.max(from), end.min(to));
            (low < high).then(|| batch.slice(low - from, high - low))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use crate::errors::BigQueryError;
    use crate::read::BigQueryBatchRows;
    use crate::testing::{BigQueryFake, BigQueryFakeCode, BigQueryFakeFault, BigQueryFakeRpc};
    use crate::{
        BigQueryDatasetId, BigQueryReadOptions, BigQueryResult, BigQueryTableId, BigQueryTableRef,
    };
    use arrow_array::RecordBatch;
    use futures::TryStreamExt;
    use gcloud_sdk::google::cloud::bigquery::storage::v1::{
        CreateReadSessionRequest, DataFormat, ReadRowsRequest, ReadSession,
    };
    use gcloud_sdk::tonic::Code;
    use serde::{Deserialize, Serialize};
    use std::panic::{catch_unwind, AssertUnwindSafe};

    const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
    const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Order {
        id: i64,
        customer: String,
        total: f64,
        paid: bool,
        receipt: Option<Vec<u8>>,
    }

    #[derive(Debug, PartialEq, Deserialize)]
    struct OrderCustomer {
        id: i64,
        customer: String,
    }

    fn order(id: i64, customer: &str) -> Order {
        Order {
            id,
            customer: customer.to_string(),
            total: id as f64 * 1.5,
            paid: id % 2 == 0,
            receipt: (id % 3 == 0).then(|| vec![0, 159, 255]),
        }
    }

    fn orders() -> BigQueryTableRef {
        SHOP.table(ORDERS)
    }

    /// A fake whose `shop.orders` holds `rows`, over `streams` read streams.
    async fn fake_with(rows: &[Order], streams: usize) -> BigQueryResult<BigQueryFake> {
        let fake = BigQueryFake::start().await?;
        fake.table(orders(), |columns| columns.from_type::<Order>())
            .rows(rows)
            .read_streams(streams)
            .create()?;
        Ok(fake)
    }

    async fn read_orders(fake: &BigQueryFake) -> BigQueryResult<Vec<Order>> {
        let mut rows: Vec<Order> = fake
            .db()
            .fluent()
            .select()
            .from(orders())
            .obj()
            .query()
            .await?;
        rows.sort_by_key(|row| row.id);
        Ok(rows)
    }

    /// Whether dropping `fake`, which verifies it, panics.
    fn drop_panics(fake: BigQueryFake) -> bool {
        catch_unwind(AssertUnwindSafe(move || drop(fake))).is_err()
    }

    #[tokio::test]
    async fn a_read_returns_the_table_rows() -> BigQueryResult<()> {
        let rows = vec![order(1, "Alice"), order(2, "Bob"), order(3, "Carol")];
        let fake = fake_with(&rows, 1).await?;

        assert_eq!(read_orders(&fake).await?, rows);
        Ok(())
    }

    #[tokio::test]
    async fn selected_fields_project_the_columns() -> BigQueryResult<()> {
        let fake = fake_with(&[order(1, "Alice"), order(2, "Bob")], 1).await?;

        let batches: Vec<RecordBatch> = fake
            .db()
            .fluent()
            .select()
            .fields(["customer", "id"])
            .from(orders())
            .record_batches()
            .await?
            .try_collect()
            .await?;

        let columns: Vec<&str> = batches[0]
            .schema_ref()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect();
        assert_eq!(columns, ["id", "customer"]);
        let read: Vec<OrderCustomer> = batches
            .iter()
            .flat_map(BigQueryBatchRows::new)
            .collect::<BigQueryResult<_>>()?;
        let expected = vec![
            OrderCustomer {
                id: 1,
                customer: "Alice".into(),
            },
            OrderCustomer {
                id: 2,
                customer: "Bob".into(),
            },
        ];
        assert_eq!(read, expected);
        Ok(())
    }

    #[tokio::test]
    async fn an_unknown_selected_field_is_a_schema_mismatch() -> BigQueryResult<()> {
        let fake = fake_with(&[order(1, "Alice")], 1).await?;

        let read = fake
            .db()
            .fluent()
            .select()
            .fields(["id", "discount"])
            .from(orders())
            .obj::<Order>()
            .query()
            .await;

        assert!(
            matches!(read, Err(BigQueryError::SchemaMismatchError(_))),
            "{read:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn rows_are_spread_over_the_read_streams() -> BigQueryResult<()> {
        let rows: Vec<Order> = (1..=7).map(|id| order(id, "Alice")).collect();
        let fake = fake_with(&rows, 3).await?;

        let batches: Vec<RecordBatch> = fake
            .db()
            .fluent()
            .select()
            .from(orders())
            .options(BigQueryReadOptions::new().with_max_stream_count(8))
            .record_batches()
            .await?
            .try_collect()
            .await?;

        let mut sizes: Vec<usize> = batches.iter().map(RecordBatch::num_rows).collect();
        sizes.sort_unstable();
        assert_eq!(sizes, [2, 2, 3]);
        assert_eq!(read_orders(&fake).await?, rows);
        Ok(())
    }

    #[tokio::test]
    async fn a_read_rule_answers_a_row_restriction() -> BigQueryResult<()> {
        let fake = fake_with(&[order(1, "Alice"), order(2, "Bob")], 1).await?;
        let bobs = vec![order(2, "Bob")];
        let rule = fake
            .when_read(orders())
            .row_restriction("customer = 'Bob'")
            .returns_rows(&bobs)?;

        let read: Vec<Order> = fake
            .db()
            .fluent()
            .select()
            .from(orders())
            .filter_sql("customer = 'Bob'")
            .obj()
            .query()
            .await?;

        assert_eq!(read, bobs);
        assert_eq!(rule.calls(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn an_unmatched_row_restriction_fails_verify() -> BigQueryResult<()> {
        let rows = vec![order(1, "Alice")];
        let fake = fake_with(&rows, 1).await?;
        assert_eq!(read_orders(&fake).await?, rows);

        let read = fake
            .db()
            .fluent()
            .select()
            .from(orders())
            .filter_sql("customer = 'Bob'")
            .obj::<Order>()
            .query()
            .await;

        match &read {
            Err(err) => assert!(err.has_code(Code::Unimplemented), "{err:?}"),
            Ok(rows) => panic!("expected the unmatched read to fail, got {rows:?}"),
        }
        assert!(drop_panics(fake));
        Ok(())
    }

    #[tokio::test]
    async fn a_fault_refuses_the_session() -> BigQueryResult<()> {
        let fake = fake_with(&[order(1, "Alice")], 1).await?;
        let fault = fake
            .when_fault(BigQueryFakeRpc::CreateReadSession)
            .on_table(orders())
            .fails(BigQueryFakeFault::status(
                BigQueryFakeCode::PermissionDenied,
                "no access",
            ))?;

        let read = read_orders(&fake).await;

        match &read {
            Err(err) => assert!(err.has_code(Code::PermissionDenied), "{err:?}"),
            Ok(rows) => panic!("expected the session to be refused, got {rows:?}"),
        }
        assert_eq!(fault.calls(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn a_read_rows_fault_is_retried() -> BigQueryResult<()> {
        let rows = vec![order(1, "Alice"), order(2, "Bob")];
        let fake = fake_with(&rows, 1).await?;
        let fault = fake
            .when_fault(BigQueryFakeRpc::ReadRows)
            .on_table(orders())
            .times(1)
            .fails(BigQueryFakeFault::status(
                BigQueryFakeCode::Unavailable,
                "try again",
            ))?;

        assert_eq!(read_orders(&fake).await?, rows);
        assert_eq!(fault.calls(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn read_rows_starts_at_its_offset() -> BigQueryResult<()> {
        let fake = fake_with(&[order(1, "Alice"), order(2, "Bob"), order(3, "Carol")], 1).await?;
        let project = fake.db().options().google_project_id.clone();
        let session = fake
            .db()
            .read_client()
            .create_read_session(CreateReadSessionRequest {
                parent: format!("projects/{project}"),
                read_session: Some(ReadSession {
                    table: orders().table_path(&project),
                    data_format: DataFormat::Arrow.into(),
                    ..Default::default()
                }),
                max_stream_count: 1,
                ..Default::default()
            })
            .await
            .map_err(BigQueryError::from)?
            .into_inner();

        let mut responses = fake
            .db()
            .read_client()
            .read_rows(ReadRowsRequest {
                read_stream: session.streams[0].name.clone(),
                offset: 2,
                ..Default::default()
            })
            .await
            .map_err(BigQueryError::from)?
            .into_inner();
        let mut row_count = 0;
        while let Some(response) = responses.message().await.map_err(BigQueryError::from)? {
            row_count += response.row_count;
        }

        assert_eq!(row_count, 1);
        Ok(())
    }

    #[tokio::test]
    async fn a_sampled_read_is_served_the_current_rows() -> BigQueryResult<()> {
        let rows = vec![order(1, "Alice"), order(2, "Bob")];
        let fake = fake_with(&rows, 1).await?;

        let mut read: Vec<Order> = fake
            .db()
            .fluent()
            .select()
            .from(orders())
            .sample_percentage(50.0)
            .obj()
            .query()
            .await?;

        read.sort_by_key(|row| row.id);
        assert_eq!(read, rows);
        Ok(())
    }

    #[tokio::test]
    async fn selected_fields_match_columns_ignoring_case() -> BigQueryResult<()> {
        let fake = fake_with(&[order(1, "Alice")], 1).await?;

        let read: Vec<OrderCustomer> = fake
            .db()
            .fluent()
            .select()
            .fields(["CUSTOMER", "Id"])
            .from(orders())
            .obj()
            .query()
            .await?;

        let expected = vec![OrderCustomer {
            id: 1,
            customer: "Alice".into(),
        }];
        assert_eq!(read, expected);
        Ok(())
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct NotedOrder {
        id: i64,
        customer: String,
        total: f64,
        paid: bool,
        receipt: Option<Vec<u8>>,
        note: Option<String>,
    }

    #[tokio::test]
    async fn a_read_rule_takes_a_column_added_after_it() -> BigQueryResult<()> {
        let fake = fake_with(&[], 1).await?;
        let bob = order(2, "Bob");
        fake.when_read(orders())
            .row_restriction("customer = 'Bob'")
            .returns_rows([&bob])?;
        fake.db()
            .fluent()
            .schema()
            .table(orders())
            .columns(|columns| columns.from_type::<NotedOrder>())
            .sync()
            .await?;

        let read: Vec<NotedOrder> = fake
            .db()
            .fluent()
            .select()
            .from(orders())
            .filter_sql("customer = 'Bob'")
            .obj()
            .query()
            .await?;

        let expected = NotedOrder {
            id: bob.id,
            customer: bob.customer,
            total: bob.total,
            paid: bob.paid,
            receipt: bob.receipt,
            note: None,
        };
        assert_eq!(read, vec![expected]);
        Ok(())
    }
}
