//! What the fake holds: datasets, tables and their rows, write and read streams, and jobs.
//!
//! Everything sits under one lock in [`FakeShared`](super::server::FakeShared), which is never
//! held across an `.await`. Keys carry the project resolved against the client's, so a table
//! named without a project and the same table named with the client's project are one table.

use crate::db::fake::wire::IpcCompression;
use crate::errors::BigQueryError;
use crate::testing::rows::rescale_decimal;
use crate::testing::BigQueryFakeJobFailure;
use crate::{
    BigQueryChangeSequenceNumber, BigQueryChangeType, BigQueryDatasetId, BigQueryDatasetRef,
    BigQueryDecimalParams, BigQueryDmlStats, BigQueryFieldSchema, BigQueryFieldType,
    BigQueryInstant, BigQueryJobId, BigQueryResult, BigQueryStatementType, BigQueryTableId,
    BigQueryTableRef, BigQueryTableSchema, BigQueryWriteMode, BigQueryWriteStreamName,
};
use arrow_array::cast::AsArray;
use arrow_array::types::{Decimal128Type, Decimal256Type};
use arrow_array::{
    new_null_array, Array, ArrayRef, ListArray, RecordBatch, RecordBatchOptions, StructArray,
};
use arrow_buffer::i256;
use arrow_schema::{ArrowError, DataType, Field, Fields, SchemaRef};
use gcloud_sdk::google::cloud::bigquery::v2;
use gcloud_sdk::tonic::Status;
use std::collections::{BTreeMap, HashMap};
use std::fmt::{Display, Formatter};
use std::num::NonZeroUsize;
use std::sync::Arc;

/// The hidden dataset that holds the result table of each query job whose rows do not come
/// inline. A dataset whose ID starts with `_` is hidden, as in BigQuery.
pub(super) const QUERY_RESULTS_DATASET: BigQueryDatasetId =
    BigQueryDatasetId::from_static("_fake_query_results");

/// The location a dataset, a job or a read session reports when its call names none,
/// BigQuery's default.
pub(super) const DEFAULT_LOCATION: &str = "US";

/// The location a call names, or [`DEFAULT_LOCATION`] for an empty one.
pub(super) fn location_or_default(location: &str) -> String {
    if location.is_empty() {
        DEFAULT_LOCATION.to_string()
    } else {
        location.to_string()
    }
}

/// A dataset, with its project resolved.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) struct DatasetKey {
    pub project: String,
    pub dataset: BigQueryDatasetId,
}

impl DatasetKey {
    /// `dataset`, in `client_project` unless it names its own.
    pub(super) fn resolve(dataset: &BigQueryDatasetRef, client_project: &str) -> Self {
        Self {
            project: dataset.project_or(client_project).to_string(),
            dataset: dataset.dataset().clone(),
        }
    }

    /// The dataset a request names by its IDs.
    ///
    /// # Errors
    /// [`BigQueryError::InvalidParametersError`] for an ID the client would never send.
    pub(super) fn new(project: &str, dataset: &str) -> BigQueryResult<Self> {
        let dataset = BigQueryDatasetRef::new(project, BigQueryDatasetId::new(dataset)?)?;
        Ok(Self::resolve(&dataset, project))
    }

    pub(super) fn table(&self, table: BigQueryTableId) -> TableKey {
        TableKey {
            dataset: self.clone(),
            table,
        }
    }

    /// The v2 `DatasetReference`.
    pub(super) fn reference(&self) -> v2::DatasetReference {
        v2::DatasetReference {
            project_id: self.project.clone(),
            dataset_id: self.dataset.to_string(),
        }
    }

    /// `project:dataset`, as BigQuery names a dataset in a dataset's `id` and in its messages.
    pub(super) fn legacy_id(&self) -> String {
        format!("{}:{}", self.project, self.dataset)
    }

    /// The `NotFound` BigQuery answers for a dataset that does not exist.
    pub(super) fn not_found(&self) -> Status {
        Status::not_found(format!("Not found: Dataset {}", self.legacy_id()))
    }

    /// The `AlreadyExists` BigQuery answers for creating a dataset that exists.
    pub(super) fn already_exists(&self) -> Status {
        Status::already_exists(format!("Already Exists: Dataset {}", self.legacy_id()))
    }
}

/// `project.dataset`.
impl Display for DatasetKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.project, self.dataset)
    }
}

/// A table, with its project resolved.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) struct TableKey {
    pub dataset: DatasetKey,
    pub table: BigQueryTableId,
}

impl TableKey {
    /// `table`, in `client_project` unless it names its own.
    pub(super) fn resolve(table: &BigQueryTableRef, client_project: &str) -> Self {
        Self {
            dataset: DatasetKey {
                project: table.project_or(client_project).to_string(),
                dataset: table.dataset().clone(),
            },
            table: table.table().clone(),
        }
    }

    /// The table a v2 request names by its IDs.
    ///
    /// # Errors
    /// [`BigQueryError::InvalidParametersError`] for an ID the client would never send.
    pub(super) fn new(project: &str, dataset: &str, table: &str) -> BigQueryResult<Self> {
        Ok(DatasetKey::new(project, dataset)?.table(BigQueryTableId::new(table)?))
    }

    /// The table of a Storage API resource name: `projects/{p}/datasets/{d}/tables/{t}`,
    /// followed by anything, such as `/streams/{s}`.
    ///
    /// # Errors
    /// [`BigQueryError::InvalidParametersError`] for a name of another shape or an ID the
    /// client would never send.
    pub(super) fn from_path(path: &str) -> BigQueryResult<Self> {
        let mut segments = path.split('/');
        let mut take = |label: &str| match (segments.next(), segments.next()) {
            (Some(found), Some(id)) if found == label => Ok(id),
            _ => Err(BigQueryError::invalid_parameters(
                "table",
                format!("{path:?} is not projects/*/datasets/*/tables/*"),
            )),
        };
        let project = take("projects")?;
        let dataset = take("datasets")?;
        let table = take("tables")?;
        Self::new(project, dataset, table)
    }

    /// The Storage API resource name, `projects/{p}/datasets/{d}/tables/{t}`.
    pub(super) fn path(&self) -> String {
        BigQueryTableRef::from(self).table_path(&self.dataset.project)
    }

    /// The v2 `TableReference`.
    pub(super) fn reference(&self) -> v2::TableReference {
        BigQueryTableRef::from(self).table_reference(&self.dataset.project)
    }

    /// `project:dataset.table`, as BigQuery names a table in a table's `id` and in its
    /// messages.
    pub(super) fn legacy_id(&self) -> String {
        format!(
            "{}:{}.{}",
            self.dataset.project, self.dataset.dataset, self.table
        )
    }

    /// The `NotFound` BigQuery answers for a table that does not exist.
    pub(super) fn not_found(&self) -> Status {
        Status::not_found(format!("Not found: Table {}", self.legacy_id()))
    }

    /// The `AlreadyExists` BigQuery answers for creating a table that exists.
    pub(super) fn already_exists(&self) -> Status {
        Status::already_exists(format!("Already Exists: Table {}", self.legacy_id()))
    }
}

/// The table, naming its project.
impl From<&TableKey> for BigQueryTableRef {
    fn from(key: &TableKey) -> Self {
        BigQueryTableRef::new(
            Some(key.dataset.project.clone()),
            key.dataset.dataset.clone(),
            key.table.clone(),
        )
    }
}

/// `project.dataset.table`.
impl Display for TableKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.dataset, self.table)
    }
}

/// The version of a dataset or a table. Every change takes a new one, so that a write whose
/// `if-match` names an older one is refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct FakeGeneration(u64);

impl FakeGeneration {
    /// The etag that names this generation.
    pub(super) fn etag(self) -> String {
        format!("fake-etag-{}", self.0)
    }
}

/// One dataset.
#[derive(Clone, Debug)]
pub(super) struct FakeDataset {
    pub generation: FakeGeneration,
    pub created: BigQueryInstant,
    /// The settings `InsertDataset` and `UpdateDataset` sent, which `GetDataset` returns. Its
    /// reference, etag and times come from the fields above.
    pub resource: v2::Dataset,
}

impl FakeDataset {
    fn new(generation: FakeGeneration) -> Self {
        Self {
            generation,
            created: BigQueryInstant::now(),
            resource: v2::Dataset::default(),
        }
    }

    /// The dataset as `GetDataset` returns it, in BigQuery's default location unless it was
    /// created in another.
    pub(super) fn resource(&self, key: &DatasetKey) -> v2::Dataset {
        let location = location_or_default(&self.resource.location);
        v2::Dataset {
            kind: "bigquery#dataset".into(),
            id: key.legacy_id(),
            dataset_reference: Some(key.reference()),
            etag: self.generation.etag(),
            creation_time: self.created.as_millisecond(),
            location,
            ..self.resource.clone()
        }
    }
}

/// One table: its schema, its visible rows and the CDC changes written to it.
#[derive(Clone, Debug)]
pub(super) struct FakeTable {
    pub schema: BigQueryTableSchema,
    /// The Arrow layout of `schema` that a read session sends, which every batch has.
    pub arrow_schema: SchemaRef,
    /// The visible rows, in the order they were acknowledged.
    pub batches: Vec<RecordBatch>,
    /// The CDC changes, in the order they were acknowledged. They are recorded and never
    /// applied to `batches`.
    pub changes: Vec<FakeChanges>,
    /// How many streams a read session of this table has.
    pub read_streams: NonZeroUsize,
    pub generation: FakeGeneration,
    pub created: BigQueryInstant,
    /// The settings `InsertTable`, `PatchTable` and `UpdateTable` sent, which `GetTable`
    /// returns. Its reference, schema, etag, row count and times come from the fields above.
    pub resource: v2::Table,
}

impl FakeTable {
    /// A table of `schema` holding `batches`, which have its read layout.
    pub(super) fn new(
        schema: BigQueryTableSchema,
        batches: Vec<RecordBatch>,
        read_streams: NonZeroUsize,
        generation: FakeGeneration,
    ) -> Self {
        Self {
            arrow_schema: Arc::new(schema.arrow_read_schema()),
            schema,
            batches,
            changes: Vec::new(),
            read_streams,
            generation,
            created: BigQueryInstant::now(),
            resource: v2::Table::default(),
        }
    }

    /// The visible row count.
    pub(super) fn num_rows(&self) -> usize {
        self.batches.iter().map(RecordBatch::num_rows).sum()
    }

    /// The table as `GetTable` returns it.
    pub(super) fn resource(&self, key: &TableKey) -> v2::Table {
        v2::Table {
            table_reference: Some(key.reference()),
            id: key.legacy_id(),
            schema: Some(v2::TableSchema::from(&self.schema)),
            etag: self.generation.etag(),
            num_rows: u64::try_from(self.num_rows()).ok(),
            creation_time: self.created.as_millisecond(),
            r#type: "TABLE".into(),
            ..self.resource.clone()
        }
    }
}

/// Rows written as CDC changes, and the change each row is.
#[derive(Clone, Debug)]
pub(super) struct FakeChanges {
    pub rows: RecordBatch,
    /// One per row of `rows`, in order.
    pub changes: Vec<FakeChange>,
}

/// What one CDC row asked for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct FakeChange {
    pub change_type: BigQueryChangeType,
    pub sequence_number: Option<BigQueryChangeSequenceNumber>,
}

/// One write stream.
#[derive(Clone, Debug)]
pub(super) struct FakeWriteStream {
    pub table: TableKey,
    /// Which decides when the stream's rows become visible.
    pub mode: BigQueryWriteMode,
    /// Every acknowledged append, visible or not, in order.
    pub appended: Vec<RecordBatch>,
    /// The rows of `appended`: the offset the next append must carry.
    pub length: i64,
    /// How many rows of a buffered stream `FlushRows` has made visible: the rows before this
    /// offset are visible.
    pub flushed: i64,
    pub finalized: bool,
    pub committed: bool,
}

impl FakeWriteStream {
    pub(super) fn new(table: TableKey, mode: BigQueryWriteMode) -> Self {
        Self {
            table,
            mode,
            appended: Vec::new(),
            length: 0,
            flushed: 0,
            finalized: false,
            committed: false,
        }
    }
}

/// One stream of a read session: the rows it serves, as the session projected them.
#[derive(Clone, Debug)]
pub(super) struct FakeReadStream {
    pub table: TableKey,
    pub arrow_schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
    /// The buffer compression the session asked for.
    pub compression: IpcCompression,
}

/// One query job.
#[derive(Clone, Debug)]
pub(super) struct FakeJob {
    pub location: String,
    pub statement_type: BigQueryStatementType,
    /// Whether the job has finished, as `GetQueryResults` and `GetJob` report it.
    pub complete: bool,
    pub cancelled: bool,
    /// The table that holds the result: the caller's destination, or a table of
    /// [`QUERY_RESULTS_DATASET`].
    pub destination: Option<TableKey>,
    pub total_rows: Option<u64>,
    pub bytes_processed: Option<i64>,
    pub dml: Option<BigQueryDmlStats>,
    /// The `error_result` of a failed job.
    pub failure: Option<BigQueryFakeJobFailure>,
}

/// Everything the fake holds.
#[derive(Debug, Default)]
pub(super) struct FakeState {
    pub datasets: BTreeMap<DatasetKey, FakeDataset>,
    pub tables: BTreeMap<TableKey, FakeTable>,
    /// By stream name, `projects/{p}/datasets/{d}/tables/{t}/streams/{s}`.
    pub write_streams: HashMap<BigQueryWriteStreamName, FakeWriteStream>,
    /// By stream name, as `CreateReadSession` returned it.
    pub read_streams: BTreeMap<String, FakeReadStream>,
    pub jobs: BTreeMap<BigQueryJobId, FakeJob>,
    last_id: u64,
}

impl FakeState {
    /// A number not handed out before, for job, query, session and stream names and for
    /// generations.
    pub(super) fn next_id(&mut self) -> u64 {
        self.last_id += 1;
        self.last_id
    }

    /// A generation not handed out before.
    pub(super) fn next_generation(&mut self) -> FakeGeneration {
        FakeGeneration(self.next_id())
    }

    /// Creates the dataset.
    ///
    /// # Errors
    /// `AlreadyExists` if the dataset exists.
    pub(super) fn create_dataset(&mut self, key: DatasetKey) -> Result<&mut FakeDataset, Status> {
        if self.datasets.contains_key(&key) {
            return Err(key.already_exists());
        }
        let dataset = FakeDataset::new(self.next_generation());
        Ok(self.datasets.entry(key).or_insert(dataset))
    }

    /// Creates the table, and its dataset unless it exists.
    ///
    /// # Errors
    /// [`BigQueryError::DataConflictError`] if the table exists.
    pub(super) fn create_table(&mut self, key: TableKey, table: FakeTable) -> BigQueryResult<()> {
        if self.tables.contains_key(&key) {
            return Err(BigQueryError::from(key.already_exists()));
        }
        if !self.datasets.contains_key(&key.dataset) {
            self.create_dataset(key.dataset.clone())?;
        }
        self.tables.insert(key, table);
        Ok(())
    }

    /// The table at `key`.
    ///
    /// # Errors
    /// [`BigQueryError::DataNotFoundError`] if there is none.
    pub(super) fn table(&self, key: &TableKey) -> BigQueryResult<&FakeTable> {
        self.tables
            .get(key)
            .ok_or_else(|| BigQueryError::from(key.not_found()))
    }

    /// Records `job` as `fake-job-<n>` and returns that ID.
    pub(super) fn add_job(&mut self, job: FakeJob) -> BigQueryJobId {
        let job_id = BigQueryJobId::new(format!("fake-job-{}", self.next_id()))
            .expect("letters, digits and dashes make a valid job ID");
        self.jobs.insert(job_id.clone(), job);
        job_id
    }
}

/// `batch` in `layout`, the read layout of `schema`, its columns matched to the table's by
/// name, ignoring case, as BigQuery does. A column the batch leaves out is NULL. Rows written or
/// scripted under an earlier schema of a table read the columns added since as NULL this way.
/// The fields of a RECORD column, REPEATED or not, are matched the same way, at every depth.
///
/// A decimal is rescaled to its column's scale, rounding half away from zero, as BigQuery
/// writes a value into a NUMERIC or BIGNUMERIC column of a declared precision and scale.
///
/// # Errors
/// A column or field the table does not have, one whose Arrow type is not the table's, a
/// decimal beyond its column's range, or a REQUIRED column or field with NULLs or left out.
pub(super) fn fit_to_layout(
    batch: &RecordBatch,
    schema: &BigQueryTableSchema,
    layout: &SchemaRef,
) -> Result<RecordBatch, String> {
    let columns = fit_fields(
        batch.schema().fields(),
        batch.columns(),
        layout.fields(),
        &schema.fields,
        batch.num_rows(),
    )?;
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(layout.clone(), columns, &options)
        .map_err(|err| format!("the Arrow rows do not fit the table: {err}"))
}

/// `columns`, the values of `fields`, `rows` long, as the fields of `layout`, the read layout
/// of `declared`, matched by name ignoring case. A field of `layout` that `fields` leaves out
/// is NULL.
fn fit_fields(
    fields: &Fields,
    columns: &[ArrayRef],
    layout: &Fields,
    declared: &[BigQueryFieldSchema],
    rows: usize,
) -> Result<Vec<ArrayRef>, String> {
    if let Some(extra) = fields.iter().find(|field| {
        !layout
            .iter()
            .any(|column| column.name().eq_ignore_ascii_case(field.name()))
    }) {
        return Err(format!(
            "the table has no column for the Arrow field {}",
            extra.name()
        ));
    }
    layout
        .iter()
        .zip(declared)
        .map(|(column, declared)| {
            let found = fields
                .iter()
                .position(|field| field.name().eq_ignore_ascii_case(column.name()));
            match found.and_then(|index| columns.get(index)) {
                Some(values) => fit_values(values, column, &declared.field_type),
                None => Ok(new_null_array(column.data_type(), rows)),
            }
        })
        .collect()
}

/// `values` as the values of `column`, the read layout of a column of `field_type`.
fn fit_values(
    values: &ArrayRef,
    column: &Field,
    field_type: &BigQueryFieldType,
) -> Result<ArrayRef, String> {
    let named = |err: ArrowError| format!("the Arrow field {}: {err}", column.name());
    let fitted: ArrayRef = match (values.data_type(), column.data_type()) {
        (DataType::Struct(fields), DataType::Struct(layout)) => {
            let record = values.as_struct();
            let declared = field_type.message_fields().ok_or_else(|| {
                format!(
                    "the Arrow field {} is a struct of a {field_type}",
                    column.name()
                )
            })?;
            let children = fit_fields(fields, record.columns(), layout, &declared, record.len())?;
            Arc::new(
                StructArray::try_new(layout.clone(), children, record.nulls().cloned())
                    .map_err(named)?,
            )
        }
        (DataType::List(_), DataType::List(item)) => {
            let list = values.as_list::<i32>();
            let items = fit_values(list.values(), item, field_type)?;
            Arc::new(
                ListArray::try_new(
                    item.clone(),
                    list.offsets().clone(),
                    items,
                    list.nulls().cloned(),
                )
                .map_err(named)?,
            )
        }
        (DataType::Decimal128(_, from_scale), DataType::Decimal128(precision, scale)) => {
            let rescale = decimal_rescale(column, *from_scale, Some(*precision), *scale)?;
            Arc::new(
                values
                    .as_primitive::<Decimal128Type>()
                    .try_unary::<_, Decimal128Type, _>(|value| {
                        rescale(i256::from_i128(value))?.to_i128().ok_or_else(|| {
                            format!("a value of {} does not fit 128 bits", column.name())
                        })
                    })?
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(named)?,
            )
        }
        (DataType::Decimal256(_, from_scale), DataType::Decimal256(precision, scale)) => {
            // An undeclared BIGNUMERIC holds about ±5.79e38, more than the 38 whole digits
            // its Arrow precision leaves at scale 38: every value that fits 256 bits.
            let bound = matches!(field_type, BigQueryFieldType::BigNumeric(Some(_)));
            let rescale =
                decimal_rescale(column, *from_scale, bound.then_some(*precision), *scale)?;
            Arc::new(
                values
                    .as_primitive::<Decimal256Type>()
                    .try_unary::<_, Decimal256Type, _>(rescale)?
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(named)?,
            )
        }
        (found, taken) if found == taken => values.clone(),
        (found, taken) => {
            return Err(format!(
                "the Arrow field {} is {found}, and its column takes {taken}",
                column.name()
            ))
        }
    };
    Ok(fitted)
}

/// The rescaling of the decimals of `column`, from `from_scale` to its `scale`, which fails
/// for a value with more digits than `precision`, or without one, beyond 256 bits.
fn decimal_rescale(
    column: &Field,
    from_scale: i8,
    precision: Option<u8>,
    scale: i8,
) -> Result<impl Fn(i256) -> Result<i256, String> + '_, String> {
    let unsigned = |scale: i8| {
        u8::try_from(scale)
            .map_err(|_| format!("the Arrow field {} has the scale {scale}", column.name()))
    };
    let from_scale = u32::from(unsigned(from_scale)?);
    let scale = unsigned(scale)?;
    Ok(move |value: i256| match precision {
        Some(precision) => BigQueryDecimalParams { precision, scale }
            .rescale(value, from_scale)
            .ok_or_else(|| {
                format!(
                    "a value of {} has more than {precision} digits",
                    column.name()
                )
            }),
        None => rescale_decimal(value, from_scale, u32::from(scale))
            .ok_or_else(|| format!("a value of {} does not fit 256 bits", column.name())),
    })
}
