//! Rows to batches: one `AppendRows` request's rows, proto or Arrow, become one Arrow batch in
//! the table's engine types.
//!
//! Both forms go through the wire types first, the types a column has on the wire: proto rows
//! are decoded into them, Arrow rows arrive in them. One conversion then takes the wire batch to
//! the engine types and checks every value against the column it lands in, so the two forms
//! share every rule: a NULL in a REQUIRED column, a STRING or BYTES over its `max_length`,
//! invalid JSON text, GeoJSON for GEOGRAPHY, a NUMERIC beyond its precision. A request with any
//! such row writes nothing.

use crate::catalog::LoadedTable;
use crate::error::EngineError;
use crate::types::{Column, ColumnName, ColumnType, Columns, Mode, RangeElement, NUMERIC_DEFAULT};
use arrow::array::{
    new_empty_array, new_null_array, Array, ArrayRef, AsArray, BinaryBuilder, BooleanBuilder,
    Date32Builder, Decimal128Builder, Float64Builder, Int64Builder, ListArray, RecordBatch,
    RecordBatchOptions, StringArray, StringBuilder, StructArray, Time64MicrosecondBuilder,
    TimestampMicrosecondBuilder, UInt32Array,
};
use arrow::buffer::{Buffer, NullBuffer, OffsetBuffer};
use arrow::compute::{cast_with_options, take, CastOptions};
use arrow::datatypes::{
    DataType, Decimal128Type, Field, FieldRef, Fields, Schema, SchemaRef, TimeUnit,
};
use arrow::error::ArrowError;
use arrow::ipc::reader::StreamDecoder;
use gcloud_sdk::google::cloud::bigquery::storage::v1::append_rows_request::MissingValueInterpretation;
use gcloud_sdk::google::cloud::bigquery::storage::v1::ProtoSchema;
use gcloud_sdk::prost_types::{FileDescriptorProto, FileDescriptorSet};
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor, ReflectMessage, Value};
use std::collections::HashMap;
use std::sync::Arc;

/// The field a change batch declares, which the change log takes and nothing else does yet.
const CHANGE_TYPE: &str = "_CHANGE_TYPE";

/// One row's problem: the row's index in its request and what is wrong, naming the field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RowError {
    pub index: u64,
    pub message: String,
}

/// Why a request's rows cannot be written. Nothing of the request is written in any case.
#[derive(Debug)]
pub(crate) enum Rejected {
    /// Rows whose values the table's columns do not take.
    Rows(Vec<RowError>),
    /// Fields the writer sends that the table does not have.
    ExtraFields(Vec<String>),
    /// The request itself cannot be used.
    Request(EngineError),
}

impl From<EngineError> for Rejected {
    fn from(error: EngineError) -> Self {
        Rejected::Request(error)
    }
}

/// Rows the engine itself produced, a query's result, that a table does not take: the
/// statement fails.
impl From<Rejected> for EngineError {
    fn from(rejected: Rejected) -> Self {
        match rejected {
            Rejected::Rows(errors) => EngineError::InvalidQuery {
                detail: errors
                    .iter()
                    .map(|error| format!("row {}: {}", error.index, error.message))
                    .collect::<Vec<_>>()
                    .join("; "),
            },
            Rejected::ExtraFields(fields) => EngineError::InvalidQuery {
                detail: format!(
                    "the result has columns the destination table does not: {}",
                    fields.join(", ")
                ),
            },
            Rejected::Request(error) => error,
        }
    }
}

/// What the writer said its rows are: a proto descriptor or an Arrow schema, sent with the first
/// request of a connection and with any later one that changes it.
pub(crate) enum WriterSchema {
    Proto(MessageDescriptor),
    Arrow(ArrowRows),
}

/// The proto rows' message, from the bare `DescriptorProto` the writer sends: a proto2 file
/// holding it, its nested types flattened beside it and named relative to it.
impl TryFrom<&ProtoSchema> for WriterSchema {
    type Error = EngineError;

    fn try_from(schema: &ProtoSchema) -> Result<Self, Self::Error> {
        let descriptor = schema.proto_descriptor.clone().ok_or_else(|| {
            EngineError::invalid("writer_schema", "a proto schema carries a descriptor")
        })?;
        let name = descriptor.name().to_string();
        let file = FileDescriptorProto {
            name: Some("smallquery_writer.proto".to_string()),
            syntax: Some("proto2".to_string()),
            message_type: vec![descriptor],
            ..Default::default()
        };
        let pool = DescriptorPool::from_file_descriptor_set(FileDescriptorSet { file: vec![file] })
            .map_err(|err| EngineError::invalid("writer_schema", err.to_string()))?;
        let message = pool.get_message_by_name(&name).ok_or_else(|| {
            EngineError::invalid("writer_schema", format!("no message named {name:?}"))
        })?;
        Ok(WriterSchema::Proto(message))
    }
}

/// Arrow rows: the writer's schema message, kept so that each request's record batch message
/// decodes on its own.
pub(crate) struct ArrowRows {
    schema_message: Vec<u8>,
}

impl ArrowRows {
    pub(crate) fn new(schema_message: Vec<u8>) -> Self {
        Self { schema_message }
    }

    /// The batch one record batch message holds.
    fn decode(&self, batch_message: &[u8]) -> Result<RecordBatch, EngineError> {
        let invalid = |detail: String| EngineError::invalid("arrow_rows", detail);
        let mut decoder = StreamDecoder::new();
        let mut schema = Buffer::from(self.schema_message.clone());
        while !schema.is_empty() {
            if decoder
                .decode(&mut schema)
                .map_err(|err| invalid(err.to_string()))?
                .is_some()
            {
                return Err(invalid("the schema message holds a record batch".into()));
            }
        }
        let mut message = Buffer::from(batch_message.to_vec());
        let mut decoded = None;
        while !message.is_empty() {
            if let Some(batch) = decoder
                .decode(&mut message)
                .map_err(|err| invalid(err.to_string()))?
            {
                decoded = Some(batch);
            }
        }
        decoded.ok_or_else(|| invalid("the request holds no complete record batch".into()))
    }
}

/// The rows of one request, as the writer sent them.
pub(crate) enum RequestRows<'r> {
    Proto(&'r [Vec<u8>]),
    Arrow(&'r [u8]),
}

/// What a field the writer leaves out gets, per column and by default.
#[derive(Debug, Clone, Default)]
pub(crate) struct MissingValues {
    pub default: MissingValueInterpretation,
    pub per_column: HashMap<ColumnName, MissingValueInterpretation>,
}

/// The table a write lands in: its declared columns and the engine Arrow schema, with the
/// field ids, that its data files carry.
pub(crate) struct WriteTarget {
    columns: Columns,
    engine: SchemaRef,
}

impl WriteTarget {
    pub(crate) fn new(table: &LoadedTable) -> Result<Self, EngineError> {
        let metadata = table.metadata();
        let schema = metadata.current_schema();
        Ok(Self {
            columns: Columns::from_iceberg(schema, metadata.properties())?,
            engine: Arc::new(iceberg::arrow::schema_to_arrow_schema(schema)?),
        })
    }

    pub(crate) fn columns(&self) -> &Columns {
        &self.columns
    }

    /// The request's rows in the table's engine types.
    pub(crate) fn batch(
        &self,
        writer: &WriterSchema,
        rows: RequestRows<'_>,
        missing: &MissingValues,
    ) -> Result<RecordBatch, Rejected> {
        let wire = match (writer, rows) {
            (WriterSchema::Proto(message), RequestRows::Proto(rows)) => {
                self.proto_rows(message, rows)?
            }
            (WriterSchema::Arrow(arrow), RequestRows::Arrow(batch)) => {
                self.arrow_rows(arrow.decode(batch)?)?
            }
            _ => {
                return Err(Rejected::Request(EngineError::invalid(
                    "rows",
                    "the rows are not in the form the writer schema declared",
                )))
            }
        };
        self.engine_batch(&wire, missing)
    }

    /// A batch in wire types, a query's result, in the table's engine types; its fields are
    /// matched to the columns by name and the columns it leaves out are NULL.
    pub(crate) fn wire_rows(&self, batch: RecordBatch) -> Result<RecordBatch, Rejected> {
        let wire = self.arrow_rows(batch)?;
        self.engine_batch(&wire, &MissingValues::default())
    }

    /// Proto rows in the wire types of the columns the descriptor names.
    fn proto_rows(
        &self,
        message: &MessageDescriptor,
        rows: &[Vec<u8>],
    ) -> Result<RecordBatch, Rejected> {
        if message.get_field_by_name(CHANGE_TYPE).is_some() {
            return Err(Rejected::Request(EngineError::unsupported(
                CHANGE_TYPE,
                "change batches of the CDC change log are not written by this release",
            )));
        }
        let mut columns = Vec::new();
        let mut extra = Vec::new();
        for field in message.fields() {
            match self.columns.find(field.name()) {
                Some(column) => columns.push(column),
                None => extra.push(field.name().to_string()),
            }
        }
        if !extra.is_empty() {
            return Err(Rejected::ExtraFields(extra));
        }
        let mut builders: Vec<WireBuilder> = columns.iter().map(|c| WireBuilder::new(c)).collect();
        let mut errors = Vec::new();
        for (index, row) in rows.iter().enumerate() {
            let index = u64::try_from(index).unwrap_or(u64::MAX);
            let decoded = match DynamicMessage::decode(message.clone(), row.as_slice()) {
                Ok(decoded) => decoded,
                Err(err) => {
                    errors.push(RowError {
                        index,
                        message: format!("the row is not a valid proto message: {err}"),
                    });
                    builders.iter_mut().for_each(WireBuilder::append_null);
                    continue;
                }
            };
            for (builder, column) in builders.iter_mut().zip(&columns) {
                if let Err(problem) = builder.append_field(&decoded, column.name.as_str()) {
                    errors.push(RowError {
                        index,
                        message: format!("field {}: {problem}", column.name),
                    });
                }
            }
        }
        if !errors.is_empty() {
            return Err(Rejected::Rows(errors));
        }
        let internal = |err: ArrowError| {
            Rejected::Request(EngineError::Internal {
                detail: format!("decoded proto rows do not form a batch: {err}"),
            })
        };
        let arrays = builders
            .into_iter()
            .map(WireBuilder::finish)
            .collect::<Result<Vec<_>, _>>()
            .map_err(internal)?;
        let fields: Fields = columns
            .iter()
            .zip(&arrays)
            .map(|(column, array)| {
                Field::new(column.name.as_str(), array.data_type().clone(), true)
            })
            .collect();
        RecordBatch::try_new_with_options(
            Arc::new(Schema::new(fields)),
            arrays,
            &RecordBatchOptions::new().with_row_count(Some(rows.len())),
        )
        .map_err(internal)
    }

    /// Arrow rows with every field matched to a column, renamed to the column's name.
    fn arrow_rows(&self, batch: RecordBatch) -> Result<RecordBatch, Rejected> {
        let schema = batch.schema();
        if schema.field_with_name(CHANGE_TYPE).is_ok() {
            return Err(Rejected::Request(EngineError::invalid(
                CHANGE_TYPE,
                "change batches are taken as proto rows only",
            )));
        }
        let mut fields = Vec::new();
        let mut extra = Vec::new();
        for field in schema.fields() {
            match self.columns.find(field.name()) {
                Some(column) => fields.push(Arc::new(
                    field.as_ref().clone().with_name(column.name.as_str()),
                )),
                None => extra.push(field.name().clone()),
            }
        }
        if !extra.is_empty() {
            return Err(Rejected::ExtraFields(extra));
        }
        RecordBatch::try_new_with_options(
            Arc::new(Schema::new(fields)),
            batch.columns().to_vec(),
            &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
        )
        .map_err(|err| Rejected::Request(EngineError::invalid("arrow_rows", err.to_string())))
    }

    /// The wire batch in the engine types: every column of the table, the ones the batch leaves
    /// out as `missing` says, each value checked against its column.
    fn engine_batch(
        &self,
        wire: &RecordBatch,
        missing: &MissingValues,
    ) -> Result<RecordBatch, Rejected> {
        let rows = wire.num_rows();
        let identity: Vec<Option<u64>> = (0..u64::try_from(rows).unwrap_or(u64::MAX))
            .map(Some)
            .collect();
        let mut problems = Problems::default();
        let mut arrays = Vec::with_capacity(self.engine.fields().len());
        for engine_field in self.engine.fields() {
            let array = match self.columns.get(engine_field.name()) {
                Some(column) => {
                    let sent = wire.column_by_name(column.name.as_str());
                    if sent.is_none() {
                        missing.check(column)?;
                    }
                    let path = column.name.to_string();
                    let conversion = Conversion {
                        path: &path,
                        rows: &identity,
                        problems: &mut problems,
                    };
                    conversion.convert(column, engine_field, sent, rows)?
                }
                // A hidden column of the engine's, which no writer sends.
                None => new_null_array(engine_field.data_type(), rows),
            };
            arrays.push(array);
        }
        if !problems.0.is_empty() {
            let mut errors = problems.0;
            errors.sort_by_key(|error| error.index);
            return Err(Rejected::Rows(errors));
        }
        RecordBatch::try_new_with_options(
            self.engine.clone(),
            arrays,
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )
        .map_err(|err| {
            Rejected::Request(EngineError::Internal {
                detail: format!("converted rows do not form a batch of the table: {err}"),
            })
        })
    }
}

impl Columns {
    /// The top-level column a writer's field names.
    fn find(&self, name: &str) -> Option<&Column> {
        by_name(self.iter(), name, |column| column.name.as_str())
    }
}

impl MissingValues {
    /// Refuses a column left out of a request whose interpretation asks for its default value
    /// expression, which this release does not evaluate on write.
    fn check(&self, column: &Column) -> Result<(), EngineError> {
        let interpretation = self
            .per_column
            .get(&column.name)
            .copied()
            .unwrap_or(self.default);
        if interpretation == MissingValueInterpretation::DefaultValue
            && column.default_value.is_some()
        {
            return Err(EngineError::unsupported(
                "DEFAULT_VALUE missing value interpretation",
                format!(
                    "column `{}` has a default value expression, which writes do not evaluate",
                    column.name
                ),
            ));
        }
        Ok(())
    }
}

/// The row problems of one request.
#[derive(Default)]
struct Problems(Vec<RowError>);

impl Problems {
    /// Records `problem` for the request row `row`; a value with no row of its own, a field of a
    /// NULL struct, has no problems.
    fn push(&mut self, row: Option<u64>, path: &str, problem: impl std::fmt::Display) {
        if let Some(index) = row {
            self.0.push(RowError {
                index,
                message: format!("field {path}: {problem}"),
            });
        }
    }
}

/// One step of the conversion from wire to engine types: the path of the field being converted,
/// for messages, and the request row each of its values belongs to, `None` for a value under a
/// NULL struct, which is no value of its own.
struct Conversion<'c> {
    path: &'c str,
    rows: &'c [Option<u64>],
    problems: &'c mut Problems,
}

impl Conversion<'_> {
    /// The engine array of `column`, from the wire array `sent` of `len` values, or from nothing
    /// when the writer left the column out.
    fn convert(
        mut self,
        column: &Column,
        engine_field: &Field,
        sent: Option<&ArrayRef>,
        len: usize,
    ) -> Result<ArrayRef, Rejected> {
        let Some(sent) = sent else {
            return Ok(self.absent(column, engine_field, len));
        };
        match column.mode {
            Mode::Repeated => self.repeated(column, engine_field, sent),
            mode => {
                let converted = self.value(&column.column_type, engine_field, sent)?;
                if mode == Mode::Required {
                    self.require(&converted, "a REQUIRED field has no value");
                }
                Ok(converted)
            }
        }
    }

    /// A column the writer left out: NULL, an empty array when REPEATED, and a problem in every
    /// row when REQUIRED.
    fn absent(self, column: &Column, engine_field: &Field, len: usize) -> ArrayRef {
        match (column.mode, engine_field.data_type()) {
            (Mode::Repeated, DataType::List(item)) => ListArray::try_new(
                item.clone(),
                OffsetBuffer::new_zeroed(len),
                new_empty_array(item.data_type()),
                None,
            )
            .map_or_else(
                |_| new_null_array(engine_field.data_type(), len),
                |list| Arc::new(list) as ArrayRef,
            ),
            (Mode::Required, data_type) => {
                for row in self.rows {
                    self.problems
                        .push(*row, self.path, "a REQUIRED field has no value");
                }
                new_null_array(data_type, len)
            }
            (_, data_type) => new_null_array(data_type, len),
        }
    }

    /// Records `problem` for every NULL of `array`.
    fn require(&mut self, array: &ArrayRef, problem: &str) {
        if array.null_count() == 0 {
            return;
        }
        for (position, row) in self.rows.iter().enumerate() {
            if array.is_null(position) {
                self.problems.push(*row, self.path, problem);
            }
        }
    }

    /// A REPEATED column: a NULL array is an empty one, and no element may be NULL.
    fn repeated(
        self,
        column: &Column,
        engine_field: &Field,
        sent: &ArrayRef,
    ) -> Result<ArrayRef, Rejected> {
        let DataType::List(engine_item) = engine_field.data_type() else {
            return Err(Rejected::Request(EngineError::Internal {
                detail: format!("the engine type of REPEATED `{}` is not a list", self.path),
            }));
        };
        let list = match sent.data_type() {
            DataType::List(item) => {
                let list = self.cast(
                    sent,
                    &DataType::List(Arc::new(item.as_ref().clone().with_name("item"))),
                )?;
                list.as_list::<i32>().clone()
            }
            other => {
                return Err(self.mismatch(format!("a REPEATED column takes a list, not {other}")))
            }
        };
        // The elements of every non-NULL array, in order; a NULL array contributes none.
        let mut indices = Vec::new();
        let mut element_rows = Vec::new();
        let mut offsets = Vec::with_capacity(list.len() + 1);
        offsets.push(0i32);
        for (position, row) in self.rows.iter().enumerate() {
            if list.is_valid(position) {
                let (start, end) = (
                    list.value_offsets()[position],
                    list.value_offsets()[position + 1],
                );
                for element in start..end {
                    indices.push(u32::try_from(element).map_err(|err| self.mismatch(err))?);
                    element_rows.push(*row);
                }
            }
            let length = i32::try_from(indices.len()).map_err(|err| self.mismatch(err))?;
            offsets.push(length);
        }
        let values = take(list.values().as_ref(), &UInt32Array::from(indices), None)
            .map_err(|err| self.mismatch(err))?;
        let path = format!("{}[]", self.path);
        let mut element = Conversion {
            path: &path,
            rows: &element_rows,
            problems: self.problems,
        };
        let converted = element.value(&column.column_type, engine_item, &values)?;
        element.require(&converted, "an array element is NULL");
        let list = ListArray::try_new(
            engine_item.clone(),
            OffsetBuffer::new(offsets.into()),
            converted,
            None,
        );
        element.or_rejected(list.map(|list| Arc::new(list) as ArrayRef), engine_field)
    }

    /// One value of `column_type` per position of `sent`, in the engine type of `engine_field`.
    fn value(
        &mut self,
        column_type: &ColumnType,
        engine_field: &Field,
        sent: &ArrayRef,
    ) -> Result<ArrayRef, Rejected> {
        let target = engine_field.data_type();
        match column_type {
            ColumnType::Struct(columns) => self.structure(columns, engine_field, sent),
            ColumnType::Range(element) => {
                self.structure(&element.bound_columns()?, engine_field, sent)
            }
            ColumnType::Numeric(params) => {
                let params = params.unwrap_or(NUMERIC_DEFAULT);
                self.numeric(sent, params.precision, params.scale)
            }
            ColumnType::String { max_length } => {
                let strings = self.strings(sent)?;
                if let Some(limit) = max_length {
                    self.each_string(&strings, |text| {
                        (u64::try_from(text.chars().count()).unwrap_or(u64::MAX) > *limit)
                            .then(|| format!("the value is longer than the maximum of {limit}"))
                    });
                }
                Ok(Arc::new(strings))
            }
            ColumnType::Json => {
                let strings = self.strings(sent)?;
                self.each_string(&strings, |text| {
                    serde_json::from_str::<serde::de::IgnoredAny>(text)
                        .err()
                        .map(|err| format!("the value is not valid JSON: {err}"))
                });
                Ok(Arc::new(strings))
            }
            ColumnType::Geography => {
                let strings = self.strings(sent)?;
                self.each_string(&strings, |text| {
                    text.trim_start()
                        .starts_with('{')
                        .then(|| "GeoJSON is not taken; a GEOGRAPHY value is WKT text".to_string())
                });
                Ok(Arc::new(strings))
            }
            ColumnType::Bytes { max_length } => {
                let bytes = self.cast(sent, target)?;
                if let Some(limit) = max_length {
                    let binary = bytes.as_binary::<i64>();
                    for (position, row) in self.rows.iter().enumerate() {
                        if binary.is_valid(position)
                            && u64::try_from(binary.value(position).len()).unwrap_or(u64::MAX)
                                > *limit
                        {
                            self.problems.push(
                                *row,
                                self.path,
                                format!("the value is longer than the maximum of {limit} bytes"),
                            );
                        }
                    }
                }
                Ok(bytes)
            }
            _ => self.cast(sent, target),
        }
    }

    /// A STRUCT or RANGE value: each field converted against its own column, the struct NULL
    /// where the writer sent NULL. The fields of a NULL struct are no values of their own, so
    /// a REQUIRED field of one is not missing.
    fn structure(
        &mut self,
        columns: &Columns,
        engine_field: &Field,
        sent: &ArrayRef,
    ) -> Result<ArrayRef, Rejected> {
        let DataType::Struct(engine_children) = engine_field.data_type() else {
            return Err(Rejected::Request(EngineError::Internal {
                detail: format!("the engine type of `{}` is not a struct", self.path),
            }));
        };
        let DataType::Struct(_) = sent.data_type() else {
            return Err(self.mismatch(format!("a struct value is sent as {}", sent.data_type())));
        };
        let sent = sent.as_struct();
        let nulls = sent.nulls().cloned();
        let child_rows: Vec<Option<u64>> = self
            .rows
            .iter()
            .enumerate()
            .map(|(position, row)| row.filter(|_| sent.is_valid(position)))
            .collect();
        let mut children = Vec::with_capacity(engine_children.len());
        for child in engine_children {
            let Some(column) = columns.get(child.name()) else {
                children.push(new_null_array(child.data_type(), sent.len()));
                continue;
            };
            let path = format!("{}.{}", self.path, column.name);
            let child_sent = by_name(
                sent.fields().iter().zip(sent.columns()),
                column.name.as_str(),
                |(field, _)| field.name(),
            )
            .map(|(_, child)| child);
            let conversion = Conversion {
                path: &path,
                rows: &child_rows,
                problems: self.problems,
            };
            children.push(conversion.convert(column, child, child_sent, sent.len())?);
        }
        let array = StructArray::try_new(engine_children.clone(), children, nulls);
        self.or_rejected(array.map(|array| Arc::new(array) as ArrayRef), engine_field)
    }

    /// NUMERIC values rescaled to `scale`, rounded half away from zero as BigQuery rounds them,
    /// and checked against `precision`.
    fn numeric(
        &mut self,
        sent: &ArrayRef,
        precision: u32,
        scale: u32,
    ) -> Result<ArrayRef, Rejected> {
        let DataType::Decimal128(_, sent_scale) = sent.data_type() else {
            return Err(self.mismatch(format!("a NUMERIC value is sent as {}", sent.data_type())));
        };
        let sent_scale = i32::from(*sent_scale);
        let values = sent.as_primitive::<Decimal128Type>();
        let target_scale = i32::try_from(scale).unwrap_or(9);
        let bound = 10i128.checked_pow(precision).unwrap_or(i128::MAX);
        let mut out = Decimal128Builder::with_capacity(values.len());
        for (position, row) in self.rows.iter().enumerate() {
            if values.is_null(position) {
                out.append_null();
                continue;
            }
            match rescale(values.value(position), sent_scale, target_scale) {
                Some(value) if value.abs() < bound => out.append_value(value),
                _ => {
                    self.problems.push(
                        *row,
                        self.path,
                        format!("the value is outside NUMERIC({precision}, {scale})"),
                    );
                    out.append_null();
                }
            }
        }
        let array = out
            .finish()
            .with_precision_and_scale(
                u8::try_from(precision).unwrap_or(38),
                i8::try_from(scale).unwrap_or(9),
            )
            .map_err(|err| self.mismatch(err))?;
        Ok(Arc::new(array))
    }

    /// The sent values as `Utf8`.
    fn strings(&self, sent: &ArrayRef) -> Result<StringArray, Rejected> {
        let cast = self.cast(sent, &DataType::Utf8)?;
        Ok(cast.as_string::<i32>().clone())
    }

    /// Records the problem `check` finds in each non-NULL string.
    fn each_string(&mut self, strings: &StringArray, check: impl Fn(&str) -> Option<String>) {
        for (position, row) in self.rows.iter().enumerate() {
            if strings.is_valid(position) {
                if let Some(problem) = check(strings.value(position)) {
                    self.problems.push(*row, self.path, problem);
                }
            }
        }
    }

    /// `sent` in `target`, the engine type of its column. The two may differ only as the type
    /// tables relabel a wire type into its engine type, which loses nothing; any other sent
    /// type is not the column's, and a cast would round or reformat the values, as `2.7` into
    /// INT64 or `12` into STRING.
    fn cast(&self, sent: &ArrayRef, target: &DataType) -> Result<ArrayRef, Rejected> {
        if sent.data_type() == target {
            return Ok(sent.clone());
        }
        if !relabels(sent.data_type(), target) {
            return Err(self.mismatch(format!(
                "a value of Arrow type {} is not of the column's type {target}",
                sent.data_type()
            )));
        }
        cast_with_options(
            sent,
            target,
            &CastOptions {
                safe: false,
                ..Default::default()
            },
        )
        .map_err(|err| self.mismatch(err))
    }

    /// The nested array `built`, or, when the rows already have problems that make it invalid,
    /// a NULL stand-in: the request is rejected for those problems, never for the stand-in.
    fn or_rejected(
        &self,
        built: Result<ArrayRef, ArrowError>,
        engine_field: &Field,
    ) -> Result<ArrayRef, Rejected> {
        match built {
            Ok(array) => Ok(array),
            Err(_) if !self.problems.0.is_empty() => {
                Ok(new_null_array(engine_field.data_type(), self.rows.len()))
            }
            Err(err) => Err(Rejected::Request(EngineError::Internal {
                detail: format!(
                    "converted values of `{}` do not form an array: {err}",
                    self.path
                ),
            })),
        }
    }

    /// A field whose sent type does not convert to its column's type.
    fn mismatch(&self, detail: impl std::fmt::Display) -> Rejected {
        Rejected::Request(EngineError::invalid(
            format!("field {}", self.path),
            detail.to_string(),
        ))
    }
}

impl RangeElement {
    /// The column type of one bound.
    fn column_type(self) -> ColumnType {
        match self {
            RangeElement::Date => ColumnType::Date,
            RangeElement::DateTime => ColumnType::DateTime,
            RangeElement::Timestamp => ColumnType::Timestamp,
        }
    }

    /// The two bounds of a RANGE value as columns: `start` and `end`, NULL when unbounded.
    fn bound_columns(self) -> Result<Columns, EngineError> {
        let bound = |name: &str| -> Result<Column, EngineError> {
            Ok(Column {
                name: name.to_string().try_into()?,
                column_type: self.column_type(),
                mode: Mode::Nullable,
                description: None,
                default_value: None,
            })
        };
        Columns::try_from(vec![bound("start")?, bound("end")?])
    }
}

/// The item of `items` that `name` names: the one of exactly that name, else the one of that
/// name without case, which BigQuery's uniqueness rule keeps unambiguous.
fn by_name<T>(
    items: impl IntoIterator<Item = T>,
    name: &str,
    name_of: impl Fn(&T) -> &str,
) -> Option<T> {
    let mut folded = None;
    for item in items {
        let item_name = name_of(&item);
        if item_name == name {
            return Some(item);
        }
        if folded.is_none() && item_name.eq_ignore_ascii_case(name) {
            folded = Some(item);
        }
    }
    folded
}

/// Whether a wire array of type `from` relabels into the engine type `to` without changing a
/// value: BYTES from `Binary` to `LargeBinary`, a TIMESTAMP's zone label from `UTC` to
/// `+00:00`, and a list's element field.
fn relabels(from: &DataType, to: &DataType) -> bool {
    let utc = |zone: &Option<Arc<str>>| {
        zone.as_deref()
            .is_some_and(|zone| ["UTC", "+00:00"].contains(&zone))
    };
    match (from, to) {
        (DataType::Binary, DataType::LargeBinary) => true,
        (
            DataType::Timestamp(TimeUnit::Microsecond, from_zone),
            DataType::Timestamp(TimeUnit::Microsecond, to_zone),
        ) => utc(from_zone) && utc(to_zone),
        (DataType::List(from), DataType::List(to)) => {
            from.data_type() == to.data_type() || relabels(from.data_type(), to.data_type())
        }
        _ => false,
    }
}

/// `value` at `from` scale, at `to` scale: exact when the scale grows, rounded half away from
/// zero when it shrinks. `None` when the value does not fit 128 bits.
fn rescale(value: i128, from: i32, to: i32) -> Option<i128> {
    if to >= from {
        let factor = 10i128.checked_pow(u32::try_from(to - from).ok()?)?;
        return value.checked_mul(factor);
    }
    let divisor = 10i128.checked_pow(u32::try_from(from - to).ok()?)?;
    let quotient = value / divisor;
    let remainder = value % divisor;
    let half = divisor / 2 + divisor % 2;
    Some(if remainder.abs() >= half {
        quotient + value.signum()
    } else {
        quotient
    })
}

/// Collects one column's proto values in its wire type.
enum WireBuilder {
    Int64(Int64Builder),
    Float64(Float64Builder),
    Boolean(BooleanBuilder),
    Date(Date32Builder),
    Time(Time64MicrosecondBuilder),
    DateTime(TimestampMicrosecondBuilder),
    Timestamp(TimestampMicrosecondBuilder),
    Numeric(Decimal128Builder),
    Utf8(StringBuilder),
    Binary(BinaryBuilder),
    /// A STRUCT or RANGE: a builder per field, by the field's name.
    Struct {
        fields: Fields,
        children: Vec<(String, WireBuilder)>,
        valid: Vec<bool>,
    },
    /// A REPEATED column's arrays.
    List {
        item: FieldRef,
        values: Box<WireBuilder>,
        offsets: Vec<i32>,
    },
}

impl WireBuilder {
    /// A builder of `column`'s values, of arrays of them when REPEATED.
    fn new(column: &Column) -> Self {
        let value = Self::of_type(&column.column_type);
        match (column.mode, column.wire_field().data_type()) {
            (Mode::Repeated, DataType::List(item)) => WireBuilder::List {
                item: item.clone(),
                values: Box::new(value),
                offsets: vec![0],
            },
            _ => value,
        }
    }

    fn of_type(column_type: &ColumnType) -> Self {
        match column_type {
            ColumnType::Int64 => WireBuilder::Int64(Int64Builder::new()),
            ColumnType::Float64 => WireBuilder::Float64(Float64Builder::new()),
            ColumnType::Bool => WireBuilder::Boolean(BooleanBuilder::new()),
            ColumnType::Date => WireBuilder::Date(Date32Builder::new()),
            ColumnType::Time => WireBuilder::Time(Time64MicrosecondBuilder::new()),
            ColumnType::DateTime => WireBuilder::DateTime(TimestampMicrosecondBuilder::new()),
            ColumnType::Timestamp => {
                WireBuilder::Timestamp(TimestampMicrosecondBuilder::new().with_timezone(WIRE_UTC))
            }
            ColumnType::Numeric(_) => WireBuilder::Numeric(Decimal128Builder::new()),
            ColumnType::String { .. } | ColumnType::Json | ColumnType::Geography => {
                WireBuilder::Utf8(StringBuilder::new())
            }
            ColumnType::Bytes { .. } => WireBuilder::Binary(BinaryBuilder::new()),
            ColumnType::Range(element) => {
                let bound = |name: &str| (name.to_string(), Self::of_type(&element.column_type()));
                WireBuilder::Struct {
                    fields: element.wire_fields(),
                    children: vec![bound("start"), bound("end")],
                    valid: Vec::new(),
                }
            }
            ColumnType::Struct(columns) => WireBuilder::Struct {
                fields: columns.wire_fields(),
                children: columns
                    .iter()
                    .map(|column| (column.name.to_string(), WireBuilder::new(column)))
                    .collect(),
                valid: Vec::new(),
            },
        }
    }

    /// Appends `message`'s value of the field `name`; NULL when the message does not set it.
    fn append_field(&mut self, message: &DynamicMessage, name: &str) -> Result<(), String> {
        let field = by_name(message.descriptor().fields(), name, |field| field.name());
        match field {
            Some(field) if field.is_list() || message.has_field(&field) => {
                self.append(&message.get_field(&field))
            }
            _ => {
                self.append_null();
                Ok(())
            }
        }
    }

    /// Appends one value. A value of the wrong proto type appends NULL and returns the problem.
    fn append(&mut self, value: &Value) -> Result<(), String> {
        let result = match (&mut *self, value) {
            (WireBuilder::Int64(b), Value::I64(v)) => {
                b.append_value(*v);
                Ok(())
            }
            (WireBuilder::Timestamp(b), Value::I64(v)) => {
                b.append_value(*v);
                Ok(())
            }
            (WireBuilder::Float64(b), Value::F64(v)) => {
                b.append_value(*v);
                Ok(())
            }
            (WireBuilder::Boolean(b), Value::Bool(v)) => {
                b.append_value(*v);
                Ok(())
            }
            (WireBuilder::Date(b), Value::I32(v)) => {
                b.append_value(*v);
                Ok(())
            }
            (WireBuilder::Time(b), Value::I64(packed)) => {
                unpack_time(*packed).map(|micros| b.append_value(micros))
            }
            (WireBuilder::DateTime(b), Value::I64(packed)) => {
                unpack_datetime(*packed).map(|micros| b.append_value(micros))
            }
            (WireBuilder::Numeric(b), Value::Bytes(bytes)) => {
                little_endian_i128(bytes).map(|v| b.append_value(v))
            }
            (WireBuilder::Utf8(b), Value::String(v)) => {
                b.append_value(v);
                Ok(())
            }
            (WireBuilder::Binary(b), Value::Bytes(v)) => {
                b.append_value(v);
                Ok(())
            }
            (
                WireBuilder::Struct {
                    children, valid, ..
                },
                Value::Message(message),
            ) => {
                valid.push(true);
                let mut problem = Ok(());
                for (name, child) in children {
                    if let Err(err) = child.append_field(message, name) {
                        problem = Err(format!("{name}: {err}"));
                    }
                }
                return problem;
            }
            (
                WireBuilder::List {
                    values, offsets, ..
                },
                Value::List(elements),
            ) => {
                let mut problem = Ok(());
                for element in elements {
                    if let Err(err) = values.append(element) {
                        problem = Err(err);
                    }
                }
                let length = offsets.last().copied().unwrap_or(0);
                let added = i32::try_from(elements.len()).unwrap_or(i32::MAX);
                offsets.push(length.saturating_add(added));
                return problem;
            }
            (builder, value) => Err(format!(
                "a {} value is sent as proto type {}",
                builder.describe(),
                describe_value(value)
            )),
        };
        if result.is_err() {
            self.append_null();
        }
        result
    }

    fn append_null(&mut self) {
        match self {
            WireBuilder::Int64(b) => b.append_null(),
            WireBuilder::Float64(b) => b.append_null(),
            WireBuilder::Boolean(b) => b.append_null(),
            WireBuilder::Date(b) => b.append_null(),
            WireBuilder::Time(b) => b.append_null(),
            WireBuilder::DateTime(b) | WireBuilder::Timestamp(b) => b.append_null(),
            WireBuilder::Numeric(b) => b.append_null(),
            WireBuilder::Utf8(b) => b.append_null(),
            WireBuilder::Binary(b) => b.append_null(),
            WireBuilder::Struct {
                children, valid, ..
            } => {
                valid.push(false);
                children
                    .iter_mut()
                    .for_each(|(_, child)| child.append_null());
            }
            WireBuilder::List { offsets, .. } => {
                let length = offsets.last().copied().unwrap_or(0);
                offsets.push(length);
            }
        }
    }

    /// The collected values.
    fn finish(self) -> Result<ArrayRef, ArrowError> {
        Ok(match self {
            WireBuilder::Int64(mut b) => Arc::new(b.finish()),
            WireBuilder::Float64(mut b) => Arc::new(b.finish()),
            WireBuilder::Boolean(mut b) => Arc::new(b.finish()),
            WireBuilder::Date(mut b) => Arc::new(b.finish()),
            WireBuilder::Time(mut b) => Arc::new(b.finish()),
            WireBuilder::DateTime(mut b) | WireBuilder::Timestamp(mut b) => Arc::new(b.finish()),
            WireBuilder::Numeric(mut b) => Arc::new(
                b.finish()
                    .with_precision_and_scale(38, PROTO_NUMERIC_SCALE)?,
            ),
            WireBuilder::Utf8(mut b) => Arc::new(b.finish()),
            WireBuilder::Binary(mut b) => Arc::new(b.finish()),
            WireBuilder::Struct {
                fields,
                children,
                valid,
            } => {
                let nulls = NullBuffer::from(valid);
                let arrays = children
                    .into_iter()
                    .map(|(_, child)| child.finish())
                    .collect::<Result<Vec<_>, _>>()?;
                let fields: Fields = fields
                    .iter()
                    .zip(&arrays)
                    .map(|(field, array)| nullable_like(field, array))
                    .collect::<Vec<FieldRef>>()
                    .into();
                Arc::new(StructArray::try_new(fields, arrays, Some(nulls))?)
            }
            WireBuilder::List {
                item,
                values,
                offsets,
            } => {
                let values = values.finish()?;
                Arc::new(ListArray::try_new(
                    nullable_like(&item, &values),
                    OffsetBuffer::new(offsets.into()),
                    values,
                    None,
                )?)
            }
        })
    }

    /// The BigQuery type this builder collects, for a problem.
    fn describe(&self) -> &'static str {
        match self {
            WireBuilder::Int64(_) => "INT64",
            WireBuilder::Float64(_) => "FLOAT64",
            WireBuilder::Boolean(_) => "BOOL",
            WireBuilder::Date(_) => "DATE",
            WireBuilder::Time(_) => "TIME",
            WireBuilder::DateTime(_) => "DATETIME",
            WireBuilder::Timestamp(_) => "TIMESTAMP",
            WireBuilder::Numeric(_) => "NUMERIC",
            WireBuilder::Utf8(_) => "STRING",
            WireBuilder::Binary(_) => "BYTES",
            WireBuilder::Struct { .. } => "STRUCT",
            WireBuilder::List { .. } => "REPEATED",
        }
    }
}

/// `field` as the field of `array`: its name and metadata with the array's type, nullable, since
/// a NULL struct holds NULL in its REQUIRED fields too and the engine conversion checks REQUIRED
/// itself.
fn nullable_like(field: &FieldRef, array: &ArrayRef) -> FieldRef {
    Arc::new(
        Field::new(field.name(), array.data_type().clone(), true)
            .with_metadata(field.metadata().clone()),
    )
}

/// The proto type of a decoded value, for a problem.
fn describe_value(value: &Value) -> &'static str {
    match value {
        Value::Bool(_) => "bool",
        Value::I32(_) => "int32",
        Value::I64(_) => "int64",
        Value::U32(_) => "uint32",
        Value::U64(_) => "uint64",
        Value::F32(_) => "float",
        Value::F64(_) => "double",
        Value::String(_) => "string",
        Value::Bytes(_) => "bytes",
        Value::EnumNumber(_) => "enum",
        Value::Message(_) => "message",
        Value::List(_) => "repeated",
        Value::Map(_) => "map",
    }
}

/// The zone label of TIMESTAMP values in the wire batch.
const WIRE_UTC: &str = "UTC";

/// The scale NUMERIC values arrive at as proto bytes.
const PROTO_NUMERIC_SCALE: i8 = 9;

const MICROS_PER_SECOND: i64 = 1_000_000;

/// The time of day, in microseconds, of a TIME packed as
/// `((hour << 12 | minute << 6 | second) << 20) | micros`.
fn unpack_time(packed: i64) -> Result<i64, String> {
    let micros = packed & 0xF_FFFF;
    let fields = packed >> 20;
    let (hour, minute, second) = ((fields >> 12) & 0x1F, (fields >> 6) & 0x3F, fields & 0x3F);
    if fields >> 17 != 0 || hour > 23 || minute > 59 || second > 59 || micros >= MICROS_PER_SECOND {
        return Err(format!("{packed} is not a packed TIME"));
    }
    Ok(((hour * 60 + minute) * 60 + second) * MICROS_PER_SECOND + micros)
}

/// The microseconds since the Unix epoch, as a civil date and time, of a DATETIME packed as
/// `((year << 26 | month << 22 | day << 17 | hour << 12 | minute << 6 | second) << 20) | micros`.
fn unpack_datetime(packed: i64) -> Result<i64, String> {
    let invalid = || format!("{packed} is not a packed DATETIME");
    let fields = packed >> 20;
    let field = |shift: u32, bits: u32| i8::try_from((fields >> shift) & ((1 << bits) - 1));
    let year = i16::try_from(fields >> 26)
        .ok()
        .filter(|year| (1..=9999).contains(year))
        .ok_or_else(invalid)?;
    let nanos = i32::try_from((packed & 0xF_FFFF) * 1000).map_err(|_| invalid())?;
    let datetime = jiff::civil::DateTime::new(
        year,
        field(22, 4).map_err(|_| invalid())?,
        field(17, 5).map_err(|_| invalid())?,
        field(12, 5).map_err(|_| invalid())?,
        field(6, 6).map_err(|_| invalid())?,
        field(0, 6).map_err(|_| invalid())?,
        nanos,
    )
    .map_err(|_| invalid())?;
    let instant = datetime
        .to_zoned(jiff::tz::TimeZone::UTC)
        .map_err(|_| invalid())?;
    Ok(instant.timestamp().as_microsecond())
}

/// A NUMERIC's unscaled value from its minimal little-endian two's complement bytes.
fn little_endian_i128(bytes: &[u8]) -> Result<i128, String> {
    if bytes.is_empty() || bytes.len() > 16 {
        return Err(format!(
            "{} bytes are not a NUMERIC value, which takes 1 to 16",
            bytes.len()
        ));
    }
    let fill = if bytes[bytes.len() - 1] & 0x80 == 0 {
        0x00
    } else {
        0xff
    };
    let mut full = [fill; 16];
    full[..bytes.len()].copy_from_slice(bytes);
    Ok(i128::from_le_bytes(full))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packed_civil_times_unpack_to_their_microseconds() {
        let time = (((13 << 12) | (45 << 6) | 30) << 20) | 123_456;
        assert_eq!(
            unpack_time(time),
            Ok(((13 * 60 + 45) * 60 + 30) * MICROS_PER_SECOND + 123_456)
        );
        let datetime = (((2026i64 << 26) | (2 << 22) | (28 << 17) | (23 << 12)) << 20) | 7;
        // 2026-02-28T23:00:00.000007Z.
        assert_eq!(unpack_datetime(datetime), Ok(1_772_319_600_000_007));
        let february_29 = ((2026i64 << 26) | (2 << 22) | (29 << 17)) << 20;
        assert!(unpack_datetime(february_29).is_err());
    }

    #[test]
    fn numeric_rescales_round_half_away_from_zero() {
        // 1.245 and -1.245 at scale 9 to scale 2.
        assert_eq!(rescale(1_245_000_000, 9, 2), Some(125));
        assert_eq!(rescale(-1_245_000_000, 9, 2), Some(-125));
        assert_eq!(rescale(1_244_999_999, 9, 2), Some(124));
        assert_eq!(rescale(15, 0, 9), Some(15_000_000_000));
    }
}
