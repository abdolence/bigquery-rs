//! The Arrow form of BigQuery values on the wire: what the `bigquery` client classifies in query
//! results and read streams, and what it sends as Arrow rows.
//!
//! | BigQuery | Wire Arrow | Field metadata |
//! |---|---|---|
//! | INT64 | `Int64` | |
//! | FLOAT64 | `Float64` | |
//! | NUMERIC(p, s) | `Decimal128(p, s)`, (38, 9) when undeclared | |
//! | BOOL | `Boolean` | |
//! | STRING | `Utf8` | |
//! | BYTES | `Binary` | |
//! | DATE | `Date32` | |
//! | TIME | `Time64(Microsecond)` | |
//! | DATETIME | `Timestamp(Microsecond, None)` | |
//! | TIMESTAMP | `Timestamp(Microsecond, "UTC")` | |
//! | GEOGRAPHY | `Utf8` | `ARROW:extension:name = google:sqlType:geography` |
//! | JSON | `Utf8` | `ARROW:extension:name = google:sqlType:json` |
//! | `RANGE<T>` | `Struct{start, end}` | `google:sqlType = range` |
//! | STRUCT | `Struct` | |
//! | REPEATED T | `List(T)` | the element's, on the list field |
//!
//! Inside the engine a column has the type Iceberg's Arrow conversion gives it, and every field
//! carries the Iceberg field id as `PARQUET:field_id`. The output step of every result casts each
//! column to its wire type and replaces the field metadata with the keys above, so no field id
//! ever reaches the client.

use crate::error::EngineError;
use crate::types::{Column, ColumnType, Columns, Mode, RangeElement, NUMERIC_DEFAULT};
use arrow::array::{Array, ArrayRef, AsArray, RecordBatch};
use arrow::compute::{cast_with_options, CastOptions};
use arrow::datatypes::{
    DataType, Field, FieldRef, Fields, IntervalUnit, Schema, SchemaRef, TimeUnit,
};
use arrow::error::ArrowError;
use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use arrow::ipc::CompressionType;
use gcloud_sdk::google::cloud::bigquery::v2;
use std::collections::HashMap;
use std::sync::Arc;

/// The field metadata key BigQuery names JSON and GEOGRAPHY values with.
pub(crate) const EXTENSION_NAME: &str = "ARROW:extension:name";
/// The field metadata key BigQuery names RANGE values with.
pub(crate) const SQL_TYPE: &str = "google:sqlType";
pub(crate) const JSON_EXTENSION: &str = "google:sqlType:json";
pub(crate) const GEOGRAPHY_EXTENSION: &str = "google:sqlType:geography";
pub(crate) const RANGE_SQL_TYPE: &str = "range";
/// The zone label of TIMESTAMP on the wire; the engine stores `+00:00`, Iceberg's label.
const UTC: &str = "UTC";
/// The name Arrow gives a list's element field.
const ITEM: &str = "item";

impl ColumnType {
    /// The wire type of one value of this type.
    pub(crate) fn wire_type(&self) -> DataType {
        match self {
            ColumnType::Int64 => DataType::Int64,
            ColumnType::Float64 => DataType::Float64,
            ColumnType::Numeric(params) => {
                let params = params.unwrap_or(NUMERIC_DEFAULT);
                // Precision and scale are at most 38 and 9, checked when the column was declared.
                DataType::Decimal128(
                    u8::try_from(params.precision).unwrap_or(38),
                    i8::try_from(params.scale).unwrap_or(9),
                )
            }
            ColumnType::Bool => DataType::Boolean,
            ColumnType::String { .. } | ColumnType::Geography | ColumnType::Json => DataType::Utf8,
            ColumnType::Bytes { .. } => DataType::Binary,
            ColumnType::Date => DataType::Date32,
            ColumnType::Time => DataType::Time64(TimeUnit::Microsecond),
            ColumnType::DateTime => DataType::Timestamp(TimeUnit::Microsecond, None),
            ColumnType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, Some(UTC.into())),
            ColumnType::Range(element) => {
                let bound = |name| Field::new(name, element.wire_type(), true);
                DataType::Struct(Fields::from(vec![bound("start"), bound("end")]))
            }
            ColumnType::Struct(fields) => DataType::Struct(fields.wire_fields()),
        }
    }

    /// The field metadata that names this type on the wire, empty for the types the Arrow type
    /// alone names.
    fn wire_metadata(&self) -> HashMap<String, String> {
        let entry = match self {
            ColumnType::Json => (EXTENSION_NAME, JSON_EXTENSION),
            ColumnType::Geography => (EXTENSION_NAME, GEOGRAPHY_EXTENSION),
            ColumnType::Range(_) => (SQL_TYPE, RANGE_SQL_TYPE),
            _ => return HashMap::new(),
        };
        HashMap::from([(entry.0.to_string(), entry.1.to_string())])
    }
}

impl RangeElement {
    fn wire_type(self) -> DataType {
        match self {
            RangeElement::Date => DataType::Date32,
            RangeElement::DateTime => DataType::Timestamp(TimeUnit::Microsecond, None),
            RangeElement::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, Some(UTC.into())),
        }
    }
}

impl Column {
    /// The column as the wire carries it: a REPEATED column is a non-null list whose elements are
    /// never null, with the element type's metadata on the list field.
    pub(crate) fn wire_field(&self) -> Field {
        let value = self.column_type.wire_type();
        let metadata = self.column_type.wire_metadata();
        let field = match self.mode {
            Mode::Repeated => Field::new(
                self.name.as_str(),
                DataType::List(Arc::new(Field::new(ITEM, value, false))),
                false,
            ),
            Mode::Required => Field::new(self.name.as_str(), value, false),
            Mode::Nullable => Field::new(self.name.as_str(), value, true),
        };
        field.with_metadata(metadata)
    }
}

impl Columns {
    /// The columns as the wire carries them.
    pub(crate) fn wire_fields(&self) -> Fields {
        self.iter().map(Column::wire_field).collect()
    }

    /// `engine`, the Arrow schema Iceberg gives these columns, with the wire metadata of each
    /// column added beside the field ids, so that a column a query passes through unchanged
    /// keeps its BigQuery type up to the output step.
    pub(crate) fn annotate(&self, engine: &Schema) -> Schema {
        let fields: Fields = engine
            .fields()
            .iter()
            .map(|field| match self.get(field.name()) {
                Some(column) => annotate_field(field, &column.column_type),
                None => field.clone(),
            })
            .collect();
        Schema::new_with_metadata(fields, engine.metadata().clone())
    }
}

/// `field` with the wire metadata of `column_type` added, nested fields included.
fn annotate_field(field: &FieldRef, column_type: &ColumnType) -> FieldRef {
    let mut metadata = field.metadata().clone();
    metadata.extend(column_type.wire_metadata());
    Arc::new(
        Field::new(
            field.name(),
            annotate_type(field.data_type(), column_type),
            field.is_nullable(),
        )
        .with_metadata(metadata),
    )
}

/// `data_type` with the wire metadata of the fields nested in it. A list's element gets none of
/// its own: the element type's metadata is on the list field, as on the wire.
fn annotate_type(data_type: &DataType, column_type: &ColumnType) -> DataType {
    match (data_type, column_type) {
        (DataType::List(item), _) => DataType::List(Arc::new(
            item.as_ref()
                .clone()
                .with_data_type(annotate_type(item.data_type(), column_type)),
        )),
        (DataType::Struct(children), ColumnType::Struct(columns)) => DataType::Struct(
            children
                .iter()
                .map(|child| match columns.get(child.name()) {
                    Some(column) => annotate_field(child, &column.column_type),
                    None => child.clone(),
                })
                .collect(),
        ),
        (data_type, _) => data_type.clone(),
    }
}

/// The output step of a result: the wire schema of its fields and the cast that gets each
/// column there.
#[derive(Debug, Clone)]
pub(crate) struct WireSchema {
    schema: SchemaRef,
}

impl WireSchema {
    /// The wire schema of `columns`, the declared columns a read returns.
    pub(crate) fn of_columns<'c>(columns: impl IntoIterator<Item = &'c Column>) -> Self {
        let fields: Fields = columns.into_iter().map(Column::wire_field).collect();
        Self {
            schema: Arc::new(Schema::new(fields)),
        }
    }

    /// The wire schema of a result whose plan produced `schema`. The BigQuery metadata a column
    /// carried through the plan stays; every other key, the Iceberg field ids above all, goes.
    ///
    /// # Errors
    /// [`EngineError::Unsupported`] for a column of a type with no wire form, naming the column
    /// and the type.
    pub(crate) fn of_result(schema: &Schema) -> Result<Self, EngineError> {
        let fields = schema
            .fields()
            .iter()
            .map(|field| inferred_wire_field(field, field.name()))
            .collect::<Result<Fields, _>>()?;
        Ok(Self {
            schema: Arc::new(Schema::new(fields)),
        })
    }

    pub(crate) fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// `batch`, whose columns are in this schema's order, in the wire types.
    ///
    /// # Errors
    /// [`EngineError::InvalidQuery`] for a value with no wire form, such as an unsigned integer
    /// above `i64::MAX`, and for an array with a NULL element, which BigQuery refuses in a
    /// result.
    pub(crate) fn normalize(&self, batch: &RecordBatch) -> Result<RecordBatch, EngineError> {
        let options = CastOptions {
            safe: false,
            ..Default::default()
        };
        let columns = self
            .schema
            .fields()
            .iter()
            .zip(batch.columns())
            .map(|(field, column)| {
                let column = if column.data_type() == field.data_type() {
                    column.clone()
                } else {
                    cast_with_options(column, field.data_type(), &options).map_err(|err| {
                        EngineError::InvalidQuery {
                            detail: format!("column `{}`: {err}", field.name()),
                        }
                    })?
                };
                refuse_null_elements(field.name(), &column)?;
                Ok(column)
            })
            .collect::<Result<Vec<_>, EngineError>>()?;
        RecordBatch::try_new(self.schema.clone(), columns).map_err(|err| EngineError::Internal {
            detail: format!("a normalized batch does not match its wire schema: {err}"),
        })
    }
}

/// The result's columns as a query's `schema` reports them, with the standard type names:
/// every column NULLABLE, an array REPEATED, as BigQuery reports a result.
///
/// # Errors
/// [`EngineError::Internal`] for a field the output step never produces.
impl TryFrom<&WireSchema> for v2::TableSchema {
    type Error = EngineError;

    fn try_from(wire: &WireSchema) -> Result<Self, Self::Error> {
        Ok(v2::TableSchema {
            fields: wire
                .schema
                .fields()
                .iter()
                .map(|field| result_field(field))
                .collect::<Result<_, _>>()?,
            foreign_type_info: None,
        })
    }
}

/// One result column of the wire schema in the v2 API's form.
fn result_field(field: &Field) -> Result<v2::TableFieldSchema, EngineError> {
    let (value_type, mode) = match field.data_type() {
        DataType::List(item) => (item.data_type(), "REPEATED"),
        data_type => (data_type, "NULLABLE"),
    };
    let extension = field.metadata().get(EXTENSION_NAME).map(String::as_str);
    let mut out = v2::TableFieldSchema {
        name: field.name().clone(),
        mode: mode.to_string(),
        ..Default::default()
    };
    out.r#type = match value_type {
        DataType::Int64 => "INT64",
        DataType::Float64 => "FLOAT64",
        DataType::Boolean => "BOOL",
        DataType::Utf8 => match extension {
            Some(JSON_EXTENSION) => "JSON",
            Some(GEOGRAPHY_EXTENSION) => "GEOGRAPHY",
            _ => "STRING",
        },
        DataType::Binary => "BYTES",
        DataType::Date32 => "DATE",
        DataType::Time64(_) => "TIME",
        DataType::Timestamp(_, None) => "DATETIME",
        DataType::Timestamp(_, Some(_)) => "TIMESTAMP",
        DataType::Interval(_) => "INTERVAL",
        DataType::Decimal128(precision, scale) => {
            if (u32::from(*precision), i64::from(*scale))
                != (NUMERIC_DEFAULT.precision, i64::from(NUMERIC_DEFAULT.scale))
            {
                out.precision = i64::from(*precision);
                out.scale = i64::from(*scale);
            }
            "NUMERIC"
        }
        DataType::Struct(children)
            if field.metadata().get(SQL_TYPE).map(String::as_str) == Some(RANGE_SQL_TYPE) =>
        {
            let element = children.first().map(|start| match start.data_type() {
                DataType::Date32 => "DATE",
                DataType::Timestamp(_, None) => "DATETIME",
                _ => "TIMESTAMP",
            });
            out.range_element_type =
                element.map(|element| v2::table_field_schema::FieldElementType {
                    r#type: element.to_string(),
                });
            "RANGE"
        }
        DataType::Struct(children) => {
            out.fields = children
                .iter()
                .map(|child| result_field(child))
                .collect::<Result<_, _>>()?;
            "STRUCT"
        }
        other => {
            return Err(EngineError::Internal {
                detail: format!("a result column of Arrow type {other} reached the output step"),
            })
        }
    }
    .to_string();
    Ok(out)
}

/// Fails on an array, at any depth of `column`, that holds a NULL element.
fn refuse_null_elements(name: &str, column: &ArrayRef) -> Result<(), EngineError> {
    match column.data_type() {
        DataType::List(_) => {
            let list = column.as_list::<i32>();
            let values = list.values();
            let has_null = (0..list.len()).any(|row| {
                list.is_valid(row) && {
                    let (start, end) = (list.value_offsets()[row], list.value_offsets()[row + 1]);
                    (start..end).any(|element| {
                        usize::try_from(element).is_ok_and(|element| values.is_null(element))
                    })
                }
            });
            if has_null {
                return Err(EngineError::InvalidQuery {
                    detail: format!("Array cannot have a null element; column `{name}`"),
                });
            }
            refuse_null_elements(name, values)
        }
        DataType::Struct(_) => column
            .as_struct()
            .columns()
            .iter()
            .try_for_each(|child| refuse_null_elements(name, child)),
        _ => Ok(()),
    }
}

/// The wire field of a result field: its type mapped as [`inferred_wire_type`] maps it, and its
/// metadata reduced to the keys that name a BigQuery type. `column` names the result column in
/// an error.
fn inferred_wire_field(field: &Field, column: &str) -> Result<Field, EngineError> {
    let metadata: HashMap<String, String> = field
        .metadata()
        .iter()
        .filter(|(key, value)| match key.as_str() {
            EXTENSION_NAME => [JSON_EXTENSION, GEOGRAPHY_EXTENSION].contains(&value.as_str()),
            SQL_TYPE => value.as_str() == RANGE_SQL_TYPE,
            _ => false,
        })
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    Ok(Field::new(
        field.name(),
        inferred_wire_type(field.data_type(), column)?,
        field.is_nullable(),
    )
    .with_metadata(metadata))
}

/// The wire type of a value of `data_type`: views, large and fixed forms to their plain forms,
/// every timestamp to microseconds with its zone labelled `UTC`, smaller and unsigned integers
/// to `Int64`, `Float32` and `Float16` to `Float64`, an untyped NULL to `Int64` as BigQuery types
/// it, and nested types element by element.
fn inferred_wire_type(data_type: &DataType, column: &str) -> Result<DataType, EngineError> {
    Ok(match data_type {
        DataType::Null
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => DataType::Int64,
        DataType::Float16 | DataType::Float32 | DataType::Float64 => DataType::Float64,
        DataType::Boolean => DataType::Boolean,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => DataType::Utf8,
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => DataType::Binary,
        DataType::Date32 | DataType::Date64 => DataType::Date32,
        DataType::Time32(_) | DataType::Time64(_) => DataType::Time64(TimeUnit::Microsecond),
        DataType::Timestamp(_, zone) => {
            DataType::Timestamp(TimeUnit::Microsecond, zone.as_ref().map(|_| UTC.into()))
        }
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
            if *scale >= 0 =>
        {
            DataType::Decimal128(*precision, *scale)
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            DataType::Interval(IntervalUnit::MonthDayNano)
        }
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|field| inferred_wire_field(field, column))
                .collect::<Result<Fields, _>>()?,
        ),
        DataType::List(item)
        | DataType::LargeList(item)
        | DataType::ListView(item)
        | DataType::LargeListView(item)
        | DataType::FixedSizeList(item, _) => {
            DataType::List(Arc::new(inferred_wire_field(item, column)?.with_name(ITEM)))
        }
        DataType::Dictionary(_, values) => inferred_wire_type(values, column)?,
        other => {
            return Err(EngineError::unsupported(
                format!("a result column of Arrow type {other}"),
                format!("column `{column}` has no BigQuery type"),
            ))
        }
    })
}

/// The compression of an IPC stream's buffers, as a read session or a query asks for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IpcCompression {
    Lz4Frame,
    Zstd,
}

impl From<IpcCompression> for CompressionType {
    fn from(compression: IpcCompression) -> Self {
        match compression {
            IpcCompression::Lz4Frame => CompressionType::LZ4_FRAME,
            IpcCompression::Zstd => CompressionType::ZSTD,
        }
    }
}

/// Encodes one result or one read stream as the separate IPC messages the client decodes one at
/// a time: the schema message once, then one record batch message per batch.
pub(crate) struct IpcEncoder {
    writer: StreamWriter<Vec<u8>>,
    schema_message: Vec<u8>,
}

impl IpcEncoder {
    /// An encoder of batches in `schema`, their buffers compressed when `compression` is set.
    pub(crate) fn new(
        schema: &Schema,
        compression: Option<IpcCompression>,
    ) -> Result<Self, EngineError> {
        let options = IpcWriteOptions::default()
            .try_with_compression(compression.map(CompressionType::from))
            .map_err(EngineError::ipc)?;
        let mut writer = StreamWriter::try_new_with_options(Vec::new(), schema, options)
            .map_err(EngineError::ipc)?;
        let schema_message = std::mem::take(writer.get_mut());
        Ok(Self {
            writer,
            schema_message,
        })
    }

    /// The schema message, which every stream of these batches starts from.
    pub(crate) fn schema_message(&self) -> &[u8] {
        &self.schema_message
    }

    /// The record batch message of `batch`.
    pub(crate) fn encode(&mut self, batch: &RecordBatch) -> Result<Vec<u8>, EngineError> {
        self.writer.write(batch).map_err(EngineError::ipc)?;
        Ok(std::mem::take(self.writer.get_mut()))
    }
}

impl EngineError {
    /// An IPC encoding failure, which no request causes: the batches are the engine's own.
    fn ipc(error: ArrowError) -> Self {
        EngineError::Internal {
            detail: format!("Arrow IPC encoding failed: {error}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, ListArray, UInt64Array};
    use arrow::datatypes::Int64Type;

    #[test]
    fn an_unsigned_value_above_int64_fails_the_result() {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::UInt64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(UInt64Array::from(vec![u64::MAX]))],
        )
        .expect("a valid batch");
        let wire = WireSchema::of_result(&schema).expect("UInt64 has a wire form");
        assert!(matches!(
            wire.normalize(&batch),
            Err(EngineError::InvalidQuery { .. })
        ));
        let fits = RecordBatch::try_new(schema, vec![Arc::new(UInt64Array::from(vec![7]))])
            .expect("a valid batch");
        let normalized = wire.normalize(&fits).expect("7 fits Int64");
        assert_eq!(
            normalized.column(0).as_primitive::<Int64Type>(),
            &Int64Array::from(vec![7])
        );
    }

    #[test]
    fn an_array_with_a_null_element_fails_the_result() {
        let list =
            ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![Some(1), None])]);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "values",
            list.data_type().clone(),
            true,
        )]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(list)]).expect("a valid batch");
        let wire = WireSchema::of_result(&schema).expect("a list has a wire form");
        assert!(matches!(
            wire.normalize(&batch),
            Err(EngineError::InvalidQuery { .. })
        ));
    }

    #[test]
    fn interval_year_month_and_decimal256_have_no_wire_form() {
        for data_type in [
            DataType::Interval(IntervalUnit::YearMonth),
            DataType::Decimal256(76, 38),
        ] {
            let schema = Schema::new(vec![Field::new("v", data_type.clone(), true)]);
            assert!(
                matches!(
                    WireSchema::of_result(&schema),
                    Err(EngineError::Unsupported { .. })
                ),
                "{data_type}"
            );
        }
    }
}
