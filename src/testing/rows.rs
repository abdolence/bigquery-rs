//! Rows on their way into the fake's tables and query results.
//!
//! Every row reaches the fake as a proto2 message, the form Storage Write takes: a test's serde
//! rows are encoded by the write path's own [`Encoder`] first, and appended rows arrive
//! encoded. [`ProtoBatchBuilder`] decodes the messages into the Arrow layout a read session
//! sends, so a value crosses the same codecs it crosses against BigQuery.

use crate::errors::BigQueryCodecErrorKind;
use crate::testing::state::FakeChange;
use crate::types::civil::{self, MICROS_PER_DAY};
use crate::types::decimal::{self, BIGNUMERIC_SCALE, NUMERIC_SCALE};
use crate::types::error::CodecError;
use crate::types::kind::FieldKind;
use crate::write::descriptor::{WritePlan, CHANGE_SEQUENCE_NUMBER_COLUMN, CHANGE_TYPE_COLUMN};
use crate::write::encoder::Encoder;
use crate::{
    BigQueryChangeType, BigQueryDecimalParams, BigQueryFieldMode, BigQueryFieldSchema,
    BigQueryFieldType, BigQueryInterval, BigQueryRangeElementType, BigQueryResult,
    BigQueryTableSchema,
};
use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Decimal256Array,
    Float64Array, Int64Array, IntervalMonthDayNanoArray, ListArray, RecordBatch,
    RecordBatchOptions, StringArray, StructArray, Time64MicrosecondArray,
    TimestampMicrosecondArray,
};
use arrow_buffer::{i256, IntervalMonthDayNano, NullBuffer, OffsetBuffer};
use arrow_schema::{ArrowError, DataType, Field, SchemaRef};
use gcloud_sdk::prost::encoding::{decode_key, decode_varint, WireType};
use gcloud_sdk::prost_types::DescriptorProto;
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Where the values of one proto field number go.
#[derive(Clone, Copy, Debug)]
enum FieldTarget {
    /// The field of the message at this index.
    Column(usize),
    /// The CDC `_CHANGE_TYPE` pseudo-column of a row.
    ChangeType,
    /// The CDC `_CHANGE_SEQUENCE_NUMBER` pseudo-column of a row.
    ChangeSequenceNumber,
}

/// How the proto fields of one message, the row, a STRUCT or a RANGE, map to its BigQuery
/// fields.
#[derive(Debug)]
struct MessageLayout {
    fields: Vec<BigQueryFieldSchema>,
    targets: BTreeMap<u32, FieldTarget>,
    /// The layout of the message of each STRUCT or RANGE field, by field index.
    nested: BTreeMap<usize, MessageLayout>,
}

/// One decoded value of a field.
#[derive(Clone, Debug)]
enum FieldValue {
    Int64(i64),
    Float64(f64),
    Bool(bool),
    /// STRING, GEOGRAPHY or JSON text.
    String(String),
    Bytes(Vec<u8>),
    /// Days since 1970-01-01.
    Date(i32),
    /// Microseconds since midnight.
    Time(i64),
    /// Civil microseconds since 1970-01-01T00:00:00.
    DateTime(i64),
    /// Microseconds since the epoch.
    Timestamp(i64),
    /// NUMERIC or BIGNUMERIC, unscaled at the scale Storage Write carries the type at.
    Decimal(i256),
    Interval(BigQueryInterval),
    /// The fields of a STRUCT, or the start and end of a RANGE, in schema order.
    Struct(Vec<Option<FieldValue>>),
    /// The elements of a REPEATED field.
    Array(Vec<FieldValue>),
}

/// The rows a [`ProtoBatchBuilder`] decoded.
#[derive(Debug)]
pub(super) struct DecodedRows {
    /// The rows, in the read layout of the table.
    pub rows: RecordBatch,
    /// Each row's change, when the rows carried the CDC pseudo-columns.
    pub changes: Option<Vec<FakeChange>>,
}

/// Decodes proto2 rows of one table into one Arrow batch of its read layout.
#[derive(Debug)]
pub(super) struct ProtoBatchBuilder {
    arrow_schema: SchemaRef,
    layout: MessageLayout,
    /// Per column, one value per row decoded so far.
    columns: Vec<Vec<Option<FieldValue>>>,
    /// Per row, its change, when the rows carry the CDC pseudo-columns.
    changes: Option<Vec<FakeChange>>,
    rows: usize,
}

/// The CDC pseudo-columns of one row.
#[derive(Debug, Default)]
struct ChangeColumns {
    change_type: Option<String>,
    sequence_number: Option<String>,
}

impl ProtoBatchBuilder {
    /// Rows numbered as the crate's own encoder numbers them: field `i` of every message is
    /// field number `i + 1`.
    pub(super) fn new(schema: &BigQueryTableSchema) -> Self {
        Self::with_layout(
            schema,
            MessageLayout::numbered(schema.fields.clone()),
            false,
        )
    }

    /// Rows described by `descriptor`, the writer schema of an append request. Each field goes
    /// to the field of its name, ignoring case as BigQuery does, at every depth, and the CDC
    /// pseudo-columns `_CHANGE_TYPE` and `_CHANGE_SEQUENCE_NUMBER` make the rows changes.
    ///
    /// # Errors
    /// [`UnknownField`](BigQueryCodecErrorKind::UnknownField) for a field that names no column
    /// of the table, or a STRUCT or RANGE field whose message type the descriptor lacks.
    pub(super) fn from_descriptor(
        schema: &BigQueryTableSchema,
        descriptor: &DescriptorProto,
    ) -> Result<Self, CodecError> {
        let mut types = BTreeMap::new();
        collect_message_types(&descriptor.nested_type, &mut types);
        let mut cdc = false;
        let layout =
            MessageLayout::described(schema.fields.clone(), descriptor, &types, Some(&mut cdc))?;
        Ok(Self::with_layout(schema, layout, cdc))
    }

    fn with_layout(schema: &BigQueryTableSchema, layout: MessageLayout, cdc: bool) -> Self {
        Self {
            arrow_schema: Arc::new(schema.arrow_read_schema()),
            layout,
            columns: vec![Vec::new(); schema.fields.len()],
            changes: cdc.then(Vec::new),
            rows: 0,
        }
    }

    /// Decodes `message` as the next row. A row that fails leaves the rows before it as they
    /// were.
    ///
    /// # Errors
    /// The field at fault for a field of the wrong wire type, a REQUIRED field the row has no
    /// value for, a value outside its type, or bytes that are not a proto message.
    pub(super) fn push(&mut self, message: &[u8]) -> Result<(), CodecError> {
        let mut change_columns = ChangeColumns::default();
        let values = self.layout.decode(message, &mut change_columns)?;
        if let Some(changes) = &mut self.changes {
            changes.push(change_columns.change()?);
        }
        for (column, value) in self.columns.iter_mut().zip(values) {
            column.push(value);
        }
        self.rows += 1;
        Ok(())
    }

    /// The rows decoded so far.
    ///
    /// # Errors
    /// [`OutOfRange`](BigQueryCodecErrorKind::OutOfRange) for a decimal beyond the precision
    /// its column declares, or Arrow's error if the columns do not make a batch, which is a
    /// defect of the fake.
    pub(super) fn finish(self) -> Result<DecodedRows, CodecError> {
        let arrays = self
            .layout
            .fields
            .iter()
            .zip(self.columns)
            .map(|(column, values)| {
                FieldValue::column_array(column, values).map_err(|err| err.at_field(&column.name))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let options = RecordBatchOptions::new().with_row_count(Some(self.rows));
        let rows = RecordBatch::try_new_with_options(self.arrow_schema, arrays, &options)
            .map_err(CodecError::layout)?;
        Ok(DecodedRows {
            rows,
            changes: self.changes,
        })
    }

    /// The rows of `rows`, described by `descriptor`, that hold a value outside the range of
    /// its column, by index, each with its error. Each row is decoded on its own, as a batch
    /// refuses an out of range value only once all its rows are in.
    ///
    /// # Errors
    /// The error [`from_descriptor`](Self::from_descriptor) returns for `descriptor`.
    pub(super) fn out_of_range_rows(
        schema: &BigQueryTableSchema,
        descriptor: &DescriptorProto,
        rows: &[Vec<u8>],
    ) -> Result<Vec<(usize, CodecError)>, CodecError> {
        let mut out_of_range = Vec::new();
        for (index, row) in rows.iter().enumerate() {
            let mut builder = Self::from_descriptor(schema, descriptor)?;
            let decoded = match builder.push(row) {
                Ok(()) => builder.finish().map(drop),
                Err(err) => Err(err),
            };
            if let Err(err) = decoded {
                if err.kind() == BigQueryCodecErrorKind::OutOfRange {
                    out_of_range.push((index, err));
                }
            }
        }
        Ok(out_of_range)
    }

    /// Encodes `rows` with the write path's encoder and decodes them into one batch in the read
    /// layout of `schema`. Errors number the rows from `first_row`.
    ///
    /// # Errors
    /// [`SerializeError`](crate::errors::BigQueryError::SerializeError) naming the row and the
    /// field of the first row that does not encode as `schema` says.
    pub(super) fn encode<T, I>(
        schema: &BigQueryTableSchema,
        rows: I,
        first_row: u64,
    ) -> BigQueryResult<RecordBatch>
    where
        T: Serialize,
        I: IntoIterator<Item = T>,
    {
        let mut encoder = Encoder::new(Arc::new(WritePlan::new(schema, false)));
        let mut builder = Self::new(schema);
        let mut message = Vec::new();
        for (row_number, row) in (first_row..).zip(rows) {
            message.clear();
            encoder
                .encode(&row, &mut message)
                .and_then(|()| builder.push(&message))
                .map_err(|err| err.with_row(row_number).into_serialize())?;
        }
        builder
            .finish()
            .map(|decoded| decoded.rows)
            .map_err(CodecError::into_serialize)
    }
}

/// Every message type declared in `types` and in the types nested in them, by simple name.
fn collect_message_types<'d>(
    types: &'d [DescriptorProto],
    out: &mut BTreeMap<&'d str, &'d DescriptorProto>,
) {
    for message in types {
        out.insert(message.name(), message);
        collect_message_types(&message.nested_type, out);
    }
}

impl MessageLayout {
    /// The layout of `fields` as the crate's own encoder numbers them, schema order from 1.
    fn numbered(fields: Vec<BigQueryFieldSchema>) -> Self {
        let targets = (0..fields.len())
            .filter_map(|index| {
                let number = u32::try_from(index + 1).ok()?;
                Some((number, FieldTarget::Column(index)))
            })
            .collect();
        let nested = fields
            .iter()
            .enumerate()
            .filter_map(|(index, field)| {
                Some((index, Self::numbered(field.field_type.message_fields()?)))
            })
            .collect();
        Self {
            fields,
            targets,
            nested,
        }
    }

    /// The layout of `fields` as `message` declares them, each proto field matched to the
    /// field of its name. `cdc` is set for the row, whose CDC pseudo-columns it records, and
    /// `None` for a nested message, where those names are ordinary fields.
    fn described(
        fields: Vec<BigQueryFieldSchema>,
        message: &DescriptorProto,
        types: &BTreeMap<&str, &DescriptorProto>,
        mut cdc: Option<&mut bool>,
    ) -> Result<Self, CodecError> {
        let mut targets = BTreeMap::new();
        let mut nested = BTreeMap::new();
        for proto_field in &message.field {
            let name = proto_field.name();
            let target = match (name, cdc.as_deref_mut()) {
                (CHANGE_TYPE_COLUMN, Some(cdc)) => {
                    *cdc = true;
                    FieldTarget::ChangeType
                }
                (CHANGE_SEQUENCE_NUMBER_COLUMN, Some(_)) => FieldTarget::ChangeSequenceNumber,
                _ => {
                    let index = fields
                        .iter()
                        .position(|column| column.name.eq_ignore_ascii_case(name))
                        .ok_or_else(|| {
                            CodecError::new(
                                BigQueryCodecErrorKind::UnknownField,
                                "the writer schema has a field the table has no column for",
                            )
                            .at_field(name)
                        })?;
                    if let Some(children) = fields[index].field_type.message_fields() {
                        let type_name = proto_field.type_name();
                        let simple_name = type_name.rsplit('.').next().unwrap_or(type_name);
                        let child = types.get(simple_name).ok_or_else(|| {
                            CodecError::new(
                                BigQueryCodecErrorKind::UnknownField,
                                format!("the writer schema declares no message `{type_name}`"),
                            )
                            .at_field(name)
                        })?;
                        let layout = Self::described(children, child, types, None)
                            .map_err(|err| err.at_field(name))?;
                        nested.insert(index, layout);
                    }
                    FieldTarget::Column(index)
                }
            };
            let number = u32::try_from(proto_field.number()).map_err(|_| {
                CodecError::new(
                    BigQueryCodecErrorKind::UnknownField,
                    format!(
                        "field number {} is not a proto field number",
                        proto_field.number()
                    ),
                )
                .at_field(name)
            })?;
            targets.insert(number, target);
        }
        Ok(Self {
            fields,
            targets,
            nested,
        })
    }

    /// The values of the fields of `message`, one slot per field. The CDC pseudo-columns go to
    /// `change_columns`.
    fn decode(
        &self,
        mut message: &[u8],
        change_columns: &mut ChangeColumns,
    ) -> Result<Vec<Option<FieldValue>>, CodecError> {
        let mut values = vec![None; self.fields.len()];
        while !message.is_empty() {
            let (number, wire_type) =
                decode_key(&mut message).map_err(CodecError::malformed_row)?;
            let target = self.targets.get(&number).copied().ok_or_else(|| {
                CodecError::new(
                    BigQueryCodecErrorKind::UnknownField,
                    format!("field number {number} is not in the writer schema"),
                )
            })?;
            match target {
                FieldTarget::Column(index) => self
                    .decode_field(index, wire_type, &mut message, &mut values[index])
                    .map_err(|err| err.at_field(&self.fields[index].name))?,
                FieldTarget::ChangeType => {
                    change_columns.change_type = Some(decode_text(wire_type, &mut message)?);
                }
                FieldTarget::ChangeSequenceNumber => {
                    change_columns.sequence_number = Some(decode_text(wire_type, &mut message)?);
                }
            }
        }
        for (field, value) in self.fields.iter().zip(&values) {
            if value.is_none() && field.mode == BigQueryFieldMode::Required {
                return Err(CodecError::new(
                    BigQueryCodecErrorKind::MissingRequiredField,
                    "the message has no value for a REQUIRED field",
                )
                .at_field(&field.name));
            }
        }
        Ok(values)
    }

    /// Reads one occurrence of field `index` into `slot`: its value, or for a REPEATED field
    /// one element or one packed run of elements, appended.
    fn decode_field(
        &self,
        index: usize,
        wire_type: WireType,
        message: &mut &[u8],
        slot: &mut Option<FieldValue>,
    ) -> Result<(), CodecError> {
        let field = &self.fields[index];
        let kind = FieldKind::from(&field.field_type);
        let repeated = field.mode == BigQueryFieldMode::Repeated;
        if repeated && kind.packable() && wire_type == WireType::LengthDelimited {
            let mut run = decode_length_delimited(message)?;
            let element_wire_type = if kind == FieldKind::Float64 {
                WireType::SixtyFourBit
            } else {
                WireType::Varint
            };
            while !run.is_empty() {
                FieldValue::decode(kind, element_wire_type, &mut run)?.append_to(slot);
            }
            return Ok(());
        }
        let value = match self.nested.get(&index) {
            Some(layout) => {
                if wire_type != WireType::LengthDelimited {
                    return Err(CodecError::wire_type_mismatch(kind, wire_type));
                }
                let nested = decode_length_delimited(message)?;
                FieldValue::Struct(layout.decode(nested, &mut ChangeColumns::default())?)
            }
            None => FieldValue::decode(kind, wire_type, message)?,
        };
        if repeated {
            value.append_to(slot);
        } else {
            *slot = Some(value);
        }
        Ok(())
    }
}

impl BigQueryFieldType {
    /// The fields of the message a value of this type is written as: a STRUCT's own, or a
    /// RANGE's `start` and `end`. `None` for a type written as a scalar.
    pub(super) fn message_fields(&self) -> Option<Vec<BigQueryFieldSchema>> {
        match self {
            BigQueryFieldType::Struct(fields) => Some(fields.clone()),
            BigQueryFieldType::Range(element) => {
                let bound_type = match element {
                    BigQueryRangeElementType::Date => BigQueryFieldType::Date,
                    BigQueryRangeElementType::DateTime => BigQueryFieldType::DateTime,
                    BigQueryRangeElementType::Timestamp => BigQueryFieldType::Timestamp,
                };
                let bound = |name: &str| BigQueryFieldSchema {
                    name: name.to_string(),
                    field_type: bound_type.clone(),
                    mode: BigQueryFieldMode::Nullable,
                    description: None,
                    default_value_expression: None,
                };
                Some(vec![bound("start"), bound("end")])
            }
            _ => None,
        }
    }
}

impl ChangeColumns {
    /// The change a CDC row asks for.
    fn change(&self) -> Result<FakeChange, CodecError> {
        let text = self.change_type.as_deref().ok_or_else(|| {
            CodecError::new(
                BigQueryCodecErrorKind::MissingRequiredField,
                "a CDC row has no change type",
            )
            .at_field(CHANGE_TYPE_COLUMN)
        })?;
        let change_type =
            BigQueryChangeType::from_text(text).map_err(|err| err.at_field(CHANGE_TYPE_COLUMN))?;
        let sequence_number = self
            .sequence_number
            .as_deref()
            .map(str::parse)
            .transpose()
            .map_err(|_| {
                CodecError::invalid_text("the sequence number is empty")
                    .at_field(CHANGE_SEQUENCE_NUMBER_COLUMN)
            })?;
        Ok(FakeChange {
            change_type,
            sequence_number,
        })
    }
}

impl FieldValue {
    /// Reads one scalar value of a field of `kind` from the front of `buffer`, which a key of
    /// `wire_type` started. The wire forms are those the crate's own encoder writes.
    fn decode(
        kind: FieldKind,
        wire_type: WireType,
        buffer: &mut &[u8],
    ) -> Result<FieldValue, CodecError> {
        // A varint of an int64 or int32 field carries the two's complement bits of the value.
        let mut signed = || {
            decode_varint(buffer)
                .map(u64::cast_signed)
                .map_err(CodecError::malformed_row)
        };
        match (kind, wire_type) {
            (FieldKind::Int64, WireType::Varint) => Ok(FieldValue::Int64(signed()?)),
            (FieldKind::Timestamp, WireType::Varint) => Ok(FieldValue::Timestamp(signed()?)),
            (FieldKind::Date, WireType::Varint) => {
                let days = signed()?;
                i32::try_from(days).map(FieldValue::Date).map_err(|_| {
                    CodecError::out_of_range(format!("DATE of {days} days is not an int32"))
                })
            }
            (FieldKind::Time, WireType::Varint) => {
                let micros = civil::unpack_time(signed()?);
                if !(0..MICROS_PER_DAY).contains(&micros) {
                    return Err(CodecError::out_of_range(format!(
                        "TIME of {micros} microseconds is not a time of day"
                    )));
                }
                Ok(FieldValue::Time(micros))
            }
            (FieldKind::DateTime, WireType::Varint) => {
                Ok(FieldValue::DateTime(civil::unpack_datetime(signed()?)?))
            }
            (FieldKind::Bool, WireType::Varint) => Ok(FieldValue::Bool(
                decode_varint(buffer).map_err(CodecError::malformed_row)? != 0,
            )),
            (FieldKind::Float64, WireType::SixtyFourBit) => {
                let (bytes, rest) = buffer.split_first_chunk::<8>().ok_or_else(|| {
                    CodecError::malformed_row("a double runs past the end of the row")
                })?;
                *buffer = rest;
                Ok(FieldValue::Float64(f64::from_le_bytes(*bytes)))
            }
            (
                FieldKind::String | FieldKind::Geography | FieldKind::Json,
                WireType::LengthDelimited,
            ) => Ok(FieldValue::String(decode_text(wire_type, buffer)?)),
            (FieldKind::Interval, WireType::LengthDelimited) => Ok(FieldValue::Interval(
                BigQueryInterval::parse_bq(&decode_text(wire_type, buffer)?)?,
            )),
            (FieldKind::Bytes, WireType::LengthDelimited) => {
                Ok(FieldValue::Bytes(decode_length_delimited(buffer)?.to_vec()))
            }
            (FieldKind::Numeric | FieldKind::BigNumeric, WireType::LengthDelimited) => {
                let bytes = decode_length_delimited(buffer)?;
                if bytes.len() > 32 {
                    return Err(CodecError::out_of_range(format!(
                        "a {} of {} bytes is wider than 256 bits",
                        kind.name(),
                        bytes.len()
                    )));
                }
                Ok(FieldValue::Decimal(decimal::decimal_from_le_bytes(bytes)))
            }
            _ => Err(CodecError::wire_type_mismatch(kind, wire_type)),
        }
    }

    /// Appends this value to the elements of a REPEATED field's `slot`.
    fn append_to(self, slot: &mut Option<FieldValue>) {
        match slot {
            Some(FieldValue::Array(elements)) => elements.push(self),
            _ => *slot = Some(FieldValue::Array(vec![self])),
        }
    }

    /// The Arrow array of `field` holding `values`, in its read layout. A REPEATED field's
    /// absent value is an empty list.
    fn column_array(
        field: &BigQueryFieldSchema,
        values: Vec<Option<FieldValue>>,
    ) -> Result<ArrayRef, CodecError> {
        let (data_type, _) = field.field_type.arrow_read_type();
        if field.mode != BigQueryFieldMode::Repeated {
            return Self::array(&field.field_type, &data_type, values);
        }
        let mut offsets = Vec::with_capacity(values.len() + 1);
        offsets.push(0i32);
        let mut elements = Vec::new();
        for value in values {
            match value {
                Some(FieldValue::Array(items)) => elements.extend(items.into_iter().map(Some)),
                None => {}
                Some(other) => {
                    return Err(CodecError::holds(
                        FieldKind::from(&field.field_type),
                        &other,
                    ))
                }
            }
            offsets.push(i32::try_from(elements.len()).map_err(|_| {
                CodecError::out_of_range("a REPEATED column holds more than 2^31 elements")
            })?);
        }
        let items = Self::array(&field.field_type, &data_type, elements)?;
        let list = ListArray::try_new(
            Arc::new(Field::new("item", data_type, true)),
            OffsetBuffer::new(offsets.into()),
            items,
            None,
        )
        .map_err(CodecError::layout)?;
        Ok(Arc::new(list))
    }

    /// The Arrow array of `data_type`, the read type of `field_type`, holding `values`.
    fn array(
        field_type: &BigQueryFieldType,
        data_type: &DataType,
        values: Vec<Option<FieldValue>>,
    ) -> Result<ArrayRef, CodecError> {
        let kind = FieldKind::from(field_type);
        Ok(match (field_type, data_type) {
            (BigQueryFieldType::Int64, _) => Arc::new(Int64Array::from(Self::native_values(
                kind,
                values,
                |value| match value {
                    FieldValue::Int64(value) => Ok(value),
                    other => Err(other),
                },
            )?)),
            (BigQueryFieldType::Float64, _) => Arc::new(Float64Array::from(Self::native_values(
                kind,
                values,
                |value| match value {
                    FieldValue::Float64(value) => Ok(value),
                    other => Err(other),
                },
            )?)),
            (BigQueryFieldType::Bool, _) => Arc::new(BooleanArray::from(Self::native_values(
                kind,
                values,
                |value| match value {
                    FieldValue::Bool(value) => Ok(value),
                    other => Err(other),
                },
            )?)),
            (
                BigQueryFieldType::String { .. }
                | BigQueryFieldType::Geography
                | BigQueryFieldType::Json,
                _,
            ) => Arc::new(StringArray::from(Self::native_values(
                kind,
                values,
                |value| match value {
                    FieldValue::String(value) => Ok(value),
                    other => Err(other),
                },
            )?)),
            (BigQueryFieldType::Bytes { .. }, _) => Arc::new(BinaryArray::from_iter(
                Self::native_values(kind, values, |value| match value {
                    FieldValue::Bytes(value) => Ok(value),
                    other => Err(other),
                })?,
            )),
            (BigQueryFieldType::Date, _) => Arc::new(Date32Array::from(Self::native_values(
                kind,
                values,
                |value| match value {
                    FieldValue::Date(value) => Ok(value),
                    other => Err(other),
                },
            )?)),
            (BigQueryFieldType::Time, _) => Arc::new(Time64MicrosecondArray::from(
                Self::native_values(kind, values, |value| match value {
                    FieldValue::Time(value) => Ok(value),
                    other => Err(other),
                })?,
            )),
            (BigQueryFieldType::DateTime, _) => Arc::new(TimestampMicrosecondArray::from(
                Self::native_values(kind, values, |value| match value {
                    FieldValue::DateTime(value) => Ok(value),
                    other => Err(other),
                })?,
            )),
            (BigQueryFieldType::Timestamp, DataType::Timestamp(_, zone)) => Arc::new(
                TimestampMicrosecondArray::from(Self::native_values(kind, values, |value| {
                    match value {
                        FieldValue::Timestamp(value) => Ok(value),
                        other => Err(other),
                    }
                })?)
                .with_timezone_opt(zone.clone()),
            ),
            (BigQueryFieldType::Numeric(params), DataType::Decimal128(precision, scale)) => {
                let unscaled = Self::decimal_values(kind, values, *params, NUMERIC_SCALE)?
                    .into_iter()
                    .map(|value| {
                        value
                            .map(|value| {
                                value.to_i128().ok_or_else(|| {
                                    CodecError::out_of_range("a NUMERIC wider than 128 bits")
                                })
                            })
                            .transpose()
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Arc::new(
                    Decimal128Array::from(unscaled)
                        .with_precision_and_scale(*precision, *scale)
                        .map_err(CodecError::layout)?,
                )
            }
            (BigQueryFieldType::BigNumeric(params), DataType::Decimal256(precision, scale)) => {
                Arc::new(
                    Decimal256Array::from(Self::decimal_values(
                        kind,
                        values,
                        *params,
                        BIGNUMERIC_SCALE,
                    )?)
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(CodecError::layout)?,
                )
            }
            (BigQueryFieldType::Interval, _) => Arc::new(IntervalMonthDayNanoArray::from(
                Self::native_values(kind, values, |value| match value {
                    FieldValue::Interval(interval) => Ok(IntervalMonthDayNano::new(
                        interval.months,
                        interval.days,
                        interval.nanos,
                    )),
                    other => Err(other),
                })?,
            )),
            (
                BigQueryFieldType::Struct(_) | BigQueryFieldType::Range(_),
                DataType::Struct(arrow_fields),
            ) => {
                let fields = field_type.message_fields().ok_or_else(|| {
                    CodecError::layout(ArrowError::SchemaError(format!(
                        "a {} column has no fields",
                        kind.name()
                    )))
                })?;
                let mut field_values = vec![Vec::with_capacity(values.len()); fields.len()];
                let mut valid = Vec::with_capacity(values.len());
                for value in values {
                    match value {
                        Some(FieldValue::Struct(members)) => {
                            valid.push(true);
                            for (column, member) in field_values.iter_mut().zip(members) {
                                column.push(member);
                            }
                        }
                        // The fields of a NULL STRUCT are NULL too, which its own
                        // validity masks for a REQUIRED field.
                        None => {
                            valid.push(false);
                            for column in &mut field_values {
                                column.push(None);
                            }
                        }
                        Some(other) => return Err(CodecError::holds(kind, &other)),
                    }
                }
                let arrays = fields
                    .iter()
                    .zip(field_values)
                    .map(|(field, values)| {
                        Self::column_array(field, values).map_err(|err| err.at_field(&field.name))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Arc::new(
                    StructArray::try_new(
                        arrow_fields.clone(),
                        arrays,
                        Some(NullBuffer::from(valid)),
                    )
                    .map_err(CodecError::layout)?,
                )
            }
            (_, data_type) => {
                return Err(CodecError::layout(ArrowError::SchemaError(format!(
                    "a {} column has the read type {data_type}",
                    kind.name()
                ))))
            }
        })
    }

    /// The values of a column of `kind` as `native` takes them out, NULLs kept.
    ///
    /// # Errors
    /// A value of another kind, which a column's own decoding never produces.
    fn native_values<T>(
        kind: FieldKind,
        values: Vec<Option<FieldValue>>,
        native: impl Fn(FieldValue) -> Result<T, FieldValue>,
    ) -> Result<Vec<Option<T>>, CodecError> {
        values
            .into_iter()
            .map(|value| {
                value
                    .map(&native)
                    .transpose()
                    .map_err(|other| CodecError::holds(kind, &other))
            })
            .collect()
    }

    /// The unscaled values of a decimal column at the scale it declares, `wire_scale` when it
    /// declares none. BigQuery rounds a value with more fractional digits than the declared
    /// scale half away from zero, and refuses one with more digits than the declared
    /// precision.
    fn decimal_values(
        kind: FieldKind,
        values: Vec<Option<FieldValue>>,
        params: Option<BigQueryDecimalParams>,
        wire_scale: u32,
    ) -> Result<Vec<Option<i256>>, CodecError> {
        let unscaled = Self::native_values(kind, values, |value| match value {
            FieldValue::Decimal(unscaled) => Ok(unscaled),
            other => Err(other),
        })?;
        let Some(params) = params else {
            return Ok(unscaled);
        };
        let out_of_range = || {
            CodecError::out_of_range(format!(
                "the value has more than {} digits, the precision of {}({}, {})",
                params.precision,
                kind.name(),
                params.precision,
                params.scale
            ))
        };
        unscaled
            .into_iter()
            .map(|value| {
                value
                    .map(|value| params.rescale(value, wire_scale).ok_or_else(out_of_range))
                    .transpose()
            })
            .collect()
    }
}

impl BigQueryDecimalParams {
    /// `unscaled`, a decimal at `from_scale`, unscaled at this scale, as BigQuery writes a
    /// value into a column of this precision and scale: rounded half away from zero, and
    /// `None` when it has more digits than the precision.
    pub(super) fn rescale(self, unscaled: i256, from_scale: u32) -> Option<i256> {
        let limit = i256::from_i128(10).checked_pow(u32::from(self.precision))?;
        rescale_decimal(unscaled, from_scale, u32::from(self.scale))
            .filter(|rescaled| rescaled.wrapping_abs() < limit)
    }
}

/// `unscaled`, a decimal at `from_scale`, unscaled at `scale`, rounded half away from zero as
/// BigQuery rounds it; `None` when it does not fit 256 bits.
pub(super) fn rescale_decimal(unscaled: i256, from_scale: u32, scale: u32) -> Option<i256> {
    let ten = i256::from_i128(10);
    if scale < from_scale {
        let divisor = ten.checked_pow(from_scale - scale)?;
        let quotient = unscaled.wrapping_div(divisor);
        let remainder = unscaled.wrapping_rem(divisor).wrapping_abs();
        if remainder.wrapping_mul(i256::from_i128(2)) >= divisor {
            quotient.checked_add(unscaled.signum())
        } else {
            Some(quotient)
        }
    } else {
        ten.checked_pow(scale - from_scale)
            .and_then(|factor| unscaled.checked_mul(factor))
    }
}

/// The bytes of a length-delimited field at the front of `buffer`.
fn decode_length_delimited<'b>(buffer: &mut &'b [u8]) -> Result<&'b [u8], CodecError> {
    let length = decode_varint(buffer).map_err(CodecError::malformed_row)?;
    let length = usize::try_from(length)
        .ok()
        .filter(|length| *length <= buffer.len())
        .ok_or_else(|| CodecError::malformed_row("a field runs past the end of the row"))?;
    let (value, rest) = buffer.split_at(length);
    *buffer = rest;
    Ok(value)
}

/// The text of a length-delimited string field at the front of `buffer`.
fn decode_text(wire_type: WireType, buffer: &mut &[u8]) -> Result<String, CodecError> {
    if wire_type != WireType::LengthDelimited {
        return Err(CodecError::type_mismatch(format!(
            "a string field has wire type {wire_type:?}"
        )));
    }
    let bytes = decode_length_delimited(buffer)?;
    String::from_utf8(bytes.to_vec())
        .map_err(|_| CodecError::invalid_text("a string field is not UTF-8"))
}

impl CodecError {
    /// A row whose bytes are not a proto2 message.
    fn malformed_row(error: impl std::fmt::Display) -> Self {
        Self::invalid_text(format!("the row is not a proto message: {error}"))
    }

    /// A field of `kind` sent with a wire type the encoder never writes for it.
    fn wire_type_mismatch(kind: FieldKind, wire_type: WireType) -> Self {
        Self::type_mismatch(format!(
            "a {} field does not take a value of wire type {wire_type:?}",
            kind.name()
        ))
    }

    /// A decoded column of `kind` holding `value` of another kind, which a column's own
    /// decoding never produces.
    fn holds(kind: FieldKind, value: &FieldValue) -> Self {
        Self::type_mismatch(format!(
            "a {} column holds the value {value:?}",
            kind.name()
        ))
    }

    /// Arrow refusing the decoded columns, which is a defect of the fake.
    fn layout(error: ArrowError) -> Self {
        Self::new(
            BigQueryCodecErrorKind::Custom,
            format!("the decoded columns do not make a batch: {error}"),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::read::BigQueryBatchRows;
    use crate::types::decimal::{self, BIGNUMERIC_SCALE, NUMERIC_SCALE};
    use crate::types::testkit::{error_kind, field, written, Canonical};
    use crate::{
        BigQueryChangeSequenceNumber, BigQueryDecimalParams, BigQueryFieldSchema,
        BigQueryFieldType, BigQueryRange, BigQueryRangeElementType,
    };
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Decimal128Type, Int64Type, TimestampMicrosecondType};
    use arrow_array::Array;
    use arrow_schema::DataType;
    use gcloud_sdk::prost::encoding::{encode_key, encode_varint};
    use proptest::prelude::*;
    use proptest::test_runner::{Config, TestCaseError, TestRunner};
    use serde::de::DeserializeOwned;
    use serde::Deserialize;

    /// One column type in every mode.
    #[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
    struct Modes<V> {
        nullable: Option<V>,
        required: V,
        repeated: Vec<V>,
    }

    /// One column type in every mode, at the top level, in a STRUCT and in a REPEATED STRUCT.
    #[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
    struct ModeRow<V> {
        nullable: Option<V>,
        required: V,
        repeated: Vec<V>,
        record: Modes<V>,
        records: Vec<Modes<V>>,
    }

    impl<V> Modes<V> {
        fn map<U>(self, convert: &impl Fn(V) -> U) -> Modes<U> {
            Modes {
                nullable: self.nullable.map(convert),
                required: convert(self.required),
                repeated: self.repeated.into_iter().map(convert).collect(),
            }
        }

        fn columns(field_type: &BigQueryFieldType) -> Vec<BigQueryFieldSchema> {
            vec![
                field("nullable", field_type.clone(), BigQueryFieldMode::Nullable),
                field("required", field_type.clone(), BigQueryFieldMode::Required),
                field("repeated", field_type.clone(), BigQueryFieldMode::Repeated),
            ]
        }
    }

    impl<V: Clone> ModeRow<V> {
        /// Row `index` over `values`: every third NULLABLE value NULL, up to three REPEATED
        /// values from the row on, and every other row with no REPEATED STRUCT.
        fn new(values: &[V], index: usize) -> Self {
            let modes = Modes {
                nullable: (index % 3 != 2).then(|| values[index].clone()),
                required: values[index].clone(),
                repeated: values[index..(index + 3).min(values.len())].to_vec(),
            };
            ModeRow {
                nullable: modes.nullable.clone(),
                required: modes.required.clone(),
                repeated: modes.repeated.clone(),
                records: if index.is_multiple_of(2) {
                    vec![modes.clone()]
                } else {
                    Vec::new()
                },
                record: modes,
            }
        }

        fn schema(field_type: &BigQueryFieldType) -> BigQueryTableSchema {
            let record = BigQueryFieldType::Struct(Modes::<V>::columns(field_type));
            let mut fields = Modes::<V>::columns(field_type);
            fields.push(field("record", record.clone(), BigQueryFieldMode::Required));
            fields.push(field("records", record, BigQueryFieldMode::Repeated));
            BigQueryTableSchema { fields }
        }
    }

    impl<V> ModeRow<V> {
        fn map<U>(self, convert: &impl Fn(V) -> U) -> ModeRow<U> {
            ModeRow {
                nullable: self.nullable.map(convert),
                required: convert(self.required),
                repeated: self.repeated.into_iter().map(convert).collect(),
                record: self.record.map(convert),
                records: self
                    .records
                    .into_iter()
                    .map(|modes| modes.map(convert))
                    .collect(),
            }
        }
    }

    fn unexpected(value: &Canonical) -> ! {
        panic!("the case's strategy gave {value:?}")
    }

    /// Writes `values` as `write` gives them in every mode of `field_type`, through the
    /// encoder and the fake, and checks that reading them as `R` gives them back.
    fn round_trip<W: Serialize, R: DeserializeOwned>(
        field_type: &BigQueryFieldType,
        values: &[Canonical],
        write: impl Fn(&Canonical) -> W,
        read: impl Fn(R) -> Canonical,
    ) -> Result<(), TestCaseError> {
        let schema = ModeRow::<Canonical>::schema(field_type);
        let rows: Vec<ModeRow<Canonical>> = (0..values.len())
            .map(|index| ModeRow::new(values, index))
            .collect();
        let written = rows
            .iter()
            .cloned()
            .map(|row| row.map(&|value| write(&value)));
        let batch = ProtoBatchBuilder::encode(&schema, written, 0)
            .map_err(|err| TestCaseError::fail(format!("{field_type:?} into the fake: {err}")))?;
        let read_back = BigQueryBatchRows::<ModeRow<R>>::new(&batch)
            .map(|row| row.map(|row| row.map(&read)))
            .collect::<BigQueryResult<Vec<_>>>()
            .map_err(|err| TestCaseError::fail(format!("{field_type:?} out of the fake: {err}")))?;
        prop_assert_eq!(read_back, rows, "{:?}", field_type);
        Ok(())
    }

    fn range_round_trip(
        element: BigQueryRangeElementType,
        values: &[Canonical],
    ) -> Result<(), TestCaseError> {
        let field_type = BigQueryFieldType::Range(element);
        let bounds = |value: &Canonical| match value {
            Canonical::Range { start, end, .. } => (*start, *end),
            other => unexpected(other),
        };
        let back = |start, end| Canonical::Range {
            element,
            start,
            end,
        };
        match element {
            BigQueryRangeElementType::Date => {
                let day = |bound: i64| i32::try_from(bound).expect("a DATE bound is i32 days");
                round_trip(
                    &field_type,
                    values,
                    |value| {
                        let (start, end) = bounds(value);
                        BigQueryRange {
                            start: start.map(day),
                            end: end.map(day),
                        }
                    },
                    |range: BigQueryRange<i32>| {
                        back(range.start.map(i64::from), range.end.map(i64::from))
                    },
                )
            }
            BigQueryRangeElementType::DateTime | BigQueryRangeElementType::Timestamp => round_trip(
                &field_type,
                values,
                |value| {
                    let (start, end) = bounds(value);
                    BigQueryRange { start, end }
                },
                |range: BigQueryRange<i64>| back(range.start, range.end),
            ),
        }
    }

    fn check(field_type: &BigQueryFieldType, values: &[Canonical]) -> Result<(), TestCaseError> {
        use BigQueryFieldType as FieldType;
        match field_type {
            FieldType::Int64 => round_trip(
                field_type,
                values,
                |value| match value {
                    Canonical::Int64(integer) => *integer,
                    other => unexpected(other),
                },
                Canonical::Int64,
            ),
            FieldType::Float64 => round_trip(
                field_type,
                values,
                |value| match value {
                    Canonical::Float64(bits) => f64::from_bits(*bits),
                    other => unexpected(other),
                },
                |float: f64| Canonical::Float64(float.to_bits()),
            ),
            FieldType::Bool => round_trip(
                field_type,
                values,
                |value| match value {
                    Canonical::Bool(flag) => *flag,
                    other => unexpected(other),
                },
                Canonical::Bool,
            ),
            FieldType::String { .. } => round_trip(
                field_type,
                values,
                |value| match value {
                    Canonical::String(text) => text.clone(),
                    other => unexpected(other),
                },
                Canonical::String,
            ),
            FieldType::Bytes { .. } => round_trip(
                field_type,
                values,
                |value| match value {
                    Canonical::Bytes(bytes) => serde_bytes::ByteBuf::from(bytes.clone()),
                    other => unexpected(other),
                },
                |bytes: serde_bytes::ByteBuf| Canonical::Bytes(bytes.into_vec()),
            ),
            FieldType::Date => round_trip(
                field_type,
                values,
                |value| match value {
                    Canonical::Date(days) => *days,
                    other => unexpected(other),
                },
                Canonical::Date,
            ),
            FieldType::Time => round_trip(
                field_type,
                values,
                |value| match value {
                    Canonical::Time(micros) => *micros,
                    other => unexpected(other),
                },
                Canonical::Time,
            ),
            FieldType::DateTime => round_trip(
                field_type,
                values,
                |value| match value {
                    Canonical::DateTime(micros) => *micros,
                    other => unexpected(other),
                },
                Canonical::DateTime,
            ),
            FieldType::Timestamp => round_trip(
                field_type,
                values,
                |value| match value {
                    Canonical::Timestamp(micros) => *micros,
                    other => unexpected(other),
                },
                Canonical::Timestamp,
            ),
            FieldType::Numeric(_) => round_trip(
                field_type,
                values,
                |value| match value {
                    Canonical::Numeric(unscaled) => {
                        written(|out| decimal::fmt_decimal_i128(*unscaled, NUMERIC_SCALE, out))
                    }
                    other => unexpected(other),
                },
                |text: String| {
                    Canonical::Numeric(
                        decimal::parse_numeric(&text)
                            .expect("NUMERIC text")
                            .to_i128()
                            .expect("a NUMERIC fits i128"),
                    )
                },
            ),
            FieldType::BigNumeric(_) => round_trip(
                field_type,
                values,
                |value| match value {
                    Canonical::BigNumeric(unscaled) => {
                        written(|out| decimal::fmt_decimal_i256(*unscaled, BIGNUMERIC_SCALE, out))
                    }
                    other => unexpected(other),
                },
                |text: String| {
                    Canonical::BigNumeric(decimal::parse_bignumeric(&text).expect("BIGNUMERIC"))
                },
            ),
            FieldType::Geography => round_trip(
                field_type,
                values,
                |value| match value {
                    Canonical::Geography(text) => text.clone(),
                    other => unexpected(other),
                },
                Canonical::Geography,
            ),
            FieldType::Json => round_trip(
                field_type,
                values,
                |value| match value {
                    Canonical::Json(json) => json.to_string(),
                    other => unexpected(other),
                },
                |text: String| Canonical::Json(serde_json::from_str(&text).expect("JSON text")),
            ),
            FieldType::Interval => round_trip(
                field_type,
                values,
                |value| match value {
                    Canonical::Interval(interval) => *interval,
                    other => unexpected(other),
                },
                Canonical::Interval,
            ),
            FieldType::Range(element) => range_round_trip(*element, values),
            FieldType::Struct(_) => panic!("STRUCT is covered by every case's record columns"),
        }
    }

    #[test]
    fn every_type_and_mode_round_trips_through_the_fake() {
        use BigQueryFieldType as FieldType;
        let mut cases: Vec<(FieldType, BoxedStrategy<Canonical>)> = [
            FieldType::Int64,
            FieldType::Float64,
            FieldType::Bool,
            FieldType::String { max_length: None },
            FieldType::Bytes { max_length: None },
            FieldType::Date,
            FieldType::Time,
            FieldType::DateTime,
            FieldType::Timestamp,
            FieldType::Numeric(None),
            FieldType::BigNumeric(None),
            FieldType::Geography,
            FieldType::Json,
            FieldType::Interval,
        ]
        .into_iter()
        .map(|field_type| {
            let values = Canonical::strategy(FieldKind::from(&field_type));
            (field_type, values)
        })
        .collect();
        for element in [
            BigQueryRangeElementType::Date,
            BigQueryRangeElementType::DateTime,
            BigQueryRangeElementType::Timestamp,
        ] {
            cases.push((
                FieldType::Range(element),
                Canonical::range_strategy(element),
            ));
        }
        for (field_type, values) in cases {
            TestRunner::new(Config::with_cases(32))
                .run(&proptest::collection::vec(values, 1..6), |values| {
                    check(&field_type, &values)
                })
                .unwrap_or_else(|failure| panic!("{field_type:?}: {failure}"));
        }
    }

    #[derive(Serialize)]
    struct Window {
        window: Option<BigQueryRange<i64>>,
    }

    #[test]
    fn an_unbounded_range_bound_is_a_null_child_of_a_valid_struct() {
        let schema = BigQueryTableSchema {
            fields: vec![field(
                "window",
                BigQueryFieldType::Range(BigQueryRangeElementType::Timestamp),
                BigQueryFieldMode::Nullable,
            )],
        };
        let rows = [
            Window {
                window: Some(BigQueryRange {
                    start: None,
                    end: Some(1_000),
                }),
            },
            Window { window: None },
        ];
        let batch = ProtoBatchBuilder::encode(&schema, rows, 0).expect("valid test rows");
        let window = batch.column(0).as_struct();
        assert_eq!(
            (window.is_valid(0), window.is_null(1)),
            (true, true),
            "the RANGE itself"
        );
        let start = window
            .column_by_name("start")
            .expect("a start bound")
            .as_primitive::<TimestampMicrosecondType>();
        let end = window
            .column_by_name("end")
            .expect("an end bound")
            .as_primitive::<TimestampMicrosecondType>();
        assert!(start.is_null(0));
        assert_eq!((end.is_valid(0), end.value(0)), (true, 1_000));
    }

    /// A message of `(field number, wire type, payload)` fields, the payload of a
    /// length-delimited field without its length.
    fn message(fields: &[(u32, WireType, &[u8])]) -> Vec<u8> {
        let mut out = Vec::new();
        for (number, wire_type, payload) in fields {
            encode_key(*number, *wire_type, &mut out);
            if *wire_type == WireType::LengthDelimited {
                encode_varint(payload.len() as u64, &mut out);
            }
            out.extend_from_slice(payload);
        }
        out
    }

    fn varint(value: u64) -> Vec<u8> {
        let mut out = Vec::new();
        encode_varint(value, &mut out);
        out
    }

    #[test]
    fn a_struct_checks_its_required_fields_only_when_present() {
        let schema = BigQueryTableSchema {
            fields: vec![field(
                "dimensions",
                BigQueryFieldType::Struct(vec![field(
                    "width",
                    BigQueryFieldType::Int64,
                    BigQueryFieldMode::Required,
                )]),
                BigQueryFieldMode::Nullable,
            )],
        };
        let mut builder = ProtoBatchBuilder::new(&schema);
        builder
            .push(&[])
            .expect("a NULL STRUCT has no fields to check");
        let decoded = builder.finish().expect("the rows make a batch");
        assert!(decoded.rows.column(0).is_null(0));

        let empty_struct = message(&[(1, WireType::LengthDelimited, &[])]);
        assert_eq!(
            error_kind(ProtoBatchBuilder::new(&schema).push(&empty_struct)),
            BigQueryCodecErrorKind::MissingRequiredField
        );
    }

    #[test]
    fn repeated_values_decode_packed_and_one_at_a_time() {
        let schema = BigQueryTableSchema {
            fields: vec![
                field(
                    "readings",
                    BigQueryFieldType::Int64,
                    BigQueryFieldMode::Repeated,
                ),
                field(
                    "labels",
                    BigQueryFieldType::String { max_length: None },
                    BigQueryFieldMode::Repeated,
                ),
            ],
        };
        let packed = [varint(1), varint(2)].concat();
        let row = message(&[
            (1, WireType::LengthDelimited, &packed),
            (1, WireType::Varint, &varint(3)),
            (2, WireType::LengthDelimited, b"north"),
            (2, WireType::LengthDelimited, b"south"),
        ]);
        let mut builder = ProtoBatchBuilder::new(&schema);
        builder.push(&row).expect("a valid row");
        builder.push(&[]).expect("a row with no values");
        let rows = builder.finish().expect("the rows make a batch").rows;

        let readings = rows.column(0).as_list::<i32>();
        let first: Vec<i64> = readings
            .value(0)
            .as_primitive::<Int64Type>()
            .values()
            .to_vec();
        assert_eq!(first, [1, 2, 3]);
        let labels = rows.column(1).as_list::<i32>();
        let first_labels = labels.value(0);
        let first: Vec<&str> = first_labels.as_string::<i32>().iter().flatten().collect();
        assert_eq!(first, ["north", "south"]);
        for list in [readings, labels] {
            assert_eq!(
                (list.is_valid(1), list.value_length(1)),
                (true, 0),
                "an absent REPEATED column is an empty list"
            );
        }
    }

    #[derive(Serialize)]
    struct Order {
        order_id: i64,
    }

    #[test]
    fn cdc_rows_carry_their_change_type_and_sequence_number() {
        let schema = BigQueryTableSchema {
            fields: vec![field(
                "order_id",
                BigQueryFieldType::Int64,
                BigQueryFieldMode::Required,
            )],
        };
        let plan = Arc::new(WritePlan::new(&schema, true));
        let mut builder = ProtoBatchBuilder::from_descriptor(&schema, plan.descriptor())
            .expect("the writer schema names the table's columns");
        let mut encoder = Encoder::new(plan);
        let sequence_number = BigQueryChangeSequenceNumber::from(7);
        for (order_id, change_type, sequence_number) in [
            (1, BigQueryChangeType::Upsert, Some(&sequence_number)),
            (2, BigQueryChangeType::Delete, None),
        ] {
            let mut row = Vec::new();
            encoder
                .encode_change(&Order { order_id }, change_type, sequence_number, &mut row)
                .expect("a valid CDC row");
            builder.push(&row).expect("a valid CDC row");
        }
        let decoded = builder.finish().expect("the rows make a batch");
        assert_eq!(
            decoded.changes,
            Some(vec![
                FakeChange {
                    change_type: BigQueryChangeType::Upsert,
                    sequence_number: Some(sequence_number),
                },
                FakeChange {
                    change_type: BigQueryChangeType::Delete,
                    sequence_number: None,
                },
            ])
        );
        let order_ids: Vec<i64> = decoded
            .rows
            .column(0)
            .as_primitive::<Int64Type>()
            .values()
            .to_vec();
        assert_eq!(order_ids, [1, 2]);
    }

    #[derive(Serialize)]
    struct Parcel {
        dimensions: Dimensions,
    }

    #[derive(Serialize)]
    struct Dimensions {
        height: i64,
        width: i64,
    }

    #[test]
    fn a_writer_schema_finds_nested_fields_by_name() {
        let dimensions = |names: [&str; 2]| BigQueryTableSchema {
            fields: vec![field(
                "dimensions",
                BigQueryFieldType::Struct(
                    names
                        .into_iter()
                        .map(|name| {
                            field(name, BigQueryFieldType::Int64, BigQueryFieldMode::Required)
                        })
                        .collect(),
                ),
                BigQueryFieldMode::Required,
            )],
        };
        let table = dimensions(["width", "height"]);
        let plan = Arc::new(WritePlan::new(&dimensions(["height", "width"]), false));
        let mut builder = ProtoBatchBuilder::from_descriptor(&table, plan.descriptor())
            .expect("the writer schema names the table's columns");
        let mut row = Vec::new();
        Encoder::new(plan)
            .encode(
                &Parcel {
                    dimensions: Dimensions {
                        height: 2,
                        width: 3,
                    },
                },
                &mut row,
            )
            .expect("a valid row");
        builder.push(&row).expect("a valid row");
        let rows = builder.finish().expect("the rows make a batch").rows;
        let dimensions = rows.column(0).as_struct();
        let value = |name: &str| {
            dimensions
                .column_by_name(name)
                .expect("a declared field")
                .as_primitive::<Int64Type>()
                .value(0)
        };
        assert_eq!((value("width"), value("height")), (3, 2));
    }

    #[derive(Serialize)]
    struct Price {
        price: String,
    }

    #[test]
    fn a_declared_numeric_scale_rounds_half_away_from_zero() {
        let schema = BigQueryTableSchema {
            fields: vec![field(
                "price",
                BigQueryFieldType::Numeric(Some(BigQueryDecimalParams {
                    precision: 5,
                    scale: 2,
                })),
                BigQueryFieldMode::Required,
            )],
        };
        let prices = ["1.005", "-1.005", "999.994"].map(|price| Price {
            price: price.to_string(),
        });
        let rows = ProtoBatchBuilder::encode(&schema, prices, 0).expect("prices in range");
        let prices = rows.column(0).as_primitive::<Decimal128Type>();
        assert_eq!(prices.data_type(), &DataType::Decimal128(5, 2));
        assert_eq!(prices.values().to_vec(), [101, -101, 99_999]);
    }

    #[test]
    fn a_value_beyond_the_declared_precision_is_out_of_range() {
        let schema = BigQueryTableSchema {
            fields: vec![field(
                "price",
                BigQueryFieldType::Numeric(Some(BigQueryDecimalParams {
                    precision: 5,
                    scale: 2,
                })),
                BigQueryFieldMode::Required,
            )],
        };
        let rounds_up = [Price {
            price: "999.995".to_string(),
        }];
        match ProtoBatchBuilder::encode(&schema, rounds_up, 0) {
            Err(crate::errors::BigQueryError::SerializeError(err)) => {
                assert_eq!(err.kind, BigQueryCodecErrorKind::OutOfRange);
            }
            other => panic!("expected a serialize error, got {other:?}"),
        }
    }
}
