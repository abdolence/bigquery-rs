//! Rows on their way into the fake's tables and query results.
//!
//! Every row reaches the fake as a proto2 message, the form Storage Write takes: a test's serde
//! rows are encoded by the write path's own [`Encoder`] first, and appended rows arrive
//! encoded. [`ProtoBatchBuilder`] decodes the messages into the Arrow layout a read session
//! sends, so a value crosses the same codecs it crosses against BigQuery.

use crate::errors::BigQueryCodecErrorKind;
use crate::testing::state::FakeChange;
use crate::types::error::CodecError;
use crate::types::kind::FieldKind;
use crate::write::descriptor::{WritePlan, CHANGE_SEQUENCE_NUMBER_COLUMN, CHANGE_TYPE_COLUMN};
use crate::write::encoder::Encoder;
use crate::{BigQueryChangeType, BigQueryFieldMode, BigQueryResult, BigQueryTableSchema};
use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array, RecordBatch, RecordBatchOptions,
    StringArray,
};
use arrow_schema::SchemaRef;
use gcloud_sdk::prost::encoding::{decode_key, decode_varint, WireType};
use gcloud_sdk::prost_types::DescriptorProto;
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Where the values of one proto field number go.
#[derive(Clone, Copy, Debug)]
enum FieldTarget {
    /// The table column at this index.
    Column(usize),
    /// The CDC `_CHANGE_TYPE` pseudo-column.
    ChangeType,
    /// The CDC `_CHANGE_SEQUENCE_NUMBER` pseudo-column.
    ChangeSequenceNumber,
}

/// One decoded value of a column.
#[derive(Clone, Debug)]
enum FieldValue {
    Int64(i64),
    Float64(f64),
    Bool(bool),
    String(String),
    Bytes(Vec<u8>),
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
    schema: BigQueryTableSchema,
    arrow_schema: SchemaRef,
    targets: BTreeMap<u32, FieldTarget>,
    /// Per column, one value per row decoded so far.
    columns: Vec<Vec<Option<FieldValue>>>,
    /// Per row, its change, when the rows carry the CDC pseudo-columns.
    changes: Option<Vec<FakeChange>>,
    rows: usize,
}

/// One row on its way in, kept apart until it decodes whole.
struct RowValues {
    values: Vec<Option<FieldValue>>,
    change_type: Option<String>,
    sequence_number: Option<String>,
}

impl ProtoBatchBuilder {
    /// Rows numbered as the crate's own encoder numbers them: column `i` is field `i + 1`.
    pub(super) fn new(schema: &BigQueryTableSchema) -> Self {
        let targets = (0..schema.fields.len())
            .filter_map(|index| {
                let number = u32::try_from(index + 1).ok()?;
                Some((number, FieldTarget::Column(index)))
            })
            .collect();
        Self::with_targets(schema, targets, false)
    }

    /// Rows described by `descriptor`, the writer schema of an append request. Each field goes
    /// to the table column of its name, ignoring case as BigQuery does, and the CDC
    /// pseudo-columns `_CHANGE_TYPE` and `_CHANGE_SEQUENCE_NUMBER` make the rows changes.
    ///
    /// # Errors
    /// [`UnknownField`](BigQueryCodecErrorKind::UnknownField) for a field that names no column
    /// of the table.
    pub(super) fn from_descriptor(
        schema: &BigQueryTableSchema,
        descriptor: &DescriptorProto,
    ) -> Result<Self, CodecError> {
        let mut targets = BTreeMap::new();
        let mut cdc = false;
        for field in &descriptor.field {
            let name = field.name();
            let target = if name == CHANGE_TYPE_COLUMN {
                cdc = true;
                FieldTarget::ChangeType
            } else if name == CHANGE_SEQUENCE_NUMBER_COLUMN {
                FieldTarget::ChangeSequenceNumber
            } else {
                let index = schema
                    .fields
                    .iter()
                    .position(|column| column.name.eq_ignore_ascii_case(name))
                    .ok_or_else(|| {
                        CodecError::new(
                            BigQueryCodecErrorKind::UnknownField,
                            "the writer schema has a field the table has no column for",
                        )
                        .at_field(name)
                    })?;
                FieldTarget::Column(index)
            };
            let number = u32::try_from(field.number()).map_err(|_| {
                CodecError::new(
                    BigQueryCodecErrorKind::UnknownField,
                    format!(
                        "field number {} is not a proto field number",
                        field.number()
                    ),
                )
                .at_field(name)
            })?;
            targets.insert(number, target);
        }
        Ok(Self::with_targets(schema, targets, cdc))
    }

    fn with_targets(
        schema: &BigQueryTableSchema,
        targets: BTreeMap<u32, FieldTarget>,
        cdc: bool,
    ) -> Self {
        Self {
            schema: schema.clone(),
            arrow_schema: Arc::new(schema.arrow_read_schema()),
            targets,
            columns: vec![Vec::new(); schema.fields.len()],
            changes: cdc.then(Vec::new),
            rows: 0,
        }
    }

    /// Decodes `message` as the next row. A row that fails leaves the rows before it as they
    /// were.
    ///
    /// # Errors
    /// The column at fault for a field of the wrong wire type, a REQUIRED column the row has
    /// no value for, a type the fake does not decode, or bytes that are not a proto message.
    pub(super) fn push(&mut self, message: &[u8]) -> Result<(), CodecError> {
        let mut row = RowValues {
            values: vec![None; self.columns.len()],
            change_type: None,
            sequence_number: None,
        };
        let mut buffer = message;
        while !buffer.is_empty() {
            let (number, wire_type) = decode_key(&mut buffer).map_err(CodecError::malformed_row)?;
            let target = self.targets.get(&number).copied().ok_or_else(|| {
                CodecError::new(
                    BigQueryCodecErrorKind::UnknownField,
                    format!("field number {number} is not in the writer schema"),
                )
            })?;
            match target {
                FieldTarget::Column(index) => {
                    let column = &self.schema.fields[index];
                    if column.mode == BigQueryFieldMode::Repeated {
                        return Err(CodecError::unsupported(
                            "the fake does not decode REPEATED columns",
                        )
                        .at_field(&column.name));
                    }
                    let kind = FieldKind::from(&column.field_type);
                    let value = FieldValue::decode(kind, wire_type, &mut buffer)
                        .map_err(|err| err.at_field(&column.name))?;
                    row.values[index] = Some(value);
                }
                FieldTarget::ChangeType => {
                    row.change_type = Some(decode_text(wire_type, &mut buffer)?);
                }
                FieldTarget::ChangeSequenceNumber => {
                    row.sequence_number = Some(decode_text(wire_type, &mut buffer)?);
                }
            }
        }
        for (column, value) in self.schema.fields.iter().zip(&row.values) {
            if value.is_none() && column.mode == BigQueryFieldMode::Required {
                return Err(CodecError::new(
                    BigQueryCodecErrorKind::MissingRequiredField,
                    "the row has no value for a REQUIRED column",
                )
                .at_field(&column.name));
            }
        }
        if let Some(changes) = &mut self.changes {
            changes.push(row.change()?);
        }
        for (column, value) in self.columns.iter_mut().zip(row.values) {
            column.push(value);
        }
        self.rows += 1;
        Ok(())
    }

    /// The rows decoded so far.
    ///
    /// # Errors
    /// Arrow's error if the columns do not make a batch, which is a defect of the fake.
    pub(super) fn finish(self) -> Result<DecodedRows, CodecError> {
        let arrays = self
            .schema
            .fields
            .iter()
            .zip(self.columns)
            .map(|(column, values)| {
                FieldValue::array(FieldKind::from(&column.field_type), values)
                    .map_err(|err| err.at_field(&column.name))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let options = RecordBatchOptions::new().with_row_count(Some(self.rows));
        let rows = RecordBatch::try_new_with_options(self.arrow_schema, arrays, &options).map_err(
            |err| {
                CodecError::new(
                    BigQueryCodecErrorKind::Custom,
                    format!("the decoded columns do not make a batch: {err}"),
                )
            },
        )?;
        Ok(DecodedRows {
            rows,
            changes: self.changes,
        })
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

impl RowValues {
    /// The change a CDC row asks for.
    fn change(&self) -> Result<FakeChange, CodecError> {
        let text = self.change_type.as_deref().ok_or_else(|| {
            CodecError::new(
                BigQueryCodecErrorKind::MissingRequiredField,
                "a CDC row has no change type",
            )
            .at_field(CHANGE_TYPE_COLUMN)
        })?;
        let change_type = [BigQueryChangeType::Upsert, BigQueryChangeType::Delete]
            .into_iter()
            .find(|change_type| change_type.name() == text)
            .ok_or_else(|| {
                CodecError::invalid_text(format!("{text:?} is not UPSERT or DELETE"))
                    .at_field(CHANGE_TYPE_COLUMN)
            })?;
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
    /// Reads one value of a column of `kind` from the front of `buffer`, which a key of
    /// `wire_type` started. The wire forms are those the crate's own encoder writes.
    fn decode(
        kind: FieldKind,
        wire_type: WireType,
        buffer: &mut &[u8],
    ) -> Result<FieldValue, CodecError> {
        match (kind, wire_type) {
            // An int64 varint carries the two's complement bits of the value.
            (FieldKind::Int64, WireType::Varint) => Ok(FieldValue::Int64(
                decode_varint(buffer)
                    .map_err(CodecError::malformed_row)?
                    .cast_signed(),
            )),
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
            (FieldKind::String, WireType::LengthDelimited) => {
                Ok(FieldValue::String(decode_text(wire_type, buffer)?))
            }
            (FieldKind::Bytes, WireType::LengthDelimited) => {
                Ok(FieldValue::Bytes(decode_length_delimited(buffer)?.to_vec()))
            }
            (
                FieldKind::Int64
                | FieldKind::Bool
                | FieldKind::Float64
                | FieldKind::String
                | FieldKind::Bytes,
                _,
            ) => Err(CodecError::type_mismatch(format!(
                "a {} column does not take a value of wire type {wire_type:?}",
                kind.name()
            ))),
            _ => Err(CodecError::unsupported(format!(
                "the fake does not decode {} columns",
                kind.name()
            ))),
        }
    }

    /// The Arrow array of a column of `kind` holding `values`.
    fn array(kind: FieldKind, values: Vec<Option<FieldValue>>) -> Result<ArrayRef, CodecError> {
        Ok(match kind {
            FieldKind::Int64 => Arc::new(Int64Array::from(Self::native_values(
                kind,
                values,
                |value| match value {
                    FieldValue::Int64(value) => Ok(value),
                    other => Err(other),
                },
            )?)),
            FieldKind::Float64 => Arc::new(Float64Array::from(Self::native_values(
                kind,
                values,
                |value| match value {
                    FieldValue::Float64(value) => Ok(value),
                    other => Err(other),
                },
            )?)),
            FieldKind::Bool => Arc::new(BooleanArray::from(Self::native_values(
                kind,
                values,
                |value| match value {
                    FieldValue::Bool(value) => Ok(value),
                    other => Err(other),
                },
            )?)),
            FieldKind::String => Arc::new(StringArray::from(Self::native_values(
                kind,
                values,
                |value| match value {
                    FieldValue::String(value) => Ok(value),
                    other => Err(other),
                },
            )?)),
            FieldKind::Bytes => Arc::new(BinaryArray::from_iter(Self::native_values(
                kind,
                values,
                |value| match value {
                    FieldValue::Bytes(value) => Ok(value),
                    other => Err(other),
                },
            )?)),
            other => {
                return Err(CodecError::unsupported(format!(
                    "the fake does not decode {} columns",
                    other.name()
                )))
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
                value.map(&native).transpose().map_err(|other| {
                    CodecError::type_mismatch(format!(
                        "a {} column holds the value {other:?}",
                        kind.name()
                    ))
                })
            })
            .collect()
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
}
