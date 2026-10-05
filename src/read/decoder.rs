//! Storage Read Arrow batches to serde.
//!
//! A [`BatchDecoder`] wraps one `RecordBatch`. Column views into the Arrow buffers are built
//! lazily, once per batch, and only for the columns a target names, so an Arrow type this
//! module does not know fails only the rows whose target asks for that column. A struct target
//! is matched to the columns once per batch and cached under the address of its `&'static`
//! fields list ([`Plan`]).
//!
//! Every value is read in the form the type mapping fixes for its column type: integers range
//! checked by serde's own visitors, temporal types as text for jiff and as integers for the
//! crate's wrappers and integer targets, NUMERIC and BIGNUMERIC as canonical text, as `f64`, or
//! as an integer when the value is whole.

use crate::errors::BigQueryCodecErrorKind;
#[cfg(test)]
use crate::read::keys::KeyMode;
use crate::read::keys::{FieldMap, Plan};
use crate::types::civil;
use crate::types::decimal::{self, BIGNUMERIC_SCALE, NUMERIC_SCALE};
use crate::types::error::CodecError;
use crate::types::interval::BigQueryInterval;
use crate::types::kind::FieldKind;
use crate::types::temporal::temporal_tag_kind;
use crate::BigQueryResult;
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Decimal128Type, Decimal256Type, Float64Type, Int64Type, IntervalMonthDayNano,
    IntervalMonthDayNanoType, Time64MicrosecondType, TimestampMicrosecondType,
};
use arrow_array::{Array, ArrayRef, BinaryArray, BooleanArray, RecordBatch, StringArray};
use arrow_buffer::{i256, NullBuffer};
use arrow_schema::{DataType, Field};
use serde::de::value::{BorrowedStrDeserializer, I32Deserializer, I64Deserializer, U8Deserializer};
use serde::de::{self, DeserializeOwned, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde::Deserialize;
use std::cell::{Cell, OnceCell, RefCell};
use std::marker::PhantomData;
use std::rc::Rc;

/// The typed view of one column's values.
enum ColumnValues<'a> {
    Int64(&'a [i64]),
    Float64(&'a [f64]),
    Bool(&'a BooleanArray),
    /// STRING and GEOGRAPHY.
    String(&'a StringArray),
    /// JSON: the text for a string target, parsed for any other. The batch's state is kept for
    /// the values holding JSON `null` that an `Option` target reads as `None`.
    Json {
        text: &'a StringArray,
        state: Rc<BatchState>,
    },
    Bytes(&'a BinaryArray),
    Date(&'a [i32]),
    Time(&'a [i64]),
    DateTime(&'a [i64]),
    Timestamp(&'a [i64]),
    Numeric {
        unscaled: &'a [i128],
        scale: u32,
    },
    BigNumeric {
        unscaled: &'a [i256],
        scale: u32,
    },
    Interval(&'a [IntervalMonthDayNano]),
    List {
        offsets: &'a [i32],
        items: Box<Column<'a>>,
    },
    /// STRUCT and RANGE.
    Struct(Box<StructColumns<'a>>),
}

pub(crate) struct Column<'a> {
    nulls: Option<&'a NullBuffer>,
    values: ColumnValues<'a>,
}

impl<'a> Column<'a> {
    fn new(
        field: &'a Field,
        array: &'a ArrayRef,
        state: &Rc<BatchState>,
    ) -> Result<Self, CodecError> {
        let unsupported = || CodecError::unsupported_arrow_type(field);
        match field.data_type() {
            DataType::List(item) => {
                let kind = FieldKind::from_arrow_list_item(field, item).ok_or_else(unsupported)?;
                let list = array.as_list::<i32>();
                let items = Column::of_kind(kind, item, list.values(), state)?;
                Ok(Column {
                    nulls: array.nulls(),
                    values: ColumnValues::List {
                        offsets: list.value_offsets(),
                        items: Box::new(items),
                    },
                })
            }
            _ => {
                let kind = FieldKind::from_arrow(field).ok_or_else(unsupported)?;
                Column::of_kind(kind, field, array, state)
            }
        }
    }

    /// A column whose kind has been classified from `field`, so its Arrow type is the one that
    /// kind is sent as.
    fn of_kind(
        kind: FieldKind,
        field: &'a Field,
        array: &'a ArrayRef,
        state: &Rc<BatchState>,
    ) -> Result<Self, CodecError> {
        let values = match kind {
            FieldKind::Int64 => ColumnValues::Int64(array.as_primitive::<Int64Type>().values()),
            FieldKind::Float64 => {
                ColumnValues::Float64(array.as_primitive::<Float64Type>().values())
            }
            FieldKind::Bool => ColumnValues::Bool(array.as_boolean()),
            FieldKind::String | FieldKind::Geography => {
                ColumnValues::String(array.as_string::<i32>())
            }
            FieldKind::Json => ColumnValues::Json {
                text: array.as_string::<i32>(),
                state: state.clone(),
            },
            FieldKind::Bytes => ColumnValues::Bytes(array.as_binary::<i32>()),
            FieldKind::Date => ColumnValues::Date(array.as_primitive::<Date32Type>().values()),
            FieldKind::Time => {
                ColumnValues::Time(array.as_primitive::<Time64MicrosecondType>().values())
            }
            FieldKind::DateTime => {
                ColumnValues::DateTime(array.as_primitive::<TimestampMicrosecondType>().values())
            }
            FieldKind::Timestamp => {
                ColumnValues::Timestamp(array.as_primitive::<TimestampMicrosecondType>().values())
            }
            FieldKind::Numeric => ColumnValues::Numeric {
                unscaled: array.as_primitive::<Decimal128Type>().values(),
                scale: decimal_scale(field.data_type(), NUMERIC_SCALE),
            },
            FieldKind::BigNumeric => ColumnValues::BigNumeric {
                unscaled: array.as_primitive::<Decimal256Type>().values(),
                scale: decimal_scale(field.data_type(), BIGNUMERIC_SCALE),
            },
            FieldKind::Interval => {
                ColumnValues::Interval(array.as_primitive::<IntervalMonthDayNanoType>().values())
            }
            FieldKind::Struct | FieldKind::Range => match field.data_type() {
                DataType::Struct(fields) => {
                    let array = array.as_struct();
                    ColumnValues::Struct(Box::new(StructColumns::new(
                        fields.iter().map(AsRef::as_ref).collect(),
                        array.columns().iter().collect(),
                        state.clone(),
                    )))
                }
                _ => return Err(CodecError::unsupported_arrow_type(field)),
            },
        };
        Ok(Column {
            nulls: array.nulls(),
            values,
        })
    }
}

/// Reads the JSON `text` of one value with `read`, which must consume all of it. A parse error,
/// or JSON that does not fit the target, is a `Custom` error, as it is for
/// [`BigQueryJson`](crate::BigQueryJson).
fn parse_json<'a, T>(
    text: &'a str,
    read: impl FnOnce(
        &mut serde_json::Deserializer<serde_json::de::StrRead<'a>>,
    ) -> Result<T, serde_json::Error>,
) -> Result<T, CodecError> {
    let mut deserializer = serde_json::Deserializer::from_str(text);
    read(&mut deserializer)
        .and_then(|value| deserializer.end().map(|()| value))
        .map_err(|err| {
            CodecError::new(
                BigQueryCodecErrorKind::Custom,
                format!("JSON column: {err}"),
            )
        })
}

fn decimal_scale(data_type: &DataType, default: u32) -> u32 {
    match data_type {
        DataType::Decimal128(_, scale) | DataType::Decimal256(_, scale) => {
            u32::try_from(*scale).unwrap_or(default)
        }
        _ => default,
    }
}

/// State shared by every node of one batch.
struct BatchState {
    plans_built: Cell<usize>,
    /// Set when a key probe found an alias: the row being decoded is decoded again, whatever
    /// its result, since a target may have swallowed the probe's error.
    redo: Cell<bool>,
    /// JSON `null` values, as `(column address, index)`, whose `Option` target could not read
    /// the `null` on an earlier pass over the current row, and that are `None` on the next.
    json_nones: RefCell<Vec<(usize, usize)>>,
}

/// A row-shaped set of columns: the batch itself, or one STRUCT column.
pub(crate) struct StructColumns<'a> {
    fields: Vec<&'a Field>,
    arrays: Vec<&'a ArrayRef>,
    columns: Vec<OnceCell<Result<Column<'a>, CodecError>>>,
    plans: RefCell<Vec<(usize, usize, Rc<Plan>)>>,
    state: Rc<BatchState>,
}

impl<'a> StructColumns<'a> {
    fn new(fields: Vec<&'a Field>, arrays: Vec<&'a ArrayRef>, state: Rc<BatchState>) -> Self {
        let columns = (0..fields.len()).map(|_| OnceCell::new()).collect();
        StructColumns {
            fields,
            arrays,
            columns,
            plans: RefCell::new(Vec::new()),
            state,
        }
    }

    pub(crate) fn name(&self, column: usize) -> &'a str {
        self.fields[column].name()
    }

    pub(crate) fn column(&self, column: usize) -> Result<&Column<'a>, CodecError> {
        self.columns[column]
            .get_or_init(|| Column::new(self.fields[column], self.arrays[column], &self.state))
            .as_ref()
            .map_err(Clone::clone)
    }

    pub(crate) fn request_redo(&self) {
        self.state.redo.set(true);
    }

    fn plan(&self, fields: &'static [&'static str]) -> Rc<Plan> {
        let key = (fields.as_ptr() as usize, fields.len());
        if let Some((_, _, plan)) = self
            .plans
            .borrow()
            .iter()
            .find(|(address, length, _)| (*address, *length) == key)
        {
            return plan.clone();
        }
        let names: Vec<&str> = self
            .fields
            .iter()
            .map(|field| field.name().as_str())
            .collect();
        let plan = Rc::new(Plan::resolve(fields, &names));
        self.plans.borrow_mut().push((key.0, key.1, plan.clone()));
        self.state.plans_built.set(self.state.plans_built.get() + 1);
        plan
    }

    fn visit_struct<V: Visitor<'a>>(
        &self,
        row: usize,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, CodecError> {
        let plan = self.plan(fields);
        visitor.visit_map(FieldMap::new(self, &plan, fields, row))
    }

    #[cfg(test)]
    fn name_key_plans(&self) -> usize {
        let own = self
            .plans
            .borrow()
            .iter()
            .filter(|(_, _, p)| p.mode() == KeyMode::Name)
            .count();
        let nested: usize = self
            .columns
            .iter()
            .filter_map(|column| column.get())
            .filter_map(|column| column.as_ref().ok())
            .map(Column::name_key_plans)
            .sum();
        own + nested
    }
}

#[cfg(test)]
impl Column<'_> {
    fn name_key_plans(&self) -> usize {
        match &self.values {
            ColumnValues::Struct(node) => node.name_key_plans(),
            ColumnValues::List { items, .. } => items.name_key_plans(),
            _ => 0,
        }
    }
}

/// Every column of a node, keyed by name: the path of `flatten`, maps and dynamic values,
/// which have to see every column.
struct EveryColumnMap<'n, 'a> {
    node: &'n StructColumns<'a>,
    row: usize,
    next_column: usize,
}

impl<'a> MapAccess<'a> for EveryColumnMap<'_, 'a> {
    type Error = CodecError;

    fn next_key_seed<K: DeserializeSeed<'a>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, CodecError> {
        if self.next_column >= self.node.fields.len() {
            return Ok(None);
        }
        self.next_column += 1;
        seed.deserialize(BorrowedStrDeserializer::new(
            self.node.name(self.next_column - 1),
        ))
        .map(Some)
    }

    fn next_value_seed<S: DeserializeSeed<'a>>(&mut self, seed: S) -> Result<S::Value, CodecError> {
        let column = self.next_column - 1;
        self.node
            .column(column)
            .and_then(|column| seed.deserialize(ValueDeserializer::new(column, self.row)))
            .map_err(|error| error.at_field(self.node.name(column)))
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.node.fields.len() - self.next_column)
    }
}

/// Every column by position, for tuple rows.
struct EveryColumnSeq<'n, 'a> {
    node: &'n StructColumns<'a>,
    row: usize,
    next_column: usize,
}

impl<'a> SeqAccess<'a> for EveryColumnSeq<'_, 'a> {
    type Error = CodecError;

    fn next_element_seed<S: DeserializeSeed<'a>>(
        &mut self,
        seed: S,
    ) -> Result<Option<S::Value>, CodecError> {
        if self.next_column >= self.node.fields.len() {
            return Ok(None);
        }
        let column = self.next_column;
        self.next_column += 1;
        self.node
            .column(column)
            .and_then(|column| seed.deserialize(ValueDeserializer::new(column, self.row)))
            .map(Some)
            .map_err(|error| error.at_field(self.node.name(column)))
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.node.fields.len() - self.next_column)
    }
}

struct ListSeq<'c, 'a> {
    items: &'c Column<'a>,
    start: usize,
    next_element: usize,
    end: usize,
}

impl<'a> SeqAccess<'a> for ListSeq<'_, 'a> {
    type Error = CodecError;

    fn next_element_seed<S: DeserializeSeed<'a>>(
        &mut self,
        seed: S,
    ) -> Result<Option<S::Value>, CodecError> {
        if self.next_element >= self.end {
            return Ok(None);
        }
        let index = self.next_element;
        self.next_element += 1;
        seed.deserialize(ValueDeserializer::new(self.items, index))
            .map(Some)
            .map_err(|error| error.at_index(index - self.start))
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.end - self.next_element)
    }
}

struct ByteSeq<'a> {
    bytes: &'a [u8],
    next_byte: usize,
}

impl<'a> SeqAccess<'a> for ByteSeq<'a> {
    type Error = CodecError;

    fn next_element_seed<S: DeserializeSeed<'a>>(
        &mut self,
        seed: S,
    ) -> Result<Option<S::Value>, CodecError> {
        let Some(&byte) = self.bytes.get(self.next_byte) else {
            return Ok(None);
        };
        self.next_byte += 1;
        seed.deserialize(U8Deserializer::new(byte)).map(Some)
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.bytes.len() - self.next_byte)
    }
}

/// INTERVAL as the sequence `(months, days, nanos)` that [`BigQueryInterval`]'s derived impl
/// reads.
struct IntervalSeq {
    value: IntervalMonthDayNano,
    next_field: u8,
}

impl<'a> SeqAccess<'a> for IntervalSeq {
    type Error = CodecError;

    fn next_element_seed<S: DeserializeSeed<'a>>(
        &mut self,
        seed: S,
    ) -> Result<Option<S::Value>, CodecError> {
        self.next_field += 1;
        match self.next_field {
            1 => seed.deserialize(I32Deserializer::new(self.value.months)),
            2 => seed.deserialize(I32Deserializer::new(self.value.days)),
            3 => seed.deserialize(I64Deserializer::new(self.value.nanoseconds)),
            _ => return Ok(None),
        }
        .map(Some)
    }
}

impl From<IntervalMonthDayNano> for BigQueryInterval {
    fn from(value: IntervalMonthDayNano) -> Self {
        BigQueryInterval {
            months: value.months,
            days: value.days,
            nanos: value.nanoseconds,
        }
    }
}

/// jiff's largest timestamp in microseconds, 25 hours below BigQuery's.
fn jiff_max_micros() -> i64 {
    jiff::Timestamp::MAX.as_microsecond()
}

/// The value of one column at one row.
pub(crate) struct ValueDeserializer<'c, 'a> {
    column: &'c Column<'a>,
    row: usize,
}

impl<'c, 'a> ValueDeserializer<'c, 'a> {
    pub(crate) fn new(column: &'c Column<'a>, row: usize) -> Self {
        ValueDeserializer { column, row }
    }

    #[inline]
    fn is_null(&self) -> bool {
        self.column
            .nulls
            .is_some_and(|nulls| nulls.is_null(self.row))
    }

    #[inline]
    fn non_null(&self) -> Result<(), CodecError> {
        if self.is_null() {
            Err(CodecError::new(
                BigQueryCodecErrorKind::NullForNonOption,
                "NULL value, and the target type is not an Option",
            ))
        } else {
            Ok(())
        }
    }

    /// The canonical text of a value whose only Rust form besides a number or a wrapper is a
    /// string. `Ok(false)` for values that are not of that sort.
    fn text(&self, out: &mut String) -> Result<bool, CodecError> {
        let row = self.row;
        match &self.column.values {
            ColumnValues::Date(values) => civil::fmt_date(values[row], out)?,
            ColumnValues::Time(values) => civil::fmt_time(values[row], out)?,
            ColumnValues::DateTime(values) => civil::fmt_datetime(values[row], out)?,
            ColumnValues::Timestamp(values) => civil::fmt_timestamp(values[row], out)?,
            ColumnValues::Numeric { unscaled, scale } => {
                decimal::fmt_decimal_i128(unscaled[row], *scale, out)
            }
            ColumnValues::BigNumeric { unscaled, scale } => {
                decimal::fmt_decimal_i256(unscaled[row], *scale, out)
            }
            ColumnValues::Interval(values) => BigQueryInterval::from(values[row]).write_bq(out),
            _ => return Ok(false),
        }
        Ok(true)
    }

    /// The integer of a temporal value: days for DATE, microseconds otherwise.
    fn temporal_integer(&self) -> Option<i64> {
        let row = self.row;
        match &self.column.values {
            ColumnValues::Date(values) => Some(i64::from(values[row])),
            ColumnValues::Time(values)
            | ColumnValues::DateTime(values)
            | ColumnValues::Timestamp(values) => Some(values[row]),
            _ => None,
        }
    }

    /// The whole number a NUMERIC or BIGNUMERIC value holds; `OutOfRange` when it has a
    /// fractional part or is beyond `i128`.
    fn whole_decimal(&self) -> Option<Result<i128, CodecError>> {
        let row = self.row;
        let (value, scale) = match &self.column.values {
            ColumnValues::Numeric { unscaled, scale } => (i256::from_i128(unscaled[row]), *scale),
            ColumnValues::BigNumeric { unscaled, scale } => (unscaled[row], *scale),
            _ => return None,
        };
        let divisor = i256::from_i128(10).wrapping_pow(scale);
        if value.wrapping_rem(divisor) != i256::ZERO {
            return Some(Err(CodecError::new(
                BigQueryCodecErrorKind::OutOfRange,
                format!(
                    "{} has a fractional part and cannot be an integer; read it into a String, \
                     an f64 or a BigQueryDecimal",
                    decimal::decimal_string(value, scale)
                ),
            )));
        }
        Some(value.wrapping_div(divisor).to_i128().ok_or_else(|| {
            CodecError::new(
                BigQueryCodecErrorKind::OutOfRange,
                format!(
                    "{} does not fit in i128",
                    decimal::decimal_string(value, scale)
                ),
            )
        }))
    }

    /// An integer target of 64 bits or fewer; serde's visitors check the narrower ranges.
    fn integer<V: Visitor<'a>>(self, visitor: V) -> Result<V::Value, CodecError> {
        self.non_null()?;
        if let Some(integer) = self.temporal_integer() {
            return visitor.visit_i64(integer);
        }
        if let Some(whole) = self.whole_decimal() {
            let whole = whole?;
            return if let Ok(integer) = i64::try_from(whole) {
                visitor.visit_i64(integer)
            } else if let Ok(integer) = u64::try_from(whole) {
                visitor.visit_u64(integer)
            } else {
                Err(CodecError::new(
                    BigQueryCodecErrorKind::OutOfRange,
                    format!("{whole} does not fit in 64 bits; read it into an i128"),
                ))
            };
        }
        self.any_non_null(visitor)
    }

    fn integer128<V: Visitor<'a>>(self, visitor: V) -> Result<V::Value, CodecError> {
        self.non_null()?;
        match self.whole_decimal() {
            Some(whole) => visitor.visit_i128(whole?),
            None => self.integer(visitor),
        }
    }

    fn any_non_null<V: Visitor<'a>>(self, visitor: V) -> Result<V::Value, CodecError> {
        let row = self.row;
        match &self.column.values {
            ColumnValues::Int64(values) => visitor.visit_i64(values[row]),
            ColumnValues::Float64(values) => visitor.visit_f64(values[row]),
            ColumnValues::Bool(values) => visitor.visit_bool(values.value(row)),
            ColumnValues::String(values) => visitor.visit_borrowed_str(values.value(row)),
            ColumnValues::Json { text, .. } => parse_json(text.value(row), |deserializer| {
                de::Deserializer::deserialize_any(deserializer, visitor)
            }),
            ColumnValues::Bytes(values) => visitor.visit_borrowed_bytes(values.value(row)),
            ColumnValues::List { offsets, items } => {
                let (start, end) = (offsets[row] as usize, offsets[row + 1] as usize);
                visitor.visit_seq(ListSeq {
                    items,
                    start,
                    next_element: start,
                    end,
                })
            }
            ColumnValues::Struct(node) => visitor.visit_map(EveryColumnMap {
                node,
                row,
                next_column: 0,
            }),
            _ => {
                let mut text = String::with_capacity(48);
                self.text(&mut text)?;
                visitor.visit_str(&text)
            }
        }
    }

    /// A string target. A jiff `Timestamp` cannot hold the last 25 hours of BigQuery's range,
    /// and its parse error would read as a bad text; it is reported as the range error it is.
    fn string<V: Visitor<'a>>(self, visitor: V) -> Result<V::Value, CodecError> {
        self.non_null()?;
        if let ColumnValues::Json { text, .. } = &self.column.values {
            return visitor.visit_borrowed_str(text.value(self.row));
        }
        if let ColumnValues::Timestamp(values) = &self.column.values {
            let micros = values[self.row];
            let mut text = String::with_capacity(32);
            civil::fmt_timestamp(micros, &mut text)?;
            return visitor.visit_str(&text).map_err(|err| {
                if micros > jiff_max_micros() {
                    CodecError::new(
                        BigQueryCodecErrorKind::OutOfRange,
                        format!(
                            "TIMESTAMP {text} is above jiff::Timestamp's maximum; read it into a \
                             String or an i64 ({err})"
                        ),
                    )
                } else {
                    err
                }
            });
        }
        self.any_non_null(visitor)
    }
}

impl<'a> de::Deserializer<'a> for ValueDeserializer<'_, 'a> {
    type Error = CodecError;

    fn deserialize_any<V: Visitor<'a>>(self, visitor: V) -> Result<V::Value, CodecError> {
        if self.is_null() {
            return visitor.visit_unit();
        }
        self.any_non_null(visitor)
    }

    /// SQL NULL is `None`. JSON `null` is offered to the target as `Some` first, so that a
    /// target that holds `null`, such as `serde_json::Value`, keeps it apart from SQL NULL; a
    /// target that cannot read it fails, and the row is decoded again with that value as `None`.
    fn deserialize_option<V: Visitor<'a>>(self, visitor: V) -> Result<V::Value, CodecError> {
        if self.is_null() {
            return visitor.visit_none();
        }
        let column = self.column;
        if let ColumnValues::Json { text, state } = &column.values {
            if text.value(self.row).trim_ascii() == "null" {
                let position = (std::ptr::from_ref(column) as usize, self.row);
                if state.json_nones.borrow().contains(&position) {
                    return visitor.visit_none();
                }
                let some = visitor.visit_some(self);
                if some.is_err() {
                    state.json_nones.borrow_mut().push(position);
                    state.redo.set(true);
                }
                return some;
            }
        }
        visitor.visit_some(self)
    }

    fn deserialize_unit<V: Visitor<'a>>(self, visitor: V) -> Result<V::Value, CodecError> {
        if self.is_null() {
            visitor.visit_unit()
        } else {
            self.any_non_null(visitor)
        }
    }

    fn deserialize_unit_struct<V: Visitor<'a>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, CodecError> {
        self.deserialize_unit(visitor)
    }

    fn deserialize_ignored_any<V: Visitor<'a>>(self, visitor: V) -> Result<V::Value, CodecError> {
        visitor.visit_unit()
    }

    fn deserialize_i8<V: Visitor<'a>>(self, visitor: V) -> Result<V::Value, CodecError> {
        self.integer(visitor)
    }

    fn deserialize_i16<V: Visitor<'a>>(self, visitor: V) -> Result<V::Value, CodecError> {
        self.integer(visitor)
    }

    fn deserialize_i32<V: Visitor<'a>>(self, visitor: V) -> Result<V::Value, CodecError> {
        self.integer(visitor)
    }

    fn deserialize_i64<V: Visitor<'a>>(self, visitor: V) -> Result<V::Value, CodecError> {
        self.integer(visitor)
    }

    fn deserialize_i128<V: Visitor<'a>>(self, visitor: V) -> Result<V::Value, CodecError> {
        self.integer128(visitor)
    }

    fn deserialize_u8<V: Visitor<'a>>(self, visitor: V) -> Result<V::Value, CodecError> {
        self.integer(visitor)
    }

    fn deserialize_u16<V: Visitor<'a>>(self, visitor: V) -> Result<V::Value, CodecError> {
        self.integer(visitor)
    }

    fn deserialize_u32<V: Visitor<'a>>(self, visitor: V) -> Result<V::Value, CodecError> {
        self.integer(visitor)
    }

    fn deserialize_u64<V: Visitor<'a>>(self, visitor: V) -> Result<V::Value, CodecError> {
        self.integer(visitor)
    }

    fn deserialize_u128<V: Visitor<'a>>(self, visitor: V) -> Result<V::Value, CodecError> {
        self.integer128(visitor)
    }

    fn deserialize_f64<V: Visitor<'a>>(self, visitor: V) -> Result<V::Value, CodecError> {
        self.non_null()?;
        match &self.column.values {
            ColumnValues::Numeric { .. } | ColumnValues::BigNumeric { .. } => {
                let mut text = String::with_capacity(48);
                self.text(&mut text)?;
                let float = text.parse().map_err(|_| {
                    CodecError::new(
                        BigQueryCodecErrorKind::OutOfRange,
                        format!("decimal `{text}` is not an f64"),
                    )
                })?;
                visitor.visit_f64(float)
            }
            ColumnValues::Int64(_) => Err(CodecError::new(
                BigQueryCodecErrorKind::TypeMismatch,
                "an INT64 column cannot be read into a float; read it into an integer type",
            )),
            _ => self.any_non_null(visitor),
        }
    }

    fn deserialize_f32<V: Visitor<'a>>(self, visitor: V) -> Result<V::Value, CodecError> {
        self.deserialize_f64(visitor)
    }

    fn deserialize_seq<V: Visitor<'a>>(self, visitor: V) -> Result<V::Value, CodecError> {
        self.non_null()?;
        match &self.column.values {
            ColumnValues::Bytes(values) => visitor.visit_seq(ByteSeq {
                bytes: values.value(self.row),
                next_byte: 0,
            }),
            ColumnValues::Interval(values) => visitor.visit_seq(IntervalSeq {
                value: values[self.row],
                next_field: 0,
            }),
            _ => self.any_non_null(visitor),
        }
    }

    fn deserialize_tuple<V: Visitor<'a>>(
        self,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value, CodecError> {
        self.deserialize_seq(visitor)
    }

    fn deserialize_tuple_struct<V: Visitor<'a>>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value, CodecError> {
        self.deserialize_seq(visitor)
    }

    fn deserialize_struct<V: Visitor<'a>>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, CodecError> {
        self.non_null()?;
        match &self.column.values {
            ColumnValues::Struct(node) => node.visit_struct(self.row, fields, visitor),
            ColumnValues::Interval(values) => visitor.visit_seq(IntervalSeq {
                value: values[self.row],
                next_field: 0,
            }),
            _ => self.any_non_null(visitor),
        }
    }

    /// A temporal wrapper gets the column's integer, after its name is checked against the
    /// column type, since the integer's unit depends on it. Any other newtype sees the value.
    fn deserialize_newtype_struct<V: Visitor<'a>>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, CodecError> {
        let Some(wanted) = temporal_tag_kind(name) else {
            return visitor.visit_newtype_struct(self);
        };
        self.non_null()?;
        let row = self.row;
        let raw = match (wanted, &self.column.values) {
            (FieldKind::Date, ColumnValues::Date(values)) => i64::from(values[row]),
            (FieldKind::Time, ColumnValues::Time(values))
            | (FieldKind::DateTime, ColumnValues::DateTime(values))
            | (FieldKind::Timestamp, ColumnValues::Timestamp(values)) => values[row],
            _ => {
                return Err(CodecError::new(
                    BigQueryCodecErrorKind::TypeMismatch,
                    format!(
                        "{name} reads a {} column, and this column is not one",
                        wanted.name()
                    ),
                ))
            }
        };
        visitor.visit_i64(raw)
    }

    fn deserialize_enum<V: Visitor<'a>>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, CodecError> {
        self.non_null()?;
        match &self.column.values {
            ColumnValues::String(values) => {
                visitor.visit_enum(BorrowedStrDeserializer::new(values.value(self.row)))
            }
            ColumnValues::Json { text, .. } => parse_json(text.value(self.row), |deserializer| {
                de::Deserializer::deserialize_enum(deserializer, name, variants, visitor)
            }),
            _ => self.any_non_null(visitor),
        }
    }

    fn deserialize_str<V: Visitor<'a>>(self, visitor: V) -> Result<V::Value, CodecError> {
        self.string(visitor)
    }

    fn deserialize_string<V: Visitor<'a>>(self, visitor: V) -> Result<V::Value, CodecError> {
        self.string(visitor)
    }

    fn deserialize_bool<V: Visitor<'a>>(self, visitor: V) -> Result<V::Value, CodecError> {
        self.non_null()?;
        self.any_non_null(visitor)
    }

    fn deserialize_char<V: Visitor<'a>>(self, visitor: V) -> Result<V::Value, CodecError> {
        self.non_null()?;
        self.any_non_null(visitor)
    }

    fn deserialize_bytes<V: Visitor<'a>>(self, visitor: V) -> Result<V::Value, CodecError> {
        self.non_null()?;
        self.any_non_null(visitor)
    }

    fn deserialize_byte_buf<V: Visitor<'a>>(self, visitor: V) -> Result<V::Value, CodecError> {
        self.non_null()?;
        self.any_non_null(visitor)
    }

    fn deserialize_map<V: Visitor<'a>>(self, visitor: V) -> Result<V::Value, CodecError> {
        self.non_null()?;
        self.any_non_null(visitor)
    }

    fn deserialize_identifier<V: Visitor<'a>>(self, visitor: V) -> Result<V::Value, CodecError> {
        self.non_null()?;
        self.any_non_null(visitor)
    }
}

/// One row of the batch.
struct RowDeserializer<'n, 'a> {
    node: &'n StructColumns<'a>,
    row: usize,
}

impl<'a> de::Deserializer<'a> for RowDeserializer<'_, 'a> {
    type Error = CodecError;

    fn deserialize_any<V: Visitor<'a>>(self, visitor: V) -> Result<V::Value, CodecError> {
        visitor.visit_map(EveryColumnMap {
            node: self.node,
            row: self.row,
            next_column: 0,
        })
    }

    fn deserialize_struct<V: Visitor<'a>>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, CodecError> {
        self.node.visit_struct(self.row, fields, visitor)
    }

    fn deserialize_seq<V: Visitor<'a>>(self, visitor: V) -> Result<V::Value, CodecError> {
        visitor.visit_seq(EveryColumnSeq {
            node: self.node,
            row: self.row,
            next_column: 0,
        })
    }

    fn deserialize_tuple<V: Visitor<'a>>(
        self,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value, CodecError> {
        self.deserialize_seq(visitor)
    }

    fn deserialize_tuple_struct<V: Visitor<'a>>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value, CodecError> {
        self.deserialize_seq(visitor)
    }

    fn deserialize_newtype_struct<V: Visitor<'a>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, CodecError> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_option<V: Visitor<'a>>(self, visitor: V) -> Result<V::Value, CodecError> {
        visitor.visit_some(self)
    }

    serde::forward_to_deserialize_any! {
        <W: Visitor<'a>>
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string bytes byte_buf unit
        unit_struct map enum identifier ignored_any
    }
}

/// A decoder over one `RecordBatch`. It is not `Send`: decode a batch on the thread that holds
/// it, and do not hold the decoder across an `.await`.
pub(crate) struct BatchDecoder<'a> {
    root: StructColumns<'a>,
    rows: usize,
    state: Rc<BatchState>,
}

impl<'a> BatchDecoder<'a> {
    pub(crate) fn new(batch: &'a RecordBatch) -> Self {
        let state = Rc::new(BatchState {
            plans_built: Cell::new(0),
            redo: Cell::new(false),
            json_nones: RefCell::new(Vec::new()),
        });
        let fields = batch
            .schema_ref()
            .fields()
            .iter()
            .map(AsRef::as_ref)
            .collect();
        let root = StructColumns::new(fields, batch.columns().iter().collect(), state.clone());
        BatchDecoder {
            root,
            rows: batch.num_rows(),
            state,
        }
    }

    pub(crate) fn num_rows(&self) -> usize {
        self.rows
    }

    /// Decodes the row at `index`. An error belongs to that row only.
    pub(crate) fn row<T: Deserialize<'a>>(&self, index: usize) -> Result<T, CodecError> {
        if index >= self.rows {
            return Err(CodecError::new(
                BigQueryCodecErrorKind::Custom,
                format!("row {index} of a batch with {} rows", self.rows),
            ));
        }
        self.state.json_nones.borrow_mut().clear();
        loop {
            let result = T::deserialize(RowDeserializer {
                node: &self.root,
                row: index,
            });
            // Every pass that asks for another adds a name-key plan or a `None` value, and
            // neither is undone within the row, so the loop ends.
            if !self.state.redo.replace(false) {
                return result;
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn plans_built(&self) -> usize {
        self.state.plans_built.get()
    }

    /// How many struct plans of this batch use name keys.
    #[cfg(test)]
    pub(crate) fn name_key_plans(&self) -> usize {
        self.root.name_key_plans()
    }
}

/// The rows of one Arrow `RecordBatch` decoded into `T`, one item per row, in order.
///
/// It decodes the batches that
/// [`record_batches`](crate::BigQuerySelectBuilder::record_batches) streams with the same type
/// mapping as [`obj`](crate::BigQuerySelectBuilder::obj). A row that fails is one
/// `Err(BigQueryError::DeserializeError)` whose `row` is the row's index in the batch, and the
/// rows after it still decode.
///
/// It is not `Send`: decode a batch on the thread that holds it, and do not hold the iterator
/// across an `.await`.
///
/// ```
/// use bigquery::arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
/// use bigquery::{BigQueryBatchRows, BigQueryResult};
/// use std::sync::Arc;
///
/// #[derive(serde::Deserialize, Debug, PartialEq)]
/// struct Person {
///     id: i64,
///     name: String,
/// }
///
/// let batch = RecordBatch::try_from_iter([
///     ("id", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
///     ("name", Arc::new(StringArray::from(vec!["Ada", "Grace"])) as ArrayRef),
/// ])?;
/// let people = BigQueryBatchRows::<Person>::new(&batch).collect::<BigQueryResult<Vec<_>>>()?;
/// assert_eq!(people[1], Person { id: 2, name: "Grace".into() });
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct BigQueryBatchRows<'a, T> {
    decoder: BatchDecoder<'a>,
    next_row: usize,
    first_row: u64,
    _target: PhantomData<fn() -> T>,
}

impl<'a, T: DeserializeOwned> BigQueryBatchRows<'a, T> {
    /// The rows of `batch`.
    pub fn new(batch: &'a RecordBatch) -> Self {
        Self::numbered_from(batch, 0)
    }

    /// The rows of `batch`, whose errors number its first row `first_row`.
    pub(crate) fn numbered_from(batch: &'a RecordBatch, first_row: u64) -> Self {
        BigQueryBatchRows {
            decoder: BatchDecoder::new(batch),
            next_row: 0,
            first_row,
            _target: PhantomData,
        }
    }
}

impl<T: DeserializeOwned> Iterator for BigQueryBatchRows<'_, T> {
    type Item = BigQueryResult<T>;

    fn next(&mut self) -> Option<Self::Item> {
        let index = self.next_row;
        if index >= self.decoder.num_rows() {
            return None;
        }
        self.next_row += 1;
        Some(self.decoder.row(index).map_err(|error| {
            error
                .with_row(self.first_row + index as u64)
                .into_deserialize()
        }))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let left = self.decoder.num_rows() - self.next_row;
        (left, Some(left))
    }
}

impl<T: DeserializeOwned> ExactSizeIterator for BigQueryBatchRows<'_, T> {}

/// Decodes every row of `batch`. A row that fails is one `Err(DeserializeError)` carrying its
/// row number, `first_row` plus its index in the batch, and the other rows are unaffected.
pub(crate) fn decode_rows<T: DeserializeOwned>(
    batch: &RecordBatch,
    first_row: u64,
) -> Vec<BigQueryResult<T>> {
    BigQueryBatchRows::numbered_from(batch, first_row).collect()
}

#[cfg(test)]
mod tests;
