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
    let mut de = serde_json::Deserializer::from_str(text);
    read(&mut de)
        .and_then(|value| de.end().map(|()| value))
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

    pub(crate) fn name(&self, c: usize) -> &'a str {
        self.fields[c].name()
    }

    pub(crate) fn column(&self, c: usize) -> Result<&Column<'a>, CodecError> {
        self.columns[c]
            .get_or_init(|| Column::new(self.fields[c], self.arrays[c], &self.state))
            .as_ref()
            .map_err(Clone::clone)
    }

    pub(crate) fn request_redo(&self) {
        self.state.redo.set(true);
    }

    fn plan(&self, fields: &'static [&'static str]) -> Rc<Plan> {
        let key = (fields.as_ptr() as usize, fields.len());
        if let Some((_, _, plan)) = self.plans.borrow().iter().find(|(a, l, _)| (*a, *l) == key) {
            return plan.clone();
        }
        let names: Vec<&str> = self.fields.iter().map(|f| f.name().as_str()).collect();
        let plan = Rc::new(Plan::resolve(fields, &names));
        self.plans.borrow_mut().push((key.0, key.1, plan.clone()));
        self.state.plans_built.set(self.state.plans_built.get() + 1);
        plan
    }

    fn visit_struct<V: Visitor<'a>>(
        &self,
        row: usize,
        fields: &'static [&'static str],
        v: V,
    ) -> Result<V::Value, CodecError> {
        let plan = self.plan(fields);
        v.visit_map(FieldMap::new(self, &plan, fields, row))
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
            .filter_map(|c| c.get())
            .filter_map(|c| c.as_ref().ok())
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
    i: usize,
}

impl<'a> MapAccess<'a> for EveryColumnMap<'_, 'a> {
    type Error = CodecError;

    fn next_key_seed<K: DeserializeSeed<'a>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, CodecError> {
        if self.i >= self.node.fields.len() {
            return Ok(None);
        }
        self.i += 1;
        seed.deserialize(BorrowedStrDeserializer::new(self.node.name(self.i - 1)))
            .map(Some)
    }

    fn next_value_seed<S: DeserializeSeed<'a>>(&mut self, seed: S) -> Result<S::Value, CodecError> {
        let c = self.i - 1;
        self.node
            .column(c)
            .and_then(|column| seed.deserialize(ValueDeserializer::new(column, self.row)))
            .map_err(|e| e.at_field(self.node.name(c)))
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.node.fields.len() - self.i)
    }
}

/// Every column by position, for tuple rows.
struct EveryColumnSeq<'n, 'a> {
    node: &'n StructColumns<'a>,
    row: usize,
    i: usize,
}

impl<'a> SeqAccess<'a> for EveryColumnSeq<'_, 'a> {
    type Error = CodecError;

    fn next_element_seed<S: DeserializeSeed<'a>>(
        &mut self,
        seed: S,
    ) -> Result<Option<S::Value>, CodecError> {
        if self.i >= self.node.fields.len() {
            return Ok(None);
        }
        let c = self.i;
        self.i += 1;
        self.node
            .column(c)
            .and_then(|column| seed.deserialize(ValueDeserializer::new(column, self.row)))
            .map(Some)
            .map_err(|e| e.at_field(self.node.name(c)))
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.node.fields.len() - self.i)
    }
}

struct ListSeq<'c, 'a> {
    items: &'c Column<'a>,
    start: usize,
    i: usize,
    end: usize,
}

impl<'a> SeqAccess<'a> for ListSeq<'_, 'a> {
    type Error = CodecError;

    fn next_element_seed<S: DeserializeSeed<'a>>(
        &mut self,
        seed: S,
    ) -> Result<Option<S::Value>, CodecError> {
        if self.i >= self.end {
            return Ok(None);
        }
        let i = self.i;
        self.i += 1;
        seed.deserialize(ValueDeserializer::new(self.items, i))
            .map(Some)
            .map_err(|e| e.at_index(i - self.start))
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.end - self.i)
    }
}

struct ByteSeq<'a> {
    bytes: &'a [u8],
    i: usize,
}

impl<'a> SeqAccess<'a> for ByteSeq<'a> {
    type Error = CodecError;

    fn next_element_seed<S: DeserializeSeed<'a>>(
        &mut self,
        seed: S,
    ) -> Result<Option<S::Value>, CodecError> {
        let Some(&byte) = self.bytes.get(self.i) else {
            return Ok(None);
        };
        self.i += 1;
        seed.deserialize(U8Deserializer::new(byte)).map(Some)
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.bytes.len() - self.i)
    }
}

/// INTERVAL as the sequence `(months, days, nanos)` that [`BigQueryInterval`]'s derived impl
/// reads.
struct IntervalSeq {
    value: IntervalMonthDayNano,
    i: u8,
}

impl<'a> SeqAccess<'a> for IntervalSeq {
    type Error = CodecError;

    fn next_element_seed<S: DeserializeSeed<'a>>(
        &mut self,
        seed: S,
    ) -> Result<Option<S::Value>, CodecError> {
        self.i += 1;
        match self.i {
            1 => seed.deserialize(I32Deserializer::new(self.value.months)),
            2 => seed.deserialize(I32Deserializer::new(self.value.days)),
            3 => seed.deserialize(I64Deserializer::new(self.value.nanoseconds)),
            _ => return Ok(None),
        }
        .map(Some)
    }
}

impl From<IntervalMonthDayNano> for BigQueryInterval {
    fn from(v: IntervalMonthDayNano) -> Self {
        BigQueryInterval {
            months: v.months,
            days: v.days,
            nanos: v.nanoseconds,
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
        self.column.nulls.is_some_and(|n| n.is_null(self.row))
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
    /// string. `false` for values that are not of that sort.
    fn text(&self, out: &mut String) -> bool {
        let r = self.row;
        match &self.column.values {
            ColumnValues::Date(v) => civil::fmt_date(v[r], out),
            ColumnValues::Time(v) => civil::fmt_time(v[r], out),
            ColumnValues::DateTime(v) => civil::fmt_datetime(v[r], out),
            ColumnValues::Timestamp(v) => civil::fmt_timestamp(v[r], out),
            ColumnValues::Numeric { unscaled: v, scale } => {
                decimal::fmt_decimal_i128(v[r], *scale, out)
            }
            ColumnValues::BigNumeric { unscaled: v, scale } => {
                decimal::fmt_decimal_i256(v[r], *scale, out)
            }
            ColumnValues::Interval(v) => BigQueryInterval::from(v[r]).write_bq(out),
            _ => return false,
        }
        true
    }

    /// The integer of a temporal value: days for DATE, microseconds otherwise.
    fn temporal_integer(&self) -> Option<i64> {
        let r = self.row;
        match &self.column.values {
            ColumnValues::Date(v) => Some(i64::from(v[r])),
            ColumnValues::Time(v) | ColumnValues::DateTime(v) | ColumnValues::Timestamp(v) => {
                Some(v[r])
            }
            _ => None,
        }
    }

    /// The whole number a NUMERIC or BIGNUMERIC value holds; `OutOfRange` when it has a
    /// fractional part or is beyond `i128`.
    fn whole_decimal(&self) -> Option<Result<i128, CodecError>> {
        let r = self.row;
        let (value, scale) = match &self.column.values {
            ColumnValues::Numeric { unscaled: v, scale } => (i256::from_i128(v[r]), *scale),
            ColumnValues::BigNumeric { unscaled: v, scale } => (v[r], *scale),
            _ => return None,
        };
        let text = || {
            let mut out = String::new();
            decimal::fmt_decimal_i256(value, scale, &mut out);
            out
        };
        let divisor = i256::from_i128(10).wrapping_pow(scale);
        if value.wrapping_rem(divisor) != i256::ZERO {
            return Some(Err(CodecError::new(
                BigQueryCodecErrorKind::OutOfRange,
                format!(
                    "{} has a fractional part and cannot be an integer; read it into a String, \
                     an f64 or a BigQueryDecimal",
                    text()
                ),
            )));
        }
        Some(value.wrapping_div(divisor).to_i128().ok_or_else(|| {
            CodecError::new(
                BigQueryCodecErrorKind::OutOfRange,
                format!("{} does not fit in i128", text()),
            )
        }))
    }

    /// An integer target of 64 bits or fewer; serde's visitors check the narrower ranges.
    fn integer<V: Visitor<'a>>(self, v: V) -> Result<V::Value, CodecError> {
        self.non_null()?;
        if let Some(x) = self.temporal_integer() {
            return v.visit_i64(x);
        }
        if let Some(whole) = self.whole_decimal() {
            let whole = whole?;
            return if let Ok(x) = i64::try_from(whole) {
                v.visit_i64(x)
            } else if let Ok(x) = u64::try_from(whole) {
                v.visit_u64(x)
            } else {
                Err(CodecError::new(
                    BigQueryCodecErrorKind::OutOfRange,
                    format!("{whole} does not fit in 64 bits; read it into an i128"),
                ))
            };
        }
        self.any_non_null(v)
    }

    fn integer128<V: Visitor<'a>>(self, v: V) -> Result<V::Value, CodecError> {
        self.non_null()?;
        match self.whole_decimal() {
            Some(whole) => v.visit_i128(whole?),
            None => self.integer(v),
        }
    }

    fn any_non_null<V: Visitor<'a>>(self, v: V) -> Result<V::Value, CodecError> {
        let r = self.row;
        match &self.column.values {
            ColumnValues::Int64(a) => v.visit_i64(a[r]),
            ColumnValues::Float64(a) => v.visit_f64(a[r]),
            ColumnValues::Bool(a) => v.visit_bool(a.value(r)),
            ColumnValues::String(a) => v.visit_borrowed_str(a.value(r)),
            ColumnValues::Json { text: a, .. } => {
                parse_json(a.value(r), |de| de::Deserializer::deserialize_any(de, v))
            }
            ColumnValues::Bytes(a) => v.visit_borrowed_bytes(a.value(r)),
            ColumnValues::List { offsets, items } => {
                let (start, end) = (offsets[r] as usize, offsets[r + 1] as usize);
                v.visit_seq(ListSeq {
                    items,
                    start,
                    i: start,
                    end,
                })
            }
            ColumnValues::Struct(node) => v.visit_map(EveryColumnMap { node, row: r, i: 0 }),
            _ => {
                let mut s = String::with_capacity(48);
                self.text(&mut s);
                v.visit_str(&s)
            }
        }
    }

    /// A string target. A jiff `Timestamp` cannot hold the last 25 hours of BigQuery's range,
    /// and its parse error would read as a bad text; it is reported as the range error it is.
    fn string<V: Visitor<'a>>(self, v: V) -> Result<V::Value, CodecError> {
        self.non_null()?;
        if let ColumnValues::Json { text: a, .. } = &self.column.values {
            return v.visit_borrowed_str(a.value(self.row));
        }
        if let ColumnValues::Timestamp(a) = &self.column.values {
            let micros = a[self.row];
            let mut s = String::with_capacity(32);
            civil::fmt_timestamp(micros, &mut s);
            return v.visit_str(&s).map_err(|err| {
                if micros > jiff_max_micros() {
                    CodecError::new(
                        BigQueryCodecErrorKind::OutOfRange,
                        format!(
                            "TIMESTAMP {s} is above jiff::Timestamp's maximum; read it into a \
                             String or an i64 ({err})"
                        ),
                    )
                } else {
                    err
                }
            });
        }
        self.any_non_null(v)
    }
}

impl<'a> de::Deserializer<'a> for ValueDeserializer<'_, 'a> {
    type Error = CodecError;

    fn deserialize_any<V: Visitor<'a>>(self, v: V) -> Result<V::Value, CodecError> {
        if self.is_null() {
            return v.visit_unit();
        }
        self.any_non_null(v)
    }

    /// SQL NULL is `None`. JSON `null` is offered to the target as `Some` first, so that a
    /// target that holds `null`, such as `serde_json::Value`, keeps it apart from SQL NULL; a
    /// target that cannot read it fails, and the row is decoded again with that value as `None`.
    fn deserialize_option<V: Visitor<'a>>(self, v: V) -> Result<V::Value, CodecError> {
        if self.is_null() {
            return v.visit_none();
        }
        let column = self.column;
        if let ColumnValues::Json { text: a, state } = &column.values {
            if a.value(self.row).trim_ascii() == "null" {
                let position = (std::ptr::from_ref(column) as usize, self.row);
                if state.json_nones.borrow().contains(&position) {
                    return v.visit_none();
                }
                let some = v.visit_some(self);
                if some.is_err() {
                    state.json_nones.borrow_mut().push(position);
                    state.redo.set(true);
                }
                return some;
            }
        }
        v.visit_some(self)
    }

    fn deserialize_unit<V: Visitor<'a>>(self, v: V) -> Result<V::Value, CodecError> {
        if self.is_null() {
            v.visit_unit()
        } else {
            self.any_non_null(v)
        }
    }

    fn deserialize_unit_struct<V: Visitor<'a>>(
        self,
        _name: &'static str,
        v: V,
    ) -> Result<V::Value, CodecError> {
        self.deserialize_unit(v)
    }

    fn deserialize_ignored_any<V: Visitor<'a>>(self, v: V) -> Result<V::Value, CodecError> {
        v.visit_unit()
    }

    fn deserialize_i8<V: Visitor<'a>>(self, v: V) -> Result<V::Value, CodecError> {
        self.integer(v)
    }

    fn deserialize_i16<V: Visitor<'a>>(self, v: V) -> Result<V::Value, CodecError> {
        self.integer(v)
    }

    fn deserialize_i32<V: Visitor<'a>>(self, v: V) -> Result<V::Value, CodecError> {
        self.integer(v)
    }

    fn deserialize_i64<V: Visitor<'a>>(self, v: V) -> Result<V::Value, CodecError> {
        self.integer(v)
    }

    fn deserialize_i128<V: Visitor<'a>>(self, v: V) -> Result<V::Value, CodecError> {
        self.integer128(v)
    }

    fn deserialize_u8<V: Visitor<'a>>(self, v: V) -> Result<V::Value, CodecError> {
        self.integer(v)
    }

    fn deserialize_u16<V: Visitor<'a>>(self, v: V) -> Result<V::Value, CodecError> {
        self.integer(v)
    }

    fn deserialize_u32<V: Visitor<'a>>(self, v: V) -> Result<V::Value, CodecError> {
        self.integer(v)
    }

    fn deserialize_u64<V: Visitor<'a>>(self, v: V) -> Result<V::Value, CodecError> {
        self.integer(v)
    }

    fn deserialize_u128<V: Visitor<'a>>(self, v: V) -> Result<V::Value, CodecError> {
        self.integer128(v)
    }

    fn deserialize_f64<V: Visitor<'a>>(self, v: V) -> Result<V::Value, CodecError> {
        self.non_null()?;
        match &self.column.values {
            ColumnValues::Numeric { .. } | ColumnValues::BigNumeric { .. } => {
                let mut s = String::with_capacity(48);
                self.text(&mut s);
                let x = s.parse().map_err(|_| {
                    CodecError::new(
                        BigQueryCodecErrorKind::OutOfRange,
                        format!("decimal `{s}` is not an f64"),
                    )
                })?;
                v.visit_f64(x)
            }
            ColumnValues::Int64(_) => Err(CodecError::new(
                BigQueryCodecErrorKind::TypeMismatch,
                "an INT64 column cannot be read into a float; read it into an integer type",
            )),
            _ => self.any_non_null(v),
        }
    }

    fn deserialize_f32<V: Visitor<'a>>(self, v: V) -> Result<V::Value, CodecError> {
        self.deserialize_f64(v)
    }

    fn deserialize_seq<V: Visitor<'a>>(self, v: V) -> Result<V::Value, CodecError> {
        self.non_null()?;
        match &self.column.values {
            ColumnValues::Bytes(a) => v.visit_seq(ByteSeq {
                bytes: a.value(self.row),
                i: 0,
            }),
            ColumnValues::Interval(a) => v.visit_seq(IntervalSeq {
                value: a[self.row],
                i: 0,
            }),
            _ => self.any_non_null(v),
        }
    }

    fn deserialize_tuple<V: Visitor<'a>>(self, _len: usize, v: V) -> Result<V::Value, CodecError> {
        self.deserialize_seq(v)
    }

    fn deserialize_tuple_struct<V: Visitor<'a>>(
        self,
        _name: &'static str,
        _len: usize,
        v: V,
    ) -> Result<V::Value, CodecError> {
        self.deserialize_seq(v)
    }

    fn deserialize_struct<V: Visitor<'a>>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        v: V,
    ) -> Result<V::Value, CodecError> {
        self.non_null()?;
        match &self.column.values {
            ColumnValues::Struct(node) => node.visit_struct(self.row, fields, v),
            ColumnValues::Interval(a) => v.visit_seq(IntervalSeq {
                value: a[self.row],
                i: 0,
            }),
            _ => self.any_non_null(v),
        }
    }

    /// A temporal wrapper gets the column's integer, after its name is checked against the
    /// column type, since the integer's unit depends on it. Any other newtype sees the value.
    fn deserialize_newtype_struct<V: Visitor<'a>>(
        self,
        name: &'static str,
        v: V,
    ) -> Result<V::Value, CodecError> {
        let Some(wanted) = temporal_tag_kind(name) else {
            return v.visit_newtype_struct(self);
        };
        self.non_null()?;
        let r = self.row;
        let raw = match (wanted, &self.column.values) {
            (FieldKind::Date, ColumnValues::Date(a)) => i64::from(a[r]),
            (FieldKind::Time, ColumnValues::Time(a))
            | (FieldKind::DateTime, ColumnValues::DateTime(a))
            | (FieldKind::Timestamp, ColumnValues::Timestamp(a)) => a[r],
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
        v.visit_i64(raw)
    }

    fn deserialize_enum<V: Visitor<'a>>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        v: V,
    ) -> Result<V::Value, CodecError> {
        self.non_null()?;
        match &self.column.values {
            ColumnValues::String(a) => {
                v.visit_enum(BorrowedStrDeserializer::new(a.value(self.row)))
            }
            ColumnValues::Json { text: a, .. } => parse_json(a.value(self.row), |de| {
                de::Deserializer::deserialize_enum(de, name, variants, v)
            }),
            _ => self.any_non_null(v),
        }
    }

    fn deserialize_str<V: Visitor<'a>>(self, v: V) -> Result<V::Value, CodecError> {
        self.string(v)
    }

    fn deserialize_string<V: Visitor<'a>>(self, v: V) -> Result<V::Value, CodecError> {
        self.string(v)
    }

    fn deserialize_bool<V: Visitor<'a>>(self, v: V) -> Result<V::Value, CodecError> {
        self.non_null()?;
        self.any_non_null(v)
    }

    fn deserialize_char<V: Visitor<'a>>(self, v: V) -> Result<V::Value, CodecError> {
        self.non_null()?;
        self.any_non_null(v)
    }

    fn deserialize_bytes<V: Visitor<'a>>(self, v: V) -> Result<V::Value, CodecError> {
        self.non_null()?;
        self.any_non_null(v)
    }

    fn deserialize_byte_buf<V: Visitor<'a>>(self, v: V) -> Result<V::Value, CodecError> {
        self.non_null()?;
        self.any_non_null(v)
    }

    fn deserialize_map<V: Visitor<'a>>(self, v: V) -> Result<V::Value, CodecError> {
        self.non_null()?;
        self.any_non_null(v)
    }

    fn deserialize_identifier<V: Visitor<'a>>(self, v: V) -> Result<V::Value, CodecError> {
        self.non_null()?;
        self.any_non_null(v)
    }
}

/// One row of the batch.
struct RowDeserializer<'n, 'a> {
    node: &'n StructColumns<'a>,
    row: usize,
}

impl<'a> de::Deserializer<'a> for RowDeserializer<'_, 'a> {
    type Error = CodecError;

    fn deserialize_any<V: Visitor<'a>>(self, v: V) -> Result<V::Value, CodecError> {
        v.visit_map(EveryColumnMap {
            node: self.node,
            row: self.row,
            i: 0,
        })
    }

    fn deserialize_struct<V: Visitor<'a>>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        v: V,
    ) -> Result<V::Value, CodecError> {
        self.node.visit_struct(self.row, fields, v)
    }

    fn deserialize_seq<V: Visitor<'a>>(self, v: V) -> Result<V::Value, CodecError> {
        v.visit_seq(EveryColumnSeq {
            node: self.node,
            row: self.row,
            i: 0,
        })
    }

    fn deserialize_tuple<V: Visitor<'a>>(self, _len: usize, v: V) -> Result<V::Value, CodecError> {
        self.deserialize_seq(v)
    }

    fn deserialize_tuple_struct<V: Visitor<'a>>(
        self,
        _name: &'static str,
        _len: usize,
        v: V,
    ) -> Result<V::Value, CodecError> {
        self.deserialize_seq(v)
    }

    fn deserialize_newtype_struct<V: Visitor<'a>>(
        self,
        _name: &'static str,
        v: V,
    ) -> Result<V::Value, CodecError> {
        v.visit_newtype_struct(self)
    }

    fn deserialize_option<V: Visitor<'a>>(self, v: V) -> Result<V::Value, CodecError> {
        v.visit_some(self)
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

    /// Decodes row `i`. An error belongs to that row only.
    pub(crate) fn row<T: Deserialize<'a>>(&self, i: usize) -> Result<T, CodecError> {
        if i >= self.rows {
            return Err(CodecError::new(
                BigQueryCodecErrorKind::Custom,
                format!("row {i} of a batch with {} rows", self.rows),
            ));
        }
        self.state.json_nones.borrow_mut().clear();
        loop {
            let result = T::deserialize(RowDeserializer {
                node: &self.root,
                row: i,
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

/// Calls `f` with every row of `batch` decoded, in order. `first_row` is the row number of the
/// batch's first row, which errors carry.
pub(crate) fn decode_each<T, F>(batch: &RecordBatch, first_row: u64, mut f: F)
where
    T: DeserializeOwned,
    F: FnMut(BigQueryResult<T>),
{
    let decoder = BatchDecoder::new(batch);
    for i in 0..decoder.num_rows() {
        f(decoder
            .row(i)
            .map_err(|e| e.with_row(first_row + i as u64).into_deserialize()));
    }
}

/// Decodes every row of `batch`. A row that fails is one `Err(DeserializeError)` carrying its
/// row number, `first_row` plus its index in the batch, and the other rows are unaffected.
pub(crate) fn decode_rows<T: DeserializeOwned>(
    batch: &RecordBatch,
    first_row: u64,
) -> Vec<BigQueryResult<T>> {
    let mut rows = Vec::with_capacity(batch.num_rows());
    decode_each(batch, first_row, |row| rows.push(row));
    rows
}

#[cfg(test)]
mod tests;
