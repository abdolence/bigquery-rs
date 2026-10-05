//! serde to Storage Write protobuf rows, against a [`WritePlan`].
//!
//! Each row is written straight into the output buffer. A nested message or packed run gets a
//! one-byte length placeholder that is patched once its content is written, and shifted right
//! in the rare case the length needs more bytes.
//!
//! `serialize_field` keys are `&'static str` arriving in declaration order, so each message
//! caches, by position, the key's address and the field it resolved to: a row of the same type
//! hits the cache on every field and never hashes a name.
//!
//! The encoder never reports `is_human_readable() == false` for an ordinary value, since types
//! such as `uuid` change their serde form on it. Only the crate's temporal wrappers, recognised
//! by their serde names, are serialized through the non-human-readable integer capture.

use crate::errors::BigQueryCodecErrorKind;
use crate::types::civil::{
    self, DATE_MAX_DAYS, DATE_MIN_DAYS, MICROS_PER_DAY, TIMESTAMP_MAX_MICROS, TIMESTAMP_MIN_MICROS,
};
use crate::types::decimal::{self, BIGNUMERIC_SCALE, NUMERIC_SCALE, TAG_DECIMAL};
use crate::types::error::CodecError;
use crate::types::interval::{BigQueryInterval, TAG_INTERVAL};
use crate::types::json::TAG_JSON;
use crate::types::kind::FieldKind;
use crate::types::temporal::{capture_integer, temporal_tag_kind};
use crate::write::descriptor::{FieldPlan, MessagePlan, WritePlan};
use crate::{BigQueryChangeSequenceNumber, BigQueryChangeType};
use arrow_buffer::i256;
use gcloud_sdk::prost::encoding::{encode_varint, encoded_len_varint};
use serde::ser::{self, Impossible, Serialize};
use std::fmt::{self, Write as _};
use std::sync::Arc;

impl FieldPlan {
    /// The error for a Rust form, described by `what`, that this field's column cannot take.
    fn mismatch(&self, what: &str) -> CodecError {
        CodecError::type_mismatch(format!(
            "{what} cannot be written to a {} column",
            self.kind.name()
        ))
    }
}

const NOT_A_ROW: &str = "a row must serialize as a struct or a map";
const NOT_A_SEQUENCE: &str = "a REPEATED field takes a sequence";
const NOT_A_KEY: &str = "a map key must be a string";

/// A JSON column's value that `serde_json` could not build.
impl From<serde_json::Error> for CodecError {
    fn from(err: serde_json::Error) -> Self {
        CodecError::new(
            BigQueryCodecErrorKind::Custom,
            format!("JSON column: {err}"),
        )
    }
}

/// Per-message cache of (key address, key length, field index) by position.
type KeyCache = Vec<(usize, usize, u32)>;

/// Encodes rows against one plan. It holds the key caches and a scratch buffer, so a writer
/// keeps one per plan and reuses it across rows.
pub(crate) struct Encoder {
    plan: Arc<WritePlan>,
    caches: Vec<KeyCache>,
    scratch: String,
}

impl Encoder {
    pub(crate) fn new(plan: Arc<WritePlan>) -> Self {
        let caches = vec![Vec::new(); plan.messages];
        Encoder {
            plan,
            caches,
            scratch: String::new(),
        }
    }

    pub(crate) fn plan(&self) -> &Arc<WritePlan> {
        &self.plan
    }

    /// Appends the encoded message for `row` to `out`. On an error `out` is left as it was.
    pub(crate) fn encode<T: Serialize + ?Sized>(
        &mut self,
        row: &T,
        out: &mut Vec<u8>,
    ) -> Result<(), CodecError> {
        let start = out.len();
        let result = self.encode_row(row, out);
        if result.is_err() {
            out.truncate(start);
        }
        result
    }

    /// Appends `row` followed by its CDC pseudo-columns. Protobuf fields may come in any
    /// order, so the two columns follow the row's own.
    ///
    /// The plan must have been compiled with `cdc`.
    pub(crate) fn encode_change<T: Serialize + ?Sized>(
        &mut self,
        row: &T,
        change_type: BigQueryChangeType,
        sequence_number: Option<&BigQueryChangeSequenceNumber>,
        out: &mut Vec<u8>,
    ) -> Result<(), CodecError> {
        let Some(cdc) = self.plan.cdc else {
            return Err(CodecError::new(
                BigQueryCodecErrorKind::Custom,
                "the write plan has no CDC pseudo-columns",
            ));
        };
        let start = out.len();
        if let Err(err) = self.encode_row(row, out) {
            out.truncate(start);
            return Err(err);
        }
        let mut output = RowOutput {
            out,
            caches: &mut self.caches,
            scratch: &mut self.scratch,
        };
        let change_type = match change_type {
            BigQueryChangeType::Upsert => "UPSERT",
            BigQueryChangeType::Delete => "DELETE",
        };
        output.put(cdc.change_type.get());
        output.length_delimited(change_type.as_bytes());
        if let Some(sequence_number) = sequence_number {
            output.put(cdc.sequence_number.get());
            output.length_delimited(sequence_number.as_str().as_bytes());
        }
        Ok(())
    }

    fn encode_row<T: Serialize + ?Sized>(
        &mut self,
        row: &T,
        out: &mut Vec<u8>,
    ) -> Result<(), CodecError> {
        let mut output = RowOutput {
            out,
            caches: &mut self.caches,
            scratch: &mut self.scratch,
        };
        row.serialize(RowSerializer {
            output: &mut output,
            message: &self.plan.root,
        })
    }
}

/// The output buffer and the encoder's reusable state, borrowed for one row.
struct RowOutput<'e> {
    out: &'e mut Vec<u8>,
    caches: &'e mut [KeyCache],
    scratch: &'e mut String,
}

impl RowOutput<'_> {
    #[inline]
    fn put(&mut self, b: &[u8]) {
        self.out.extend_from_slice(b);
    }

    #[inline]
    fn byte(&mut self, b: u8) {
        self.out.push(b);
    }

    #[inline]
    fn varint(&mut self, v: u64) {
        encode_varint(v, self.out);
    }

    #[inline]
    fn length_delimited(&mut self, b: &[u8]) {
        self.varint(b.len() as u64);
        self.put(b);
    }

    /// Opens a length-delimited payload whose length is not known yet.
    #[inline]
    fn begin(&mut self) -> usize {
        self.out.push(0);
        self.out.len()
    }

    /// Closes the payload opened at `mark`, writing its length into the placeholder.
    #[inline]
    fn end(&mut self, mark: usize) {
        let len = self.out.len() - mark;
        if len < 0x80 {
            self.out[mark - 1] = len as u8;
            return;
        }
        let n = encoded_len_varint(len as u64);
        let extra = n - 1;
        self.out.resize(self.out.len() + extra, 0);
        self.out.copy_within(mark..mark + len, mark + extra);
        encode_varint(len as u64, &mut &mut self.out[mark - 1..mark - 1 + n]);
    }
}

macro_rules! reject {
    ($err:expr; $($m:ident($($t:ty),*)),* $(,)?) => {
        $(fn $m(self, $(_: $t),*) -> Result<Self::Ok, CodecError> { Err($err) })*
    };
}

/// The top-level row: a struct or a map.
struct RowSerializer<'r, 'e, 'p> {
    output: &'r mut RowOutput<'e>,
    message: &'p MessagePlan,
}

impl<'r, 'e, 'p> ser::Serializer for RowSerializer<'r, 'e, 'p> {
    type Ok = ();
    type Error = CodecError;
    type SerializeSeq = Impossible<(), CodecError>;
    type SerializeTuple = Impossible<(), CodecError>;
    type SerializeTupleStruct = Impossible<(), CodecError>;
    type SerializeTupleVariant = Impossible<(), CodecError>;
    type SerializeMap = MessageSerializer<'r, 'e, 'p>;
    type SerializeStruct = MessageSerializer<'r, 'e, 'p>;
    type SerializeStructVariant = Impossible<(), CodecError>;

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, CodecError> {
        Ok(MessageSerializer::new(self.output, self.message, None))
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, CodecError> {
        Ok(MessageSerializer::new(self.output, self.message, None))
    }

    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        v: &T,
    ) -> Result<(), CodecError> {
        v.serialize(self)
    }

    fn serialize_some<T: Serialize + ?Sized>(self, v: &T) -> Result<(), CodecError> {
        v.serialize(self)
    }

    reject!(CodecError::type_mismatch(NOT_A_ROW);
        serialize_bool(bool), serialize_i8(i8), serialize_i16(i16), serialize_i32(i32),
        serialize_i64(i64), serialize_u8(u8), serialize_u16(u16), serialize_u32(u32),
        serialize_u64(u64), serialize_f32(f32), serialize_f64(f64), serialize_char(char),
        serialize_str(&str), serialize_bytes(&[u8]), serialize_none(), serialize_unit(),
        serialize_unit_struct(&'static str),
        serialize_unit_variant(&'static str, u32, &'static str),
    );

    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: &T,
    ) -> Result<(), CodecError> {
        Err(CodecError::type_mismatch(NOT_A_ROW))
    }

    fn serialize_seq(self, _: Option<usize>) -> Result<Self::SerializeSeq, CodecError> {
        Err(CodecError::type_mismatch(NOT_A_ROW))
    }

    fn serialize_tuple(self, _: usize) -> Result<Self::SerializeTuple, CodecError> {
        Err(CodecError::type_mismatch(NOT_A_ROW))
    }

    fn serialize_tuple_struct(
        self,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleStruct, CodecError> {
        Err(CodecError::type_mismatch(NOT_A_ROW))
    }

    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleVariant, CodecError> {
        Err(CodecError::type_mismatch(NOT_A_ROW))
    }

    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStructVariant, CodecError> {
        Err(CodecError::type_mismatch(NOT_A_ROW))
    }
}

impl MessagePlan {
    fn field_index(&self, key: &str) -> Result<usize, CodecError> {
        self.index_of(key).ok_or_else(|| {
            CodecError::new(
                BigQueryCodecErrorKind::UnknownField,
                "no column of this name in the table schema",
            )
            .at_field(key)
        })
    }
}

/// One message: the row, a STRUCT or a RANGE.
struct MessageSerializer<'r, 'e, 'p> {
    output: &'r mut RowOutput<'e>,
    message: &'p MessagePlan,
    /// The payload mark of a nested message; `None` for the row itself.
    mark: Option<usize>,
    cursor: usize,
    seen: u64,
    map_key: Option<usize>,
}

impl<'r, 'e, 'p> MessageSerializer<'r, 'e, 'p> {
    fn new(output: &'r mut RowOutput<'e>, message: &'p MessagePlan, mark: Option<usize>) -> Self {
        MessageSerializer {
            output,
            message,
            mark,
            cursor: 0,
            seen: 0,
            map_key: None,
        }
    }

    #[inline]
    fn lookup(&mut self, key: &'static str) -> Result<usize, CodecError> {
        let k = self.cursor;
        self.cursor += 1;
        let ptr = key.as_ptr() as usize;
        let cache = &mut self.output.caches[self.message.id];
        if let Some(&(p, l, idx)) = cache.get(k) {
            if p == ptr && l == key.len() {
                return Ok(idx as usize);
            }
        }
        let idx = self.message.field_index(key)?;
        let entry = (ptr, key.len(), idx as u32);
        match k.cmp(&cache.len()) {
            std::cmp::Ordering::Less => cache[k] = entry,
            std::cmp::Ordering::Equal => cache.push(entry),
            std::cmp::Ordering::Greater => {}
        }
        Ok(idx)
    }

    fn field<T: Serialize + ?Sized>(&mut self, idx: usize, v: &T) -> Result<(), CodecError> {
        let field = &self.message.fields[idx];
        if idx < 64 {
            self.seen |= 1 << idx;
        }
        let result = if field.repeated {
            v.serialize(RepeatedSerializer {
                output: self.output,
                field,
            })
        } else {
            ValueSerializer {
                output: self.output,
                field,
                packed: false,
                in_array: false,
            }
            .value(v)
        };
        result.map_err(|e| e.at_field(&self.message.names[idx]))
    }

    fn finish(self) -> Result<(), CodecError> {
        let missing = self.message.required & !self.seen;
        if missing != 0 {
            let names: Vec<&str> = (0..64)
                .filter(|i| missing & (1 << i) != 0)
                .map(|i| self.message.names[i].as_str())
                .collect();
            return Err(CodecError::new(
                BigQueryCodecErrorKind::MissingRequiredField,
                format!("REQUIRED field(s) not written: {}", names.join(", ")),
            )
            .at_field(names[0]));
        }
        if let Some(mark) = self.mark {
            self.output.end(mark);
        }
        Ok(())
    }
}

impl ser::SerializeStruct for MessageSerializer<'_, '_, '_> {
    type Ok = ();
    type Error = CodecError;

    #[inline]
    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        v: &T,
    ) -> Result<(), CodecError> {
        let idx = self.lookup(key)?;
        self.field(idx, v)
    }

    fn end(self) -> Result<(), CodecError> {
        self.finish()
    }
}

impl ser::SerializeMap for MessageSerializer<'_, '_, '_> {
    type Ok = ();
    type Error = CodecError;

    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<(), CodecError> {
        let key = key.serialize(KeyCapture)?;
        self.map_key = Some(self.message.field_index(&key)?);
        Ok(())
    }

    fn serialize_value<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<(), CodecError> {
        let idx = self.map_key.take().ok_or_else(|| {
            CodecError::new(BigQueryCodecErrorKind::Custom, "a map value without a key")
        })?;
        self.field(idx, v)
    }

    fn end(self) -> Result<(), CodecError> {
        self.finish()
    }
}

/// A REPEATED field, which takes a sequence.
struct RepeatedSerializer<'r, 'e, 'p> {
    output: &'r mut RowOutput<'e>,
    field: &'p FieldPlan,
}

impl<'r, 'e, 'p> RepeatedSerializer<'r, 'e, 'p> {
    fn sequence(self, len: Option<usize>) -> RepeatedSequence<'r, 'e, 'p> {
        let mark = if self.field.kind.packable() && len != Some(0) {
            self.output.put(self.field.packed_key.get());
            Some(self.output.begin())
        } else {
            None
        };
        RepeatedSequence {
            output: self.output,
            field: self.field,
            mark,
            i: 0,
        }
    }
}

impl<'r, 'e, 'p> ser::Serializer for RepeatedSerializer<'r, 'e, 'p> {
    type Ok = ();
    type Error = CodecError;
    type SerializeSeq = RepeatedSequence<'r, 'e, 'p>;
    type SerializeTuple = RepeatedSequence<'r, 'e, 'p>;
    type SerializeTupleStruct = Impossible<(), CodecError>;
    type SerializeTupleVariant = Impossible<(), CodecError>;
    type SerializeMap = Impossible<(), CodecError>;
    type SerializeStruct = Impossible<(), CodecError>;
    type SerializeStructVariant = Impossible<(), CodecError>;

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, CodecError> {
        Ok(self.sequence(len))
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, CodecError> {
        Ok(self.sequence(Some(len)))
    }

    /// BigQuery stores a NULL array as an empty one.
    fn serialize_none(self) -> Result<(), CodecError> {
        Ok(())
    }

    fn serialize_unit(self) -> Result<(), CodecError> {
        Ok(())
    }

    fn serialize_some<T: Serialize + ?Sized>(self, v: &T) -> Result<(), CodecError> {
        v.serialize(self)
    }

    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        v: &T,
    ) -> Result<(), CodecError> {
        v.serialize(self)
    }

    reject!(CodecError::type_mismatch(NOT_A_SEQUENCE);
        serialize_bool(bool), serialize_i8(i8), serialize_i16(i16), serialize_i32(i32),
        serialize_i64(i64), serialize_u8(u8), serialize_u16(u16), serialize_u32(u32),
        serialize_u64(u64), serialize_f32(f32), serialize_f64(f64), serialize_char(char),
        serialize_str(&str), serialize_bytes(&[u8]), serialize_unit_struct(&'static str),
        serialize_unit_variant(&'static str, u32, &'static str),
    );

    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: &T,
    ) -> Result<(), CodecError> {
        Err(CodecError::type_mismatch(NOT_A_SEQUENCE))
    }

    fn serialize_tuple_struct(
        self,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleStruct, CodecError> {
        Err(CodecError::type_mismatch(NOT_A_SEQUENCE))
    }

    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleVariant, CodecError> {
        Err(CodecError::type_mismatch(NOT_A_SEQUENCE))
    }

    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap, CodecError> {
        Err(CodecError::type_mismatch(NOT_A_SEQUENCE))
    }

    fn serialize_struct(
        self,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStruct, CodecError> {
        Err(CodecError::type_mismatch(NOT_A_SEQUENCE))
    }

    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStructVariant, CodecError> {
        Err(CodecError::type_mismatch(NOT_A_SEQUENCE))
    }
}

struct RepeatedSequence<'r, 'e, 'p> {
    output: &'r mut RowOutput<'e>,
    field: &'p FieldPlan,
    /// Set when the elements form one packed run.
    mark: Option<usize>,
    i: usize,
}

impl RepeatedSequence<'_, '_, '_> {
    fn element<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<(), CodecError> {
        let i = self.i;
        self.i += 1;
        ValueSerializer {
            output: self.output,
            field: self.field,
            packed: self.mark.is_some(),
            in_array: true,
        }
        .value(v)
        .map_err(|e| e.at_index(i))
    }

    fn done(self) -> Result<(), CodecError> {
        if let Some(mark) = self.mark {
            self.output.end(mark);
        }
        Ok(())
    }
}

impl ser::SerializeSeq for RepeatedSequence<'_, '_, '_> {
    type Ok = ();
    type Error = CodecError;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<(), CodecError> {
        self.element(v)
    }

    fn end(self) -> Result<(), CodecError> {
        self.done()
    }
}

impl ser::SerializeTuple for RepeatedSequence<'_, '_, '_> {
    type Ok = ();
    type Error = CodecError;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<(), CodecError> {
        self.element(v)
    }

    fn end(self) -> Result<(), CodecError> {
        self.done()
    }
}

impl FieldKind {
    /// Whether the integer form of a kind exists: INT64, the temporal kinds and the decimals.
    fn takes_integers(self) -> bool {
        matches!(
            self,
            FieldKind::Int64
                | FieldKind::Date
                | FieldKind::Time
                | FieldKind::DateTime
                | FieldKind::Timestamp
                | FieldKind::Numeric
                | FieldKind::BigNumeric
        )
    }

    /// jiff prints a year below zero with a sign and six digits, which no BigQuery text form has;
    /// it is a date before BigQuery's year 1, not malformed text.
    fn check_signed_year(self, s: &str) -> Result<(), CodecError> {
        if s.starts_with('-') {
            return Err(CodecError::out_of_range(format!(
                "{} `{s}` is before BigQuery's 0001-01-01",
                self.name()
            )));
        }
        Ok(())
    }
}

fn numeric_in_range(v: i256) -> Result<i256, CodecError> {
    let limit = i256::from_i128(10i128.pow(38));
    if v >= limit || v <= limit.wrapping_neg() {
        let mut text = String::new();
        decimal::fmt_decimal_i256(v, NUMERIC_SCALE, &mut text);
        return Err(CodecError::out_of_range(format!(
            "NUMERIC {text} has more than 29 integer digits"
        )));
    }
    Ok(v)
}

/// One value of a field: the whole field, an array element, or one entry of a packed run
/// (`packed`, written without its key).
struct ValueSerializer<'r, 'e, 'p> {
    output: &'r mut RowOutput<'e>,
    field: &'p FieldPlan,
    packed: bool,
    in_array: bool,
}

impl<'r, 'e, 'p> ValueSerializer<'r, 'e, 'p> {
    /// Writes `v`, which a JSON column takes in any shape.
    #[inline]
    fn value<T: Serialize + ?Sized>(self, v: &T) -> Result<(), CodecError> {
        if self.field.kind == FieldKind::Json {
            v.serialize(JsonSerializer(self))
        } else {
            v.serialize(self)
        }
    }

    #[inline]
    fn key(&mut self) {
        if !self.packed {
            self.output.put(self.field.key.get());
        }
    }

    #[inline]
    fn length_delimited(mut self, b: &[u8]) -> Result<(), CodecError> {
        self.key();
        self.output.length_delimited(b);
        Ok(())
    }

    fn decimal(self, v: i256) -> Result<(), CodecError> {
        let (buf, n) = decimal::decimal_le_bytes(v);
        self.length_delimited(&buf[..n])
    }

    #[inline]
    fn varint_field(mut self, v: u64) -> Result<(), CodecError> {
        self.key();
        self.output.varint(v);
        Ok(())
    }

    /// An integer in the column's integer form: days, microseconds of the day, civil or epoch
    /// microseconds, or a whole number for INT64 and the decimals.
    fn integer(self, v: i64) -> Result<(), CodecError> {
        match self.field.kind {
            FieldKind::Int64 => self.varint_field(v as u64),
            FieldKind::Date => {
                let days = i32::try_from(v)
                    .ok()
                    .filter(|d| (DATE_MIN_DAYS..=DATE_MAX_DAYS).contains(d))
                    .ok_or_else(|| {
                        CodecError::out_of_range(format!(
                            "DATE of {v} days is outside BigQuery's range"
                        ))
                    })?;
                self.varint_field(i64::from(days) as u64)
            }
            FieldKind::Time => {
                if !(0..MICROS_PER_DAY).contains(&v) {
                    return Err(CodecError::out_of_range(format!(
                        "{v} microseconds is not a time of day"
                    )));
                }
                self.varint_field(civil::pack_time(v) as u64)
            }
            FieldKind::DateTime => {
                if !(TIMESTAMP_MIN_MICROS..=TIMESTAMP_MAX_MICROS).contains(&v) {
                    return Err(CodecError::out_of_range(format!(
                        "DATETIME of {v} civil microseconds is outside BigQuery's range"
                    )));
                }
                self.varint_field(civil::pack_datetime(v) as u64)
            }
            FieldKind::Timestamp => {
                if !(TIMESTAMP_MIN_MICROS..=TIMESTAMP_MAX_MICROS).contains(&v) {
                    return Err(CodecError::out_of_range(format!(
                        "TIMESTAMP of {v} microseconds is outside BigQuery's range"
                    )));
                }
                self.varint_field(v as u64)
            }
            FieldKind::Numeric => {
                self.decimal(i256::from_i128(i128::from(v) * 10i128.pow(NUMERIC_SCALE)))
            }
            FieldKind::BigNumeric => {
                // 10^38 does not fit i128, so it is built as 10^19 squared; any i64 times it
                // stays well inside i256.
                let half = i256::from_i128(10i128.pow(BIGNUMERIC_SCALE / 2));
                self.decimal(i256::from_i128(i128::from(v)).wrapping_mul(half.wrapping_mul(half)))
            }
            _ => Err(self.field.mismatch("an integer")),
        }
    }

    fn wide_integer(self, v: i128) -> Result<(), CodecError> {
        if !self.field.kind.takes_integers() {
            return Err(self.field.mismatch("an integer"));
        }
        let v = i64::try_from(v).map_err(|_| {
            CodecError::out_of_range(format!(
                "{v} does not fit the INT64 range a {} column takes",
                self.field.kind.name()
            ))
        })?;
        self.integer(v)
    }

    fn text(self, s: &str) -> Result<(), CodecError> {
        let kind = self.field.kind;
        match kind {
            FieldKind::String | FieldKind::Geography | FieldKind::Json => {
                self.length_delimited(s.as_bytes())
            }
            FieldKind::Date => {
                kind.check_signed_year(s)?;
                let days = civil::parse_date(s)?;
                self.integer(i64::from(days))
            }
            FieldKind::Time => {
                let micros = civil::parse_time(s)?;
                self.integer(micros)
            }
            FieldKind::DateTime => {
                kind.check_signed_year(s)?;
                let micros = civil::parse_datetime(s)?;
                self.integer(micros)
            }
            FieldKind::Timestamp => {
                kind.check_signed_year(s)?;
                let micros = civil::parse_timestamp(s)?;
                self.integer(micros)
            }
            FieldKind::Numeric => {
                let v = decimal::parse_numeric(s)?;
                self.decimal(v)
            }
            FieldKind::BigNumeric => {
                let v = decimal::parse_bignumeric(s)?;
                self.decimal(v)
            }
            FieldKind::Interval => {
                let interval = BigQueryInterval::parse_bq(s)?;
                self.interval(interval)
            }
            _ => Err(self.field.mismatch("a string")),
        }
    }

    fn interval(self, interval: BigQueryInterval) -> Result<(), CodecError> {
        if interval.nanos % 1000 != 0 {
            return Err(CodecError::out_of_range(format!(
                "INTERVAL keeps microseconds; {} nanoseconds is not a whole number of them",
                interval.nanos
            )));
        }
        let mut text = std::mem::take(self.output.scratch);
        text.clear();
        interval.write_bq(&mut text);
        let ValueSerializer {
            output,
            field,
            packed,
            in_array,
        } = self;
        let result = ValueSerializer {
            output: &mut *output,
            field,
            packed,
            in_array,
        }
        .length_delimited(text.as_bytes());
        *output.scratch = text;
        result
    }

    fn null(self) -> Result<(), CodecError> {
        if self.in_array {
            Err(CodecError::new(
                BigQueryCodecErrorKind::NullArrayElement,
                "an ARRAY element cannot be NULL",
            ))
        } else if self.field.required {
            Err(CodecError::new(
                BigQueryCodecErrorKind::NullForRequired,
                "NULL for a REQUIRED field",
            ))
        } else {
            Ok(())
        }
    }

    fn message(mut self) -> Result<MessageSerializer<'r, 'e, 'p>, CodecError> {
        let sub = self
            .field
            .nested
            .as_deref()
            .ok_or_else(|| self.field.mismatch("a struct or a map"))?;
        self.key();
        let mark = self.output.begin();
        Ok(MessageSerializer::new(self.output, sub, Some(mark)))
    }
}

impl<'r, 'e, 'p> ser::Serializer for ValueSerializer<'r, 'e, 'p> {
    type Ok = ();
    type Error = CodecError;
    type SerializeSeq = Compound<'r, 'e, 'p>;
    type SerializeTuple = Compound<'r, 'e, 'p>;
    type SerializeTupleStruct = Compound<'r, 'e, 'p>;
    type SerializeTupleVariant = Impossible<(), CodecError>;
    type SerializeMap = MessageSerializer<'r, 'e, 'p>;
    type SerializeStruct = Compound<'r, 'e, 'p>;
    type SerializeStructVariant = Impossible<(), CodecError>;

    fn serialize_bool(self, v: bool) -> Result<(), CodecError> {
        if self.field.kind != FieldKind::Bool {
            return Err(self.field.mismatch("a bool"));
        }
        self.varint_field(u64::from(v))
    }

    fn serialize_i8(self, v: i8) -> Result<(), CodecError> {
        self.serialize_i64(i64::from(v))
    }

    fn serialize_i16(self, v: i16) -> Result<(), CodecError> {
        self.serialize_i64(i64::from(v))
    }

    fn serialize_i32(self, v: i32) -> Result<(), CodecError> {
        self.serialize_i64(i64::from(v))
    }

    #[inline]
    fn serialize_i64(self, v: i64) -> Result<(), CodecError> {
        self.integer(v)
    }

    fn serialize_i128(self, v: i128) -> Result<(), CodecError> {
        self.wide_integer(v)
    }

    fn serialize_u8(self, v: u8) -> Result<(), CodecError> {
        self.serialize_i64(i64::from(v))
    }

    fn serialize_u16(self, v: u16) -> Result<(), CodecError> {
        self.serialize_i64(i64::from(v))
    }

    fn serialize_u32(self, v: u32) -> Result<(), CodecError> {
        self.serialize_i64(i64::from(v))
    }

    fn serialize_u64(self, v: u64) -> Result<(), CodecError> {
        self.wide_integer(i128::from(v))
    }

    fn serialize_u128(self, v: u128) -> Result<(), CodecError> {
        match i128::try_from(v) {
            Ok(v) => self.wide_integer(v),
            Err(_) => self.wide_integer(i128::MAX),
        }
    }

    fn serialize_f32(self, v: f32) -> Result<(), CodecError> {
        self.serialize_f64(f64::from(v))
    }

    #[inline]
    fn serialize_f64(mut self, v: f64) -> Result<(), CodecError> {
        match self.field.kind {
            FieldKind::Float64 => {
                self.key();
                self.output.put(&v.to_le_bytes());
                Ok(())
            }
            FieldKind::Numeric => {
                let d = numeric_in_range(decimal::decimal_from_f64(v, NUMERIC_SCALE)?)?;
                self.decimal(d)
            }
            FieldKind::BigNumeric => {
                let d = decimal::decimal_from_f64(v, BIGNUMERIC_SCALE)?;
                self.decimal(d)
            }
            _ => Err(self.field.mismatch("a float")),
        }
    }

    fn serialize_char(self, v: char) -> Result<(), CodecError> {
        self.text(v.encode_utf8(&mut [0u8; 4]))
    }

    #[inline]
    fn serialize_str(self, v: &str) -> Result<(), CodecError> {
        self.text(v)
    }

    fn collect_str<T: fmt::Display + ?Sized>(self, v: &T) -> Result<(), CodecError> {
        let mut text = std::mem::take(self.output.scratch);
        text.clear();
        let _ = write!(text, "{v}");
        let ValueSerializer {
            output,
            field,
            packed,
            in_array,
        } = self;
        let result = ValueSerializer {
            output: &mut *output,
            field,
            packed,
            in_array,
        }
        .text(&text);
        *output.scratch = text;
        result
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<(), CodecError> {
        match self.field.kind {
            FieldKind::Bytes => self.length_delimited(v),
            _ => Err(self.field.mismatch("bytes")),
        }
    }

    fn serialize_none(self) -> Result<(), CodecError> {
        self.null()
    }

    fn serialize_some<T: Serialize + ?Sized>(self, v: &T) -> Result<(), CodecError> {
        v.serialize(self)
    }

    fn serialize_unit(self) -> Result<(), CodecError> {
        self.null()
    }

    fn serialize_unit_struct(self, _: &'static str) -> Result<(), CodecError> {
        self.null()
    }

    fn serialize_unit_variant(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
    ) -> Result<(), CodecError> {
        self.text(variant)
    }

    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        name: &'static str,
        v: &T,
    ) -> Result<(), CodecError> {
        if let Some(kind) = temporal_tag_kind(name) {
            // The integer has a unit, so it only means something on its own column type.
            if kind != self.field.kind {
                return Err(self.field.mismatch(name));
            }
            let raw = capture_integer(v)?;
            return self.integer(raw);
        }
        let tagged_kind_matches = match name {
            TAG_DECIMAL => matches!(self.field.kind, FieldKind::Numeric | FieldKind::BigNumeric),
            TAG_JSON => self.field.kind == FieldKind::Json,
            _ => true,
        };
        if !tagged_kind_matches {
            return Err(self.field.mismatch(name));
        }
        v.serialize(self)
    }

    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: &T,
    ) -> Result<(), CodecError> {
        Err(self.field.mismatch("an enum variant with data"))
    }

    fn serialize_seq(mut self, _len: Option<usize>) -> Result<Self::SerializeSeq, CodecError> {
        match self.field.kind {
            FieldKind::Bytes => {
                self.key();
                let mark = self.output.begin();
                Ok(Compound::Bytes {
                    output: self.output,
                    field: self.field,
                    mark,
                })
            }
            _ if self.in_array => Err(CodecError::new(
                BigQueryCodecErrorKind::UnsupportedType,
                "an ARRAY of ARRAY is not a BigQuery column type",
            )),
            _ => Err(self.field.mismatch("a sequence")),
        }
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, CodecError> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_struct(
        self,
        _: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, CodecError> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleVariant, CodecError> {
        Err(self.field.mismatch("an enum variant with data"))
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, CodecError> {
        self.message()
    }

    fn serialize_struct(
        self,
        name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, CodecError> {
        if self.field.kind == FieldKind::Interval {
            if name != TAG_INTERVAL {
                return Err(self.field.mismatch(name));
            }
            return Ok(Compound::Interval {
                value: self,
                parts: [None; 3],
            });
        }
        Ok(Compound::Message(self.message()?))
    }

    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStructVariant, CodecError> {
        Err(self.field.mismatch("an enum variant with data"))
    }
}

// `serde_json`'s `Value` serializer, whose compound states own their content, so a JSON
// column's value can be built inside the row's serializer.
use serde_json::value::Serializer as JsonValueSer;
type JsonValueSeq = <JsonValueSer as ser::Serializer>::SerializeSeq;
type JsonValueTupleVariant = <JsonValueSer as ser::Serializer>::SerializeTupleVariant;
type JsonValueMap = <JsonValueSer as ser::Serializer>::SerializeMap;
type JsonValueStructVariant = <JsonValueSer as ser::Serializer>::SerializeStructVariant;

/// One value of a JSON column. A string is the JSON text as it is, and `None` is SQL NULL;
/// any other shape, `()` and `serde_json::Value::Null` included, is printed as JSON text.
/// Inside a printed value, strings are JSON strings.
struct JsonSerializer<'r, 'e, 'p>(ValueSerializer<'r, 'e, 'p>);

impl JsonSerializer<'_, '_, '_> {
    fn print(self, value: Result<serde_json::Value, serde_json::Error>) -> Result<(), CodecError> {
        let text = value.map_err(CodecError::from)?.to_string();
        self.0.text(&text)
    }
}

impl<'r, 'e, 'p> ser::Serializer for JsonSerializer<'r, 'e, 'p> {
    type Ok = ();
    type Error = CodecError;
    type SerializeSeq = JsonCompound<'r, 'e, 'p, JsonValueSeq>;
    type SerializeTuple = JsonCompound<'r, 'e, 'p, JsonValueSeq>;
    type SerializeTupleStruct = JsonCompound<'r, 'e, 'p, JsonValueSeq>;
    type SerializeTupleVariant = JsonCompound<'r, 'e, 'p, JsonValueTupleVariant>;
    type SerializeMap = JsonCompound<'r, 'e, 'p, JsonValueMap>;
    type SerializeStruct = JsonCompound<'r, 'e, 'p, JsonValueMap>;
    type SerializeStructVariant = JsonCompound<'r, 'e, 'p, JsonValueStructVariant>;

    fn serialize_bool(self, v: bool) -> Result<(), CodecError> {
        self.print(JsonValueSer.serialize_bool(v))
    }

    fn serialize_i8(self, v: i8) -> Result<(), CodecError> {
        self.print(JsonValueSer.serialize_i8(v))
    }

    fn serialize_i16(self, v: i16) -> Result<(), CodecError> {
        self.print(JsonValueSer.serialize_i16(v))
    }

    fn serialize_i32(self, v: i32) -> Result<(), CodecError> {
        self.print(JsonValueSer.serialize_i32(v))
    }

    fn serialize_i64(self, v: i64) -> Result<(), CodecError> {
        self.print(JsonValueSer.serialize_i64(v))
    }

    fn serialize_i128(self, v: i128) -> Result<(), CodecError> {
        self.print(JsonValueSer.serialize_i128(v))
    }

    fn serialize_u8(self, v: u8) -> Result<(), CodecError> {
        self.print(JsonValueSer.serialize_u8(v))
    }

    fn serialize_u16(self, v: u16) -> Result<(), CodecError> {
        self.print(JsonValueSer.serialize_u16(v))
    }

    fn serialize_u32(self, v: u32) -> Result<(), CodecError> {
        self.print(JsonValueSer.serialize_u32(v))
    }

    fn serialize_u64(self, v: u64) -> Result<(), CodecError> {
        self.print(JsonValueSer.serialize_u64(v))
    }

    fn serialize_u128(self, v: u128) -> Result<(), CodecError> {
        self.print(JsonValueSer.serialize_u128(v))
    }

    fn serialize_f32(self, v: f32) -> Result<(), CodecError> {
        self.print(JsonValueSer.serialize_f32(v))
    }

    fn serialize_f64(self, v: f64) -> Result<(), CodecError> {
        self.print(JsonValueSer.serialize_f64(v))
    }

    fn serialize_char(self, v: char) -> Result<(), CodecError> {
        self.0.serialize_char(v)
    }

    fn serialize_str(self, v: &str) -> Result<(), CodecError> {
        self.0.text(v)
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<(), CodecError> {
        self.print(JsonValueSer.serialize_bytes(v))
    }

    fn serialize_none(self) -> Result<(), CodecError> {
        self.0.null()
    }

    fn serialize_some<T: Serialize + ?Sized>(self, v: &T) -> Result<(), CodecError> {
        v.serialize(self)
    }

    fn serialize_unit(self) -> Result<(), CodecError> {
        self.print(JsonValueSer.serialize_unit())
    }

    fn serialize_unit_struct(self, name: &'static str) -> Result<(), CodecError> {
        self.print(JsonValueSer.serialize_unit_struct(name))
    }

    fn serialize_unit_variant(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
    ) -> Result<(), CodecError> {
        self.print(JsonValueSer.serialize_unit_variant(name, index, variant))
    }

    /// The crate's wrappers keep their own rules, so `BigQueryJson` stays the JSON text it
    /// printed; any other newtype is transparent, as in `serde_json`.
    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        name: &'static str,
        v: &T,
    ) -> Result<(), CodecError> {
        if matches!(name, TAG_JSON | TAG_DECIMAL) || temporal_tag_kind(name).is_some() {
            self.0.serialize_newtype_struct(name, v)
        } else {
            v.serialize(self)
        }
    }

    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
        v: &T,
    ) -> Result<(), CodecError> {
        self.print(JsonValueSer.serialize_newtype_variant(name, index, variant, v))
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, CodecError> {
        JsonCompound::new(self, JsonValueSer.serialize_seq(len))
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, CodecError> {
        JsonCompound::new(self, JsonValueSer.serialize_tuple(len))
    }

    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, CodecError> {
        JsonCompound::new(self, JsonValueSer.serialize_tuple_struct(name, len))
    }

    fn serialize_tuple_variant(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, CodecError> {
        JsonCompound::new(
            self,
            JsonValueSer.serialize_tuple_variant(name, index, variant, len),
        )
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, CodecError> {
        JsonCompound::new(self, JsonValueSer.serialize_map(len))
    }

    fn serialize_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, CodecError> {
        JsonCompound::new(self, JsonValueSer.serialize_struct(name, len))
    }

    fn serialize_struct_variant(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, CodecError> {
        JsonCompound::new(
            self,
            JsonValueSer.serialize_struct_variant(name, index, variant, len),
        )
    }
}

/// A compound value of a JSON column, built as a [`serde_json::Value`] and printed at its end.
struct JsonCompound<'r, 'e, 'p, C> {
    target: JsonSerializer<'r, 'e, 'p>,
    inner: C,
}

impl<'r, 'e, 'p, C> JsonCompound<'r, 'e, 'p, C> {
    fn new(
        target: JsonSerializer<'r, 'e, 'p>,
        inner: Result<C, serde_json::Error>,
    ) -> Result<Self, CodecError> {
        Ok(JsonCompound {
            target,
            inner: inner.map_err(CodecError::from)?,
        })
    }
}

impl<C: ser::SerializeSeq<Ok = serde_json::Value, Error = serde_json::Error>> ser::SerializeSeq
    for JsonCompound<'_, '_, '_, C>
{
    type Ok = ();
    type Error = CodecError;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<(), CodecError> {
        self.inner.serialize_element(v).map_err(CodecError::from)
    }

    fn end(self) -> Result<(), CodecError> {
        self.target.print(self.inner.end())
    }
}

impl<C: ser::SerializeTuple<Ok = serde_json::Value, Error = serde_json::Error>> ser::SerializeTuple
    for JsonCompound<'_, '_, '_, C>
{
    type Ok = ();
    type Error = CodecError;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<(), CodecError> {
        self.inner.serialize_element(v).map_err(CodecError::from)
    }

    fn end(self) -> Result<(), CodecError> {
        self.target.print(self.inner.end())
    }
}

impl<C: ser::SerializeTupleStruct<Ok = serde_json::Value, Error = serde_json::Error>>
    ser::SerializeTupleStruct for JsonCompound<'_, '_, '_, C>
{
    type Ok = ();
    type Error = CodecError;

    fn serialize_field<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<(), CodecError> {
        self.inner.serialize_field(v).map_err(CodecError::from)
    }

    fn end(self) -> Result<(), CodecError> {
        self.target.print(self.inner.end())
    }
}

impl<C: ser::SerializeTupleVariant<Ok = serde_json::Value, Error = serde_json::Error>>
    ser::SerializeTupleVariant for JsonCompound<'_, '_, '_, C>
{
    type Ok = ();
    type Error = CodecError;

    fn serialize_field<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<(), CodecError> {
        self.inner.serialize_field(v).map_err(CodecError::from)
    }

    fn end(self) -> Result<(), CodecError> {
        self.target.print(self.inner.end())
    }
}

impl<C: ser::SerializeMap<Ok = serde_json::Value, Error = serde_json::Error>> ser::SerializeMap
    for JsonCompound<'_, '_, '_, C>
{
    type Ok = ();
    type Error = CodecError;

    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<(), CodecError> {
        self.inner.serialize_key(key).map_err(CodecError::from)
    }

    fn serialize_value<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<(), CodecError> {
        self.inner.serialize_value(v).map_err(CodecError::from)
    }

    fn end(self) -> Result<(), CodecError> {
        self.target.print(self.inner.end())
    }
}

impl<C: ser::SerializeStruct<Ok = serde_json::Value, Error = serde_json::Error>>
    ser::SerializeStruct for JsonCompound<'_, '_, '_, C>
{
    type Ok = ();
    type Error = CodecError;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        v: &T,
    ) -> Result<(), CodecError> {
        self.inner.serialize_field(key, v).map_err(CodecError::from)
    }

    fn skip_field(&mut self, key: &'static str) -> Result<(), CodecError> {
        self.inner.skip_field(key).map_err(CodecError::from)
    }

    fn end(self) -> Result<(), CodecError> {
        self.target.print(self.inner.end())
    }
}

impl<C: ser::SerializeStructVariant<Ok = serde_json::Value, Error = serde_json::Error>>
    ser::SerializeStructVariant for JsonCompound<'_, '_, '_, C>
{
    type Ok = ();
    type Error = CodecError;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        v: &T,
    ) -> Result<(), CodecError> {
        self.inner.serialize_field(key, v).map_err(CodecError::from)
    }

    fn end(self) -> Result<(), CodecError> {
        self.target.print(self.inner.end())
    }
}

/// The compound values one field can take: a nested message, BYTES as a sequence of `u8`,
/// or a [`BigQueryInterval`] as its struct.
enum Compound<'r, 'e, 'p> {
    Message(MessageSerializer<'r, 'e, 'p>),
    Bytes {
        output: &'r mut RowOutput<'e>,
        field: &'p FieldPlan,
        mark: usize,
    },
    Interval {
        value: ValueSerializer<'r, 'e, 'p>,
        parts: [Option<i64>; 3],
    },
}

const INTERVAL_PARTS: [&str; 3] = ["months", "days", "nanos"];

impl Compound<'_, '_, '_> {
    fn element<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<(), CodecError> {
        match self {
            Compound::Bytes { output, field, .. } => {
                let byte = capture_integer(v)
                    .ok()
                    .and_then(|b| u8::try_from(b).ok())
                    .ok_or_else(|| field.mismatch("a sequence of anything but u8"))?;
                output.byte(byte);
                Ok(())
            }
            Compound::Interval { value, .. } => Err(value.field.mismatch("a sequence")),
            Compound::Message(_) => Err(CodecError::new(
                BigQueryCodecErrorKind::TypeMismatch,
                "a STRUCT takes named fields",
            )),
        }
    }

    fn finish(self) -> Result<(), CodecError> {
        match self {
            Compound::Message(m) => m.finish(),
            Compound::Bytes { output, mark, .. } => {
                output.end(mark);
                Ok(())
            }
            Compound::Interval { value, parts } => {
                let [Some(months), Some(days), Some(nanos)] = parts else {
                    return Err(value
                        .field
                        .mismatch("an INTERVAL without months, days and nanos"));
                };
                let part = |value: i64, name: &str| {
                    i32::try_from(value).map_err(|_| {
                        CodecError::out_of_range(format!("INTERVAL {name} {value} is out of range"))
                    })
                };
                let interval = BigQueryInterval {
                    months: part(months, "months")?,
                    days: part(days, "days")?,
                    nanos,
                };
                value.interval(interval)
            }
        }
    }
}

impl ser::SerializeSeq for Compound<'_, '_, '_> {
    type Ok = ();
    type Error = CodecError;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<(), CodecError> {
        self.element(v)
    }

    fn end(self) -> Result<(), CodecError> {
        self.finish()
    }
}

impl ser::SerializeTuple for Compound<'_, '_, '_> {
    type Ok = ();
    type Error = CodecError;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<(), CodecError> {
        self.element(v)
    }

    fn end(self) -> Result<(), CodecError> {
        self.finish()
    }
}

impl ser::SerializeTupleStruct for Compound<'_, '_, '_> {
    type Ok = ();
    type Error = CodecError;

    fn serialize_field<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<(), CodecError> {
        self.element(v)
    }

    fn end(self) -> Result<(), CodecError> {
        self.finish()
    }
}

impl ser::SerializeStruct for Compound<'_, '_, '_> {
    type Ok = ();
    type Error = CodecError;

    #[inline]
    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        v: &T,
    ) -> Result<(), CodecError> {
        match self {
            Compound::Message(m) => ser::SerializeStruct::serialize_field(m, key, v),
            Compound::Interval { parts, .. } => {
                let i = INTERVAL_PARTS
                    .iter()
                    .position(|k| *k == key)
                    .ok_or_else(|| {
                        CodecError::new(
                            BigQueryCodecErrorKind::UnknownField,
                            "an INTERVAL has months, days and nanos",
                        )
                        .at_field(key)
                    })?;
                parts[i] = Some(capture_integer(v).map_err(|e| e.at_field(key))?);
                Ok(())
            }
            Compound::Bytes { field, .. } => Err(field.mismatch("a struct")),
        }
    }

    fn end(self) -> Result<(), CodecError> {
        self.finish()
    }
}

/// Captures a map key, which must be a string.
struct KeyCapture;

impl ser::Serializer for KeyCapture {
    type Ok = String;
    type Error = CodecError;
    type SerializeSeq = Impossible<String, CodecError>;
    type SerializeTuple = Impossible<String, CodecError>;
    type SerializeTupleStruct = Impossible<String, CodecError>;
    type SerializeTupleVariant = Impossible<String, CodecError>;
    type SerializeMap = Impossible<String, CodecError>;
    type SerializeStruct = Impossible<String, CodecError>;
    type SerializeStructVariant = Impossible<String, CodecError>;

    fn serialize_str(self, v: &str) -> Result<String, CodecError> {
        Ok(v.to_string())
    }

    fn serialize_char(self, v: char) -> Result<String, CodecError> {
        Ok(v.to_string())
    }

    reject!(CodecError::type_mismatch(NOT_A_KEY);
        serialize_bool(bool), serialize_i8(i8), serialize_i16(i16), serialize_i32(i32),
        serialize_i64(i64), serialize_u8(u8), serialize_u16(u16), serialize_u32(u32),
        serialize_u64(u64), serialize_f32(f32), serialize_f64(f64), serialize_bytes(&[u8]),
        serialize_none(), serialize_unit(), serialize_unit_struct(&'static str),
        serialize_unit_variant(&'static str, u32, &'static str),
    );

    fn serialize_some<T: Serialize + ?Sized>(self, v: &T) -> Result<String, CodecError> {
        v.serialize(self)
    }

    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        v: &T,
    ) -> Result<String, CodecError> {
        v.serialize(self)
    }

    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: &T,
    ) -> Result<String, CodecError> {
        Err(CodecError::type_mismatch(NOT_A_KEY))
    }

    fn serialize_seq(self, _: Option<usize>) -> Result<Self::SerializeSeq, CodecError> {
        Err(CodecError::type_mismatch(NOT_A_KEY))
    }

    fn serialize_tuple(self, _: usize) -> Result<Self::SerializeTuple, CodecError> {
        Err(CodecError::type_mismatch(NOT_A_KEY))
    }

    fn serialize_tuple_struct(
        self,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleStruct, CodecError> {
        Err(CodecError::type_mismatch(NOT_A_KEY))
    }

    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleVariant, CodecError> {
        Err(CodecError::type_mismatch(NOT_A_KEY))
    }

    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap, CodecError> {
        Err(CodecError::type_mismatch(NOT_A_KEY))
    }

    fn serialize_struct(
        self,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStruct, CodecError> {
        Err(CodecError::type_mismatch(NOT_A_KEY))
    }

    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStructVariant, CodecError> {
        Err(CodecError::type_mismatch(NOT_A_KEY))
    }
}

#[cfg(test)]
pub(crate) mod tests;

#[cfg(test)]
mod roundtrip;
