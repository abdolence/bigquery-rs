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
use crate::types::kind::BqKind;
use crate::types::temporal::{capture_int, temporal_tag_kind};
use crate::write::descriptor::{FieldPlan, MsgPlan, WritePlan};
use crate::{BigQueryChangeSequenceNumber, BigQueryChangeType};
use arrow_buffer::i256;
use serde::ser::{self, Impossible, Serialize};
use std::fmt::{self, Write as _};
use std::sync::Arc;

/// The number of bytes `v` takes as a varint.
pub(crate) fn varint_len(v: u64) -> usize {
    let bits = u64::BITS - (v | 1).leading_zeros();
    bits.div_ceil(7) as usize
}

fn varint_bytes(mut v: u64, buf: &mut [u8; 10]) -> usize {
    let mut n = 0;
    while v >= 0x80 {
        buf[n] = (v as u8) | 0x80;
        v >>= 7;
        n += 1;
    }
    buf[n] = v as u8;
    n + 1
}

fn mismatch(field: &FieldPlan, what: &str) -> CodecError {
    CodecError::new(
        BigQueryCodecErrorKind::TypeMismatch,
        format!("{what} cannot be written to a {} column", field.kind.name()),
    )
}

fn out_of_range(message: String) -> CodecError {
    CodecError::new(BigQueryCodecErrorKind::OutOfRange, message)
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
        let mut st = St {
            out,
            caches: &mut self.caches,
            scratch: &mut self.scratch,
        };
        let change_type = match change_type {
            BigQueryChangeType::Upsert => "UPSERT",
            BigQueryChangeType::Delete => "DELETE",
        };
        st.put(cdc.change_type.get());
        st.len_delim(change_type.as_bytes());
        if let Some(sequence_number) = sequence_number {
            st.put(cdc.sequence_number.get());
            st.len_delim(sequence_number.as_str().as_bytes());
        }
        Ok(())
    }

    fn encode_row<T: Serialize + ?Sized>(
        &mut self,
        row: &T,
        out: &mut Vec<u8>,
    ) -> Result<(), CodecError> {
        let mut st = St {
            out,
            caches: &mut self.caches,
            scratch: &mut self.scratch,
        };
        row.serialize(RowSer {
            st: &mut st,
            msg: &self.plan.root,
        })
    }
}

/// The output buffer and the encoder's reusable state, borrowed for one row.
struct St<'e> {
    out: &'e mut Vec<u8>,
    caches: &'e mut [KeyCache],
    scratch: &'e mut String,
}

impl St<'_> {
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
        if v < 0x80 {
            return self.byte(v as u8);
        }
        let mut buf = [0u8; 10];
        let n = varint_bytes(v, &mut buf);
        self.put(&buf[..n]);
    }

    #[inline]
    fn len_delim(&mut self, b: &[u8]) {
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
        let mut buf = [0u8; 10];
        let n = varint_bytes(len as u64, &mut buf);
        let extra = n - 1;
        self.out.resize(self.out.len() + extra, 0);
        self.out.copy_within(mark..mark + len, mark + extra);
        self.out[mark - 1..mark - 1 + n].copy_from_slice(&buf[..n]);
    }
}

macro_rules! reject {
    ($err:expr; $($m:ident($($t:ty),*)),* $(,)?) => {
        $(fn $m(self, $(_: $t),*) -> Result<Self::Ok, CodecError> { Err($err) })*
    };
}

fn not_a_row() -> CodecError {
    CodecError::new(
        BigQueryCodecErrorKind::TypeMismatch,
        "a row must serialize as a struct or a map",
    )
}

/// The top-level row: a struct or a map.
struct RowSer<'r, 'e, 'p> {
    st: &'r mut St<'e>,
    msg: &'p MsgPlan,
}

impl<'r, 'e, 'p> ser::Serializer for RowSer<'r, 'e, 'p> {
    type Ok = ();
    type Error = CodecError;
    type SerializeSeq = Impossible<(), CodecError>;
    type SerializeTuple = Impossible<(), CodecError>;
    type SerializeTupleStruct = Impossible<(), CodecError>;
    type SerializeTupleVariant = Impossible<(), CodecError>;
    type SerializeMap = MsgSer<'r, 'e, 'p>;
    type SerializeStruct = MsgSer<'r, 'e, 'p>;
    type SerializeStructVariant = Impossible<(), CodecError>;

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, CodecError> {
        Ok(MsgSer::new(self.st, self.msg, None))
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, CodecError> {
        Ok(MsgSer::new(self.st, self.msg, None))
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

    reject!(not_a_row();
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
        Err(not_a_row())
    }

    fn serialize_seq(self, _: Option<usize>) -> Result<Self::SerializeSeq, CodecError> {
        Err(not_a_row())
    }

    fn serialize_tuple(self, _: usize) -> Result<Self::SerializeTuple, CodecError> {
        Err(not_a_row())
    }

    fn serialize_tuple_struct(
        self,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleStruct, CodecError> {
        Err(not_a_row())
    }

    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleVariant, CodecError> {
        Err(not_a_row())
    }

    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStructVariant, CodecError> {
        Err(not_a_row())
    }
}

fn by_name(msg: &MsgPlan, key: &str) -> Result<usize, CodecError> {
    msg.index_of(key).ok_or_else(|| {
        CodecError::new(
            BigQueryCodecErrorKind::UnknownField,
            "no column of this name in the table schema",
        )
        .at_field(key)
    })
}

/// One message: the row, a STRUCT or a RANGE.
struct MsgSer<'r, 'e, 'p> {
    st: &'r mut St<'e>,
    msg: &'p MsgPlan,
    /// The payload mark of a nested message; `None` for the row itself.
    mark: Option<usize>,
    cursor: usize,
    seen: u64,
    map_key: Option<usize>,
}

impl<'r, 'e, 'p> MsgSer<'r, 'e, 'p> {
    fn new(st: &'r mut St<'e>, msg: &'p MsgPlan, mark: Option<usize>) -> Self {
        MsgSer {
            st,
            msg,
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
        let cache = &mut self.st.caches[self.msg.id];
        if let Some(&(p, l, idx)) = cache.get(k) {
            if p == ptr && l == key.len() {
                return Ok(idx as usize);
            }
        }
        let idx = by_name(self.msg, key)?;
        let entry = (ptr, key.len(), idx as u32);
        match k.cmp(&cache.len()) {
            std::cmp::Ordering::Less => cache[k] = entry,
            std::cmp::Ordering::Equal => cache.push(entry),
            std::cmp::Ordering::Greater => {}
        }
        Ok(idx)
    }

    fn field<T: Serialize + ?Sized>(&mut self, idx: usize, v: &T) -> Result<(), CodecError> {
        let f = &self.msg.fields[idx];
        if idx < 64 {
            self.seen |= 1 << idx;
        }
        let result = if f.repeated {
            v.serialize(RepSer { st: self.st, f })
        } else {
            ValSer {
                st: self.st,
                f,
                packed: false,
                in_array: false,
            }
            .value(v)
        };
        result.map_err(|e| e.at_field(&self.msg.names[idx]))
    }

    fn finish(self) -> Result<(), CodecError> {
        let missing = self.msg.required & !self.seen;
        if missing != 0 {
            let names: Vec<&str> = (0..64)
                .filter(|i| missing & (1 << i) != 0)
                .map(|i| self.msg.names[i].as_str())
                .collect();
            return Err(CodecError::new(
                BigQueryCodecErrorKind::MissingRequiredField,
                format!("REQUIRED field(s) not written: {}", names.join(", ")),
            )
            .at_field(names[0]));
        }
        if let Some(mark) = self.mark {
            self.st.end(mark);
        }
        Ok(())
    }
}

impl ser::SerializeStruct for MsgSer<'_, '_, '_> {
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

impl ser::SerializeMap for MsgSer<'_, '_, '_> {
    type Ok = ();
    type Error = CodecError;

    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<(), CodecError> {
        let key = key.serialize(KeyCapture)?;
        self.map_key = Some(by_name(self.msg, &key)?);
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

fn not_a_seq() -> CodecError {
    CodecError::new(
        BigQueryCodecErrorKind::TypeMismatch,
        "a REPEATED field takes a sequence",
    )
}

/// A REPEATED field, which takes a sequence.
struct RepSer<'r, 'e, 'p> {
    st: &'r mut St<'e>,
    f: &'p FieldPlan,
}

impl<'r, 'e, 'p> RepSer<'r, 'e, 'p> {
    fn seq(self, len: Option<usize>) -> SeqSer<'r, 'e, 'p> {
        let mark = if self.f.kind.packable() && len != Some(0) {
            self.st.put(self.f.packed_key.get());
            Some(self.st.begin())
        } else {
            None
        };
        SeqSer {
            st: self.st,
            f: self.f,
            mark,
            i: 0,
        }
    }
}

impl<'r, 'e, 'p> ser::Serializer for RepSer<'r, 'e, 'p> {
    type Ok = ();
    type Error = CodecError;
    type SerializeSeq = SeqSer<'r, 'e, 'p>;
    type SerializeTuple = SeqSer<'r, 'e, 'p>;
    type SerializeTupleStruct = Impossible<(), CodecError>;
    type SerializeTupleVariant = Impossible<(), CodecError>;
    type SerializeMap = Impossible<(), CodecError>;
    type SerializeStruct = Impossible<(), CodecError>;
    type SerializeStructVariant = Impossible<(), CodecError>;

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, CodecError> {
        Ok(self.seq(len))
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, CodecError> {
        Ok(self.seq(Some(len)))
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

    reject!(not_a_seq();
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
        Err(not_a_seq())
    }

    fn serialize_tuple_struct(
        self,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleStruct, CodecError> {
        Err(not_a_seq())
    }

    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleVariant, CodecError> {
        Err(not_a_seq())
    }

    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap, CodecError> {
        Err(not_a_seq())
    }

    fn serialize_struct(
        self,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStruct, CodecError> {
        Err(not_a_seq())
    }

    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStructVariant, CodecError> {
        Err(not_a_seq())
    }
}

struct SeqSer<'r, 'e, 'p> {
    st: &'r mut St<'e>,
    f: &'p FieldPlan,
    /// Set when the elements form one packed run.
    mark: Option<usize>,
    i: usize,
}

impl SeqSer<'_, '_, '_> {
    fn element<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<(), CodecError> {
        let i = self.i;
        self.i += 1;
        ValSer {
            st: self.st,
            f: self.f,
            packed: self.mark.is_some(),
            in_array: true,
        }
        .value(v)
        .map_err(|e| e.at_index(i))
    }

    fn done(self) -> Result<(), CodecError> {
        if let Some(mark) = self.mark {
            self.st.end(mark);
        }
        Ok(())
    }
}

impl ser::SerializeSeq for SeqSer<'_, '_, '_> {
    type Ok = ();
    type Error = CodecError;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<(), CodecError> {
        self.element(v)
    }

    fn end(self) -> Result<(), CodecError> {
        self.done()
    }
}

impl ser::SerializeTuple for SeqSer<'_, '_, '_> {
    type Ok = ();
    type Error = CodecError;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<(), CodecError> {
        self.element(v)
    }

    fn end(self) -> Result<(), CodecError> {
        self.done()
    }
}

/// Whether the integer form of a kind exists: INT64, the temporal kinds and the decimals.
fn takes_integers(kind: BqKind) -> bool {
    matches!(
        kind,
        BqKind::Int64
            | BqKind::Date
            | BqKind::Time
            | BqKind::DateTime
            | BqKind::Timestamp
            | BqKind::Numeric
            | BqKind::BigNumeric
    )
}

/// jiff prints a year below zero with a sign and six digits, which no BigQuery text form has;
/// it is a date before BigQuery's year 1, not malformed text.
fn check_signed_year(kind: BqKind, s: &str) -> Result<(), CodecError> {
    if s.starts_with('-') {
        return Err(out_of_range(format!(
            "{} `{s}` is before BigQuery's 0001-01-01",
            kind.name()
        )));
    }
    Ok(())
}

fn numeric_in_range(v: i256) -> Result<i256, CodecError> {
    let limit = i256::from_i128(10i128.pow(38));
    if v >= limit || v <= limit.wrapping_neg() {
        let mut text = String::new();
        decimal::fmt_decimal_i256(v, NUMERIC_SCALE, &mut text);
        return Err(out_of_range(format!(
            "NUMERIC {text} has more than 29 integer digits"
        )));
    }
    Ok(v)
}

/// One value of a field: the whole field, an array element, or one entry of a packed run
/// (`packed`, written without its key).
struct ValSer<'r, 'e, 'p> {
    st: &'r mut St<'e>,
    f: &'p FieldPlan,
    packed: bool,
    in_array: bool,
}

impl<'r, 'e, 'p> ValSer<'r, 'e, 'p> {
    /// Writes `v`, which a JSON column takes in any shape.
    #[inline]
    fn value<T: Serialize + ?Sized>(self, v: &T) -> Result<(), CodecError> {
        if self.f.kind == BqKind::Json {
            v.serialize(JsonSer(self))
        } else {
            v.serialize(self)
        }
    }

    #[inline]
    fn key(&mut self) {
        if !self.packed {
            self.st.put(self.f.key.get());
        }
    }

    #[inline]
    fn len_delim(mut self, b: &[u8]) -> Result<(), CodecError> {
        self.key();
        self.st.len_delim(b);
        Ok(())
    }

    fn decimal(self, v: i256) -> Result<(), CodecError> {
        let (buf, n) = decimal::decimal_le_bytes(v);
        self.len_delim(&buf[..n])
    }

    #[inline]
    fn varint_field(mut self, v: u64) -> Result<(), CodecError> {
        self.key();
        self.st.varint(v);
        Ok(())
    }

    /// An integer in the column's integer form: days, microseconds of the day, civil or epoch
    /// microseconds, or a whole number for INT64 and the decimals.
    fn int(self, v: i64) -> Result<(), CodecError> {
        match self.f.kind {
            BqKind::Int64 => self.varint_field(v as u64),
            BqKind::Date => {
                let days = i32::try_from(v)
                    .ok()
                    .filter(|d| (DATE_MIN_DAYS..=DATE_MAX_DAYS).contains(d))
                    .ok_or_else(|| {
                        out_of_range(format!("DATE of {v} days is outside BigQuery's range"))
                    })?;
                self.varint_field(i64::from(days) as u64)
            }
            BqKind::Time => {
                if !(0..MICROS_PER_DAY).contains(&v) {
                    return Err(out_of_range(format!(
                        "{v} microseconds is not a time of day"
                    )));
                }
                self.varint_field(civil::pack_time(v) as u64)
            }
            BqKind::DateTime => {
                if !(TIMESTAMP_MIN_MICROS..=TIMESTAMP_MAX_MICROS).contains(&v) {
                    return Err(out_of_range(format!(
                        "DATETIME of {v} civil microseconds is outside BigQuery's range"
                    )));
                }
                self.varint_field(civil::pack_datetime(v) as u64)
            }
            BqKind::Timestamp => {
                if !(TIMESTAMP_MIN_MICROS..=TIMESTAMP_MAX_MICROS).contains(&v) {
                    return Err(out_of_range(format!(
                        "TIMESTAMP of {v} microseconds is outside BigQuery's range"
                    )));
                }
                self.varint_field(v as u64)
            }
            BqKind::Numeric => {
                self.decimal(i256::from_i128(i128::from(v) * 10i128.pow(NUMERIC_SCALE)))
            }
            BqKind::BigNumeric => {
                // 10^38 does not fit i128, so it is built as 10^19 squared; any i64 times it
                // stays well inside i256.
                let half = i256::from_i128(10i128.pow(BIGNUMERIC_SCALE / 2));
                self.decimal(i256::from_i128(i128::from(v)).wrapping_mul(half.wrapping_mul(half)))
            }
            _ => Err(mismatch(self.f, "an integer")),
        }
    }

    fn wide_int(self, v: i128) -> Result<(), CodecError> {
        if !takes_integers(self.f.kind) {
            return Err(mismatch(self.f, "an integer"));
        }
        let v = i64::try_from(v).map_err(|_| {
            out_of_range(format!(
                "{v} does not fit the INT64 range a {} column takes",
                self.f.kind.name()
            ))
        })?;
        self.int(v)
    }

    fn text(self, s: &str) -> Result<(), CodecError> {
        let kind = self.f.kind;
        match kind {
            BqKind::String | BqKind::Geography | BqKind::Json => self.len_delim(s.as_bytes()),
            BqKind::Date => {
                check_signed_year(kind, s)?;
                let days = civil::parse_date(s)?;
                self.int(i64::from(days))
            }
            BqKind::Time => {
                let micros = civil::parse_time(s)?;
                self.int(micros)
            }
            BqKind::DateTime => {
                check_signed_year(kind, s)?;
                let micros = civil::parse_datetime(s)?;
                self.int(micros)
            }
            BqKind::Timestamp => {
                check_signed_year(kind, s)?;
                let micros = civil::parse_timestamp(s)?;
                self.int(micros)
            }
            BqKind::Numeric => {
                let v = decimal::parse_numeric(s)?;
                self.decimal(v)
            }
            BqKind::BigNumeric => {
                let v = decimal::parse_bignumeric(s)?;
                self.decimal(v)
            }
            BqKind::Interval => {
                let interval = BigQueryInterval::parse_bq(s)?;
                self.interval(interval)
            }
            _ => Err(mismatch(self.f, "a string")),
        }
    }

    fn interval(self, interval: BigQueryInterval) -> Result<(), CodecError> {
        if interval.nanos % 1000 != 0 {
            return Err(out_of_range(format!(
                "INTERVAL keeps microseconds; {} nanoseconds is not a whole number of them",
                interval.nanos
            )));
        }
        let mut text = std::mem::take(self.st.scratch);
        text.clear();
        interval.write_bq(&mut text);
        let ValSer {
            st,
            f,
            packed,
            in_array,
        } = self;
        let result = ValSer {
            st: &mut *st,
            f,
            packed,
            in_array,
        }
        .len_delim(text.as_bytes());
        *st.scratch = text;
        result
    }

    fn null(self) -> Result<(), CodecError> {
        if self.in_array {
            Err(CodecError::new(
                BigQueryCodecErrorKind::NullArrayElement,
                "an ARRAY element cannot be NULL",
            ))
        } else if self.f.required {
            Err(CodecError::new(
                BigQueryCodecErrorKind::NullForRequired,
                "NULL for a REQUIRED field",
            ))
        } else {
            Ok(())
        }
    }

    fn message(mut self) -> Result<MsgSer<'r, 'e, 'p>, CodecError> {
        let sub = self
            .f
            .sub
            .as_deref()
            .ok_or_else(|| mismatch(self.f, "a struct or a map"))?;
        self.key();
        let mark = self.st.begin();
        Ok(MsgSer::new(self.st, sub, Some(mark)))
    }
}

impl<'r, 'e, 'p> ser::Serializer for ValSer<'r, 'e, 'p> {
    type Ok = ();
    type Error = CodecError;
    type SerializeSeq = Compound<'r, 'e, 'p>;
    type SerializeTuple = Compound<'r, 'e, 'p>;
    type SerializeTupleStruct = Compound<'r, 'e, 'p>;
    type SerializeTupleVariant = Impossible<(), CodecError>;
    type SerializeMap = MsgSer<'r, 'e, 'p>;
    type SerializeStruct = Compound<'r, 'e, 'p>;
    type SerializeStructVariant = Impossible<(), CodecError>;

    fn serialize_bool(self, v: bool) -> Result<(), CodecError> {
        if self.f.kind != BqKind::Bool {
            return Err(mismatch(self.f, "a bool"));
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
        self.int(v)
    }

    fn serialize_i128(self, v: i128) -> Result<(), CodecError> {
        self.wide_int(v)
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
        self.wide_int(i128::from(v))
    }

    fn serialize_u128(self, v: u128) -> Result<(), CodecError> {
        match i128::try_from(v) {
            Ok(v) => self.wide_int(v),
            Err(_) => self.wide_int(i128::MAX),
        }
    }

    fn serialize_f32(self, v: f32) -> Result<(), CodecError> {
        self.serialize_f64(f64::from(v))
    }

    #[inline]
    fn serialize_f64(mut self, v: f64) -> Result<(), CodecError> {
        match self.f.kind {
            BqKind::Float64 => {
                self.key();
                self.st.put(&v.to_le_bytes());
                Ok(())
            }
            BqKind::Numeric => {
                let d = numeric_in_range(decimal::decimal_from_f64(v, NUMERIC_SCALE)?)?;
                self.decimal(d)
            }
            BqKind::BigNumeric => {
                let d = decimal::decimal_from_f64(v, BIGNUMERIC_SCALE)?;
                self.decimal(d)
            }
            _ => Err(mismatch(self.f, "a float")),
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
        let mut text = std::mem::take(self.st.scratch);
        text.clear();
        let _ = write!(text, "{v}");
        let ValSer {
            st,
            f,
            packed,
            in_array,
        } = self;
        let result = ValSer {
            st: &mut *st,
            f,
            packed,
            in_array,
        }
        .text(&text);
        *st.scratch = text;
        result
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<(), CodecError> {
        match self.f.kind {
            BqKind::Bytes => self.len_delim(v),
            _ => Err(mismatch(self.f, "bytes")),
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
            if kind != self.f.kind {
                return Err(mismatch(self.f, name));
            }
            let raw = capture_int(v)?;
            return self.int(raw);
        }
        let tagged_kind_matches = match name {
            TAG_DECIMAL => matches!(self.f.kind, BqKind::Numeric | BqKind::BigNumeric),
            TAG_JSON => self.f.kind == BqKind::Json,
            _ => true,
        };
        if !tagged_kind_matches {
            return Err(mismatch(self.f, name));
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
        Err(mismatch(self.f, "an enum variant with data"))
    }

    fn serialize_seq(mut self, _len: Option<usize>) -> Result<Self::SerializeSeq, CodecError> {
        match self.f.kind {
            BqKind::Bytes => {
                self.key();
                let mark = self.st.begin();
                Ok(Compound::Bytes {
                    st: self.st,
                    f: self.f,
                    mark,
                })
            }
            _ if self.in_array => Err(CodecError::new(
                BigQueryCodecErrorKind::UnsupportedType,
                "an ARRAY of ARRAY is not a BigQuery column type",
            )),
            _ => Err(mismatch(self.f, "a sequence")),
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
        Err(mismatch(self.f, "an enum variant with data"))
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, CodecError> {
        self.message()
    }

    fn serialize_struct(
        self,
        name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, CodecError> {
        if self.f.kind == BqKind::Interval {
            if name != TAG_INTERVAL {
                return Err(mismatch(self.f, name));
            }
            return Ok(Compound::Interval {
                v: self,
                parts: [None; 3],
            });
        }
        Ok(Compound::Msg(self.message()?))
    }

    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStructVariant, CodecError> {
        Err(mismatch(self.f, "an enum variant with data"))
    }
}

fn json_error(err: serde_json::Error) -> CodecError {
    CodecError::new(
        BigQueryCodecErrorKind::Custom,
        format!("JSON column: {err}"),
    )
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
struct JsonSer<'r, 'e, 'p>(ValSer<'r, 'e, 'p>);

impl JsonSer<'_, '_, '_> {
    fn print(self, value: Result<serde_json::Value, serde_json::Error>) -> Result<(), CodecError> {
        let text = value.map_err(json_error)?.to_string();
        self.0.text(&text)
    }
}

impl<'r, 'e, 'p> ser::Serializer for JsonSer<'r, 'e, 'p> {
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
    target: JsonSer<'r, 'e, 'p>,
    inner: C,
}

impl<'r, 'e, 'p, C> JsonCompound<'r, 'e, 'p, C> {
    fn new(
        target: JsonSer<'r, 'e, 'p>,
        inner: Result<C, serde_json::Error>,
    ) -> Result<Self, CodecError> {
        Ok(JsonCompound {
            target,
            inner: inner.map_err(json_error)?,
        })
    }
}

impl<C: ser::SerializeSeq<Ok = serde_json::Value, Error = serde_json::Error>> ser::SerializeSeq
    for JsonCompound<'_, '_, '_, C>
{
    type Ok = ();
    type Error = CodecError;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<(), CodecError> {
        self.inner.serialize_element(v).map_err(json_error)
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
        self.inner.serialize_element(v).map_err(json_error)
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
        self.inner.serialize_field(v).map_err(json_error)
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
        self.inner.serialize_field(v).map_err(json_error)
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
        self.inner.serialize_key(key).map_err(json_error)
    }

    fn serialize_value<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<(), CodecError> {
        self.inner.serialize_value(v).map_err(json_error)
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
        self.inner.serialize_field(key, v).map_err(json_error)
    }

    fn skip_field(&mut self, key: &'static str) -> Result<(), CodecError> {
        self.inner.skip_field(key).map_err(json_error)
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
        self.inner.serialize_field(key, v).map_err(json_error)
    }

    fn end(self) -> Result<(), CodecError> {
        self.target.print(self.inner.end())
    }
}

/// The compound values one field can take: a nested message, BYTES as a sequence of `u8`,
/// or a [`BigQueryInterval`] as its struct.
enum Compound<'r, 'e, 'p> {
    Msg(MsgSer<'r, 'e, 'p>),
    Bytes {
        st: &'r mut St<'e>,
        f: &'p FieldPlan,
        mark: usize,
    },
    Interval {
        v: ValSer<'r, 'e, 'p>,
        parts: [Option<i64>; 3],
    },
}

const INTERVAL_PARTS: [&str; 3] = ["months", "days", "nanos"];

impl Compound<'_, '_, '_> {
    fn element<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<(), CodecError> {
        match self {
            Compound::Bytes { st, f, .. } => {
                let byte = capture_int(v)
                    .ok()
                    .and_then(|b| u8::try_from(b).ok())
                    .ok_or_else(|| mismatch(f, "a sequence of anything but u8"))?;
                st.byte(byte);
                Ok(())
            }
            Compound::Interval { v, .. } => Err(mismatch(v.f, "a sequence")),
            Compound::Msg(_) => Err(CodecError::new(
                BigQueryCodecErrorKind::TypeMismatch,
                "a STRUCT takes named fields",
            )),
        }
    }

    fn finish(self) -> Result<(), CodecError> {
        match self {
            Compound::Msg(m) => m.finish(),
            Compound::Bytes { st, mark, .. } => {
                st.end(mark);
                Ok(())
            }
            Compound::Interval { v, parts } => {
                let [Some(months), Some(days), Some(nanos)] = parts else {
                    return Err(mismatch(v.f, "an INTERVAL without months, days and nanos"));
                };
                let part = |value: i64, name: &str| {
                    i32::try_from(value).map_err(|_| {
                        out_of_range(format!("INTERVAL {name} {value} is out of range"))
                    })
                };
                let interval = BigQueryInterval {
                    months: part(months, "months")?,
                    days: part(days, "days")?,
                    nanos,
                };
                v.interval(interval)
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
            Compound::Msg(m) => ser::SerializeStruct::serialize_field(m, key, v),
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
                parts[i] = Some(capture_int(v).map_err(|e| e.at_field(key))?);
                Ok(())
            }
            Compound::Bytes { f, .. } => Err(mismatch(f, "a struct")),
        }
    }

    fn end(self) -> Result<(), CodecError> {
        self.finish()
    }
}

fn not_a_key() -> CodecError {
    CodecError::new(
        BigQueryCodecErrorKind::TypeMismatch,
        "a map key must be a string",
    )
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

    reject!(not_a_key();
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
        Err(not_a_key())
    }

    fn serialize_seq(self, _: Option<usize>) -> Result<Self::SerializeSeq, CodecError> {
        Err(not_a_key())
    }

    fn serialize_tuple(self, _: usize) -> Result<Self::SerializeTuple, CodecError> {
        Err(not_a_key())
    }

    fn serialize_tuple_struct(
        self,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleStruct, CodecError> {
        Err(not_a_key())
    }

    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleVariant, CodecError> {
        Err(not_a_key())
    }

    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap, CodecError> {
        Err(not_a_key())
    }

    fn serialize_struct(
        self,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStruct, CodecError> {
        Err(not_a_key())
    }

    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStructVariant, CodecError> {
        Err(not_a_key())
    }
}

#[cfg(test)]
pub(crate) mod tests;

#[cfg(test)]
mod roundtrip;
