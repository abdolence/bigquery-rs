//! The temporal wrappers' serde protocol.
//!
//! jiff's serde impls speak text only, so a plain jiff field costs a print and a parse per value
//! in the codecs. Each wrapper serializes as a newtype struct with a crate-private name around
//! an inner value that writes text to a human-readable serializer and the BigQuery integer to
//! any other. serde_json sees a transparent newtype and writes what plain jiff writes; the
//! codecs recognise the name, take the integer through [`capture_int`], and read by handing the
//! visitor the Arrow integer. The integer has a unit, so the codecs check the name against the
//! column type.

use crate::errors::BigQueryCodecErrorKind;
use crate::types::civil;
use crate::types::error::CodecError;
use crate::types::kind::BqKind;
use serde::de::{Unexpected, Visitor};
use serde::ser::Impossible;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt::Formatter;
use std::marker::PhantomData;

pub(crate) const TAG_TIMESTAMP: &str = "BigQueryTimestamp";
pub(crate) const TAG_DATE: &str = "BigQueryDate";
pub(crate) const TAG_TIME: &str = "BigQueryTime";
pub(crate) const TAG_DATETIME: &str = "BigQueryDateTime";

/// The column kind a temporal wrapper's serde name stands for; `None` for any other name.
pub(crate) fn temporal_tag_kind(name: &str) -> Option<BqKind> {
    match name {
        TAG_TIMESTAMP => Some(BqKind::Timestamp),
        TAG_DATE => Some(BqKind::Date),
        TAG_TIME => Some(BqKind::Time),
        TAG_DATETIME => Some(BqKind::DateTime),
        _ => None,
    }
}

/// Serializes `value` through a serializer that is not human-readable and takes one integer,
/// so a temporal wrapper or its inner value yields its BigQuery integer. Anything that is not a
/// single integer is `TypeMismatch`; a `u64` above `i64::MAX` is `OutOfRange`. The integer is
/// not checked against BigQuery's range, which the caller does for its column type.
pub(crate) fn capture_int<T: Serialize + ?Sized>(value: &T) -> Result<i64, CodecError> {
    value.serialize(IntCapture)
}

/// A jiff civil or absolute time with a BigQuery integer form.
trait Temporal: Sized + Copy + std::fmt::Display + Serialize + for<'de> Deserialize<'de> {
    const TAG: &'static str;
    const EXPECTING: &'static str;
    /// Whether the integer form is an `i32`, as DATE days are, rather than an `i64`.
    const INT_IS_I32: bool;

    /// The integer form, unchecked against BigQuery's range so that other formats keep any
    /// jiff value.
    fn to_int(self) -> i64;
    fn from_int(v: i64) -> Result<Self, CodecError>;
}

impl Temporal for jiff::Timestamp {
    const TAG: &'static str = TAG_TIMESTAMP;
    const EXPECTING: &'static str = "a TIMESTAMP as RFC 3339 text or microseconds since the epoch";
    const INT_IS_I32: bool = false;

    fn to_int(self) -> i64 {
        civil::raw_timestamp_micros(self)
    }

    fn from_int(v: i64) -> Result<Self, CodecError> {
        civil::jiff_timestamp(v)
    }
}

impl Temporal for jiff::civil::Date {
    const TAG: &'static str = TAG_DATE;
    const EXPECTING: &'static str = "a DATE as YYYY-MM-DD text or days since 1970-01-01";
    const INT_IS_I32: bool = true;

    fn to_int(self) -> i64 {
        i64::from(civil::raw_date_days(self))
    }

    fn from_int(v: i64) -> Result<Self, CodecError> {
        let days = i32::try_from(v).map_err(|_| {
            CodecError::new(
                BigQueryCodecErrorKind::OutOfRange,
                format!("DATE of {v} days is outside BigQuery's range"),
            )
        })?;
        civil::jiff_date(days)
    }
}

impl Temporal for jiff::civil::Time {
    const TAG: &'static str = TAG_TIME;
    const EXPECTING: &'static str = "a TIME as HH:MM:SS text or microseconds since midnight";
    const INT_IS_I32: bool = false;

    fn to_int(self) -> i64 {
        civil::time_micros(self)
    }

    fn from_int(v: i64) -> Result<Self, CodecError> {
        civil::jiff_time(v)
    }
}

impl Temporal for jiff::civil::DateTime {
    const TAG: &'static str = TAG_DATETIME;
    const EXPECTING: &'static str =
        "a DATETIME as YYYY-MM-DDTHH:MM:SS text or civil microseconds since 1970-01-01";
    const INT_IS_I32: bool = false;

    fn to_int(self) -> i64 {
        civil::raw_datetime_micros(self)
    }

    fn from_int(v: i64) -> Result<Self, CodecError> {
        civil::jiff_datetime(v)
    }
}

/// The value inside a wrapper's newtype struct: text or the integer, by the serializer.
struct Inner<T>(T);

impl<T: Temporal> Serialize for Inner<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            serializer.collect_str(&self.0)
        } else if T::INT_IS_I32 {
            // A jiff date is within ±9999 years, so its days always fit i32.
            serializer.serialize_i32(self.0.to_int() as i32)
        } else {
            serializer.serialize_i64(self.0.to_int())
        }
    }
}

fn serialize_temporal<T: Temporal, S: Serializer>(
    value: T,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_newtype_struct(T::TAG, &Inner(value))
}

fn deserialize_temporal<'de, T: Temporal, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<T, D::Error> {
    deserializer.deserialize_newtype_struct(T::TAG, TemporalVisitor(PhantomData))
}

struct TemporalVisitor<T>(PhantomData<T>);

impl<'de, T: Temporal> Visitor<'de> for TemporalVisitor<T> {
    type Value = T;

    fn expecting(&self, f: &mut Formatter) -> std::fmt::Result {
        f.write_str(T::EXPECTING)
    }

    /// The codecs' read path: the Arrow integer of the column.
    fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<T, E> {
        T::from_int(v).map_err(|_| E::invalid_value(Unexpected::Signed(v), &self))
    }

    fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<T, E> {
        match i64::try_from(v) {
            Ok(v) => self.visit_i64(v),
            Err(_) => Err(E::invalid_value(Unexpected::Unsigned(v), &self)),
        }
    }

    /// A value serde buffered on the way, as under `flatten`.
    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<T, E> {
        T::deserialize(serde::de::value::StrDeserializer::<E>::new(v))
    }

    /// Any other format: the inner value as the wrapper's `Serialize` wrote it.
    fn visit_newtype_struct<D: Deserializer<'de>>(self, deserializer: D) -> Result<T, D::Error> {
        if deserializer.is_human_readable() {
            T::deserialize(deserializer)
        } else if T::INT_IS_I32 {
            deserializer.deserialize_i32(self)
        } else {
            deserializer.deserialize_i64(self)
        }
    }
}

/// A TIMESTAMP column with the codecs' integer fast path; any other format sees it as a plain
/// `jiff::Timestamp`: RFC 3339 text in human-readable formats, microseconds since the epoch in
/// the others.
///
/// Reading a TIMESTAMP above `9999-12-30T22:00:00.999999999Z`, jiff's maximum, fails that row
/// with [`OutOfRange`](crate::errors::BigQueryCodecErrorKind::OutOfRange); read such values
/// into a `String` or an `i64`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BigQueryTimestamp(pub jiff::Timestamp);

/// A DATE column with the codecs' integer fast path; any other format sees it as a plain
/// `jiff::civil::Date`, or `i32` days since 1970-01-01 in formats that are not human-readable.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BigQueryDate(pub jiff::civil::Date);

/// A TIME column with the codecs' integer fast path; any other format sees it as a plain
/// `jiff::civil::Time`, or `i64` microseconds since midnight in formats that are not
/// human-readable. Sub-microsecond digits are dropped on write.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BigQueryTime(pub jiff::civil::Time);

/// A DATETIME column with the codecs' integer fast path; any other format sees it as a plain
/// `jiff::civil::DateTime`, or `i64` civil microseconds since 1970-01-01T00:00:00 in formats
/// that are not human-readable.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BigQueryDateTime(pub jiff::civil::DateTime);

impl Serialize for BigQueryTimestamp {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serialize_temporal(self.0, serializer)
    }
}

impl<'de> Deserialize<'de> for BigQueryTimestamp {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserialize_temporal(deserializer).map(BigQueryTimestamp)
    }
}

impl Serialize for BigQueryDate {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serialize_temporal(self.0, serializer)
    }
}

impl<'de> Deserialize<'de> for BigQueryDate {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserialize_temporal(deserializer).map(BigQueryDate)
    }
}

impl Serialize for BigQueryTime {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serialize_temporal(self.0, serializer)
    }
}

impl<'de> Deserialize<'de> for BigQueryTime {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserialize_temporal(deserializer).map(BigQueryTime)
    }
}

impl Serialize for BigQueryDateTime {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serialize_temporal(self.0, serializer)
    }
}

impl<'de> Deserialize<'de> for BigQueryDateTime {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserialize_temporal(deserializer).map(BigQueryDateTime)
    }
}

fn serialize_optional<T: Temporal, S: Serializer>(
    value: &Option<T>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    match value {
        Some(value) => serializer.serialize_some(&Wrapped(*value)),
        None => serializer.serialize_none(),
    }
}

fn deserialize_optional<'de, T: Temporal, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<T>, D::Error> {
    Option::<Wrapped<T>>::deserialize(deserializer).map(|value| value.map(|value| value.0))
}

/// Any temporal type with its wrapper's serde form, for the `Option` `with` modules.
struct Wrapped<T>(T);

impl<T: Temporal> Serialize for Wrapped<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serialize_temporal(self.0, serializer)
    }
}

impl<'de, T: Temporal> Deserialize<'de> for Wrapped<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserialize_temporal(deserializer).map(Wrapped)
    }
}

/// `#[serde(with = "bigquery::serialize_as_timestamp")]` for a `jiff::Timestamp` field, the same
/// as [`BigQueryTimestamp`].
pub mod serialize_as_timestamp {
    use serde::{Deserializer, Serializer};

    /// Serializes `value` as [`BigQueryTimestamp`](crate::BigQueryTimestamp) does.
    pub fn serialize<S: Serializer>(
        value: &jiff::Timestamp,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        super::serialize_temporal(*value, serializer)
    }

    /// Deserializes a value as [`BigQueryTimestamp`](crate::BigQueryTimestamp) does.
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<jiff::Timestamp, D::Error> {
        super::deserialize_temporal(deserializer)
    }
}

/// `#[serde(with = "bigquery::serialize_as_optional_timestamp")]` for an
/// `Option<jiff::Timestamp>` field, the same as `Option<BigQueryTimestamp>`.
pub mod serialize_as_optional_timestamp {
    use serde::{Deserializer, Serializer};

    /// Serializes `value` as `Option<BigQueryTimestamp>` does.
    pub fn serialize<S: Serializer>(
        value: &Option<jiff::Timestamp>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        super::serialize_optional(value, serializer)
    }

    /// Deserializes a value as `Option<BigQueryTimestamp>` does.
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<jiff::Timestamp>, D::Error> {
        super::deserialize_optional(deserializer)
    }
}

/// `#[serde(with = "bigquery::serialize_as_date")]` for a `jiff::civil::Date` field, the same as
/// [`BigQueryDate`].
pub mod serialize_as_date {
    use serde::{Deserializer, Serializer};

    /// Serializes `value` as [`BigQueryDate`](crate::BigQueryDate) does.
    pub fn serialize<S: Serializer>(
        value: &jiff::civil::Date,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        super::serialize_temporal(*value, serializer)
    }

    /// Deserializes a value as [`BigQueryDate`](crate::BigQueryDate) does.
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<jiff::civil::Date, D::Error> {
        super::deserialize_temporal(deserializer)
    }
}

/// `#[serde(with = "bigquery::serialize_as_optional_date")]` for an `Option<jiff::civil::Date>`
/// field, the same as `Option<BigQueryDate>`.
pub mod serialize_as_optional_date {
    use serde::{Deserializer, Serializer};

    /// Serializes `value` as `Option<BigQueryDate>` does.
    pub fn serialize<S: Serializer>(
        value: &Option<jiff::civil::Date>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        super::serialize_optional(value, serializer)
    }

    /// Deserializes a value as `Option<BigQueryDate>` does.
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<jiff::civil::Date>, D::Error> {
        super::deserialize_optional(deserializer)
    }
}

/// `#[serde(with = "bigquery::serialize_as_time")]` for a `jiff::civil::Time` field, the same as
/// [`BigQueryTime`].
pub mod serialize_as_time {
    use serde::{Deserializer, Serializer};

    /// Serializes `value` as [`BigQueryTime`](crate::BigQueryTime) does.
    pub fn serialize<S: Serializer>(
        value: &jiff::civil::Time,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        super::serialize_temporal(*value, serializer)
    }

    /// Deserializes a value as [`BigQueryTime`](crate::BigQueryTime) does.
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<jiff::civil::Time, D::Error> {
        super::deserialize_temporal(deserializer)
    }
}

/// `#[serde(with = "bigquery::serialize_as_optional_time")]` for an `Option<jiff::civil::Time>`
/// field, the same as `Option<BigQueryTime>`.
pub mod serialize_as_optional_time {
    use serde::{Deserializer, Serializer};

    /// Serializes `value` as `Option<BigQueryTime>` does.
    pub fn serialize<S: Serializer>(
        value: &Option<jiff::civil::Time>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        super::serialize_optional(value, serializer)
    }

    /// Deserializes a value as `Option<BigQueryTime>` does.
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<jiff::civil::Time>, D::Error> {
        super::deserialize_optional(deserializer)
    }
}

/// `#[serde(with = "bigquery::serialize_as_datetime")]` for a `jiff::civil::DateTime` field, the
/// same as [`BigQueryDateTime`].
pub mod serialize_as_datetime {
    use serde::{Deserializer, Serializer};

    /// Serializes `value` as [`BigQueryDateTime`](crate::BigQueryDateTime) does.
    pub fn serialize<S: Serializer>(
        value: &jiff::civil::DateTime,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        super::serialize_temporal(*value, serializer)
    }

    /// Deserializes a value as [`BigQueryDateTime`](crate::BigQueryDateTime) does.
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<jiff::civil::DateTime, D::Error> {
        super::deserialize_temporal(deserializer)
    }
}

/// `#[serde(with = "bigquery::serialize_as_optional_datetime")]` for an
/// `Option<jiff::civil::DateTime>` field, the same as `Option<BigQueryDateTime>`.
pub mod serialize_as_optional_datetime {
    use serde::{Deserializer, Serializer};

    /// Serializes `value` as `Option<BigQueryDateTime>` does.
    pub fn serialize<S: Serializer>(
        value: &Option<jiff::civil::DateTime>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        super::serialize_optional(value, serializer)
    }

    /// Deserializes a value as `Option<BigQueryDateTime>` does.
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<jiff::civil::DateTime>, D::Error> {
        super::deserialize_optional(deserializer)
    }
}

/// The serializer behind [`capture_int`]. It is the only serializer in the crate that reports
/// `is_human_readable() == false`: ordinary values never see it, since types such as `uuid`
/// change their form when it is `false`.
struct IntCapture;

fn not_an_integer(what: &str) -> CodecError {
    CodecError::new(
        BigQueryCodecErrorKind::TypeMismatch,
        format!("expected an integer form of a temporal value, got {what}"),
    )
}

impl Serializer for IntCapture {
    type Ok = i64;
    type Error = CodecError;
    type SerializeSeq = Impossible<i64, CodecError>;
    type SerializeTuple = Impossible<i64, CodecError>;
    type SerializeTupleStruct = Impossible<i64, CodecError>;
    type SerializeTupleVariant = Impossible<i64, CodecError>;
    type SerializeMap = Impossible<i64, CodecError>;
    type SerializeStruct = Impossible<i64, CodecError>;
    type SerializeStructVariant = Impossible<i64, CodecError>;

    fn is_human_readable(&self) -> bool {
        false
    }

    fn serialize_i8(self, v: i8) -> Result<i64, CodecError> {
        Ok(i64::from(v))
    }

    fn serialize_i16(self, v: i16) -> Result<i64, CodecError> {
        Ok(i64::from(v))
    }

    fn serialize_i32(self, v: i32) -> Result<i64, CodecError> {
        Ok(i64::from(v))
    }

    fn serialize_i64(self, v: i64) -> Result<i64, CodecError> {
        Ok(v)
    }

    fn serialize_u8(self, v: u8) -> Result<i64, CodecError> {
        Ok(i64::from(v))
    }

    fn serialize_u16(self, v: u16) -> Result<i64, CodecError> {
        Ok(i64::from(v))
    }

    fn serialize_u32(self, v: u32) -> Result<i64, CodecError> {
        Ok(i64::from(v))
    }

    fn serialize_u64(self, v: u64) -> Result<i64, CodecError> {
        i64::try_from(v).map_err(|_| {
            CodecError::new(
                BigQueryCodecErrorKind::OutOfRange,
                format!("{v} does not fit in i64"),
            )
        })
    }

    fn serialize_bool(self, _: bool) -> Result<i64, CodecError> {
        Err(not_an_integer("a bool"))
    }

    fn serialize_f32(self, _: f32) -> Result<i64, CodecError> {
        Err(not_an_integer("a float"))
    }

    fn serialize_f64(self, _: f64) -> Result<i64, CodecError> {
        Err(not_an_integer("a float"))
    }

    fn serialize_char(self, _: char) -> Result<i64, CodecError> {
        Err(not_an_integer("a char"))
    }

    fn serialize_str(self, _: &str) -> Result<i64, CodecError> {
        Err(not_an_integer("text"))
    }

    fn serialize_bytes(self, _: &[u8]) -> Result<i64, CodecError> {
        Err(not_an_integer("bytes"))
    }

    fn serialize_none(self) -> Result<i64, CodecError> {
        Err(not_an_integer("None"))
    }

    fn serialize_some<T: Serialize + ?Sized>(self, _: &T) -> Result<i64, CodecError> {
        Err(not_an_integer("an Option"))
    }

    fn serialize_unit(self) -> Result<i64, CodecError> {
        Err(not_an_integer("unit"))
    }

    fn serialize_unit_struct(self, _: &'static str) -> Result<i64, CodecError> {
        Err(not_an_integer("a unit struct"))
    }

    fn serialize_unit_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
    ) -> Result<i64, CodecError> {
        Err(not_an_integer("an enum variant"))
    }

    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        value: &T,
    ) -> Result<i64, CodecError> {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: &T,
    ) -> Result<i64, CodecError> {
        Err(not_an_integer("an enum variant"))
    }

    fn serialize_seq(self, _: Option<usize>) -> Result<Self::SerializeSeq, CodecError> {
        Err(not_an_integer("a sequence"))
    }

    fn serialize_tuple(self, _: usize) -> Result<Self::SerializeTuple, CodecError> {
        Err(not_an_integer("a tuple"))
    }

    fn serialize_tuple_struct(
        self,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleStruct, CodecError> {
        Err(not_an_integer("a tuple struct"))
    }

    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleVariant, CodecError> {
        Err(not_an_integer("an enum variant"))
    }

    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap, CodecError> {
        Err(not_an_integer("a map"))
    }

    fn serialize_struct(
        self,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStruct, CodecError> {
        Err(not_an_integer("a struct"))
    }

    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStructVariant, CodecError> {
        Err(not_an_integer("an enum variant"))
    }
}

#[cfg(test)]
mod tests;
