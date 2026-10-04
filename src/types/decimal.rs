//! NUMERIC and BIGNUMERIC as unscaled integers, their text and their Storage Write bytes.

use crate::errors::BigQueryCodecErrorKind;
use crate::types::error::CodecError;
use arrow_buffer::i256;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt::Display;
use std::str::FromStr;

pub(crate) const NUMERIC_SCALE: u32 = 9;
pub(crate) const BIGNUMERIC_SCALE: u32 = 38;

pub(crate) const TAG_DECIMAL: &str = "BigQueryDecimal";

/// Writes the magnitude `digits` with the point `scale` digits from the right and trailing
/// fractional zeros removed.
fn place_point(neg: bool, digits: &[u8], scale: usize, out: &mut String) {
    if neg {
        out.push('-');
    }
    let (int, frac) = if digits.len() > scale {
        digits.split_at(digits.len() - scale)
    } else {
        (&b"0"[..], digits)
    };
    int.iter().for_each(|&c| out.push(char::from(c)));
    let pad = scale - frac.len();
    let mut f = frac;
    while let [rest @ .., b'0'] = f {
        f = rest;
    }
    if !f.is_empty() {
        out.push('.');
        (0..pad).for_each(|_| out.push('0'));
        f.iter().for_each(|&c| out.push(char::from(c)));
    }
}

/// The canonical text of an unscaled value at `scale`: no exponent, no trailing fractional
/// zeros, and `0` for zero.
pub(crate) fn fmt_decimal_i128(v: i128, scale: u32, out: &mut String) {
    if v == 0 {
        out.push('0');
        return;
    }
    let mut buf = [0u8; 40];
    let mut i = buf.len();
    let mut m = v.unsigned_abs();
    while m > 0 {
        i -= 1;
        buf[i] = b'0' + (m % 10) as u8;
        m /= 10;
    }
    place_point(v < 0, &buf[i..], scale as usize, out);
}

/// [`fmt_decimal_i128`] for the full `i256` range.
pub(crate) fn fmt_decimal_i256(v: i256, scale: u32, out: &mut String) {
    if let Some(small) = v.to_i128() {
        return fmt_decimal_i128(small, scale, out);
    }
    let s = v.to_string();
    let (neg, mag) = match s.strip_prefix('-') {
        Some(m) => (true, m),
        None => (false, s.as_str()),
    };
    place_point(neg, mag.as_bytes(), scale as usize, out);
}

/// Plain decimal text (`-123.45`, no exponent) as an unscaled integer at `scale`. More
/// fractional digits than `scale` is `OutOfRange`, never a rounding.
fn parse_decimal(s: &str, scale: u32) -> Result<i256, CodecError> {
    let bad = || {
        CodecError::new(
            BigQueryCodecErrorKind::InvalidText,
            format!("invalid decimal `{s}`, expected plain decimal text such as -123.45"),
        )
    };
    let (neg, body) = match s.as_bytes().first() {
        Some(b'-') => (true, &s[1..]),
        Some(b'+') => (false, &s[1..]),
        _ => (false, s),
    };
    let (int, frac) = body.split_once('.').unwrap_or((body, ""));
    if int.is_empty() && frac.is_empty() {
        return Err(bad());
    }
    if !int.bytes().chain(frac.bytes()).all(|c| c.is_ascii_digit()) {
        return Err(bad());
    }
    if frac.len() > scale as usize {
        return Err(CodecError::out_of_range(format!(
            "decimal `{s}` has more than {scale} fractional digits"
        )));
    }
    let ten = i256::from_i128(10);
    let mut acc = i256::ZERO;
    let pad = scale as usize - frac.len();
    for c in int
        .bytes()
        .chain(frac.bytes())
        .chain(std::iter::repeat_n(b'0', pad))
    {
        let d = i256::from_i128(i128::from(c - b'0'));
        // Accumulated as a negative number so that i256::MIN is reachable.
        acc = acc
            .checked_mul(ten)
            .and_then(|a| a.checked_sub(d))
            .ok_or_else(|| CodecError::out_of_range(format!("decimal `{s}` is out of range")))?;
    }
    if neg {
        Ok(acc)
    } else {
        acc.checked_neg()
            .ok_or_else(|| CodecError::out_of_range(format!("decimal `{s}` is out of range")))
    }
}

/// NUMERIC text as its unscaled value at scale 9, within 29 integer digits.
pub(crate) fn parse_numeric(s: &str) -> Result<i256, CodecError> {
    let v = parse_decimal(s, NUMERIC_SCALE)?;
    let lim = i256::from_i128(10i128.pow(38));
    if v >= lim || v <= lim.wrapping_neg() {
        return Err(CodecError::out_of_range(format!(
            "NUMERIC `{s}` has more than 29 integer digits"
        )));
    }
    Ok(v)
}

/// BIGNUMERIC text as its unscaled value at scale 38, within the `i256` range.
pub(crate) fn parse_bignumeric(s: &str) -> Result<i256, CodecError> {
    parse_decimal(s, BIGNUMERIC_SCALE)
}

/// The decimal nearest to `x`, rounded at `scale`; `OutOfRange` for NaN and infinities.
pub(crate) fn decimal_from_f64(x: f64, scale: u32) -> Result<i256, CodecError> {
    if !x.is_finite() {
        return Err(CodecError::out_of_range(format!(
            "{x} has no NUMERIC or BIGNUMERIC value"
        )));
    }
    let mut s = format!("{x:.*}", scale as usize);
    if s.contains('.') {
        while s.ends_with('0') {
            s.pop();
        }
        if s.ends_with('.') {
            s.pop();
        }
    }
    parse_decimal(&s, scale)
}

/// NUMERIC and BIGNUMERIC Storage Write bytes, as Google's `BigDecimalByteStringEncoder` writes
/// them: the unscaled value in minimal little-endian two's complement. The value is
/// `buf[..len]`.
pub(crate) fn decimal_le_bytes(v: i256) -> ([u8; 32], usize) {
    let le = v.to_le_bytes();
    let mut len = 32;
    while len > 1 {
        let top = le[len - 1];
        let next_sign = le[len - 2] & 0x80;
        if (top == 0x00 && next_sign == 0) || (top == 0xff && next_sign != 0) {
            len -= 1;
        } else {
            break;
        }
    }
    (le, len)
}

/// The inverse of [`decimal_le_bytes`]: little-endian two's complement of up to 32 bytes,
/// sign-extended. Bytes beyond 32 are ignored. The tests read written bytes back with it.
#[cfg(test)]
pub(crate) fn decimal_from_le_bytes(bytes: &[u8]) -> i256 {
    let negative = bytes.last().is_some_and(|b| b & 0x80 != 0);
    let mut buf = if negative { [0xff; 32] } else { [0; 32] };
    let n = bytes.len().min(32);
    buf[..n].copy_from_slice(&bytes[..n]);
    i256::from_le_bytes(buf)
}

/// A NUMERIC or BIGNUMERIC value held in any decimal type that round-trips through its
/// `Display` and `FromStr` text, such as `bigdecimal::BigDecimal` or `rust_decimal::Decimal`.
///
/// The text form is used whatever the type's own serde impl does, since some decimal types
/// serialize as `f64` and would lose digits. In other serde formats it is the decimal text.
/// A value whose text `T::from_str` rejects fails its row with
/// [`Custom`](crate::errors::BigQueryCodecErrorKind::Custom).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BigQueryDecimal<T>(pub T);

impl<T: Display> Serialize for BigQueryDecimal<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_newtype_struct(TAG_DECIMAL, &DecimalText(&self.0))
    }
}

struct DecimalText<'a, T>(&'a T);

impl<T: Display> Serialize for DecimalText<'_, T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self.0)
    }
}

impl<'de, T> Deserialize<'de> for BigQueryDecimal<T>
where
    T: FromStr,
    T::Err: Display,
{
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer
            .deserialize_newtype_struct(TAG_DECIMAL, DecimalVisitor(std::marker::PhantomData))
    }
}

struct DecimalVisitor<T>(std::marker::PhantomData<T>);

impl<'de, T> serde::de::Visitor<'de> for DecimalVisitor<T>
where
    T: FromStr,
    T::Err: Display,
{
    type Value = BigQueryDecimal<T>;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("decimal text")
    }

    fn visit_newtype_struct<D: Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
        d.deserialize_str(self)
    }

    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
        v.parse().map(BigQueryDecimal).map_err(E::custom)
    }
}

/// `#[serde(with = "bigquery::serialize_as_decimal")]` for a bare decimal field, the same as
/// [`BigQueryDecimal`].
pub mod serialize_as_decimal {
    use super::BigQueryDecimal;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::fmt::Display;
    use std::str::FromStr;

    /// Serializes `value` as [`BigQueryDecimal`] does.
    pub fn serialize<T: Display, S: Serializer>(
        value: &T,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        BigQueryDecimal(value).serialize(serializer)
    }

    /// Deserializes a value as [`BigQueryDecimal`] does.
    pub fn deserialize<'de, T, D>(deserializer: D) -> Result<T, D::Error>
    where
        T: FromStr,
        T::Err: Display,
        D: Deserializer<'de>,
    {
        BigQueryDecimal::<T>::deserialize(deserializer).map(|decimal| decimal.0)
    }
}

/// `#[serde(with = "bigquery::serialize_as_optional_decimal")]` for an `Option` decimal field,
/// the same as `Option<BigQueryDecimal<T>>`.
pub mod serialize_as_optional_decimal {
    use super::BigQueryDecimal;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::fmt::Display;
    use std::str::FromStr;

    /// Serializes `value` as `Option<BigQueryDecimal<T>>` does.
    pub fn serialize<T: Display, S: Serializer>(
        value: &Option<T>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        value.as_ref().map(BigQueryDecimal).serialize(serializer)
    }

    /// Deserializes a value as `Option<BigQueryDecimal<T>>` does.
    pub fn deserialize<'de, T, D>(deserializer: D) -> Result<Option<T>, D::Error>
    where
        T: FromStr,
        T::Err: Display,
        D: Deserializer<'de>,
    {
        Option::<BigQueryDecimal<T>>::deserialize(deserializer)
            .map(|decimal| decimal.map(|decimal| decimal.0))
    }
}

#[cfg(test)]
mod tests;
