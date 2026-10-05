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
fn place_point(negative: bool, digits: &[u8], scale: usize, out: &mut String) {
    if negative {
        out.push('-');
    }
    let (integer_digits, fraction_digits) = if digits.len() > scale {
        digits.split_at(digits.len() - scale)
    } else {
        (&b"0"[..], digits)
    };
    integer_digits
        .iter()
        .for_each(|&digit| out.push(char::from(digit)));
    let padding = scale - fraction_digits.len();
    let mut trimmed = fraction_digits;
    while let [rest @ .., b'0'] = trimmed {
        trimmed = rest;
    }
    if !trimmed.is_empty() {
        out.push('.');
        (0..padding).for_each(|_| out.push('0'));
        trimmed
            .iter()
            .for_each(|&digit| out.push(char::from(digit)));
    }
}

/// The canonical text of an unscaled value at `scale`: no exponent, no trailing fractional
/// zeros, and `0` for zero.
pub(crate) fn fmt_decimal_i128(unscaled: i128, scale: u32, out: &mut String) {
    if unscaled == 0 {
        out.push('0');
        return;
    }
    let mut buffer = [0u8; 40];
    let mut index = buffer.len();
    let mut remaining = unscaled.unsigned_abs();
    while remaining > 0 {
        index -= 1;
        buffer[index] = b'0' + (remaining % 10) as u8;
        remaining /= 10;
    }
    place_point(unscaled < 0, &buffer[index..], scale as usize, out);
}

/// [`fmt_decimal_i128`] for the full `i256` range.
pub(crate) fn fmt_decimal_i256(unscaled: i256, scale: u32, out: &mut String) {
    if let Some(small) = unscaled.to_i128() {
        return fmt_decimal_i128(small, scale, out);
    }
    let text = unscaled.to_string();
    let (negative, magnitude) = match text.strip_prefix('-') {
        Some(magnitude) => (true, magnitude),
        None => (false, text.as_str()),
    };
    place_point(negative, magnitude.as_bytes(), scale as usize, out);
}

/// [`fmt_decimal_i256`] into a new `String`.
pub(crate) fn decimal_string(unscaled: i256, scale: u32) -> String {
    let mut text = String::new();
    fmt_decimal_i256(unscaled, scale, &mut text);
    text
}

/// Plain decimal text (`-123.45`, no exponent) as an unscaled integer at `scale`. More
/// fractional digits than `scale` is `OutOfRange`, never a rounding.
fn parse_decimal(text: &str, scale: u32) -> Result<i256, CodecError> {
    let invalid = || {
        CodecError::new(
            BigQueryCodecErrorKind::InvalidText,
            format!("invalid decimal `{text}`, expected plain decimal text such as -123.45"),
        )
    };
    let (negative, body) = match text.as_bytes().first() {
        Some(b'-') => (true, &text[1..]),
        Some(b'+') => (false, &text[1..]),
        _ => (false, text),
    };
    let (integer_digits, fraction_digits) = body.split_once('.').unwrap_or((body, ""));
    if integer_digits.is_empty() && fraction_digits.is_empty() {
        return Err(invalid());
    }
    if !integer_digits
        .bytes()
        .chain(fraction_digits.bytes())
        .all(|digit| digit.is_ascii_digit())
    {
        return Err(invalid());
    }
    if fraction_digits.len() > scale as usize {
        return Err(CodecError::out_of_range(format!(
            "decimal `{text}` has more than {scale} fractional digits"
        )));
    }
    let ten = i256::from_i128(10);
    let mut accumulated = i256::ZERO;
    let padding = scale as usize - fraction_digits.len();
    for digit in integer_digits
        .bytes()
        .chain(fraction_digits.bytes())
        .chain(std::iter::repeat_n(b'0', padding))
    {
        let digit = i256::from_i128(i128::from(digit - b'0'));
        // Accumulated as a negative number so that i256::MIN is reachable.
        accumulated = accumulated
            .checked_mul(ten)
            .and_then(|accumulated| accumulated.checked_sub(digit))
            .ok_or_else(|| CodecError::out_of_range(format!("decimal `{text}` is out of range")))?;
    }
    if negative {
        Ok(accumulated)
    } else {
        accumulated
            .checked_neg()
            .ok_or_else(|| CodecError::out_of_range(format!("decimal `{text}` is out of range")))
    }
}

/// NUMERIC text as its unscaled value at scale 9, within 29 integer digits.
pub(crate) fn parse_numeric(text: &str) -> Result<i256, CodecError> {
    numeric_in_range(parse_decimal(text, NUMERIC_SCALE)?)
}

/// `unscaled` at scale 9 when it is within NUMERIC's 29 integer digits, `OutOfRange` otherwise.
pub(crate) fn numeric_in_range(unscaled: i256) -> Result<i256, CodecError> {
    let limit = i256::from_i128(10i128.pow(38));
    if unscaled >= limit || unscaled <= limit.wrapping_neg() {
        return Err(CodecError::out_of_range(format!(
            "NUMERIC {} has more than 29 integer digits",
            decimal_string(unscaled, NUMERIC_SCALE)
        )));
    }
    Ok(unscaled)
}

/// BIGNUMERIC text as its unscaled value at scale 38, within the `i256` range.
pub(crate) fn parse_bignumeric(text: &str) -> Result<i256, CodecError> {
    parse_decimal(text, BIGNUMERIC_SCALE)
}

/// The decimal nearest to `float`, rounded at `scale`; `OutOfRange` for NaN and infinities.
pub(crate) fn decimal_from_f64(float: f64, scale: u32) -> Result<i256, CodecError> {
    if !float.is_finite() {
        return Err(CodecError::out_of_range(format!(
            "{float} has no NUMERIC or BIGNUMERIC value"
        )));
    }
    let mut text = format!("{float:.*}", scale as usize);
    if text.contains('.') {
        while text.ends_with('0') {
            text.pop();
        }
        if text.ends_with('.') {
            text.pop();
        }
    }
    parse_decimal(&text, scale)
}

/// NUMERIC and BIGNUMERIC Storage Write bytes, as Google's `BigDecimalByteStringEncoder` writes
/// them: the unscaled value in minimal little-endian two's complement. The value is
/// the array's first `length` bytes, for the returned `(array, length)`.
pub(crate) fn decimal_le_bytes(unscaled: i256) -> ([u8; 32], usize) {
    let little_endian = unscaled.to_le_bytes();
    let mut length = 32;
    while length > 1 {
        let top = little_endian[length - 1];
        let next_sign = little_endian[length - 2] & 0x80;
        if (top == 0x00 && next_sign == 0) || (top == 0xff && next_sign != 0) {
            length -= 1;
        } else {
            break;
        }
    }
    (little_endian, length)
}

/// The inverse of [`decimal_le_bytes`]: little-endian two's complement of up to 32 bytes,
/// sign-extended. Bytes beyond 32 are ignored. The tests read written bytes back with it.
#[cfg(test)]
pub(crate) fn decimal_from_le_bytes(bytes: &[u8]) -> i256 {
    let negative = bytes.last().is_some_and(|byte| byte & 0x80 != 0);
    let mut buffer = if negative { [0xff; 32] } else { [0; 32] };
    let length = bytes.len().min(32);
    buffer[..length].copy_from_slice(&bytes[..length]);
    i256::from_le_bytes(buffer)
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

    fn visit_newtype_struct<D: Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_str(self)
    }

    fn visit_str<E: serde::de::Error>(self, text: &str) -> Result<Self::Value, E> {
        text.parse().map(BigQueryDecimal).map_err(E::custom)
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
mod tests {
    use super::*;
    use crate::types::testkit::{error_kind, written};

    const I256_MAX_AT_38: &str =
        "578960446186580977117854925043439539266.34992332820282019728792003956564819967";
    const I256_MIN_AT_38: &str =
        "-578960446186580977117854925043439539266.34992332820282019728792003956564819968";

    #[test]
    fn decimals_format_trimmed_and_parse_at_scale() {
        assert_eq!(
            written(|text| fmt_decimal_i128(123_450_000_000, 9, text)),
            "123.45"
        );
        assert_eq!(
            written(|text| fmt_decimal_i128(-1, 9, text)),
            "-0.000000001"
        );
        assert_eq!(written(|text| fmt_decimal_i128(0, 9, text)), "0");
        assert_eq!(
            written(|text| fmt_decimal_i128(5_000_000_000, 9, text)),
            "5"
        );
        assert_eq!(
            written(|text| fmt_decimal_i128(i128::pow(10, 38) - 1, 9, text)),
            "99999999999999999999999999999.999999999"
        );
        assert_eq!(
            written(|text| fmt_decimal_i256(i256::MAX, 38, text)),
            I256_MAX_AT_38
        );
        assert_eq!(
            written(|text| fmt_decimal_i256(i256::MIN, 38, text)),
            I256_MIN_AT_38
        );

        assert_eq!(
            parse_numeric("123.45").ok(),
            Some(i256::from_i128(123_450_000_000))
        );
        assert_eq!(
            parse_numeric("-0.000000001").ok(),
            Some(i256::from_i128(-1))
        );
        assert_eq!(
            parse_numeric("+7").ok(),
            Some(i256::from_i128(7_000_000_000))
        );
        assert_eq!(parse_numeric(".5").ok(), Some(i256::from_i128(500_000_000)));
        assert_eq!(
            error_kind(parse_numeric("0.0000000001")),
            BigQueryCodecErrorKind::OutOfRange,
            "more digits than the scale"
        );
        assert_eq!(
            error_kind(parse_numeric("100000000000000000000000000000")),
            BigQueryCodecErrorKind::OutOfRange,
            "30 integer digits"
        );
        assert_eq!(
            parse_numeric("99999999999999999999999999999.999999999").ok(),
            Some(i256::from_i128(i128::pow(10, 38) - 1))
        );
        assert_eq!(
            error_kind(parse_numeric("1e5")),
            BigQueryCodecErrorKind::InvalidText
        );
        assert_eq!(
            error_kind(parse_numeric("")),
            BigQueryCodecErrorKind::InvalidText
        );
        assert_eq!(
            error_kind(parse_numeric("-")),
            BigQueryCodecErrorKind::InvalidText
        );
        assert_eq!(parse_bignumeric(I256_MIN_AT_38).ok(), Some(i256::MIN));
        assert_eq!(parse_bignumeric(I256_MAX_AT_38).ok(), Some(i256::MAX));
        assert_eq!(
            error_kind(parse_bignumeric(
                "578960446186580977117854925043439539266.34992332820282019728792003956564819968"
            )),
            BigQueryCodecErrorKind::OutOfRange
        );
        assert_eq!(
            parse_decimal("123.45", 9).ok(),
            Some(i256::from_i128(123_450_000_000))
        );

        assert_eq!(
            decimal_from_f64(1.5, 9).ok(),
            Some(i256::from_i128(1_500_000_000))
        );
        assert_eq!(
            decimal_from_f64(0.1234567894, 9).ok(),
            Some(i256::from_i128(123_456_789))
        );
        assert_eq!(
            error_kind(decimal_from_f64(f64::NAN, 9)),
            BigQueryCodecErrorKind::OutOfRange
        );
        assert_eq!(
            error_kind(decimal_from_f64(f64::INFINITY, 9)),
            BigQueryCodecErrorKind::OutOfRange
        );
    }

    #[test]
    fn decimal_wire_bytes_are_minimal_twos_complement() {
        let wire_bytes = |unscaled: i256| {
            let (little_endian, length) = decimal_le_bytes(unscaled);
            little_endian[..length].to_vec()
        };
        assert_eq!(wire_bytes(i256::from_i128(0)), vec![0]);
        assert_eq!(wire_bytes(i256::from_i128(127)), vec![127]);
        assert_eq!(wire_bytes(i256::from_i128(128)), vec![128, 0]);
        assert_eq!(wire_bytes(i256::from_i128(-1)), vec![0xff]);
        assert_eq!(wire_bytes(i256::from_i128(-129)), vec![0x7f, 0xff]);
        assert_eq!(
            wire_bytes(i256::from_i128(123_456_789_000)),
            vec![0x08, 0x1a, 0x99, 0xbe, 0x1c]
        );
        assert_eq!(wire_bytes(i256::MAX).len(), 32);
        for unscaled in [0, 127, 128, -1, -129, 123_456_789_000, i128::MIN, i128::MAX] {
            let unscaled = i256::from_i128(unscaled);
            assert_eq!(decimal_from_le_bytes(&wire_bytes(unscaled)), unscaled);
        }
        assert_eq!(decimal_from_le_bytes(&wire_bytes(i256::MIN)), i256::MIN);
    }

    #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
    struct Price {
        #[serde(with = "serialize_as_decimal")]
        amount: i128,
        #[serde(with = "serialize_as_optional_decimal")]
        discount: Option<f64>,
    }

    #[test]
    fn decimal_wrapper_is_its_display_text() {
        let text = serde_json::to_string(&BigQueryDecimal(12345i64)).expect("valid test input");
        assert_eq!(text, r#""12345""#);
        assert_eq!(
            serde_json::from_str::<BigQueryDecimal<i64>>(&text).expect("valid test input"),
            BigQueryDecimal(12345)
        );
        assert!(serde_json::from_str::<BigQueryDecimal<i64>>(r#""1.5""#).is_err());

        let price = Price {
            amount: -7,
            discount: Some(0.5),
        };
        let text = serde_json::to_string(&price).expect("valid test input");
        assert_eq!(text, r#"{"amount":"-7","discount":"0.5"}"#);
        assert_eq!(
            serde_json::from_str::<Price>(&text).expect("valid test input"),
            price
        );
        let none = Price {
            discount: None,
            ..price
        };
        let text = serde_json::to_string(&none).expect("valid test input");
        assert_eq!(text, r#"{"amount":"-7","discount":null}"#);
        assert_eq!(
            serde_json::from_str::<Price>(&text).expect("valid test input"),
            none
        );
    }
}
