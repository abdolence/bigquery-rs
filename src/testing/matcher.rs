//! The call a `when_query` matcher sees, and the parameter values it reads back in serde
//! terms.

use crate::query::{bytes_from_base64, float_from_text};
use crate::types::error::CodecError;
use crate::BigQueryInterval;
use gcloud_sdk::google::cloud::bigquery::v2::{
    QueryParameter, QueryParameterType, QueryParameterValue,
};
use serde::de::value::{Error as ValueError, MapDeserializer, SeqDeserializer};
use serde::de::{DeserializeOwned, IntoDeserializer, Visitor};
use serde::Deserializer;
use std::fmt::{Debug, Formatter};

/// A query or job call as a [`when_query`](super::BigQueryFake::when_query) matcher sees it:
/// the SQL text and the parameters, read back in serde terms.
#[derive(Clone, Copy)]
pub struct BigQueryFakeQuery<'a> {
    sql: &'a str,
    parameters: &'a [QueryParameter],
}

impl<'a> BigQueryFakeQuery<'a> {
    pub(super) fn new(sql: &'a str, parameters: &'a [QueryParameter]) -> Self {
        Self { sql, parameters }
    }

    /// The SQL text, as the code under test sent it.
    pub fn sql(&self) -> &'a str {
        self.sql
    }

    /// The named parameter `@name` read as `T`, the way a column of the parameter's type reads
    /// into a row field: a STRUCT as a struct or map, an ARRAY as a sequence, a RANGE as a
    /// [`BigQueryRange`](crate::BigQueryRange), NUMERIC as its wrapper, text, a float or a
    /// whole number, JSON as its document or text, INTERVAL as its wrapper or text, and the
    /// temporal types as their wrappers or text. Four reads differ from a column's: a temporal
    /// parameter does not read as an integer, an INT64 parameter also reads as a float, BYTES
    /// does not read as text, and JSON `null` read as an `Option` of a type that cannot hold
    /// `null` makes the whole read `None`. Names compare ignoring ASCII case, as in BigQuery.
    ///
    /// `None` when the call has no such parameter, and when its value does not read as `T`.
    pub fn param<T: DeserializeOwned>(&self, name: &str) -> Option<T> {
        self.parameters
            .iter()
            .find(|parameter| {
                !parameter.name.is_empty() && parameter.name.eq_ignore_ascii_case(name)
            })
            .and_then(Self::decode)
    }

    /// The positional parameter `?` at `index`, the first being 0, read as `T` as
    /// [`param`](Self::param) reads a named one.
    ///
    /// `None` when the call has no positional parameter at `index`, and when its value does not
    /// read as `T`.
    pub fn positional_param<T: DeserializeOwned>(&self, index: usize) -> Option<T> {
        self.parameters
            .iter()
            .filter(|parameter| parameter.name.is_empty())
            .nth(index)
            .and_then(Self::decode)
    }

    fn decode<T: DeserializeOwned>(parameter: &QueryParameter) -> Option<T> {
        let parameter_type = parameter.parameter_type.as_ref()?;
        let default_value = QueryParameterValue::default();
        let parameter_value = parameter.parameter_value.as_ref().unwrap_or(&default_value);
        let value = ParamValue::try_from((parameter_type, parameter_value)).ok()?;
        T::deserialize(value).ok()
    }
}

impl Debug for BigQueryFakeQuery<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BigQueryFakeQuery")
            .field("sql", &self.sql)
            .field(
                "parameters",
                &format_args!("{}", super::rules::ShownParameters(self.parameters)),
            )
            .finish()
    }
}

/// A parameter value in the serde data model, as the row decoder presents the same value of a
/// column.
#[derive(Debug, Clone, PartialEq)]
enum ParamValue {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    Text(String),
    /// NUMERIC or BIGNUMERIC, by its decimal text.
    Decimal(String),
    /// JSON, by its document text.
    Json(String),
    Interval(BigQueryInterval),
    Sequence(Vec<ParamValue>),
    Struct(Vec<(String, ParamValue)>),
}

/// The value of a parameter of `ty`, the inverse of the encoding of `query::params`. A scalar
/// without a value is NULL, and so is a STRUCT without field values, since the encoder writes
/// every field of a non-NULL one; BYTES is a sequence of bytes and RANGE a struct of `start`
/// and `end`.
impl TryFrom<(&QueryParameterType, &QueryParameterValue)> for ParamValue {
    type Error = CodecError;

    fn try_from(
        (ty, value): (&QueryParameterType, &QueryParameterValue),
    ) -> Result<Self, Self::Error> {
        let malformed =
            |kind: &str| CodecError::type_mismatch(format!("{value:?} is not a {kind} value"));
        match ty.r#type.as_str() {
            "ARRAY" => {
                let element = ty.array_type.as_deref().ok_or_else(|| malformed("ARRAY"))?;
                value
                    .array_values
                    .iter()
                    .enumerate()
                    .map(|(index, item)| {
                        ParamValue::try_from((element, item)).map_err(|err| err.at_index(index))
                    })
                    .collect::<Result<_, _>>()
                    .map(ParamValue::Sequence)
            }
            "STRUCT" if value.struct_values.is_empty() && !ty.struct_types.is_empty() => {
                Ok(ParamValue::Null)
            }
            "STRUCT" => {
                let absent = QueryParameterValue::default();
                ty.struct_types
                    .iter()
                    .map(|field| {
                        let field_type =
                            field.r#type.as_ref().ok_or_else(|| malformed("STRUCT"))?;
                        let field_value = value.struct_values.get(&field.name).unwrap_or(&absent);
                        ParamValue::try_from((field_type, field_value))
                            .map(|decoded| (field.name.clone(), decoded))
                            .map_err(|err| err.at_field(&field.name))
                    })
                    .collect::<Result<_, _>>()
                    .map(ParamValue::Struct)
            }
            "RANGE" => {
                let Some(range) = value.range_value.as_deref() else {
                    return Ok(ParamValue::Null);
                };
                let element = ty
                    .range_element_type
                    .as_deref()
                    .ok_or_else(|| malformed("RANGE"))?;
                let bound = |bound: &Option<Box<QueryParameterValue>>, name: &str| {
                    bound
                        .as_deref()
                        .map_or(Ok(ParamValue::Null), |bound| {
                            ParamValue::try_from((element, bound))
                        })
                        .map(|decoded| (name.to_string(), decoded))
                        .map_err(|err| err.at_field(name))
                };
                Ok(ParamValue::Struct(vec![
                    bound(&range.start, "start")?,
                    bound(&range.end, "end")?,
                ]))
            }
            kind => {
                let Some(text) = value.value.as_deref() else {
                    return Ok(ParamValue::Null);
                };
                match kind {
                    "BOOL" => text
                        .parse()
                        .map(ParamValue::Bool)
                        .map_err(|_| malformed(kind)),
                    "INT64" => text
                        .parse()
                        .map(ParamValue::Int64)
                        .map_err(|_| malformed(kind)),
                    "FLOAT64" => float_from_text(text)
                        .map(ParamValue::Float64)
                        .ok_or_else(|| malformed(kind)),
                    "BYTES" => bytes_from_base64(text)
                        .map(|bytes| {
                            ParamValue::Sequence(
                                bytes
                                    .into_iter()
                                    .map(|byte| ParamValue::Int64(byte.into()))
                                    .collect(),
                            )
                        })
                        .ok_or_else(|| malformed(kind)),
                    "INTERVAL" => BigQueryInterval::parse_bq(text).map(ParamValue::Interval),
                    "NUMERIC" | "BIGNUMERIC" => Ok(ParamValue::Decimal(text.to_string())),
                    "JSON" => Ok(ParamValue::Json(text.to_string())),
                    _ => Ok(ParamValue::Text(text.to_string())),
                }
            }
        }
    }
}

impl ParamValue {
    /// The whole number a decimal holds, `None` when it has a non-zero fractional part.
    fn whole_decimal(text: &str) -> Option<i128> {
        let (whole, fraction) = text.split_once('.').unwrap_or((text, ""));
        fraction
            .bytes()
            .all(|digit| digit == b'0')
            .then(|| whole.parse().ok())
            .flatten()
    }

    /// An integer target: a whole decimal as its number, as a NUMERIC column reads into one.
    fn integer<'de, V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ValueError> {
        let ParamValue::Decimal(text) = &self else {
            return self.deserialize_any(visitor);
        };
        match Self::whole_decimal(text) {
            Some(whole) => {
                if let Ok(integer) = i64::try_from(whole) {
                    visitor.visit_i64(integer)
                } else if let Ok(unsigned) = u64::try_from(whole) {
                    visitor.visit_u64(unsigned)
                } else {
                    visitor.visit_i128(whole)
                }
            }
            None => self.deserialize_any(visitor),
        }
    }

    /// JSON text as the self-describing value it holds.
    fn json_document(text: &str) -> Result<serde_json::Value, ValueError> {
        serde_json::from_str(text).map_err(serde::de::Error::custom)
    }
}

impl<'de> Deserializer<'de> for ParamValue {
    type Error = ValueError;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ValueError> {
        match self {
            ParamValue::Null => visitor.visit_unit(),
            ParamValue::Bool(value) => visitor.visit_bool(value),
            ParamValue::Int64(value) => visitor.visit_i64(value),
            ParamValue::Float64(value) => visitor.visit_f64(value),
            ParamValue::Text(value) | ParamValue::Decimal(value) => visitor.visit_string(value),
            ParamValue::Json(text) => Self::json_document(&text)?
                .deserialize_any(visitor)
                .map_err(serde::de::Error::custom),
            ParamValue::Interval(interval) => {
                let mut text = String::new();
                interval.write_bq(&mut text);
                visitor.visit_string(text)
            }
            ParamValue::Sequence(items) => {
                let mut items = SeqDeserializer::new(items.into_iter());
                let value = visitor.visit_seq(&mut items)?;
                items.end()?;
                Ok(value)
            }
            ParamValue::Struct(fields) => {
                let mut fields = MapDeserializer::new(fields.into_iter());
                let value = visitor.visit_map(&mut fields)?;
                fields.end()?;
                Ok(value)
            }
        }
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ValueError> {
        match self {
            ParamValue::Null => visitor.visit_none(),
            value => visitor.visit_some(value),
        }
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, ValueError> {
        visitor.visit_newtype_struct(self)
    }

    /// An enum from a STRING parameter's variant name, or from the document a JSON one holds.
    fn deserialize_enum<V: Visitor<'de>>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, ValueError> {
        match self {
            ParamValue::Text(variant) => variant
                .into_deserializer()
                .deserialize_enum(name, variants, visitor),
            ParamValue::Json(text) => Self::json_document(&text)?
                .deserialize_enum(name, variants, visitor)
                .map_err(serde::de::Error::custom),
            other => other.deserialize_any(visitor),
        }
    }

    /// JSON as its text, as a JSON column reads into a string.
    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ValueError> {
        match self {
            ParamValue::Json(text) => visitor.visit_string(text),
            other => other.deserialize_any(visitor),
        }
    }

    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ValueError> {
        self.deserialize_string(visitor)
    }

    /// INTERVAL as the sequence `(months, days, nanos)`, as an INTERVAL column reads into
    /// [`BigQueryInterval`].
    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ValueError> {
        match self {
            ParamValue::Interval(interval) => ParamValue::Sequence(vec![
                ParamValue::Int64(interval.months.into()),
                ParamValue::Int64(interval.days.into()),
                ParamValue::Int64(interval.nanos),
            ])
            .deserialize_any(visitor),
            other => other.deserialize_any(visitor),
        }
    }

    fn deserialize_tuple<V: Visitor<'de>>(
        self,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value, ValueError> {
        self.deserialize_seq(visitor)
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value, ValueError> {
        self.deserialize_seq(visitor)
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, ValueError> {
        self.deserialize_seq(visitor)
    }

    /// A decimal as the float its text parses to, as a NUMERIC column reads into one.
    fn deserialize_f64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ValueError> {
        match self {
            ParamValue::Decimal(text) => match text.parse() {
                Ok(float) => visitor.visit_f64(float),
                Err(_) => ParamValue::Decimal(text).deserialize_any(visitor),
            },
            other => other.deserialize_any(visitor),
        }
    }

    fn deserialize_f32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ValueError> {
        self.deserialize_f64(visitor)
    }

    fn deserialize_i8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ValueError> {
        self.integer(visitor)
    }

    fn deserialize_i16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ValueError> {
        self.integer(visitor)
    }

    fn deserialize_i32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ValueError> {
        self.integer(visitor)
    }

    fn deserialize_i64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ValueError> {
        self.integer(visitor)
    }

    fn deserialize_i128<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ValueError> {
        self.integer(visitor)
    }

    fn deserialize_u8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ValueError> {
        self.integer(visitor)
    }

    fn deserialize_u16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ValueError> {
        self.integer(visitor)
    }

    fn deserialize_u32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ValueError> {
        self.integer(visitor)
    }

    fn deserialize_u64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ValueError> {
        self.integer(visitor)
    }

    fn deserialize_u128<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ValueError> {
        self.integer(visitor)
    }

    serde::forward_to_deserialize_any! {
        bool char bytes byte_buf unit unit_struct map identifier ignored_any
    }
}

impl<'de> IntoDeserializer<'de, ValueError> for ParamValue {
    type Deserializer = Self;

    fn into_deserializer(self) -> Self {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::{infer_param, typed_param, ParamLabel};
    use crate::{
        BigQueryDateTime, BigQueryDecimal, BigQueryFieldMode, BigQueryFieldSchema,
        BigQueryFieldType, BigQueryInterval, BigQueryJson, BigQueryTime, BigQueryTimestamp,
    };
    use serde::{Deserialize, Serialize};

    /// `value` encoded as the client encodes the named parameter `@value`, then read back as
    /// `T`.
    fn round_trip<V: Serialize, T: DeserializeOwned>(value: &V) -> Option<T> {
        let parameter = infer_param(ParamLabel::Named("value"), value).expect("the value encodes");
        BigQueryFakeQuery::new("SELECT @value", &[parameter]).param("value")
    }

    #[test]
    fn scalars_read_back_as_they_were_bound() {
        let timestamp = BigQueryTimestamp("2024-03-01T12:30:00.25Z".parse().expect("valid"));
        let datetime = BigQueryDateTime(jiff::civil::datetime(2024, 3, 1, 12, 30, 0, 250_000_000));
        let time = BigQueryTime(jiff::civil::time(12, 30, 0, 250_000_000));
        let decimal = BigQueryDecimal(12.5_f64);
        let interval = BigQueryInterval {
            months: -14,
            days: 3,
            nanos: 1_500_000_000,
        };
        let json = BigQueryJson(vec![1, 2]);

        assert_eq!(round_trip(&true), Some(true));
        assert_eq!(round_trip(&-7_i64), Some(-7_i64));
        assert_eq!(round_trip(&0.5_f64), Some(0.5_f64));
        assert!(round_trip::<_, f64>(&f64::NAN).is_some_and(f64::is_nan));
        assert_eq!(
            round_trip(&serde_bytes::ByteBuf::from(b"r-1".to_vec())),
            Some(b"r-1".to_vec())
        );
        assert_eq!(round_trip(&timestamp), Some(timestamp));
        assert_eq!(round_trip(&datetime), Some(datetime));
        assert_eq!(round_trip(&time), Some(time));
        assert_eq!(round_trip(&decimal), Some(decimal));
        assert_eq!(round_trip(&interval), Some(interval));
        assert_eq!(round_trip(&json), Some(json));
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Buyer {
        customer: Option<String>,
    }

    #[test]
    fn a_null_struct_reads_as_none() {
        let buyer = BigQueryFieldType::Struct(vec![BigQueryFieldSchema {
            name: "customer".into(),
            field_type: BigQueryFieldType::String { max_length: None },
            mode: BigQueryFieldMode::Nullable,
            description: None,
            default_value_expression: None,
        }]);
        let parameter = typed_param(ParamLabel::Named("buyer"), &buyer.into(), &None::<Buyer>)
            .expect("NULL encodes as any type");

        let read: Option<Option<Buyer>> =
            BigQueryFakeQuery::new("SELECT @buyer", &[parameter]).param("buyer");

        assert_eq!(read, Some(None));
    }

    #[test]
    fn an_interval_reads_into_a_string_as_its_text() {
        let interval = BigQueryInterval {
            months: 14,
            days: 3,
            nanos: 1_500_000_000,
        };

        assert_eq!(
            round_trip(&interval),
            Some("1-2 3 0:0:1.500000".to_string())
        );
    }

    #[test]
    fn json_reads_into_a_json_value_as_its_document() {
        let document = serde_json::json!({"tier": "gold", "points": 3});

        assert_eq!(round_trip(&BigQueryJson(&document)), Some(document));
    }

    #[test]
    fn a_numeric_reads_into_a_float_and_a_whole_one_into_an_integer() {
        assert_eq!(round_trip(&BigQueryDecimal(12.5_f64)), Some(12.5_f64));
        assert_eq!(round_trip(&BigQueryDecimal(12_i64)), Some(12_i64));
        assert_eq!(round_trip::<_, i64>(&BigQueryDecimal(12.5_f64)), None);
    }

    #[test]
    fn a_whole_numeric_beyond_i64_reads_into_u64() {
        let beyond_i64 = BigQueryDecimal("9223372036854775808".to_string());
        assert_eq!(round_trip(&beyond_i64), Some(9_223_372_036_854_775_808_u64));
    }
}
