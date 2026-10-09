//! The call a `when_query` matcher sees, and the parameter values it reads back in serde
//! terms.

use crate::types::error::CodecError;
use crate::BigQueryInterval;
use base64::Engine;
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
    /// [`BigQueryRange`](crate::BigQueryRange), the temporal types and NUMERIC as their wrappers
    /// or text. Names compare ignoring ASCII case, as in BigQuery.
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
                &super::rules::ShownParameters(self.parameters).to_string(),
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
    Sequence(Vec<ParamValue>),
    Struct(Vec<(String, ParamValue)>),
}

/// The value of a parameter of `ty`, the inverse of the encoding of `query::params`. A scalar
/// without a value is NULL; BYTES is a sequence of bytes, INTERVAL a struct of its three parts
/// and RANGE a struct of `start` and `end`.
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
                    "FLOAT64" => match text {
                        "NaN" => Ok(f64::NAN),
                        "Infinity" => Ok(f64::INFINITY),
                        "-Infinity" => Ok(f64::NEG_INFINITY),
                        text => text.parse().map_err(|_| malformed(kind)),
                    }
                    .map(ParamValue::Float64),
                    "BYTES" => base64::engine::general_purpose::STANDARD
                        .decode(text)
                        .map(|bytes| {
                            ParamValue::Sequence(
                                bytes
                                    .into_iter()
                                    .map(|byte| ParamValue::Int64(byte.into()))
                                    .collect(),
                            )
                        })
                        .map_err(|_| malformed(kind)),
                    "INTERVAL" => {
                        let interval = BigQueryInterval::parse_bq(text)?;
                        Ok(ParamValue::Struct(vec![
                            ("months".into(), ParamValue::Int64(interval.months.into())),
                            ("days".into(), ParamValue::Int64(interval.days.into())),
                            ("nanos".into(), ParamValue::Int64(interval.nanos)),
                        ]))
                    }
                    _ => Ok(ParamValue::Text(text.to_string())),
                }
            }
        }
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
            ParamValue::Text(value) => visitor.visit_string(value),
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

    /// A unit variant from its name, as a STRING parameter carries a serialized enum.
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
            other => other.deserialize_any(visitor),
        }
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string bytes byte_buf
        unit unit_struct seq tuple tuple_struct map struct identifier ignored_any
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
    use crate::query::{infer_param, ParamLabel};
    use crate::{
        BigQueryDateTime, BigQueryDecimal, BigQueryInterval, BigQueryJson, BigQueryTime,
        BigQueryTimestamp,
    };
    use serde::Serialize;

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
}
