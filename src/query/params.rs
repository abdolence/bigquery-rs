//! Query parameters: serde values encoded as `QueryParameter`s, with the type inferred from the
//! serde form or declared by the caller.
//!
//! A value is first serialized into a [`SerializedValue`], a tree that keeps what serde said
//! about it: the crate's wrappers are recognised there by their serde names. Inference reads the
//! type off the tree; a declared type accepts the forms the write path accepts for that column
//! type.

use crate::errors::{BigQueryCodecErrorKind, BigQueryError};
use crate::sql::{dotted_path, is_identifier, SqlLiteral};
use crate::types::civil;
use crate::types::decimal::{
    decimal_string, parse_bignumeric, parse_numeric, BIGNUMERIC_SCALE, NUMERIC_SCALE, TAG_DECIMAL,
};
use crate::types::error::CodecError;
use crate::types::interval::TAG_INTERVAL;
use crate::types::json::TAG_JSON;
use crate::types::kind::FieldKind;
use crate::types::range::TAG_RANGE;
use crate::types::temporal::{capture_integer, temporal_tag_kind};
use crate::{
    BigQueryFieldMode, BigQueryFieldType, BigQueryInterval, BigQueryParamType,
    BigQueryRangeElementType, BigQueryResult,
};
use base64::Engine;
use gcloud_sdk::google::cloud::bigquery::v2::{
    QueryParameter, QueryParameterStructType, QueryParameterType, QueryParameterValue, RangeValue,
};
use serde::ser::{self, Impossible, Serialize};

/// How a parameter is addressed in the statement.
#[derive(Clone, Copy, Debug)]
pub(crate) enum ParamLabel<'a> {
    /// `@name`.
    Named(&'a str),
    /// The zero-based position of a `?`.
    Positional(usize),
}

impl ParamLabel<'_> {
    fn name(self) -> String {
        match self {
            ParamLabel::Named(name) => name.to_string(),
            ParamLabel::Positional(_) => String::new(),
        }
    }

    /// How errors name the parameter: its name, or `positional parameter <n>` counted from 1.
    fn describe(self) -> String {
        match self {
            ParamLabel::Named(name) => name.to_string(),
            ParamLabel::Positional(index) => format!("positional parameter {}", index + 1),
        }
    }

    fn locate(self, err: CodecError) -> CodecError {
        match self {
            ParamLabel::Named(name) => err.at_field(name),
            ParamLabel::Positional(index) => err.at_index(index),
        }
    }

    /// Refuses a name that is not a GoogleSQL identifier: ASCII letters, digits and
    /// underscores, not starting with a digit. An empty name would be sent as a positional
    /// parameter, and any other name could not be written as `@name` in the statement.
    fn check(self) -> Result<Self, BigQueryError> {
        let ParamLabel::Named(name) = self else {
            return Ok(self);
        };
        if is_identifier(name.as_bytes()) {
            Ok(self)
        } else {
            Err(BigQueryError::invalid_parameters(
                name.to_string(),
                format!(
                    "{name:?} is not a parameter name: a GoogleSQL identifier of ASCII letters, \
                     digits and underscores, not starting with a digit"
                ),
            ))
        }
    }
}

/// Encodes `value` with its type inferred from its serde form.
pub(crate) fn infer_param<V: Serialize + ?Sized>(
    label: ParamLabel,
    value: &V,
) -> Result<QueryParameter, BigQueryError> {
    let label = label.check()?;
    let node = value
        .serialize(CaptureSerializer)
        .map_err(|error| label.locate(error).into_serialize())?;
    node.infer(label)
}

/// The SQL literal of `value`, its type inferred as for a parameter, or `None` when `value`
/// is NULL. `field` names the value in errors.
pub(crate) fn literal_of<V: Serialize + ?Sized>(
    field: &str,
    value: &V,
) -> Result<Option<SqlLiteral>, BigQueryError> {
    let label = ParamLabel::Named(field);
    let node = value
        .serialize(CaptureSerializer)
        .map_err(|error| label.locate(error).into_serialize())?;
    if node == SerializedValue::Null {
        return Ok(None);
    }
    let (param_type, value) = node.infer_at(String::new()).map_err(|error| match error {
        InferError::Codec(err) => label.locate(err).into_serialize(),
        InferError::Untyped { path, what } => BigQueryError::invalid_parameters(
            field.to_string(),
            InferError::untyped_description(&path, what),
        ),
    })?;
    SqlLiteral::try_from((&param_type, &value))
        .map(Some)
        .map_err(|error| label.locate(error).into_serialize())
}

/// Encodes `value` as a parameter of type `param_type`. `None` is a NULL of that type.
pub(crate) fn typed_param<V: Serialize + ?Sized>(
    label: ParamLabel,
    param_type: &BigQueryParamType,
    value: &V,
) -> Result<QueryParameter, BigQueryError> {
    let label = label.check()?;
    let node = value
        .serialize(CaptureSerializer)
        .map_err(|error| label.locate(error).into_serialize())?;
    let mode = if param_type.repeated {
        BigQueryFieldMode::Repeated
    } else {
        BigQueryFieldMode::Nullable
    };
    let parameter_value = node
        .coerce_field(&param_type.field_type, mode)
        .map_err(|error| label.locate(error).into_serialize())?;
    Ok(QueryParameter {
        name: label.name(),
        parameter_type: Some(param_type.field_type.param_type(mode)),
        parameter_value: Some(parameter_value),
    })
}

/// Encodes each top-level field of a struct or string-keyed map as a named parameter, its type
/// inferred.
pub(crate) fn struct_params<P: Serialize + ?Sized>(
    params: &P,
) -> Result<Vec<QueryParameter>, BigQueryError> {
    let node = params.serialize(CaptureSerializer).map_err(|error| {
        BigQueryError::invalid_parameters(
            "params",
            format!("the parameters do not serialize: {error}"),
        )
    })?;
    let SerializedValue::Struct(fields) = node else {
        return Err(BigQueryError::invalid_parameters(
            "params",
            "params takes a struct or a map with string keys",
        ));
    };
    fields
        .iter()
        .map(|(name, node)| node.infer(ParamLabel::Named(name)))
        .collect()
}

/// The `parameter_mode` that `params` need: `NAMED`, `POSITIONAL`, or empty for none.
pub(crate) fn parameter_mode(params: &[QueryParameter]) -> BigQueryResult<&'static str> {
    let named = params.iter().filter(|param| !param.name.is_empty()).count();
    match (named, params.len()) {
        (_, 0) => Ok(""),
        (named, total) if named == total => Ok("NAMED"),
        (0, _) => Ok("POSITIONAL"),
        _ => Err(BigQueryError::invalid_parameters(
            "query_parameters",
            "named and positional parameters cannot be used in one query",
        )),
    }
}

/// The parameters a builder collects, encoded as they are added, and the first one that failed
/// to encode. Builders never fail, so the failure waits for the terminal, which returns it
/// without sending anything.
#[derive(Clone, Debug, Default)]
pub(crate) struct ParamList {
    parameters: Vec<QueryParameter>,
    failure: Option<BigQueryError>,
}

impl ParamList {
    /// Adds `@name`, its type inferred from the value's serde form.
    pub(crate) fn named<V: Serialize + ?Sized>(&mut self, name: &str, value: &V) {
        let encoded = infer_param(ParamLabel::Named(name), value);
        self.push(encoded);
    }

    /// Adds `@name` of the declared type `param_type`.
    pub(crate) fn named_as<V: Serialize + ?Sized>(
        &mut self,
        name: &str,
        param_type: &BigQueryParamType,
        value: &V,
    ) {
        let encoded = typed_param(ParamLabel::Named(name), param_type, value);
        self.push(encoded);
    }

    /// Adds every top-level field of a struct or string-keyed map as a named parameter.
    pub(crate) fn fields<P: Serialize + ?Sized>(&mut self, params: &P) {
        match struct_params(params) {
            Ok(encoded) => encoded.into_iter().for_each(|param| self.push(Ok(param))),
            Err(failure) => self.push(Err(failure)),
        }
    }

    /// Adds the next positional `?`, its type inferred.
    pub(crate) fn positional<V: Serialize + ?Sized>(&mut self, value: &V) {
        let encoded = infer_param(self.next_position(), value);
        self.push(encoded);
    }

    /// Adds the next positional `?` of the declared type `param_type`.
    pub(crate) fn positional_as<V: Serialize + ?Sized>(
        &mut self,
        param_type: &BigQueryParamType,
        value: &V,
    ) {
        let encoded = typed_param(self.next_position(), param_type, value);
        self.push(encoded);
    }

    /// The parameters in the order they were added.
    ///
    /// # Errors
    /// The first failure to encode one, unchanged.
    pub(crate) fn into_parameters(self) -> BigQueryResult<Vec<QueryParameter>> {
        match self.failure {
            Some(failure) => Err(failure),
            None => Ok(self.parameters),
        }
    }

    fn next_position(&self) -> ParamLabel<'static> {
        ParamLabel::Positional(self.parameters.len())
    }

    fn push(&mut self, encoded: Result<QueryParameter, BigQueryError>) {
        match encoded {
            Ok(parameter) if self.failure.is_none() => self.parameters.push(parameter),
            Ok(_) => {}
            Err(failure) => {
                self.failure.get_or_insert(failure);
            }
        }
    }
}

/// A serialized value with what serde said about it.
#[derive(Clone, Debug, PartialEq)]
enum SerializedValue {
    Null,
    Bool(bool),
    Integer(i128),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    /// A temporal wrapper: its kind and BigQuery integer.
    Temporal {
        kind: FieldKind,
        value: i64,
    },
    /// `BigQueryJson`: the JSON text.
    Json(String),
    /// `BigQueryDecimal`: the decimal text.
    Decimal(String),
    Interval(BigQueryInterval),
    /// `BigQueryRange`: `Null` for an unbounded end.
    Range {
        start: Box<SerializedValue>,
        end: Box<SerializedValue>,
    },
    Sequence(Vec<SerializedValue>),
    /// A struct or a string-keyed map, its entries in serialization order.
    Struct(Vec<(String, SerializedValue)>),
}

struct CaptureSerializer;

impl ser::Serializer for CaptureSerializer {
    type Ok = SerializedValue;
    type Error = CodecError;
    type SerializeSeq = CapturedSequence;
    type SerializeTuple = CapturedSequence;
    type SerializeTupleStruct = CapturedSequence;
    type SerializeTupleVariant = Impossible<SerializedValue, CodecError>;
    type SerializeMap = CapturedMap;
    type SerializeStruct = CapturedStruct;
    type SerializeStructVariant = Impossible<SerializedValue, CodecError>;

    fn serialize_bool(self, value: bool) -> Result<SerializedValue, CodecError> {
        Ok(SerializedValue::Bool(value))
    }

    fn serialize_i8(self, value: i8) -> Result<SerializedValue, CodecError> {
        Ok(SerializedValue::Integer(value.into()))
    }

    fn serialize_i16(self, value: i16) -> Result<SerializedValue, CodecError> {
        Ok(SerializedValue::Integer(value.into()))
    }

    fn serialize_i32(self, value: i32) -> Result<SerializedValue, CodecError> {
        Ok(SerializedValue::Integer(value.into()))
    }

    fn serialize_i64(self, value: i64) -> Result<SerializedValue, CodecError> {
        Ok(SerializedValue::Integer(value.into()))
    }

    fn serialize_i128(self, value: i128) -> Result<SerializedValue, CodecError> {
        Ok(SerializedValue::Integer(value))
    }

    fn serialize_u8(self, value: u8) -> Result<SerializedValue, CodecError> {
        Ok(SerializedValue::Integer(value.into()))
    }

    fn serialize_u16(self, value: u16) -> Result<SerializedValue, CodecError> {
        Ok(SerializedValue::Integer(value.into()))
    }

    fn serialize_u32(self, value: u32) -> Result<SerializedValue, CodecError> {
        Ok(SerializedValue::Integer(value.into()))
    }

    fn serialize_u64(self, value: u64) -> Result<SerializedValue, CodecError> {
        Ok(SerializedValue::Integer(value.into()))
    }

    fn serialize_u128(self, value: u128) -> Result<SerializedValue, CodecError> {
        i128::try_from(value)
            .map(SerializedValue::Integer)
            .map_err(|_| CodecError::out_of_range(format!("{value} is above INT64")))
    }

    fn serialize_f32(self, value: f32) -> Result<SerializedValue, CodecError> {
        Ok(SerializedValue::Float(value.into()))
    }

    fn serialize_f64(self, value: f64) -> Result<SerializedValue, CodecError> {
        Ok(SerializedValue::Float(value))
    }

    fn serialize_char(self, value: char) -> Result<SerializedValue, CodecError> {
        Ok(SerializedValue::String(value.to_string()))
    }

    fn serialize_str(self, value: &str) -> Result<SerializedValue, CodecError> {
        Ok(SerializedValue::String(value.to_string()))
    }

    fn serialize_bytes(self, value: &[u8]) -> Result<SerializedValue, CodecError> {
        Ok(SerializedValue::Bytes(value.to_vec()))
    }

    fn serialize_none(self) -> Result<SerializedValue, CodecError> {
        Ok(SerializedValue::Null)
    }

    fn serialize_some<T: Serialize + ?Sized>(
        self,
        value: &T,
    ) -> Result<SerializedValue, CodecError> {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<SerializedValue, CodecError> {
        Ok(SerializedValue::Null)
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<SerializedValue, CodecError> {
        Ok(SerializedValue::Null)
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
    ) -> Result<SerializedValue, CodecError> {
        Ok(SerializedValue::String(variant.to_string()))
    }

    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<SerializedValue, CodecError> {
        if let Some(kind) = temporal_tag_kind(name) {
            return Ok(SerializedValue::Temporal {
                kind,
                value: capture_integer(value)?,
            });
        }
        match name {
            TAG_JSON => match value.serialize(self)? {
                SerializedValue::String(text) => Ok(SerializedValue::Json(text)),
                other => Err(CodecError::type_mismatch(format!(
                    "JSON text expected, got {}",
                    other.describe()
                ))),
            },
            TAG_DECIMAL => match value.serialize(self)? {
                SerializedValue::String(text) => Ok(SerializedValue::Decimal(text)),
                other => Err(CodecError::type_mismatch(format!(
                    "decimal text expected, got {}",
                    other.describe()
                ))),
            },
            _ => value.serialize(self),
        }
    }

    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        name: &'static str,
        _index: u32,
        variant: &'static str,
        _value: &T,
    ) -> Result<SerializedValue, CodecError> {
        Err(CodecError::type_mismatch(format!(
            "the enum variant {name}::{variant} holds data, which no parameter type takes"
        )))
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<CapturedSequence, CodecError> {
        Ok(CapturedSequence(Vec::with_capacity(
            len.unwrap_or_default(),
        )))
    }

    fn serialize_tuple(self, len: usize) -> Result<CapturedSequence, CodecError> {
        Ok(CapturedSequence(Vec::with_capacity(len)))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<CapturedSequence, CodecError> {
        Ok(CapturedSequence(Vec::with_capacity(len)))
    }

    fn serialize_tuple_variant(
        self,
        name: &'static str,
        _index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, CodecError> {
        Err(CodecError::type_mismatch(format!(
            "the enum variant {name}::{variant} holds data, which no parameter type takes"
        )))
    }

    fn serialize_map(self, len: Option<usize>) -> Result<CapturedMap, CodecError> {
        Ok(CapturedMap {
            fields: Vec::with_capacity(len.unwrap_or_default()),
            key: None,
        })
    }

    fn serialize_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<CapturedStruct, CodecError> {
        Ok(CapturedStruct {
            name,
            fields: Vec::with_capacity(len),
        })
    }

    fn serialize_struct_variant(
        self,
        name: &'static str,
        _index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, CodecError> {
        Err(CodecError::type_mismatch(format!(
            "the enum variant {name}::{variant} holds data, which no parameter type takes"
        )))
    }
}

struct CapturedSequence(Vec<SerializedValue>);

impl CapturedSequence {
    fn push<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), CodecError> {
        let index = self.0.len();
        self.0.push(
            value
                .serialize(CaptureSerializer)
                .map_err(|error| error.at_index(index))?,
        );
        Ok(())
    }
}

impl ser::SerializeSeq for CapturedSequence {
    type Ok = SerializedValue;
    type Error = CodecError;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), CodecError> {
        self.push(value)
    }

    fn end(self) -> Result<SerializedValue, CodecError> {
        Ok(SerializedValue::Sequence(self.0))
    }
}

impl ser::SerializeTuple for CapturedSequence {
    type Ok = SerializedValue;
    type Error = CodecError;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), CodecError> {
        self.push(value)
    }

    fn end(self) -> Result<SerializedValue, CodecError> {
        Ok(SerializedValue::Sequence(self.0))
    }
}

impl ser::SerializeTupleStruct for CapturedSequence {
    type Ok = SerializedValue;
    type Error = CodecError;

    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), CodecError> {
        self.push(value)
    }

    fn end(self) -> Result<SerializedValue, CodecError> {
        Ok(SerializedValue::Sequence(self.0))
    }
}

struct CapturedMap {
    fields: Vec<(String, SerializedValue)>,
    key: Option<String>,
}

impl ser::SerializeMap for CapturedMap {
    type Ok = SerializedValue;
    type Error = CodecError;

    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<(), CodecError> {
        match key.serialize(CaptureSerializer)? {
            SerializedValue::String(key) => {
                self.key = Some(key);
                Ok(())
            }
            other => Err(CodecError::type_mismatch(format!(
                "a map parameter needs string keys, got {}",
                other.describe()
            ))),
        }
    }

    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), CodecError> {
        let key = self.key.take().unwrap_or_default();
        let node = value
            .serialize(CaptureSerializer)
            .map_err(|error| error.at_field(&key))?;
        self.fields.push((key, node));
        Ok(())
    }

    fn end(self) -> Result<SerializedValue, CodecError> {
        Ok(SerializedValue::Struct(self.fields))
    }
}

struct CapturedStruct {
    name: &'static str,
    fields: Vec<(String, SerializedValue)>,
}

impl CapturedStruct {
    fn take_field(&mut self, name: &str) -> SerializedValue {
        self.fields
            .iter()
            .position(|(field_name, _)| field_name == name)
            .map(|index| self.fields.remove(index).1)
            .unwrap_or(SerializedValue::Null)
    }

    fn interval_part<T: TryFrom<i128>>(&mut self, name: &str) -> Result<T, CodecError> {
        match self.take_field(name) {
            SerializedValue::Integer(value) => T::try_from(value).map_err(|_| {
                CodecError::out_of_range(format!("INTERVAL {name} {value} is out of range"))
            }),
            other => Err(CodecError::type_mismatch(format!(
                "INTERVAL {name} must be an integer, got {}",
                other.describe()
            ))
            .at_field(name)),
        }
    }
}

impl ser::SerializeStruct for CapturedStruct {
    type Ok = SerializedValue;
    type Error = CodecError;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), CodecError> {
        let node = value
            .serialize(CaptureSerializer)
            .map_err(|error| error.at_field(key))?;
        self.fields.push((key.to_string(), node));
        Ok(())
    }

    fn end(mut self) -> Result<SerializedValue, CodecError> {
        match self.name {
            TAG_INTERVAL => Ok(SerializedValue::Interval(BigQueryInterval {
                months: self.interval_part("months")?,
                days: self.interval_part("days")?,
                nanos: self.interval_part("nanos")?,
            })),
            TAG_RANGE => {
                let start = self.take_field("start");
                let end = self.take_field("end");
                Ok(SerializedValue::Range {
                    start: Box::new(start),
                    end: Box::new(end),
                })
            }
            _ => Ok(SerializedValue::Struct(self.fields)),
        }
    }
}

impl From<BigQueryRangeElementType> for FieldKind {
    fn from(element: BigQueryRangeElementType) -> Self {
        match element {
            BigQueryRangeElementType::Date => FieldKind::Date,
            BigQueryRangeElementType::DateTime => FieldKind::DateTime,
            BigQueryRangeElementType::Timestamp => FieldKind::Timestamp,
        }
    }
}

/// Why inference failed: a value that has no form of its type, or one whose type the value
/// does not tell.
enum InferError {
    Codec(CodecError),
    Untyped { path: String, what: &'static str },
}

impl From<CodecError> for InferError {
    fn from(err: CodecError) -> Self {
        InferError::Codec(err)
    }
}

impl InferError {
    /// Why the value of kind `what` at the dotted `path` inside a parameter has no type.
    fn untyped_description(path: &str, what: &str) -> String {
        if path.is_empty() {
            format!("the type of {what} cannot be inferred from its value")
        } else {
            format!("the type of {what} at `{path}` cannot be inferred from its value")
        }
    }

    fn at_field(self, name: &str) -> Self {
        match self {
            InferError::Codec(err) => InferError::Codec(err.at_field(name)),
            untyped => untyped,
        }
    }

    fn at_index(self, index: usize) -> Self {
        match self {
            InferError::Codec(err) => InferError::Codec(err.at_index(index)),
            untyped => untyped,
        }
    }
}

impl FieldKind {
    fn param_type(self) -> QueryParameterType {
        QueryParameterType {
            r#type: self.name().to_string(),
            ..Default::default()
        }
    }
}

fn array_type(element: QueryParameterType) -> QueryParameterType {
    QueryParameterType {
        r#type: "ARRAY".into(),
        array_type: Some(Box::new(element)),
        ..Default::default()
    }
}

impl FieldKind {
    fn range_param_type(self) -> QueryParameterType {
        QueryParameterType {
            r#type: FieldKind::Range.name().to_string(),
            range_element_type: Some(Box::new(self.param_type())),
            ..Default::default()
        }
    }
}

fn text_value(text: String) -> QueryParameterValue {
    QueryParameterValue {
        value: Some(text),
        ..Default::default()
    }
}

type EncodedParameter = (QueryParameterType, QueryParameterValue);

impl SerializedValue {
    /// The value as a parameter addressed by `label`, its type inferred from its serde form.
    fn infer(&self, label: ParamLabel) -> Result<QueryParameter, BigQueryError> {
        let label = label.check()?;
        let (parameter_type, parameter_value) =
            self.infer_at(String::new()).map_err(|error| match error {
                InferError::Codec(err) => label.locate(err).into_serialize(),
                InferError::Untyped { path, what } => BigQueryError::invalid_parameters(
                    label.describe(),
                    format!(
                        "{}; declare it with param_as",
                        InferError::untyped_description(&path, what)
                    ),
                ),
            })?;
        Ok(QueryParameter {
            name: label.name(),
            parameter_type: Some(parameter_type),
            parameter_value: Some(parameter_value),
        })
    }

    fn infer_at(&self, path: String) -> Result<EncodedParameter, InferError> {
        let scalar = |kind: FieldKind, text: String| Ok((kind.param_type(), text_value(text)));
        match self {
            SerializedValue::Null => Err(InferError::Untyped { path, what: "NULL" }),
            SerializedValue::Bool(value) => scalar(FieldKind::Bool, value.to_string()),
            SerializedValue::Integer(value) => scalar(FieldKind::Int64, int64_text(*value)?),
            SerializedValue::Float(value) => scalar(FieldKind::Float64, float_text(*value)),
            SerializedValue::String(value) => scalar(FieldKind::String, value.clone()),
            SerializedValue::Bytes(value) => scalar(FieldKind::Bytes, base64_text(value)),
            SerializedValue::Temporal { kind, value } => scalar(*kind, kind.temporal_text(*value)?),
            SerializedValue::Json(text) => scalar(FieldKind::Json, text.clone()),
            SerializedValue::Decimal(text) => match parse_numeric(text) {
                Ok(value) => scalar(FieldKind::Numeric, decimal_string(value, NUMERIC_SCALE)),
                Err(err) if err.kind() == BigQueryCodecErrorKind::OutOfRange => {
                    let value = parse_bignumeric(text)?;
                    scalar(
                        FieldKind::BigNumeric,
                        decimal_string(value, BIGNUMERIC_SCALE),
                    )
                }
                Err(err) => Err(err.into()),
            },
            SerializedValue::Interval(interval) => {
                scalar(FieldKind::Interval, interval.param_text()?)
            }
            SerializedValue::Range { start, end } => {
                let bound_kind = |bound: &SerializedValue| match bound {
                    SerializedValue::Null => Ok(None),
                    SerializedValue::Temporal { kind, .. }
                        if matches!(
                            kind,
                            FieldKind::Date | FieldKind::DateTime | FieldKind::Timestamp
                        ) =>
                    {
                        Ok(Some(*kind))
                    }
                    _ => Err(InferError::Untyped {
                        path: path.clone(),
                        what: "a RANGE bound that is not a DATE, DATETIME or TIMESTAMP wrapper",
                    }),
                };
                let element = match (bound_kind(start)?, bound_kind(end)?) {
                    (Some(start_kind), Some(end_kind)) if start_kind != end_kind => {
                        return Err(CodecError::type_mismatch(format!(
                            "a RANGE from a {} to a {}",
                            start_kind.name(),
                            end_kind.name()
                        ))
                        .into())
                    }
                    (Some(kind), _) | (None, Some(kind)) => kind,
                    (None, None) => {
                        return Err(InferError::Untyped {
                            path,
                            what: "a RANGE unbounded at both ends",
                        })
                    }
                };
                Ok((element.range_param_type(), element.range_value(start, end)?))
            }
            SerializedValue::Sequence(items) => {
                let mut element_type = None;
                let mut values = Vec::with_capacity(items.len());
                for (index, item) in items.iter().enumerate() {
                    match item {
                        SerializedValue::Sequence(_) => {
                            return Err(CodecError::new(
                                BigQueryCodecErrorKind::UnsupportedType,
                                "an ARRAY of ARRAYs is not a BigQuery type",
                            )
                            .at_index(index)
                            .into())
                        }
                        SerializedValue::Null => {
                            return Err(CodecError::new(
                                BigQueryCodecErrorKind::NullArrayElement,
                                "an ARRAY parameter cannot hold NULL",
                            )
                            .at_index(index)
                            .into())
                        }
                        _ => {}
                    }
                    let (param_type, value) = item
                        .infer_at(format!("{path}[{index}]"))
                        .map_err(|error| error.at_index(index))?;
                    match &element_type {
                        None => element_type = Some(param_type),
                        Some(first) if *first != param_type => {
                            return Err(InferError::Untyped {
                                path,
                                what: "an ARRAY whose elements have different types",
                            })
                        }
                        Some(_) => {}
                    }
                    values.push(value);
                }
                let Some(element_type) = element_type else {
                    return Err(InferError::Untyped {
                        path,
                        what: "an empty ARRAY",
                    });
                };
                Ok((
                    array_type(element_type),
                    QueryParameterValue {
                        array_values: values,
                        ..Default::default()
                    },
                ))
            }
            SerializedValue::Struct(fields) => {
                let mut struct_types = Vec::with_capacity(fields.len());
                let mut struct_values = std::collections::HashMap::with_capacity(fields.len());
                for (name, field) in fields {
                    let (param_type, value) = field
                        .infer_at(dotted_path(&path, name))
                        .map_err(|error| error.at_field(name))?;
                    struct_types.push(QueryParameterStructType {
                        name: name.clone(),
                        r#type: Some(param_type),
                        ..Default::default()
                    });
                    struct_values.insert(name.clone(), value);
                }
                Ok((
                    QueryParameterType {
                        r#type: FieldKind::Struct.name().to_string(),
                        struct_types,
                        ..Default::default()
                    },
                    QueryParameterValue {
                        struct_values,
                        ..Default::default()
                    },
                ))
            }
        }
    }
}

impl BigQueryFieldType {
    /// The parameter type of a column type in a mode; REPEATED is an ARRAY of it.
    fn param_type(&self, mode: BigQueryFieldMode) -> QueryParameterType {
        let element = match self {
            BigQueryFieldType::Range(element) => FieldKind::from(*element).range_param_type(),
            BigQueryFieldType::Struct(fields) => QueryParameterType {
                r#type: FieldKind::Struct.name().to_string(),
                struct_types: fields
                    .iter()
                    .map(|field| QueryParameterStructType {
                        name: field.name.clone(),
                        r#type: Some(field.field_type.param_type(field.mode)),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            },
            other => FieldKind::from(other).param_type(),
        };
        match mode {
            BigQueryFieldMode::Repeated => array_type(element),
            BigQueryFieldMode::Nullable | BigQueryFieldMode::Required => element,
        }
    }
}

impl SerializedValue {
    /// The value of `node` as a parameter of `ty` in `mode`. A REPEATED NULL is an empty ARRAY,
    /// since BigQuery has no NULL ARRAY.
    fn coerce_field(
        &self,
        param_type: &BigQueryFieldType,
        mode: BigQueryFieldMode,
    ) -> Result<QueryParameterValue, CodecError> {
        if mode != BigQueryFieldMode::Repeated {
            return self.coerce(param_type);
        }
        match self {
            SerializedValue::Null => Ok(QueryParameterValue::default()),
            SerializedValue::Sequence(items) => {
                let array_values = items
                    .iter()
                    .enumerate()
                    .map(|(index, item)| match item {
                        SerializedValue::Null => Err(CodecError::new(
                            BigQueryCodecErrorKind::NullArrayElement,
                            "an ARRAY parameter cannot hold NULL",
                        )
                        .at_index(index)),
                        item => item
                            .coerce(param_type)
                            .map_err(|error| error.at_index(index)),
                    })
                    .collect::<Result<_, _>>()?;
                Ok(QueryParameterValue {
                    array_values,
                    ..Default::default()
                })
            }
            other => Err(CodecError::type_mismatch(format!(
                "an ARRAY<{param_type}> parameter takes a sequence, got {}",
                other.describe()
            ))),
        }
    }

    fn coerce(&self, param_type: &BigQueryFieldType) -> Result<QueryParameterValue, CodecError> {
        use BigQueryFieldType as FieldType;
        let text = |t: String| Ok(text_value(t));
        match (param_type, self) {
            (_, SerializedValue::Null) => Ok(QueryParameterValue::default()),
            (FieldType::Int64, SerializedValue::Integer(value)) => text(int64_text(*value)?),
            (FieldType::Float64, SerializedValue::Float(value)) => text(float_text(*value)),
            (FieldType::Numeric(_), _) => text(decimal_string(
                self.declared_decimal(NUMERIC_SCALE)?,
                NUMERIC_SCALE,
            )),
            (FieldType::BigNumeric(_), _) => text(decimal_string(
                self.declared_decimal(BIGNUMERIC_SCALE)?,
                BIGNUMERIC_SCALE,
            )),
            (FieldType::Bool, SerializedValue::Bool(value)) => text(value.to_string()),
            (FieldType::String { .. } | FieldType::Geography, SerializedValue::String(value)) => {
                text(value.clone())
            }
            (FieldType::Bytes { .. }, SerializedValue::Bytes(value)) => text(base64_text(value)),
            (FieldType::Bytes { .. }, SerializedValue::Sequence(items)) => {
                let bytes = items
                    .iter()
                    .enumerate()
                    .map(|(index, item)| match item {
                        SerializedValue::Integer(value) => u8::try_from(*value).map_err(|_| {
                            CodecError::out_of_range(format!("{value} is not a byte"))
                                .at_index(index)
                        }),
                        other => Err(CodecError::type_mismatch(format!(
                            "BYTES takes bytes, got {}",
                            other.describe()
                        ))
                        .at_index(index)),
                    })
                    .collect::<Result<Vec<u8>, _>>()?;
                text(base64_text(&bytes))
            }
            (FieldType::Date | FieldType::Time | FieldType::DateTime | FieldType::Timestamp, _) => {
                let kind = FieldKind::from(param_type);
                text(kind.temporal_text(self.declared_temporal(kind)?)?)
            }
            (FieldType::Json, SerializedValue::Json(value) | SerializedValue::String(value)) => {
                text(value.clone())
            }
            (FieldType::Interval, SerializedValue::Interval(value)) => text(value.param_text()?),
            (FieldType::Interval, SerializedValue::String(value)) => {
                text(BigQueryInterval::parse_bq(value)?.param_text()?)
            }
            (FieldType::Range(element), SerializedValue::Range { start, end }) => {
                FieldKind::from(*element).range_value(start, end)
            }
            (FieldType::Struct(fields), SerializedValue::Struct(entries)) => {
                if let Some((name, _)) = entries
                    .iter()
                    .find(|(name, _)| !fields.iter().any(|field| &field.name == name))
                {
                    return Err(CodecError::new(
                        BigQueryCodecErrorKind::UnknownField,
                        format!("the declared {param_type} has no field `{name}`"),
                    )
                    .at_field(name));
                }
                let mut struct_values = std::collections::HashMap::with_capacity(fields.len());
                for field in fields {
                    let node = entries
                        .iter()
                        .find(|(name, _)| *name == field.name)
                        .map_or(&SerializedValue::Null, |(_, node)| node);
                    let value = node
                        .coerce_field(&field.field_type, field.mode)
                        .map_err(|error| error.at_field(&field.name))?;
                    struct_values.insert(field.name.clone(), value);
                }
                Ok(QueryParameterValue {
                    struct_values,
                    ..Default::default()
                })
            }
            (param_type, other) => Err(CodecError::type_mismatch(format!(
                "a {param_type} parameter takes no {}",
                other.describe()
            ))),
        }
    }

    fn describe(&self) -> String {
        match self {
            SerializedValue::Null => "NULL".into(),
            SerializedValue::Bool(_) => "bool".into(),
            SerializedValue::Integer(_) => "integer".into(),
            SerializedValue::Float(_) => "float".into(),
            SerializedValue::String(_) => "string".into(),
            SerializedValue::Bytes(_) => "bytes".into(),
            SerializedValue::Temporal { kind, .. } => format!("{} wrapper", kind.name()),
            SerializedValue::Json(_) => "BigQueryJson".into(),
            SerializedValue::Decimal(_) => "BigQueryDecimal".into(),
            SerializedValue::Interval(_) => "BigQueryInterval".into(),
            SerializedValue::Range { .. } => "BigQueryRange".into(),
            SerializedValue::Sequence(_) => "sequence".into(),
            SerializedValue::Struct(_) => "struct or map".into(),
        }
    }
}

impl FieldKind {
    /// A RANGE of this element kind from its two bounds; a NULL bound is unbounded.
    fn range_value(
        self,
        start: &SerializedValue,
        end: &SerializedValue,
    ) -> Result<QueryParameterValue, CodecError> {
        Ok(QueryParameterValue {
            range_value: Some(Box::new(RangeValue {
                start: start.range_bound(self, "start")?,
                end: end.range_bound(self, "end")?,
            })),
            ..Default::default()
        })
    }
}

impl SerializedValue {
    /// One bound of a RANGE of `element`, `None` when unbounded. `name` locates errors.
    fn range_bound(
        &self,
        element: FieldKind,
        name: &str,
    ) -> Result<Option<Box<QueryParameterValue>>, CodecError> {
        if *self == SerializedValue::Null {
            return Ok(None);
        }
        let micros = self
            .declared_temporal(element)
            .map_err(|error| error.at_field(name))?;
        let text = element
            .temporal_text(micros)
            .map_err(|error| error.at_field(name))?;
        Ok(Some(Box::new(text_value(text))))
    }

    /// The BigQuery integer of a temporal value given for a parameter of `kind`: the wrapper of
    /// that kind, the integer form, or the text form.
    fn declared_temporal(&self, kind: FieldKind) -> Result<i64, CodecError> {
        match self {
            SerializedValue::Temporal { kind: k, value } if *k == kind => Ok(*value),
            SerializedValue::Temporal { kind: k, .. } => Err(CodecError::type_mismatch(format!(
                "a {} wrapper on a {} parameter",
                k.name(),
                kind.name()
            ))),
            SerializedValue::Integer(value) => i64::try_from(*value).map_err(|_| {
                CodecError::out_of_range(format!("{value} is outside the {} range", kind.name()))
            }),
            SerializedValue::String(text) => match kind {
                FieldKind::Date => civil::parse_date(text).map(i64::from),
                FieldKind::Time => civil::parse_time(text),
                FieldKind::DateTime => civil::parse_datetime(text),
                _ => civil::parse_timestamp(text),
            },
            other => Err(CodecError::type_mismatch(format!(
                "a {} parameter takes no {}",
                kind.name(),
                other.describe()
            ))),
        }
    }

    /// The unscaled value at `scale` of a decimal given for a NUMERIC or BIGNUMERIC parameter: the
    /// text, a `BigQueryDecimal`, an integer, or an `f64` rounded at the scale.
    fn declared_decimal(&self, scale: u32) -> Result<arrow_buffer::i256, CodecError> {
        let parse = |text: &str| {
            if scale == NUMERIC_SCALE {
                parse_numeric(text)
            } else {
                parse_bignumeric(text)
            }
        };
        match self {
            SerializedValue::String(text) | SerializedValue::Decimal(text) => parse(text),
            SerializedValue::Integer(value) => parse(&value.to_string()),
            SerializedValue::Float(float) => {
                let value = crate::types::decimal::decimal_from_f64(*float, scale)?;
                parse(&decimal_string(value, scale))
            }
            other => Err(CodecError::type_mismatch(format!(
                "a decimal parameter takes no {}",
                other.describe()
            ))),
        }
    }
}

fn int64_text(value: i128) -> Result<String, CodecError> {
    i64::try_from(value)
        .map(|value| value.to_string())
        .map_err(|_| CodecError::out_of_range(format!("{value} is outside INT64")))
}

/// The shortest text that parses back to the same `f64`, and BigQuery's names for the special
/// values.
fn float_text(float: f64) -> String {
    if float.is_nan() {
        "NaN".into()
    } else if float == f64::INFINITY {
        "Infinity".into()
    } else if float == f64::NEG_INFINITY {
        "-Infinity".into()
    } else {
        // `Debug` is the shortest round-trip form and switches to an exponent where `Display`
        // would print hundreds of digits.
        format!("{float:?}")
    }
}

fn base64_text(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

impl BigQueryInterval {
    fn param_text(&self) -> Result<String, CodecError> {
        if self.nanos % 1000 != 0 {
            return Err(CodecError::out_of_range(format!(
                "INTERVAL time part of {} ns is not a whole number of microseconds",
                self.nanos
            )));
        }
        let mut out = String::new();
        self.write_bq(&mut out);
        Ok(out)
    }
}

impl FieldKind {
    /// The parameter text of a temporal integer: `YYYY-MM-DD`, `HH:MM:SS[.ffffff]`, the two with a
    /// space for DATETIME, and with `+00:00` for TIMESTAMP, which BigQuery accepts.
    fn temporal_text(self, value: i64) -> Result<String, CodecError> {
        let outside = || {
            CodecError::out_of_range(format!(
                "{value} is outside BigQuery's {} range",
                self.name()
            ))
        };
        let mut out = String::new();
        let date_time = |micros: i64, out: &mut String| {
            // Every arm below checks the range before formatting, so the day count fits i32.
            civil::fmt_date(micros.div_euclid(civil::MICROS_PER_DAY) as i32, out)?;
            out.push(' ');
            civil::fmt_time(micros, out)
        };
        let datetime_min = i64::from(civil::DATE_MIN_DAYS) * civil::MICROS_PER_DAY;
        let datetime_end = (i64::from(civil::DATE_MAX_DAYS) + 1) * civil::MICROS_PER_DAY;
        match self {
            FieldKind::Date => {
                let days = i32::try_from(value)
                    .ok()
                    .filter(|d| (civil::DATE_MIN_DAYS..=civil::DATE_MAX_DAYS).contains(d))
                    .ok_or_else(outside)?;
                civil::fmt_date(days, &mut out)?;
            }
            FieldKind::Time => {
                if !(0..civil::MICROS_PER_DAY).contains(&value) {
                    return Err(outside());
                }
                civil::fmt_time(value, &mut out)?;
            }
            FieldKind::DateTime => {
                if !(datetime_min..datetime_end).contains(&value) {
                    return Err(outside());
                }
                date_time(value, &mut out)?;
            }
            FieldKind::Timestamp => {
                if !(civil::TIMESTAMP_MIN_MICROS..=civil::TIMESTAMP_MAX_MICROS).contains(&value) {
                    return Err(outside());
                }
                date_time(value, &mut out)?;
                out.push_str("+00:00");
            }
            other => {
                return Err(CodecError::type_mismatch(format!(
                    "{} is not a temporal type",
                    other.name()
                )))
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::BigQueryError;
    use crate::types::testkit::field;
    use crate::{
        BigQueryDate, BigQueryDecimal, BigQueryFieldMode, BigQueryJson, BigQueryRange,
        BigQueryTimestamp,
    };
    use serde::Serialize;

    fn param_type(name: &str) -> QueryParameterType {
        QueryParameterType {
            r#type: name.into(),
            ..Default::default()
        }
    }

    fn array_param_type(element: QueryParameterType) -> QueryParameterType {
        QueryParameterType {
            r#type: "ARRAY".into(),
            array_type: Some(Box::new(element)),
            ..Default::default()
        }
    }

    fn param_value(text: &str) -> QueryParameterValue {
        QueryParameterValue {
            value: Some(text.into()),
            ..Default::default()
        }
    }

    fn param(
        name: &str,
        param_type: QueryParameterType,
        value: QueryParameterValue,
    ) -> QueryParameter {
        QueryParameter {
            name: name.into(),
            parameter_type: Some(param_type),
            parameter_value: Some(value),
        }
    }

    fn infer<V: Serialize + ?Sized>(value: &V) -> BigQueryResult<QueryParameter> {
        infer_param(ParamLabel::Named("value"), value)
    }

    fn typed<V: Serialize + ?Sized>(
        param_type: impl Into<BigQueryParamType>,
        value: &V,
    ) -> BigQueryResult<QueryParameter> {
        typed_param(ParamLabel::Named("value"), &param_type.into(), value)
    }

    #[derive(Serialize)]
    enum Colour {
        Red,
    }

    #[test]
    fn scalars_infer_their_bigquery_types() -> BigQueryResult<()> {
        assert_eq!(
            infer(&41i32)?,
            param("value", param_type("INT64"), param_value("41"))
        );
        assert_eq!(
            infer(&(u64::MAX >> 1))?,
            param(
                "value",
                param_type("INT64"),
                param_value("9223372036854775807")
            )
        );
        assert_eq!(
            infer(&1.5f64)?,
            param("value", param_type("FLOAT64"), param_value("1.5"))
        );
        assert_eq!(
            infer(&true)?,
            param("value", param_type("BOOL"), param_value("true"))
        );
        assert_eq!(
            infer("Åsa")?,
            param("value", param_type("STRING"), param_value("Åsa"))
        );
        assert_eq!(
            infer(&'x')?,
            param("value", param_type("STRING"), param_value("x"))
        );
        assert_eq!(
            infer(&Colour::Red)?,
            param("value", param_type("STRING"), param_value("Red"))
        );
        assert_eq!(
            infer(&serde_bytes::ByteBuf::from(vec![0u8, 255]))?,
            param("value", param_type("BYTES"), param_value("AP8="))
        );
        assert_eq!(
            infer(&Some(7i64))?,
            param("value", param_type("INT64"), param_value("7"))
        );
        Ok(())
    }

    #[test]
    fn integer_above_int64_is_out_of_range() {
        match infer(&u64::MAX) {
            Err(BigQueryError::SerializeError(err)) => {
                assert_eq!(err.kind, BigQueryCodecErrorKind::OutOfRange);
                assert_eq!(err.path, "value");
                assert_eq!(err.row, None);
            }
            other => panic!("expected OutOfRange, got {other:?}"),
        }
    }

    #[test]
    fn float_text_round_trips_and_names_special_values() -> BigQueryResult<()> {
        for float in [0.1, -0.0, 5e-324, 1e300, f64::MAX, 123_456.789] {
            let text = infer(&float)?
                .parameter_value
                .and_then(|value| value.value)
                .unwrap_or_default();
            let back: f64 = text.parse().expect("FLOAT64 text parses");
            assert_eq!(back.to_bits(), float.to_bits(), "{float} as {text}");
        }
        assert_eq!(
            infer(&f64::NAN)?,
            param("value", param_type("FLOAT64"), param_value("NaN"))
        );
        assert_eq!(
            infer(&f64::INFINITY)?,
            param("value", param_type("FLOAT64"), param_value("Infinity"))
        );
        assert_eq!(
            infer(&f64::NEG_INFINITY)?,
            param("value", param_type("FLOAT64"), param_value("-Infinity"))
        );
        Ok(())
    }

    #[test]
    fn wrappers_are_recognised_by_their_serde_names() -> BigQueryResult<()> {
        let timestamp: jiff::Timestamp = "2026-10-04T12:34:56.123456Z".parse().expect("valid");
        let date = jiff::civil::date(2024, 2, 29);
        assert_eq!(
            infer(&BigQueryTimestamp(timestamp))?,
            param(
                "value",
                param_type("TIMESTAMP"),
                param_value("2026-10-04 12:34:56.123456+00:00")
            )
        );
        assert_eq!(
            infer(&BigQueryDate(date))?,
            param("value", param_type("DATE"), param_value("2024-02-29"))
        );
        assert_eq!(
            infer(&crate::BigQueryTime(jiff::civil::time(4, 5, 6, 0)))?,
            param("value", param_type("TIME"), param_value("04:05:06"))
        );
        assert_eq!(
            infer(&crate::BigQueryDateTime(date.at(23, 59, 59, 999_999_000)))?,
            param(
                "value",
                param_type("DATETIME"),
                param_value("2024-02-29 23:59:59.999999")
            )
        );
        assert_eq!(
            infer(&BigQueryJson(serde_json::json!({"stad": "Malmö"})))?,
            param(
                "value",
                param_type("JSON"),
                param_value(r#"{"stad":"Malmö"}"#)
            )
        );
        assert_eq!(
            infer(&BigQueryDecimal("123.450"))?,
            param("value", param_type("NUMERIC"), param_value("123.45"))
        );
        assert_eq!(
            infer(&BigQueryDecimal("0.00000000000000000000000000000000000001"))?,
            param(
                "value",
                param_type("BIGNUMERIC"),
                param_value("0.00000000000000000000000000000000000001")
            )
        );
        assert_eq!(
            infer(&crate::BigQueryInterval {
                months: 14,
                days: -3,
                nanos: 3_723_500_000_000,
            })?,
            param(
                "value",
                param_type("INTERVAL"),
                param_value("1-2 -3 1:2:3.500000")
            )
        );
        let range = BigQueryRange {
            start: Some(BigQueryDate(date)),
            end: None,
        };
        assert_eq!(
            infer(&range)?,
            param(
                "value",
                QueryParameterType {
                    r#type: "RANGE".into(),
                    range_element_type: Some(Box::new(param_type("DATE"))),
                    ..Default::default()
                },
                QueryParameterValue {
                    range_value: Some(Box::new(RangeValue {
                        start: Some(Box::new(param_value("2024-02-29"))),
                        end: None,
                    })),
                    ..Default::default()
                }
            )
        );
        Ok(())
    }

    #[test]
    fn plain_jiff_value_infers_string() -> BigQueryResult<()> {
        let timestamp: jiff::Timestamp = "2026-10-04T12:34:56Z".parse().expect("valid");
        assert_eq!(
            infer(&timestamp)?,
            param(
                "value",
                param_type("STRING"),
                param_value("2026-10-04T12:34:56Z")
            )
        );
        Ok(())
    }

    #[derive(Serialize)]
    struct Book {
        pages: i64,
        author: String,
    }

    #[test]
    fn sequences_and_structs_infer_array_and_struct_in_field_order() -> BigQueryResult<()> {
        assert_eq!(
            infer(&vec![1i64, 2, 3])?,
            param(
                "value",
                array_param_type(param_type("INT64")),
                QueryParameterValue {
                    array_values: vec![param_value("1"), param_value("2"), param_value("3")],
                    ..Default::default()
                }
            )
        );
        let struct_type = QueryParameterType {
            r#type: "STRUCT".into(),
            struct_types: vec![
                QueryParameterStructType {
                    name: "pages".into(),
                    r#type: Some(param_type("INT64")),
                    ..Default::default()
                },
                QueryParameterStructType {
                    name: "author".into(),
                    r#type: Some(param_type("STRING")),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let struct_value = QueryParameterValue {
            struct_values: [
                ("pages".to_string(), param_value("7")),
                ("author".to_string(), param_value("Ursula")),
            ]
            .into_iter()
            .collect(),
            ..Default::default()
        };
        assert_eq!(
            infer(&Book {
                pages: 7,
                author: "Ursula".into()
            })?,
            param("value", struct_type.clone(), struct_value.clone())
        );
        assert_eq!(
            infer(&OrderedMap(vec![
                ("pages", 7.into()),
                ("author", "Ursula".into())
            ]))?,
            param("value", struct_type, struct_value)
        );
        Ok(())
    }

    /// A map that serializes its entries in the given order.
    struct OrderedMap(Vec<(&'static str, serde_json::Value)>);

    impl Serialize for OrderedMap {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            use serde::ser::SerializeMap;
            let mut map = serializer.serialize_map(Some(self.0.len()))?;
            for (k, v) in &self.0 {
                map.serialize_entry(k, v)?;
            }
            map.end()
        }
    }

    fn assert_points_at_param_as(result: BigQueryResult<QueryParameter>, what: &str) {
        match result {
            Err(BigQueryError::InvalidParametersError(err)) => {
                assert_eq!(err.public.field, "value", "{what}");
            }
            other => panic!("{what}: expected InvalidParametersError, got {other:?}"),
        }
    }

    #[test]
    fn values_without_a_type_point_at_param_as() {
        assert_points_at_param_as(infer(&None::<i64>), "None");
        assert_points_at_param_as(infer(&Vec::<i64>::new()), "an empty array");
        assert_points_at_param_as(
            infer(&vec![serde_json::json!(1), serde_json::json!("Ursula")]),
            "mixed elements",
        );
        assert_points_at_param_as(
            infer(&serde_json::json!({"author": null})),
            "a NULL struct field",
        );
    }

    #[test]
    fn param_as_takes_the_write_forms_of_the_declared_type() -> BigQueryResult<()> {
        let timestamp: jiff::Timestamp = "2026-10-04T12:34:56.123456Z".parse().expect("valid");
        let expected = param(
            "value",
            param_type("TIMESTAMP"),
            param_value("2026-10-04 12:34:56.123456+00:00"),
        );
        assert_eq!(typed(BigQueryFieldType::Timestamp, &timestamp)?, expected);
        assert_eq!(
            typed(BigQueryFieldType::Timestamp, &1_791_117_296_123_456i64)?.parameter_type,
            expected.parameter_type
        );
        assert_eq!(
            typed(
                BigQueryFieldType::Timestamp,
                "2026-10-04T14:34:56.123456+02:00"
            )?,
            expected
        );
        assert_eq!(
            typed(BigQueryFieldType::Date, "2024-02-29")?,
            param("value", param_type("DATE"), param_value("2024-02-29"))
        );
        assert_eq!(
            typed(BigQueryFieldType::Numeric(None), &1.25f64)?,
            param("value", param_type("NUMERIC"), param_value("1.25"))
        );
        assert_eq!(
            typed(BigQueryFieldType::Bytes { max_length: None }, &vec![1u8, 2])?,
            param("value", param_type("BYTES"), param_value("AQI="))
        );
        assert_eq!(
            typed(BigQueryFieldType::Int64, &None::<i64>)?,
            param("value", param_type("INT64"), QueryParameterValue::default())
        );
        assert_eq!(
            typed(
                BigQueryParamType::array_of(BigQueryFieldType::String { max_length: None }),
                &["fiction", "poetry"]
            )?,
            param(
                "value",
                array_param_type(param_type("STRING")),
                QueryParameterValue {
                    array_values: vec![param_value("fiction"), param_value("poetry")],
                    ..Default::default()
                }
            )
        );
        Ok(())
    }

    #[test]
    fn param_as_struct_follows_the_declared_fields() -> BigQueryResult<()> {
        let declared = BigQueryFieldType::Struct(vec![
            field(
                "author",
                BigQueryFieldType::String { max_length: None },
                BigQueryFieldMode::Nullable,
            ),
            field(
                "pages",
                BigQueryFieldType::Int64,
                BigQueryFieldMode::Nullable,
            ),
        ]);
        let encoded = typed(
            declared.clone(),
            &Book {
                pages: 7,
                author: "Ursula".into(),
            },
        )?;
        let types: Vec<_> = encoded
            .parameter_type
            .map(|struct_type| {
                struct_type
                    .struct_types
                    .into_iter()
                    .map(|field| field.name)
                    .collect()
            })
            .unwrap_or_default();
        assert_eq!(types, ["author", "pages"]);
        match typed(
            declared,
            &serde_json::json!({"author": "Ursula", "isbn": 1}),
        ) {
            Err(BigQueryError::SerializeError(err)) => {
                assert_eq!(err.kind, BigQueryCodecErrorKind::UnknownField);
                assert_eq!(err.path, "value.isbn");
            }
            other => panic!("expected UnknownField, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn param_as_rejects_forms_outside_the_declared_type() {
        let date = jiff::civil::date(2024, 2, 29);
        for (what, result) in [
            (
                "an integer as FLOAT64",
                typed(BigQueryFieldType::Float64, &1i64),
            ),
            (
                "a DATE wrapper as TIMESTAMP",
                typed(BigQueryFieldType::Timestamp, &BigQueryDate(date)),
            ),
            (
                "a string as BYTES",
                typed(BigQueryFieldType::Bytes { max_length: None }, "AQI="),
            ),
        ] {
            match result {
                Err(BigQueryError::SerializeError(err)) => {
                    assert_eq!(err.kind, BigQueryCodecErrorKind::TypeMismatch, "{what}");
                    assert_eq!(err.path, "value", "{what}");
                }
                other => panic!("{what}: expected TypeMismatch, got {other:?}"),
            }
        }
        match typed(BigQueryFieldType::Date, "0000-01-01") {
            Err(BigQueryError::SerializeError(err)) => {
                assert_eq!(err.kind, BigQueryCodecErrorKind::OutOfRange)
            }
            other => panic!("expected OutOfRange, got {other:?}"),
        }
    }

    #[derive(Serialize)]
    struct Filter {
        min: i64,
        name: &'static str,
    }

    #[test]
    fn struct_params_send_each_top_level_field() -> BigQueryResult<()> {
        let params = struct_params(&Filter {
            min: 10,
            name: "Ursula",
        })?;
        assert_eq!(
            params,
            [
                param("min", param_type("INT64"), param_value("10")),
                param("name", param_type("STRING"), param_value("Ursula"))
            ]
        );
        match struct_params(&5i64) {
            Err(BigQueryError::InvalidParametersError(err)) => {
                assert_eq!(err.public.field, "params")
            }
            other => panic!("expected InvalidParametersError, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn positional_parameter_errors_name_its_position() {
        match infer_param(ParamLabel::Positional(1), &None::<i64>) {
            Err(BigQueryError::InvalidParametersError(err)) => {
                assert_eq!(err.public.field, "positional parameter 2")
            }
            other => panic!("expected InvalidParametersError, got {other:?}"),
        }
    }

    #[test]
    fn named_and_positional_parameters_cannot_mix() -> BigQueryResult<()> {
        let named = param("min_pages", param_type("INT64"), param_value("1"));
        let positional = param("", param_type("INT64"), param_value("1"));
        assert_eq!(parameter_mode(&[])?, "");
        assert_eq!(parameter_mode(std::slice::from_ref(&named))?, "NAMED");
        assert_eq!(
            parameter_mode(std::slice::from_ref(&positional))?,
            "POSITIONAL"
        );
        assert!(matches!(
            parameter_mode(&[named, positional]),
            Err(BigQueryError::InvalidParametersError(_))
        ));
        Ok(())
    }

    #[test]
    fn parameter_names_must_be_googlesql_identifiers() {
        for name in [
            "", "a b", "a;b", "a--", "--", "`a`", "a'b", "a\"b", "1a", "@a", "a-b", "a.b", "ä",
            "a\nb", "a\0",
        ] {
            for result in [
                infer_param(ParamLabel::Named(name), &1i64),
                typed_param(
                    ParamLabel::Named(name),
                    &BigQueryFieldType::Int64.into(),
                    &1i64,
                ),
            ] {
                match result {
                    Err(BigQueryError::InvalidParametersError(_)) => {}
                    other => panic!("{name:?}: expected InvalidParametersError, got {other:?}"),
                }
            }
        }
        for name in ["a", "_a1", "A_b", "_"] {
            assert!(
                infer_param(ParamLabel::Named(name), &1i64).is_ok(),
                "{name:?}"
            );
        }
        let map: std::collections::BTreeMap<&str, i64> = [("ok", 1), ("bad name", 2)].into();
        assert!(matches!(
            struct_params(&map),
            Err(BigQueryError::InvalidParametersError(_))
        ));
    }

    /// A value that serializes as one of the shapes a parameter error describes.
    enum Malformed {
        JsonWrapper,
        DecimalWrapper,
        IntegerMapKey,
        IntervalPart,
    }

    impl Serialize for Malformed {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            use serde::ser::{SerializeMap, SerializeStruct};
            match self {
                Malformed::JsonWrapper => serializer.serialize_newtype_struct(TAG_JSON, &271828i64),
                Malformed::DecimalWrapper => {
                    serializer.serialize_newtype_struct(TAG_DECIMAL, &271828i64)
                }
                Malformed::IntegerMapKey => {
                    let mut map = serializer.serialize_map(Some(1))?;
                    map.serialize_entry(&271828i64, "price")?;
                    map.end()
                }
                Malformed::IntervalPart => {
                    let mut interval = serializer.serialize_struct(TAG_INTERVAL, 3)?;
                    interval.serialize_field("months", "s3cr3t")?;
                    interval.serialize_field("days", &0i64)?;
                    interval.serialize_field("nanos", &0i64)?;
                    interval.end()
                }
            }
        }
    }

    #[test]
    fn a_parameter_error_leaves_the_value_out() {
        for (value, kind, text) in [
            (Malformed::JsonWrapper, "integer", "271828"),
            (Malformed::DecimalWrapper, "integer", "271828"),
            (Malformed::IntegerMapKey, "integer", "271828"),
            (Malformed::IntervalPart, "string", "s3cr3t"),
        ] {
            let message = match infer(&value) {
                Err(err) => err.to_string(),
                Ok(param) => panic!("{kind}: expected an error, got {param:?}"),
            };
            assert!(!message.contains(text), "{kind}: {message}");
        }
    }
}
