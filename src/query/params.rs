//! Query parameters: serde values encoded as `QueryParameter`s, with the type inferred from the
//! serde form or declared by the caller.
//!
//! A value is first serialized into a [`Node`], a tree that keeps what serde said about it: the
//! crate's wrappers are recognised there by their serde names. Inference reads the type off the
//! tree; a declared type accepts the forms the write path accepts for that column type.

use crate::errors::{
    BigQueryCodecErrorKind, BigQueryError, BigQueryErrorPublicGenericDetails,
    BigQueryInvalidParametersError, BigQueryInvalidParametersPublicDetails,
    BigQuerySerializationError,
};
use crate::sql::SqlLiteral;
use crate::types::civil;
use crate::types::decimal::{
    fmt_decimal_i256, parse_bignumeric, parse_numeric, BIGNUMERIC_SCALE, NUMERIC_SCALE, TAG_DECIMAL,
};
use crate::types::error::CodecError;
use crate::types::interval::TAG_INTERVAL;
use crate::types::json::TAG_JSON;
use crate::types::kind::BqKind;
use crate::types::range::TAG_RANGE;
use crate::types::temporal::{capture_int, temporal_tag_kind};
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
            ParamLabel::Positional(i) => format!("positional parameter {}", i + 1),
        }
    }

    fn locate(self, err: CodecError) -> CodecError {
        match self {
            ParamLabel::Named(name) => err.at_field(name),
            ParamLabel::Positional(i) => err.at_index(i),
        }
    }
}

impl ParamLabel<'_> {
    /// Refuses a name that is not a GoogleSQL identifier: ASCII letters, digits and
    /// underscores, not starting with a digit. An empty name would be sent as a positional
    /// parameter, and any other name could not be written as `@name` in the statement.
    fn check(self) -> Result<Self, ParamFailure> {
        let ParamLabel::Named(name) = self else {
            return Ok(self);
        };
        let mut chars = name.chars();
        let valid = chars
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
            && chars.all(|c| c.is_ascii_alphanumeric() || c == '_');
        if valid {
            Ok(self)
        } else {
            Err(invalid(
                name.to_string(),
                format!(
                    "{name:?} is not a parameter name: a GoogleSQL identifier of ASCII letters, \
                     digits and underscores, not starting with a digit"
                ),
            ))
        }
    }
}

/// A parameter that could not be encoded. The builders keep it until the terminal, since
/// [`BigQueryError`] is not `Clone` and the builders are.
#[derive(Debug, Clone)]
pub(crate) enum ParamFailure {
    /// The value does not have a form of its type.
    Serialize(BigQuerySerializationError),
    /// The type cannot be inferred, or `params` was not given a struct.
    Invalid(BigQueryInvalidParametersError),
}

impl From<ParamFailure> for BigQueryError {
    fn from(failure: ParamFailure) -> Self {
        match failure {
            ParamFailure::Serialize(err) => BigQueryError::SerializeError(err),
            ParamFailure::Invalid(err) => BigQueryError::InvalidParametersError(err),
        }
    }
}

fn invalid(field: String, error: String) -> ParamFailure {
    ParamFailure::Invalid(BigQueryInvalidParametersError::new(
        BigQueryInvalidParametersPublicDetails::new(field, error),
    ))
}

fn serialize_failure(label: ParamLabel, err: CodecError) -> ParamFailure {
    match label.locate(err).into_serialize() {
        BigQueryError::SerializeError(err) => ParamFailure::Serialize(err),
        other => ParamFailure::Serialize(BigQuerySerializationError::new(
            BigQueryErrorPublicGenericDetails::new("CUSTOM".into()),
            BigQueryCodecErrorKind::Custom,
            label.describe(),
            other.to_string(),
        )),
    }
}

/// Encodes `value` with its type inferred from its serde form.
pub(crate) fn infer_param<V: Serialize + ?Sized>(
    label: ParamLabel,
    value: &V,
) -> Result<QueryParameter, ParamFailure> {
    let label = label.check()?;
    let node = value
        .serialize(NodeSerializer)
        .map_err(|e| serialize_failure(label, e))?;
    infer_node(label, &node)
}

fn infer_node(label: ParamLabel, node: &Node) -> Result<QueryParameter, ParamFailure> {
    let label = label.check()?;
    let (parameter_type, parameter_value) = infer(node, String::new()).map_err(|e| match e {
        InferError::Codec(err) => serialize_failure(label, err),
        InferError::Untyped { path, what } => {
            let at = if path.is_empty() {
                String::new()
            } else {
                format!(" at `{path}`")
            };
            invalid(
                label.describe(),
                format!(
                    "the type of {what}{at} cannot be inferred from its value; declare it with \
                     param_as"
                ),
            )
        }
    })?;
    Ok(QueryParameter {
        name: label.name(),
        parameter_type: Some(parameter_type),
        parameter_value: Some(parameter_value),
    })
}

/// The SQL literal of `value`, its type inferred as for a parameter, or `None` when `value`
/// is NULL. `field` names the value in errors.
pub(crate) fn literal_of<V: Serialize + ?Sized>(
    field: &str,
    value: &V,
) -> Result<Option<SqlLiteral>, ParamFailure> {
    let label = ParamLabel::Named(field);
    let node = value
        .serialize(NodeSerializer)
        .map_err(|e| serialize_failure(label, e))?;
    if node == Node::Null {
        return Ok(None);
    }
    let (ty, value) = infer(&node, String::new()).map_err(|e| match e {
        InferError::Codec(err) => serialize_failure(label, err),
        InferError::Untyped { path, what } => {
            let at = if path.is_empty() {
                String::new()
            } else {
                format!(" at `{path}`")
            };
            invalid(
                field.to_string(),
                format!("the type of {what}{at} cannot be inferred from its value"),
            )
        }
    })?;
    SqlLiteral::try_from((&ty, &value))
        .map(Some)
        .map_err(|e| serialize_failure(label, e))
}

/// Encodes `value` as a parameter of type `ty`. `None` is a NULL of that type.
pub(crate) fn typed_param<V: Serialize + ?Sized>(
    label: ParamLabel,
    ty: &BigQueryParamType,
    value: &V,
) -> Result<QueryParameter, ParamFailure> {
    let label = label.check()?;
    let node = value
        .serialize(NodeSerializer)
        .map_err(|e| serialize_failure(label, e))?;
    let mode = if ty.repeated {
        BigQueryFieldMode::Repeated
    } else {
        BigQueryFieldMode::Nullable
    };
    let parameter_value =
        coerce_field(&node, &ty.field_type, mode).map_err(|e| serialize_failure(label, e))?;
    Ok(QueryParameter {
        name: label.name(),
        parameter_type: Some(field_param_type(&ty.field_type, mode)),
        parameter_value: Some(parameter_value),
    })
}

/// Encodes each top-level field of a struct or string-keyed map as a named parameter, its type
/// inferred.
pub(crate) fn struct_params<P: Serialize + ?Sized>(
    params: &P,
) -> Result<Vec<QueryParameter>, ParamFailure> {
    let node = params.serialize(NodeSerializer).map_err(|e| {
        invalid(
            "params".into(),
            format!("the parameters do not serialize: {e}"),
        )
    })?;
    let Node::Struct(fields) = node else {
        return Err(invalid(
            "params".into(),
            "params takes a struct or a map with string keys".into(),
        ));
    };
    fields
        .iter()
        .map(|(name, node)| infer_node(ParamLabel::Named(name), node))
        .collect()
}

/// The `parameter_mode` that `params` need: `NAMED`, `POSITIONAL`, or empty for none.
pub(crate) fn parameter_mode(params: &[QueryParameter]) -> BigQueryResult<&'static str> {
    let named = params.iter().filter(|p| !p.name.is_empty()).count();
    match (named, params.len()) {
        (_, 0) => Ok(""),
        (n, len) if n == len => Ok("NAMED"),
        (0, _) => Ok("POSITIONAL"),
        _ => Err(BigQueryError::invalid_parameters(
            "query_parameters",
            "named and positional parameters cannot be used in one query",
        )),
    }
}

/// A serialized value with what serde said about it.
#[derive(Clone, Debug, PartialEq)]
enum Node {
    Null,
    Bool(bool),
    Int(i128),
    Float(f64),
    Str(String),
    Bytes(Vec<u8>),
    /// A temporal wrapper: its kind and BigQuery integer.
    Temporal(BqKind, i64),
    /// `BigQueryJson`: the JSON text.
    Json(String),
    /// `BigQueryDecimal`: the decimal text.
    Decimal(String),
    Interval(BigQueryInterval),
    /// `BigQueryRange`: start and end, `Null` for an unbounded end.
    Range(Box<Node>, Box<Node>),
    Seq(Vec<Node>),
    /// A struct or a string-keyed map, its entries in serialization order.
    Struct(Vec<(String, Node)>),
}

struct NodeSerializer;

fn mismatch(message: impl Into<String>) -> CodecError {
    CodecError::new(BigQueryCodecErrorKind::TypeMismatch, message)
}

fn out_of_range(message: impl Into<String>) -> CodecError {
    CodecError::new(BigQueryCodecErrorKind::OutOfRange, message)
}

impl ser::Serializer for NodeSerializer {
    type Ok = Node;
    type Error = CodecError;
    type SerializeSeq = SeqNode;
    type SerializeTuple = SeqNode;
    type SerializeTupleStruct = SeqNode;
    type SerializeTupleVariant = Impossible<Node, CodecError>;
    type SerializeMap = MapNode;
    type SerializeStruct = StructNode;
    type SerializeStructVariant = Impossible<Node, CodecError>;

    fn serialize_bool(self, v: bool) -> Result<Node, CodecError> {
        Ok(Node::Bool(v))
    }

    fn serialize_i8(self, v: i8) -> Result<Node, CodecError> {
        Ok(Node::Int(v.into()))
    }

    fn serialize_i16(self, v: i16) -> Result<Node, CodecError> {
        Ok(Node::Int(v.into()))
    }

    fn serialize_i32(self, v: i32) -> Result<Node, CodecError> {
        Ok(Node::Int(v.into()))
    }

    fn serialize_i64(self, v: i64) -> Result<Node, CodecError> {
        Ok(Node::Int(v.into()))
    }

    fn serialize_i128(self, v: i128) -> Result<Node, CodecError> {
        Ok(Node::Int(v))
    }

    fn serialize_u8(self, v: u8) -> Result<Node, CodecError> {
        Ok(Node::Int(v.into()))
    }

    fn serialize_u16(self, v: u16) -> Result<Node, CodecError> {
        Ok(Node::Int(v.into()))
    }

    fn serialize_u32(self, v: u32) -> Result<Node, CodecError> {
        Ok(Node::Int(v.into()))
    }

    fn serialize_u64(self, v: u64) -> Result<Node, CodecError> {
        Ok(Node::Int(v.into()))
    }

    fn serialize_u128(self, v: u128) -> Result<Node, CodecError> {
        i128::try_from(v)
            .map(Node::Int)
            .map_err(|_| out_of_range(format!("{v} is above INT64")))
    }

    fn serialize_f32(self, v: f32) -> Result<Node, CodecError> {
        Ok(Node::Float(v.into()))
    }

    fn serialize_f64(self, v: f64) -> Result<Node, CodecError> {
        Ok(Node::Float(v))
    }

    fn serialize_char(self, v: char) -> Result<Node, CodecError> {
        Ok(Node::Str(v.to_string()))
    }

    fn serialize_str(self, v: &str) -> Result<Node, CodecError> {
        Ok(Node::Str(v.to_string()))
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Node, CodecError> {
        Ok(Node::Bytes(v.to_vec()))
    }

    fn serialize_none(self) -> Result<Node, CodecError> {
        Ok(Node::Null)
    }

    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<Node, CodecError> {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Node, CodecError> {
        Ok(Node::Null)
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Node, CodecError> {
        Ok(Node::Null)
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
    ) -> Result<Node, CodecError> {
        Ok(Node::Str(variant.to_string()))
    }

    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<Node, CodecError> {
        if let Some(kind) = temporal_tag_kind(name) {
            return Ok(Node::Temporal(kind, capture_int(value)?));
        }
        match name {
            TAG_JSON => match value.serialize(self)? {
                Node::Str(text) => Ok(Node::Json(text)),
                other => Err(mismatch(format!(
                    "JSON text expected, got {}",
                    describe(&other)
                ))),
            },
            TAG_DECIMAL => match value.serialize(self)? {
                Node::Str(text) => Ok(Node::Decimal(text)),
                other => Err(mismatch(format!(
                    "decimal text expected, got {}",
                    describe(&other)
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
    ) -> Result<Node, CodecError> {
        Err(mismatch(format!(
            "the enum variant {name}::{variant} holds data, which no parameter type takes"
        )))
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<SeqNode, CodecError> {
        Ok(SeqNode(Vec::with_capacity(len.unwrap_or_default())))
    }

    fn serialize_tuple(self, len: usize) -> Result<SeqNode, CodecError> {
        Ok(SeqNode(Vec::with_capacity(len)))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<SeqNode, CodecError> {
        Ok(SeqNode(Vec::with_capacity(len)))
    }

    fn serialize_tuple_variant(
        self,
        name: &'static str,
        _index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, CodecError> {
        Err(mismatch(format!(
            "the enum variant {name}::{variant} holds data, which no parameter type takes"
        )))
    }

    fn serialize_map(self, len: Option<usize>) -> Result<MapNode, CodecError> {
        Ok(MapNode {
            fields: Vec::with_capacity(len.unwrap_or_default()),
            key: None,
        })
    }

    fn serialize_struct(self, name: &'static str, len: usize) -> Result<StructNode, CodecError> {
        Ok(StructNode {
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
        Err(mismatch(format!(
            "the enum variant {name}::{variant} holds data, which no parameter type takes"
        )))
    }
}

struct SeqNode(Vec<Node>);

impl SeqNode {
    fn push<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), CodecError> {
        let i = self.0.len();
        self.0
            .push(value.serialize(NodeSerializer).map_err(|e| e.at_index(i))?);
        Ok(())
    }
}

impl ser::SerializeSeq for SeqNode {
    type Ok = Node;
    type Error = CodecError;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), CodecError> {
        self.push(value)
    }

    fn end(self) -> Result<Node, CodecError> {
        Ok(Node::Seq(self.0))
    }
}

impl ser::SerializeTuple for SeqNode {
    type Ok = Node;
    type Error = CodecError;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), CodecError> {
        self.push(value)
    }

    fn end(self) -> Result<Node, CodecError> {
        Ok(Node::Seq(self.0))
    }
}

impl ser::SerializeTupleStruct for SeqNode {
    type Ok = Node;
    type Error = CodecError;

    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), CodecError> {
        self.push(value)
    }

    fn end(self) -> Result<Node, CodecError> {
        Ok(Node::Seq(self.0))
    }
}

struct MapNode {
    fields: Vec<(String, Node)>,
    key: Option<String>,
}

impl ser::SerializeMap for MapNode {
    type Ok = Node;
    type Error = CodecError;

    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<(), CodecError> {
        match key.serialize(NodeSerializer)? {
            Node::Str(key) => {
                self.key = Some(key);
                Ok(())
            }
            other => Err(mismatch(format!(
                "a map parameter needs string keys, got {}",
                describe(&other)
            ))),
        }
    }

    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), CodecError> {
        let key = self.key.take().unwrap_or_default();
        let node = value
            .serialize(NodeSerializer)
            .map_err(|e| e.at_field(&key))?;
        self.fields.push((key, node));
        Ok(())
    }

    fn end(self) -> Result<Node, CodecError> {
        Ok(Node::Struct(self.fields))
    }
}

struct StructNode {
    name: &'static str,
    fields: Vec<(String, Node)>,
}

impl StructNode {
    fn take_field(&mut self, name: &str) -> Node {
        self.fields
            .iter()
            .position(|(n, _)| n == name)
            .map(|i| self.fields.remove(i).1)
            .unwrap_or(Node::Null)
    }

    fn interval_part<T: TryFrom<i128>>(&mut self, name: &str) -> Result<T, CodecError> {
        match self.take_field(name) {
            Node::Int(v) => T::try_from(v)
                .map_err(|_| out_of_range(format!("INTERVAL {name} {v} is out of range"))),
            other => Err(mismatch(format!(
                "INTERVAL {name} must be an integer, got {}",
                describe(&other)
            ))
            .at_field(name)),
        }
    }
}

impl ser::SerializeStruct for StructNode {
    type Ok = Node;
    type Error = CodecError;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), CodecError> {
        let node = value
            .serialize(NodeSerializer)
            .map_err(|e| e.at_field(key))?;
        self.fields.push((key.to_string(), node));
        Ok(())
    }

    fn end(mut self) -> Result<Node, CodecError> {
        match self.name {
            TAG_INTERVAL => Ok(Node::Interval(BigQueryInterval {
                months: self.interval_part("months")?,
                days: self.interval_part("days")?,
                nanos: self.interval_part("nanos")?,
            })),
            TAG_RANGE => {
                let start = self.take_field("start");
                let end = self.take_field("end");
                Ok(Node::Range(Box::new(start), Box::new(end)))
            }
            _ => Ok(Node::Struct(self.fields)),
        }
    }
}

impl From<BigQueryRangeElementType> for BqKind {
    fn from(element: BigQueryRangeElementType) -> Self {
        match element {
            BigQueryRangeElementType::Date => BqKind::Date,
            BigQueryRangeElementType::DateTime => BqKind::DateTime,
            BigQueryRangeElementType::Timestamp => BqKind::Timestamp,
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
    fn at_field(self, name: &str) -> Self {
        match self {
            InferError::Codec(err) => InferError::Codec(err.at_field(name)),
            untyped => untyped,
        }
    }

    fn at_index(self, i: usize) -> Self {
        match self {
            InferError::Codec(err) => InferError::Codec(err.at_index(i)),
            untyped => untyped,
        }
    }
}

fn join_field(path: &str, name: &str) -> String {
    if path.is_empty() {
        name.to_string()
    } else {
        format!("{path}.{name}")
    }
}

fn scalar_type(kind: BqKind) -> QueryParameterType {
    QueryParameterType {
        r#type: kind.name().to_string(),
        ..Default::default()
    }
}

fn array_type(element: QueryParameterType) -> QueryParameterType {
    QueryParameterType {
        r#type: "ARRAY".into(),
        array_type: Some(Box::new(element)),
        ..Default::default()
    }
}

fn range_type(element: BqKind) -> QueryParameterType {
    QueryParameterType {
        r#type: BqKind::Range.name().to_string(),
        range_element_type: Some(Box::new(scalar_type(element))),
        ..Default::default()
    }
}

fn text_value(text: String) -> QueryParameterValue {
    QueryParameterValue {
        value: Some(text),
        ..Default::default()
    }
}

type Encoded = (QueryParameterType, QueryParameterValue);

fn infer(node: &Node, path: String) -> Result<Encoded, InferError> {
    let scalar = |kind: BqKind, text: String| Ok((scalar_type(kind), text_value(text)));
    match node {
        Node::Null => Err(InferError::Untyped { path, what: "NULL" }),
        Node::Bool(v) => scalar(BqKind::Bool, v.to_string()),
        Node::Int(v) => scalar(BqKind::Int64, int64_text(*v)?),
        Node::Float(v) => scalar(BqKind::Float64, float_text(*v)),
        Node::Str(v) => scalar(BqKind::String, v.clone()),
        Node::Bytes(v) => scalar(BqKind::Bytes, base64_text(v)),
        Node::Temporal(kind, v) => scalar(*kind, temporal_text(*kind, *v)?),
        Node::Json(text) => scalar(BqKind::Json, text.clone()),
        Node::Decimal(text) => match parse_numeric(text) {
            Ok(v) => scalar(BqKind::Numeric, decimal_text(v, NUMERIC_SCALE)),
            Err(err) if err.kind() == BigQueryCodecErrorKind::OutOfRange => {
                let v = parse_bignumeric(text)?;
                scalar(BqKind::BigNumeric, decimal_text(v, BIGNUMERIC_SCALE))
            }
            Err(err) => Err(err.into()),
        },
        Node::Interval(interval) => scalar(BqKind::Interval, interval_text(interval)?),
        Node::Range(start, end) => {
            let bound_kind = |bound: &Node| match bound {
                Node::Null => Ok(None),
                Node::Temporal(kind, _)
                    if matches!(kind, BqKind::Date | BqKind::DateTime | BqKind::Timestamp) =>
                {
                    Ok(Some(*kind))
                }
                _ => Err(InferError::Untyped {
                    path: path.clone(),
                    what: "a RANGE bound that is not a DATE, DATETIME or TIMESTAMP wrapper",
                }),
            };
            let element = match (bound_kind(start)?, bound_kind(end)?) {
                (Some(a), Some(b)) if a != b => {
                    return Err(
                        mismatch(format!("a RANGE from a {} to a {}", a.name(), b.name())).into(),
                    )
                }
                (Some(kind), _) | (None, Some(kind)) => kind,
                (None, None) => {
                    return Err(InferError::Untyped {
                        path,
                        what: "a RANGE unbounded at both ends",
                    })
                }
            };
            Ok((range_type(element), range_value(start, end, element)?))
        }
        Node::Seq(items) => {
            let mut element_type = None;
            let mut values = Vec::with_capacity(items.len());
            for (i, item) in items.iter().enumerate() {
                match item {
                    Node::Seq(_) => {
                        return Err(CodecError::new(
                            BigQueryCodecErrorKind::UnsupportedType,
                            "an ARRAY of ARRAYs is not a BigQuery type",
                        )
                        .at_index(i)
                        .into())
                    }
                    Node::Null => {
                        return Err(CodecError::new(
                            BigQueryCodecErrorKind::NullArrayElement,
                            "an ARRAY parameter cannot hold NULL",
                        )
                        .at_index(i)
                        .into())
                    }
                    _ => {}
                }
                let (ty, value) = infer(item, format!("{path}[{i}]")).map_err(|e| e.at_index(i))?;
                match &element_type {
                    None => element_type = Some(ty),
                    Some(first) if *first != ty => {
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
        Node::Struct(fields) => {
            let mut struct_types = Vec::with_capacity(fields.len());
            let mut struct_values = std::collections::HashMap::with_capacity(fields.len());
            for (name, field) in fields {
                let (ty, value) =
                    infer(field, join_field(&path, name)).map_err(|e| e.at_field(name))?;
                struct_types.push(QueryParameterStructType {
                    name: name.clone(),
                    r#type: Some(ty),
                    ..Default::default()
                });
                struct_values.insert(name.clone(), value);
            }
            Ok((
                QueryParameterType {
                    r#type: BqKind::Struct.name().to_string(),
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

/// The parameter type of a column type in a mode; REPEATED is an ARRAY of it.
fn field_param_type(ty: &BigQueryFieldType, mode: BigQueryFieldMode) -> QueryParameterType {
    let element = match ty {
        BigQueryFieldType::Range(element) => range_type((*element).into()),
        BigQueryFieldType::Struct(fields) => QueryParameterType {
            r#type: BqKind::Struct.name().to_string(),
            struct_types: fields
                .iter()
                .map(|f| QueryParameterStructType {
                    name: f.name.clone(),
                    r#type: Some(field_param_type(&f.field_type, f.mode)),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        },
        other => scalar_type(other.into()),
    };
    match mode {
        BigQueryFieldMode::Repeated => array_type(element),
        BigQueryFieldMode::Nullable | BigQueryFieldMode::Required => element,
    }
}

/// The value of `node` as a parameter of `ty` in `mode`. A REPEATED NULL is an empty ARRAY,
/// since BigQuery has no NULL ARRAY.
fn coerce_field(
    node: &Node,
    ty: &BigQueryFieldType,
    mode: BigQueryFieldMode,
) -> Result<QueryParameterValue, CodecError> {
    if mode != BigQueryFieldMode::Repeated {
        return coerce(node, ty);
    }
    match node {
        Node::Null => Ok(QueryParameterValue::default()),
        Node::Seq(items) => {
            let array_values = items
                .iter()
                .enumerate()
                .map(|(i, item)| match item {
                    Node::Null => Err(CodecError::new(
                        BigQueryCodecErrorKind::NullArrayElement,
                        "an ARRAY parameter cannot hold NULL",
                    )
                    .at_index(i)),
                    item => coerce(item, ty).map_err(|e| e.at_index(i)),
                })
                .collect::<Result<_, _>>()?;
            Ok(QueryParameterValue {
                array_values,
                ..Default::default()
            })
        }
        other => Err(mismatch(format!(
            "an ARRAY<{ty}> parameter takes a sequence, got {}",
            describe(other)
        ))),
    }
}

fn coerce(node: &Node, ty: &BigQueryFieldType) -> Result<QueryParameterValue, CodecError> {
    use BigQueryFieldType as T;
    let text = |t: String| Ok(text_value(t));
    match (ty, node) {
        (_, Node::Null) => Ok(QueryParameterValue::default()),
        (T::Int64, Node::Int(v)) => text(int64_text(*v)?),
        (T::Float64, Node::Float(v)) => text(float_text(*v)),
        (T::Numeric(_), node) => text(decimal_text(
            declared_decimal(node, NUMERIC_SCALE)?,
            NUMERIC_SCALE,
        )),
        (T::BigNumeric(_), node) => text(decimal_text(
            declared_decimal(node, BIGNUMERIC_SCALE)?,
            BIGNUMERIC_SCALE,
        )),
        (T::Bool, Node::Bool(v)) => text(v.to_string()),
        (T::String { .. } | T::Geography, Node::Str(v)) => text(v.clone()),
        (T::Bytes { .. }, Node::Bytes(v)) => text(base64_text(v)),
        (T::Bytes { .. }, Node::Seq(items)) => {
            let bytes = items
                .iter()
                .enumerate()
                .map(|(i, item)| match item {
                    Node::Int(v) => u8::try_from(*v)
                        .map_err(|_| out_of_range(format!("{v} is not a byte")).at_index(i)),
                    other => Err(
                        mismatch(format!("BYTES takes bytes, got {}", describe(other))).at_index(i),
                    ),
                })
                .collect::<Result<Vec<u8>, _>>()?;
            text(base64_text(&bytes))
        }
        (T::Date | T::Time | T::DateTime | T::Timestamp, node) => {
            let kind = BqKind::from(ty);
            text(temporal_text(kind, declared_temporal(kind, node)?)?)
        }
        (T::Json, Node::Json(v) | Node::Str(v)) => text(v.clone()),
        (T::Interval, Node::Interval(v)) => text(interval_text(v)?),
        (T::Interval, Node::Str(v)) => text(interval_text(&BigQueryInterval::parse_bq(v)?)?),
        (T::Range(element), Node::Range(start, end)) => range_value(start, end, (*element).into()),
        (T::Struct(fields), Node::Struct(entries)) => {
            if let Some((name, _)) = entries
                .iter()
                .find(|(name, _)| !fields.iter().any(|f| &f.name == name))
            {
                return Err(CodecError::new(
                    BigQueryCodecErrorKind::UnknownField,
                    format!("the declared {ty} has no field `{name}`"),
                )
                .at_field(name));
            }
            let mut struct_values = std::collections::HashMap::with_capacity(fields.len());
            for field in fields {
                let node = entries
                    .iter()
                    .find(|(name, _)| *name == field.name)
                    .map_or(&Node::Null, |(_, node)| node);
                let value = coerce_field(node, &field.field_type, field.mode)
                    .map_err(|e| e.at_field(&field.name))?;
                struct_values.insert(field.name.clone(), value);
            }
            Ok(QueryParameterValue {
                struct_values,
                ..Default::default()
            })
        }
        (ty, other) => Err(mismatch(format!(
            "a {ty} parameter takes no {}",
            describe(other)
        ))),
    }
}

fn describe(node: &Node) -> String {
    match node {
        Node::Null => "NULL".into(),
        Node::Bool(_) => "bool".into(),
        Node::Int(_) => "integer".into(),
        Node::Float(_) => "float".into(),
        Node::Str(_) => "string".into(),
        Node::Bytes(_) => "bytes".into(),
        Node::Temporal(kind, _) => format!("{} wrapper", kind.name()),
        Node::Json(_) => "BigQueryJson".into(),
        Node::Decimal(_) => "BigQueryDecimal".into(),
        Node::Interval(_) => "BigQueryInterval".into(),
        Node::Range(..) => "BigQueryRange".into(),
        Node::Seq(_) => "sequence".into(),
        Node::Struct(_) => "struct or map".into(),
    }
}

fn range_value(
    start: &Node,
    end: &Node,
    element: BqKind,
) -> Result<QueryParameterValue, CodecError> {
    let bound = |node: &Node, name: &str| -> Result<Option<Box<QueryParameterValue>>, CodecError> {
        match node {
            Node::Null => Ok(None),
            node => {
                let micros = declared_temporal(element, node).map_err(|e| e.at_field(name))?;
                Ok(Some(Box::new(text_value(
                    temporal_text(element, micros).map_err(|e| e.at_field(name))?,
                ))))
            }
        }
    };
    Ok(QueryParameterValue {
        range_value: Some(Box::new(RangeValue {
            start: bound(start, "start")?,
            end: bound(end, "end")?,
        })),
        ..Default::default()
    })
}

/// The BigQuery integer of a temporal value given for a parameter of `kind`: the wrapper of
/// that kind, the integer form, or the text form.
fn declared_temporal(kind: BqKind, node: &Node) -> Result<i64, CodecError> {
    match node {
        Node::Temporal(k, v) if *k == kind => Ok(*v),
        Node::Temporal(k, _) => Err(mismatch(format!(
            "a {} wrapper on a {} parameter",
            k.name(),
            kind.name()
        ))),
        Node::Int(v) => i64::try_from(*v)
            .map_err(|_| out_of_range(format!("{v} is outside the {} range", kind.name()))),
        Node::Str(s) => match kind {
            BqKind::Date => civil::parse_date(s).map(i64::from),
            BqKind::Time => civil::parse_time(s),
            BqKind::DateTime => civil::parse_datetime(s),
            _ => civil::parse_timestamp(s),
        },
        other => Err(mismatch(format!(
            "a {} parameter takes no {}",
            kind.name(),
            describe(other)
        ))),
    }
}

/// The unscaled value at `scale` of a decimal given for a NUMERIC or BIGNUMERIC parameter: the
/// text, a `BigQueryDecimal`, an integer, or an `f64` rounded at the scale.
fn declared_decimal(node: &Node, scale: u32) -> Result<arrow_buffer::i256, CodecError> {
    let parse = |s: &str| {
        if scale == NUMERIC_SCALE {
            parse_numeric(s)
        } else {
            parse_bignumeric(s)
        }
    };
    match node {
        Node::Str(s) | Node::Decimal(s) => parse(s),
        Node::Int(v) => parse(&v.to_string()),
        Node::Float(x) => {
            let v = crate::types::decimal::decimal_from_f64(*x, scale)?;
            parse(&decimal_text(v, scale))
        }
        other => Err(mismatch(format!(
            "a decimal parameter takes no {}",
            describe(other)
        ))),
    }
}

fn int64_text(v: i128) -> Result<String, CodecError> {
    i64::try_from(v)
        .map(|v| v.to_string())
        .map_err(|_| out_of_range(format!("{v} is outside INT64")))
}

/// The shortest text that parses back to the same `f64`, and BigQuery's names for the special
/// values.
fn float_text(x: f64) -> String {
    if x.is_nan() {
        "NaN".into()
    } else if x == f64::INFINITY {
        "Infinity".into()
    } else if x == f64::NEG_INFINITY {
        "-Infinity".into()
    } else {
        // `Debug` is the shortest round-trip form and switches to an exponent where `Display`
        // would print hundreds of digits.
        format!("{x:?}")
    }
}

fn base64_text(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn decimal_text(v: arrow_buffer::i256, scale: u32) -> String {
    let mut out = String::new();
    fmt_decimal_i256(v, scale, &mut out);
    out
}

fn interval_text(interval: &BigQueryInterval) -> Result<String, CodecError> {
    if interval.nanos % 1000 != 0 {
        return Err(out_of_range(format!(
            "INTERVAL time part of {} ns is not a whole number of microseconds",
            interval.nanos
        )));
    }
    let mut out = String::new();
    interval.write_bq(&mut out);
    Ok(out)
}

/// The parameter text of a temporal integer: `YYYY-MM-DD`, `HH:MM:SS[.ffffff]`, the two with a
/// space for DATETIME, and with `+00:00` for TIMESTAMP, the form the probe sent.
fn temporal_text(kind: BqKind, v: i64) -> Result<String, CodecError> {
    let outside = || out_of_range(format!("{v} is outside BigQuery's {} range", kind.name()));
    let mut out = String::new();
    let date_time = |micros: i64, out: &mut String| {
        // Every arm below checks the range before formatting, so the day count fits i32.
        civil::fmt_date(micros.div_euclid(civil::MICROS_PER_DAY) as i32, out);
        out.push(' ');
        civil::fmt_time(micros.rem_euclid(civil::MICROS_PER_DAY), out);
    };
    let datetime_min = i64::from(civil::DATE_MIN_DAYS) * civil::MICROS_PER_DAY;
    let datetime_end = (i64::from(civil::DATE_MAX_DAYS) + 1) * civil::MICROS_PER_DAY;
    match kind {
        BqKind::Date => {
            let days = i32::try_from(v)
                .ok()
                .filter(|d| (civil::DATE_MIN_DAYS..=civil::DATE_MAX_DAYS).contains(d))
                .ok_or_else(outside)?;
            civil::fmt_date(days, &mut out);
        }
        BqKind::Time => {
            if !(0..civil::MICROS_PER_DAY).contains(&v) {
                return Err(outside());
            }
            civil::fmt_time(v, &mut out);
        }
        BqKind::DateTime => {
            if !(datetime_min..datetime_end).contains(&v) {
                return Err(outside());
            }
            date_time(v, &mut out);
        }
        BqKind::Timestamp => {
            if !(civil::TIMESTAMP_MIN_MICROS..=civil::TIMESTAMP_MAX_MICROS).contains(&v) {
                return Err(outside());
            }
            date_time(v, &mut out);
            out.push_str("+00:00");
        }
        other => return Err(mismatch(format!("{} is not a temporal type", other.name()))),
    }
    Ok(out)
}

#[cfg(test)]
mod tests;
