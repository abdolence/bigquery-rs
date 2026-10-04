//! One vocabulary for the schemas that three sources name differently: the v2 API's legacy
//! type strings, the Storage API's enums, and the physical types of an Arrow schema.

use crate::db::proto::NonEmpty;
use crate::errors::BigQueryError;
use crate::types::error::CodecError;
use crate::types::kind::BqKind;
use crate::BigQueryResult;
use arrow_schema::{DataType, Field, TimeUnit};
use gcloud_sdk::google::cloud::bigquery::storage::v1 as storage;
use gcloud_sdk::google::cloud::bigquery::v2;
use std::fmt::{Display, Formatter};
use storage::table_field_schema::{Mode as StorageMode, Type as StorageType};

/// The type of a column, with the parameters that are part of it.
///
/// `Display` prints GoogleSQL type syntax, as in `STRING(10)`, `NUMERIC(10, 2)`,
/// `RANGE<DATE>` and `STRUCT<a INT64, b ARRAY<STRING>>`. An ARRAY column is the
/// [`Repeated`](BigQueryFieldMode::Repeated) mode of its element type, not a type of its own.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum BigQueryFieldType {
    /// INT64, `INTEGER` in the v2 API.
    Int64,
    /// FLOAT64, `FLOAT` in the v2 API.
    Float64,
    /// NUMERIC, `DECIMAL` in the v2 API, with its declared precision and scale if any.
    Numeric(Option<BigQueryDecimalParams>),
    /// BIGNUMERIC, `BIGDECIMAL` in the v2 API, with its declared precision and scale if any.
    BigNumeric(Option<BigQueryDecimalParams>),
    /// BOOL, `BOOLEAN` in the v2 API.
    Bool,
    /// STRING, with its declared maximum length in characters if any.
    String {
        /// The `n` of `STRING(n)`.
        max_length: Option<u64>,
    },
    /// BYTES, with its declared maximum length in bytes if any.
    Bytes {
        /// The `n` of `BYTES(n)`.
        max_length: Option<u64>,
    },
    /// DATE.
    Date,
    /// TIME.
    Time,
    /// DATETIME.
    DateTime,
    /// TIMESTAMP with microsecond precision.
    Timestamp,
    /// GEOGRAPHY.
    Geography,
    /// JSON.
    Json,
    /// INTERVAL.
    Interval,
    /// RANGE of the element type.
    Range(BigQueryRangeElementType),
    /// STRUCT, `RECORD` in the v2 API, with its fields in order.
    Struct(Vec<BigQueryFieldSchema>),
}

/// The declared precision and scale of a NUMERIC or BIGNUMERIC column. A declared precision
/// without a scale has scale 0, so `NUMERIC(10)` equals `NUMERIC(10, 0)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct BigQueryDecimalParams {
    /// The total number of digits.
    pub precision: u8,
    /// The number of digits after the point.
    pub scale: u8,
}

/// The element type of a RANGE column.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BigQueryRangeElementType {
    /// `RANGE<DATE>`.
    Date,
    /// `RANGE<DATETIME>`.
    DateTime,
    /// `RANGE<TIMESTAMP>`.
    Timestamp,
}

/// The mode of a column.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BigQueryFieldMode {
    /// May be NULL. A column with an empty mode, as DDL creates it, is NULLABLE.
    Nullable,
    /// Never NULL.
    Required,
    /// An ARRAY of the column's type.
    Repeated,
}

impl BigQueryFieldMode {
    /// The name the v2 API and DDL use, as in `NULLABLE`.
    pub(crate) fn name(self) -> &'static str {
        match self {
            BigQueryFieldMode::Nullable => "NULLABLE",
            BigQueryFieldMode::Required => "REQUIRED",
            BigQueryFieldMode::Repeated => "REPEATED",
        }
    }

    /// The mode a v2 `mode` names, in any case. An empty one is NULLABLE, as DDL creates it.
    pub(crate) fn parse(name: &str) -> Option<Self> {
        match name.to_ascii_uppercase().as_str() {
            "" | "NULLABLE" => Some(BigQueryFieldMode::Nullable),
            "REQUIRED" => Some(BigQueryFieldMode::Required),
            "REPEATED" => Some(BigQueryFieldMode::Repeated),
            _ => None,
        }
    }
}

impl Display for BigQueryFieldMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// One column, or one field of a STRUCT.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct BigQueryFieldSchema {
    /// The name as BigQuery returns it.
    pub name: String,
    /// The type, with its parameters.
    pub field_type: BigQueryFieldType,
    /// The mode.
    pub mode: BigQueryFieldMode,
    /// The column description; an empty one is `None`.
    pub description: Option<String>,
    /// The column's default value expression, such as `CURRENT_TIMESTAMP()`.
    pub default_value_expression: Option<String>,
}

/// A table schema, normalised so that schemas from the v2 API, the Storage API and Arrow
/// compare with `==`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct BigQueryTableSchema {
    /// The columns, in table order.
    pub fields: Vec<BigQueryFieldSchema>,
}

/// `max_length`, `precision` and `scale` as the proto carries them: 0 is unset.
struct TypeParams {
    max_length: i64,
    precision: i64,
    scale: i64,
    timestamp_precision: Option<i64>,
}

impl TypeParams {
    fn max_length(&self) -> Result<Option<u64>, CodecError> {
        match self.max_length {
            0 => Ok(None),
            n => u64::try_from(n)
                .map(Some)
                .map_err(|_| CodecError::unsupported(format!("max_length {n} is negative"))),
        }
    }

    fn decimal(&self) -> Result<Option<BigQueryDecimalParams>, CodecError> {
        if self.precision == 0 {
            return Ok(None);
        }
        let narrow = |what: &str, v: i64| {
            u8::try_from(v)
                .map_err(|_| CodecError::unsupported(format!("{what} {v} is out of range")))
        };
        Ok(Some(BigQueryDecimalParams {
            precision: narrow("precision", self.precision)?,
            scale: narrow("scale", self.scale)?,
        }))
    }

    fn timestamp(&self) -> Result<BigQueryFieldType, CodecError> {
        match self.timestamp_precision {
            None | Some(6) => Ok(BigQueryFieldType::Timestamp),
            Some(p) => Err(CodecError::unsupported(format!(
                "TIMESTAMP with timestamp_precision {p} is not supported, only 6"
            ))),
        }
    }
}

/// One v2 column. The error keeps its path as a [`CodecError`], which the public schema
/// conversion turns into a [`BigQueryError`], so this is not a public `TryFrom`.
fn field_from_v2(field: &v2::TableFieldSchema) -> Result<BigQueryFieldSchema, CodecError> {
    let at = |err: CodecError| err.at_field(&field.name);
    let params = TypeParams {
        max_length: field.max_length,
        precision: field.precision,
        scale: field.scale,
        timestamp_precision: field.timestamp_precision,
    };
    let field_type = match field.r#type.to_ascii_uppercase().as_str() {
        "INTEGER" | "INT64" => BigQueryFieldType::Int64,
        "FLOAT" | "FLOAT64" => BigQueryFieldType::Float64,
        "BOOLEAN" | "BOOL" => BigQueryFieldType::Bool,
        "NUMERIC" | "DECIMAL" => BigQueryFieldType::Numeric(params.decimal().map_err(at)?),
        "BIGNUMERIC" | "BIGDECIMAL" => BigQueryFieldType::BigNumeric(params.decimal().map_err(at)?),
        "STRING" => BigQueryFieldType::String {
            max_length: params.max_length().map_err(at)?,
        },
        "BYTES" => BigQueryFieldType::Bytes {
            max_length: params.max_length().map_err(at)?,
        },
        "DATE" => BigQueryFieldType::Date,
        "TIME" => BigQueryFieldType::Time,
        "DATETIME" => BigQueryFieldType::DateTime,
        "TIMESTAMP" => params.timestamp().map_err(at)?,
        "GEOGRAPHY" => BigQueryFieldType::Geography,
        "JSON" => BigQueryFieldType::Json,
        "INTERVAL" => BigQueryFieldType::Interval,
        "RANGE" => BigQueryFieldType::Range(
            BigQueryRangeElementType::parse(
                field.range_element_type.as_ref().map(|e| e.r#type.as_str()),
            )
            .map_err(at)?,
        ),
        "RECORD" | "STRUCT" => BigQueryFieldType::Struct(
            field
                .fields
                .iter()
                .map(field_from_v2)
                .collect::<Result<_, _>>()
                .map_err(at)?,
        ),
        other => {
            return Err(at(CodecError::unsupported(format!(
                "column type `{other}` is not supported"
            ))))
        }
    };
    let mode = BigQueryFieldMode::parse(&field.mode).ok_or_else(|| {
        at(CodecError::unsupported(format!(
            "column mode `{}` is not supported",
            field.mode.to_ascii_uppercase()
        )))
    })?;
    Ok(BigQueryFieldSchema {
        name: field.name.clone(),
        field_type,
        mode,
        description: field.description.clone().and_then(NonEmpty::non_empty),
        default_value_expression: field
            .default_value_expression
            .clone()
            .and_then(NonEmpty::non_empty),
    })
}

fn storage_type(value: i32) -> Result<StorageType, CodecError> {
    match StorageType::try_from(value) {
        Ok(StorageType::Unspecified) | Err(_) => Err(CodecError::unsupported(format!(
            "Storage API column type {value} is not supported"
        ))),
        Ok(ty) => Ok(ty),
    }
}

/// One Storage API column, as [`field_from_v2`].
fn field_from_storage(
    field: &storage::TableFieldSchema,
) -> Result<BigQueryFieldSchema, CodecError> {
    let at = |err: CodecError| err.at_field(&field.name);
    let params = TypeParams {
        max_length: field.max_length,
        precision: field.precision,
        scale: field.scale,
        timestamp_precision: field.timestamp_precision,
    };
    let field_type = match storage_type(field.r#type).map_err(at)? {
        StorageType::Int64 => BigQueryFieldType::Int64,
        StorageType::Double => BigQueryFieldType::Float64,
        StorageType::Bool => BigQueryFieldType::Bool,
        StorageType::Numeric => BigQueryFieldType::Numeric(params.decimal().map_err(at)?),
        StorageType::Bignumeric => BigQueryFieldType::BigNumeric(params.decimal().map_err(at)?),
        StorageType::String => BigQueryFieldType::String {
            max_length: params.max_length().map_err(at)?,
        },
        StorageType::Bytes => BigQueryFieldType::Bytes {
            max_length: params.max_length().map_err(at)?,
        },
        StorageType::Date => BigQueryFieldType::Date,
        StorageType::Time => BigQueryFieldType::Time,
        StorageType::Datetime => BigQueryFieldType::DateTime,
        StorageType::Timestamp => params.timestamp().map_err(at)?,
        StorageType::Geography => BigQueryFieldType::Geography,
        StorageType::Json => BigQueryFieldType::Json,
        StorageType::Interval => BigQueryFieldType::Interval,
        StorageType::Range => {
            let element = field
                .range_element_type
                .as_ref()
                .map(|e| storage_type(e.r#type).map(|ty| ty.as_str_name()))
                .transpose()
                .map_err(at)?;
            BigQueryFieldType::Range(BigQueryRangeElementType::parse(element).map_err(at)?)
        }
        StorageType::Struct => BigQueryFieldType::Struct(
            field
                .fields
                .iter()
                .map(field_from_storage)
                .collect::<Result<_, _>>()
                .map_err(at)?,
        ),
        StorageType::Unspecified => {
            return Err(at(CodecError::unsupported("TYPE_UNSPECIFIED".to_string())))
        }
    };
    let mode = match StorageMode::try_from(field.mode) {
        Ok(StorageMode::Unspecified | StorageMode::Nullable) => BigQueryFieldMode::Nullable,
        Ok(StorageMode::Required) => BigQueryFieldMode::Required,
        Ok(StorageMode::Repeated) => BigQueryFieldMode::Repeated,
        Err(_) => {
            return Err(at(CodecError::unsupported(format!(
                "Storage API column mode {} is not supported",
                field.mode
            ))))
        }
    };
    Ok(BigQueryFieldSchema {
        name: field.name.clone(),
        field_type,
        mode,
        description: field.description.clone().non_empty(),
        default_value_expression: field.default_value_expression.clone().non_empty(),
    })
}

/// The schema of `GetWriteStream` and `CreateWriteStream`. A type the crate does not handle is
/// a [`DeserializeError`](BigQueryError::DeserializeError) of kind `UnsupportedType`, with the
/// column's path.
impl TryFrom<&storage::TableSchema> for BigQueryTableSchema {
    type Error = BigQueryError;

    fn try_from(schema: &storage::TableSchema) -> Result<Self, Self::Error> {
        schema
            .fields
            .iter()
            .map(field_from_storage)
            .collect::<Result<_, _>>()
            .map(|fields| BigQueryTableSchema { fields })
            .map_err(CodecError::into_deserialize)
    }
}

/// The schema of `GetTable` and query results, with the v2 API's legacy type names. A type the
/// crate does not handle is a [`DeserializeError`](BigQueryError::DeserializeError) of kind
/// `UnsupportedType`, with the column's path.
impl TryFrom<&v2::TableSchema> for BigQueryTableSchema {
    type Error = BigQueryError;

    fn try_from(schema: &v2::TableSchema) -> Result<Self, Self::Error> {
        schema
            .fields
            .iter()
            .map(field_from_v2)
            .collect::<Result<_, _>>()
            .map(|fields| BigQueryTableSchema { fields })
            .map_err(CodecError::into_deserialize)
    }
}

impl From<&BigQueryFieldSchema> for v2::TableFieldSchema {
    fn from(field: &BigQueryFieldSchema) -> Self {
        let mut out = v2::TableFieldSchema {
            name: field.name.clone(),
            r#type: BqKind::from(&field.field_type).name().to_string(),
            mode: field.mode.name().to_string(),
            description: field.description.clone(),
            default_value_expression: field.default_value_expression.clone(),
            ..Default::default()
        };
        match &field.field_type {
            BigQueryFieldType::String { max_length } | BigQueryFieldType::Bytes { max_length } => {
                // A length beyond i64 cannot be declared in BigQuery.
                out.max_length = max_length.map_or(0, |n| i64::try_from(n).unwrap_or(i64::MAX));
            }
            BigQueryFieldType::Numeric(Some(params))
            | BigQueryFieldType::BigNumeric(Some(params)) => {
                out.precision = i64::from(params.precision);
                out.scale = i64::from(params.scale);
            }
            BigQueryFieldType::Range(element) => {
                out.range_element_type = Some(v2::table_field_schema::FieldElementType {
                    r#type: element.to_string(),
                });
            }
            BigQueryFieldType::Struct(fields) => {
                out.fields = fields.iter().map(v2::TableFieldSchema::from).collect();
            }
            _ => {}
        }
        out
    }
}

/// The schema for `InsertTable` and `PatchTable`, with the standard type names, which BigQuery
/// accepts and stores under the legacy ones.
impl From<&BigQueryTableSchema> for v2::TableSchema {
    fn from(schema: &BigQueryTableSchema) -> Self {
        v2::TableSchema {
            fields: schema
                .fields
                .iter()
                .map(v2::TableFieldSchema::from)
                .collect(),
            ..Default::default()
        }
    }
}

impl BigQueryTableSchema {
    /// The schema of a read session or an inline query result.
    ///
    /// Arrow carries no type parameters, so `max_length`, precision and scale are unset, and
    /// no descriptions or default values.
    ///
    /// # Errors
    /// [`BigQueryError::DeserializeError`] of kind `UnsupportedType` for an Arrow type that
    /// BigQuery does not send, with the column's path.
    pub fn from_arrow(schema: &arrow_schema::Schema) -> BigQueryResult<Self> {
        schema
            .fields()
            .iter()
            .map(|field| field_from_arrow(field))
            .collect::<Result<_, _>>()
            .map(|fields| BigQueryTableSchema { fields })
            .map_err(CodecError::into_deserialize)
    }
}

fn field_from_arrow(field: &Field) -> Result<BigQueryFieldSchema, CodecError> {
    let at = |err: CodecError| err.at_field(field.name());
    let (element, kind, mode) = match field.data_type() {
        DataType::List(item) => (
            item.as_ref(),
            BqKind::from_arrow_list_item(field, item),
            BigQueryFieldMode::Repeated,
        ),
        _ => (
            field,
            BqKind::from_arrow(field),
            if field.is_nullable() {
                BigQueryFieldMode::Nullable
            } else {
                BigQueryFieldMode::Required
            },
        ),
    };
    let kind = kind.ok_or_else(|| {
        at(CodecError::unsupported(format!(
            "Arrow type {} is not one BigQuery sends",
            element.data_type()
        )))
    })?;
    let field_type = match kind {
        BqKind::Int64 => BigQueryFieldType::Int64,
        BqKind::Float64 => BigQueryFieldType::Float64,
        BqKind::Bool => BigQueryFieldType::Bool,
        BqKind::String => BigQueryFieldType::String { max_length: None },
        BqKind::Bytes => BigQueryFieldType::Bytes { max_length: None },
        BqKind::Date => BigQueryFieldType::Date,
        BqKind::Time => BigQueryFieldType::Time,
        BqKind::DateTime => BigQueryFieldType::DateTime,
        BqKind::Timestamp => BigQueryFieldType::Timestamp,
        BqKind::Numeric => BigQueryFieldType::Numeric(None),
        BqKind::BigNumeric => BigQueryFieldType::BigNumeric(None),
        BqKind::Geography => BigQueryFieldType::Geography,
        BqKind::Json => BigQueryFieldType::Json,
        BqKind::Interval => BigQueryFieldType::Interval,
        BqKind::Range => BigQueryFieldType::Range(range_element_from_arrow(element).map_err(at)?),
        BqKind::Struct => match element.data_type() {
            DataType::Struct(children) => BigQueryFieldType::Struct(
                children
                    .iter()
                    .map(|child| field_from_arrow(child))
                    .collect::<Result<_, _>>()
                    .map_err(at)?,
            ),
            other => {
                return Err(at(CodecError::unsupported(format!(
                    "Arrow type {other} is not a STRUCT"
                ))))
            }
        },
    };
    Ok(BigQueryFieldSchema {
        name: field.name().clone(),
        field_type,
        mode,
        description: None,
        default_value_expression: None,
    })
}

fn range_element_from_arrow(range: &Field) -> Result<BigQueryRangeElementType, CodecError> {
    let start = match range.data_type() {
        DataType::Struct(children) => children.iter().find(|child| child.name() == "start"),
        _ => None,
    };
    match start.map(|start| start.data_type()) {
        Some(DataType::Date32) => Ok(BigQueryRangeElementType::Date),
        Some(DataType::Timestamp(TimeUnit::Microsecond, None)) => {
            Ok(BigQueryRangeElementType::DateTime)
        }
        Some(DataType::Timestamp(TimeUnit::Microsecond, Some(_))) => {
            Ok(BigQueryRangeElementType::Timestamp)
        }
        other => Err(CodecError::unsupported(format!(
            "a RANGE whose start is {other:?} is not one BigQuery sends"
        ))),
    }
}

impl BigQueryRangeElementType {
    /// The element type a v2 or Storage API `range_element_type` names, in any case; `None` is
    /// a RANGE that names none.
    fn parse(name: Option<&str>) -> Result<Self, CodecError> {
        match name.map(str::to_ascii_uppercase).as_deref() {
            Some("DATE") => Ok(BigQueryRangeElementType::Date),
            Some("DATETIME") => Ok(BigQueryRangeElementType::DateTime),
            Some("TIMESTAMP") => Ok(BigQueryRangeElementType::Timestamp),
            Some(other) => Err(CodecError::unsupported(format!(
                "RANGE of {other} is not supported"
            ))),
            None => Err(CodecError::unsupported(
                "RANGE without a range_element_type",
            )),
        }
    }
}

impl Display for BigQueryRangeElementType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            BigQueryRangeElementType::Date => "DATE",
            BigQueryRangeElementType::DateTime => "DATETIME",
            BigQueryRangeElementType::Timestamp => "TIMESTAMP",
        })
    }
}

impl Display for BigQueryFieldType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let name = BqKind::from(self).name();
        match self {
            BigQueryFieldType::String {
                max_length: Some(n),
            }
            | BigQueryFieldType::Bytes {
                max_length: Some(n),
            } => write!(f, "{name}({n})"),
            BigQueryFieldType::Numeric(Some(p)) | BigQueryFieldType::BigNumeric(Some(p)) => {
                write!(f, "{name}({}, {})", p.precision, p.scale)
            }
            BigQueryFieldType::Range(element) => write!(f, "RANGE<{element}>"),
            BigQueryFieldType::Struct(fields) => {
                f.write_str("STRUCT<")?;
                for (i, field) in fields.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    match field.mode {
                        BigQueryFieldMode::Repeated => {
                            write!(f, "{} ARRAY<{}>", field.name, field.field_type)?
                        }
                        _ => write!(f, "{} {}", field.name, field.field_type)?,
                    }
                }
                f.write_str(">")
            }
            _ => f.write_str(name),
        }
    }
}

#[cfg(test)]
mod tests;
