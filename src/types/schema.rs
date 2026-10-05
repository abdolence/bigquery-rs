//! One vocabulary for the schemas that three sources name differently: the v2 API's legacy
//! type strings, the Storage API's enums, and the physical types of an Arrow schema.

use crate::errors::BigQueryError;
use crate::types::error::CodecError;
use crate::types::kind::FieldKind;
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
fn storage_type(value: i32) -> Result<StorageType, CodecError> {
    match StorageType::try_from(value) {
        Ok(StorageType::Unspecified) | Err(_) => Err(CodecError::unsupported(format!(
            "Storage API column type {value} is not supported"
        ))),
        Ok(storage_type) => Ok(storage_type),
    }
}

impl BigQueryFieldSchema {
    /// One v2 column. The error keeps its path as a [`CodecError`], which the public schema
    /// conversion turns into a [`BigQueryError`], so this is not a public `TryFrom`.
    fn from_v2(field: &v2::TableFieldSchema) -> Result<BigQueryFieldSchema, CodecError> {
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
            "BIGNUMERIC" | "BIGDECIMAL" => {
                BigQueryFieldType::BigNumeric(params.decimal().map_err(at)?)
            }
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
                    .map(BigQueryFieldSchema::from_v2)
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
            description: field.description.clone().filter(|v| !v.is_empty()),
            default_value_expression: field
                .default_value_expression
                .clone()
                .filter(|v| !v.is_empty()),
        })
    }

    /// One Storage API column, as [`BigQueryFieldSchema::from_v2`].
    fn from_storage(field: &storage::TableFieldSchema) -> Result<BigQueryFieldSchema, CodecError> {
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
                    .map(BigQueryFieldSchema::from_storage)
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
            description: Some(field.description.clone()).filter(|v| !v.is_empty()),
            default_value_expression: Some(field.default_value_expression.clone())
                .filter(|v| !v.is_empty()),
        })
    }
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
            .map(BigQueryFieldSchema::from_storage)
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
            .map(BigQueryFieldSchema::from_v2)
            .collect::<Result<_, _>>()
            .map(|fields| BigQueryTableSchema { fields })
            .map_err(CodecError::into_deserialize)
    }
}

impl From<&BigQueryFieldSchema> for v2::TableFieldSchema {
    fn from(field: &BigQueryFieldSchema) -> Self {
        let mut out = v2::TableFieldSchema {
            name: field.name.clone(),
            r#type: FieldKind::from(&field.field_type).name().to_string(),
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
            .map(|field| BigQueryFieldSchema::from_arrow_field(field))
            .collect::<Result<_, _>>()
            .map(|fields| BigQueryTableSchema { fields })
            .map_err(CodecError::into_deserialize)
    }
}

impl BigQueryFieldSchema {
    fn from_arrow_field(field: &Field) -> Result<BigQueryFieldSchema, CodecError> {
        let at = |err: CodecError| err.at_field(field.name());
        let (element, kind, mode) = match field.data_type() {
            DataType::List(item) => (
                item.as_ref(),
                FieldKind::from_arrow_list_item(field, item),
                BigQueryFieldMode::Repeated,
            ),
            _ => (
                field,
                FieldKind::from_arrow(field),
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
            FieldKind::Int64 => BigQueryFieldType::Int64,
            FieldKind::Float64 => BigQueryFieldType::Float64,
            FieldKind::Bool => BigQueryFieldType::Bool,
            FieldKind::String => BigQueryFieldType::String { max_length: None },
            FieldKind::Bytes => BigQueryFieldType::Bytes { max_length: None },
            FieldKind::Date => BigQueryFieldType::Date,
            FieldKind::Time => BigQueryFieldType::Time,
            FieldKind::DateTime => BigQueryFieldType::DateTime,
            FieldKind::Timestamp => BigQueryFieldType::Timestamp,
            FieldKind::Numeric => BigQueryFieldType::Numeric(None),
            FieldKind::BigNumeric => BigQueryFieldType::BigNumeric(None),
            FieldKind::Geography => BigQueryFieldType::Geography,
            FieldKind::Json => BigQueryFieldType::Json,
            FieldKind::Interval => BigQueryFieldType::Interval,
            FieldKind::Range => BigQueryFieldType::Range(
                BigQueryRangeElementType::from_arrow_range(element).map_err(at)?,
            ),
            FieldKind::Struct => match element.data_type() {
                DataType::Struct(children) => BigQueryFieldType::Struct(
                    children
                        .iter()
                        .map(|child| BigQueryFieldSchema::from_arrow_field(child))
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
}

impl BigQueryRangeElementType {
    fn from_arrow_range(range: &Field) -> Result<BigQueryRangeElementType, CodecError> {
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
        let name = FieldKind::from(self).name();
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
mod tests {
    use super::*;
    use crate::db::fake::table::v2_field;
    use crate::db::fake::write::column;
    use crate::errors::BigQueryCodecErrorKind;
    use crate::types::testkit::field;
    use arrow_schema::{DataType, Field, Fields, IntervalUnit, Schema, TimeUnit};
    use std::collections::HashMap;
    use storage::table_field_schema::{Mode as StorageMode, Type as StorageType};

    fn v2_schema(fields: Vec<v2::TableFieldSchema>) -> BigQueryResult<BigQueryTableSchema> {
        BigQueryTableSchema::try_from(&v2::TableSchema {
            fields,
            ..Default::default()
        })
    }

    fn storage_schema(
        fields: Vec<storage::TableFieldSchema>,
    ) -> BigQueryResult<BigQueryTableSchema> {
        BigQueryTableSchema::try_from(&storage::TableSchema { fields })
    }

    fn unsupported(result: BigQueryResult<BigQueryTableSchema>) -> String {
        match result {
            Err(BigQueryError::DeserializeError(err)) => {
                assert_eq!(err.kind, BigQueryCodecErrorKind::UnsupportedType, "{err}");
                err.path
            }
            other => panic!("expected UnsupportedType, got {other:?}"),
        }
    }

    #[test]
    fn legacy_and_storage_type_names_normalise_to_one_type() {
        let pairs = [
            (
                "INTEGER",
                "INT64",
                StorageType::Int64,
                BigQueryFieldType::Int64,
            ),
            (
                "FLOAT",
                "FLOAT64",
                StorageType::Double,
                BigQueryFieldType::Float64,
            ),
            (
                "BOOLEAN",
                "BOOL",
                StorageType::Bool,
                BigQueryFieldType::Bool,
            ),
            (
                "DECIMAL",
                "NUMERIC",
                StorageType::Numeric,
                BigQueryFieldType::Numeric(None),
            ),
            (
                "BIGDECIMAL",
                "BIGNUMERIC",
                StorageType::Bignumeric,
                BigQueryFieldType::BigNumeric(None),
            ),
            (
                "STRING",
                "string",
                StorageType::String,
                BigQueryFieldType::String { max_length: None },
            ),
            (
                "BYTES",
                "Bytes",
                StorageType::Bytes,
                BigQueryFieldType::Bytes { max_length: None },
            ),
            ("DATE", "date", StorageType::Date, BigQueryFieldType::Date),
            ("TIME", "TIME", StorageType::Time, BigQueryFieldType::Time),
            (
                "DATETIME",
                "DATETIME",
                StorageType::Datetime,
                BigQueryFieldType::DateTime,
            ),
            (
                "TIMESTAMP",
                "TIMESTAMP",
                StorageType::Timestamp,
                BigQueryFieldType::Timestamp,
            ),
            (
                "GEOGRAPHY",
                "GEOGRAPHY",
                StorageType::Geography,
                BigQueryFieldType::Geography,
            ),
            ("JSON", "JSON", StorageType::Json, BigQueryFieldType::Json),
            (
                "INTERVAL",
                "INTERVAL",
                StorageType::Interval,
                BigQueryFieldType::Interval,
            ),
        ];
        for (legacy, standard, storage_type, expected) in pairs {
            let want = BigQueryTableSchema {
                fields: vec![field(
                    "order_id",
                    expected.clone(),
                    BigQueryFieldMode::Required,
                )],
            };
            assert_eq!(
                v2_schema(vec![v2_field("order_id", legacy, "REQUIRED")])
                    .ok()
                    .as_ref(),
                Some(&want),
                "{legacy}"
            );
            assert_eq!(
                v2_schema(vec![v2_field("order_id", standard, "required")])
                    .ok()
                    .as_ref(),
                Some(&want),
                "{standard}"
            );
            assert_eq!(
                storage_schema(vec![column(
                    "order_id",
                    storage_type,
                    StorageMode::Required
                )])
                .ok()
                .as_ref(),
                Some(&want),
                "{storage_type:?}"
            );
        }

        let mut legacy_record = v2_field("line_items", "RECORD", "REPEATED");
        legacy_record.fields = vec![v2_field("quantity", "INTEGER", "NULLABLE")];
        let mut storage_struct = column("line_items", StorageType::Struct, StorageMode::Repeated);
        storage_struct.fields = vec![column(
            "quantity",
            StorageType::Int64,
            StorageMode::Nullable,
        )];
        let mut legacy_range = v2_field("shipping_window", "RANGE", "NULLABLE");
        legacy_range.range_element_type = Some(v2::table_field_schema::FieldElementType {
            r#type: "DATETIME".to_string(),
        });
        let mut storage_range =
            column("shipping_window", StorageType::Range, StorageMode::Nullable);
        storage_range.range_element_type = Some(storage::table_field_schema::FieldElementType {
            r#type: StorageType::Datetime.into(),
        });
        let from_v2 = v2_schema(vec![legacy_record, legacy_range]).expect("valid test input");
        let from_storage =
            storage_schema(vec![storage_struct, storage_range]).expect("valid test input");
        assert_eq!(from_v2, from_storage);
        assert_eq!(
            from_v2.fields[0].field_type,
            BigQueryFieldType::Struct(vec![field(
                "quantity",
                BigQueryFieldType::Int64,
                BigQueryFieldMode::Nullable
            )])
        );
        assert_eq!(
            from_v2.fields[1].field_type,
            BigQueryFieldType::Range(BigQueryRangeElementType::DateTime)
        );

        let mut described = v2_field("details", "STRING", "NULLABLE");
        described.description = Some("a note".to_string());
        described.default_value_expression = Some("'x'".to_string());
        described.max_length = 10;
        let mut storage_described = column("details", StorageType::String, StorageMode::Nullable);
        storage_described.description = "a note".to_string();
        storage_described.default_value_expression = "'x'".to_string();
        storage_described.max_length = 10;
        let from_v2 = v2_schema(vec![described]).expect("valid test input");
        assert_eq!(
            from_v2,
            storage_schema(vec![storage_described]).expect("valid test input")
        );
        assert_eq!(
            from_v2.fields[0].field_type,
            BigQueryFieldType::String {
                max_length: Some(10)
            }
        );
        assert_eq!(from_v2.fields[0].description.as_deref(), Some("a note"));

        let back = v2::TableSchema::from(&from_v2);
        assert_eq!(back.fields[0].r#type, "STRING");
        assert_eq!(back.fields[0].mode, "NULLABLE");
        assert_eq!(back.fields[0].max_length, 10);
        assert_eq!(v2_schema(back.fields).ok(), Some(from_v2));
    }

    #[test]
    fn standard_names_go_out_to_v2_and_read_back_equal() {
        let schema = BigQueryTableSchema {
            fields: vec![
                field(
                    "order_id",
                    BigQueryFieldType::Int64,
                    BigQueryFieldMode::Required,
                ),
                field(
                    "price",
                    BigQueryFieldType::Float64,
                    BigQueryFieldMode::Nullable,
                ),
                field(
                    "in_stock",
                    BigQueryFieldType::Bool,
                    BigQueryFieldMode::Repeated,
                ),
                field(
                    "total",
                    BigQueryFieldType::Numeric(Some(BigQueryDecimalParams {
                        precision: 10,
                        scale: 2,
                    })),
                    BigQueryFieldMode::Nullable,
                ),
                field(
                    "shipping_window",
                    BigQueryFieldType::Range(BigQueryRangeElementType::Timestamp),
                    BigQueryFieldMode::Nullable,
                ),
                field(
                    "details",
                    BigQueryFieldType::Struct(vec![field(
                        "attributes",
                        BigQueryFieldType::Json,
                        BigQueryFieldMode::Nullable,
                    )]),
                    BigQueryFieldMode::Nullable,
                ),
            ],
        };
        let v2 = v2::TableSchema::from(&schema);
        let names: Vec<&str> = v2
            .fields
            .iter()
            .map(|field| field.r#type.as_str())
            .collect();
        assert_eq!(
            names,
            ["INT64", "FLOAT64", "BOOL", "NUMERIC", "RANGE", "STRUCT"]
        );
        assert_eq!(v2.fields[3].precision, 10);
        assert_eq!(v2.fields[3].scale, 2);
        assert_eq!(
            v2.fields[4]
                .range_element_type
                .as_ref()
                .map(|element| element.r#type.as_str()),
            Some("TIMESTAMP")
        );
        assert_eq!(BigQueryTableSchema::try_from(&v2).ok(), Some(schema));
    }

    #[test]
    fn empty_mode_is_nullable() {
        let schema = v2_schema(vec![v2_field("order_id", "INT64", "")]).expect("valid test input");
        assert_eq!(schema.fields[0].mode, BigQueryFieldMode::Nullable);
        let schema = storage_schema(vec![column(
            "order_id",
            StorageType::Int64,
            StorageMode::Unspecified,
        )])
        .expect("valid test input");
        assert_eq!(schema.fields[0].mode, BigQueryFieldMode::Nullable);
        assert_eq!(
            unsupported(v2_schema(vec![v2_field("order_id", "INT64", "OPTIONAL")])),
            "order_id"
        );
    }

    #[test]
    fn unknown_type_name_is_unsupported() {
        assert_eq!(
            unsupported(v2_schema(vec![v2_field("order_id", "INT", "NULLABLE")])),
            "order_id"
        );
        let mut nested = v2_field("line_items", "RECORD", "NULLABLE");
        nested.fields = vec![v2_field("attributes", "VARCHAR", "NULLABLE")];
        assert_eq!(
            unsupported(v2_schema(vec![nested])),
            "line_items.attributes"
        );
        assert_eq!(
            unsupported(storage_schema(vec![column(
                "order_id",
                StorageType::Unspecified,
                StorageMode::Nullable
            )])),
            "order_id"
        );
        let mut no_element = v2_field("shipping_window", "RANGE", "NULLABLE");
        no_element.range_element_type = None;
        assert_eq!(unsupported(v2_schema(vec![no_element])), "shipping_window");
        let mut bad_element = v2_field("shipping_window", "RANGE", "NULLABLE");
        bad_element.range_element_type = Some(v2::table_field_schema::FieldElementType {
            r#type: "INT64".to_string(),
        });
        assert_eq!(unsupported(v2_schema(vec![bad_element])), "shipping_window");
    }

    #[test]
    fn numeric_precision_without_scale_equals_scale_zero() {
        let mut precision_only = v2_field("total", "NUMERIC", "NULLABLE");
        precision_only.precision = 10;
        let mut precision_and_scale_zero = precision_only.clone();
        precision_and_scale_zero.scale = 0;
        let mut storage_precision_only =
            column("total", StorageType::Numeric, StorageMode::Nullable);
        storage_precision_only.precision = 10;
        let want = BigQueryFieldType::Numeric(Some(BigQueryDecimalParams {
            precision: 10,
            scale: 0,
        }));
        assert_eq!(
            v2_schema(vec![precision_only])
                .expect("valid test input")
                .fields[0]
                .field_type,
            want
        );
        assert_eq!(
            v2_schema(vec![precision_and_scale_zero])
                .expect("valid test input")
                .fields[0]
                .field_type,
            want
        );
        assert_eq!(
            storage_schema(vec![storage_precision_only])
                .expect("valid test input")
                .fields[0]
                .field_type,
            want
        );

        let mut no_precision = v2_field("total", "BIGNUMERIC", "NULLABLE");
        no_precision.scale = 5;
        assert_eq!(
            v2_schema(vec![no_precision])
                .expect("valid test input")
                .fields[0]
                .field_type,
            BigQueryFieldType::BigNumeric(None),
            "precision 0 is unset whatever the scale"
        );
        let mut max_on_int = v2_field("order_id", "INT64", "NULLABLE");
        max_on_int.max_length = 10;
        assert_eq!(
            v2_schema(vec![max_on_int])
                .expect("valid test input")
                .fields[0]
                .field_type,
            BigQueryFieldType::Int64,
            "max_length is kept for STRING and BYTES only"
        );
    }

    #[test]
    fn timestamp_picosecond_precision_is_unsupported() {
        let mut pico = v2_field("placed_at", "TIMESTAMP", "NULLABLE");
        pico.timestamp_precision = Some(12);
        assert_eq!(unsupported(v2_schema(vec![pico])), "placed_at");
        let mut storage_pico = column("placed_at", StorageType::Timestamp, StorageMode::Nullable);
        storage_pico.timestamp_precision = Some(12);
        assert_eq!(unsupported(storage_schema(vec![storage_pico])), "placed_at");
        let mut micro = v2_field("placed_at", "TIMESTAMP", "NULLABLE");
        micro.timestamp_precision = Some(6);
        assert_eq!(
            v2_schema(vec![micro]).expect("valid test input").fields[0].field_type,
            BigQueryFieldType::Timestamp
        );
    }

    #[test]
    fn display_prints_googlesql_type_syntax() {
        let cases = [
            (BigQueryFieldType::Int64, "INT64"),
            (BigQueryFieldType::Float64, "FLOAT64"),
            (BigQueryFieldType::Bool, "BOOL"),
            (BigQueryFieldType::String { max_length: None }, "STRING"),
            (
                BigQueryFieldType::String {
                    max_length: Some(10),
                },
                "STRING(10)",
            ),
            (
                BigQueryFieldType::Bytes {
                    max_length: Some(3),
                },
                "BYTES(3)",
            ),
            (BigQueryFieldType::Numeric(None), "NUMERIC"),
            (
                BigQueryFieldType::Numeric(Some(BigQueryDecimalParams {
                    precision: 10,
                    scale: 2,
                })),
                "NUMERIC(10, 2)",
            ),
            (
                BigQueryFieldType::BigNumeric(Some(BigQueryDecimalParams {
                    precision: 40,
                    scale: 0,
                })),
                "BIGNUMERIC(40, 0)",
            ),
            (BigQueryFieldType::Date, "DATE"),
            (BigQueryFieldType::Time, "TIME"),
            (BigQueryFieldType::DateTime, "DATETIME"),
            (BigQueryFieldType::Timestamp, "TIMESTAMP"),
            (BigQueryFieldType::Geography, "GEOGRAPHY"),
            (BigQueryFieldType::Json, "JSON"),
            (BigQueryFieldType::Interval, "INTERVAL"),
            (
                BigQueryFieldType::Range(BigQueryRangeElementType::Date),
                "RANGE<DATE>",
            ),
            (
                BigQueryFieldType::Range(BigQueryRangeElementType::Timestamp),
                "RANGE<TIMESTAMP>",
            ),
            (
                BigQueryFieldType::Struct(vec![
                    field(
                        "quantity",
                        BigQueryFieldType::Int64,
                        BigQueryFieldMode::Required,
                    ),
                    field(
                        "tags",
                        BigQueryFieldType::String { max_length: None },
                        BigQueryFieldMode::Repeated,
                    ),
                ]),
                "STRUCT<quantity INT64, tags ARRAY<STRING>>",
            ),
        ];
        for (field_type, text) in cases {
            assert_eq!(field_type.to_string(), text);
        }
    }

    #[test]
    fn arrow_schema_normalises_to_the_same_vocabulary() {
        let extension =
            |name: &str| HashMap::from([("ARROW:extension:name".to_string(), name.to_string())]);
        let range_metadata = HashMap::from([("google:sqlType".to_string(), "range".to_string())]);
        let timestamp_type = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        let schema = Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("attributes", DataType::Utf8, true)
                .with_metadata(extension("google:sqlType:json")),
            Field::new_list("tags", Field::new("item", DataType::Utf8, true), false)
                .with_metadata(extension("google:sqlType:geography")),
            Field::new(
                "shipping_window",
                DataType::Struct(Fields::from(vec![
                    Field::new("start", timestamp_type.clone(), true),
                    Field::new("end", timestamp_type, true),
                ])),
                true,
            )
            .with_metadata(range_metadata),
            Field::new(
                "details",
                DataType::Struct(Fields::from(vec![Field::new(
                    "lead_time",
                    DataType::Interval(IntervalUnit::MonthDayNano),
                    true,
                )
                .with_metadata(extension("google:sqlType:interval"))])),
                true,
            ),
        ]);
        let got = BigQueryTableSchema::from_arrow(&schema).expect("valid test input");
        assert_eq!(
            got,
            BigQueryTableSchema {
                fields: vec![
                    field(
                        "order_id",
                        BigQueryFieldType::Int64,
                        BigQueryFieldMode::Required
                    ),
                    field(
                        "attributes",
                        BigQueryFieldType::Json,
                        BigQueryFieldMode::Nullable
                    ),
                    field(
                        "tags",
                        BigQueryFieldType::Geography,
                        BigQueryFieldMode::Repeated
                    ),
                    field(
                        "shipping_window",
                        BigQueryFieldType::Range(BigQueryRangeElementType::Timestamp),
                        BigQueryFieldMode::Nullable
                    ),
                    field(
                        "details",
                        BigQueryFieldType::Struct(vec![field(
                            "lead_time",
                            BigQueryFieldType::Interval,
                            BigQueryFieldMode::Nullable
                        )]),
                        BigQueryFieldMode::Nullable
                    ),
                ]
            }
        );
        let bad = Schema::new(vec![Field::new("weight", DataType::Float16, true)]);
        assert_eq!(unsupported(BigQueryTableSchema::from_arrow(&bad)), "weight");
    }
}
