//! BigQuery's column types and their storage as Iceberg types.
//!
//! Each BigQuery type has one Iceberg type, and Iceberg's own Arrow conversion
//! (`iceberg::arrow::schema_to_arrow_schema`) gives the type a column has inside the engine's
//! query plans:
//!
//! | BigQuery | Iceberg | Engine Arrow |
//! |---|---|---|
//! | INT64 | `long` | `Int64` |
//! | FLOAT64 | `double` | `Float64` |
//! | NUMERIC(p, s), (38, 9) when undeclared | `decimal(p, s)` | `Decimal128(p, s)` |
//! | BOOL | `boolean` | `Boolean` |
//! | STRING(n) | `string` | `Utf8` |
//! | BYTES(n) | `binary` | `LargeBinary` |
//! | DATE | `date` | `Date32` |
//! | TIME | `time` | `Time64(Microsecond)` |
//! | DATETIME | `timestamp` | `Timestamp(Microsecond, None)` |
//! | TIMESTAMP | `timestamptz` | `Timestamp(Microsecond, "+00:00")` |
//! | GEOGRAPHY, JSON | `string` | `Utf8` |
//! | `RANGE<T>` | `struct<start T, end T>` | `Struct` |
//! | STRUCT | `struct` | `Struct` |
//! | REPEATED T | `list<T>`, the element required | `List` |
//!
//! What the Iceberg type cannot say is kept in table properties keyed by field ID, which
//! survives a rename: `bigquery.column.<id>.type` holds the BigQuery type wherever reading the
//! Iceberg type back would give another one (JSON, GEOGRAPHY, RANGE, a declared NUMERIC(38, 9)),
//! `.max_length` the `n` of STRING(n) and BYTES(n), and `.default` the default value expression.
//! BIGNUMERIC has no Iceberg type, since Iceberg decimals stop at precision 38, and INTERVAL
//! none either: both are refused as column types.

use crate::error::EngineError;
use gcloud_sdk::google::cloud::bigquery::v2;
use iceberg::spec::{
    ListType, NestedField, NestedFieldRef, PrimitiveType, Schema, StructType, Type,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

/// The labels of a dataset or a table, by key.
pub(crate) type Labels = std::collections::BTreeMap<String, String>;

/// The prefix of every table property the engine keys by column.
const COLUMN_PROPERTY: &str = "bigquery.column.";

/// A column name as BigQuery allows it: 1 to 300 characters of letters, digits and `_`,
/// starting with a letter or `_`, and none of BigQuery's reserved prefixes nor the engine's own,
/// `_bigquery_lite_`, compared without case.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String")]
pub(crate) struct ColumnName(String);

/// Prefixes BigQuery reserves for its pseudo-columns, then the one of the engine's hidden
/// columns.
const RESERVED_PREFIXES: &[&str] = &[
    "_TABLE_",
    "_FILE_",
    "_PARTITION",
    "_ROW_TIMESTAMP",
    "__ROOT__",
    "_COLIDENTIFIER",
    "_CHANGE_SEQUENCE_NUMBER",
    "_CHANGE_TYPE",
    "_CHANGE_TIMESTAMP",
    HIDDEN_PREFIX,
];

/// The prefix of the engine's hidden columns, which no table lists.
pub(crate) const HIDDEN_PREFIX: &str = "_bigquery_lite_";

impl ColumnName {
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for ColumnName {
    type Error = EngineError;

    fn try_from(name: String) -> Result<Self, Self::Error> {
        let invalid = |rule: &str| EngineError::invalid(format!("column name {name:?}"), rule);
        if name.is_empty() || name.chars().count() > 300 {
            return Err(invalid("a column name is 1 to 300 characters long"));
        }
        if !name.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_') {
            return Err(invalid("a column name starts with a letter or `_`"));
        }
        if !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
            return Err(invalid("a column name holds letters, digits and `_` only"));
        }
        let upper = name.to_ascii_uppercase();
        if let Some(prefix) = RESERVED_PREFIXES
            .iter()
            .find(|prefix| upper.starts_with(&prefix.to_ascii_uppercase()))
        {
            return Err(invalid(&format!("the prefix `{prefix}` is reserved")));
        }
        Ok(Self(name))
    }
}

impl Display for ColumnName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// The declared precision and scale of a NUMERIC column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DecimalParams {
    pub precision: u32,
    pub scale: u32,
}

/// NUMERIC without a declared precision and scale.
const NUMERIC_DEFAULT: DecimalParams = DecimalParams {
    precision: 38,
    scale: 9,
};

/// The element type of a RANGE column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RangeElement {
    Date,
    DateTime,
    Timestamp,
}

impl RangeElement {
    fn name(self) -> &'static str {
        match self {
            RangeElement::Date => "DATE",
            RangeElement::DateTime => "DATETIME",
            RangeElement::Timestamp => "TIMESTAMP",
        }
    }

    fn iceberg(self) -> PrimitiveType {
        match self {
            RangeElement::Date => PrimitiveType::Date,
            RangeElement::DateTime => PrimitiveType::Timestamp,
            RangeElement::Timestamp => PrimitiveType::Timestamptz,
        }
    }
}

/// A BigQuery column type the engine stores.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ColumnType {
    Int64,
    Float64,
    Numeric(Option<DecimalParams>),
    Bool,
    String { max_length: Option<u64> },
    Bytes { max_length: Option<u64> },
    Date,
    Time,
    DateTime,
    Timestamp,
    Geography,
    Json,
    Range(RangeElement),
    Struct(Columns),
}

impl ColumnType {
    /// The name the v2 API reports, the standard one.
    fn name(&self) -> &'static str {
        match self {
            ColumnType::Int64 => "INT64",
            ColumnType::Float64 => "FLOAT64",
            ColumnType::Numeric(_) => "NUMERIC",
            ColumnType::Bool => "BOOL",
            ColumnType::String { .. } => "STRING",
            ColumnType::Bytes { .. } => "BYTES",
            ColumnType::Date => "DATE",
            ColumnType::Time => "TIME",
            ColumnType::DateTime => "DATETIME",
            ColumnType::Timestamp => "TIMESTAMP",
            ColumnType::Geography => "GEOGRAPHY",
            ColumnType::Json => "JSON",
            ColumnType::Range(_) => "RANGE",
            ColumnType::Struct(_) => "STRUCT",
        }
    }

    /// The type as `bigquery.column.<id>.type` records it, for the types whose Iceberg type
    /// reads back as another.
    fn annotation(&self) -> Option<String> {
        match self {
            ColumnType::Geography | ColumnType::Json => Some(self.name().to_string()),
            ColumnType::Range(element) => Some(format!("RANGE<{}>", element.name())),
            ColumnType::Numeric(Some(params)) if *params == NUMERIC_DEFAULT => {
                Some(format!("NUMERIC({}, {})", params.precision, params.scale))
            }
            _ => None,
        }
    }

    /// The type a `bigquery.column.<id>.type` annotation records.
    fn from_annotation(annotation: &str) -> Option<Self> {
        Some(match annotation {
            "GEOGRAPHY" => ColumnType::Geography,
            "JSON" => ColumnType::Json,
            "RANGE<DATE>" => ColumnType::Range(RangeElement::Date),
            "RANGE<DATETIME>" => ColumnType::Range(RangeElement::DateTime),
            "RANGE<TIMESTAMP>" => ColumnType::Range(RangeElement::Timestamp),
            numeric => {
                let params = numeric.strip_prefix("NUMERIC(")?.strip_suffix(')')?;
                let (precision, scale) = params.split_once(", ")?;
                ColumnType::Numeric(Some(DecimalParams {
                    precision: precision.parse().ok()?,
                    scale: scale.parse().ok()?,
                }))
            }
        })
    }
}

/// How many values a column holds per row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mode {
    Nullable,
    Required,
    Repeated,
}

impl Mode {
    fn name(self) -> &'static str {
        match self {
            Mode::Nullable => "NULLABLE",
            Mode::Required => "REQUIRED",
            Mode::Repeated => "REPEATED",
        }
    }
}

/// One column of a table or one field of a STRUCT.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Column {
    pub name: ColumnName,
    pub column_type: ColumnType,
    pub mode: Mode,
    pub description: Option<String>,
    pub default_value: Option<String>,
}

/// The columns of a table or the fields of a STRUCT, no two of whose names differ only by case,
/// since BigQuery treats such names as one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Columns(Vec<Column>);

impl TryFrom<Vec<Column>> for Columns {
    type Error = EngineError;

    fn try_from(columns: Vec<Column>) -> Result<Self, Self::Error> {
        let mut seen: HashMap<String, &ColumnName> = HashMap::new();
        for column in &columns {
            if let Some(other) =
                seen.insert(column.name.as_str().to_ascii_lowercase(), &column.name)
            {
                return Err(EngineError::invalid(
                    format!("columns `{other}` and `{}`", column.name),
                    "column names that differ only by case name one column",
                ));
            }
        }
        Ok(Self(columns))
    }
}

impl Columns {
    pub(crate) fn iter(&self) -> impl Iterator<Item = &Column> {
        self.0.iter()
    }

    /// The top-level column named `name`, matched exactly.
    pub(crate) fn get(&self, name: &str) -> Option<&Column> {
        self.0.iter().find(|column| column.name.as_str() == name)
    }

    /// The Iceberg schema of these columns and the column properties of the table.
    ///
    /// A column that `base` already has keeps its field ID, matched by name at every level;
    /// every other field gets the next ID from `first_new_id` on. Built without a base, the IDs
    /// are the provisional ones table creation reassigns, so the properties are computed again
    /// from the created schema.
    pub(crate) fn iceberg_schema(
        &self,
        base: Option<&Schema>,
        first_new_id: i32,
    ) -> Result<(Schema, HashMap<String, String>), EngineError> {
        let mut builder = IcebergFields {
            next_id: first_new_id,
            properties: HashMap::new(),
        };
        let fields = builder.fields(self, base.map(Schema::as_struct))?;
        let schema = Schema::builder().with_fields(fields).build()?;
        Ok((schema, builder.properties))
    }

    /// The columns stored as `schema`, with the column properties of `properties`. The engine's
    /// hidden columns are left out.
    pub(crate) fn from_iceberg(
        schema: &Schema,
        properties: &HashMap<String, String>,
    ) -> Result<Self, EngineError> {
        let columns = schema
            .as_struct()
            .fields()
            .iter()
            .filter(|field| !field.name.starts_with(HIDDEN_PREFIX))
            .map(|field| Column::from_iceberg(field, properties))
            .collect::<Result<Vec<_>, _>>()?;
        Columns::try_from(columns)
    }

    /// Checks that these columns can become `to` in place, as BigQuery allows it: columns are
    /// added, REQUIRED relaxes to NULLABLE, and descriptions and defaults change; a dropped or
    /// retyped column and a new REQUIRED one are refused.
    pub(crate) fn check_change(&self, to: &Columns) -> Result<(), EngineError> {
        for column in self.iter() {
            let Some(changed) = to.get(column.name.as_str()) else {
                return Err(EngineError::invalid(
                    format!("column `{}`", column.name),
                    "a column cannot be dropped in place",
                ));
            };
            let mode_allowed = changed.mode == column.mode
                || (column.mode == Mode::Required && changed.mode == Mode::Nullable);
            if !mode_allowed {
                return Err(EngineError::invalid(
                    format!("column `{}`", column.name),
                    "REQUIRED may relax to NULLABLE, and no other mode changes in place",
                ));
            }
            match (&column.column_type, &changed.column_type) {
                (ColumnType::Struct(from), ColumnType::Struct(to)) => from.check_change(to)?,
                (from, to) if from == to => {}
                _ => {
                    return Err(EngineError::invalid(
                        format!("column `{}`", column.name),
                        "a column's type cannot change in place",
                    ))
                }
            }
        }
        if let Some(added) = to.iter().find(|column| {
            self.get(column.name.as_str()).is_none() && column.mode == Mode::Required
        }) {
            return Err(EngineError::invalid(
                format!("column `{}`", added.name),
                "a REQUIRED column cannot be added to an existing table",
            ));
        }
        Ok(())
    }

    /// The field ID of the top-level column `name` in `schema`.
    pub(crate) fn field_id(schema: &Schema, name: &ColumnName) -> Result<i32, EngineError> {
        schema
            .as_struct()
            .fields()
            .iter()
            .find(|field| field.name == name.as_str())
            .map(|field| field.id)
            .ok_or_else(|| {
                EngineError::invalid(format!("column `{name}`"), "the table has no such column")
            })
    }
}

/// Builds Iceberg fields from columns, assigning field IDs and collecting the column properties.
struct IcebergFields {
    next_id: i32,
    properties: HashMap<String, String>,
}

impl IcebergFields {
    fn id(&mut self, existing: Option<&NestedFieldRef>) -> i32 {
        match existing {
            Some(field) => field.id,
            None => {
                let id = self.next_id;
                self.next_id += 1;
                id
            }
        }
    }

    fn fields(
        &mut self,
        columns: &Columns,
        base: Option<&StructType>,
    ) -> Result<Vec<NestedFieldRef>, EngineError> {
        columns
            .iter()
            .map(|column| {
                let existing = base.and_then(|base| base.field_by_name(column.name.as_str()));
                self.field(column, existing).map(Arc::new)
            })
            .collect()
    }

    fn field(
        &mut self,
        column: &Column,
        existing: Option<&NestedFieldRef>,
    ) -> Result<NestedField, EngineError> {
        let id = self.id(existing);
        let mut annotate = |key: &str, value: String| {
            self.properties
                .insert(format!("{COLUMN_PROPERTY}{id}.{key}"), value);
        };
        if let Some(annotation) = column.column_type.annotation() {
            annotate("type", annotation);
        }
        if let ColumnType::String {
            max_length: Some(n),
        }
        | ColumnType::Bytes {
            max_length: Some(n),
        } = column.column_type
        {
            annotate("max_length", n.to_string());
        }
        if let Some(default) = &column.default_value {
            annotate("default", default.clone());
        }
        let existing_type = existing.map(|field| field.field_type.as_ref());
        let field = match column.mode {
            Mode::Repeated => {
                let element_existing = match existing_type {
                    Some(Type::List(list)) => Some(&list.element_field),
                    _ => None,
                };
                let element_id = self.id(element_existing);
                let element_type = self.value_type(
                    &column.column_type,
                    element_existing.map(|field| field.field_type.as_ref()),
                )?;
                let element = NestedField::list_element(element_id, element_type, true);
                NestedField::required(
                    id,
                    column.name.as_str(),
                    Type::List(ListType::new(Arc::new(element))),
                )
            }
            mode => NestedField::new(
                id,
                column.name.as_str(),
                self.value_type(&column.column_type, existing_type)?,
                mode == Mode::Required,
            ),
        };
        Ok(match &column.description {
            Some(description) => field.with_doc(description),
            None => field,
        })
    }

    /// The Iceberg type of one value of `column_type`; `existing` is the type the column has
    /// in the base schema, whose nested field IDs are kept.
    fn value_type(
        &mut self,
        column_type: &ColumnType,
        existing: Option<&Type>,
    ) -> Result<Type, EngineError> {
        let existing_struct = match existing {
            Some(Type::Struct(existing)) => Some(existing),
            _ => None,
        };
        Ok(Type::Primitive(match column_type {
            ColumnType::Int64 => PrimitiveType::Long,
            ColumnType::Float64 => PrimitiveType::Double,
            ColumnType::Numeric(params) => {
                let params = params.unwrap_or(NUMERIC_DEFAULT);
                PrimitiveType::Decimal {
                    precision: params.precision,
                    scale: params.scale,
                }
            }
            ColumnType::Bool => PrimitiveType::Boolean,
            ColumnType::String { .. } | ColumnType::Geography | ColumnType::Json => {
                PrimitiveType::String
            }
            ColumnType::Bytes { .. } => PrimitiveType::Binary,
            ColumnType::Date => PrimitiveType::Date,
            ColumnType::Time => PrimitiveType::Time,
            ColumnType::DateTime => PrimitiveType::Timestamp,
            ColumnType::Timestamp => PrimitiveType::Timestamptz,
            ColumnType::Range(element) => {
                let mut bound = |name: &str| {
                    let existing = existing_struct.and_then(|s| s.field_by_name(name));
                    Arc::new(NestedField::optional(
                        self.id(existing),
                        name,
                        Type::Primitive(element.iceberg()),
                    ))
                };
                let fields = vec![bound("start"), bound("end")];
                return Ok(Type::Struct(StructType::new(fields)));
            }
            ColumnType::Struct(fields) => {
                let fields = self.fields(fields, existing_struct)?;
                return Ok(Type::Struct(StructType::new(fields)));
            }
        }))
    }
}

impl Column {
    /// The column an Iceberg field stores, with the column properties of `properties`.
    fn from_iceberg(
        field: &NestedField,
        properties: &HashMap<String, String>,
    ) -> Result<Self, EngineError> {
        let property = |key: &str| properties.get(&format!("{COLUMN_PROPERTY}{}.{key}", field.id));
        let unsupported = |detail: String| {
            EngineError::unsupported(
                format!("the Iceberg type of column `{}`", field.name),
                detail,
            )
        };
        let (mode, value_type) = match field.field_type.as_ref() {
            Type::List(list) => (Mode::Repeated, list.element_field.field_type.as_ref()),
            value_type if field.required => (Mode::Required, value_type),
            value_type => (Mode::Nullable, value_type),
        };
        let max_length = property("max_length")
            .map(|n| {
                n.parse().map_err(|_| {
                    EngineError::invalid(
                        format!("max_length of column `{}`", field.name),
                        format!("{n:?} is not a length"),
                    )
                })
            })
            .transpose()?;
        let natural = match value_type {
            Type::Primitive(primitive) => match primitive {
                PrimitiveType::Int | PrimitiveType::Long => ColumnType::Int64,
                PrimitiveType::Float | PrimitiveType::Double => ColumnType::Float64,
                PrimitiveType::Decimal { precision, scale } => {
                    let params = DecimalParams {
                        precision: *precision,
                        scale: *scale,
                    };
                    ColumnType::Numeric((params != NUMERIC_DEFAULT).then_some(params))
                }
                PrimitiveType::Boolean => ColumnType::Bool,
                PrimitiveType::String => ColumnType::String { max_length },
                PrimitiveType::Binary | PrimitiveType::Fixed(_) => ColumnType::Bytes { max_length },
                PrimitiveType::Date => ColumnType::Date,
                PrimitiveType::Time => ColumnType::Time,
                PrimitiveType::Timestamp => ColumnType::DateTime,
                PrimitiveType::Timestamptz => ColumnType::Timestamp,
                other => return Err(unsupported(format!("{other} has no BigQuery type"))),
            },
            Type::Struct(fields) => ColumnType::Struct(Columns::try_from(
                fields
                    .fields()
                    .iter()
                    .map(|nested| Column::from_iceberg(nested, properties))
                    .collect::<Result<Vec<_>, _>>()?,
            )?),
            other => return Err(unsupported(format!("{other} has no BigQuery type"))),
        };
        let column_type = match property("type") {
            Some(annotation) => ColumnType::from_annotation(annotation).ok_or_else(|| {
                EngineError::invalid(
                    format!("type of column `{}`", field.name),
                    format!("{annotation:?} is not a column type"),
                )
            })?,
            None => natural,
        };
        Ok(Self {
            name: ColumnName::try_from(field.name.clone())?,
            column_type,
            mode,
            description: field.doc.clone(),
            default_value: property("default").cloned(),
        })
    }
}

/// A column as an `InsertTable`, `PatchTable` or `UpdateTable` body declares it.
///
/// # Errors
/// [`EngineError::Unsupported`] for BIGNUMERIC, INTERVAL and the options the engine does not
/// implement, naming the column; [`EngineError::Invalid`] for an invalid name, mode or
/// parameter.
impl TryFrom<&v2::TableFieldSchema> for Column {
    type Error = EngineError;

    fn try_from(field: &v2::TableFieldSchema) -> Result<Self, Self::Error> {
        let name = ColumnName::try_from(field.name.clone())?;
        let unsupported = |construct: &str| {
            EngineError::unsupported(construct, format!("declared on column `{name}`"))
        };
        let invalid = |what: &str, detail: String| {
            EngineError::invalid(format!("{what} of column `{name}`"), detail)
        };
        if field
            .policy_tags
            .as_ref()
            .is_some_and(|tags| !tags.names.is_empty())
        {
            return Err(unsupported("policy tags"));
        }
        if !field.data_policies.is_empty() || field.data_policy_list.is_some() {
            return Err(unsupported("data policies"));
        }
        if field.collation.as_ref().is_some_and(|c| !c.is_empty()) {
            return Err(unsupported("collation"));
        }
        if field.rounding_mode != 0 {
            return Err(unsupported("rounding_mode"));
        }
        if !field.foreign_type_definition.is_empty() {
            return Err(unsupported("foreign_type_definition"));
        }
        if field.generated_column.is_some() {
            return Err(unsupported("generated columns"));
        }
        let max_length = match field.max_length {
            0 => None,
            n => Some(
                u64::try_from(n).map_err(|_| invalid("max_length", format!("{n} is negative")))?,
            ),
        };
        let column_type = match field.r#type.to_ascii_uppercase().as_str() {
            "INTEGER" | "INT64" => ColumnType::Int64,
            "FLOAT" | "FLOAT64" => ColumnType::Float64,
            "BOOLEAN" | "BOOL" => ColumnType::Bool,
            "NUMERIC" | "DECIMAL" => ColumnType::Numeric(match (field.precision, field.scale) {
                (0, 0) => None,
                (precision, scale) => {
                    let precision = u32::try_from(precision)
                        .ok()
                        .filter(|p| (1..=38).contains(p))
                        .ok_or_else(|| {
                            invalid("precision", format!("{precision} is not 1 to 38"))
                        })?;
                    let scale = u32::try_from(scale)
                        .ok()
                        .filter(|s| *s <= 9 && *s <= precision)
                        .ok_or_else(|| {
                            invalid(
                                "scale",
                                format!("{scale} is not 0 to 9 and at most the precision"),
                            )
                        })?;
                    Some(DecimalParams { precision, scale })
                }
            }),
            "STRING" => ColumnType::String { max_length },
            "BYTES" => ColumnType::Bytes { max_length },
            "DATE" => ColumnType::Date,
            "TIME" => ColumnType::Time,
            "DATETIME" => ColumnType::DateTime,
            "TIMESTAMP" => match field.timestamp_precision {
                None | Some(6) => ColumnType::Timestamp,
                Some(_) => {
                    return Err(unsupported(
                        "TIMESTAMP with a timestamp_precision other than 6",
                    ))
                }
            },
            "GEOGRAPHY" => ColumnType::Geography,
            "JSON" => ColumnType::Json,
            "RANGE" => ColumnType::Range(
                match field
                    .range_element_type
                    .as_ref()
                    .map(|element| element.r#type.to_ascii_uppercase())
                    .as_deref()
                {
                    Some("DATE") => RangeElement::Date,
                    Some("DATETIME") => RangeElement::DateTime,
                    Some("TIMESTAMP") => RangeElement::Timestamp,
                    other => {
                        return Err(invalid(
                            "range_element_type",
                            format!("{other:?} is not DATE, DATETIME or TIMESTAMP"),
                        ))
                    }
                },
            ),
            "RECORD" | "STRUCT" => ColumnType::Struct(Columns::try_from(
                field
                    .fields
                    .iter()
                    .map(Column::try_from)
                    .collect::<Result<Vec<_>, _>>()?,
            )?),
            "BIGNUMERIC" | "BIGDECIMAL" => return Err(unsupported("BIGNUMERIC")),
            "INTERVAL" => return Err(unsupported("INTERVAL")),
            other => return Err(invalid("type", format!("{other:?} is not a column type"))),
        };
        if max_length.is_some()
            && !matches!(
                column_type,
                ColumnType::String { .. } | ColumnType::Bytes { .. }
            )
        {
            return Err(invalid(
                "max_length",
                "only STRING and BYTES have one".to_string(),
            ));
        }
        if matches!(column_type, ColumnType::Struct(ref fields) if fields.iter().next().is_none()) {
            return Err(invalid(
                "fields",
                "a STRUCT has at least one field".to_string(),
            ));
        }
        let mode = match field.mode.to_ascii_uppercase().as_str() {
            "" | "NULLABLE" => Mode::Nullable,
            "REQUIRED" => Mode::Required,
            "REPEATED" => Mode::Repeated,
            other => return Err(invalid("mode", format!("{other:?} is not a mode"))),
        };
        Ok(Self {
            name,
            column_type,
            mode,
            description: field.description.clone().filter(|d| !d.is_empty()),
            default_value: field
                .default_value_expression
                .clone()
                .filter(|d| !d.is_empty()),
        })
    }
}

/// The column as `GetTable` reports it, with the standard type names.
impl From<&Column> for v2::TableFieldSchema {
    fn from(column: &Column) -> Self {
        let mut field = v2::TableFieldSchema {
            name: column.name.to_string(),
            r#type: column.column_type.name().to_string(),
            mode: column.mode.name().to_string(),
            description: column.description.clone(),
            default_value_expression: column.default_value.clone(),
            ..Default::default()
        };
        match &column.column_type {
            ColumnType::String { max_length } | ColumnType::Bytes { max_length } => {
                field.max_length = max_length.map_or(0, |n| i64::try_from(n).unwrap_or(i64::MAX));
            }
            ColumnType::Numeric(Some(params)) => {
                field.precision = i64::from(params.precision);
                field.scale = i64::from(params.scale);
            }
            ColumnType::Range(element) => {
                field.range_element_type = Some(v2::table_field_schema::FieldElementType {
                    r#type: element.name().to_string(),
                });
            }
            ColumnType::Struct(fields) => {
                field.fields = fields.iter().map(v2::TableFieldSchema::from).collect();
            }
            _ => {}
        }
        field
    }
}

/// The columns as `GetTable` reports them.
impl From<&Columns> for v2::TableSchema {
    fn from(columns: &Columns) -> Self {
        v2::TableSchema {
            fields: columns.iter().map(v2::TableFieldSchema::from).collect(),
            foreign_type_info: None,
        }
    }
}

/// The columns an `InsertTable` body declares.
impl TryFrom<&v2::TableSchema> for Columns {
    type Error = EngineError;

    fn try_from(schema: &v2::TableSchema) -> Result<Self, Self::Error> {
        if schema.foreign_type_info.is_some() {
            return Err(EngineError::unsupported(
                "foreign_type_info",
                "declared on the schema",
            ));
        }
        Columns::try_from(
            schema
                .fields
                .iter()
                .map(Column::try_from)
                .collect::<Result<Vec<_>, _>>()?,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field(name: &str, field_type: &str) -> v2::TableFieldSchema {
        v2::TableFieldSchema {
            name: name.to_string(),
            r#type: field_type.to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn bignumeric_and_interval_columns_are_refused_by_name() {
        for (field_type, construct) in [("BIGNUMERIC", "BIGNUMERIC"), ("INTERVAL", "INTERVAL")] {
            let refused = Column::try_from(&field("amount", field_type));
            assert!(
                matches!(&refused, Err(EngineError::Unsupported { construct: c, .. }) if c == construct),
                "{refused:?}"
            );
        }
    }

    #[test]
    fn reserved_prefixes_are_refused_in_any_case() {
        for name in [
            "_change_type",
            "_PARTITIONTIME",
            "_bigquery_lite_change",
            "_Table_suffix",
        ] {
            let refused = ColumnName::try_from(name.to_string());
            assert!(
                matches!(refused, Err(EngineError::Invalid { .. })),
                "{name}: {refused:?}"
            );
        }
        assert!(ColumnName::try_from("_partner".to_string()).is_ok());
    }
}
