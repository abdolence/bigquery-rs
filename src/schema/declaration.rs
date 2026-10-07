//! What a `db.fluent().schema().table(..)` chain declares, and the checks the crate needs before
//! it can diff or render a declaration.

use crate::db::proto::millis;
use crate::errors::{
    BigQueryError, BigQuerySchemaInferenceError, BigQuerySchemaInferenceErrorKind,
};
use crate::sql::{column_segment_violation, dotted_path};
use crate::BigQueryInstant;
use crate::BigQueryLabels;
use crate::{
    BigQueryDecimalParams, BigQueryFieldMode, BigQueryFieldSchema, BigQueryFieldType,
    BigQueryRangeElementType, BigQueryResult, BigQueryTableRef,
};
use serde::de::DeserializeOwned;
use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use std::time::Duration;

/// One declared column, or one field of a declared RECORD, as
/// [`BigQuerySchemaColumnsBuilder::field`] starts it or
/// [`from_type`](BigQuerySchemaColumnsBuilder::from_type) infers it.
///
/// A column started with `field` is NULLABLE until [`required`](Self::required) or
/// [`repeated`](Self::repeated) says otherwise, and has no type until one of the type methods
/// sets it; a column without a type is refused at `.plan()`/`.sync()`. A description or default
/// value left undeclared is not cleared: whatever the table already holds for it stays.
#[derive(Debug, Clone, PartialEq)]
pub struct BigQuerySchemaColumn {
    name: String,
    kind: Option<ColumnKind>,
    mode: BigQueryFieldMode,
    description: Option<String>,
    default_value: Option<String>,
    renamed_from: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum ColumnKind {
    Type(BigQueryFieldType),
    Record(Vec<BigQuerySchemaColumn>),
    /// A field inference found no type for; refused at `.plan()`/`.sync()` unless a type method
    /// replaces it.
    Uninferred(BigQuerySchemaInferenceError),
}

impl BigQuerySchemaColumn {
    fn new(name: String) -> Self {
        Self {
            name,
            kind: None,
            mode: BigQueryFieldMode::Nullable,
            description: None,
            default_value: None,
            renamed_from: None,
        }
    }

    pub(super) fn inferred(name: String, kind: ColumnKind, mode: BigQueryFieldMode) -> Self {
        Self {
            kind: Some(kind),
            mode,
            ..Self::new(name)
        }
    }

    /// Sets any type, including a STRUCT given as its fields.
    pub fn of_type(self, field_type: BigQueryFieldType) -> Self {
        Self {
            kind: Some(ColumnKind::Type(field_type)),
            ..self
        }
    }

    /// INT64.
    pub fn int64(self) -> Self {
        self.of_type(BigQueryFieldType::Int64)
    }

    /// FLOAT64.
    pub fn float64(self) -> Self {
        self.of_type(BigQueryFieldType::Float64)
    }

    /// NUMERIC with BigQuery's default precision and scale.
    pub fn numeric(self) -> Self {
        self.of_type(BigQueryFieldType::Numeric(None))
    }

    /// `NUMERIC(precision, scale)`.
    pub fn numeric_with(self, precision: u8, scale: u8) -> Self {
        self.of_type(BigQueryFieldType::Numeric(Some(BigQueryDecimalParams {
            precision,
            scale,
        })))
    }

    /// BIGNUMERIC with BigQuery's default precision and scale.
    pub fn bignumeric(self) -> Self {
        self.of_type(BigQueryFieldType::BigNumeric(None))
    }

    /// `BIGNUMERIC(precision, scale)`.
    pub fn bignumeric_with(self, precision: u8, scale: u8) -> Self {
        self.of_type(BigQueryFieldType::BigNumeric(Some(BigQueryDecimalParams {
            precision,
            scale,
        })))
    }

    /// BOOL.
    pub fn bool(self) -> Self {
        self.of_type(BigQueryFieldType::Bool)
    }

    /// STRING with no maximum length.
    pub fn string(self) -> Self {
        self.of_type(BigQueryFieldType::String { max_length: None })
    }

    /// `STRING(max_length)`, in characters.
    pub fn string_with_max_length(self, max_length: u64) -> Self {
        self.of_type(BigQueryFieldType::String {
            max_length: Some(max_length),
        })
    }

    /// BYTES with no maximum length.
    pub fn bytes(self) -> Self {
        self.of_type(BigQueryFieldType::Bytes { max_length: None })
    }

    /// `BYTES(max_length)`, in bytes.
    pub fn bytes_with_max_length(self, max_length: u64) -> Self {
        self.of_type(BigQueryFieldType::Bytes {
            max_length: Some(max_length),
        })
    }

    /// DATE.
    pub fn date(self) -> Self {
        self.of_type(BigQueryFieldType::Date)
    }

    /// TIME.
    pub fn time(self) -> Self {
        self.of_type(BigQueryFieldType::Time)
    }

    /// DATETIME.
    pub fn datetime(self) -> Self {
        self.of_type(BigQueryFieldType::DateTime)
    }

    /// TIMESTAMP.
    pub fn timestamp(self) -> Self {
        self.of_type(BigQueryFieldType::Timestamp)
    }

    /// GEOGRAPHY.
    pub fn geography(self) -> Self {
        self.of_type(BigQueryFieldType::Geography)
    }

    /// JSON.
    pub fn json(self) -> Self {
        self.of_type(BigQueryFieldType::Json)
    }

    /// INTERVAL.
    pub fn interval(self) -> Self {
        self.of_type(BigQueryFieldType::Interval)
    }

    /// `RANGE<element>`.
    pub fn range(self, element: BigQueryRangeElementType) -> Self {
        self.of_type(BigQueryFieldType::Range(element))
    }

    /// A RECORD (STRUCT) with the fields the closure declares. Nested fields take every column
    /// method except [`renamed_from`](Self::renamed_from): BigQuery renames top-level columns
    /// only.
    pub fn record<F>(self, fields: F) -> Self
    where
        F: FnOnce(BigQuerySchemaColumnsBuilder) -> Vec<BigQuerySchemaColumn>,
    {
        Self {
            kind: Some(ColumnKind::Record(fields(BigQuerySchemaColumnsBuilder))),
            ..self
        }
    }

    /// NULLABLE: may be NULL. A column started with `field` is NULLABLE already; on an inferred
    /// column this relaxes the REQUIRED that a field which is not an `Option` infers.
    pub fn nullable(self) -> Self {
        Self {
            mode: BigQueryFieldMode::Nullable,
            ..self
        }
    }

    /// REQUIRED: never NULL.
    ///
    /// BigQuery cannot add a REQUIRED column to an existing table or make a NULLABLE column
    /// REQUIRED, so either is a change that is impossible in place.
    pub fn required(self) -> Self {
        Self {
            mode: BigQueryFieldMode::Required,
            ..self
        }
    }

    /// REPEATED: an ARRAY of the column's type.
    pub fn repeated(self) -> Self {
        Self {
            mode: BigQueryFieldMode::Repeated,
            ..self
        }
    }

    /// The column description. It is always sent as a value, never as SQL.
    pub fn description(self, description: impl Into<String>) -> Self {
        Self {
            description: Some(description.into()),
            ..self
        }
    }

    /// The column's default value, as a GoogleSQL expression such as `CURRENT_TIMESTAMP()` or
    /// `'none'`.
    ///
    /// **The expression is trusted SQL.** It is sent to BigQuery as it is and written into
    /// generated DDL as one parenthesized operand, `DEFAULT (expr)`, so it must never carry
    /// caller-supplied text; a string default is written as its own quoted literal, `"'none'"`.
    /// A recreate writes the existing table's default for every column the declaration gives
    /// none, and that text is trusted the same way: anyone allowed to update the table's
    /// metadata can have written it.
    ///
    /// BigQuery cannot add a column with a default in one step: `.sync()` adds the column and
    /// then sets the default in a second `PatchTable`, and the rows already in the table stay
    /// NULL in that column.
    pub fn default_value(self, expression: impl Into<String>) -> Self {
        Self {
            default_value: Some(expression.into()),
            ..self
        }
    }

    /// Declares that this column used to be called `old_name`.
    ///
    /// When the table has `old_name` and not this column, `.sync()` renames it with DDL and the
    /// values are kept; once the table has this column, the declaration is a no-op. A writer
    /// still sending `old_name` after the rename loses that value silently for a few seconds and
    /// then fails, as for a dropped column, so rename after the writers have moved to the new
    /// name. Top-level columns only.
    pub fn renamed_from(self, old_name: impl Into<String>) -> Self {
        Self {
            renamed_from: Some(old_name.into()),
            ..self
        }
    }
}

/// Builds the column list for [`columns`](crate::BigQueryTableSchemaBuilder::columns) and
/// [`record`](BigQuerySchemaColumn::record).
#[derive(Debug, Clone, Copy)]
pub struct BigQuerySchemaColumnsBuilder;

impl BigQuerySchemaColumnsBuilder {
    /// Starts a column called `name`; [`path!`](crate::path!) builds the name from a struct's
    /// field.
    pub fn field(&self, name: impl Into<String>) -> BigQuerySchemaColumn {
        BigQuerySchemaColumn::new(name.into())
    }

    /// Collects the columns, in table order.
    pub fn fields<I>(&self, columns: I) -> Vec<BigQuerySchemaColumn>
    where
        I: IntoIterator<Item = BigQuerySchemaColumn>,
    {
        columns.into_iter().collect()
    }

    /// Infers the columns from the row type `T` through its `Deserialize` impl, the same serde
    /// path the crate's codecs read `T` with. Each field the impl reads is one column, in
    /// declaration order and under its serde name, so `rename`, `rename_all` and `skip` are
    /// followed and an `alias` adds no column. An alias is told from the field's own name by
    /// building a sample value of the field. An alias on a field without one, as a REQUIRED
    /// `uuid::Uuid` has none, is told apart only when that field is the struct's one field
    /// without a sample value and is not `#[serde(default)]`; otherwise each name not told apart
    /// is refused at its own path with
    /// [`UnresolvedAlias`](crate::errors::BigQuerySchemaInferenceErrorKind::UnresolvedAlias).
    ///
    /// Each field maps by the type mapping of the book's "Type mapping" chapter:
    ///
    /// - `Option<T>` is NULLABLE, a bare `T` REQUIRED, and `Vec<T>` or `Option<Vec<T>>`
    ///   REPEATED. A column started with [`field`](Self::field) is NULLABLE instead, until
    ///   [`required`](BigQuerySchemaColumn::required) says otherwise;
    /// - integers are INT64, floats FLOAT64, `bool` BOOL, and `Vec<u8>`, `[u8; N]` and serde
    ///   bytes BYTES;
    /// - `jiff::Timestamp` is TIMESTAMP and `jiff::civil::Time` TIME. Any other type that reads
    ///   from a string, a unit-variant enum included, is STRING;
    /// - the temporal wrappers and their `serialize_as_*` modules are their own types,
    ///   `BigQueryDecimal<T>` is NUMERIC, `BigQueryJson<T>` JSON, `BigQueryInterval` INTERVAL and
    ///   `BigQueryRange<T>` a RANGE of its element;
    /// - a struct is a RECORD of its own fields.
    ///
    /// Some columns need [`with`](BigQuerySchemaColumns::with), since the Rust type does not
    /// tell them apart: NUMERIC, BIGNUMERIC, GEOGRAPHY and JSON held in a `String` infer as
    /// STRING, and a plain `jiff::civil::Date` or `jiff::civil::DateTime`, a map,
    /// `serde_json::Value`, an enum with data or a `#[serde(flatten)]` struct infers no type at
    /// all. `.plan()` and `.sync()` refuse a column without an inferred type, before any request,
    /// with [`SchemaInferenceError`](crate::errors::BigQueryError::SchemaInferenceError) naming
    /// its path, unless `.with(..)` gives it one.
    ///
    /// ```rust
    /// # use bigquery::*;
    /// #[derive(serde::Deserialize)]
    /// struct Order {
    ///     id: i64,
    ///     customer: Option<String>,
    ///     total: String,
    ///     placed_at: jiff::Timestamp,
    /// }
    ///
    /// # fn declare(columns: BigQuerySchemaColumnsBuilder) {
    /// let inferred = columns
    ///     .from_type::<Order>()
    ///     .with(path!(Order::total), |total| total.numeric());
    ///
    /// let by_hand = columns.fields([
    ///     columns.field("id").int64().required(),
    ///     columns.field("customer").string(),
    ///     columns.field("total").numeric().required(),
    ///     columns.field("placed_at").timestamp().required(),
    /// ]);
    /// assert_eq!(inferred, by_hand.into());
    /// # }
    /// # declare(BigQuerySchemaColumnsBuilder);
    /// ```
    pub fn from_type<T: DeserializeOwned>(&self) -> BigQuerySchemaColumns {
        BigQuerySchemaColumns::infer::<T>()
    }
}

/// The columns a [`columns`](crate::BigQueryTableSchemaBuilder::columns) closure declares, in
/// table order: the list [`fields`](BigQuerySchemaColumnsBuilder::fields) collects, or the one
/// [`from_type`](BigQuerySchemaColumnsBuilder::from_type) infers.
///
/// The closure cannot return an error, so a row type with no columns to infer, or a
/// [`with`](Self::with) path that names no column, is kept here and refused by `.plan()` and
/// `.sync()`.
#[derive(Debug, Clone, PartialEq)]
pub struct BigQuerySchemaColumns {
    pub(super) columns: Vec<BigQuerySchemaColumn>,
    /// The first problem found; the declaration is refused with it.
    pub(super) problem: Option<BigQuerySchemaInferenceError>,
}

impl BigQuerySchemaColumns {
    /// Replaces the column at `path` with what `column` makes of it.
    ///
    /// The closure gets the column as it is, so `.description(..)`, `.default_value(..)` or
    /// `.renamed_from(..)` keep its type and mode, a type method such as `.numeric()` replaces
    /// the type and keeps the mode, and `.nullable()` relaxes a REQUIRED one. A dotted path, as
    /// [`path!`](crate::path!) builds it, reaches a field of a RECORD column.
    ///
    /// A RECORD column comes with its fields: they stay when the closure keeps the RECORD, a
    /// type method drops them, and [`record`](BigQuerySchemaColumn::record) replaces them. A
    /// field without an inferred type goes away with them, so `.with("payload", |c| c.json())`
    /// settles every field of `payload` at once.
    ///
    /// The column keeps its name whatever the closure returns, so `|_| columns.field("other")
    /// .int64()` changes the type of the column at `path` and not its name. The name is the
    /// field's serde name, which the codecs read and write the row type under; a column under
    /// another name would not match the field. Rename the field in the row type, and use
    /// `renamed_from(..)` to keep the table's values.
    ///
    /// A path that names no column is refused by `.plan()` and `.sync()` with
    /// [`UnknownColumn`](crate::errors::BigQuerySchemaInferenceErrorKind::UnknownColumn).
    pub fn with<F>(mut self, path: impl Into<String>, column: F) -> Self
    where
        F: FnOnce(BigQuerySchemaColumn) -> BigQuerySchemaColumn,
    {
        let path = path.into();
        match self.column_mut(&path) {
            Some(found) => {
                let current = std::mem::replace(found, BigQuerySchemaColumn::new(String::new()));
                let name = current.name.clone();
                *found = BigQuerySchemaColumn {
                    name,
                    ..column(current)
                };
            }
            None => {
                self.problem.get_or_insert(
                    BigQuerySchemaInferenceError::new(
                        BigQuerySchemaInferenceErrorKind::UnknownColumn,
                    )
                    .with_path(path),
                );
            }
        }
        self
    }

    fn column_mut(&mut self, path: &str) -> Option<&mut BigQuerySchemaColumn> {
        let mut segments = path.split('.');
        let top = segments.next()?;
        let mut column = self.columns.iter_mut().find(|column| column.name == top)?;
        for segment in segments {
            column = match &mut column.kind {
                Some(ColumnKind::Record(fields)) => {
                    fields.iter_mut().find(|field| field.name == segment)?
                }
                _ => return None,
            };
        }
        Some(column)
    }
}

impl From<Vec<BigQuerySchemaColumn>> for BigQuerySchemaColumns {
    fn from(columns: Vec<BigQuerySchemaColumn>) -> Self {
        Self {
            columns,
            problem: None,
        }
    }
}

/// The unit of time partitioning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BigQueryPartitionUnit {
    /// One partition per hour.
    Hour,
    /// One partition per day.
    Day,
    /// One partition per month.
    Month,
    /// One partition per year.
    Year,
}

impl BigQueryPartitionUnit {
    /// The name the v2 API and DDL use, as in `DAY`.
    pub(crate) fn name(self) -> &'static str {
        match self {
            BigQueryPartitionUnit::Hour => "HOUR",
            BigQueryPartitionUnit::Day => "DAY",
            BigQueryPartitionUnit::Month => "MONTH",
            BigQueryPartitionUnit::Year => "YEAR",
        }
    }

    /// The unit the v2 API names `name`, if the crate models it.
    pub(crate) fn parse(name: &str) -> Option<Self> {
        match name {
            "HOUR" => Some(BigQueryPartitionUnit::Hour),
            "DAY" => Some(BigQueryPartitionUnit::Day),
            "MONTH" => Some(BigQueryPartitionUnit::Month),
            "YEAR" => Some(BigQueryPartitionUnit::Year),
            _ => None,
        }
    }
}

impl Display for BigQueryPartitionUnit {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// How a table is partitioned. Partition expiration is separate, see
/// [`partition_expiration`](crate::BigQueryTableSchemaBuilder::partition_expiration), because it
/// can change in place while the partitioning cannot.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BigQueryPartitioning {
    /// Time-unit partitioning on a DATE, TIMESTAMP or DATETIME column, or by ingestion time
    /// when `column` is `None`.
    Time {
        /// The partition size.
        unit: BigQueryPartitionUnit,
        /// The partitioning column; `None` is ingestion time.
        column: Option<String>,
    },
    /// Integer-range partitioning on an INT64 column.
    Range {
        /// The partitioning column.
        column: String,
        /// The start of the first partition, inclusive.
        start: i64,
        /// The end of the last partition, exclusive.
        end: i64,
        /// The width of each partition.
        interval: i64,
    },
}

impl BigQueryPartitioning {
    /// The partitioning column, if the table has one.
    pub(crate) fn column(&self) -> Option<&str> {
        match self {
            BigQueryPartitioning::Time { column, .. } => column.as_deref(),
            BigQueryPartitioning::Range { column, .. } => Some(column),
        }
    }

    /// Whether `self` and `other` partition the same way, column names compared as BigQuery
    /// does, ignoring case.
    pub(crate) fn same_as(&self, other: &BigQueryPartitioning) -> bool {
        let same_column = match (self.column(), other.column()) {
            (Some(a), Some(b)) => a.eq_ignore_ascii_case(b),
            (None, None) => true,
            _ => false,
        };
        same_column
            && match (self, other) {
                (
                    BigQueryPartitioning::Time { unit: a, .. },
                    BigQueryPartitioning::Time { unit: b, .. },
                ) => a == b,
                (
                    BigQueryPartitioning::Range {
                        start: s1,
                        end: e1,
                        interval: i1,
                        ..
                    },
                    BigQueryPartitioning::Range {
                        start: s2,
                        end: e2,
                        interval: i2,
                        ..
                    },
                ) => (s1, e1, i1) == (s2, e2, i2),
                _ => false,
            }
    }
}

impl Display for BigQueryPartitioning {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            BigQueryPartitioning::Time {
                unit,
                column: Some(column),
            } => write!(f, "{unit} on `{column}`"),
            BigQueryPartitioning::Time { unit, column: None } => {
                write!(f, "{unit} by ingestion time")
            }
            BigQueryPartitioning::Range {
                column,
                start,
                end,
                interval,
            } => write!(f, "RANGE on `{column}` from {start} to {end} by {interval}"),
        }
    }
}

/// What `.sync()` may do when a change is impossible in place.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum BigQueryRecreatePolicy {
    /// Recreate only a table whose `GetTable` `num_rows` is 0.
    IfEmpty,
    /// Recreate whatever the table holds; every row is lost.
    DangerouslyWithDataLoss,
}

/// One validated declared column: its schema, and the name it had before, for a top-level
/// rename.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DeclaredColumn {
    pub field: BigQueryFieldSchema,
    pub renamed_from: Option<String>,
}

/// The fluent chain's settings before the checks in
/// [`TryFrom<BigQueryTableDeclarationDraft> for BigQueryTableDeclaration`].
#[derive(Debug, Clone)]
pub(crate) struct BigQueryTableDeclarationDraft {
    pub table: BigQueryTableRef,
    pub columns: BigQuerySchemaColumns,
    pub primary_key: Option<Vec<String>>,
    pub partitioning: Option<BigQueryPartitioning>,
    pub partition_expiration: Option<Duration>,
    pub clustering: Option<Vec<String>>,
    pub description: Option<String>,
    pub labels: BigQueryLabels,
    pub expiration: Option<BigQueryInstant>,
    pub allow_widening: bool,
    pub prune: bool,
    pub recreate: Option<BigQueryRecreatePolicy>,
    pub snapshot_first: bool,
}

impl BigQueryTableDeclarationDraft {
    pub(crate) fn new(table: BigQueryTableRef) -> Self {
        Self {
            table,
            columns: Vec::new().into(),
            primary_key: None,
            partitioning: None,
            partition_expiration: None,
            clustering: None,
            description: None,
            labels: BigQueryLabels::new(),
            expiration: None,
            allow_widening: false,
            prune: false,
            recreate: None,
            snapshot_first: false,
        }
    }
}

/// One table's declared schema and settings, with the sync options, checked.
///
/// A declaration owns its one table: `.prune_undeclared()` reaches nothing outside it. Built by
/// [`BigQueryTableSchemaBuilder`](crate::BigQueryTableSchemaBuilder).
#[derive(Debug, Clone, PartialEq)]
pub struct BigQueryTableDeclaration {
    pub(crate) table: BigQueryTableRef,
    pub(crate) columns: Vec<DeclaredColumn>,
    pub(crate) primary_key: Option<Vec<String>>,
    pub(crate) partitioning: Option<BigQueryPartitioning>,
    /// Whole milliseconds that fit an INT64, as the v2 API stores it.
    pub(crate) partition_expiration: Option<Duration>,
    pub(crate) clustering: Option<Vec<String>>,
    pub(crate) description: Option<String>,
    pub(crate) labels: BigQueryLabels,
    /// Whole milliseconds, as the v2 API stores it.
    pub(crate) expiration: Option<BigQueryInstant>,
    pub(crate) allow_widening: bool,
    pub(crate) prune: bool,
    pub(crate) recreate: Option<BigQueryRecreatePolicy>,
    pub(crate) snapshot_first: bool,
}

impl BigQueryTableDeclaration {
    /// The table this declaration owns.
    pub fn table(&self) -> &BigQueryTableRef {
        &self.table
    }
}

/// Why `name` cannot be a column or field name here, if it cannot: the diff splits paths on
/// `.`, so a name is one column path segment without one.
fn column_name_violation(name: &str) -> Option<String> {
    if name.contains('.') {
        Some("contains `.`; declare a nested field inside `record(..)`".into())
    } else {
        column_segment_violation(name)
    }
}

fn check_name(field: &str, name: &str) -> BigQueryResult<()> {
    match column_name_violation(name) {
        Some(violation) => Err(BigQueryError::invalid_parameters(
            field,
            format!("the column name {name:?} {violation}"),
        )),
        None => Ok(()),
    }
}

impl DeclaredColumn {
    /// Checks one level of columns and converts them, `path` naming the enclosing record.
    fn checked(columns: Vec<BigQuerySchemaColumn>, path: &str) -> BigQueryResult<Vec<Self>> {
        let mut seen = HashSet::new();
        let mut out = Vec::with_capacity(columns.len());
        for column in columns {
            let at = dotted_path(path, &column.name);
            check_name("columns", &column.name)?;
            if !seen.insert(column.name.to_ascii_lowercase()) {
                return Err(BigQueryError::invalid_parameters(
                    "columns",
                    format!("the column `{at}` is declared twice (names ignore case)"),
                ));
            }
            if let Some(old) = &column.renamed_from {
                if !path.is_empty() {
                    return Err(BigQueryError::invalid_parameters(
                        "renamed_from",
                        format!(
                            "`{at}` is a nested field; BigQuery renames top-level columns only"
                        ),
                    ));
                }
                check_name("renamed_from", old)?;
            }
            let field_type = match column.kind {
                None => {
                    return Err(BigQueryError::invalid_parameters(
                        "columns",
                        format!("the column `{at}` has no type"),
                    ))
                }
                Some(ColumnKind::Uninferred(error)) => {
                    return Err(BigQueryError::SchemaInferenceError(error))
                }
                Some(ColumnKind::Type(BigQueryFieldType::Struct(fields))) => {
                    for field in &fields {
                        check_name("columns", &field.name)?;
                    }
                    BigQueryFieldType::Struct(fields)
                }
                Some(ColumnKind::Type(field_type)) => field_type,
                Some(ColumnKind::Record(fields)) => BigQueryFieldType::Struct(
                    Self::checked(fields, &at)?
                        .into_iter()
                        .map(|c| c.field)
                        .collect(),
                ),
            };
            out.push(Self {
                field: BigQueryFieldSchema {
                    name: column.name,
                    field_type,
                    mode: column.mode,
                    description: column.description,
                    default_value_expression: column.default_value,
                },
                renamed_from: column.renamed_from,
            });
        }
        Ok(out)
    }
}

/// Refuses what the diff and the DDL renderer cannot represent: a column without a type,
/// inferred or declared, a name the renderer cannot quote unambiguously or the diff cannot tell
/// apart, a nested rename, a partitioning column that is not declared (its type picks the
/// `PARTITION BY` expression), and options that only mean something next to another one.
/// Everything else, lengths and BigQuery's naming rules included, is left to BigQuery.
impl TryFrom<BigQueryTableDeclarationDraft> for BigQueryTableDeclaration {
    type Error = BigQueryError;

    fn try_from(draft: BigQueryTableDeclarationDraft) -> Result<Self, Self::Error> {
        if let Some(problem) = draft.columns.problem {
            return Err(BigQueryError::SchemaInferenceError(problem));
        }
        let columns = DeclaredColumn::checked(draft.columns.columns, "")?;
        if let Some(old) = columns
            .iter()
            .filter_map(|c| c.renamed_from.as_ref())
            .find(|old| {
                columns
                    .iter()
                    .any(|c| c.field.name.eq_ignore_ascii_case(old))
            })
        {
            return Err(BigQueryError::invalid_parameters(
                "renamed_from",
                format!("`{old}` is renamed from and declared as a column at once"),
            ));
        }
        if let Some(column) = draft.partitioning.as_ref().and_then(|p| p.column()) {
            if !columns
                .iter()
                .any(|c| c.field.name.eq_ignore_ascii_case(column))
            {
                return Err(BigQueryError::invalid_parameters(
                    "partition_by",
                    format!("the partitioning column `{column}` is not declared"),
                ));
            }
        }
        if draft.partition_expiration.is_some() && draft.partitioning.is_none() {
            return Err(BigQueryError::invalid_parameters(
                "partition_expiration",
                "needs a partition_by_* declaration",
            ));
        }
        if draft.clustering.as_ref().is_some_and(Vec::is_empty) {
            return Err(BigQueryError::invalid_parameters(
                "cluster_by",
                "needs at least one column; prune_undeclared() removes undeclared clustering",
            ));
        }
        if draft.primary_key.as_ref().is_some_and(Vec::is_empty) {
            return Err(BigQueryError::invalid_parameters(
                "primary_key",
                "needs at least one column; prune_undeclared() removes an undeclared key",
            ));
        }
        if draft.snapshot_first && draft.recreate.is_none() {
            return Err(BigQueryError::invalid_parameters(
                "snapshot_first",
                "needs recreate_if_empty() or dangerously_recreate_with_data_loss()",
            ));
        }
        Ok(Self {
            table: draft.table,
            columns,
            primary_key: draft.primary_key,
            partitioning: draft.partitioning,
            partition_expiration: draft
                .partition_expiration
                .map(|duration| {
                    millis::<i64>("partition_expiration", duration)
                        .map(|millis| Duration::from_millis(millis.unsigned_abs()))
                })
                .transpose()?,
            clustering: draft.clustering,
            description: draft.description,
            labels: draft.labels,
            expiration: draft
                .expiration
                .map(|instant| {
                    BigQueryInstant::from_millisecond(instant.as_millisecond()).map_err(|err| {
                        BigQueryError::invalid_parameters("expiration", format!("{instant}: {err}"))
                    })
                })
                .transpose()?,
            allow_widening: draft.allow_widening,
            prune: draft.prune,
            recreate: draft.recreate,
            snapshot_first: draft.snapshot_first,
        })
    }
}
