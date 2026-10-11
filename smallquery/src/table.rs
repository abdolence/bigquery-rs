//! A BigQuery table's definition and where each part of it lives in the Iceberg table.
//!
//! The metadata file is the single source of truth, so every change is a commit. What Iceberg
//! has no field for is a table property: `bigquery.description`, `bigquery.label.<key>`,
//! `bigquery.expiration_ms`, `bigquery.creation_time_ms`, `bigquery.partitioning` (the declared
//! partitioning, returned as it was declared, its column by field ID), `bigquery.clustering`
//! and `bigquery.primary_key` (JSON lists of field IDs; field IDs survive a rename),
//! `bigquery.max_staleness`, and the column properties of [`crate::types`]. Time partitioning is
//! also the table's partition spec, and clustering its sort order.

use crate::catalog::{EngineCommit, LoadedTable};
use crate::error::EngineError;
use crate::ids::{Location, TableRef};
use crate::types::{ColumnName, ColumnType, Columns, Labels, Mode};
use gcloud_sdk::google::cloud::bigquery::v2;
use iceberg::spec::{
    NullOrder, Schema, SortDirection, SortField, SortOrder, TableMetadata, TableMetadataBuilder,
    Transform, UnboundPartitionSpec,
};
use iceberg::{TableCreation, TableRequirement, TableUpdate};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

const DESCRIPTION: &str = "bigquery.description";
const LABEL: &str = "bigquery.label.";
const EXPIRATION_MS: &str = "bigquery.expiration_ms";
const CREATION_TIME_MS: &str = "bigquery.creation_time_ms";
const PARTITIONING: &str = "bigquery.partitioning";
const CLUSTERING: &str = "bigquery.clustering";
const PRIMARY_KEY: &str = "bigquery.primary_key";
const MAX_STALENESS: &str = "bigquery.max_staleness";
const COLUMN: &str = "bigquery.column.";

/// The column properties a definition owns; others under `bigquery.column.` belong to the
/// engine's own features.
const COLUMN_KEYS: &[&str] = &["type", "max_length", "default"];

/// The unit of time partitioning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub(crate) enum TimeUnit {
    Hour,
    Day,
    Month,
    Year,
}

impl TimeUnit {
    fn name(self) -> &'static str {
        match self {
            TimeUnit::Hour => "HOUR",
            TimeUnit::Day => "DAY",
            TimeUnit::Month => "MONTH",
            TimeUnit::Year => "YEAR",
        }
    }

    fn transform(self) -> Transform {
        match self {
            TimeUnit::Hour => Transform::Hour,
            TimeUnit::Day => Transform::Day,
            TimeUnit::Month => Transform::Month,
            TimeUnit::Year => Transform::Year,
        }
    }

    fn suffix(self) -> &'static str {
        match self {
            TimeUnit::Hour => "hour",
            TimeUnit::Day => "day",
            TimeUnit::Month => "month",
            TimeUnit::Year => "year",
        }
    }
}

/// A table's partitioning as BigQuery declares it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Partitioning {
    Time {
        unit: TimeUnit,
        column: ColumnName,
    },
    /// Integer ranges `[start, end)` of `interval` each.
    Range {
        column: ColumnName,
        start: i64,
        end: i64,
        interval: i64,
    },
}

/// `bigquery.partitioning` as the table property stores it: the column by its field ID, which
/// survives a rename.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum StoredPartitioning {
    Time {
        unit: TimeUnit,
        field_id: i32,
    },
    Range {
        field_id: i32,
        start: i64,
        end: i64,
        interval: i64,
    },
}

impl StoredPartitioning {
    /// The partitioning, with its column named as `schema` names the field now.
    fn resolve(self, schema: &Schema) -> Result<Partitioning, EngineError> {
        Ok(match self {
            StoredPartitioning::Time { unit, field_id } => Partitioning::Time {
                unit,
                column: column_named_by(schema, PARTITIONING, field_id)?,
            },
            StoredPartitioning::Range {
                field_id,
                start,
                end,
                interval,
            } => Partitioning::Range {
                column: column_named_by(schema, PARTITIONING, field_id)?,
                start,
                end,
                interval,
            },
        })
    }
}

impl Partitioning {
    /// The stored form, with the column's field ID in `schema`.
    fn stored(&self, schema: &Schema) -> Result<StoredPartitioning, EngineError> {
        let field_id = Columns::field_id(schema, self.column())?;
        Ok(match self {
            Partitioning::Time { unit, .. } => StoredPartitioning::Time {
                unit: *unit,
                field_id,
            },
            Partitioning::Range {
                start,
                end,
                interval,
                ..
            } => StoredPartitioning::Range {
                field_id,
                start: *start,
                end: *end,
                interval: *interval,
            },
        })
    }

    fn column(&self) -> &ColumnName {
        match self {
            Partitioning::Time { column, .. } | Partitioning::Range { column, .. } => column,
        }
    }

    /// The Iceberg partition field: `day(c)` named `c_day` for time, `truncate[interval](c)`
    /// named `c_trunc` for a range whose start is a multiple of its interval. Any other range
    /// is recorded and reported, and its rows are not stored by partition.
    fn partition_field(&self) -> Option<(String, Transform)> {
        match self {
            Partitioning::Time { unit, column } => {
                Some((format!("{column}_{}", unit.suffix()), unit.transform()))
            }
            Partitioning::Range {
                column,
                start,
                interval,
                ..
            } => {
                let width = u32::try_from(*interval).ok()?;
                (start % interval == 0)
                    .then(|| (format!("{column}_trunc"), Transform::Truncate(width)))
            }
        }
    }

    /// The partitioning a `PatchTable`, `UpdateTable` or `InsertTable` body declares.
    fn from_body(body: &v2::Table) -> Result<Option<Self>, EngineError> {
        match (&body.time_partitioning, &body.range_partitioning) {
            (Some(_), Some(_)) => Err(EngineError::invalid(
                "partitioning",
                "a table has time or range partitioning, not both",
            )),
            (Some(time), None) => {
                if time.expiration_ms.is_some_and(|ms| ms > 0) {
                    return Err(EngineError::unsupported(
                        "partition expiration",
                        "recording an expiration without expiring rows would be a wrong answer",
                    ));
                }
                let column = match time.field.as_deref() {
                    Some(field) if !field.is_empty() => ColumnName::try_from(field.to_string())?,
                    _ => {
                        return Err(EngineError::unsupported(
                            "ingestion-time partitioning",
                            "an ingestion-time partition has no column",
                        ))
                    }
                };
                let unit = match time.r#type.to_ascii_uppercase().as_str() {
                    "HOUR" => TimeUnit::Hour,
                    "DAY" | "" => TimeUnit::Day,
                    "MONTH" => TimeUnit::Month,
                    "YEAR" => TimeUnit::Year,
                    other => {
                        return Err(EngineError::invalid(
                            "time_partitioning.type",
                            format!("{other:?} is not HOUR, DAY, MONTH or YEAR"),
                        ))
                    }
                };
                Ok(Some(Partitioning::Time { unit, column }))
            }
            (None, Some(range)) => {
                let bounds = range.range.clone().unwrap_or_default();
                let bound = |what: &str, text: &str| {
                    text.parse::<i64>().map_err(|_| {
                        EngineError::invalid(
                            format!("range_partitioning.range.{what}"),
                            format!("{text:?} is not an INT64"),
                        )
                    })
                };
                let (start, end, interval) = (
                    bound("start", &bounds.start)?,
                    bound("end", &bounds.end)?,
                    bound("interval", &bounds.interval)?,
                );
                if interval <= 0 || start >= end {
                    return Err(EngineError::invalid(
                        "range_partitioning.range",
                        format!("start {start}, end {end} and interval {interval} are not a range"),
                    ));
                }
                Ok(Some(Partitioning::Range {
                    column: ColumnName::try_from(range.field.clone())?,
                    start,
                    end,
                    interval,
                }))
            }
            (None, None) => Ok(None),
        }
    }

    /// Checks that the partitioning column exists and has a type it can partition by.
    fn validate(&self, columns: &Columns) -> Result<(), EngineError> {
        let name = self.column();
        let column = columns.get(name.as_str()).ok_or_else(|| {
            EngineError::invalid(
                format!("partitioning column `{name}`"),
                "the table has no such column",
            )
        })?;
        let fits = column.mode != Mode::Repeated
            && match self {
                Partitioning::Time {
                    unit: TimeUnit::Hour,
                    ..
                } => matches!(
                    column.column_type,
                    ColumnType::Timestamp | ColumnType::DateTime
                ),
                Partitioning::Time { .. } => matches!(
                    column.column_type,
                    ColumnType::Date | ColumnType::Timestamp | ColumnType::DateTime
                ),
                Partitioning::Range { .. } => column.column_type == ColumnType::Int64,
            };
        if !fits {
            return Err(EngineError::invalid(
                format!("partitioning column `{name}`"),
                "the column's type cannot be partitioned this way",
            ));
        }
        if let Some((field, _)) = self.partition_field() {
            if let Some(other) = columns.get(&field) {
                return Err(EngineError::invalid(
                    format!("partitioning column `{name}`"),
                    format!(
                        "its partition field is named `{field}`, as the column `{}` is",
                        other.name
                    ),
                ));
            }
        }
        Ok(())
    }

    /// The partitioning in the v2 API's form.
    fn to_body(&self) -> (Option<v2::TimePartitioning>, Option<v2::RangePartitioning>) {
        match self {
            Partitioning::Time { unit, column } => (
                Some(v2::TimePartitioning {
                    r#type: unit.name().to_string(),
                    expiration_ms: None,
                    field: Some(column.to_string()),
                }),
                None,
            ),
            Partitioning::Range {
                column,
                start,
                end,
                interval,
            } => (
                None,
                Some(v2::RangePartitioning {
                    field: column.to_string(),
                    range: Some(v2::range_partitioning::Range {
                        start: start.to_string(),
                        end: end.to_string(),
                        interval: interval.to_string(),
                    }),
                }),
            ),
        }
    }
}

/// A BigQuery table's definition: what `InsertTable` creates and `GetTable` reports, beside
/// the statistics and times.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableDefinition {
    pub columns: Columns,
    pub description: Option<String>,
    pub labels: Labels,
    pub partitioning: Option<Partitioning>,
    pub clustering: Vec<ColumnName>,
    pub primary_key: Vec<ColumnName>,
    pub expiration_ms: Option<i64>,
    pub max_staleness: Option<String>,
}

/// The settable fields of a table body the engine does not implement, and whether each is set.
fn unsupported_fields(body: &v2::Table) -> [(&'static str, bool); 17] {
    [
        (
            "friendly_name",
            body.friendly_name.as_ref().is_some_and(|v| !v.is_empty()),
        ),
        ("views", body.view.is_some()),
        ("materialized views", body.materialized_view.is_some()),
        (
            "external tables",
            body.external_data_configuration.is_some(),
        ),
        ("BigLake tables", body.biglake_configuration.is_some()),
        (
            "encryption_configuration",
            body.encryption_configuration.is_some(),
        ),
        ("table snapshots", body.snapshot_definition.is_some()),
        ("table clones", body.clone_definition.is_some()),
        (
            "default_collation",
            body.default_collation
                .as_ref()
                .is_some_and(|v| !v.is_empty()),
        ),
        ("default_rounding_mode", body.default_rounding_mode != 0),
        (
            "require_partition_filter",
            body.require_partition_filter == Some(true),
        ),
        ("partition_definition", body.partition_definition.is_some()),
        ("restrictions", body.restrictions.is_some()),
        ("resource_tags", !body.resource_tags.is_empty()),
        ("table replication", body.table_replication_info.is_some()),
        (
            "external_catalog_table_options",
            body.external_catalog_table_options.is_some(),
        ),
        (
            "foreign keys",
            body.table_constraints
                .as_ref()
                .is_some_and(|c| !c.foreign_keys.is_empty()),
        ),
    ]
}

fn column_names(names: &[String]) -> Result<Vec<ColumnName>, EngineError> {
    names
        .iter()
        .map(|name| ColumnName::try_from(name.clone()))
        .collect()
}

fn primary_key_of(body: &v2::Table) -> Result<Vec<ColumnName>, EngineError> {
    column_names(
        body.table_constraints
            .as_ref()
            .and_then(|c| c.primary_key.as_ref())
            .map(|key| key.columns.as_slice())
            .unwrap_or_default(),
    )
}

/// The whole definition a body declares, as `InsertTable` and `UpdateTable` take it.
///
/// # Errors
/// [`EngineError::Unsupported`] for a construct the engine does not implement, and
/// [`EngineError::Invalid`] for an invalid name, a missing schema or a reference to a column
/// the table does not have.
impl TryFrom<&v2::Table> for TableDefinition {
    type Error = EngineError;

    fn try_from(body: &v2::Table) -> Result<Self, Self::Error> {
        EngineError::refuse_unsupported("table", &unsupported_fields(body))?;
        let schema = body
            .schema
            .as_ref()
            .ok_or_else(|| EngineError::invalid("schema", "a table declares its columns"))?;
        let definition = Self {
            columns: Columns::try_from(schema)?,
            description: body.description.clone().filter(|d| !d.is_empty()),
            labels: body.labels.clone().into_iter().collect(),
            partitioning: Partitioning::from_body(body)?,
            clustering: column_names(
                body.clustering
                    .as_ref()
                    .map(|c| c.fields.as_slice())
                    .unwrap_or_default(),
            )?,
            primary_key: primary_key_of(body)?,
            expiration_ms: body.expiration_time.filter(|ms| *ms > 0),
            max_staleness: Some(body.max_staleness.clone()).filter(|s| !s.is_empty()),
        };
        definition.validate()?;
        Ok(definition)
    }
}

impl TableDefinition {
    /// Checks that every column the partitioning, clustering and primary key name exists.
    fn validate(&self) -> Result<(), EngineError> {
        if let Some(partitioning) = &self.partitioning {
            partitioning.validate(&self.columns)?;
        }
        for (what, names) in [
            ("clustering", &self.clustering),
            ("primary key", &self.primary_key),
        ] {
            for name in names {
                match self.columns.get(name.as_str()) {
                    Some(column) if column.mode != Mode::Repeated => {}
                    Some(_) => {
                        return Err(EngineError::invalid(
                            format!("{what} column `{name}`"),
                            "a REPEATED column cannot be one",
                        ))
                    }
                    None => {
                        return Err(EngineError::invalid(
                            format!("{what} column `{name}`"),
                            "the table has no such column",
                        ))
                    }
                }
            }
        }
        Ok(())
    }

    /// This definition with what a `PatchTable` body sets, keeping what it leaves out; labels
    /// merge.
    pub(crate) fn patched(&self, body: &v2::Table) -> Result<Self, EngineError> {
        EngineError::refuse_unsupported("table", &unsupported_fields(body))?;
        let mut patched = self.clone();
        if let Some(schema) = &body.schema {
            patched.columns = Columns::try_from(schema)?;
        }
        if let Some(description) = &body.description {
            patched.description = Some(description.clone()).filter(|d| !d.is_empty());
        }
        patched
            .labels
            .extend(body.labels.iter().map(|(k, v)| (k.clone(), v.clone())));
        if body.time_partitioning.is_some() || body.range_partitioning.is_some() {
            patched.partitioning = Partitioning::from_body(body)?;
        }
        if let Some(clustering) = &body.clustering {
            patched.clustering = column_names(&clustering.fields)?;
        }
        if body.table_constraints.is_some() {
            patched.primary_key = primary_key_of(body)?;
        }
        if let Some(ms) = body.expiration_time {
            patched.expiration_ms = Some(ms).filter(|ms| *ms > 0);
        }
        if !body.max_staleness.is_empty() {
            patched.max_staleness = Some(body.max_staleness.clone());
        }
        patched.validate()?;
        Ok(patched)
    }

    /// Checks that a table defined as `self` can become `to` in place, as BigQuery allows it:
    /// columns are added, REQUIRED relaxes to NULLABLE, and descriptions and defaults change;
    /// a dropped or retyped column, a new REQUIRED column, and a partitioning change are
    /// refused.
    pub(crate) fn check_change(&self, to: &TableDefinition) -> Result<(), EngineError> {
        if self.partitioning != to.partitioning {
            return Err(EngineError::invalid(
                "partitioning",
                "a table's partitioning cannot change",
            ));
        }
        self.columns.check_change(&to.columns)
    }

    /// The metadata of a new table at `location`, created now.
    pub(crate) fn metadata(
        &self,
        table: &TableRef,
        location: String,
        now_ms: i64,
    ) -> Result<TableMetadata, EngineError> {
        let (schema, _) = self.columns.iceberg_schema(None, 1)?;
        let mut spec = UnboundPartitionSpec::builder();
        if let Some(partitioning) = &self.partitioning {
            if let Some((name, transform)) = partitioning.partition_field() {
                let source = Columns::field_id(&schema, partitioning.column())?;
                spec = spec.add_partition_field(source, name, transform)?;
            }
        }
        let creation = TableCreation::builder()
            .name(table.table.to_string())
            .location(location)
            .sort_order(self.sort_order(&schema)?)
            .partition_spec(spec.build())
            .schema(schema)
            .properties([(CREATION_TIME_MS.to_string(), now_ms.to_string())])
            .build();
        let created = TableMetadataBuilder::from_table_creation(creation)?
            .build()?
            .metadata;
        let properties = self.properties(created.current_schema())?;
        Ok(created
            .into_builder(None)
            .set_properties(properties)?
            .build()?
            .metadata)
    }

    /// The commit that changes a table from this definition, as `metadata` stores it, to `to`;
    /// call [`TableDefinition::check_change`] first.
    pub(crate) fn commit(
        &self,
        to: &TableDefinition,
        metadata: &TableMetadata,
    ) -> Result<EngineCommit, EngineError> {
        let current = metadata.current_schema();
        let (schema, _) = to
            .columns
            .iceberg_schema(Some(current), metadata.last_column_id() + 1)?;
        let mut updates = Vec::new();
        if schema.as_struct() != current.as_struct() {
            updates.push(TableUpdate::AddSchema {
                schema: schema.clone(),
            });
            updates.push(TableUpdate::SetCurrentSchema { schema_id: -1 });
        }
        if self.clustering != to.clustering {
            updates.push(TableUpdate::AddSortOrder {
                sort_order: to.sort_order(&schema)?,
            });
            updates.push(TableUpdate::SetDefaultSortOrder { sort_order_id: -1 });
        }
        let desired = to.properties(&schema)?;
        let set: HashMap<String, String> = desired
            .iter()
            .filter(|(key, value)| metadata.properties().get(*key) != Some(*value))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        let removed: Vec<String> = metadata
            .properties()
            .keys()
            .filter(|key| is_owned(key) && !desired.contains_key(*key))
            .cloned()
            .collect();
        if !set.is_empty() {
            updates.push(TableUpdate::SetProperties { updates: set });
        }
        if !removed.is_empty() {
            updates.push(TableUpdate::RemoveProperties { removals: removed });
        }
        Ok(EngineCommit {
            requirements: vec![
                TableRequirement::UuidMatch {
                    uuid: metadata.uuid(),
                },
                TableRequirement::CurrentSchemaIdMatch {
                    current_schema_id: metadata.current_schema_id(),
                },
            ],
            updates,
        })
    }

    /// The partitioning in the v2 API's form.
    fn partitioning_body(&self) -> (Option<v2::TimePartitioning>, Option<v2::RangePartitioning>) {
        self.partitioning
            .as_ref()
            .map(Partitioning::to_body)
            .unwrap_or_default()
    }

    /// The clustering in the v2 API's form.
    fn clustering_body(&self) -> Option<v2::Clustering> {
        (!self.clustering.is_empty()).then(|| v2::Clustering {
            fields: self.clustering.iter().map(ToString::to_string).collect(),
        })
    }

    /// The sort order clustering sets, on `schema`'s field IDs.
    fn sort_order(&self, schema: &Schema) -> Result<SortOrder, EngineError> {
        if self.clustering.is_empty() {
            return Ok(SortOrder::unsorted_order());
        }
        Ok(SortOrder {
            order_id: 1,
            fields: self
                .clustering
                .iter()
                .map(|column| {
                    Ok(SortField {
                        source_id: Columns::field_id(schema, column)?,
                        transform: Transform::Identity,
                        direction: SortDirection::Ascending,
                        null_order: NullOrder::First,
                    })
                })
                .collect::<Result<_, EngineError>>()?,
        })
    }

    /// The table properties this definition owns, for `schema`, whose field IDs are final.
    fn properties(&self, schema: &Schema) -> Result<HashMap<String, String>, EngineError> {
        let (_, mut properties) = self
            .columns
            .iceberg_schema(Some(schema), schema.highest_field_id() + 1)?;
        if let Some(description) = &self.description {
            properties.insert(DESCRIPTION.to_string(), description.clone());
        }
        for (key, value) in &self.labels {
            properties.insert(format!("{LABEL}{key}"), value.clone());
        }
        if let Some(ms) = self.expiration_ms {
            properties.insert(EXPIRATION_MS.to_string(), ms.to_string());
        }
        if let Some(partitioning) = &self.partitioning {
            properties.insert(
                PARTITIONING.to_string(),
                to_json(&partitioning.stored(schema)?)?,
            );
        }
        for (key, columns) in [
            (CLUSTERING, &self.clustering),
            (PRIMARY_KEY, &self.primary_key),
        ] {
            if !columns.is_empty() {
                let ids = columns
                    .iter()
                    .map(|column| Columns::field_id(schema, column))
                    .collect::<Result<Vec<_>, _>>()?;
                properties.insert(key.to_string(), to_json(&ids)?);
            }
        }
        if let Some(staleness) = &self.max_staleness {
            properties.insert(MAX_STALENESS.to_string(), staleness.clone());
        }
        Ok(properties)
    }
}

fn to_json(value: &impl Serialize) -> Result<String, EngineError> {
    serde_json::to_string(value)
        .map_err(|err| EngineError::invalid("table property", err.to_string()))
}

/// Whether a table property is one a definition owns, which a change of the definition sets
/// or removes.
fn is_owned(key: &str) -> bool {
    match key.strip_prefix(COLUMN) {
        Some(column) => column
            .split_once('.')
            .is_some_and(|(_, suffix)| COLUMN_KEYS.contains(&suffix)),
        None => {
            key.starts_with(LABEL)
                || [
                    DESCRIPTION,
                    EXPIRATION_MS,
                    PARTITIONING,
                    CLUSTERING,
                    PRIMARY_KEY,
                    MAX_STALENESS,
                ]
                .contains(&key)
        }
    }
}

fn parse_property<T: std::str::FromStr>(
    properties: &HashMap<String, String>,
    key: &str,
) -> Result<Option<T>, EngineError> {
    properties
        .get(key)
        .map(|value| {
            value.parse().map_err(|_| {
                EngineError::invalid(format!("table property {key}"), format!("{value:?}"))
            })
        })
        .transpose()
}

fn from_json<T: for<'de> Deserialize<'de>>(
    properties: &HashMap<String, String>,
    key: &str,
) -> Result<Option<T>, EngineError> {
    properties
        .get(key)
        .map(|value| {
            serde_json::from_str(value).map_err(|err| {
                EngineError::invalid(format!("table property {key}"), err.to_string())
            })
        })
        .transpose()
}

/// The column whose field ID the table property `key` stores, by its name in `schema`.
fn column_named_by(schema: &Schema, key: &str, id: i32) -> Result<ColumnName, EngineError> {
    let field = schema.field_by_id(id).ok_or_else(|| {
        EngineError::invalid(
            format!("table property {key}"),
            format!("the table has no field {id}"),
        )
    })?;
    ColumnName::try_from(field.name.clone())
}

/// The definition a table's metadata stores.
impl TryFrom<&TableMetadata> for TableDefinition {
    type Error = EngineError;

    fn try_from(metadata: &TableMetadata) -> Result<Self, Self::Error> {
        let properties = metadata.properties();
        let schema = metadata.current_schema();
        let names = |key: &str| -> Result<Vec<ColumnName>, EngineError> {
            let ids: Vec<i32> = from_json(properties, key)?.unwrap_or_default();
            ids.into_iter()
                .map(|id| column_named_by(schema, key, id))
                .collect()
        };
        Ok(Self {
            columns: Columns::from_iceberg(schema, properties)?,
            description: properties.get(DESCRIPTION).cloned(),
            labels: properties
                .iter()
                .filter_map(|(key, value)| {
                    key.strip_prefix(LABEL)
                        .map(|label| (label.to_string(), value.clone()))
                })
                .collect(),
            partitioning: from_json::<StoredPartitioning>(properties, PARTITIONING)?
                .map(|stored| stored.resolve(schema))
                .transpose()?,
            clustering: names(CLUSTERING)?,
            primary_key: names(PRIMARY_KEY)?,
            expiration_ms: parse_property(properties, EXPIRATION_MS)?,
            max_staleness: properties.get(MAX_STALENESS).cloned(),
        })
    }
}

/// How much of a table `GetTable` reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TableView {
    /// Everything the engine knows of the table.
    Full,
    /// Without the row and byte counts.
    Basic,
}

impl LoadedTable {
    /// A total the current snapshot's summary reports, as `total-records`: zero for a table
    /// without a snapshot, which holds nothing, and `None` when the snapshot does not report it.
    fn snapshot_total(&self, key: &str) -> Result<Option<i64>, EngineError> {
        match self.metadata().current_snapshot() {
            None => Ok(Some(0)),
            Some(snapshot) => parse_property(&snapshot.summary().additional_properties, key),
        }
    }

    /// The rows the table holds, `None` when its snapshot does not report them.
    pub(crate) fn total_rows(&self) -> Result<Option<u64>, EngineError> {
        Ok(self
            .snapshot_total("total-records")?
            .and_then(|rows| u64::try_from(rows).ok()))
    }

    /// When the table was created, in milliseconds since the Unix epoch.
    pub(crate) fn creation_time_ms(&self) -> Result<Option<i64>, EngineError> {
        parse_property(self.metadata().properties(), CREATION_TIME_MS)
    }

    /// The table as `GetTable` reports it, in the location of its dataset.
    pub(crate) fn bigquery_table(
        &self,
        table: &TableRef,
        location: &Location,
        view: TableView,
    ) -> Result<v2::Table, EngineError> {
        let metadata = self.metadata();
        let definition = TableDefinition::try_from(metadata)?;
        let (time_partitioning, range_partitioning) = definition.partitioning_body();
        let (num_rows, num_bytes) = match view {
            TableView::Basic => (None, None),
            TableView::Full => (self.total_rows()?, self.snapshot_total("total-files-size")?),
        };
        Ok(v2::Table {
            kind: "bigquery#table".to_string(),
            etag: self.version().to_string(),
            id: table.to_string(),
            table_reference: Some(table.into()),
            schema: Some(v2::TableSchema::from(&definition.columns)),
            time_partitioning,
            range_partitioning,
            clustering: definition.clustering_body(),
            table_constraints: (!definition.primary_key.is_empty()).then(|| v2::TableConstraints {
                primary_key: Some(v2::PrimaryKey {
                    columns: definition
                        .primary_key
                        .iter()
                        .map(ToString::to_string)
                        .collect(),
                }),
                foreign_keys: Vec::new(),
            }),
            num_rows,
            num_bytes,
            creation_time: self.creation_time_ms()?.unwrap_or(0),
            expiration_time: definition.expiration_ms,
            last_modified_time: u64::try_from(metadata.last_updated_ms()).unwrap_or(0),
            r#type: "TABLE".to_string(),
            location: location.to_string(),
            description: definition.description,
            labels: definition.labels.into_iter().collect(),
            max_staleness: definition.max_staleness.unwrap_or_default(),
            ..Default::default()
        })
    }

    /// The table as `ListTables` reports it.
    pub(crate) fn list_format(&self, table: &TableRef) -> Result<v2::ListFormatTable, EngineError> {
        let metadata = self.metadata();
        let definition = TableDefinition::try_from(metadata)?;
        let (time_partitioning, range_partitioning) = definition.partitioning_body();
        Ok(v2::ListFormatTable {
            kind: "bigquery#table".to_string(),
            id: table.to_string(),
            table_reference: Some(table.into()),
            r#type: "TABLE".to_string(),
            time_partitioning,
            range_partitioning,
            clustering: definition.clustering_body(),
            creation_time: self.creation_time_ms()?.unwrap_or(0),
            expiration_time: definition.expiration_ms.unwrap_or(0),
            labels: definition.labels.into_iter().collect(),
            ..Default::default()
        })
    }
}

impl From<&TableRef> for v2::TableReference {
    fn from(table: &TableRef) -> Self {
        v2::TableReference {
            project_id: table.project.to_string(),
            dataset_id: table.dataset.to_string(),
            table_id: table.table.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn field(name: &str, field_type: &str, mode: &str) -> v2::TableFieldSchema {
        v2::TableFieldSchema {
            name: name.to_string(),
            r#type: field_type.to_string(),
            mode: mode.to_string(),
            ..Default::default()
        }
    }

    fn definition(fields: Vec<v2::TableFieldSchema>) -> TableDefinition {
        TableDefinition::try_from(&v2::Table {
            schema: Some(v2::TableSchema {
                fields,
                ..Default::default()
            }),
            ..Default::default()
        })
        .expect("a valid definition")
    }

    #[test]
    fn partitioning_follows_its_column_through_a_rename() {
        let declared = TableDefinition::try_from(&v2::Table {
            schema: Some(v2::TableSchema {
                fields: vec![
                    field("id", "INT64", "REQUIRED"),
                    field("placed_at", "TIMESTAMP", "NULLABLE"),
                ],
                ..Default::default()
            }),
            time_partitioning: Some(v2::TimePartitioning {
                r#type: "DAY".to_string(),
                field: Some("placed_at".to_string()),
                expiration_ms: None,
            }),
            ..Default::default()
        })
        .expect("a valid definition");
        let orders = TableRef {
            project: "lite-project".parse().expect("a valid project"),
            dataset: "shop".parse().expect("a valid dataset"),
            table: "orders".parse().expect("a valid table"),
        };
        let metadata = declared
            .metadata(&orders, "file:///lite/orders".to_string(), 0)
            .expect("the metadata builds");
        let renamed_fields = metadata
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .map(|field| {
                let mut field = field.as_ref().clone();
                if field.name == "placed_at" {
                    field.name = "ordered_at".to_string();
                }
                Arc::new(field)
            })
            .collect::<Vec<_>>();
        let renamed_schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(renamed_fields)
            .build()
            .expect("the renamed schema is valid");
        let renamed = metadata
            .into_builder(None)
            .add_current_schema(renamed_schema)
            .and_then(TableMetadataBuilder::build)
            .expect("the rename applies")
            .metadata;

        let stored = TableDefinition::try_from(&renamed).expect("the definition reads back");
        assert_eq!(
            stored.partitioning,
            Some(Partitioning::Time {
                unit: TimeUnit::Day,
                column: ColumnName::try_from("ordered_at".to_string()).expect("a valid name"),
            })
        );
    }

    #[test]
    fn a_table_adds_nullable_columns_and_relaxes_required_ones_in_place() {
        let from = definition(vec![field("id", "INT64", "REQUIRED")]);
        let to = definition(vec![
            field("id", "INT64", "NULLABLE"),
            field("note", "STRING", "NULLABLE"),
        ]);
        assert!(from.check_change(&to).is_ok());
    }

    #[test]
    fn a_table_refuses_dropped_retyped_and_added_required_columns() {
        let from = definition(vec![
            field("id", "INT64", "REQUIRED"),
            field("note", "STRING", "NULLABLE"),
        ]);
        for to in [
            definition(vec![field("id", "INT64", "REQUIRED")]),
            definition(vec![
                field("id", "INT64", "REQUIRED"),
                field("note", "BYTES", "NULLABLE"),
            ]),
            definition(vec![
                field("id", "INT64", "REQUIRED"),
                field("note", "STRING", "NULLABLE"),
                field("code", "STRING", "REQUIRED"),
            ]),
        ] {
            let refused = from.check_change(&to);
            assert!(
                matches!(refused, Err(EngineError::Invalid { .. })),
                "{refused:?}"
            );
        }
    }

    #[test]
    fn ingestion_time_partitioning_and_partition_expiration_are_refused() {
        let schema = Some(v2::TableSchema {
            fields: vec![field("at", "TIMESTAMP", "NULLABLE")],
            ..Default::default()
        });
        for (time_partitioning, construct) in [
            (
                v2::TimePartitioning {
                    r#type: "DAY".to_string(),
                    ..Default::default()
                },
                "ingestion-time partitioning",
            ),
            (
                v2::TimePartitioning {
                    r#type: "DAY".to_string(),
                    field: Some("at".to_string()),
                    expiration_ms: Some(1000),
                },
                "partition expiration",
            ),
        ] {
            let refused = TableDefinition::try_from(&v2::Table {
                schema: schema.clone(),
                time_partitioning: Some(time_partitioning),
                ..Default::default()
            });
            assert!(
                matches!(&refused, Err(EngineError::Unsupported { construct: c, .. }) if c == construct),
                "{refused:?}"
            );
        }
    }

    #[test]
    fn a_range_whose_start_is_not_a_multiple_of_its_interval_is_not_stored_by_partition() {
        let range = |start: &str| v2::Table {
            schema: Some(v2::TableSchema {
                fields: vec![field("bucket", "INT64", "NULLABLE")],
                ..Default::default()
            }),
            range_partitioning: Some(v2::RangePartitioning {
                field: "bucket".to_string(),
                range: Some(v2::range_partitioning::Range {
                    start: start.to_string(),
                    end: "100".to_string(),
                    interval: "10".to_string(),
                }),
            }),
            ..Default::default()
        };
        let table: TableRef =
            TableRef::parse("lite-project", "shop", "buckets").expect("valid IDs");
        let spec = |start: &str| {
            TableDefinition::try_from(&range(start))
                .and_then(|definition| {
                    definition.metadata(&table, "file:///root/lite-project/shop/buckets".into(), 0)
                })
                .expect("the table builds")
                .default_partition_spec()
                .fields()
                .iter()
                .map(|field| (field.name.clone(), field.transform))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            spec("0"),
            vec![("bucket_trunc".to_string(), Transform::Truncate(10))]
        );
        assert_eq!(spec("5"), Vec::new());
    }
}
