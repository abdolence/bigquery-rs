//! A table as `GetTable` returns it, in the terms the diff compares.

use crate::errors::BigQueryError;
use crate::{BigQueryPartitionUnit, BigQueryPartitioning, BigQueryResult, BigQueryTableSchema};
use gcloud_sdk::google::cloud::bigquery::v2;
use std::collections::BTreeMap;

/// A table's current state, with the raw `GetTable` body kept for the patch and update bodies,
/// which must carry everything the crate does not model (policy tags, collation, foreign keys)
/// back unchanged.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LiveTable {
    pub raw: v2::Table,
    pub schema: BigQueryTableSchema,
    pub partitioning: Option<BigQueryPartitioning>,
    pub partition_expiration_ms: Option<i64>,
    pub clustering: Vec<String>,
    pub primary_key: Option<Vec<String>>,
    pub description: Option<String>,
    pub labels: BTreeMap<String, String>,
    pub expiration_ms: Option<i64>,
    pub num_rows: Option<u64>,
    pub num_bytes: Option<i64>,
}

fn unsupported(table: &v2::Table, what: String) -> BigQueryError {
    let id = table
        .table_reference
        .as_ref()
        .map(|r| format!("{}.{}.{}", r.project_id, r.dataset_id, r.table_id))
        .unwrap_or_default();
    BigQueryError::invalid_parameters("table", format!("{id}: {what}"))
}

fn partition_unit(table: &v2::Table, name: &str) -> BigQueryResult<BigQueryPartitionUnit> {
    match name {
        "HOUR" => Ok(BigQueryPartitionUnit::Hour),
        "DAY" => Ok(BigQueryPartitionUnit::Day),
        "MONTH" => Ok(BigQueryPartitionUnit::Month),
        "YEAR" => Ok(BigQueryPartitionUnit::Year),
        other => Err(unsupported(
            table,
            format!("time partitioning type {other:?} is not supported"),
        )),
    }
}

fn range_bound(table: &v2::Table, what: &str, text: &str) -> BigQueryResult<i64> {
    text.parse().map_err(|_| {
        unsupported(
            table,
            format!("range partitioning {what} {text:?} is not an INT64"),
        )
    })
}

/// The partitioning of `raw` and its partition expiration in milliseconds, as the crate models
/// them.
///
/// # Errors
/// [`BigQueryError::InvalidParametersError`] for partitioning the crate does not model.
pub(crate) fn table_partitioning(
    raw: &v2::Table,
) -> BigQueryResult<(Option<BigQueryPartitioning>, Option<i64>)> {
    Ok(match (&raw.time_partitioning, &raw.range_partitioning) {
        (Some(time), _) => (
            Some(BigQueryPartitioning::Time {
                unit: partition_unit(raw, &time.r#type)?,
                column: time.field.clone().filter(|f| !f.is_empty()),
            }),
            time.expiration_ms.filter(|ms| *ms > 0),
        ),
        (None, Some(range)) => {
            let bounds = range.range.clone().unwrap_or_default();
            (
                Some(BigQueryPartitioning::Range {
                    column: range.field.clone(),
                    start: range_bound(raw, "start", &bounds.start)?,
                    end: range_bound(raw, "end", &bounds.end)?,
                    interval: range_bound(raw, "interval", &bounds.interval)?,
                }),
                None,
            )
        }
        (None, None) => (None, None),
    })
}

/// # Errors
/// [`BigQueryError::InvalidParametersError`] for a view, a materialized view or an external
/// table, which a declaration cannot own, and for partitioning the crate does not model; a
/// column type the crate does not handle is a [`BigQueryError::DeserializeError`].
impl TryFrom<v2::Table> for LiveTable {
    type Error = BigQueryError;

    fn try_from(raw: v2::Table) -> Result<Self, Self::Error> {
        if !raw.r#type.is_empty() && raw.r#type != "TABLE" {
            return Err(unsupported(
                &raw,
                format!("a {} cannot be managed as a table", raw.r#type),
            ));
        }
        let schema = raw
            .schema
            .as_ref()
            .map(BigQueryTableSchema::try_from)
            .transpose()?
            .unwrap_or(BigQueryTableSchema { fields: Vec::new() });
        let (partitioning, partition_expiration_ms) = table_partitioning(&raw)?;
        Ok(Self {
            schema,
            partitioning,
            partition_expiration_ms,
            clustering: raw
                .clustering
                .as_ref()
                .map(|c| c.fields.clone())
                .unwrap_or_default(),
            primary_key: raw
                .table_constraints
                .as_ref()
                .and_then(|c| c.primary_key.as_ref())
                .map(|k| k.columns.clone())
                .filter(|columns| !columns.is_empty()),
            description: raw.description.clone().filter(|d| !d.is_empty()),
            labels: raw.labels.clone().into_iter().collect(),
            expiration_ms: raw.expiration_time.filter(|ms| *ms > 0),
            num_rows: raw.num_rows,
            num_bytes: raw.num_bytes,
            raw,
        })
    }
}
