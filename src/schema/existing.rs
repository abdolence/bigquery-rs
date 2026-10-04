//! A table as `GetTable` returns it, in the terms the diff compares.

use crate::db::proto::{duration_ms, timestamp_ms};
use crate::errors::BigQueryError;
use crate::BigQueryLabels;
use crate::{
    BigQueryInstant, BigQueryPartitionUnit, BigQueryPartitioning, BigQueryResult,
    BigQueryTableSchema,
};
use gcloud_sdk::google::cloud::bigquery::v2;
use std::time::Duration;

/// A table's current state, with the raw `GetTable` body kept for the patch and update bodies,
/// which must carry everything the crate does not model (policy tags, collation, foreign keys)
/// back unchanged.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ExistingTable {
    pub raw: v2::Table,
    pub schema: BigQueryTableSchema,
    pub partitioning: Option<BigQueryPartitioning>,
    pub partition_expiration: Option<Duration>,
    pub clustering: Vec<String>,
    pub primary_key: Option<Vec<String>>,
    pub description: Option<String>,
    pub labels: BigQueryLabels,
    pub expiration: Option<BigQueryInstant>,
    pub num_rows: Option<u64>,
    pub num_bytes: Option<i64>,
}

fn range_bound(table: &v2::Table, what: &str, text: &str) -> BigQueryResult<i64> {
    text.parse().map_err(|_| {
        BigQueryError::unsupported_table(
            table,
            format!("range partitioning {what} {text:?} is not an INT64"),
        )
    })
}

impl BigQueryPartitioning {
    /// The partitioning of `raw` and its partition expiration, as the crate models them.
    ///
    /// # Errors
    /// [`BigQueryError::InvalidParametersError`] for partitioning the crate does not model, and
    /// [`BigQueryError::DeserializeError`] for a partition expiration that is not a duration.
    pub(crate) fn from_table(
        raw: &v2::Table,
    ) -> BigQueryResult<(Option<BigQueryPartitioning>, Option<Duration>)> {
        Ok(match (&raw.time_partitioning, &raw.range_partitioning) {
            (Some(time), _) => (
                Some(BigQueryPartitioning::Time {
                    unit: BigQueryPartitionUnit::parse(&time.r#type).ok_or_else(|| {
                        BigQueryError::unsupported_table(
                            raw,
                            format!("time partitioning type {:?} is not supported", time.r#type),
                        )
                    })?,
                    column: time.field.clone().filter(|v| !v.is_empty()),
                }),
                duration_ms(
                    "time_partitioning.expiration_ms",
                    time.expiration_ms.filter(|ms| *ms > 0),
                )?,
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
}

/// # Errors
/// [`BigQueryError::InvalidParametersError`] for a view, a materialized view or an external
/// table, which a declaration cannot own, and for partitioning the crate does not model; a
/// column type the crate does not handle, or an expiration out of range, is a
/// [`BigQueryError::DeserializeError`].
impl TryFrom<v2::Table> for ExistingTable {
    type Error = BigQueryError;

    fn try_from(raw: v2::Table) -> Result<Self, Self::Error> {
        if !raw.r#type.is_empty() && raw.r#type != "TABLE" {
            return Err(BigQueryError::unsupported_table(
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
        let (partitioning, partition_expiration) = BigQueryPartitioning::from_table(&raw)?;
        let expiration = match raw.expiration_time.filter(|ms| *ms > 0) {
            Some(ms) => timestamp_ms("expiration_time", ms)?,
            None => None,
        };
        Ok(Self {
            schema,
            partitioning,
            partition_expiration,
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
                .filter(|v| !v.is_empty()),
            description: raw.description.clone().filter(|v| !v.is_empty()),
            labels: raw.labels.clone().into_iter().collect(),
            expiration,
            num_rows: raw.num_rows,
            num_bytes: raw.num_bytes,
            raw,
        })
    }
}
