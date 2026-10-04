use crate::{BigQueryFieldType, BigQueryTableSchema};
use rsb_derive::Builder;

/// What a query sends.
#[derive(Debug, PartialEq, Clone, Builder)]
pub struct BigQueryQueryParams {
    /// The GoogleSQL statement.
    pub sql: String,
}

/// The type of a query parameter, for a value whose type cannot be inferred.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BigQueryParamType {
    /// The scalar type, or the element type of an array.
    pub field_type: BigQueryFieldType,
    /// Whether the parameter is an ARRAY of `field_type`.
    pub repeated: bool,
}

impl From<BigQueryFieldType> for BigQueryParamType {
    fn from(field_type: BigQueryFieldType) -> Self {
        Self {
            field_type,
            repeated: false,
        }
    }
}

impl BigQueryParamType {
    /// An ARRAY of `element`.
    pub fn array_of(element: BigQueryFieldType) -> Self {
        Self {
            field_type: element,
            repeated: true,
        }
    }
}

/// What a statement did, as the `Query` response reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BigQueryQueryOutcome {
    /// The job that ran the statement.
    pub job: Option<BigQueryJobRef>,
    /// `SELECT`, `INSERT`, `CREATE_TABLE`, and so on.
    pub statement_type: Option<String>,
    /// Rows a DML statement changed.
    pub num_dml_affected_rows: Option<i64>,
    /// Rows a DML statement inserted, updated and deleted.
    pub dml_stats: Option<BigQueryDmlStats>,
    /// Rows in the result.
    pub total_rows: Option<u64>,
    /// Bytes the statement processed.
    pub total_bytes_processed: Option<i64>,
    /// Whether the result came from the query cache.
    pub cache_hit: Option<bool>,
}

/// The rows a DML statement changed, by kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BigQueryDmlStats {
    /// Rows inserted.
    pub inserted: i64,
    /// Rows updated.
    pub updated: i64,
    /// Rows deleted.
    pub deleted: i64,
}

/// What a dry run reports without running the statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BigQueryDryRunResult {
    /// Bytes the statement would process.
    pub total_bytes_processed: Option<i64>,
    /// The schema of the result.
    pub schema: Option<BigQueryTableSchema>,
}

/// A BigQuery job.
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct BigQueryJobRef {
    /// The project that runs the job.
    pub project_id: String,
    /// The job's ID.
    pub job_id: String,
    /// Where the job runs, when BigQuery reported it.
    pub location: Option<String>,
}
