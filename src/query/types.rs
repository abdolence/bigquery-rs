use crate::db::proto::millis;
#[cfg(doc)]
use crate::errors::BigQueryError;
use crate::BigQueryResult;
use crate::{
    BigQueryDatasetRef, BigQueryFieldType, BigQueryReadOptions, BigQueryStatementType,
    BigQueryTableRef, BigQueryTableSchema,
};
use crate::{BigQueryJobId, BigQueryLabels, BigQueryLocation, BigQueryQueryId, BigQueryRequestId};
use gcloud_sdk::google::cloud::bigquery::v2::QueryParameter;
use rsb_derive::Builder;
use std::borrow::Cow;
use std::time::Duration;

/// What a query sends: the statement, its parameters and the job settings, as
/// [`BigQueryQueryBuilder`](crate::BigQueryQueryBuilder) collects them.
#[derive(Debug, PartialEq, Clone, Builder)]
pub struct BigQueryQueryParams {
    /// The GoogleSQL statement. Borrowed when it comes from
    /// [`sql_file!`](crate::sql_file!), so the file's text is copied only into each request.
    pub sql: Cow<'static, str>,
    /// The parameters, encoded. Named ones carry their name, positional ones an empty name.
    #[default = "Vec::new()"]
    pub(crate) query_parameters: Vec<QueryParameter>,
    /// Where the job runs. Unset, the client's
    /// [`location`](crate::BigQueryDbOptions::location) is sent, and with neither BigQuery
    /// finds the location from the tables the statement reads.
    pub location: Option<BigQueryLocation>,
    /// The dataset that unqualified table names resolve in.
    pub default_dataset: Option<BigQueryDatasetRef>,
    /// Labels attached to the job.
    #[default = "BigQueryLabels::new()"]
    pub labels: BigQueryLabels,
    /// The job fails without running if it would bill more bytes than this.
    pub maximum_bytes_billed: Option<i64>,
    /// Whether a cached result may be returned. BigQuery's default is `true`.
    pub use_query_cache: Option<bool>,
    /// How long the first `Query` call waits for the job before the client polls it. Defaults to
    /// 10 seconds, BigQuery's own default.
    pub timeout: Option<Duration>,
    /// How long BigQuery lets the job run before it cancels it.
    pub job_timeout: Option<Duration>,
    /// The idempotency key of the `Query` call. Unset, each terminal call sends a fresh random
    /// one, which every retry of that call repeats.
    pub request_id: Option<BigQueryRequestId>,
    /// The most rows the first response may carry inline. A result with more is read through
    /// the Storage Read API from the job's destination table.
    ///
    /// Unset by default, so BigQuery decides how much to send inline. A small result reaches
    /// its last row sooner inline than through a Storage Read session.
    pub inline_rows_limit: Option<u32>,
    /// How a result read through the Storage Read API opens its session.
    #[default = "BigQueryReadOptions::new()"]
    pub read_options: BigQueryReadOptions,
    /// Whether BigQuery must run the query as a job. Defaults to
    /// [`Optional`](BigQueryJobCreation::Optional).
    #[default = "BigQueryJobCreation::Optional"]
    pub job_creation: BigQueryJobCreation,
    /// The table of your own that the job writes the result into. Unset, BigQuery writes it
    /// into a temporary table of the job.
    ///
    /// With a destination the query always runs as a job, created with `InsertJob`, so
    /// [`job_creation`](Self::job_creation), [`inline_rows_limit`](Self::inline_rows_limit) and
    /// [`request_id`](Self::request_id) do not apply: the rows are read from the table through
    /// the Storage Read API, and the job's own ID is what makes a retried `InsertJob` safe.
    pub destination: Option<BigQueryQueryDestination>,
}

/// How long `Query` waits for the job by default, BigQuery's own default.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

impl BigQueryQueryParams {
    /// The `timeout_ms` that `Query` and `GetQueryResults` take, with BigQuery's default when
    /// none is set.
    ///
    /// # Errors
    /// [`BigQueryError::InvalidParametersError`] for `timeout` if it does not fit in `u32`
    /// milliseconds.
    pub(crate) fn timeout_ms(&self) -> BigQueryResult<u32> {
        millis("timeout", self.timeout.unwrap_or(DEFAULT_TIMEOUT))
    }

    /// The `job_timeout_ms` of the `Query` request.
    ///
    /// # Errors
    /// [`BigQueryError::InvalidParametersError`] for `job_timeout` if it does not fit in `i64`
    /// milliseconds.
    pub(crate) fn job_timeout_ms(&self) -> BigQueryResult<Option<i64>> {
        self.job_timeout
            .map(|d| millis("job_timeout", d))
            .transpose()
    }
}

/// Whether a query runs as a BigQuery job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BigQueryJobCreation {
    /// BigQuery decides, and answers a short query whose result fits in the response without
    /// creating a job: it reports a [`BigQueryQueryId`] and no job. A query that runs long, a
    /// result too large for the response, and some statements still create one.
    #[default]
    Optional,
    /// Every query creates a job, so that the outcome always names one for the job calls to
    /// read or cancel.
    Required,
}

/// The table a query writes its result into, and what happens to the rows the table already
/// holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BigQueryQueryDestination {
    /// The table. BigQuery creates it when it does not exist, with the result's schema.
    pub table: BigQueryTableRef,
    /// What the job does when the table already holds rows.
    pub write: BigQueryDestinationWrite,
}

/// What a query does when its destination table already holds rows. Each one applies only
/// if the job succeeds, as one atomic update of the table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum BigQueryDestinationWrite {
    /// The job fails if the table holds any rows, so nothing is ever overwritten
    /// (`WRITE_EMPTY`, BigQuery's default).
    #[default]
    IfEmpty,
    /// The result is added to the rows the table holds (`WRITE_APPEND`).
    Append,
    /// The table's rows and schema are replaced by the result (`WRITE_TRUNCATE`), so every row
    /// it held is lost.
    Overwrite,
}

impl BigQueryDestinationWrite {
    /// The `write_disposition` of the job configuration.
    pub(crate) fn disposition(self) -> &'static str {
        match self {
            Self::IfEmpty => "WRITE_EMPTY",
            Self::Append => "WRITE_APPEND",
            Self::Overwrite => "WRITE_TRUNCATE",
        }
    }
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

/// What a statement did, as [`execute`](crate::BigQueryQueryBuilder::execute) reports it: the
/// same figures a query's [`BigQueryJobStats`] carries.
pub type BigQueryQueryOutcome = BigQueryJobStats;

/// What a query used, from the responses the query already received; the
/// `_with_stats` terminals return it with the rows.
///
/// A figure BigQuery did not report is `None`. A result answered whole in the first `Query`
/// response carries that response's figures; any other result also has the figures of the
/// job's statistics, which the query reads before it streams rows.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BigQueryJobStats {
    /// The job that ran the query, `None` when BigQuery ran it without one.
    pub job: Option<BigQueryJobRef>,
    /// The ID BigQuery gave the query, when it reported one.
    pub query_id: Option<BigQueryQueryId>,
    /// The kind of statement.
    pub statement_type: Option<BigQueryStatementType>,
    /// Rows in the result.
    pub total_rows: Option<u64>,
    /// Bytes the query processed.
    pub total_bytes_processed: Option<i64>,
    /// Bytes billed for the query, after BigQuery's rounding and minimums.
    pub total_bytes_billed: Option<i64>,
    /// Slot milliseconds the job used.
    pub total_slot_ms: Option<i64>,
    /// Whether the result came from the query cache.
    pub cache_hit: Option<bool>,
    /// Rows a DML statement changed.
    pub num_dml_affected_rows: Option<i64>,
    /// Rows a DML statement inserted, updated and deleted.
    pub dml_stats: Option<BigQueryDmlStats>,
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
    pub job_id: BigQueryJobId,
    /// Where the job runs, when BigQuery reported it.
    pub location: Option<BigQueryLocation>,
}

impl BigQueryJobRef {
    /// The location as a v2 request field takes it, empty for none.
    pub(crate) fn location_field(&self) -> String {
        self.location
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default()
    }
}

impl From<gcloud_sdk::google::cloud::bigquery::v2::JobReference> for BigQueryJobRef {
    fn from(job: gcloud_sdk::google::cloud::bigquery::v2::JobReference) -> Self {
        Self {
            project_id: job.project_id,
            job_id: BigQueryJobId::reported(job.job_id),
            location: job.location.and_then(BigQueryLocation::reported),
        }
    }
}

impl From<gcloud_sdk::google::cloud::bigquery::v2::DmlStats> for BigQueryDmlStats {
    /// BigQuery leaves out the counts of the kinds a statement did not do, so a count missing
    /// from reported stats is zero.
    fn from(stats: gcloud_sdk::google::cloud::bigquery::v2::DmlStats) -> Self {
        Self {
            inserted: stats.inserted_row_count.unwrap_or(0),
            updated: stats.updated_row_count.unwrap_or(0),
            deleted: stats.deleted_row_count.unwrap_or(0),
        }
    }
}
