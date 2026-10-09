use crate::errors::BigQueryError;
use crate::query::ParamList;
use crate::sql::parameter_position;
use crate::{
    BigQueryDatasetRef, BigQueryDestinationWrite, BigQueryDryRunResult, BigQueryJobCreation,
    BigQueryJobStats, BigQueryParamType, BigQueryQueryDestination, BigQueryQueryOutcome,
    BigQueryQueryParams, BigQueryQuerySupport, BigQueryReadOptions, BigQueryResult, BigQuerySql,
    BigQueryTableRef,
};
use crate::{BigQueryLabels, BigQueryLocation, BigQueryRequestId};
use arrow_array::RecordBatch;
use futures::stream::BoxStream;
use gcloud_sdk::google::cloud::bigquery::v2::QueryParameter;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::marker::PhantomData;
use std::time::Duration;

/// A GoogleSQL query being built, from
/// [`BigQueryExprBuilder::query`](crate::BigQueryExprBuilder::query): add parameters and job
/// settings, then pick a terminal.
///
/// The builder methods never fail. A parameter that cannot be encoded is kept, and the
/// terminal returns its error without sending anything.
///
/// A result that comes complete in the first `Query` response is decoded from its inline
/// Arrow; a larger one is read through the Storage Read API from the job's destination table.
#[derive(Clone, Debug)]
pub struct BigQueryQueryBuilder<'a, D>
where
    D: BigQueryQuerySupport,
{
    db: &'a D,
    params: BigQueryQueryParams,
    /// The statement's parameters, moved into `params` by the terminal.
    parameters: ParamList,
    /// The parameter names a statement from [`sql_file!`](crate::sql_file!) declares, which
    /// the bound named parameters must match.
    declared_parameters: Option<&'static [&'static str]>,
}

impl<'a, D> BigQueryQueryBuilder<'a, D>
where
    D: BigQueryQuerySupport,
{
    pub(crate) fn new(db: &'a D, sql: BigQuerySql) -> Self {
        let (text, declared_parameters) = sql.into_parts();
        Self {
            db,
            params: BigQueryQueryParams::new(text),
            parameters: ParamList::default(),
            declared_parameters,
        }
    }

    /// Adds the named parameter `@name`, its type inferred from the value's serde form:
    /// integers are INT64, floats FLOAT64, strings STRING, sequences ARRAY, structs and maps
    /// STRUCT, and the crate's wrappers ([`BigQueryTimestamp`](crate::BigQueryTimestamp),
    /// [`BigQueryDecimal`](crate::BigQueryDecimal), ...) their own types.
    ///
    /// A plain `jiff` value is a STRING, since jiff serializes as text; use a wrapper or
    /// [`param_as`](Self::param_as). `None`, an empty sequence and elements of different types
    /// cannot be inferred, and the terminal fails with
    /// [`InvalidParametersError`](crate::errors::BigQueryError::InvalidParametersError).
    pub fn param<N: Into<String>, V: Serialize>(mut self, name: N, value: V) -> Self {
        self.parameters.named(&name.into(), &value);
        self
    }

    /// Adds the named parameter `@name` of type `ty`, the value in any form the write path
    /// takes for that type, so `param_as("t", BigQueryFieldType::Timestamp, ts)` works for a
    /// plain `jiff::Timestamp`. `None` is a NULL of that type.
    pub fn param_as<N: Into<String>, V: Serialize>(
        mut self,
        name: N,
        ty: impl Into<BigQueryParamType>,
        value: V,
    ) -> Self {
        self.parameters.named_as(&name.into(), &ty.into(), &value);
        self
    }

    /// Adds every top-level field of a struct or string-keyed map as a named parameter,
    /// inferred as by [`param`](Self::param).
    pub fn params<P: Serialize + ?Sized>(mut self, params: &P) -> Self {
        self.parameters.fields(params);
        self
    }

    /// Adds the next positional parameter `?`, inferred as by [`param`](Self::param). Named
    /// and positional parameters together fail at the terminal.
    pub fn positional_param<V: Serialize>(mut self, value: V) -> Self {
        self.parameters.positional(&value);
        self
    }

    /// Adds the next positional parameter `?` of type `ty`, as by
    /// [`param_as`](Self::param_as).
    pub fn positional_param_as<V: Serialize>(
        mut self,
        ty: impl Into<BigQueryParamType>,
        value: V,
    ) -> Self {
        self.parameters.positional_as(&ty.into(), &value);
        self
    }

    /// Runs the job in this location. Leave it unset unless needed: BigQuery finds the
    /// location from the tables the statement reads, and a wrong one fails with
    /// [`DataNotFoundError`](crate::errors::BigQueryError::DataNotFoundError).
    pub fn location(self, location: BigQueryLocation) -> Self {
        Self {
            params: self.params.with_location(location),
            ..self
        }
    }

    /// Resolves unqualified table names in this dataset: a [`BigQueryDatasetId`](crate::BigQueryDatasetId) in the
    /// client's project, or a [`BigQueryDatasetRef`] for another project.
    pub fn default_dataset(self, dataset: impl Into<BigQueryDatasetRef>) -> Self {
        Self {
            params: self.params.with_default_dataset(dataset.into()),
            ..self
        }
    }

    /// Attaches a label to the job.
    pub fn label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.params.labels.insert(key.into(), value.into());
        self
    }

    /// Attaches labels to the job, in addition to those already set.
    pub fn labels(mut self, labels: impl Into<BigQueryLabels>) -> Self {
        for (key, value) in labels.into() {
            self.params.labels.insert(key, value);
        }
        self
    }

    /// Fails the job without running it if it would bill more bytes than this.
    pub fn maximum_bytes_billed(self, bytes: i64) -> Self {
        Self {
            params: self.params.with_maximum_bytes_billed(bytes),
            ..self
        }
    }

    /// Whether BigQuery may answer from its query cache; it does by default.
    pub fn use_query_cache(self, use_query_cache: bool) -> Self {
        Self {
            params: self.params.with_use_query_cache(use_query_cache),
            ..self
        }
    }

    /// How long the first `Query` call waits for the job, 10 seconds by default. A job still
    /// running then is polled until it completes.
    pub fn timeout(self, timeout: Duration) -> Self {
        Self {
            params: self.params.with_timeout(timeout),
            ..self
        }
    }

    /// How long BigQuery lets the job run before it cancels it.
    pub fn job_timeout(self, job_timeout: Duration) -> Self {
        Self {
            params: self.params.with_job_timeout(job_timeout),
            ..self
        }
    }

    /// Sets the idempotency key of the `Query` call. Without it each terminal call sends a
    /// fresh random key, repeated by its retries, so that a retried DML statement is not run
    /// twice.
    pub fn request_id(self, request_id: BigQueryRequestId) -> Self {
        Self {
            params: self.params.with_request_id(request_id),
            ..self
        }
    }

    /// The most rows the first response may carry inline. A result with more is read through
    /// the Storage Read API, which pays a session to open and then streams faster.
    pub fn inline_rows_limit(self, rows: u32) -> Self {
        Self {
            params: self.params.with_inline_rows_limit(rows),
            ..self
        }
    }

    /// Makes BigQuery run the query as a job, which it otherwise skips for a short query whose
    /// result fits in the first response.
    ///
    /// A query without a job is answered sooner, and is still listed in the
    /// `INFORMATION_SCHEMA.JOBS` views with its labels, under its
    /// [`query_id`](crate::BigQueryJobStats::query_id). But its outcome names no
    /// [`job`](crate::BigQueryQueryOutcome::job), so there is nothing for `GetJob` to read or
    /// for [`cancel_job`](crate::BigQueryDb::cancel_job) to cancel. Set this when a query needs
    /// a job resource: for job history read through the job calls, for a job ID it is sure to
    /// get, or to be cancelled by its job.
    pub fn job_creation_required(self) -> Self {
        Self {
            params: self.params.with_job_creation(BigQueryJobCreation::Required),
            ..self
        }
    }

    /// How a result read through the Storage Read API opens its session.
    pub fn read_options(self, options: BigQueryReadOptions) -> Self {
        Self {
            params: self.params.with_read_options(options),
            ..self
        }
    }

    /// Writes the result into `table`, a table of your own, and reads the rows back from it
    /// through the Storage Read API. BigQuery creates the table when it does not exist, and
    /// the job fails with
    /// [`DataConflictError`](crate::errors::BigQueryError::DataConflictError) if the table
    /// already holds rows, so nothing is overwritten.
    ///
    /// A destination table has no size limit on the result, which a query's temporary table
    /// has. The query then always runs as a job, created with `InsertJob`, so
    /// [`job_creation_required`](Self::job_creation_required),
    /// [`inline_rows_limit`](Self::inline_rows_limit) and [`request_id`](Self::request_id) do
    /// not apply. Unlike a temporary table, the table stays, and its storage and the read of
    /// it are billed.
    ///
    /// [`dry_run`](Self::dry_run) sends the same job configuration as a dry run, so BigQuery
    /// checks the destination too, and writes nothing.
    pub fn destination_table(self, table: impl Into<BigQueryTableRef>) -> Self {
        self.destination(table.into(), BigQueryDestinationWrite::IfEmpty)
    }

    /// Writes the result into `table` after the rows it already holds, as
    /// [`destination_table`](Self::destination_table) otherwise. The rows read back are the
    /// whole table's, the earlier ones included.
    pub fn append_to_destination_table(self, table: impl Into<BigQueryTableRef>) -> Self {
        self.destination(table.into(), BigQueryDestinationWrite::Append)
    }

    /// Replaces every row of `table` and its schema with the result, as
    /// [`destination_table`](Self::destination_table) otherwise. The rows the table held are
    /// lost once the job succeeds.
    pub fn dangerously_overwrite_destination_table(
        self,
        table: impl Into<BigQueryTableRef>,
    ) -> Self {
        self.destination(table.into(), BigQueryDestinationWrite::Overwrite)
    }

    fn destination(self, table: BigQueryTableRef, write: BigQueryDestinationWrite) -> Self {
        Self {
            params: self
                .params
                .with_destination(BigQueryQueryDestination { table, write }),
            ..self
        }
    }

    fn checked(self) -> BigQueryResult<(&'a D, BigQueryQueryParams)> {
        let parameters = self.parameters.into_parameters()?;
        if let Some(declared) = self.declared_parameters {
            Self::check_bound_names(declared, &parameters)?;
        }
        let mut params = self.params;
        params.query_parameters = parameters;
        Ok((self.db, params))
    }

    /// Refuses named parameters that differ from the names a [`sql_file!`](crate::sql_file!)
    /// statement declares: the macro checks the file against its list when the caller compiles,
    /// and this checks the `.param` calls against the same list. Names compare ignoring ASCII
    /// case, as BigQuery compares them, so a name bound twice in different case is refused too:
    /// BigQuery answers it as a duplicate parameter.
    fn check_bound_names(declared: &[&str], bound: &[QueryParameter]) -> BigQueryResult<()> {
        let named: Vec<&str> = bound
            .iter()
            .map(|param| param.name.as_str())
            .filter(|name| !name.is_empty())
            .collect();
        if let Some(unbound) = declared
            .iter()
            .find(|name| parameter_position(&named, name.as_bytes()).is_none())
        {
            return Err(BigQueryError::invalid_parameters(
                *unbound,
                format!("the SQL file uses @{unbound}, and no value is bound to it"),
            ));
        }
        for (index, name) in named.iter().enumerate() {
            if parameter_position(declared, name.as_bytes()).is_none() {
                return Err(BigQueryError::invalid_parameters(
                    *name,
                    format!("a value is bound to {name}, which the SQL file does not use"),
                ));
            }
            if parameter_position(&named, name.as_bytes()) != Some(index) {
                return Err(BigQueryError::invalid_parameters(
                    *name,
                    format!("a value is bound to {name} more than once"),
                ));
            }
        }
        Ok(())
    }

    /// Reads the result rows as `T`, with serde. A DML or DDL statement has no rows.
    pub fn obj<T>(self) -> BigQueryQueryObjBuilder<'a, D, T>
    where
        T: DeserializeOwned + Send + 'static,
    {
        BigQueryQueryObjBuilder {
            query: self,
            _target: PhantomData,
        }
    }

    /// Runs the statement, waits for it, and reports what it did without reading its rows:
    /// the row counts of a DML statement, the bytes processed, whether the cache answered.
    pub async fn execute(self) -> BigQueryResult<BigQueryQueryOutcome> {
        let (db, params) = self.checked()?;
        db.execute_query(params).await
    }

    /// Validates the statement and reports the bytes it would process and its result schema,
    /// without running it.
    pub async fn dry_run(self) -> BigQueryResult<BigQueryDryRunResult> {
        let (db, params) = self.checked()?;
        db.dry_run_query(params).await
    }

    /// Streams the result as Arrow record batches. A read stream of a large result that fails
    /// for good is one `Err` item, and then the stream ends.
    pub async fn record_batches<'b>(
        self,
    ) -> BigQueryResult<BoxStream<'b, BigQueryResult<RecordBatch>>> {
        let (db, params) = self.checked()?;
        db.query_record_batches(params).await
    }
}

/// A typed query: pick a terminal.
///
/// Rows of a large result are decoded on each read stream's task, so `T` is `Send + 'static`
/// and they arrive in no particular order. Dropping the stream does not cancel the job; see
/// [`BigQueryDb::cancel_job`](crate::BigQueryDb::cancel_job).
#[derive(Clone, Debug)]
pub struct BigQueryQueryObjBuilder<'a, D, T>
where
    D: BigQueryQuerySupport,
{
    query: BigQueryQueryBuilder<'a, D>,
    _target: PhantomData<fn() -> T>,
}

impl<'a, D, T> BigQueryQueryObjBuilder<'a, D, T>
where
    D: BigQueryQuerySupport,
    T: DeserializeOwned + Send + 'static,
{
    fn checked(self) -> BigQueryResult<(&'a D, BigQueryQueryParams)> {
        self.query.checked()
    }

    /// Reads every row into a `Vec`, failing on the first row or stream error.
    pub async fn query(self) -> BigQueryResult<Vec<T>> {
        let (db, params) = self.checked()?;
        db.query_obj(params).await
    }

    /// Reads every row into a `Vec`, as [`query`](Self::query), with what the job used: bytes
    /// processed and billed, slot milliseconds, whether the cache answered.
    pub async fn query_with_stats(self) -> BigQueryResult<(Vec<T>, BigQueryJobStats)> {
        let (db, params) = self.checked()?;
        db.query_obj_with_stats(params).await
    }

    /// Streams the rows, as [`stream_query_with_errors`](Self::stream_query_with_errors), with
    /// what the job used.
    ///
    /// The stats are returned beside the stream rather than at its end: the query waits for
    /// its job to finish before the first row streams, so every figure BigQuery reports is
    /// known by then, from the `Query` response or from the job a large result is read from.
    /// What reading the rows cost is on the read's span instead.
    pub async fn stream_query_with_stats<'b>(
        self,
    ) -> BigQueryResult<(BoxStream<'b, BigQueryResult<T>>, BigQueryJobStats)> {
        let (db, params) = self.checked()?;
        db.stream_query_obj_with_stats(params).await
    }

    /// Streams the rows. A row that fails to decode is logged at `error!` and skipped; the log
    /// line names the error's kind, row and field path and leaves out its message, which can
    /// hold the cell's text. A read stream that fails for good is logged too and ends the
    /// stream, so a stream that ends does not mean every row was read; use
    /// [`stream_query_with_errors`](Self::stream_query_with_errors) to tell the two apart and to
    /// get each error in full.
    pub async fn stream_query<'b>(self) -> BigQueryResult<BoxStream<'b, T>> {
        let (db, params) = self.checked()?;
        db.stream_query_obj(params).await
    }

    /// Streams the rows, yielding every failure as an `Err` item. A row that fails to decode
    /// is one `Err(DeserializeError)` and the stream goes on; a read stream that fails for good
    /// is one `Err`, and then the stream ends.
    pub async fn stream_query_with_errors<'b>(
        self,
    ) -> BigQueryResult<BoxStream<'b, BigQueryResult<T>>> {
        let (db, params) = self.checked()?;
        db.stream_query_obj_with_errors(params).await
    }
}
