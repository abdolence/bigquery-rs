//! The query path: a statement planned over tables pinned at one snapshot runs as a job, and its
//! result is normalized to the wire types and answered inline or written to a table.
//!
//! `Query` runs the statement on a task and waits up to `timeout_ms`. A result that fits the
//! request's `max_results` and the engine's inline byte limit is answered inline, without a job,
//! unless the request requires one. Anything else is a job: the response names it, and its rows
//! are in an anonymous result table, `<project>._smallquery_anon.<job_id>`, which `GetJob` names
//! and the client reads through Storage Read. `InsertJob` writes into the caller's destination
//! table instead.
//!
//! A job's location is the request's, else the location of the first dataset the statement
//! names, else the engine's default; a statement that names a dataset in another location fails
//! with `NotFound`, as BigQuery answers it. The anonymous result dataset is the engine's own and
//! exempt, since the client reads result tables through Storage Read, which names no location.

mod jobs;

pub(crate) use jobs::JobRegistry;

use crate::catalog::{now_ms, EngineCatalog, NewDataset};
use crate::config::{ByteSize, EngineConfig};
use crate::error::{EngineError, Resource};
use crate::ids::{DatasetId, DatasetRef, JobId, Location, ProjectId, TableId, TableRef};
use crate::services::page;
use crate::sql::{register_table, NameScope, Parameters, SqlEngine, Statement, StatementKind};
use crate::table::TableDefinition;
use crate::types::{Columns, IpcCompression, IpcEncoder, WireSchema};
use crate::write::{TableWriter, WriteTarget, Writes};
use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use gcloud_sdk::google::cloud::bigquery::v2;
use iceberg_datafusion::physical_plan::IcebergTableScan;
use jobs::{Completed, InlineRows, Job, JobFailure, JobKey, JobState, ResultRows};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tracing::warn;
use uuid::Uuid;

/// The dataset of each project that holds its jobs' anonymous result tables.
const ANONYMOUS_DATASET: &str = "_smallquery_anon";

/// How long `Query` and `GetQueryResults` wait for a job when the request sets no timeout.
const DEFAULT_WAIT: Duration = Duration::from_secs(10);

/// The page token of a finished query whose rows are in a table, which tells the client that
/// the response holds none of them.
const ROWS_IN_TABLE: &str = "rows-in-result-table";

/// The query side of one engine: planning, the jobs and their results.
#[derive(Debug, Clone)]
pub(crate) struct Queries {
    catalog: Arc<EngineCatalog>,
    sql: Arc<SqlEngine>,
    writes: Arc<Writes>,
    jobs: Arc<JobRegistry>,
    permits: Arc<Semaphore>,
    default_location: Location,
    inline_limit: ByteSize,
}

/// A statement and how to run it, from `Query` or `InsertJob`.
#[derive(Debug, Clone)]
struct QuerySpec {
    project: ProjectId,
    sql: String,
    default_dataset: Option<DatasetRef>,
    location: Option<Location>,
    parameters: Parameters,
    maximum_bytes_billed: Option<u64>,
    job_timeout: Option<Duration>,
    output: Output,
    compression: IpcCompression,
}

/// Where a statement's rows go.
#[derive(Debug, Clone)]
enum Output {
    /// Inline when the result has at most `max_rows` rows, if set, and fits the byte limit;
    /// else the anonymous result table.
    InlineOrTable { max_rows: Option<u64> },
    /// The anonymous result table: the request required a job.
    Table,
    /// The caller's table.
    Destination(Destination),
}

/// A caller's destination table and its dispositions.
#[derive(Debug, Clone)]
struct Destination {
    table: TableRef,
    create_if_needed: bool,
    write: WriteDisposition,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WriteDisposition {
    /// `WRITE_APPEND`.
    Append,
    /// `WRITE_EMPTY`: the table must hold no rows.
    Empty,
}

/// A statement planned over one snapshot of each table it names.
struct Planned {
    session: SessionContext,
    plan: LogicalPlan,
    kind: StatementKind,
    location: Location,
}

/// A statement's result: its wire schema, its normalized batches and the bytes its scans read.
struct StatementResult {
    wire: WireSchema,
    batches: Vec<RecordBatch>,
    bytes_processed: u64,
}

impl StatementResult {
    fn rows(&self) -> u64 {
        self.batches
            .iter()
            .map(|batch| u64::try_from(batch.num_rows()).unwrap_or(u64::MAX))
            .sum()
    }

    fn bytes(&self) -> u64 {
        self.batches
            .iter()
            .map(|batch| u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX))
            .sum()
    }
}

impl Queries {
    pub(crate) fn new(
        config: &EngineConfig,
        catalog: Arc<EngineCatalog>,
        sql: Arc<SqlEngine>,
        writes: Arc<Writes>,
    ) -> Self {
        Self {
            catalog,
            sql,
            writes,
            jobs: Arc::new(JobRegistry::new(config.job_ttl)),
            permits: Arc::new(Semaphore::new(config.max_concurrent_jobs)),
            default_location: config.default_location.clone(),
            inline_limit: config.inline_result_limit,
        }
    }

    /// `Query`: runs one statement and answers inline, or with the job that holds its rows.
    ///
    /// # Errors
    /// The statement's own failure when it fails within the wait; [`EngineError::AlreadyExists`]
    /// for a repeated `request_id` that does not require a job.
    pub(crate) async fn query(
        &self,
        request: v2::PostQueryRequest,
    ) -> Result<v2::QueryResponse, EngineError> {
        use v2::query_request::JobCreationMode;
        let project = ProjectId::from_str(&request.project_id)?;
        let query = request
            .query_request
            .ok_or_else(|| EngineError::invalid("query_request", "the request carries none"))?;
        refuse_legacy_sql(query.use_legacy_sql)?;
        let required = query.job_creation_mode == JobCreationMode::JobCreationRequired as i32;
        let wait = wait_of(query.timeout_ms);
        if !query.request_id.is_empty() {
            if let Some(job) = self.jobs.by_request(&project, &query.request_id) {
                if !required {
                    return Err(EngineError::AlreadyExists {
                        what: job.resource(),
                    });
                }
                job.report();
                return job_response(&job, &job.wait(wait).await);
            }
        }
        let spec = QuerySpec {
            default_dataset: dataset_of(&project, query.default_dataset.as_ref())?,
            location: location_of(&query.location)?,
            parameters: Parameters::try_from(query.query_parameters.as_slice())?,
            maximum_bytes_billed: query
                .maximum_bytes_billed
                .and_then(|bytes| u64::try_from(bytes).ok()),
            job_timeout: timeout_of(query.job_timeout_ms),
            output: if required {
                Output::Table
            } else {
                Output::InlineOrTable {
                    max_rows: query.max_results.map(u64::from),
                }
            },
            compression: IpcCompression::of_query(&query),
            sql: query.query.clone(),
            project,
        };
        let planned = self.plan(&spec).await?;
        if query.dry_run {
            let location = planned.location.clone();
            let kind = planned.kind;
            let (wire, bytes) = estimate(planned).await?;
            return Ok(v2::QueryResponse {
                kind: "bigquery#queryResponse".to_string(),
                schema: Some(v2::TableSchema::try_from(&wire)?),
                location: location.to_string(),
                total_bytes_processed: i64::try_from(bytes).ok(),
                job_complete: Some(true),
                cache_hit: Some(false),
                statement_type: kind.name().to_string(),
                ..Default::default()
            });
        }
        let configuration = v2::JobConfiguration {
            job_type: "QUERY".to_string(),
            query: Some(v2::JobConfigurationQuery {
                query: query.query,
                default_dataset: query.default_dataset,
                use_legacy_sql: Some(false),
                parameter_mode: query.parameter_mode,
                query_parameters: query.query_parameters,
                maximum_bytes_billed: query.maximum_bytes_billed,
                ..Default::default()
            }),
            job_timeout_ms: query.job_timeout_ms,
            labels: query.labels,
            ..Default::default()
        };
        let job = self.start(spec, planned, JobId::random(), configuration)?;
        self.jobs.remember_request(&query.request_id, &job);
        let state = job.wait(wait).await;
        if let Some(finished) = job.report() {
            self.jobs.forget(&job.key);
            return inline_response(&job, &finished);
        }
        job_response(&job, &state)
    }

    /// `InsertJob`: a query job, into the caller's destination table when it names one.
    ///
    /// # Errors
    /// [`EngineError::AlreadyExists`] for a job ID in use, [`EngineError::Unsupported`] for a
    /// job that is not a query and for `WRITE_TRUNCATE`, and the statement's planning errors.
    pub(crate) async fn insert_job(
        &self,
        request: v2::InsertJobRequest,
    ) -> Result<v2::Job, EngineError> {
        let project = ProjectId::from_str(&request.project_id)?;
        let body = request
            .job
            .ok_or_else(|| EngineError::invalid("job", "the request carries none"))?;
        let configuration = body.configuration.unwrap_or_default();
        let query = configuration.query.clone().ok_or_else(|| {
            EngineError::unsupported(
                "load, copy and extract jobs",
                "this release runs query jobs only",
            )
        })?;
        refuse_legacy_sql(query.use_legacy_sql)?;
        let reference = body.job_reference.unwrap_or_default();
        let job_id = match reference.job_id.as_str() {
            "" => JobId::random(),
            id => JobId::from_str(id)?,
        };
        if let Ok(existing) = self.jobs.get(&project, &job_id, None) {
            return Err(EngineError::AlreadyExists {
                what: existing.resource(),
            });
        }
        let output = match &query.destination_table {
            Some(destination) => Output::Destination(Destination {
                table: TableRef::parse(
                    &destination.project_id,
                    &destination.dataset_id,
                    &destination.table_id,
                )?,
                create_if_needed: match query.create_disposition.as_str() {
                    "" | "CREATE_IF_NEEDED" => true,
                    "CREATE_NEVER" => false,
                    other => {
                        return Err(EngineError::invalid(
                            "create_disposition",
                            format!("{other:?} is not a create disposition"),
                        ))
                    }
                },
                write: match query.write_disposition.as_str() {
                    "" | "WRITE_EMPTY" => WriteDisposition::Empty,
                    "WRITE_APPEND" => WriteDisposition::Append,
                    "WRITE_TRUNCATE" => {
                        return Err(EngineError::unsupported(
                            "WRITE_TRUNCATE",
                            "this release appends to a destination table",
                        ))
                    }
                    other => {
                        return Err(EngineError::invalid(
                            "write_disposition",
                            format!("{other:?} is not a write disposition"),
                        ))
                    }
                },
            }),
            None => Output::Table,
        };
        let spec = QuerySpec {
            default_dataset: dataset_of(&project, query.default_dataset.as_ref())?,
            location: location_of(reference.location.as_deref().unwrap_or_default())?,
            parameters: Parameters::try_from(query.query_parameters.as_slice())?,
            maximum_bytes_billed: query
                .maximum_bytes_billed
                .and_then(|bytes| u64::try_from(bytes).ok()),
            job_timeout: timeout_of(configuration.job_timeout_ms),
            output,
            compression: IpcCompression::None,
            sql: query.query.clone(),
            project: project.clone(),
        };
        let planned = self.plan(&spec).await?;
        if configuration.dry_run == Some(true) {
            let job = Job::new(
                JobKey {
                    project,
                    job: job_id,
                },
                planned.location.clone(),
                configuration,
            );
            let kind = planned.kind;
            let (wire, bytes) = estimate(planned).await?;
            job.finish(Ok(Completed {
                statement: kind,
                schema: v2::TableSchema::try_from(&wire)?,
                total_rows: 0,
                bytes_processed: bytes,
                rows: ResultRows::None,
            }));
            return Ok(job.v2());
        }
        let job = self.start(spec, planned, job_id, configuration)?;
        job.report();
        Ok(job.v2())
    }

    /// `GetQueryResults`: waits up to `timeout_ms` for the job. Rows are never returned here:
    /// the client reads them from the job's table.
    ///
    /// # Errors
    /// [`EngineError::NotFound`] for an unknown job or another location, and
    /// [`EngineError::Unsupported`] for a request that asks for rows.
    pub(crate) async fn get_query_results(
        &self,
        request: v2::GetQueryResultsRequest,
    ) -> Result<v2::GetQueryResultsResponse, EngineError> {
        if request.max_results.is_some_and(|rows| rows > 0) {
            return Err(EngineError::unsupported(
                "rows of GetQueryResults",
                "rows are read from the job's table through the Storage Read API",
            ));
        }
        let job = self.job(&request.project_id, &request.job_id, &request.location)?;
        job.report();
        let state = job.wait(wait_of(request.timeout_ms)).await;
        let mut response = v2::GetQueryResultsResponse {
            kind: "bigquery#getQueryResultsResponse".to_string(),
            job_reference: Some(job.reference()),
            job_complete: Some(false),
            ..Default::default()
        };
        if let JobState::Done(finished) = state {
            response.job_complete = Some(true);
            match &finished.outcome {
                Ok(completed) => {
                    response.schema = Some(completed.schema.clone());
                    response.total_rows = Some(completed.total_rows);
                    response.total_bytes_processed = i64::try_from(completed.bytes_processed).ok();
                    response.cache_hit = Some(false);
                }
                Err(failure) => response.errors = vec![failure.proto()],
            }
        }
        Ok(response)
    }

    /// `GetJob`.
    ///
    /// # Errors
    /// [`EngineError::NotFound`] for an unknown job or another location.
    pub(crate) fn get_job(&self, request: &v2::GetJobRequest) -> Result<v2::Job, EngineError> {
        let job = self.job(&request.project_id, &request.job_id, &request.location)?;
        job.report();
        Ok(job.v2())
    }

    /// `CancelJob`: stops a running job; a finished one stays finished.
    ///
    /// # Errors
    /// [`EngineError::NotFound`] for an unknown job or another location.
    pub(crate) fn cancel_job(
        &self,
        request: &v2::CancelJobRequest,
    ) -> Result<v2::JobCancelResponse, EngineError> {
        let job = self.job(&request.project_id, &request.job_id, &request.location)?;
        job.cancel();
        Ok(v2::JobCancelResponse {
            kind: "bigquery#jobCancelResponse".to_string(),
            job: Some(job.v2()),
        })
    }

    /// `DeleteJob`: removes a finished job and its anonymous result table.
    ///
    /// # Errors
    /// [`EngineError::NotFound`] for an unknown job or another location, and
    /// [`EngineError::Precondition`] for a running one.
    pub(crate) fn delete_job(&self, request: &v2::DeleteJobRequest) -> Result<(), EngineError> {
        let job = self.job(&request.project_id, &request.job_id, &request.location)?;
        self.jobs.remove(&job)?;
        self.drop_results(vec![job]);
        Ok(())
    }

    /// `ListJobs`: the project's jobs, newest first, filtered and paged.
    pub(crate) fn list_jobs(
        &self,
        request: &v2::ListJobsRequest,
    ) -> Result<v2::JobList, EngineError> {
        use v2::list_jobs_request::StateFilter;
        let project = ProjectId::from_str(&request.project_id)?;
        let states: Vec<StateFilter> = request
            .state_filter
            .iter()
            .filter_map(|state| StateFilter::try_from(*state).ok())
            .collect();
        let jobs: Vec<v2::ListFormatJob> = if request.parent_job_id.is_empty() {
            self.jobs
                .list(&project)
                .into_iter()
                .map(|job| job.v2())
                .filter(|job| {
                    let state = job
                        .status
                        .as_ref()
                        .map_or("", |status| status.state.as_str());
                    states.is_empty()
                        || states.iter().any(|filter| match filter {
                            StateFilter::Done => state == "DONE",
                            StateFilter::Pending => state == "PENDING",
                            StateFilter::Running => state == "RUNNING",
                        })
                })
                .filter(|job| {
                    let created = job
                        .statistics
                        .as_ref()
                        .map_or(0, |statistics| statistics.creation_time);
                    let created = u64::try_from(created).unwrap_or(0);
                    created >= request.min_creation_time
                        && request.max_creation_time.is_none_or(|max| created <= max)
                })
                .map(|job| v2::ListFormatJob {
                    id: job.id,
                    kind: job.kind,
                    job_reference: job.job_reference,
                    state: job
                        .status
                        .as_ref()
                        .map(|status| status.state.clone())
                        .unwrap_or_default(),
                    error_result: job
                        .status
                        .as_ref()
                        .and_then(|status| status.error_result.clone()),
                    statistics: job.statistics,
                    configuration: job.configuration,
                    status: job.status,
                    ..Default::default()
                })
                .collect()
        } else {
            Vec::new()
        };
        let max_results = request.max_results.and_then(|max| u32::try_from(max).ok());
        let (jobs, next_page_token) = page(jobs, max_results, &request.page_token)?;
        Ok(v2::JobList {
            kind: "bigquery#jobList".to_string(),
            next_page_token,
            jobs,
            ..Default::default()
        })
    }

    /// The job a request names, in the location it names, if any.
    fn job(&self, project: &str, job: &str, location: &str) -> Result<Arc<Job>, EngineError> {
        self.jobs.get(
            &ProjectId::from_str(project)?,
            &JobId::from_str(job)?,
            location_of(location)?.as_ref(),
        )
    }

    /// Parses the statement, resolves the location of its job, loads every table it names at
    /// the current snapshot and plans it with its parameters bound.
    async fn plan(&self, spec: &QuerySpec) -> Result<Planned, EngineError> {
        let statement = Statement::from_str(&spec.sql)?;
        let prepared = statement.prepare(&NameScope {
            project: spec.project.clone(),
            default_dataset: spec.default_dataset.clone(),
        })?;
        let mut location = spec.location.clone();
        let session = self.sql.session();
        for table in prepared.tables() {
            let dataset_ref = table.dataset_ref();
            let dataset = self.catalog.dataset(&dataset_ref).await?;
            if dataset_ref.dataset.as_str() != ANONYMOUS_DATASET {
                let location = location.get_or_insert_with(|| dataset.location.clone());
                if dataset.location != *location {
                    return Err(EngineError::NotFoundInLocation {
                        dataset: dataset_ref,
                        location: location.clone(),
                    });
                }
            }
            let loaded = self.catalog.load(table).await?;
            register_table(&session, table, &loaded).await?;
        }
        let kind = prepared.kind();
        let plan = prepared.plan(&session, &spec.parameters).await?;
        Ok(Planned {
            session,
            plan,
            kind,
            location: location.unwrap_or_else(|| self.default_location.clone()),
        })
    }

    /// Registers a job for `planned` and starts it on a task.
    fn start(
        &self,
        spec: QuerySpec,
        planned: Planned,
        job_id: JobId,
        configuration: v2::JobConfiguration,
    ) -> Result<Arc<Job>, EngineError> {
        let job = Arc::new(Job::new(
            JobKey {
                project: spec.project.clone(),
                job: job_id,
            },
            planned.location.clone(),
            configuration,
        ));
        let evicted = self.jobs.insert(job.clone())?;
        self.drop_results(evicted);
        let queries = self.clone();
        let running = job.clone();
        let task = tokio::spawn(async move { queries.run(running, spec, planned).await });
        job.attach(task.abort_handle());
        Ok(job)
    }

    /// Runs a job to its end: waits for a permit, executes, delivers the rows, and finishes the
    /// job with what happened, or with its timeout.
    async fn run(self, job: Arc<Job>, spec: QuerySpec, planned: Planned) {
        let Ok(_permit) = self.permits.clone().acquire_owned().await else {
            job.finish(Err(JobFailure::from(&EngineError::Internal {
                detail: "the engine's job permits are closed".to_string(),
            })));
            return;
        };
        job.start();
        let delivered = self.deliver(&job, &spec, planned);
        let outcome = match spec.job_timeout {
            Some(timeout) => tokio::time::timeout(timeout, delivered)
                .await
                .unwrap_or_else(|_| Err(JobFailure::timed_out(timeout))),
            None => delivered.await,
        };
        if let Err(failure) = outcome {
            job.finish(Err(failure));
        }
    }

    /// Executes the statement and puts its rows where the job's output says, finishing the job.
    async fn deliver(
        &self,
        job: &Job,
        spec: &QuerySpec,
        planned: Planned,
    ) -> Result<(), JobFailure> {
        let kind = planned.kind;
        let failure = |err: EngineError| JobFailure::from(&err);
        let result = execute(planned, spec.maximum_bytes_billed)
            .await
            .map_err(failure)?;
        let completed = |rows| -> Result<Completed, JobFailure> {
            Ok(Completed {
                statement: kind,
                schema: v2::TableSchema::try_from(&result.wire).map_err(failure)?,
                total_rows: result.rows(),
                bytes_processed: result.bytes_processed,
                rows,
            })
        };
        let table = match &spec.output {
            Output::InlineOrTable { max_rows } => {
                let fits = max_rows.is_none_or(|max| result.rows() <= max)
                    && result.bytes() <= self.inline_limit.bytes();
                if fits {
                    let inline = InlineRows::of(&result, spec.compression).map_err(failure)?;
                    if job
                        .finish_inline(completed(ResultRows::Inline(inline))?)
                        .is_ok()
                    {
                        return Ok(());
                    }
                }
                self.write_anonymous(job, &result).await.map_err(failure)?
            }
            Output::Table => self.write_anonymous(job, &result).await.map_err(failure)?,
            Output::Destination(destination) => {
                self.write_destination(destination, &result)
                    .await
                    .map_err(failure)?;
                destination.table.clone()
            }
        };
        job.finish(Ok(completed(ResultRows::Table(table))?));
        Ok(())
    }

    /// Writes the result into the job's anonymous result table, created for it.
    async fn write_anonymous(
        &self,
        job: &Job,
        result: &StatementResult,
    ) -> Result<TableRef, EngineError> {
        let dataset = DatasetRef {
            project: job.key.project.clone(),
            dataset: DatasetId::from_str(ANONYMOUS_DATASET)?,
        };
        let created = self
            .catalog
            .create_dataset(
                &dataset,
                NewDataset {
                    location: self.default_location.clone(),
                    description: None,
                    labels: Default::default(),
                    default_table_expiration_ms: None,
                },
            )
            .await;
        match created {
            Ok(_) | Err(EngineError::AlreadyExists { .. }) => {}
            Err(err) => return Err(err),
        }
        let table = dataset.table(TableId::from_str(job.key.job.as_str())?);
        self.create_result_table(&table, &result.wire).await?;
        self.append_result(&table, result).await?;
        Ok(table)
    }

    /// Appends the result to the caller's destination table, created when allowed.
    async fn write_destination(
        &self,
        destination: &Destination,
        result: &StatementResult,
    ) -> Result<(), EngineError> {
        match self.catalog.load(&destination.table).await {
            Ok(loaded) => {
                // A table whose snapshot does not report its rows is not known to be empty.
                if destination.write == WriteDisposition::Empty && loaded.total_rows()? != Some(0) {
                    return Err(EngineError::AlreadyExists {
                        what: Resource::Table(destination.table.clone()),
                    });
                }
            }
            Err(EngineError::NotFound { .. }) if destination.create_if_needed => {
                self.create_result_table(&destination.table, &result.wire)
                    .await?;
            }
            Err(err) => return Err(err),
        }
        self.append_result(&destination.table, result).await
    }

    /// Creates `table` with the result's columns.
    async fn create_result_table(
        &self,
        table: &TableRef,
        wire: &WireSchema,
    ) -> Result<(), EngineError> {
        let definition = TableDefinition {
            columns: Columns::try_from(&v2::TableSchema::try_from(wire)?)?,
            description: None,
            labels: Default::default(),
            partitioning: None,
            clustering: Vec::new(),
            primary_key: Vec::new(),
            expiration_ms: None,
            max_staleness: None,
        };
        let metadata = definition.metadata(table, self.catalog.table_location(table), now_ms())?;
        self.catalog.create_table(table, metadata).await?;
        Ok(())
    }

    /// Writes the result's rows into `table` and lands them in one commit.
    async fn append_result(
        &self,
        table: &TableRef,
        result: &StatementResult,
    ) -> Result<(), EngineError> {
        let loaded = self.catalog.load(table).await?;
        let target = WriteTarget::new(&loaded)?;
        let batches = result
            .batches
            .iter()
            .map(|batch| target.wire_rows(batch.clone()))
            .collect::<Result<Vec<_>, _>>()?;
        let files = TableWriter::new(&loaded)?.write(batches).await?;
        if files.rows > 0 {
            self.writes.append_files(table, files).await?;
        }
        Ok(())
    }

    /// Deletes the anonymous result tables of jobs that left the registry.
    fn drop_results(&self, jobs: Vec<Arc<Job>>) {
        for job in jobs {
            let Some(table) = job.result_table() else {
                continue;
            };
            if table.dataset.as_str() != ANONYMOUS_DATASET {
                continue;
            }
            let catalog = self.catalog.clone();
            tokio::spawn(async move {
                if let Err(err) = catalog.purge(&table).await {
                    warn!(%table, %err, "A job's result table could not be deleted.");
                }
            });
        }
    }
}

/// Runs the planned statement and normalizes its result to the wire types.
///
/// # Errors
/// [`EngineError::BytesBilledLimitExceeded`] when the scan's estimate passes
/// `maximum_bytes_billed`, and the statement's own failures.
async fn execute(
    planned: Planned,
    maximum_bytes_billed: Option<u64>,
) -> Result<StatementResult, EngineError> {
    let physical = physical_plan(&planned.session, planned.plan).await?;
    let bytes_processed = scanned_bytes(&physical).await?;
    if let Some(limit) = maximum_bytes_billed {
        if bytes_processed > limit {
            return Err(EngineError::BytesBilledLimitExceeded {
                estimated: bytes_processed,
                limit,
            });
        }
    }
    let wire = WireSchema::of_result(&physical.schema())?;
    let batches = collect(physical, planned.session.task_ctx())
        .await?
        .iter()
        .map(|batch| wire.normalize(batch))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(StatementResult {
        wire,
        batches,
        bytes_processed,
    })
}

/// The result schema and the bytes the scans would read, without running the statement.
async fn estimate(planned: Planned) -> Result<(WireSchema, u64), EngineError> {
    let physical = physical_plan(&planned.session, planned.plan).await?;
    Ok((
        WireSchema::of_result(&physical.schema())?,
        scanned_bytes(&physical).await?,
    ))
}

/// The physical plan of `plan`, analyzed and optimized: the result schema comes from it, never
/// from the logical plan before analysis, whose types the analyzer may still change.
async fn physical_plan(
    session: &SessionContext,
    plan: LogicalPlan,
) -> Result<Arc<dyn ExecutionPlan>, EngineError> {
    Ok(session
        .execute_logical_plan(plan)
        .await?
        .create_physical_plan()
        .await?)
}

/// The bytes the plan's Iceberg scans read: the length of every file task they plan, with their
/// pushed-down predicates.
async fn scanned_bytes(plan: &Arc<dyn ExecutionPlan>) -> Result<u64, EngineError> {
    let mut total = 0u64;
    let mut pending = vec![Arc::clone(plan)];
    while let Some(node) = pending.pop() {
        if let Some(scan) = node.as_any().downcast_ref::<IcebergTableScan>() {
            let mut builder = scan.table().scan();
            if let Some(snapshot) = scan.snapshot_id() {
                builder = builder.snapshot_id(snapshot);
            }
            builder = match scan.projection() {
                Some(columns) => builder.select(columns.iter().cloned()),
                None => builder.select_all(),
            };
            if let Some(predicate) = scan.predicates() {
                builder = builder.with_filter(predicate.clone());
            }
            let tasks = builder.build()?.plan_files().await?;
            total = tasks
                .try_fold(total, |sum, task| async move {
                    Ok(sum.saturating_add(task.length))
                })
                .await?;
        }
        pending.extend(node.children().into_iter().cloned());
    }
    Ok(total)
}

impl InlineRows {
    /// The whole result as one schema message and one record batch message, the form the
    /// client decodes an inline result in.
    fn of(result: &StatementResult, compression: IpcCompression) -> Result<Self, EngineError> {
        let mut encoder = IpcEncoder::new(result.wire.schema(), compression)?;
        let schema = encoder.schema_message().to_vec();
        if result.rows() == 0 {
            return Ok(Self {
                schema,
                batch: Vec::new(),
            });
        }
        let whole = concat_batches(result.wire.schema(), &result.batches).map_err(|err| {
            EngineError::Internal {
                detail: format!("the result's batches do not concatenate: {err}"),
            }
        })?;
        Ok(Self {
            schema,
            batch: encoder.encode(&whole)?,
        })
    }
}

/// The answer to `Query` for a job: complete with its figures, or not yet.
///
/// # Errors
/// The job's failure, when it failed.
fn job_response(job: &Job, state: &JobState) -> Result<v2::QueryResponse, EngineError> {
    let mut response = v2::QueryResponse {
        kind: "bigquery#queryResponse".to_string(),
        job_reference: Some(job.reference()),
        location: job.location.to_string(),
        job_complete: Some(false),
        ..Default::default()
    };
    if let JobState::Done(finished) = state {
        let completed = finished
            .outcome
            .as_ref()
            .map_err(|failure| failure.error(job.resource()))?;
        response.job_complete = Some(true);
        response.page_token = ROWS_IN_TABLE.to_string();
        response.schema = Some(completed.schema.clone());
        response.total_rows = Some(completed.total_rows);
        response.total_bytes_processed = i64::try_from(completed.bytes_processed).ok();
        response.cache_hit = Some(false);
        response.statement_type = completed.statement.name().to_string();
    }
    Ok(response)
}

/// The answer to `Query` with its whole result inline, without a job.
fn inline_response(job: &Job, finished: &jobs::Finished) -> Result<v2::QueryResponse, EngineError> {
    let completed = finished
        .outcome
        .as_ref()
        .map_err(|failure| failure.error(job.resource()))?;
    let ResultRows::Inline(rows) = &completed.rows else {
        return Err(EngineError::Internal {
            detail: "an inline answer for a result kept in a table".to_string(),
        });
    };
    Ok(v2::QueryResponse {
        kind: "bigquery#queryResponse".to_string(),
        query_id: Uuid::new_v4().to_string(),
        location: job.location.to_string(),
        schema: Some(completed.schema.clone()),
        total_rows: Some(completed.total_rows),
        total_bytes_processed: i64::try_from(completed.bytes_processed).ok(),
        job_complete: Some(true),
        cache_hit: Some(false),
        statement_type: completed.statement.name().to_string(),
        results_schema: Some(v2::query_response::ResultsSchema::ArrowSchema(
            v2::ArrowSchema {
                serialized_schema: rows.schema.clone(),
            },
        )),
        results: Some(v2::query_response::Results::ArrowRecordBatch(
            v2::ArrowRecordBatch {
                serialized_record_batch: rows.batch.clone(),
            },
        )),
        ..Default::default()
    })
}

/// Refuses legacy SQL, which the engine does not parse.
fn refuse_legacy_sql(use_legacy_sql: Option<bool>) -> Result<(), EngineError> {
    if use_legacy_sql == Some(true) {
        return Err(EngineError::unsupported(
            "legacy SQL",
            "set use_legacy_sql to false",
        ));
    }
    Ok(())
}

/// The location a request names, `None` when it names none.
fn location_of(location: &str) -> Result<Option<Location>, EngineError> {
    match location {
        "" => Ok(None),
        location => Location::from_str(location).map(Some),
    }
}

/// The default dataset a request names, in `project` when it names no project.
fn dataset_of(
    project: &ProjectId,
    dataset: Option<&v2::DatasetReference>,
) -> Result<Option<DatasetRef>, EngineError> {
    dataset
        .filter(|dataset| !dataset.dataset_id.is_empty())
        .map(|dataset| {
            let project = match dataset.project_id.as_str() {
                "" => project.clone(),
                other => ProjectId::from_str(other)?,
            };
            Ok(DatasetRef {
                project,
                dataset: DatasetId::from_str(&dataset.dataset_id)?,
            })
        })
        .transpose()
}

/// How long a call waits for its job.
fn wait_of(timeout_ms: Option<u32>) -> Duration {
    timeout_ms.map_or(DEFAULT_WAIT, |ms| Duration::from_millis(u64::from(ms)))
}

/// A job's timeout, `None` when the request sets none.
fn timeout_of(job_timeout_ms: Option<i64>) -> Option<Duration> {
    job_timeout_ms
        .and_then(|ms| u64::try_from(ms).ok())
        .filter(|ms| *ms > 0)
        .map(Duration::from_millis)
}

impl IpcCompression {
    /// The compression a `Query` request asks its inline result in.
    fn of_query(query: &v2::QueryRequest) -> Self {
        use v2::arrow_serialization_options::CompressionCodec;
        use v2::query_request::ResultsFormatSerializationOptions;
        match &query.results_format_serialization_options {
            Some(ResultsFormatSerializationOptions::ArrowSerializationOptions(arrow)) => {
                match CompressionCodec::try_from(arrow.buffer_compression) {
                    Ok(CompressionCodec::Lz4Frame) => IpcCompression::Lz4Frame,
                    Ok(CompressionCodec::Zstd) => IpcCompression::Zstd,
                    _ => IpcCompression::None,
                }
            }
            None => IpcCompression::None,
        }
    }
}

#[cfg(test)]
mod tests;
