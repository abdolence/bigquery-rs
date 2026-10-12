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

use crate::catalog::{now_ms, EngineCatalog, NewDataset, SnapshotChange};
use crate::config::{ByteSize, EngineConfig};
use crate::error::EngineError;
use crate::ids::{DatasetId, DatasetRef, JobId, Location, ProjectId, TableId, TableRef};
use crate::services::page;
use crate::sql::{NameScope, Parameters, SqlEngine, Statement, StatementKind};
use crate::table::TableDefinition;
use crate::types::{Columns, IpcCompression, IpcEncoder, WireSchema};
use crate::write::{FileSink, TableWriter, WriteDisposition, WriteTarget, Writes, WrittenFiles};
use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::{execute_stream, ExecutionPlan};
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
    job_ttl: Duration,
    /// The most bytes of one result ever held for an inline answer.
    #[cfg(test)]
    inline_peak: Arc<std::sync::atomic::AtomicUsize>,
}

/// A statement and how to run it, from `Query` or `InsertJob`.
#[derive(Debug, Clone)]
struct QuerySpec {
    project: ProjectId,
    sql: String,
    default_dataset: Option<DatasetRef>,
    location: Option<Location>,
    parameters: Parameters,
    maximum_bytes_billed: Option<ByteSize>,
    job_timeout: Option<Duration>,
    output: Output,
    compression: Option<IpcCompression>,
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
    create: CreateDisposition,
    write: DestinationWrite,
}

/// A query's `create_disposition`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CreateDisposition {
    /// `CREATE_IF_NEEDED`, the default.
    IfNeeded,
    /// `CREATE_NEVER`: the table must exist.
    Never,
}

impl FromStr for CreateDisposition {
    type Err = EngineError;

    fn from_str(disposition: &str) -> Result<Self, Self::Err> {
        match disposition {
            "" | "CREATE_IF_NEEDED" => Ok(CreateDisposition::IfNeeded),
            "CREATE_NEVER" => Ok(CreateDisposition::Never),
            other => Err(EngineError::invalid(
                "create_disposition",
                format!("{other:?} is not a create disposition"),
            )),
        }
    }
}

/// A query's `write_disposition`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DestinationWrite {
    /// `WRITE_EMPTY`, the default, and `WRITE_APPEND`: the result is appended.
    Append(WriteDisposition),
    /// `WRITE_TRUNCATE`: the result replaces every row of the table in one snapshot.
    Overwrite,
}

impl FromStr for DestinationWrite {
    type Err = EngineError;

    fn from_str(disposition: &str) -> Result<Self, Self::Err> {
        match disposition {
            "" | "WRITE_EMPTY" => Ok(DestinationWrite::Append(WriteDisposition::Empty)),
            "WRITE_APPEND" => Ok(DestinationWrite::Append(WriteDisposition::Append)),
            "WRITE_TRUNCATE" => Ok(DestinationWrite::Overwrite),
            other => Err(EngineError::invalid(
                "write_disposition",
                format!("{other:?} is not a write disposition"),
            )),
        }
    }
}

/// A statement planned over one snapshot of each table it names.
struct Planned {
    session: SessionContext,
    plan: LogicalPlan,
    kind: StatementKind,
    location: Location,
}

/// A running statement: its wire schema, the bytes its scans read, and its batches as the plan
/// produces them.
struct Execution {
    wire: WireSchema,
    bytes_processed: ByteSize,
    session: SessionContext,
    stream: SendableRecordBatchStream,
}

impl Execution {
    /// The next batch of the result, in the wire types.
    async fn next(&mut self) -> Result<Option<RecordBatch>, EngineError> {
        match self.stream.try_next().await? {
            Some(batch) => Ok(Some(self.wire.normalize(&batch)?)),
            None => Ok(None),
        }
    }
}

/// The batches of a result held for an inline answer: no more rows than the request's limit and
/// no more bytes than the engine's inline limit, reserved from the engine's memory budget.
struct InlineBuffer {
    batches: Vec<RecordBatch>,
    rows: u64,
    max_rows: Option<u64>,
    limit: ByteSize,
    reservation: MemoryReservation,
    #[cfg(test)]
    peak: Arc<std::sync::atomic::AtomicUsize>,
}

impl InlineBuffer {
    /// Holds `batch` when the result still fits inline with it, else hands it back: the result
    /// then goes to a table, and this buffer holds no more.
    fn push(&mut self, batch: RecordBatch) -> Result<(), RecordBatch> {
        let rows = self
            .rows
            .saturating_add(u64::try_from(batch.num_rows()).unwrap_or(u64::MAX));
        let bytes = batch.get_array_memory_size();
        let fits = self.max_rows.is_none_or(|max| rows <= max)
            && u64::try_from(self.reservation.size().saturating_add(bytes))
                .is_ok_and(|held| held <= self.limit.bytes())
            && self.reservation.try_grow(bytes).is_ok();
        if !fits {
            return Err(batch);
        }
        self.rows = rows;
        self.batches.push(batch);
        #[cfg(test)]
        self.peak
            .fetch_max(self.reservation.size(), std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}

/// A table a result is written into: data files its job's commit lands.
struct ResultSink {
    table: TableRef,
    target: WriteTarget,
    files: FileSink,
    write: DestinationWrite,
}

impl ResultSink {
    /// A sink into `table` as it is now.
    async fn open(
        catalog: &EngineCatalog,
        table: TableRef,
        write: DestinationWrite,
    ) -> Result<Self, EngineError> {
        let loaded = catalog.load(&table).await?;
        Ok(Self {
            target: WriteTarget::new(&loaded)?,
            files: TableWriter::new(&loaded)?.open().await?,
            table,
            write,
        })
    }

    /// Writes one batch of the result, in the wire types.
    async fn write(&mut self, batch: RecordBatch) -> Result<(), EngineError> {
        self.files.write(self.target.wire_rows(batch)?).await
    }
}

/// A result written as files of its table, which the job's commit is still to land.
struct Written {
    table: TableRef,
    files: WrittenFiles,
    write: DestinationWrite,
    completed: Completed,
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
            job_ttl: config.job_ttl,
            #[cfg(test)]
            inline_peak: Arc::default(),
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
                return job.query_response(&job.wait(wait).await);
            }
        }
        let spec = QuerySpec {
            default_dataset: dataset_of(&project, query.default_dataset.as_ref())?,
            location: location_of(&query.location)?,
            parameters: Parameters::try_from(query.query_parameters.as_slice())?,
            maximum_bytes_billed: maximum_bytes_billed(query.maximum_bytes_billed),
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
            let (wire, bytes) = planned.estimate().await?;
            return Ok(v2::QueryResponse {
                kind: "bigquery#queryResponse".to_string(),
                schema: Some(v2::TableSchema::try_from(&wire)?),
                location: location.to_string(),
                total_bytes_processed: i64::try_from(bytes.bytes()).ok(),
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
            return job.inline_response(&finished);
        }
        job.query_response(&state)
    }

    /// `InsertJob`: a query job, into the caller's destination table when it names one.
    ///
    /// # Errors
    /// [`EngineError::AlreadyExists`] for a job ID in use, [`EngineError::Unsupported`] for a
    /// job that is not a query, and the statement's planning errors.
    pub(crate) async fn insert_job(
        &self,
        request: v2::InsertJobRequest,
    ) -> Result<v2::Job, EngineError> {
        let project = ProjectId::from_str(&request.project_id)?;
        let body = request
            .job
            .ok_or_else(|| EngineError::invalid("job", "the request carries none"))?;
        let configuration = body
            .configuration
            .ok_or_else(|| EngineError::invalid("job.configuration", "the job carries none"))?;
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
                create: query.create_disposition.parse()?,
                write: query.write_disposition.parse()?,
            }),
            None => Output::Table,
        };
        let spec = QuerySpec {
            default_dataset: dataset_of(&project, query.default_dataset.as_ref())?,
            location: location_of(reference.location.as_deref().unwrap_or_default())?,
            parameters: Parameters::try_from(query.query_parameters.as_slice())?,
            maximum_bytes_billed: maximum_bytes_billed(query.maximum_bytes_billed),
            job_timeout: timeout_of(configuration.job_timeout_ms),
            output,
            compression: None,
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
            let (wire, bytes) = planned.estimate().await?;
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
                    response.total_bytes_processed =
                        i64::try_from(completed.bytes_processed.bytes()).ok();
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
        self.drop_result_table(&job);
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
            self.catalog
                .load(table)
                .await?
                .register(&session, table)
                .await?;
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
        for evicted in self.jobs.insert(job.clone())? {
            self.drop_result_table(&evicted);
        }
        let queries = self.clone();
        let running = job.clone();
        tokio::spawn(async move { queries.run(running, spec, planned).await });
        Ok(job)
    }

    /// Runs a job to its end. Until its rows are written, `CancelJob` and the job's timeout stop
    /// it; once their commit is queued, it finishes with the commit's outcome, since the rows
    /// may land whatever the job reports. A job that ends without rows to keep drops its
    /// anonymous result table.
    async fn run(self, job: Arc<Job>, spec: QuerySpec, planned: Planned) {
        let delivered = tokio::select! {
            biased;
            delivered = self.deliver_in_time(&job, &spec, planned) => delivered,
            () = job.finished() => Err(JobFailure::stopped()),
        };
        let outcome = match delivered {
            // Answered inline.
            Ok(None) => return,
            Ok(Some(written)) if job.begin_commit() => self
                .land(&written.table, written.files, written.write)
                .await
                .map(|()| written.completed)
                .map_err(|err| JobFailure::from(&err)),
            Ok(Some(_)) => Err(JobFailure::stopped()),
            Err(failure) => Err(failure),
        };
        if outcome.is_err() {
            self.drop_result_table(&job);
        }
        job.finish(outcome);
    }

    async fn land(
        &self,
        table: &TableRef,
        files: WrittenFiles,
        write: DestinationWrite,
    ) -> Result<(), EngineError> {
        match write {
            DestinationWrite::Append(disposition) => {
                self.writes.append_files(table, files, disposition).await
            }
            DestinationWrite::Overwrite => {
                let change = SnapshotChange::Overwrite {
                    table_uuid: files.table_uuid,
                    files: files.files,
                };
                self.catalog.commit_snapshot(table, &change).await?;
                Ok(())
            }
        }
    }

    /// Waits for a permit, then delivers the job's rows within its timeout.
    async fn deliver_in_time(
        &self,
        job: &Job,
        spec: &QuerySpec,
        planned: Planned,
    ) -> Result<Option<Written>, JobFailure> {
        let _permit = self.permits.acquire().await.map_err(|_| {
            JobFailure::from(&EngineError::Internal {
                detail: "the engine's job permits are closed".to_string(),
            })
        })?;
        job.start();
        let delivered = self.deliver(job, spec, planned);
        match spec.job_timeout {
            Some(timeout) => tokio::time::timeout(timeout, delivered)
                .await
                .unwrap_or_else(|_| Err(JobFailure::timed_out(timeout))),
            None => delivered.await,
        }
    }

    /// Runs the statement and puts its rows where the job's output says: inline, finishing the
    /// job and returning `None`, or written batch by batch into files of a table, never held
    /// whole, for the job's commit to land.
    async fn deliver(
        &self,
        job: &Job,
        spec: &QuerySpec,
        planned: Planned,
    ) -> Result<Option<Written>, JobFailure> {
        let failure = |err: EngineError| JobFailure::from(&err);
        let statement = planned.kind;
        let mut execution = planned
            .execute(spec.maximum_bytes_billed)
            .await
            .map_err(failure)?;
        let schema = v2::TableSchema::try_from(&execution.wire).map_err(failure)?;
        let bytes_processed = execution.bytes_processed;
        let (mut sink, held) = match &spec.output {
            Output::InlineOrTable { max_rows } => {
                let mut buffer = self.inline_buffer(&execution, *max_rows);
                let overflow = loop {
                    match execution.next().await.map_err(failure)? {
                        Some(batch) => {
                            if let Err(batch) = buffer.push(batch) {
                                break Some(batch);
                            }
                        }
                        None => break None,
                    }
                };
                if overflow.is_none() {
                    let inline = Completed {
                        statement,
                        schema: schema.clone(),
                        total_rows: buffer.rows,
                        bytes_processed,
                        rows: ResultRows::Inline(
                            InlineRows::new(&execution.wire, &buffer.batches, spec.compression)
                                .map_err(failure)?,
                        ),
                    };
                    if job.finish_inline(inline).is_ok() {
                        return Ok(None);
                    }
                }
                let mut held = std::mem::take(&mut buffer.batches);
                drop(buffer);
                held.extend(overflow);
                let sink = self
                    .anonymous_sink(job, &execution.wire)
                    .await
                    .map_err(failure)?;
                (sink, held)
            }
            Output::Table => {
                let sink = self
                    .anonymous_sink(job, &execution.wire)
                    .await
                    .map_err(failure)?;
                (sink, Vec::new())
            }
            Output::Destination(destination) => {
                let sink = self
                    .destination_sink(destination, &execution.wire)
                    .await
                    .map_err(failure)?;
                (sink, Vec::new())
            }
        };
        for batch in held {
            sink.write(batch).await.map_err(failure)?;
        }
        while let Some(batch) = execution.next().await.map_err(failure)? {
            sink.write(batch).await.map_err(failure)?;
        }
        let files = sink.files.close().await.map_err(failure)?;
        Ok(Some(Written {
            completed: Completed {
                statement,
                schema,
                total_rows: files.rows,
                bytes_processed,
                rows: ResultRows::Table(sink.table.clone()),
            },
            table: sink.table,
            files,
            write: sink.write,
        }))
    }

    /// An empty buffer for the inline answer of `execution`'s result.
    fn inline_buffer(&self, execution: &Execution, max_rows: Option<u64>) -> InlineBuffer {
        InlineBuffer {
            batches: Vec::new(),
            rows: 0,
            max_rows,
            limit: self.inline_limit,
            reservation: MemoryConsumer::new("inline result")
                .register(&execution.session.runtime_env().memory_pool),
            #[cfg(test)]
            peak: Arc::clone(&self.inline_peak),
        }
    }

    /// A sink into the job's anonymous result table, created for it.
    async fn anonymous_sink(
        &self,
        job: &Job,
        wire: &WireSchema,
    ) -> Result<ResultSink, EngineError> {
        let table = job
            .anonymous_table()?
            .ok_or_else(|| EngineError::Internal {
                detail: format!(
                    "job {} has a destination and no anonymous result table",
                    job.key.job
                ),
            })?;
        let created = self
            .catalog
            .create_dataset(
                &table.dataset_ref(),
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
        self.create_result_table(&table, wire).await?;
        ResultSink::open(
            &self.catalog,
            table,
            DestinationWrite::Append(WriteDisposition::Append),
        )
        .await
    }

    /// A sink into the caller's destination table, created when allowed. Its write
    /// disposition is checked by the commit.
    async fn destination_sink(
        &self,
        destination: &Destination,
        wire: &WireSchema,
    ) -> Result<ResultSink, EngineError> {
        match self.catalog.load(&destination.table).await {
            Ok(_) => {}
            Err(EngineError::NotFound { .. })
                if destination.create == CreateDisposition::IfNeeded =>
            {
                match self.create_result_table(&destination.table, wire).await {
                    Ok(()) | Err(EngineError::AlreadyExists { .. }) => {}
                    Err(err) => return Err(err),
                }
            }
            Err(err) => return Err(err),
        }
        ResultSink::open(&self.catalog, destination.table.clone(), destination.write).await
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

    /// Deletes the anonymous result table of `job`, if it has one, on a task of its own.
    fn drop_result_table(&self, job: &Job) {
        let table = match job.anonymous_table() {
            Ok(Some(table)) => table,
            Ok(None) => return,
            Err(err) => {
                warn!(job = %job.key.job, %err, "A job's result table has no valid name.");
                return;
            }
        };
        let catalog = self.catalog.clone();
        tokio::spawn(async move {
            if let Err(err) = catalog.purge_anonymous(&table).await {
                warn!(%table, %err, "A job's result table could not be deleted.");
            }
        });
    }

    /// Deletes every anonymous result table under the root last changed more than `job_ttl`
    /// ago: no job outlives the process that ran it, and any job whose table is younger may
    /// still be running in another engine on the root.
    pub(crate) async fn drop_expired_results(&self) -> Result<(), EngineError> {
        let anonymous = DatasetId::from_str(ANONYMOUS_DATASET)?;
        let now = now_ms();
        let ttl = i64::try_from(self.job_ttl.as_millis()).unwrap_or(i64::MAX);
        for project in self.catalog.projects().await? {
            let dataset = DatasetRef {
                project,
                dataset: anonymous.clone(),
            };
            for (table, loaded) in self.catalog.tables(&dataset).await? {
                if now.saturating_sub(loaded.metadata().last_updated_ms()) >= ttl {
                    self.catalog.purge_anonymous(&dataset.table(table)).await?;
                }
            }
        }
        Ok(())
    }
}

impl Planned {
    /// The physical plan, analyzed and optimized: the result schema comes from it, never from
    /// the logical plan before analysis, whose types the analyzer may still change.
    async fn physical(&self) -> Result<Arc<dyn ExecutionPlan>, EngineError> {
        Ok(self
            .session
            .execute_logical_plan(self.plan.clone())
            .await?
            .create_physical_plan()
            .await?)
    }

    /// Starts the statement.
    ///
    /// # Errors
    /// [`EngineError::BytesBilledLimitExceeded`] when the scan's estimate passes
    /// `maximum_bytes_billed`, and the statement's own failures.
    async fn execute(
        self,
        maximum_bytes_billed: Option<ByteSize>,
    ) -> Result<Execution, EngineError> {
        let physical = self.physical().await?;
        let bytes_processed = scanned_bytes(&physical).await?;
        if let Some(limit) = maximum_bytes_billed {
            if bytes_processed > limit {
                return Err(EngineError::BytesBilledLimitExceeded {
                    estimated: bytes_processed,
                    limit,
                });
            }
        }
        Ok(Execution {
            wire: WireSchema::of_result(&physical.schema())?,
            bytes_processed,
            stream: execute_stream(physical, self.session.task_ctx())?,
            session: self.session,
        })
    }

    /// The result schema and the bytes the scans would read, without running the statement.
    async fn estimate(self) -> Result<(WireSchema, ByteSize), EngineError> {
        let physical = self.physical().await?;
        Ok((
            WireSchema::of_result(&physical.schema())?,
            scanned_bytes(&physical).await?,
        ))
    }
}

/// The bytes the plan's Iceberg scans read: the length of every file task they plan, with their
/// pushed-down predicates.
async fn scanned_bytes(plan: &Arc<dyn ExecutionPlan>) -> Result<ByteSize, EngineError> {
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
    Ok(ByteSize::from_bytes(total))
}

impl InlineRows {
    /// `batches`, a whole result, as one schema message and one record batch message, the form
    /// the client decodes an inline result in.
    fn new(
        wire: &WireSchema,
        batches: &[RecordBatch],
        compression: Option<IpcCompression>,
    ) -> Result<Self, EngineError> {
        let mut encoder = IpcEncoder::new(wire.schema(), compression)?;
        let schema = encoder.schema_message().to_vec();
        if batches.iter().all(|batch| batch.num_rows() == 0) {
            return Ok(Self {
                schema,
                batch: Vec::new(),
            });
        }
        let whole =
            concat_batches(wire.schema(), batches).map_err(|err| EngineError::Internal {
                detail: format!("the result's batches do not concatenate: {err}"),
            })?;
        Ok(Self {
            schema,
            batch: encoder.encode(&whole)?,
        })
    }
}

impl Job {
    /// The anonymous table that holds the job's rows: every job's but a dry run's and one with
    /// a destination, whether the table was ever created or not.
    fn anonymous_table(&self) -> Result<Option<TableRef>, EngineError> {
        let destination = self
            .configuration
            .query
            .as_ref()
            .is_some_and(|query| query.destination_table.is_some());
        if destination || self.configuration.dry_run == Some(true) {
            return Ok(None);
        }
        let dataset = DatasetRef {
            project: self.key.project.clone(),
            dataset: DatasetId::from_str(ANONYMOUS_DATASET)?,
        };
        Ok(Some(
            dataset.table(TableId::from_str(self.key.job.as_str())?),
        ))
    }

    /// The answer to `Query` for this job in `state`: complete with its figures, or not yet.
    ///
    /// # Errors
    /// The job's failure, when it failed.
    fn query_response(&self, state: &JobState) -> Result<v2::QueryResponse, EngineError> {
        let mut response = v2::QueryResponse {
            kind: "bigquery#queryResponse".to_string(),
            job_reference: Some(self.reference()),
            location: self.location.to_string(),
            job_complete: Some(false),
            ..Default::default()
        };
        if let JobState::Done(finished) = state {
            let completed = finished
                .outcome
                .as_ref()
                .map_err(|failure| failure.error(self.resource()))?;
            response.job_complete = Some(true);
            response.page_token = ROWS_IN_TABLE.to_string();
            response.schema = Some(completed.schema.clone());
            response.total_rows = Some(completed.total_rows);
            response.total_bytes_processed = i64::try_from(completed.bytes_processed.bytes()).ok();
            response.cache_hit = Some(false);
            response.statement_type = completed.statement.name().to_string();
        }
        Ok(response)
    }

    /// The answer to `Query` with the whole result inline, without a job.
    fn inline_response(&self, finished: &jobs::Finished) -> Result<v2::QueryResponse, EngineError> {
        let completed = finished
            .outcome
            .as_ref()
            .map_err(|failure| failure.error(self.resource()))?;
        let ResultRows::Inline(rows) = &completed.rows else {
            return Err(EngineError::Internal {
                detail: "an inline answer for a result kept in a table".to_string(),
            });
        };
        Ok(v2::QueryResponse {
            kind: "bigquery#queryResponse".to_string(),
            query_id: Uuid::new_v4().to_string(),
            location: self.location.to_string(),
            schema: Some(completed.schema.clone()),
            total_rows: Some(completed.total_rows),
            total_bytes_processed: i64::try_from(completed.bytes_processed.bytes()).ok(),
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

/// A request's `maximum_bytes_billed`, `None` when it sets none.
fn maximum_bytes_billed(bytes: Option<i64>) -> Option<ByteSize> {
    bytes
        .and_then(|bytes| u64::try_from(bytes).ok())
        .map(ByteSize::from_bytes)
}

/// A job's timeout, `None` when the request sets none.
fn timeout_of(job_timeout_ms: Option<i64>) -> Option<Duration> {
    job_timeout_ms
        .and_then(|ms| u64::try_from(ms).ok())
        .filter(|ms| *ms > 0)
        .map(Duration::from_millis)
}

impl IpcCompression {
    /// The compression a `Query` request asks its inline result in, `None` for none.
    fn of_query(query: &v2::QueryRequest) -> Option<Self> {
        use v2::arrow_serialization_options::CompressionCodec;
        use v2::query_request::ResultsFormatSerializationOptions;
        match &query.results_format_serialization_options {
            Some(ResultsFormatSerializationOptions::ArrowSerializationOptions(arrow)) => {
                match CompressionCodec::try_from(arrow.buffer_compression) {
                    Ok(CompressionCodec::Lz4Frame) => Some(IpcCompression::Lz4Frame),
                    Ok(CompressionCodec::Zstd) => Some(IpcCompression::Zstd),
                    _ => None,
                }
            }
            None => None,
        }
    }
}

#[cfg(test)]
mod tests;
