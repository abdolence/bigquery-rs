//! The job registry: every query that runs as a job, in memory, keyed by project and job ID.
//!
//! A job is kept until `DeleteJob`, for `job_ttl` after it finished, or until the registry passes
//! [`MAX_JOBS`] and it is among the oldest finished ones. A job's result table goes with it.
//! Jobs live in the process: they do not survive a restart, and another engine on the same root
//! does not see them.

use crate::error::{EngineError, Resource};
use crate::ids::{JobId, Location, ProjectId, TableRef};
use crate::sql::StatementKind;
use gcloud_sdk::google::cloud::bigquery::v2;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::watch;
use tokio::task::AbortHandle;

/// The most jobs the registry holds; past it the oldest finished ones go.
const MAX_JOBS: usize = 10_000;

/// How long a `request_id` answers with the job its first request created.
const REQUEST_ID_TTL: Duration = Duration::from_secs(15 * 60);

/// A job's identity: the project that owns it and its ID.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct JobKey {
    pub project: ProjectId,
    pub job: JobId,
}

/// Why a job failed, as `error_result` reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct JobFailure {
    /// BigQuery's reason: `invalidQuery`, `notImplemented`, `notFound`, `stopped`, ….
    pub reason: &'static str,
    pub message: String,
}

impl From<&EngineError> for JobFailure {
    fn from(error: &EngineError) -> Self {
        let reason = match error {
            EngineError::InvalidQuery { .. } | EngineError::Invalid { .. } => "invalidQuery",
            EngineError::Unsupported { .. } => "notImplemented",
            EngineError::NotFound { .. } | EngineError::NotFoundInLocation { .. } => "notFound",
            EngineError::AlreadyExists { .. } => "duplicate",
            EngineError::BytesBilledLimitExceeded { .. } => "bytesBilledLimitExceeded",
            EngineError::ResourcesExceeded { .. } => "resourcesExceeded",
            _ => "internalError",
        };
        Self {
            reason,
            message: error.to_string(),
        }
    }
}

impl JobFailure {
    /// A job stopped by `CancelJob`.
    pub(crate) fn stopped() -> Self {
        Self {
            reason: "stopped",
            message: "Job execution was cancelled: User requested cancellation".to_string(),
        }
    }

    /// The failure as the answer of a call that waited for the job.
    pub(crate) fn error(&self, what: Resource) -> EngineError {
        EngineError::JobFailed {
            what,
            reason: self.reason,
            message: self.message.clone(),
        }
    }

    /// A job stopped by its `job_timeout_ms`.
    pub(crate) fn timed_out(timeout: Duration) -> Self {
        Self {
            reason: "timeout",
            message: format!(
                "Job execution was cancelled: Job timed out after {} ms",
                timeout.as_millis()
            ),
        }
    }

    /// The failure as `error_result` and `errors` report it.
    pub(crate) fn proto(&self) -> v2::ErrorProto {
        v2::ErrorProto {
            reason: self.reason.to_string(),
            message: self.message.clone(),
            ..Default::default()
        }
    }
}

/// Where a finished query's rows are.
#[derive(Debug, Clone)]
pub(crate) enum ResultRows {
    /// In the response, for a query answered without a job.
    Inline(InlineRows),
    /// In a table: the job's anonymous result table or the caller's destination.
    Table(TableRef),
    /// Nowhere: a dry run.
    None,
}

/// A whole result as one Arrow IPC schema message and one record batch message.
#[derive(Debug, Clone)]
pub(crate) struct InlineRows {
    pub schema: Vec<u8>,
    /// Empty for a result without rows.
    pub batch: Vec<u8>,
}

/// What a finished query reports.
#[derive(Debug, Clone)]
pub(crate) struct Completed {
    pub statement: StatementKind,
    pub schema: v2::TableSchema,
    pub total_rows: u64,
    pub bytes_processed: u64,
    pub rows: ResultRows,
}

/// A job's state.
#[derive(Debug, Clone)]
pub(crate) enum JobState {
    /// Waiting for one of the engine's job permits.
    Pending,
    Running {
        started: SystemTime,
    },
    Done(Arc<Finished>),
}

#[derive(Debug)]
pub(crate) struct Finished {
    pub started: SystemTime,
    pub ended: SystemTime,
    pub outcome: Result<Completed, JobFailure>,
}

/// One job.
#[derive(Debug)]
pub(crate) struct Job {
    pub key: JobKey,
    pub location: Location,
    pub configuration: v2::JobConfiguration,
    created: SystemTime,
    state: watch::Sender<JobState>,
    task: Mutex<Option<AbortHandle>>,
    /// Whether a caller has been told of the job, after which its rows go to a table: a query
    /// answered inline has no job to read them from later.
    reported: Mutex<bool>,
}

impl Job {
    pub(crate) fn new(
        key: JobKey,
        location: Location,
        configuration: v2::JobConfiguration,
    ) -> Self {
        let (state, _) = watch::channel(JobState::Pending);
        Self {
            key,
            location,
            configuration,
            created: SystemTime::now(),
            state,
            task: Mutex::default(),
            reported: Mutex::new(false),
        }
    }

    pub(crate) fn state(&self) -> JobState {
        self.state.borrow().clone()
    }

    pub(crate) fn resource(&self) -> Resource {
        Resource::Job {
            project: self.key.project.clone(),
            job: self.key.job.clone(),
            location: Some(self.location.clone()),
        }
    }

    /// Keeps the handle of the job's task, which `CancelJob` aborts.
    pub(crate) fn attach(&self, task: AbortHandle) {
        *self.task.lock().unwrap_or_else(PoisonError::into_inner) = Some(task);
    }

    pub(crate) fn start(&self) {
        let started = SystemTime::now();
        self.state.send_if_modified(|state| match state {
            JobState::Pending => {
                *state = JobState::Running { started };
                true
            }
            _ => false,
        });
    }

    /// Finishes the job with `outcome`, unless it finished already: a cancel that won keeps its
    /// `stopped`.
    pub(crate) fn finish(&self, outcome: Result<Completed, JobFailure>) {
        let ended = SystemTime::now();
        self.state.send_if_modified(|state| {
            let started = match state {
                JobState::Pending => ended,
                JobState::Running { started } => *started,
                JobState::Done(_) => return false,
            };
            *state = JobState::Done(Arc::new(Finished {
                started,
                ended,
                outcome,
            }));
            true
        });
    }

    /// Finishes the job with rows kept in memory, unless a caller has been told of the job,
    /// in which case its rows must go to a table and the result is handed back.
    pub(crate) fn finish_inline(&self, completed: Completed) -> Result<(), Completed> {
        let reported = self.reported.lock().unwrap_or_else(PoisonError::into_inner);
        if *reported {
            return Err(completed);
        }
        self.finish(Ok(completed));
        Ok(())
    }

    /// Marks the job as told to a caller, unless it finished with rows in memory, which are
    /// returned for an answer without a job.
    pub(crate) fn report(&self) -> Option<Arc<Finished>> {
        let mut reported = self.reported.lock().unwrap_or_else(PoisonError::into_inner);
        if let JobState::Done(finished) = self.state() {
            if let Ok(Completed {
                rows: ResultRows::Inline(_),
                ..
            }) = &finished.outcome
            {
                if !*reported {
                    return Some(finished);
                }
            }
        }
        *reported = true;
        None
    }

    /// Stops a running job; a finished one stays finished.
    pub(crate) fn cancel(&self) {
        if let Some(task) = self
            .task
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        {
            task.abort();
        }
        self.finish(Err(JobFailure::stopped()));
    }

    /// Waits up to `timeout` for the job to finish, and returns its state then.
    pub(crate) async fn wait(&self, timeout: Duration) -> JobState {
        let mut state = self.state.subscribe();
        let _ = tokio::time::timeout(
            timeout,
            state.wait_for(|state| matches!(state, JobState::Done(_))),
        )
        .await;
        self.state()
    }

    /// The table that holds the job's rows, when it finished with one.
    pub(crate) fn result_table(&self) -> Option<TableRef> {
        match self.state() {
            JobState::Done(finished) => match &finished.outcome {
                Ok(Completed {
                    rows: ResultRows::Table(table),
                    ..
                }) => Some(table.clone()),
                _ => None,
            },
            _ => None,
        }
    }

    /// The job as `GetJob` reports it.
    pub(crate) fn v2(&self) -> v2::Job {
        let mut configuration = self.configuration.clone();
        let mut statistics = v2::JobStatistics {
            creation_time: millis(self.created),
            ..Default::default()
        };
        let mut status = v2::JobStatus {
            state: "PENDING".to_string(),
            ..Default::default()
        };
        match self.state() {
            JobState::Pending => {}
            JobState::Running { started } => {
                status.state = "RUNNING".to_string();
                statistics.start_time = millis(started);
            }
            JobState::Done(finished) => {
                status.state = "DONE".to_string();
                statistics.start_time = millis(finished.started);
                statistics.end_time = millis(finished.ended);
                match &finished.outcome {
                    Ok(completed) => {
                        let bytes = i64::try_from(completed.bytes_processed).ok();
                        statistics.total_bytes_processed = bytes;
                        statistics.query = Some(v2::JobStatistics2 {
                            total_bytes_processed: bytes,
                            statement_type: completed.statement.name().to_string(),
                            cache_hit: Some(false),
                            schema: Some(completed.schema.clone()),
                            ..Default::default()
                        });
                        if let (ResultRows::Table(table), Some(query)) =
                            (&completed.rows, configuration.query.as_mut())
                        {
                            query.destination_table = Some(table.into());
                        }
                    }
                    Err(failure) => {
                        status.error_result = Some(failure.proto());
                        status.errors = vec![failure.proto()];
                    }
                }
            }
        }
        v2::Job {
            kind: "bigquery#job".to_string(),
            id: format!("{}:{}.{}", self.key.project, self.location, self.key.job),
            job_reference: Some(self.reference()),
            configuration: Some(configuration),
            statistics: Some(statistics),
            status: Some(status),
            ..Default::default()
        }
    }

    pub(crate) fn reference(&self) -> v2::JobReference {
        v2::JobReference {
            project_id: self.key.project.to_string(),
            job_id: self.key.job.to_string(),
            location: Some(self.location.to_string()),
        }
    }

    fn finished_at(&self) -> Option<SystemTime> {
        match self.state() {
            JobState::Done(finished) => Some(finished.ended),
            _ => None,
        }
    }
}

/// Milliseconds since the Unix epoch.
fn millis(time: SystemTime) -> i64 {
    time.duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |since| {
            i64::try_from(since.as_millis()).unwrap_or(i64::MAX)
        })
}

/// Every job of the engine, and the jobs each recent `request_id` created.
#[derive(Debug)]
pub(crate) struct JobRegistry {
    ttl: Duration,
    jobs: Mutex<HashMap<JobKey, Arc<Job>>>,
    requests: Mutex<HashMap<(ProjectId, String), (JobKey, Instant)>>,
}

impl JobRegistry {
    pub(crate) fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            jobs: Mutex::default(),
            requests: Mutex::default(),
        }
    }

    /// Adds `job`, and returns the jobs it pushed out, whose result tables go with them.
    ///
    /// # Errors
    /// [`EngineError::AlreadyExists`] naming `project:location.job_id` when the ID is taken.
    pub(crate) fn insert(&self, job: Arc<Job>) -> Result<Vec<Arc<Job>>, EngineError> {
        let mut jobs = self.jobs.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(existing) = jobs.get(&job.key) {
            return Err(EngineError::AlreadyExists {
                what: existing.resource(),
            });
        }
        jobs.insert(job.key.clone(), job);
        let now = SystemTime::now();
        let mut finished: Vec<(SystemTime, JobKey)> = jobs
            .values()
            .filter_map(|job| job.finished_at().map(|ended| (ended, job.key.clone())))
            .collect();
        finished.sort_by_key(|(ended, _)| *ended);
        let mut evicted = Vec::new();
        for (ended, key) in finished {
            let expired = now.duration_since(ended).is_ok_and(|age| age >= self.ttl);
            if !expired && jobs.len() <= MAX_JOBS {
                break;
            }
            if let Some(job) = jobs.remove(&key) {
                evicted.push(job);
            }
        }
        Ok(evicted)
    }

    /// The job, when `location`, if the request names one, is the job's.
    ///
    /// # Errors
    /// [`EngineError::NotFound`] for an unknown job or another location.
    pub(crate) fn get(
        &self,
        project: &ProjectId,
        job: &JobId,
        location: Option<&Location>,
    ) -> Result<Arc<Job>, EngineError> {
        let key = JobKey {
            project: project.clone(),
            job: job.clone(),
        };
        let found = self
            .jobs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&key)
            .cloned();
        match found {
            Some(found) if location.is_none_or(|location| *location == found.location) => Ok(found),
            _ => Err(EngineError::NotFound {
                what: Resource::Job {
                    project: project.clone(),
                    job: job.clone(),
                    location: location.cloned(),
                },
            }),
        }
    }

    /// Removes a finished job.
    ///
    /// # Errors
    /// [`EngineError::Precondition`] for a job that has not finished.
    pub(crate) fn remove(&self, job: &Arc<Job>) -> Result<(), EngineError> {
        if !matches!(job.state(), JobState::Done(_)) {
            return Err(EngineError::Precondition {
                what: job.resource(),
                detail: "a running job cannot be deleted".to_string(),
            });
        }
        self.jobs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&job.key);
        Ok(())
    }

    /// Forgets a job that was never told to a caller, a query answered inline.
    pub(crate) fn forget(&self, key: &JobKey) {
        self.jobs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(key);
    }

    /// The project's jobs, newest first.
    pub(crate) fn list(&self, project: &ProjectId) -> Vec<Arc<Job>> {
        let mut jobs: Vec<Arc<Job>> = self
            .jobs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .filter(|job| job.key.project == *project)
            .cloned()
            .collect();
        jobs.sort_by_key(|job| std::cmp::Reverse(job.created));
        jobs
    }

    /// The job a recent request with `request_id` created.
    pub(crate) fn by_request(&self, project: &ProjectId, request_id: &str) -> Option<Arc<Job>> {
        let key = {
            let mut requests = self.requests.lock().unwrap_or_else(PoisonError::into_inner);
            requests.retain(|_, (_, at)| at.elapsed() < REQUEST_ID_TTL);
            requests
                .get(&(project.clone(), request_id.to_string()))
                .map(|(key, _)| key.clone())
        }?;
        self.jobs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&key)
            .cloned()
    }

    /// Records that the request `request_id` created `job`.
    pub(crate) fn remember_request(&self, request_id: &str, job: &Job) {
        if request_id.is_empty() {
            return;
        }
        self.requests
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(
                (job.key.project.clone(), request_id.to_string()),
                (job.key.clone(), Instant::now()),
            );
    }
}
