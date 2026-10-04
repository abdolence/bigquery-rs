//! Jobs: get, delete and list, through the v2 `JobService`, next to
//! [`BigQueryDb::cancel_job`].

use crate::admin::{logging_errors, paged};
use crate::db::proto::{timestamp_ms, NonEmpty};
use crate::errors::{BigQueryError, BigQueryJobErrorEntry};
use crate::BigQueryInstant;
use crate::{BigQueryDb, BigQueryJobRef, BigQueryResult, BigQueryStatementType};
use crate::{BigQueryJobId, BigQueryLabels};
use futures::stream::BoxStream;
use gcloud_sdk::google::cloud::bigquery::v2;
use rsb_derive::Builder;
use std::fmt::{Display, Formatter};
use tracing::Span;

/// Where a job is in its run, as BigQuery names it in `JobStatus.state`: `PENDING`, `RUNNING`
/// or `DONE`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BigQueryJobState {
    /// Waiting to run.
    Pending,
    /// Running.
    Running,
    /// Finished, with or without an error.
    Done,
    /// A state this crate does not know, as BigQuery named it.
    Other(String),
}

impl BigQueryJobState {
    /// The name as BigQuery writes it, such as `RUNNING`.
    pub fn as_str(&self) -> &str {
        match self {
            Self::Pending => "PENDING",
            Self::Running => "RUNNING",
            Self::Done => "DONE",
            Self::Other(name) => name,
        }
    }

    fn known(name: &str) -> Option<Self> {
        Some(match name {
            "PENDING" => Self::Pending,
            "RUNNING" => Self::Running,
            "DONE" => Self::Done,
            _ => return None,
        })
    }
}

impl From<&str> for BigQueryJobState {
    fn from(name: &str) -> Self {
        Self::known(name).unwrap_or_else(|| Self::Other(name.to_string()))
    }
}

impl From<String> for BigQueryJobState {
    fn from(name: String) -> Self {
        Self::known(&name).unwrap_or(Self::Other(name))
    }
}

impl Display for BigQueryJobState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What kind of work a job does, as BigQuery names it in `JobConfiguration.job_type`: `QUERY`,
/// `LOAD`, `EXTRACT`, `COPY` or `UNKNOWN`.
///
/// A name BigQuery adds later reads as [`Other`](Self::Other) with BigQuery's text, so
/// [`as_str`](Self::as_str) gives back what BigQuery sent for every value.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BigQueryJobType {
    /// A query.
    Query,
    /// A load into a table.
    Load,
    /// An extract from a table to Cloud Storage.
    Extract,
    /// A table copy.
    Copy,
    /// BigQuery's own `UNKNOWN`, which it reports for a job whose kind it cannot tell.
    Unknown,
    /// A type this crate does not know, as BigQuery named it.
    Other(String),
}

impl BigQueryJobType {
    /// The name as BigQuery writes it, such as `QUERY`.
    pub fn as_str(&self) -> &str {
        match self {
            Self::Query => "QUERY",
            Self::Load => "LOAD",
            Self::Extract => "EXTRACT",
            Self::Copy => "COPY",
            Self::Unknown => "UNKNOWN",
            Self::Other(name) => name,
        }
    }

    fn known(name: &str) -> Option<Self> {
        Some(match name {
            "QUERY" => Self::Query,
            "LOAD" => Self::Load,
            "EXTRACT" => Self::Extract,
            "COPY" => Self::Copy,
            "UNKNOWN" => Self::Unknown,
            _ => return None,
        })
    }
}

impl From<&str> for BigQueryJobType {
    fn from(name: &str) -> Self {
        Self::known(name).unwrap_or_else(|| Self::Other(name.to_string()))
    }
}

impl From<String> for BigQueryJobType {
    fn from(name: String) -> Self {
        Self::known(&name).unwrap_or(Self::Other(name))
    }
}

impl Display for BigQueryJobType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A job as `GetJob` or `ListJobs` returns it.
///
/// A job that failed is still a job: its failure is in [`error`](Self::error), not an `Err`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BigQueryJob {
    /// The job.
    pub reference: BigQueryJobRef,
    /// What kind of work the job does.
    pub job_type: Option<BigQueryJobType>,
    /// Where the job is in its run.
    pub state: Option<BigQueryJobState>,
    /// Why a finished job failed.
    pub error: Option<BigQueryJobErrorEntry>,
    /// Who ran the job.
    pub user_email: Option<String>,
    /// The job's labels.
    pub labels: BigQueryLabels,
    /// For a query, the kind of statement it ran.
    pub statement_type: Option<BigQueryStatementType>,
    /// When the job was created.
    pub creation_time: Option<BigQueryInstant>,
    /// When the job started running.
    pub start_time: Option<BigQueryInstant>,
    /// When the job finished.
    pub end_time: Option<BigQueryInstant>,
    /// The bytes the job processed.
    pub total_bytes_processed: Option<i64>,
    /// For a query, the bytes billed.
    pub total_bytes_billed: Option<i64>,
}

/// The parts `Job` and `ListFormatJob` share, so both convert one way.
struct JobParts {
    reference: Option<v2::JobReference>,
    configuration: Option<v2::JobConfiguration>,
    statistics: Option<v2::JobStatistics>,
    status: Option<v2::JobStatus>,
    user_email: String,
}

impl TryFrom<JobParts> for BigQueryJob {
    type Error = BigQueryError;

    fn try_from(job: JobParts) -> Result<Self, Self::Error> {
        let reference = job
            .reference
            .ok_or_else(|| {
                BigQueryError::unexpected_response("BigQuery returned no job_reference")
            })?
            .into();
        let configuration = job.configuration.unwrap_or_default();
        let statistics = job.statistics.unwrap_or_default();
        let query = statistics.query.clone().unwrap_or_default();
        let status = job.status.unwrap_or_default();
        Ok(Self {
            reference,
            job_type: configuration.job_type.non_empty().map(Into::into),
            state: status.state.non_empty().map(Into::into),
            error: status.error_result.map(Into::into),
            user_email: job.user_email.non_empty(),
            labels: configuration.labels.into_iter().collect(),
            statement_type: query.statement_type.non_empty().map(Into::into),
            creation_time: timestamp_ms("creation_time", statistics.creation_time)?,
            start_time: timestamp_ms("start_time", statistics.start_time)?,
            end_time: timestamp_ms("end_time", statistics.end_time)?,
            total_bytes_processed: statistics.total_bytes_processed,
            total_bytes_billed: query.total_bytes_billed,
        })
    }
}

/// # Errors
/// [`BigQueryError::SystemError`] for a missing job reference, and
/// [`BigQueryError::DeserializeError`] for a time out of range.
impl TryFrom<v2::Job> for BigQueryJob {
    type Error = BigQueryError;

    fn try_from(job: v2::Job) -> Result<Self, Self::Error> {
        JobParts {
            reference: job.job_reference,
            configuration: job.configuration,
            statistics: job.statistics,
            status: job.status,
            user_email: job.user_email,
        }
        .try_into()
    }
}

/// # Errors
/// As for a `Job`.
impl TryFrom<v2::ListFormatJob> for BigQueryJob {
    type Error = BigQueryError;

    fn try_from(job: v2::ListFormatJob) -> Result<Self, Self::Error> {
        JobParts {
            reference: job.job_reference,
            configuration: job.configuration,
            statistics: job.statistics,
            status: job.status,
            user_email: job.user_email,
        }
        .try_into()
    }
}

/// Which jobs [`BigQueryDb::stream_jobs`] lists, newest first.
#[derive(Debug, PartialEq, Clone, Builder)]
pub struct BigQueryListJobsParams {
    /// The project whose jobs are listed. Unset, the client's
    /// [`google_project_id`](crate::BigQueryDbOptions::google_project_id).
    pub project_id: Option<String>,
    /// Every user's jobs rather than only the caller's, which needs the Owner role on the
    /// project.
    #[default = "false"]
    pub all_users: bool,
    /// Only jobs created at or after this time.
    pub min_creation_time: Option<BigQueryInstant>,
    /// Only jobs created at or before this time.
    pub max_creation_time: Option<BigQueryInstant>,
    /// Only jobs in one of these states; empty is every state. [`Other`](BigQueryJobState::Other) cannot be sent.
    #[default = "Vec::new()"]
    pub states: Vec<BigQueryJobState>,
    /// Only the child jobs of this script job.
    pub parent_job_id: Option<BigQueryJobId>,
    /// How many jobs one `ListJobs` call returns; unset is BigQuery's default.
    pub page_size: Option<u32>,
}

impl BigQueryDb {
    /// Reads `job`.
    ///
    /// # Errors
    /// [`DataNotFoundError`](BigQueryError::DataNotFoundError) for a job that does not exist,
    /// or that is looked up in the wrong `location`.
    pub async fn get_job(&self, job: &BigQueryJobRef) -> BigQueryResult<BigQueryJob> {
        let request = v2::GetJobRequest {
            project_id: job.project_id.clone(),
            job_id: job.job_id.to_string(),
            location: job.location_field(),
        };
        self.retry(&job.admin_span(), "get a job", &request, |r| {
            let mut client = self.job_client();
            async move { client.get_job(r).await }
        })
        .await?
        .try_into()
    }

    /// Deletes the metadata of a finished `job`, such as a failed query's SQL that should not
    /// stay in the job history. It does not cancel a running job: see
    /// [`cancel_job`](Self::cancel_job).
    ///
    /// # Errors
    /// What BigQuery returns for a job that is still running, and
    /// [`DataNotFoundError`](BigQueryError::DataNotFoundError) for a missing one.
    pub async fn delete_job(&self, job: &BigQueryJobRef) -> BigQueryResult<()> {
        let request = v2::DeleteJobRequest {
            project_id: job.project_id.clone(),
            job_id: job.job_id.to_string(),
            location: job.location_field(),
        };
        self.retry(&job.admin_span(), "delete a job", &request, |r| {
            let mut client = self.job_client();
            async move { client.delete_job(r).await }
        })
        .await
    }

    /// Streams the jobs `params` selects, newest first, fetching each page as the stream
    /// reaches it. A failed page is logged and ends the stream; see
    /// [`stream_jobs_with_errors`](Self::stream_jobs_with_errors) to see it.
    ///
    /// # Errors
    /// [`InvalidParametersError`](BigQueryError::InvalidParametersError) for a creation time
    /// before 1970 or an [`Other`](BigQueryJobState::Other) state, before any request.
    pub async fn stream_jobs<'b>(
        &self,
        params: BigQueryListJobsParams,
    ) -> BigQueryResult<BoxStream<'b, BigQueryJob>> {
        Ok(logging_errors(
            self.stream_jobs_with_errors(params).await?,
            "jobs",
        ))
    }

    /// Like [`stream_jobs`](Self::stream_jobs), with a failed page as the stream's last item.
    ///
    /// # Errors
    /// As for [`stream_jobs`](Self::stream_jobs).
    pub async fn stream_jobs_with_errors<'b>(
        &self,
        params: BigQueryListJobsParams,
    ) -> BigQueryResult<BoxStream<'b, BigQueryResult<BigQueryJob>>> {
        let template = params.into_request(&self.options().google_project_id)?;
        let db = self.clone();
        let span = tracing::debug_span!(
            "BigQuery jobs",
            "/bigquery/project" = template.project_id.as_str()
        );
        Ok(paged(move |page_token| {
            let db = db.clone();
            let span = span.clone();
            let request = v2::ListJobsRequest {
                page_token,
                ..template.clone()
            };
            async move {
                let page = db
                    .retry(&span, "list jobs", &request, |r| {
                        let mut client = db.job_client();
                        async move { client.list_jobs(r).await }
                    })
                    .await?;
                let jobs = page
                    .jobs
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<BigQueryResult<_>>()?;
                Ok((jobs, page.next_page_token))
            }
        }))
    }
}

impl BigQueryJobRef {
    /// The span of one job admin call.
    fn admin_span(&self) -> Span {
        tracing::debug_span!("BigQuery job", "/bigquery/job_id" = self.job_id.as_str())
    }
}

/// Milliseconds since the epoch, as `ListJobs` takes its creation-time bounds.
fn creation_bound(field: &str, at: BigQueryInstant) -> BigQueryResult<u64> {
    u64::try_from(at.as_millisecond())
        .map_err(|_| BigQueryError::invalid_parameters(field, format!("{at} is before 1970")))
}

impl BigQueryListJobsParams {
    /// The first page's request, with `default_project_id` for an unset project.
    fn into_request(self, default_project_id: &str) -> BigQueryResult<v2::ListJobsRequest> {
        use v2::list_jobs_request::{Projection, StateFilter};
        let state_filter = self
            .states
            .into_iter()
            .map(|state| match state {
                BigQueryJobState::Pending => Ok(StateFilter::Pending.into()),
                BigQueryJobState::Running => Ok(StateFilter::Running.into()),
                BigQueryJobState::Done => Ok(StateFilter::Done.into()),
                BigQueryJobState::Other(name) => Err(BigQueryError::invalid_parameters(
                    "states",
                    format!("{name:?} is not a state ListJobs filters on"),
                )),
            })
            .collect::<BigQueryResult<_>>()?;
        Ok(v2::ListJobsRequest {
            project_id: self
                .project_id
                .unwrap_or_else(|| default_project_id.to_string()),
            all_users: self.all_users,
            max_results: self.page_size.map(|n| i32::try_from(n).unwrap_or(i32::MAX)),
            min_creation_time: self
                .min_creation_time
                .map(|at| creation_bound("min_creation_time", at))
                .transpose()?
                .unwrap_or(0),
            max_creation_time: self
                .max_creation_time
                .map(|at| creation_bound("max_creation_time", at))
                .transpose()?,
            page_token: String::new(),
            projection: Projection::Full.into(),
            state_filter,
            parent_job_id: self
                .parent_job_id
                .map(|id| id.to_string())
                .unwrap_or_default(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_types_map_known_names_and_keep_unknown_ones() {
        for name in ["QUERY", "LOAD", "EXTRACT", "COPY", "UNKNOWN"] {
            let parsed = BigQueryJobType::from(name);
            assert!(
                !matches!(parsed, BigQueryJobType::Other(_)),
                "{name} reads as {parsed:?}"
            );
            assert_eq!(parsed.as_str(), name);
        }
        let unknown = BigQueryJobType::from("SNAPSHOT".to_string());
        assert_eq!(unknown, BigQueryJobType::Other("SNAPSHOT".into()));
        assert_eq!(unknown.to_string(), "SNAPSHOT");
    }

    #[test]
    fn job_states_map_known_names_and_keep_unknown_ones() {
        for name in ["PENDING", "RUNNING", "DONE"] {
            let parsed = BigQueryJobState::from(name);
            assert!(
                !matches!(parsed, BigQueryJobState::Other(_)),
                "{name} reads as {parsed:?}"
            );
            assert_eq!(parsed.as_str(), name);
        }
        let unknown = BigQueryJobState::from("PAUSED".to_string());
        assert_eq!(unknown, BigQueryJobState::Other("PAUSED".into()));
        assert_eq!(unknown.to_string(), "PAUSED");
    }
}
