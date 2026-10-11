use crate::config::ByteSize;
use crate::ids::{
    DatasetRef, JobId, Location, ProjectId, ReadStreamName, RowOffset, TableRef, WriteStreamName,
};
use std::fmt::{Display, Formatter};
use tonic::{Code, Status};

/// Every failure the engine reports, at startup and per request.
///
/// A request's failure reaches the client as the gRPC status of `From<&EngineError> for
/// Status`, whose code is what the client branches on; the message is for people.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum EngineError {
    /// A setting of [`EngineConfig`](crate::EngineConfig) or of a listener is unusable. Only
    /// startup and `serve_*` calls return it.
    #[error("invalid {field}: {reason}")]
    Config {
        /// The setting, as its field is named.
        field: &'static str,
        /// Why it cannot be used, with the value.
        reason: String,
    },
    /// The table format refused an operation.
    #[error(transparent)]
    Iceberg(#[from] iceberg::Error),
    /// The storage under the root failed an operation.
    #[error("{backend} storage failed to {operation}: {detail}")]
    Storage {
        /// The backend of the root.
        backend: BackendKind,
        /// What the engine asked of it.
        operation: &'static str,
        /// The path and the backend's own error.
        detail: String,
    },
    /// A construct the engine does not implement, named as BigQuery names it.
    #[error("{construct} is not supported by smallquery: {detail}")]
    Unsupported {
        /// The construct: an option, a type, a method.
        construct: String,
        /// Where it was found.
        detail: String,
    },
    /// The resource does not exist.
    #[error("Not found: {what}")]
    NotFound {
        /// The missing resource.
        what: Resource,
    },
    /// The resource exists already.
    #[error("Already Exists: {what}")]
    AlreadyExists {
        /// The existing resource.
        what: Resource,
    },
    /// The resource is not in the state the request requires, such as a stale etag.
    #[error("{what}: {detail}")]
    Precondition {
        /// The resource the precondition is about.
        what: Resource,
        /// The precondition that failed.
        detail: String,
    },
    /// A value in a request is invalid.
    #[error("invalid {what}: {detail}")]
    Invalid {
        /// The value, as the request names it.
        what: String,
        /// The rule it breaks.
        detail: String,
    },
    /// A commit lost to concurrent commits on the same table until its retry deadline passed.
    /// Retrying the whole request is safe.
    #[error("{what}: the commit lost to concurrent commits {attempts} times and gave up")]
    Aborted {
        /// The table.
        what: Resource,
        /// The attempts made.
        attempts: u32,
    },
    /// A commit may or may not have landed, and the engine cannot tell which: its predecessor
    /// pointer was pruned while the commit stalled. Retrying it may apply it twice; an operator
    /// reconciles the table from the pointer version and the metadata location.
    #[error(
        "{what}: the outcome of the commit as pointer version {pointer_version} naming \
         {metadata_location} is unknown"
    )]
    CommitOutcomeUnknown {
        /// The table.
        what: Resource,
        /// The pointer version the commit created.
        pointer_version: u64,
        /// The metadata file that pointer names.
        metadata_location: String,
    },
    /// A table's newest catalog pointer names a metadata file that does not exist, and no newer
    /// pointer explains why: the root lost a file the table needs. Retrying does not help; an
    /// operator restores the file or reconciles the table.
    #[error(
        "{what}: catalog pointer version {pointer_version} names {metadata_location}, which \
         does not exist"
    )]
    MissingMetadata {
        /// The table.
        what: Resource,
        /// The version of the pointer.
        pointer_version: u64,
        /// The metadata file the pointer names.
        metadata_location: String,
    },
    /// A local I/O failure outside the storage backend, such as binding a listener.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// A statement that does not parse or plan, or fails while it runs: invalid SQL, an unknown
    /// column, a type mismatch, an arithmetic error.
    #[error("invalid query: {detail}")]
    InvalidQuery {
        /// What is wrong, as the planner or the executor reported it.
        detail: String,
    },
    /// A statement names a dataset that is not in the location its job runs in, which BigQuery
    /// answers as a dataset that does not exist.
    #[error("Not found: Dataset {dataset} was not found in location {location}")]
    NotFoundInLocation {
        /// The dataset the statement names.
        dataset: DatasetRef,
        /// The location of the job.
        location: Location,
    },
    /// A statement needed more memory than the engine's budget.
    #[error("resources exceeded: {detail}")]
    ResourcesExceeded {
        /// What ran out, as the executor reported it.
        detail: String,
    },
    /// A query's estimated scan is above the `maximum_bytes_billed` its request set.
    #[error(
        "the query scans an estimated {} bytes, above the maximum_bytes_billed of {}",
        estimated.bytes(),
        limit.bytes()
    )]
    BytesBilledLimitExceeded {
        /// The bytes the scan is estimated to read.
        estimated: ByteSize,
        /// The limit the request set.
        limit: ByteSize,
    },
    /// A job that failed, with the reason its `error_result` reports.
    #[error("{what} failed: {message}")]
    JobFailed {
        /// The job.
        what: Resource,
        /// Why it failed.
        reason: JobFailureReason,
        /// What went wrong.
        message: String,
    },
    /// A write stream that cannot take the request in the state it is in.
    #[error("{what}: {problem}")]
    WriteStream {
        /// The stream.
        what: Resource,
        /// What about its state refuses the request.
        problem: StreamProblem,
    },
    /// An offset past the end of a stream.
    #[error("{what}: {detail}")]
    OutOfRange {
        /// The stream.
        what: Resource,
        /// The offset and where the stream ends.
        detail: String,
    },
    /// A failure inside the engine that no request can cause or avoid.
    #[error("internal error: {detail}")]
    Internal {
        /// What failed.
        detail: String,
    },
}

/// Why a write stream refuses a request. Each is one of the Storage API's `StorageError`
/// codes, which the client branches on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum StreamProblem {
    /// `OFFSET_ALREADY_EXISTS`: the rows at the offset were appended already.
    OffsetAlreadyExists {
        /// The offset of the request.
        offset: RowOffset,
        /// The offset the stream takes next.
        next: RowOffset,
    },
    /// `OFFSET_OUT_OF_RANGE`: the offset is past the one the stream takes next.
    OffsetOutOfRange {
        /// The offset of the request.
        offset: RowOffset,
        /// The offset the stream takes next.
        next: RowOffset,
    },
    /// `STREAM_FINALIZED`: the stream takes no more rows.
    Finalized,
    /// `STREAM_ALREADY_COMMITTED`: the stream's rows are committed.
    AlreadyCommitted,
    /// `INVALID_STREAM_STATE`: a batch commit names a stream that is not finalized.
    NotFinalized,
    /// `INVALID_STREAM_STATE`: a commit of the stream's rows ended with an unknown outcome, so
    /// nothing more goes through the stream, which could land a row twice.
    OutcomeUnknown,
    /// `INVALID_STREAM_STATE`: what the table says the stream committed is not what this
    /// process knows of it, so another process changed the stream, or its staged requests
    /// overlap.
    ChangedElsewhere,
    /// `INVALID_STREAM_TYPE`: the request applies to other streams only.
    WrongType {
        /// The request.
        request: &'static str,
        /// The streams it applies to.
        applies_to: &'static str,
    },
}

impl Display for StreamProblem {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamProblem::OffsetAlreadyExists { offset, next } => write!(
                f,
                "the rows at offset {offset} were appended already; the next offset is {next}"
            ),
            StreamProblem::OffsetOutOfRange { offset, next } => {
                write!(f, "offset {offset} is past the next offset, {next}")
            }
            StreamProblem::Finalized => f.write_str("the stream is finalized"),
            StreamProblem::AlreadyCommitted => f.write_str("the stream's rows are committed"),
            StreamProblem::NotFinalized => f.write_str("the stream is not finalized"),
            StreamProblem::OutcomeUnknown => {
                f.write_str("a commit of the stream's rows ended with an unknown outcome")
            }
            StreamProblem::ChangedElsewhere => {
                f.write_str("the stream's committed state changed outside this process")
            }
            StreamProblem::WrongType {
                request,
                applies_to,
            } => write!(f, "{request} applies to {applies_to} only"),
        }
    }
}

/// Why a job failed: one of BigQuery's error reasons, which a job's `error_result` reports as
/// its `Display`, with the failures that share a reason told apart where the status a waiting
/// call answers differs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum JobFailureReason {
    /// `invalidQuery`: the statement or a value it produced is invalid.
    InvalidQuery,
    /// `notImplemented`: a construct the engine does not implement.
    NotImplemented,
    /// `notFound`: a resource the statement names does not exist.
    NotFound,
    /// `duplicate`: the destination exists, or holds rows under `WRITE_EMPTY`.
    Duplicate,
    /// `bytesBilledLimitExceeded`: the scan is above `maximum_bytes_billed`.
    BytesBilledLimitExceeded,
    /// `resourcesExceeded`: the statement needed more memory than the engine's budget.
    ResourcesExceeded,
    /// `stopped`: `CancelJob` stopped the job.
    Stopped,
    /// `timeout`: the job ran past its `job_timeout_ms`.
    Timeout,
    /// `internalError` for a commit whose outcome is unknown: its rows may have landed, so the
    /// job must not be run again.
    CommitOutcomeUnknown,
    /// `internalError`: any other failure inside the engine.
    Internal,
}

impl Display for JobFailureReason {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            JobFailureReason::InvalidQuery => "invalidQuery",
            JobFailureReason::NotImplemented => "notImplemented",
            JobFailureReason::NotFound => "notFound",
            JobFailureReason::Duplicate => "duplicate",
            JobFailureReason::BytesBilledLimitExceeded => "bytesBilledLimitExceeded",
            JobFailureReason::ResourcesExceeded => "resourcesExceeded",
            JobFailureReason::Stopped => "stopped",
            JobFailureReason::Timeout => "timeout",
            JobFailureReason::CommitOutcomeUnknown | JobFailureReason::Internal => "internalError",
        })
    }
}

/// The reason a job that failed with this error reports.
impl From<&EngineError> for JobFailureReason {
    fn from(error: &EngineError) -> Self {
        match error {
            EngineError::InvalidQuery { .. } | EngineError::Invalid { .. } => {
                JobFailureReason::InvalidQuery
            }
            EngineError::Unsupported { .. } => JobFailureReason::NotImplemented,
            EngineError::NotFound { .. } | EngineError::NotFoundInLocation { .. } => {
                JobFailureReason::NotFound
            }
            EngineError::AlreadyExists { .. } => JobFailureReason::Duplicate,
            EngineError::BytesBilledLimitExceeded { .. } => {
                JobFailureReason::BytesBilledLimitExceeded
            }
            EngineError::ResourcesExceeded { .. } => JobFailureReason::ResourcesExceeded,
            EngineError::CommitOutcomeUnknown { .. } => JobFailureReason::CommitOutcomeUnknown,
            EngineError::JobFailed { reason, .. } => *reason,
            _ => JobFailureReason::Internal,
        }
    }
}

/// The storage backend a root lives on, chosen by the root URI's scheme.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum BackendKind {
    /// A directory on a local filesystem, `file://`.
    Local,
}

impl Display for BackendKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            BackendKind::Local => f.write_str("local"),
        }
    }
}

/// A resource an error is about, displayed in BigQuery's legacy form, as `Table
/// project:dataset.table`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Resource {
    /// A project.
    Project(ProjectId),
    /// A dataset.
    Dataset(DatasetRef),
    /// A table.
    Table(TableRef),
    /// A job, with the location it runs in when that is known.
    Job {
        /// The project that owns the job.
        project: ProjectId,
        /// The job.
        job: JobId,
        /// The location the job runs in, or the request named.
        location: Option<Location>,
    },
    /// A write stream.
    WriteStream(Box<WriteStreamName>),
    /// A stream of a read session.
    ReadStream(Box<ReadStreamName>),
}

impl Display for Resource {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Resource::Project(project) => write!(f, "Project {project}"),
            Resource::Dataset(dataset) => write!(f, "Dataset {dataset}"),
            Resource::Table(table) => write!(f, "Table {table}"),
            // `project:location.job_id`, the form the client parses out of `AlreadyExists`.
            Resource::Job {
                project,
                job,
                location: Some(location),
            } => write!(f, "Job {project}:{location}.{job}"),
            Resource::Job {
                project,
                job,
                location: None,
            } => write!(f, "Job {project}:{job}"),
            Resource::WriteStream(name) => write!(f, "Write stream {name}"),
            Resource::ReadStream(name) => write!(f, "Read stream {name}"),
        }
    }
}

impl EngineError {
    /// An [`EngineError::Unsupported`] for `construct`.
    pub(crate) fn unsupported(construct: impl Into<String>, detail: impl Into<String>) -> Self {
        EngineError::Unsupported {
            construct: construct.into(),
            detail: detail.into(),
        }
    }

    /// Refuses the first of a request's `fields` on `resource` that is set, as
    /// [`EngineError::Unsupported`] naming it; `fields` pairs each field's name with whether
    /// the request sets it.
    pub(crate) fn refuse_unsupported(
        resource: &str,
        fields: &[(&'static str, bool)],
    ) -> Result<(), Self> {
        match fields.iter().find(|(_, set)| *set) {
            Some((construct, _)) => Err(Self::unsupported(
                *construct,
                format!("set on a {resource}"),
            )),
            None => Ok(()),
        }
    }

    /// An [`EngineError::Invalid`] for `what`.
    pub(crate) fn invalid(what: impl Into<String>, detail: impl Into<String>) -> Self {
        EngineError::Invalid {
            what: what.into(),
            detail: detail.into(),
        }
    }

    /// An [`EngineError::Config`] for `field`.
    pub(crate) fn config(field: &'static str, reason: impl Into<String>) -> Self {
        EngineError::Config {
            field,
            reason: reason.into(),
        }
    }

    /// An [`EngineError::WriteStream`]: `name` refuses the request for `problem`.
    pub(crate) fn stream(name: &WriteStreamName, problem: StreamProblem) -> Self {
        EngineError::WriteStream {
            what: Resource::WriteStream(Box::new(name.clone())),
            problem,
        }
    }

    /// An [`EngineError::NotFound`] for the write stream `name`.
    pub(crate) fn stream_not_found(name: &WriteStreamName) -> Self {
        EngineError::NotFound {
            what: Resource::WriteStream(Box::new(name.clone())),
        }
    }

    /// Whether a commit that failed with this error may have landed all the same: an unknown
    /// outcome, or a commit task that ended without one. The commit loop's other errors come
    /// before any pointer create could reach storage.
    pub(crate) fn commit_may_have_landed(&self) -> bool {
        matches!(
            self,
            EngineError::CommitOutcomeUnknown { .. } | EngineError::Internal { .. }
        )
    }
}

/// The status a request that failed with this error answers: the iceberg kinds map as their
/// names say, a commit that gave up is `Aborted`, which the client retries, and a table that
/// lost a file is `DataLoss`, which it does not. A commit whose outcome is unknown is `Unknown`,
/// which the `bigquery` client does not retry as long as the message does not read as a
/// transport error, so a commit that may have landed is never sent again.
impl From<&EngineError> for Status {
    fn from(error: &EngineError) -> Self {
        let code = match error {
            EngineError::Config { .. } | EngineError::Invalid { .. } => Code::InvalidArgument,
            EngineError::Iceberg(error) => match error.kind() {
                iceberg::ErrorKind::TableNotFound | iceberg::ErrorKind::NamespaceNotFound => {
                    Code::NotFound
                }
                iceberg::ErrorKind::TableAlreadyExists
                | iceberg::ErrorKind::NamespaceAlreadyExists => Code::AlreadyExists,
                iceberg::ErrorKind::CatalogCommitConflicts => Code::Aborted,
                iceberg::ErrorKind::FeatureUnsupported => Code::Unimplemented,
                iceberg::ErrorKind::DataInvalid => Code::InvalidArgument,
                iceberg::ErrorKind::PreconditionFailed => Code::FailedPrecondition,
                _ => Code::Internal,
            },
            EngineError::Storage { .. } | EngineError::Io(_) => Code::Internal,
            EngineError::CommitOutcomeUnknown { .. } => Code::Unknown,
            EngineError::MissingMetadata { .. } => Code::DataLoss,
            EngineError::Unsupported { .. } => Code::Unimplemented,
            EngineError::NotFound { .. } => Code::NotFound,
            EngineError::AlreadyExists { .. } => Code::AlreadyExists,
            EngineError::Precondition { .. } => Code::FailedPrecondition,
            EngineError::Aborted { .. } => Code::Aborted,
            EngineError::InvalidQuery { .. } | EngineError::BytesBilledLimitExceeded { .. } => {
                Code::InvalidArgument
            }
            EngineError::NotFoundInLocation { .. } => Code::NotFound,
            EngineError::ResourcesExceeded { .. } => Code::ResourceExhausted,
            EngineError::Internal { .. } => Code::Internal,
            EngineError::OutOfRange { .. } => Code::OutOfRange,
            EngineError::WriteStream { problem, .. } => match problem {
                StreamProblem::OffsetAlreadyExists { .. } => Code::AlreadyExists,
                StreamProblem::OffsetOutOfRange { .. } => Code::OutOfRange,
                StreamProblem::WrongType { .. } => Code::InvalidArgument,
                StreamProblem::Finalized
                | StreamProblem::AlreadyCommitted
                | StreamProblem::NotFinalized
                | StreamProblem::OutcomeUnknown
                | StreamProblem::ChangedElsewhere => Code::FailedPrecondition,
            },
            EngineError::JobFailed { reason, .. } => match reason {
                JobFailureReason::InvalidQuery | JobFailureReason::BytesBilledLimitExceeded => {
                    Code::InvalidArgument
                }
                JobFailureReason::NotImplemented => Code::Unimplemented,
                JobFailureReason::NotFound => Code::NotFound,
                JobFailureReason::Duplicate => Code::AlreadyExists,
                JobFailureReason::ResourcesExceeded => Code::ResourceExhausted,
                // A stopped job is not sent again.
                JobFailureReason::Stopped | JobFailureReason::Timeout => Code::Cancelled,
                JobFailureReason::CommitOutcomeUnknown => Code::Unknown,
                JobFailureReason::Internal => Code::Internal,
            },
        };
        Status::new(code, error.to_string())
    }
}

/// The engine's reading of a DataFusion failure: an engine error the engine's own code raised
/// inside a plan comes back as itself, a memory limit is `ResourcesExceeded`, a construct
/// DataFusion does not implement is `Unsupported`, and everything a statement can cause, from
/// a parse error to an overflow, is `InvalidQuery`.
impl From<datafusion::error::DataFusionError> for EngineError {
    fn from(error: datafusion::error::DataFusionError) -> Self {
        use datafusion::error::DataFusionError;
        match error {
            DataFusionError::External(inner) => match inner.downcast::<EngineError>() {
                Ok(engine) => *engine,
                Err(inner) => match inner.downcast::<iceberg::Error>() {
                    Ok(iceberg) => EngineError::Iceberg(*iceberg),
                    Err(inner) => EngineError::InvalidQuery {
                        detail: inner.to_string(),
                    },
                },
            },
            DataFusionError::Context(_, inner) | DataFusionError::Diagnostic(_, inner) => {
                EngineError::from(*inner)
            }
            DataFusionError::Shared(inner) => match std::sync::Arc::try_unwrap(inner) {
                Ok(inner) => EngineError::from(inner),
                Err(shared) => EngineError::InvalidQuery {
                    detail: shared.strip_backtrace(),
                },
            },
            DataFusionError::ResourcesExhausted(detail) => {
                EngineError::ResourcesExceeded { detail }
            }
            DataFusionError::NotImplemented(detail) => EngineError::unsupported("SQL", detail),
            DataFusionError::IoError(err) => EngineError::Io(err),
            DataFusionError::Internal(detail) => EngineError::Internal { detail },
            DataFusionError::ExecutionJoin(err) => EngineError::Internal {
                detail: err.to_string(),
            },
            other => EngineError::InvalidQuery {
                detail: other.strip_backtrace(),
            },
        }
    }
}

/// The DataFusion error an engine failure inside a plan raises, which
/// `From<DataFusionError> for EngineError` turns back into this error.
impl From<EngineError> for datafusion::error::DataFusionError {
    fn from(error: EngineError) -> Self {
        datafusion::error::DataFusionError::External(Box::new(error))
    }
}

impl From<EngineError> for Status {
    fn from(error: EngineError) -> Self {
        Status::from(&error)
    }
}

/// The iceberg error a `Catalog` method answers for an engine failure, so that iceberg's own
/// callers see the kinds they branch on.
impl From<EngineError> for iceberg::Error {
    fn from(error: EngineError) -> Self {
        use iceberg::ErrorKind;
        let kind = match &error {
            EngineError::Iceberg(inner) => inner.kind(),
            EngineError::NotFound {
                what: Resource::Table(_),
            } => ErrorKind::TableNotFound,
            EngineError::NotFound { .. } | EngineError::NotFoundInLocation { .. } => {
                ErrorKind::NamespaceNotFound
            }
            EngineError::AlreadyExists {
                what: Resource::Table(_),
            } => ErrorKind::TableAlreadyExists,
            EngineError::AlreadyExists { .. } => ErrorKind::NamespaceAlreadyExists,
            EngineError::Precondition { .. } | EngineError::WriteStream { .. } => {
                ErrorKind::PreconditionFailed
            }
            EngineError::Unsupported { .. } => ErrorKind::FeatureUnsupported,
            EngineError::Invalid { .. }
            | EngineError::Config { .. }
            | EngineError::InvalidQuery { .. }
            | EngineError::BytesBilledLimitExceeded { .. } => ErrorKind::DataInvalid,
            EngineError::Aborted { .. } => ErrorKind::CatalogCommitConflicts,
            EngineError::Storage { .. }
            | EngineError::Io(_)
            | EngineError::CommitOutcomeUnknown { .. }
            | EngineError::MissingMetadata { .. }
            | EngineError::ResourcesExceeded { .. }
            | EngineError::OutOfRange { .. }
            | EngineError::JobFailed { .. }
            | EngineError::Internal { .. } => ErrorKind::Unexpected,
        };
        match error {
            EngineError::Iceberg(inner) => inner,
            other => iceberg::Error::new(kind, other.to_string()).with_source(other),
        }
    }
}
