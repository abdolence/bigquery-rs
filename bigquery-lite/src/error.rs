use crate::ids::{DatasetRef, ProjectId, TableRef};
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
    #[error("{construct} is not supported by bigquery-lite: {detail}")]
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
}

impl Display for Resource {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Resource::Project(project) => write!(f, "Project {project}"),
            Resource::Dataset(dataset) => write!(f, "Dataset {dataset}"),
            Resource::Table(table) => write!(f, "Table {table}"),
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
}

/// The status a request that failed with this error answers: the iceberg kinds map as their
/// names say, a commit that gave up is `Aborted`, which the client retries, and a table that
/// lost a file is `DataLoss`, which it does not.
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
            EngineError::Storage { .. }
            | EngineError::Io(_)
            | EngineError::CommitOutcomeUnknown { .. } => Code::Internal,
            EngineError::MissingMetadata { .. } => Code::DataLoss,
            EngineError::Unsupported { .. } => Code::Unimplemented,
            EngineError::NotFound { .. } => Code::NotFound,
            EngineError::AlreadyExists { .. } => Code::AlreadyExists,
            EngineError::Precondition { .. } => Code::FailedPrecondition,
            EngineError::Aborted { .. } => Code::Aborted,
        };
        Status::new(code, error.to_string())
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
            EngineError::NotFound { .. } => ErrorKind::NamespaceNotFound,
            EngineError::AlreadyExists {
                what: Resource::Table(_),
            } => ErrorKind::TableAlreadyExists,
            EngineError::AlreadyExists { .. } => ErrorKind::NamespaceAlreadyExists,
            EngineError::Precondition { .. } => ErrorKind::PreconditionFailed,
            EngineError::Unsupported { .. } => ErrorKind::FeatureUnsupported,
            EngineError::Invalid { .. } | EngineError::Config { .. } => ErrorKind::DataInvalid,
            EngineError::Aborted { .. } => ErrorKind::CatalogCommitConflicts,
            EngineError::Storage { .. }
            | EngineError::Io(_)
            | EngineError::CommitOutcomeUnknown { .. }
            | EngineError::MissingMetadata { .. } => ErrorKind::Unexpected,
        };
        match error {
            EngineError::Iceberg(inner) => inner,
            other => iceberg::Error::new(kind, other.to_string()).with_source(other),
        }
    }
}
