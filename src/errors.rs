//! The crate's error type and the pieces it is built from.
//!
//! Every fallible call in this crate returns [`BigQueryError`]. BigQuery reports failures over
//! gRPC with a status code and a message only, with no `details()` payload, so the
//! classification here is by [`tonic::Code`](gcloud_sdk::tonic::Code), plus the message for the
//! rate-limit errors that BigQuery sends under non-retryable codes.

use crate::{BigQueryJobRef, BigQueryTablePlan, BigQueryTableRef};
use rsb_derive::Builder;
use std::error::Error;
use std::fmt::Display;
use std::fmt::Formatter;

/// The main error type for all BigQuery operations.
#[derive(Debug)]
pub enum BigQueryError {
    /// An error from the client side rather than from BigQuery: authentication, the token source,
    /// channel setup, etc.
    SystemError(BigQuerySystemError),
    /// An error reported by BigQuery that has no more specific variant.
    DatabaseError(BigQueryDatabaseError),
    /// The resource already exists (`ALREADY_EXISTS`), or a schema sync found the table
    /// changed since it read it (`FAILED_PRECONDITION` on its `if-match`).
    DataConflictError(BigQueryDataConflictError),
    /// The resource was not found (`NOT_FOUND`). A dataset or job looked up in the wrong
    /// `location` is reported this way too.
    DataNotFoundError(BigQueryDataNotFoundError),
    /// The caller passed parameters that cannot be sent at all.
    InvalidParametersError(BigQueryInvalidParametersError),
    /// A row or parameter that the crate could not write.
    SerializeError(BigQuerySerializationError),
    /// A value that the crate could not read into the target type.
    DeserializeError(BigQuerySerializationError),
    /// BigQuery rejected rows of one append; nothing of that batch was written.
    RowErrors(BigQueryRowErrors),
    /// The rows or the projection do not match the table's schema.
    SchemaMismatchError(BigQuerySchemaMismatchError),
    /// A write stream in a state that cannot take the call.
    WriteStreamError(BigQueryWriteStreamError),
    /// A job that finished with errors. Boxed, since its details would make every
    /// `BigQueryResult` larger.
    JobError(Box<BigQueryJobError>),
    /// A schema sync refused its plan and wrote nothing. Boxed, since it carries the plan.
    SchemaChangeRefused(Box<BigQuerySchemaChangeRefusedError>),
}

impl BigQueryError {
    /// Builds an [`InvalidParametersError`](BigQueryError::InvalidParametersError) naming the
    /// offending field and why it was rejected.
    pub(crate) fn invalid_parameters(field: impl Into<String>, error: impl Into<String>) -> Self {
        BigQueryError::InvalidParametersError(BigQueryInvalidParametersError::new(
            BigQueryInvalidParametersPublicDetails::new(field.into(), error.into()),
        ))
    }

    /// Whether sending the same request again might succeed.
    pub fn retry_possible(&self) -> bool {
        matches!(self, BigQueryError::DatabaseError(err) if err.retry_possible)
    }
}

impl Display for BigQueryError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match *self {
            BigQueryError::SystemError(ref err) => err.fmt(f),
            BigQueryError::DatabaseError(ref err) => err.fmt(f),
            BigQueryError::DataConflictError(ref err) => err.fmt(f),
            BigQueryError::DataNotFoundError(ref err) => err.fmt(f),
            BigQueryError::InvalidParametersError(ref err) => err.fmt(f),
            BigQueryError::SerializeError(ref err) => write!(f, "Serialize error: {err}"),
            BigQueryError::DeserializeError(ref err) => write!(f, "Deserialize error: {err}"),
            BigQueryError::RowErrors(ref err) => err.fmt(f),
            BigQueryError::SchemaMismatchError(ref err) => err.fmt(f),
            BigQueryError::WriteStreamError(ref err) => err.fmt(f),
            BigQueryError::JobError(ref err) => err.fmt(f),
            BigQueryError::SchemaChangeRefused(ref err) => err.fmt(f),
        }
    }
}

impl Error for BigQueryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match *self {
            BigQueryError::SystemError(ref err) => Some(err),
            BigQueryError::DatabaseError(ref err) => Some(err),
            BigQueryError::DataConflictError(ref err) => Some(err),
            BigQueryError::DataNotFoundError(ref err) => Some(err),
            BigQueryError::InvalidParametersError(ref err) => Some(err),
            BigQueryError::SerializeError(ref err) => Some(err),
            BigQueryError::DeserializeError(ref err) => Some(err),
            BigQueryError::RowErrors(ref err) => Some(err),
            BigQueryError::SchemaMismatchError(ref err) => Some(err),
            BigQueryError::WriteStreamError(ref err) => Some(err),
            BigQueryError::JobError(ref err) => Some(err.as_ref()),
            BigQueryError::SchemaChangeRefused(ref err) => Some(err.as_ref()),
        }
    }
}

/// Details shared by most error kinds.
#[derive(Debug, Eq, PartialEq, Clone, Builder)]
pub struct BigQueryErrorPublicGenericDetails {
    /// The gRPC status code name (`Unavailable`, `PermissionDenied`, ...) or, for failures below
    /// gRPC, a crate-defined code such as `CONNECTION_CLOSED`.
    pub code: String,
}

impl Display for BigQueryErrorPublicGenericDetails {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Error code: {}", self.code)
    }
}

/// An error from the client side rather than from BigQuery.
#[derive(Debug, Eq, PartialEq, Clone, Builder)]
pub struct BigQuerySystemError {
    /// Generic public details about the error.
    pub public: BigQueryErrorPublicGenericDetails,
    /// What failed.
    pub message: String,
}

impl Display for BigQuerySystemError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "BigQuery system/internal error: {}. {}",
            self.public, self.message
        )
    }
}

impl Error for BigQuerySystemError {}

/// An error reported by BigQuery.
#[derive(Debug, Eq, PartialEq, Clone, Builder)]
pub struct BigQueryDatabaseError {
    /// Generic public details about the error.
    pub public: BigQueryErrorPublicGenericDetails,
    /// The status as BigQuery reported it.
    pub details: String,
    /// Whether sending the same request again might succeed.
    pub retry_possible: bool,
}

impl Display for BigQueryDatabaseError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "Database general error occurred: {}. {}. Retry possibility: {}",
            self.public, self.details, self.retry_possible
        )
    }
}

impl Error for BigQueryDatabaseError {}

/// The resource already exists, or changed since it was read.
#[derive(Debug, Eq, PartialEq, Clone, Builder)]
pub struct BigQueryDataConflictError {
    /// Generic public details about the error.
    pub public: BigQueryErrorPublicGenericDetails,
    /// The status as BigQuery reported it.
    pub details: String,
}

impl Display for BigQueryDataConflictError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "Database conflict error occurred: {}. {}",
            self.public, self.details
        )
    }
}

impl Error for BigQueryDataConflictError {}

/// The resource was not found.
#[derive(Debug, Eq, PartialEq, Clone, Builder)]
pub struct BigQueryDataNotFoundError {
    /// Generic public details about the error.
    pub public: BigQueryErrorPublicGenericDetails,
    /// The status as BigQuery reported it, which names the missing resource.
    pub data_detail_message: String,
}

impl Display for BigQueryDataNotFoundError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "Data not found error occurred: {}. {}",
            self.public, self.data_detail_message
        )
    }
}

impl Error for BigQueryDataNotFoundError {}

/// Which parameter was invalid and why.
#[derive(Debug, Eq, PartialEq, Clone, Builder)]
pub struct BigQueryInvalidParametersPublicDetails {
    /// The name of the parameter.
    pub field: String,
    /// Why it was rejected.
    pub error: String,
}

impl Display for BigQueryInvalidParametersPublicDetails {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "Invalid parameters error: {}. {}",
            self.field, self.error
        )
    }
}

/// The caller passed parameters that cannot be sent.
#[derive(Debug, Eq, PartialEq, Clone, Builder)]
pub struct BigQueryInvalidParametersError {
    /// Which parameter was invalid and why.
    pub public: BigQueryInvalidParametersPublicDetails,
}

impl Display for BigQueryInvalidParametersError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", self.public)
    }
}

impl Error for BigQueryInvalidParametersError {}

/// A value the crate's codecs could not convert, in either direction.
#[derive(Debug, Eq, PartialEq, Clone, Builder)]
pub struct BigQuerySerializationError {
    /// Generic public details; the code is the kind's name, such as `TYPE_MISMATCH`.
    pub public: BigQueryErrorPublicGenericDetails,
    /// What went wrong.
    pub kind: BigQueryCodecErrorKind,
    /// Read: the row's offset in its read stream, or its index in an inline result.
    /// Write: the row's index in write order. `None` for a parameter.
    pub row: Option<u64>,
    /// The field path, e.g. `recs[1].v`; empty for the row itself.
    pub path: String,
    /// The failure as the codec or the target type's serde impl reported it.
    pub message: String,
}

impl Display for BigQuerySerializationError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", self.public)?;
        if let Some(row) = self.row {
            write!(f, ", row {row}")?;
        }
        if !self.path.is_empty() {
            write!(f, ", field `{}`", self.path)?;
        }
        write!(f, ": {}", self.message)
    }
}

impl Error for BigQuerySerializationError {}

/// The kinds of [`BigQuerySerializationError`].
#[non_exhaustive]
#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum BigQueryCodecErrorKind {
    /// The Rust form is not one the column's type accepts, in either direction.
    TypeMismatch,
    /// Read: a NULL into a target that is not an `Option`.
    NullForNonOption,
    /// Write: `None` for a REQUIRED field.
    NullForRequired,
    /// Write: `None` inside a REPEATED field.
    NullArrayElement,
    /// The value is outside the BigQuery type or outside the Rust target.
    OutOfRange,
    /// A text form that does not parse, such as a malformed DATE.
    InvalidText,
    /// Write: a field or map key with no column in the table schema.
    UnknownField,
    /// Write: a REQUIRED field the row never wrote.
    MissingRequiredField,
    /// A column type the crate does not handle, such as TIMESTAMP with picosecond precision.
    UnsupportedType,
    /// Write: one encoded row is larger than the request budget.
    RowTooLarge,
    /// An error raised by the target type's own serde impl.
    Custom,
}

impl BigQueryCodecErrorKind {
    /// The kind's name as an error code, such as `TYPE_MISMATCH`.
    pub fn code(self) -> &'static str {
        match self {
            BigQueryCodecErrorKind::TypeMismatch => "TYPE_MISMATCH",
            BigQueryCodecErrorKind::NullForNonOption => "NULL_FOR_NON_OPTION",
            BigQueryCodecErrorKind::NullForRequired => "NULL_FOR_REQUIRED",
            BigQueryCodecErrorKind::NullArrayElement => "NULL_ARRAY_ELEMENT",
            BigQueryCodecErrorKind::OutOfRange => "OUT_OF_RANGE",
            BigQueryCodecErrorKind::InvalidText => "INVALID_TEXT",
            BigQueryCodecErrorKind::UnknownField => "UNKNOWN_FIELD",
            BigQueryCodecErrorKind::MissingRequiredField => "MISSING_REQUIRED_FIELD",
            BigQueryCodecErrorKind::UnsupportedType => "UNSUPPORTED_TYPE",
            BigQueryCodecErrorKind::RowTooLarge => "ROW_TOO_LARGE",
            BigQueryCodecErrorKind::Custom => "CUSTOM",
        }
    }
}

impl Display for BigQueryCodecErrorKind {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.write_str(self.code())
    }
}

/// The rows BigQuery rejected in one append request.
#[derive(Debug, Eq, PartialEq, Clone, Builder)]
pub struct BigQueryRowErrors {
    /// Generic public details; the code is `ROW_ERRORS`.
    pub public: BigQueryErrorPublicGenericDetails,
    /// The batch's index in send order.
    pub batch_index: u64,
    /// The write-order index of the batch's first row.
    pub first_row: u64,
    /// The number of rows in the batch, none of which were written.
    pub row_count: u64,
    /// The rows BigQuery named.
    pub errors: Vec<BigQueryRowError>,
}

impl Display for BigQueryRowErrors {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}: batch {} (rows {} to {}) was rejected with {} row errors",
            self.public,
            self.batch_index,
            self.first_row,
            (self.first_row + self.row_count).saturating_sub(1),
            self.errors.len()
        )?;
        if let Some(first) = self.errors.first() {
            write!(f, ", first: row {}: {}", first.row, first.message)?;
        }
        Ok(())
    }
}

impl Error for BigQueryRowErrors {}

/// One row BigQuery rejected.
#[derive(Debug, Eq, PartialEq, Clone, Builder)]
pub struct BigQueryRowError {
    /// The row's index in write order.
    pub row: u64,
    /// BigQuery's code for the failure.
    pub code: String,
    /// BigQuery's message for the failure.
    pub message: String,
}

/// The rows or the projection do not match the table's schema.
#[derive(Debug, Eq, PartialEq, Clone, Builder)]
pub struct BigQuerySchemaMismatchError {
    /// Generic public details about the error.
    pub public: BigQueryErrorPublicGenericDetails,
    /// The table whose schema did not match.
    pub table: BigQueryTableRef,
    /// What did not match, as BigQuery or the crate reported it.
    pub details: String,
}

impl Display for BigQuerySchemaMismatchError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "Schema mismatch on {}: {}. {}",
            self.table, self.public, self.details
        )
    }
}

impl Error for BigQuerySchemaMismatchError {}

/// A write stream in a state that cannot take the call.
#[derive(Debug, Eq, PartialEq, Clone, Builder)]
pub struct BigQueryWriteStreamError {
    /// Generic public details; the code is the `StorageError` name, such as `STREAM_FINALIZED`.
    pub public: BigQueryErrorPublicGenericDetails,
    /// The write stream's resource name.
    pub stream: String,
    /// The error as BigQuery reported it.
    pub details: String,
}

impl Display for BigQueryWriteStreamError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "Write stream error on {}: {}. {}",
            self.stream, self.public, self.details
        )
    }
}

impl Error for BigQueryWriteStreamError {}

/// A job that finished with errors.
#[derive(Debug, Eq, PartialEq, Clone, Builder)]
pub struct BigQueryJobError {
    /// Generic public details about the error.
    pub public: BigQueryErrorPublicGenericDetails,
    /// The failed job, when BigQuery named one.
    pub job: Option<BigQueryJobRef>,
    /// The job's `error_result` as BigQuery reported it.
    pub details: String,
    /// Every error the job reported.
    pub errors: Vec<BigQueryJobErrorEntry>,
}

impl Display for BigQueryJobError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Job error: {}", self.public)?;
        if let Some(job) = &self.job {
            write!(f, ", job {}:{}", job.project_id, job.job_id)?;
        }
        write!(f, ". {}", self.details)
    }
}

impl Error for BigQueryJobError {}

/// A schema sync's plan has changes that are impossible in place, and the declaration does not
/// opt in to recreating the table, or opts in with `recreate_if_empty()` and the table is not
/// empty. Nothing was written.
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct BigQuerySchemaChangeRefusedError {
    /// The plan, with its `impossible` changes and its `refusal`.
    pub plan: BigQueryTablePlan,
}

impl Display for BigQuerySchemaChangeRefusedError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "Schema sync of {} refused, nothing was written: {} changes are impossible in place",
            self.plan.table,
            self.plan.impossible.len()
        )?;
        if let Some(refusal) = &self.plan.refusal {
            write!(f, " and {refusal}")?;
        }
        Ok(())
    }
}

impl Error for BigQuerySchemaChangeRefusedError {}

/// One error a job reported, as in the v2 `ErrorProto`.
#[derive(Debug, Eq, PartialEq, Clone, Builder)]
pub struct BigQueryJobErrorEntry {
    /// A short code, such as `invalidQuery`.
    pub reason: String,
    /// Where the error occurred, if BigQuery said.
    pub location: String,
    /// A description of the error.
    pub message: String,
}

impl From<gcloud_sdk::error::Error> for BigQueryError {
    fn from(e: gcloud_sdk::error::Error) -> Self {
        BigQueryError::SystemError(BigQuerySystemError::new(
            BigQueryErrorPublicGenericDetails::new(format!("{:?}", e.kind())),
            format!("GCloud system error: {e}"),
        ))
    }
}

impl From<gcloud_sdk::tonic::Status> for BigQueryError {
    fn from(status: gcloud_sdk::tonic::Status) -> Self {
        use gcloud_sdk::tonic::Code;
        match status.code() {
            Code::AlreadyExists => {
                BigQueryError::DataConflictError(BigQueryDataConflictError::new(
                    BigQueryErrorPublicGenericDetails::new(format!("{:?}", status.code())),
                    format!("{status}"),
                ))
            }
            Code::NotFound => BigQueryError::DataNotFoundError(BigQueryDataNotFoundError::new(
                BigQueryErrorPublicGenericDetails::new(format!("{:?}", status.code())),
                format!("{status}"),
            )),
            Code::Aborted | Code::Unavailable | Code::ResourceExhausted | Code::Internal => {
                database_error(&status, format!("{status}"), true)
            }
            Code::PermissionDenied | Code::InvalidArgument if is_rate_limit(&status) => {
                database_error(&status, format!("{status}"), true)
            }
            Code::Unknown => check_hyper_errors(status),
            _ => database_error(&status, format!("{status}"), false),
        }
    }
}

/// Whether `status` is BigQuery's per-table or per-project rate limit. BigQuery reports it as
/// `PermissionDenied: Exceeded rate limits: ...` for `PatchTable` and as
/// `InvalidArgument: Job exceeded rate limits: ...` for DDL, so the code alone says it is
/// permanent while waiting is what makes it pass. Quota errors without "rate limits" are daily
/// or per-request quotas that waiting a few seconds does not clear.
fn is_rate_limit(status: &gcloud_sdk::tonic::Status) -> bool {
    status
        .message()
        .to_ascii_lowercase()
        .contains("exceeded rate limits")
}

/// Classifies an `Unknown` status by the transport error underneath it. A connection that
/// closed, timed out or broke mid-body ("error reading a body from connection") is worth
/// retrying: a stream resumes from its offset and a unary call is sent again. A request hyper
/// refused to send, or a response it could not parse, is not.
fn check_hyper_errors(status: gcloud_sdk::tonic::Status) -> BigQueryError {
    let hyper_error = status
        .source()
        .and_then(|source| source.downcast_ref::<hyper::Error>());
    match hyper_error {
        Some(err) if err.is_closed() => connection_error("CONNECTION_CLOSED", err),
        Some(err) if err.is_timeout() => connection_error("CONNECTION_TIMEOUT", err),
        Some(err) if err.is_user() || err.is_parse() => {
            database_error(&status, format!("Hyper error: {err}"), false)
        }
        Some(err) => connection_error("CONNECTION_LOST", err),
        None if status.message().contains("transport error") => {
            BigQueryError::DatabaseError(BigQueryDatabaseError::new(
                BigQueryErrorPublicGenericDetails::new("CONNECTION_ERROR".into()),
                format!("{status}"),
                true,
            ))
        }
        None => database_error(&status, format!("{status}"), false),
    }
}

fn connection_error(code: &str, err: &hyper::Error) -> BigQueryError {
    BigQueryError::DatabaseError(BigQueryDatabaseError::new(
        BigQueryErrorPublicGenericDetails::new(code.into()),
        format!("Hyper error: {err}"),
        true,
    ))
}

fn database_error(
    status: &gcloud_sdk::tonic::Status,
    details: String,
    retry_possible: bool,
) -> BigQueryError {
    BigQueryError::DatabaseError(BigQueryDatabaseError::new(
        BigQueryErrorPublicGenericDetails::new(format!("{:?}", status.code())),
        details,
        retry_possible,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use gcloud_sdk::tonic::{Code, Status};

    fn classify(code: Code, message: &str) -> BigQueryError {
        BigQueryError::from(Status::new(code, message))
    }

    #[test]
    fn transient_codes_are_retryable() {
        for code in [
            Code::Unavailable,
            Code::ResourceExhausted,
            Code::Aborted,
            Code::Internal,
        ] {
            let err = classify(code, "backend error");
            assert!(err.retry_possible(), "{code:?} must be retryable: {err}");
        }
    }

    #[test]
    fn permanent_codes_are_not_retryable() {
        for code in [
            Code::InvalidArgument,
            Code::PermissionDenied,
            Code::Unauthenticated,
            Code::FailedPrecondition,
            Code::OutOfRange,
            Code::Unimplemented,
        ] {
            let err = classify(
                code,
                "Syntax error: Unexpected identifier \"SELEC\" at [1:1]",
            );
            assert!(
                !err.retry_possible(),
                "{code:?} must not be retryable: {err}"
            );
            assert!(matches!(err, BigQueryError::DatabaseError(_)), "{err:?}");
        }
    }

    #[test]
    fn patch_rate_limit_under_permission_denied_is_retryable() {
        let err = classify(
            Code::PermissionDenied,
            "Exceeded rate limits: too many table update operations for this table. For more \
             information, see https://cloud.google.com/bigquery/docs/troubleshoot-quotas",
        );
        assert!(err.retry_possible(), "{err}");
    }

    #[test]
    fn ddl_rate_limit_under_invalid_argument_is_retryable() {
        let err = classify(
            Code::InvalidArgument,
            "Job exceeded rate limits: Your table exceeded quota for table update operations. \
             For more information, see https://cloud.google.com/bigquery/docs/troubleshoot-quotas",
        );
        assert!(err.retry_possible(), "{err}");
    }

    #[test]
    fn rate_limit_error_keeps_the_reported_code() {
        match classify(Code::PermissionDenied, "Exceeded rate limits: too many") {
            BigQueryError::DatabaseError(err) => assert_eq!(err.public.code, "PermissionDenied"),
            other => panic!("expected a database error, got {other:?}"),
        }
    }

    #[test]
    fn not_found_is_data_not_found() {
        let err = classify(
            Code::NotFound,
            "Not found: Dataset latestbit:ds was not found in location US",
        );
        match err {
            BigQueryError::DataNotFoundError(err) => {
                assert!(err.data_detail_message.contains("latestbit:ds"), "{err}");
            }
            other => panic!("expected DataNotFoundError, got {other:?}"),
        }
    }

    #[test]
    fn already_exists_is_data_conflict() {
        let err = classify(Code::AlreadyExists, "Already Exists: Dataset latestbit:ds");
        assert!(
            matches!(err, BigQueryError::DataConflictError(_)),
            "{err:?}"
        );
        assert!(!err.retry_possible());
    }

    #[test]
    fn unknown_transport_error_is_retryable() {
        let err = classify(Code::Unknown, "transport error");
        assert!(err.retry_possible(), "{err}");
    }

    #[test]
    fn unknown_without_cause_is_not_retryable() {
        let err = classify(Code::Unknown, "something odd");
        assert!(!err.retry_possible(), "{err}");
    }
}
