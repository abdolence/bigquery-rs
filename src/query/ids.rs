//! The identifiers of a job, of a job-less query and of a `Query` call.

use crate::errors::BigQueryError;
use crate::BigQueryResult;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

/// A BigQuery job ID.
///
/// Checked only for what would break the job's resource path
/// (`projects/{p}/jobs/{job_id}`): it must not be empty, and must not hold a `/` or a control
/// character. BigQuery's own rule (letters, digits, `_` and `-`, at most 1,024 characters) is
/// BigQuery's to enforce. A job ID BigQuery returned is kept as it came.
///
/// ```rust
/// use bigquery::BigQueryJobId;
///
/// let job: BigQueryJobId = "bquxjob_1a2b3c".parse()?;
/// assert_eq!(job.to_string(), "bquxjob_1a2b3c");
/// assert!(BigQueryJobId::new("a/b").is_err());
/// # Ok::<(), bigquery::errors::BigQueryError>(())
/// ```
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct BigQueryJobId(String);

impl BigQueryJobId {
    /// Checks `id` and wraps it.
    ///
    /// # Errors
    /// [`BigQueryError::InvalidParametersError`] for the field `job_id` if it is empty or holds
    /// a `/` or a control character.
    pub fn new(id: impl Into<String>) -> BigQueryResult<Self> {
        let id = id.into();
        if id.is_empty() {
            return Err(BigQueryError::invalid_parameters(
                "job_id",
                "must not be empty",
            ));
        }
        if let Some(ch) = id.chars().find(|c| *c == '/' || c.is_control()) {
            return Err(BigQueryError::invalid_parameters(
                "job_id",
                format!(
                    "must not contain / or control characters; got {ch:?} in \"{}\"",
                    id.escape_debug()
                ),
            ));
        }
        Ok(Self(id))
    }

    /// A job ID BigQuery returned. BigQuery made it, so it is not checked again.
    pub(crate) fn reported(id: String) -> Self {
        Self(id)
    }

    /// The ID.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for BigQueryJobId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<&str> for BigQueryJobId {
    type Error = BigQueryError;

    fn try_from(id: &str) -> Result<Self, Self::Error> {
        Self::new(id)
    }
}

impl TryFrom<String> for BigQueryJobId {
    type Error = BigQueryError;

    fn try_from(id: String) -> Result<Self, Self::Error> {
        Self::new(id)
    }
}

impl FromStr for BigQueryJobId {
    type Err = BigQueryError;

    fn from_str(id: &str) -> Result<Self, Self::Err> {
        Self::new(id)
    }
}

/// The ID BigQuery reports for a query it answered, with or without a job.
///
/// A query that ran without a job is listed in the `INFORMATION_SCHEMA.JOBS` views with this
/// ID as its `job_id`, labels included, but the job calls such as `GetJob` refuse it, since it
/// is not a valid job ID. When BigQuery did create a job, it reported the job's ID here.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct BigQueryQueryId(String);

impl BigQueryQueryId {
    /// A query ID BigQuery returned.
    pub(crate) fn reported(id: String) -> Self {
        Self(id)
    }

    /// The ID.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for BigQueryQueryId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// The idempotency key of a `Query` call: a retry that repeats it does not run the statement
/// twice, within the window BigQuery keeps keys for.
///
/// Checked only for being non-empty, since an empty key means none. BigQuery recommends a
/// UUID; what it accepts is BigQuery's to say.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct BigQueryRequestId(String);

impl BigQueryRequestId {
    /// Checks `id` and wraps it.
    ///
    /// # Errors
    /// [`BigQueryError::InvalidParametersError`] for the field `request_id` if it is empty.
    pub fn new(id: impl Into<String>) -> BigQueryResult<Self> {
        let id = id.into();
        if id.is_empty() {
            return Err(BigQueryError::invalid_parameters(
                "request_id",
                "must not be empty",
            ));
        }
        Ok(Self(id))
    }

    /// The key.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for BigQueryRequestId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<&str> for BigQueryRequestId {
    type Error = BigQueryError;

    fn try_from(id: &str) -> Result<Self, Self::Error> {
        Self::new(id)
    }
}

impl TryFrom<String> for BigQueryRequestId {
    type Error = BigQueryError;

    fn try_from(id: String) -> Result<Self, Self::Error> {
        Self::new(id)
    }
}

impl FromStr for BigQueryRequestId {
    type Err = BigQueryError;

    fn from_str(id: &str) -> Result<Self, Self::Err> {
        Self::new(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_job_id_that_would_break_its_resource_path_is_refused() {
        for bad in ["", "a/b", "a\nb"] {
            match BigQueryJobId::new(bad) {
                Err(BigQueryError::InvalidParametersError(err)) => {
                    assert_eq!(err.public.field, "job_id", "{bad:?}")
                }
                other => panic!("{bad:?}: expected an invalid job ID, got {other:?}"),
            }
        }
        assert!(BigQueryJobId::new("bquxjob_1-a").is_ok());
    }

    #[test]
    fn an_empty_request_id_is_refused() {
        match BigQueryRequestId::new("") {
            Err(BigQueryError::InvalidParametersError(err)) => {
                assert_eq!(err.public.field, "request_id")
            }
            other => panic!("expected an invalid request ID, got {other:?}"),
        }
        assert_eq!(
            BigQueryRequestId::new("req-1").expect("a key").as_str(),
            "req-1"
        );
    }
}
