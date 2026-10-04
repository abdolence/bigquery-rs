//! The endpoint a client channel connects to.

use crate::errors::BigQueryError;
use crate::BigQueryResult;
use gcloud_sdk::tonic::codegen::http::Uri;
use std::borrow::Cow;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

/// The URL of a gRPC endpoint: an absolute `http` or `https` URI with a host.
///
/// [`BIGQUERY`](Self::BIGQUERY) and [`BIGQUERY_STORAGE`](Self::BIGQUERY_STORAGE) are Google's
/// two endpoints, and what a client connects to unless
/// [`BigQueryDbOptions`](crate::BigQueryDbOptions) overrides them. Whether anything answers
/// there is found out when the client connects.
///
/// ```rust
/// use bigquery::BigQueryEndpoint;
///
/// let local: BigQueryEndpoint = "http://localhost:9050".parse()?;
/// assert_eq!(local.as_ref(), "http://localhost:9050");
/// assert_eq!(BigQueryEndpoint::BIGQUERY.as_ref(), "https://bigquery.googleapis.com");
/// assert!("localhost:9050".parse::<BigQueryEndpoint>().is_err());
/// # Ok::<(), bigquery::errors::BigQueryError>(())
/// ```
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct BigQueryEndpoint(Cow<'static, str>);

impl BigQueryEndpoint {
    /// The BigQuery v2 API: datasets, tables, jobs and queries.
    pub const BIGQUERY: Self = Self(Cow::Borrowed("https://bigquery.googleapis.com"));

    /// The BigQuery Storage Read and Write APIs.
    pub const BIGQUERY_STORAGE: Self =
        Self(Cow::Borrowed("https://bigquerystorage.googleapis.com"));

    /// Parses `url` and wraps it as given.
    ///
    /// # Errors
    /// [`BigQueryError::InvalidParametersError`] for the field `endpoint` if `url` is not an
    /// absolute `http` or `https` URI with a host.
    pub fn new(url: impl Into<String>) -> BigQueryResult<Self> {
        let url = url.into();
        let invalid = |why: &str| {
            BigQueryError::invalid_parameters(
                "endpoint",
                format!("{why}: \"{}\"", url.escape_debug()),
            )
        };
        let uri: Uri = url
            .parse()
            .map_err(|err| invalid(&format!("is not a URI ({err})")))?;
        if !matches!(uri.scheme_str(), Some("http" | "https")) {
            return Err(invalid("must start with http:// or https://"));
        }
        if uri.host().is_none_or(str::is_empty) {
            return Err(invalid("must name a host"));
        }
        Ok(Self(Cow::Owned(url)))
    }

    /// The URL.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for BigQueryEndpoint {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Display for BigQueryEndpoint {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<&str> for BigQueryEndpoint {
    type Error = BigQueryError;

    fn try_from(url: &str) -> Result<Self, Self::Error> {
        Self::new(url)
    }
}

impl TryFrom<String> for BigQueryEndpoint {
    type Error = BigQueryError;

    fn try_from(url: String) -> Result<Self, Self::Error> {
        Self::new(url)
    }
}

impl FromStr for BigQueryEndpoint {
    type Err = BigQueryError;

    fn from_str(url: &str) -> Result<Self, Self::Err> {
        Self::new(url)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_endpoint_must_be_an_absolute_http_uri_with_a_host() {
        for bad in [
            "",
            "localhost:9050",
            "ftp://example.com",
            "/just/a/path",
            "http://",
        ] {
            let err = BigQueryEndpoint::new(bad).expect_err(bad);
            assert!(err.to_string().contains("endpoint"), "{bad}: {err}");
        }
        for good in [
            "http://127.0.0.1:9050",
            "https://eu-bigquery.googleapis.com",
        ] {
            assert_eq!(BigQueryEndpoint::new(good).expect(good).as_str(), good);
        }
        for google in [
            BigQueryEndpoint::BIGQUERY,
            BigQueryEndpoint::BIGQUERY_STORAGE,
        ] {
            assert!(BigQueryEndpoint::new(google.as_str()).is_ok(), "{google}");
        }
    }
}
