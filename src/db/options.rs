use crate::BigQueryLocation;
use gcloud_sdk::GoogleEnvironment;
use rsb_derive::Builder;
use url::Url;

/// Google's BigQuery v2 API endpoint: datasets, tables, jobs and queries.
pub const BIGQUERY_API_URL: &str = "https://bigquery.googleapis.com";

/// Google's BigQuery Storage Read and Write API endpoint.
pub const BIGQUERY_STORAGE_API_URL: &str = "https://bigquerystorage.googleapis.com";

/// Configuration options for the [`BigQueryDb`](crate::BigQueryDb) client.
///
/// # Examples
///
/// ```rust
/// use bigquery::{BigQueryDbOptions, BigQueryLocation};
///
/// let options = BigQueryDbOptions::new("my-gcp-project-id".to_string())
///     .with_location(BigQueryLocation::from_static("EU"))
///     .with_bigquery_api_url("https://eu-bigquery.googleapis.com".parse()?)
///     .with_max_retries(5);
///
/// assert_eq!(options.effective_bigquery_api_url().host_str(), Some("eu-bigquery.googleapis.com"));
/// # Ok::<(), bigquery::url::ParseError>(())
/// ```
#[derive(Debug, Eq, PartialEq, Clone, Builder)]
pub struct BigQueryDbOptions {
    /// The Google Cloud project that runs the jobs and owns the datasets by default.
    pub google_project_id: String,

    /// The location (`US`, `EU`, `europe-west1`, ...) to send with jobs and queries.
    ///
    /// Leave it `None` unless you need it: BigQuery finds the location of a dataset or a job by
    /// itself, and a wrong location fails with `NotFound` instead of being ignored.
    pub location: Option<BigQueryLocation>,

    /// The maximum number of times a failed retryable request is sent again. Defaults to `3`.
    #[default = "3"]
    pub max_retries: usize,

    /// Overrides [`BIGQUERY_API_URL`].
    ///
    /// Client construction refuses a scheme other than `http` or `https`, or no host.
    pub bigquery_api_url: Option<Url>,

    /// Overrides [`BIGQUERY_STORAGE_API_URL`], under the same rules as `bigquery_api_url`.
    ///
    /// There is no emulator support: the BigQuery emulators speak only the REST API, while this
    /// crate uses gRPC throughout.
    pub bigquery_storage_api_url: Option<Url>,
}

impl BigQueryDbOptions {
    /// Builds options for the project detected from the environment: the `GCP_PROJECT`,
    /// `PROJECT_ID` or `GCP_PROJECT_ID` variables, the quota project of the local credentials,
    /// or the GCE metadata server. Returns `None` if none of them names a project.
    pub async fn for_default_project_id() -> Option<BigQueryDbOptions> {
        GoogleEnvironment::detect_google_project_id()
            .await
            .map(BigQueryDbOptions::new)
    }

    /// The v2 API endpoint these options connect to.
    pub fn effective_bigquery_api_url(&self) -> Url {
        self.bigquery_api_url.clone().unwrap_or_else(|| {
            Url::parse(BIGQUERY_API_URL).expect("BIGQUERY_API_URL is an absolute https URL")
        })
    }

    /// The Storage API endpoint these options connect to.
    pub fn effective_bigquery_storage_api_url(&self) -> Url {
        self.bigquery_storage_api_url.clone().unwrap_or_else(|| {
            Url::parse(BIGQUERY_STORAGE_API_URL)
                .expect("BIGQUERY_STORAGE_API_URL is an absolute https URL")
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_endpoints_are_the_two_google_hosts() {
        let options = BigQueryDbOptions::new("p".to_string());
        assert_eq!(
            options.effective_bigquery_api_url().host_str(),
            Some("bigquery.googleapis.com")
        );
        assert_eq!(
            options.effective_bigquery_storage_api_url().host_str(),
            Some("bigquerystorage.googleapis.com")
        );
    }
}
