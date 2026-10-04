use gcloud_sdk::GoogleEnvironment;
use rsb_derive::Builder;

/// The endpoint of the BigQuery v2 API: datasets, tables, jobs and queries.
pub const BIGQUERY_API_URL: &str = "https://bigquery.googleapis.com";

/// The endpoint of the BigQuery Storage Read and Write APIs.
pub const BIGQUERY_STORAGE_API_URL: &str = "https://bigquerystorage.googleapis.com";

/// Configuration options for the [`BigQueryDb`](crate::BigQueryDb) client.
///
/// # Examples
///
/// ```rust
/// use bigquery::BigQueryDbOptions;
///
/// let options = BigQueryDbOptions::new("my-gcp-project-id".to_string())
///     .with_location("EU".to_string())
///     .with_max_retries(5);
///
/// assert_eq!(options.location.as_deref(), Some("EU"));
/// ```
#[derive(Debug, Eq, PartialEq, Clone, Builder)]
pub struct BigQueryDbOptions {
    /// The Google Cloud project that runs the jobs and owns the datasets by default.
    pub google_project_id: String,

    /// The location (`US`, `EU`, `europe-west1`, ...) to send with jobs and queries.
    ///
    /// Leave it `None` unless you need it: BigQuery finds the location of a dataset or a job by
    /// itself, and a wrong location fails with `NotFound` instead of being ignored.
    pub location: Option<String>,

    /// The maximum number of times a failed retryable request is sent again. Defaults to `3`.
    #[default = "3"]
    pub max_retries: usize,

    /// Overrides [`BIGQUERY_API_URL`].
    pub bigquery_api_url: Option<String>,

    /// Overrides [`BIGQUERY_STORAGE_API_URL`].
    ///
    /// There is no emulator support: the BigQuery emulators speak only the REST API, while this
    /// crate uses gRPC throughout.
    pub bigquery_storage_api_url: Option<String>,
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
    pub fn effective_bigquery_api_url(&self) -> &str {
        self.bigquery_api_url.as_deref().unwrap_or(BIGQUERY_API_URL)
    }

    /// The Storage API endpoint these options connect to.
    pub fn effective_bigquery_storage_api_url(&self) -> &str {
        self.bigquery_storage_api_url
            .as_deref()
            .unwrap_or(BIGQUERY_STORAGE_API_URL)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_leave_location_unset_and_retry_three_times() {
        let options = BigQueryDbOptions::new("p".to_string());
        assert_eq!(options.google_project_id, "p");
        assert_eq!(options.location, None);
        assert_eq!(options.max_retries, 3);
        assert_eq!(options.bigquery_api_url, None);
        assert_eq!(options.bigquery_storage_api_url, None);
    }

    #[test]
    fn default_endpoints_are_the_two_google_hosts() {
        let options = BigQueryDbOptions::new("p".to_string());
        assert_eq!(
            options.effective_bigquery_api_url(),
            "https://bigquery.googleapis.com"
        );
        assert_eq!(
            options.effective_bigquery_storage_api_url(),
            "https://bigquerystorage.googleapis.com"
        );
    }

    #[test]
    fn endpoint_overrides_are_used() {
        let options = BigQueryDbOptions::new("p".to_string())
            .with_bigquery_api_url("http://localhost:1".to_string())
            .with_bigquery_storage_api_url("http://localhost:2".to_string());
        assert_eq!(options.effective_bigquery_api_url(), "http://localhost:1");
        assert_eq!(
            options.effective_bigquery_storage_api_url(),
            "http://localhost:2"
        );
    }

    #[test]
    fn builder_sets_location_and_retries() {
        let options = BigQueryDbOptions::new("p".to_string())
            .with_location("EU".to_string())
            .with_max_retries(0);
        assert_eq!(options.location.as_deref(), Some("EU"));
        assert_eq!(options.max_retries, 0);
    }
}
