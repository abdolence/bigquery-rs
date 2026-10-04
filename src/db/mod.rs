mod options;
pub use options::*;

mod retry;
pub(crate) use retry::retry_delay;

mod ids;
pub use ids::*;

mod labels;
pub use labels::*;

mod location;
pub use location::*;

mod endpoint;
pub use endpoint::*;

mod table_ref;
pub use table_ref::*;

mod support;
pub(crate) use support::*;

#[cfg(test)]
pub(crate) mod fake;

use crate::errors::BigQueryError;
use crate::BigQueryResult;
use gcloud_sdk::google::cloud::bigquery::storage::v1::big_query_read_client::BigQueryReadClient;
use gcloud_sdk::google::cloud::bigquery::storage::v1::big_query_write_client::BigQueryWriteClient;
use gcloud_sdk::google::cloud::bigquery::v2::dataset_service_client::DatasetServiceClient;
use gcloud_sdk::google::cloud::bigquery::v2::job_service_client::JobServiceClient;
use gcloud_sdk::google::cloud::bigquery::v2::model_service_client::ModelServiceClient;
use gcloud_sdk::google::cloud::bigquery::v2::project_service_client::ProjectServiceClient;
use gcloud_sdk::google::cloud::bigquery::v2::routine_service_client::RoutineServiceClient;
use gcloud_sdk::google::cloud::bigquery::v2::row_access_policy_service_client::RowAccessPolicyServiceClient;
use gcloud_sdk::google::cloud::bigquery::v2::table_service_client::TableServiceClient;
use gcloud_sdk::{
    GoogleApi, GoogleApiClient, GoogleAuthMiddleware, TokenSourceType, GCP_DEFAULT_SCOPES,
};
use std::fmt::Formatter;
use std::sync::Arc;
use tracing::info;

/// The largest response the v2 clients accept. Inline query results, Arrow or rows, are the
/// large ones; tonic's 4 MiB default is too small for them.
const V2_MAX_DECODING_MESSAGE_SIZE: usize = 128 * 1024 * 1024;

/// The largest response the Storage Read client accepts. One `ReadRowsResponse` carries up to
/// 128 MB of serialized rows, plus the message framing around them.
const STORAGE_READ_MAX_DECODING_MESSAGE_SIZE: usize = 256 * 1024 * 1024;

struct BigQueryDbInner {
    options: BigQueryDbOptions,
    v2: GoogleApi<JobServiceClient<GoogleAuthMiddleware>>,
    storage: GoogleApi<BigQueryReadClient<GoogleAuthMiddleware>>,
}

/// The main entry point for working with Google BigQuery.
///
/// It holds two authenticated gRPC channels: one to the v2 API (datasets, tables, jobs and
/// queries) and one to the Storage Read and Write APIs. Clones are cheap and share both
/// channels, so create one instance and clone it wherever it is needed.
#[derive(Clone)]
pub struct BigQueryDb {
    inner: Arc<BigQueryDbInner>,
}

impl BigQueryDb {
    /// Creates a client for `google_project_id` with default [`BigQueryDbOptions`] and the
    /// application default credentials.
    ///
    /// # Example
    /// ```rust,no_run
    /// use bigquery::*;
    ///
    /// # async fn run() -> BigQueryResult<()> {
    /// let db = BigQueryDb::new("my-gcp-project-id").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn new<S>(google_project_id: S) -> BigQueryResult<Self>
    where
        S: AsRef<str>,
    {
        Self::with_options(BigQueryDbOptions::new(
            google_project_id.as_ref().to_string(),
        ))
        .await
    }

    /// Creates a client with the given options and the application default credentials.
    pub async fn with_options(options: BigQueryDbOptions) -> BigQueryResult<Self> {
        Self::with_options_token_source(
            options,
            GCP_DEFAULT_SCOPES.clone(),
            TokenSourceType::Default,
        )
        .await
    }

    /// Creates a client for the project detected from the environment, see
    /// [`BigQueryDbOptions::for_default_project_id`].
    ///
    /// # Errors
    /// [`BigQueryError::InvalidParametersError`] if no project can be detected.
    pub async fn for_default_project_id() -> BigQueryResult<Self> {
        match BigQueryDbOptions::for_default_project_id().await {
            Some(options) => Self::with_options(options).await,
            None => Err(BigQueryError::invalid_parameters(
                "google_project_id",
                "Unable to detect google_project_id from GCP_PROJECT, PROJECT_ID, GCP_PROJECT_ID, \
                 the local credentials or the metadata server",
            )),
        }
    }

    /// Creates a client with the given options, authenticating with the service account key
    /// file at `service_account_key_path`.
    pub async fn with_options_service_account_key_file(
        options: BigQueryDbOptions,
        service_account_key_path: std::path::PathBuf,
    ) -> BigQueryResult<Self> {
        Self::with_options_token_source(
            options,
            GCP_DEFAULT_SCOPES.clone(),
            TokenSourceType::File(service_account_key_path),
        )
        .await
    }

    /// Creates a client with full control over the OAuth2 `token_scopes` and where the token
    /// comes from.
    ///
    /// Both channels share one token, so the token source is asked for a new one only when it
    /// expires, never once per channel.
    ///
    /// # Errors
    /// [`BigQueryError::InvalidParametersError`] for the field `google_project_id` if it is
    /// empty, before any credentials are read.
    pub async fn with_options_token_source(
        options: BigQueryDbOptions,
        token_scopes: Vec<String>,
        token_source_type: TokenSourceType,
    ) -> BigQueryResult<Self> {
        if options.google_project_id.is_empty() {
            return Err(BigQueryError::invalid_parameters(
                "google_project_id",
                "must not be empty",
            ));
        }
        let api_url = options.effective_bigquery_api_url().to_string();
        let storage_api_url = options.effective_bigquery_storage_api_url().to_string();

        info!(
            google_project_id = options.google_project_id,
            api_url,
            storage_api_url,
            token_scopes = token_scopes.join(", "),
            "Creating a new BigQuery client.",
        );

        let v2 = GoogleApiClient::from_function_with_token_source(
            |channel| {
                JobServiceClient::new(channel)
                    .max_decoding_message_size(V2_MAX_DECODING_MESSAGE_SIZE)
            },
            api_url,
            None,
            token_scopes,
            token_source_type,
        )
        .await?;

        let storage = v2
            .connect_with_endpoint(
                |channel| {
                    BigQueryReadClient::new(channel)
                        .max_decoding_message_size(STORAGE_READ_MAX_DECODING_MESSAGE_SIZE)
                },
                storage_api_url,
                None,
            )
            .await?;

        Ok(Self {
            inner: Arc::new(BigQueryDbInner {
                options,
                v2,
                storage,
            }),
        })
    }

    /// The options this client was created with.
    pub fn options(&self) -> &BigQueryDbOptions {
        &self.inner.options
    }

    /// The raw gRPC client of the v2 `JobService`, on the shared v2 channel.
    ///
    /// The raw clients are for calls this crate has no API for. Their types come from
    /// `gcloud-sdk`, which this crate does not re-export, so they can change in a patch release.
    pub fn job_client(&self) -> JobServiceClient<GoogleAuthMiddleware> {
        self.inner.v2.get()
    }

    /// The raw gRPC client of the v2 `DatasetService`, see [`BigQueryDb::job_client`].
    pub fn dataset_client(&self) -> DatasetServiceClient<GoogleAuthMiddleware> {
        self.inner.v2.get_with(|channel| {
            DatasetServiceClient::new(channel)
                .max_decoding_message_size(V2_MAX_DECODING_MESSAGE_SIZE)
        })
    }

    /// The raw gRPC client of the v2 `TableService`, see [`BigQueryDb::job_client`].
    pub fn table_client(&self) -> TableServiceClient<GoogleAuthMiddleware> {
        self.inner.v2.get_with(|channel| {
            TableServiceClient::new(channel).max_decoding_message_size(V2_MAX_DECODING_MESSAGE_SIZE)
        })
    }

    /// The raw gRPC client of the v2 `ModelService`, see [`BigQueryDb::job_client`].
    pub fn model_client(&self) -> ModelServiceClient<GoogleAuthMiddleware> {
        self.inner.v2.get_with(|channel| {
            ModelServiceClient::new(channel).max_decoding_message_size(V2_MAX_DECODING_MESSAGE_SIZE)
        })
    }

    /// The raw gRPC client of the v2 `RoutineService`, see [`BigQueryDb::job_client`].
    pub fn routine_client(&self) -> RoutineServiceClient<GoogleAuthMiddleware> {
        self.inner.v2.get_with(|channel| {
            RoutineServiceClient::new(channel)
                .max_decoding_message_size(V2_MAX_DECODING_MESSAGE_SIZE)
        })
    }

    /// The raw gRPC client of the v2 `RowAccessPolicyService`, see [`BigQueryDb::job_client`].
    pub fn row_access_policy_client(&self) -> RowAccessPolicyServiceClient<GoogleAuthMiddleware> {
        self.inner.v2.get_with(|channel| {
            RowAccessPolicyServiceClient::new(channel)
                .max_decoding_message_size(V2_MAX_DECODING_MESSAGE_SIZE)
        })
    }

    /// The raw gRPC client of the v2 `ProjectService`, see [`BigQueryDb::job_client`].
    pub fn project_client(&self) -> ProjectServiceClient<GoogleAuthMiddleware> {
        self.inner.v2.get_with(|channel| {
            ProjectServiceClient::new(channel)
                .max_decoding_message_size(V2_MAX_DECODING_MESSAGE_SIZE)
        })
    }

    /// The raw gRPC client of the Storage Read API, on the shared storage channel, see
    /// [`BigQueryDb::job_client`].
    pub fn read_client(&self) -> BigQueryReadClient<GoogleAuthMiddleware> {
        self.inner.storage.get()
    }

    /// The raw gRPC client of the Storage Write API, on the shared storage channel, see
    /// [`BigQueryDb::job_client`].
    pub fn write_client(&self) -> BigQueryWriteClient<GoogleAuthMiddleware> {
        self.inner.storage.get_with(BigQueryWriteClient::new)
    }
}

impl std::fmt::Debug for BigQueryDb {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BigQueryDb")
            .field("options", &self.inner.options)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::BigQueryInvalidParametersError;

    #[tokio::test]
    async fn an_empty_project_id_fails_client_construction() {
        let result = BigQueryDb::with_options(BigQueryDbOptions::new(String::new())).await;
        match result {
            Err(BigQueryError::InvalidParametersError(BigQueryInvalidParametersError {
                public,
            })) => assert_eq!(public.field, "google_project_id"),
            Err(other) => panic!("expected an invalid-parameters error, got {other:?}"),
            Ok(_) => panic!("expected an invalid-parameters error, got a client"),
        }
    }
}
