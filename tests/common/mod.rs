use bigquery::*;

pub type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// The project the integration tests run against, from `GCP_PROJECT`. `None` when it is unset,
/// so a plain `cargo test` without credentials skips the live tests instead of failing them.
pub fn test_project() -> Option<String> {
    std::env::var("GCP_PROJECT").ok()
}

/// Creates a client for `project` with debug logging for this crate and gcloud-sdk.
pub async fn setup(project: &str) -> TestResult<BigQueryDb> {
    let filter =
        tracing_subscriber::EnvFilter::builder().parse("info,bigquery=debug,gcloud_sdk=debug")?;
    // Several tests in one binary each call this; only the first can install the subscriber.
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();

    Ok(BigQueryDb::new(project).await?)
}

/// A dataset name unique to this run, `<prefix>_<unix seconds>_<nanos>`, so that concurrent runs
/// never share one and a leftover is easy to attribute.
pub fn scratch_dataset_id(prefix: &str) -> TestResult<String> {
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?;
    Ok(format!("{prefix}_{}_{}", now.as_secs(), now.subsec_nanos()))
}
