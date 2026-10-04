use bigquery::errors::BigQueryError;
use bigquery::*;
use futures::FutureExt;
use gcloud_sdk::google::cloud::bigquery::v2 as bq;
use std::panic::AssertUnwindSafe;

pub type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// The label key a live test may put on its jobs, valued with its scratch dataset's name, so
/// that a job naming no table in the dataset still counts towards the bytes billed.
pub const RUN_LABEL: &str = "bq_test_run";

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
pub fn scratch_dataset_id(prefix: &str) -> TestResult<BigQueryDatasetId> {
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?;
    Ok(BigQueryDatasetId::new(format!(
        "{prefix}_{}_{}",
        now.as_secs(),
        now.subsec_nanos()
    ))?)
}

/// A dataset in the US made for one live test and named after its test binary.
#[allow(
    dead_code,
    reason = "every test binary compiles this module and uses part of it"
)]
pub struct Scratch {
    pub db: BigQueryDb,
    pub project: String,
    pub dataset: BigQueryDatasetId,
}

#[allow(
    dead_code,
    reason = "every test binary compiles this module and uses part of it"
)]
impl Scratch {
    /// The scratch dataset with its project set.
    pub fn dataset_ref(&self) -> BigQueryResult<BigQueryDatasetRef> {
        BigQueryDatasetRef::new(&self.project, self.dataset.clone())
    }

    /// How many of this run's jobs since `since_ms` there were and the bytes they billed, from
    /// `ListJobs`, which bills nothing. A job is this run's when its query names the dataset,
    /// runs with it as the default dataset, or carries it as its [`RUN_LABEL`].
    async fn bytes_billed(&self, since_ms: u64) -> TestResult<(usize, i64)> {
        let (mut jobs, mut billed) = (0, 0);
        let mut page_token = String::new();
        loop {
            let page = self
                .db
                .job_client()
                .list_jobs(bq::ListJobsRequest {
                    project_id: self.project.clone(),
                    min_creation_time: since_ms,
                    projection: bq::list_jobs_request::Projection::Full.into(),
                    page_token: page_token.clone(),
                    ..Default::default()
                })
                .await
                .map_err(BigQueryError::from)?
                .into_inner();
            for job in page.jobs {
                let dataset = self.dataset.as_str();
                let ours = job.configuration.as_ref().is_some_and(|c| {
                    c.labels.get(RUN_LABEL).map(String::as_str) == Some(dataset)
                        || c.query.as_ref().is_some_and(|q| {
                            q.query.contains(dataset)
                                || q.default_dataset
                                    .as_ref()
                                    .is_some_and(|d| d.dataset_id == dataset)
                        })
                });
                if ours {
                    jobs += 1;
                    billed += job
                        .statistics
                        .and_then(|s| s.query)
                        .and_then(|q| q.total_bytes_billed)
                        .unwrap_or(0);
                }
            }
            if page.next_page_token.is_empty() {
                return Ok((jobs, billed));
            }
            page_token = page.next_page_token;
        }
    }
}

/// Runs `body` against a fresh scratch dataset and deletes the dataset with its contents
/// afterwards, also when `body` fails or panics, then prints the bytes the run's jobs billed as
/// `LIVE bytes billed <name>: <n> over <jobs> jobs`. Without `GCP_PROJECT` the test is skipped.
///
/// The dataset's tables expire after two hours, so a run that dies before its cleanup leaves
/// nothing behind for long.
#[allow(
    dead_code,
    reason = "every test binary compiles this module and uses part of it"
)]
pub async fn with_scratch<F>(name: &str, body: F) -> TestResult
where
    F: for<'s> AsyncFnOnce(&'s Scratch) -> TestResult,
{
    let Some(project) = test_project() else {
        eprintln!("GCP_PROJECT is not set, skipping the live test {name}");
        return Ok(());
    };
    let db = setup(&project).await?;
    let dataset = scratch_dataset_id(concat!("bq_", env!("CARGO_CRATE_NAME")))?;
    let started_ms = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?;
    // Created through the v2 API: the crate's dataset create sets no default table expiration.
    db.dataset_client()
        .insert_dataset(bq::InsertDatasetRequest {
            project_id: project.clone(),
            dataset: Some(bq::Dataset {
                dataset_reference: Some(bq::DatasetReference {
                    dataset_id: dataset.to_string(),
                    project_id: project.clone(),
                }),
                location: "US".into(),
                default_table_expiration_ms: Some(2 * 3600 * 1000),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .map_err(BigQueryError::from)?;
    let scratch = Scratch {
        db,
        project,
        dataset,
    };
    let result = AssertUnwindSafe(body(&scratch)).catch_unwind().await;
    match scratch.bytes_billed(started_ms).await {
        Ok((jobs, billed)) => eprintln!("LIVE bytes billed {name}: {billed} over {jobs} jobs"),
        Err(err) => eprintln!("LIVE bytes billed {name}: not reported ({err})"),
    }
    let cleanup = scratch
        .db
        .fluent()
        .schema()
        .dataset(scratch.dataset.clone())
        .dangerously_delete_with_contents()
        .await;
    if let Err(err) = &cleanup {
        eprintln!(
            "failed to delete the scratch dataset {}: {err}",
            scratch.dataset
        );
    }
    match result {
        Ok(result) => result?,
        Err(panic) => std::panic::resume_unwind(panic),
    }
    Ok(cleanup?)
}
