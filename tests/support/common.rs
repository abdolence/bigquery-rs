use bigquery::errors::BigQueryError;
use bigquery::*;
use futures::{FutureExt, StreamExt};
use gcloud_sdk::google::cloud::bigquery::v2 as bq;
use std::panic::AssertUnwindSafe;

pub type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// The label key a live test may put on its jobs, valued with its run's name, so that a job
/// naming none of the run's tables still counts towards the bytes billed.
pub const RUN_LABEL: &str = "bq_test_run";

/// The one dataset every live test works in. The test account can write to no other dataset
/// and create none, so a test must never create, update or delete a dataset; it makes tables
/// named after its run here instead. The dataset expires its tables after an hour.
pub const CI_DATASET: BigQueryDatasetId = BigQueryDatasetId::from_static("bigquery_rs_ci");

/// The location of [`CI_DATASET`], where every job of a live test runs.
#[allow(
    dead_code,
    reason = "every test binary compiles this module and uses part of it"
)]
pub const CI_LOCATION: BigQueryLocation = BigQueryLocation::from_static("US");

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

/// A run name unique to one live test, `<test binary>_<unix seconds>_<nanos>`, so that
/// concurrent runs never share a table and a leftover is easy to attribute. It is also a valid
/// label value, which [`RUN_LABEL`] needs.
fn run_name() -> TestResult<String> {
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?;
    Ok(format!(
        "{}_{}_{}",
        env!("CARGO_CRATE_NAME"),
        now.as_secs(),
        now.subsec_nanos()
    ))
}

/// One live test's share of [`CI_DATASET`]: the tables whose IDs start with its run name.
#[allow(
    dead_code,
    reason = "every test binary compiles this module and uses part of it"
)]
pub struct Scratch {
    pub db: BigQueryDb,
    pub project: String,
    pub dataset: BigQueryDatasetId,
    pub run: String,
}

#[allow(
    dead_code,
    reason = "every test binary compiles this module and uses part of it"
)]
impl Scratch {
    /// The CI dataset with its project set.
    pub fn dataset_ref(&self) -> BigQueryResult<BigQueryDatasetRef> {
        BigQueryDatasetRef::new(&self.project, self.dataset.clone())
    }

    /// The ID of this run's table `name`, `<run>_<name>`. Every table a test makes goes
    /// through here, so that the cleanup finds it and no other run sees it.
    pub fn table_id(&self, name: &str) -> BigQueryTableId {
        BigQueryTableId::new(format!("{}_{name}", self.run))
            .expect("a run name and a table name join into a valid table ID")
    }

    /// This run's table `name` in the CI dataset.
    pub fn table(&self, name: &str) -> BigQueryTableRef {
        self.dataset.table(self.table_id(name))
    }

    /// This run's table `name` as a quoted GoogleSQL path, `` `project.dataset.<run>_<name>` ``.
    pub fn table_sql(&self, name: &str) -> String {
        format!(
            "`{}.{}.{}`",
            self.project,
            self.dataset,
            self.table_id(name)
        )
    }

    /// How many of this run's jobs since `since_ms` there were and the bytes they billed, from
    /// `ListJobs`, which bills nothing. A job is this run's when its query names the run or it
    /// carries the run as its [`RUN_LABEL`].
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
                let run = self.run.as_str();
                let ours = job.configuration.as_ref().is_some_and(|configuration| {
                    configuration.labels.get(RUN_LABEL).map(String::as_str) == Some(run)
                        || configuration
                            .query
                            .as_ref()
                            .is_some_and(|query| query.query.contains(run))
                });
                if ours {
                    jobs += 1;
                    billed += job
                        .statistics
                        .and_then(|statistics| statistics.query)
                        .and_then(|query| query.total_bytes_billed)
                        .unwrap_or(0);
                }
            }
            if page.next_page_token.is_empty() {
                return Ok((jobs, billed));
            }
            page_token = page.next_page_token;
        }
    }

    /// Deletes every table of this run, whether a test made it through the API or with SQL.
    async fn drop_tables(&self) -> TestResult {
        let prefix = format!("{}_", self.run);
        let tables: Vec<BigQueryTableRef> = self
            .db
            .fluent()
            .schema()
            .dataset(self.dataset.clone())
            .tables()
            .stream_all_with_errors()
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .filter(|table| table.reference.table().as_str().starts_with(&prefix))
            .map(|table| table.reference)
            .collect();
        for table in tables {
            match self.db.fluent().schema().table(table).delete().await {
                Ok(()) | Err(BigQueryError::DataNotFoundError(_)) => {}
                Err(err) => return Err(err.into()),
            }
        }
        Ok(())
    }
}

/// Runs `body` with a fresh run name in [`CI_DATASET`] and drops the run's tables afterwards,
/// also when `body` fails or panics, then prints the bytes the run's jobs billed as
/// `LIVE bytes billed <name>: <n> over <jobs> jobs`. Without `GCP_PROJECT` the test is skipped.
///
/// The dataset's tables expire after an hour, so a run that dies before its cleanup leaves
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
    let started_ms = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?;
    let scratch = Scratch {
        db: setup(&project).await?,
        project,
        dataset: CI_DATASET,
        run: run_name()?,
    };
    let result = AssertUnwindSafe(body(&scratch)).catch_unwind().await;
    match scratch.bytes_billed(started_ms).await {
        Ok((jobs, billed)) => eprintln!("LIVE bytes billed {name}: {billed} over {jobs} jobs"),
        Err(err) => eprintln!("LIVE bytes billed {name}: not reported ({err})"),
    }
    let cleanup = scratch.drop_tables().await;
    if let Err(err) = &cleanup {
        eprintln!(
            "failed to drop the tables of the run {}: {err}",
            scratch.run
        );
    }
    match result {
        Ok(result) => result?,
        Err(panic) => std::panic::resume_unwind(panic),
    }
    cleanup
}
