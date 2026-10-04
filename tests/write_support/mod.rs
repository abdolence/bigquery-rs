//! A scratch dataset for the live write tests: tables created in it, read back by query,
//! and the dataset deleted whatever the test's outcome.

use crate::common::*;
use bigquery::errors::BigQueryError;
use bigquery::*;
use futures::FutureExt;
use gcloud_sdk::google::cloud::bigquery::v2 as bq;
use gcloud_sdk::prost_types::value::Kind;
use std::future::Future;
use std::panic::AssertUnwindSafe;

pub struct Scratch {
    pub db: BigQueryDb,
    pub project: String,
    pub dataset: String,
}

impl Scratch {
    /// A table with `columns` in the scratch dataset, with `primary_key` as a
    /// `PRIMARY KEY ... NOT ENFORCED` if set.
    pub async fn create_table(
        &self,
        table: &str,
        columns: Vec<BigQueryFieldSchema>,
        primary_key: Option<&str>,
    ) -> TestResult<BigQueryTableRef> {
        let schema = BigQueryTableSchema { fields: columns };
        self.db
            .table_client()
            .insert_table(bq::InsertTableRequest {
                project_id: self.project.clone(),
                dataset_id: self.dataset.clone(),
                table: Some(bq::Table {
                    table_reference: Some(bq::TableReference {
                        project_id: self.project.clone(),
                        dataset_id: self.dataset.clone(),
                        table_id: table.to_string(),
                    }),
                    schema: Some(bq::TableSchema::from(&schema)),
                    table_constraints: primary_key.map(|key| bq::TableConstraints {
                        primary_key: Some(bq::PrimaryKey {
                            columns: vec![key.to_string()],
                        }),
                        foreign_keys: Vec::new(),
                    }),
                    ..Default::default()
                }),
            })
            .await
            .map_err(BigQueryError::from)?;
        Ok(BigQueryTableRef::from((
            self.project.as_str(),
            self.dataset.as_str(),
            table,
        )))
    }

    /// The rows of `sql` as text cells, `None` for NULL, and the bytes BigQuery billed.
    pub async fn query(&self, sql: &str) -> TestResult<(Vec<Vec<Option<String>>>, i64)> {
        let response = self
            .db
            .job_client()
            .query(bq::PostQueryRequest {
                project_id: self.project.clone(),
                query_request: Some(bq::QueryRequest {
                    query: sql.to_string(),
                    use_legacy_sql: Some(false),
                    timeout_ms: Some(60_000),
                    ..Default::default()
                }),
            })
            .await
            .map_err(BigQueryError::from)?
            .into_inner();
        assert_eq!(
            response.job_complete,
            Some(true),
            "{sql} did not complete in time"
        );
        let rows = response
            .rows
            .iter()
            .map(|row| {
                let Some(Kind::ListValue(cells)) = row.fields.get("f").and_then(|f| f.kind.clone())
                else {
                    return Vec::new();
                };
                cells
                    .values
                    .into_iter()
                    .map(|cell| match cell.kind {
                        Some(Kind::StructValue(cell)) => {
                            match cell.fields.get("v").and_then(|v| v.kind.clone()) {
                                Some(Kind::StringValue(text)) => Some(text),
                                _ => None,
                            }
                        }
                        _ => None,
                    })
                    .collect()
            })
            .collect();
        Ok((rows, response.total_bytes_billed.unwrap_or_default()))
    }

    pub fn table_sql(&self, table: &str) -> String {
        format!("`{}.{}.{table}`", self.project, self.dataset)
    }
}

/// Runs `body` against a fresh `bqp3_*` dataset in `GCP_PROJECT` and deletes the dataset
/// afterwards, also when `body` fails. Without `GCP_PROJECT` the test is skipped.
pub async fn with_scratch<F, Fut>(name: &str, body: F) -> TestResult
where
    F: FnOnce(Scratch) -> Fut,
    Fut: Future<Output = TestResult>,
{
    let Some(project) = test_project() else {
        eprintln!("GCP_PROJECT is not set, skipping the live test {name}");
        return Ok(());
    };
    let db = setup(&project).await?;
    let dataset = scratch_dataset_id("bqp3")?;
    db.dataset_client()
        .insert_dataset(bq::InsertDatasetRequest {
            project_id: project.clone(),
            dataset: Some(bq::Dataset {
                dataset_reference: Some(bq::DatasetReference {
                    dataset_id: dataset.clone(),
                    project_id: project.clone(),
                }),
                default_table_expiration_ms: Some(2 * 3600 * 1000),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .map_err(BigQueryError::from)?;
    let cleanup_db = db.clone();
    // A failed assertion panics; the dataset is deleted before the panic goes on.
    let result = AssertUnwindSafe(body(Scratch {
        db,
        project: project.clone(),
        dataset: dataset.clone(),
    }))
    .catch_unwind()
    .await;
    let cleanup = cleanup_db
        .dataset_client()
        .delete_dataset(bq::DeleteDatasetRequest {
            project_id: project,
            dataset_id: dataset.clone(),
            delete_contents: true,
        })
        .await;
    if let Err(status) = &cleanup {
        eprintln!("failed to delete the scratch dataset {dataset}: {status}");
    }
    match result {
        Ok(result) => result?,
        Err(panic) => std::panic::resume_unwind(panic),
    }
    cleanup.map_err(BigQueryError::from)?;
    Ok(())
}
