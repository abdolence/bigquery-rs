//! The tables the live write tests create in the CI dataset, and the queries that read them
//! back.

use crate::common::*;
use bigquery::errors::BigQueryError;
use bigquery::*;
use gcloud_sdk::google::cloud::bigquery::v2 as bq;
use gcloud_sdk::prost_types::value::Kind;

/// This run's table `name` with `columns`, with `primary_key` as a
/// `PRIMARY KEY ... NOT ENFORCED` if set.
pub async fn create_table(
    s: &Scratch,
    name: &str,
    columns: Vec<BigQueryFieldSchema>,
    primary_key: Option<&str>,
) -> TestResult<BigQueryTableRef> {
    let schema = BigQueryTableSchema { fields: columns };
    s.db.table_client()
        .insert_table(bq::InsertTableRequest {
            project_id: s.project.clone(),
            dataset_id: s.dataset.to_string(),
            table: Some(bq::Table {
                table_reference: Some(bq::TableReference {
                    project_id: s.project.clone(),
                    dataset_id: s.dataset.to_string(),
                    table_id: s.table_id(name).to_string(),
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
    Ok(s.dataset_ref()?.table(s.table_id(name)))
}

/// The rows of `sql` as text cells, `None` for NULL, and the bytes BigQuery billed.
pub async fn query_rows(s: &Scratch, sql: &str) -> TestResult<(Vec<Vec<Option<String>>>, i64)> {
    let response =
        s.db.job_client()
            .query(bq::PostQueryRequest {
                project_id: s.project.clone(),
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
