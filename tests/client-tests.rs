use bigquery::errors::BigQueryError;
use bigquery::*;
use gcloud_sdk::google::cloud::bigquery::storage::v1 as storage;
use gcloud_sdk::google::cloud::bigquery::v2 as bq;

#[path = "support/common.rs"]
mod common;
use common::*;

const TABLE_ID: &str = "two_columns";

#[tokio::test]
async fn both_channels_serve_calls() -> TestResult {
    with_scratch("both_channels_serve_calls", async |s: &Scratch| {
        let account =
            s.db.project_client()
                .get_service_account(bq::GetServiceAccountRequest {
                    project_id: s.project.clone(),
                })
                .await
                .map_err(BigQueryError::from)?
                .into_inner();
        assert!(
            account.email.ends_with(".gserviceaccount.com"),
            "unexpected service account: {}",
            account.email
        );

        let schema = write_stream_schema(&s.db, &s.project, s.dataset.as_str()).await?;
        let names: Vec<&str> = schema.fields.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names, ["id", "name"]);
        Ok(())
    })
    .await
}

/// Creates a two-column table in `dataset_id` and reads its schema back through the Storage
/// Write API's `GetWriteStream` on the `_default` stream.
async fn write_stream_schema(
    db: &BigQueryDb,
    project: &str,
    dataset_id: &str,
) -> TestResult<storage::TableSchema> {
    let field = |name: &str, r#type: &str| bq::TableFieldSchema {
        name: name.to_string(),
        r#type: r#type.to_string(),
        mode: "NULLABLE".to_string(),
        ..Default::default()
    };
    db.table_client()
        .insert_table(bq::InsertTableRequest {
            project_id: project.to_string(),
            dataset_id: dataset_id.to_string(),
            table: Some(bq::Table {
                table_reference: Some(bq::TableReference {
                    project_id: project.to_string(),
                    dataset_id: dataset_id.to_string(),
                    table_id: TABLE_ID.to_string(),
                }),
                schema: Some(bq::TableSchema {
                    fields: vec![field("id", "INT64"), field("name", "STRING")],
                    ..Default::default()
                }),
                ..Default::default()
            }),
        })
        .await
        .map_err(BigQueryError::from)?;

    let stream = db
        .write_client()
        .get_write_stream(storage::GetWriteStreamRequest {
            name: format!(
                "projects/{project}/datasets/{dataset_id}/tables/{TABLE_ID}/streams/_default"
            ),
            view: storage::WriteStreamView::Full.into(),
        })
        .await
        .map_err(BigQueryError::from)?
        .into_inner();

    stream
        .table_schema
        .ok_or_else(|| "GetWriteStream(FULL) returned no table_schema".into())
}
