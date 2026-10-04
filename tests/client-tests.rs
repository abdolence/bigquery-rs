use bigquery::errors::BigQueryError;
use bigquery::*;
use gcloud_sdk::google::cloud::bigquery::storage::v1 as storage;
use gcloud_sdk::google::cloud::bigquery::v2 as bq;

mod common;
use common::*;

const TABLE_ID: &str = "client_probe";

#[tokio::test]
async fn both_channels_serve_calls() -> TestResult {
    let Some(project) = test_project() else {
        eprintln!("GCP_PROJECT is not set, skipping the live client test");
        return Ok(());
    };
    let db = setup(&project).await?;

    let account = db
        .project_client()
        .get_service_account(bq::GetServiceAccountRequest {
            project_id: project.clone(),
        })
        .await
        .map_err(BigQueryError::from)?
        .into_inner();
    assert!(
        account.email.ends_with(".gserviceaccount.com"),
        "unexpected service account: {}",
        account.email
    );

    let dataset_id = scratch_dataset_id("bqp1")?;
    db.dataset_client()
        .insert_dataset(bq::InsertDatasetRequest {
            project_id: project.clone(),
            dataset: Some(bq::Dataset {
                dataset_reference: Some(bq::DatasetReference {
                    dataset_id: dataset_id.to_string(),
                    project_id: project.clone(),
                }),
                default_table_expiration_ms: Some(2 * 3600 * 1000),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .map_err(BigQueryError::from)?;

    let result = write_stream_schema(&db, &project, dataset_id.as_str()).await;

    let cleanup = db
        .dataset_client()
        .delete_dataset(bq::DeleteDatasetRequest {
            project_id: project.clone(),
            dataset_id: dataset_id.to_string(),
            delete_contents: true,
        })
        .await
        .map_err(BigQueryError::from);

    let schema = result?;
    cleanup?;

    let names: Vec<&str> = schema.fields.iter().map(|f| f.name.as_str()).collect();
    assert_eq!(names, ["id", "name"]);
    Ok(())
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
