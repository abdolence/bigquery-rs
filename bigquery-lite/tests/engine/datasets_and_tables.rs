//! Datasets and tables through the `bigquery` client's admin calls and schema sync.

use super::support::{client, serve, PROJECT};
use bigquery::errors::BigQueryError;
use bigquery::*;
use futures::StreamExt;
use gcloud_sdk::google::cloud::bigquery::v2;
use tonic::Code;

const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");
const EU: BigQueryLocation = BigQueryLocation::from_static("EU");

/// `orders` with every column type the engine stores, partitioned by day on a TIMESTAMP and
/// clustered by one column; `revised` adds a column and a label.
fn orders(db: &BigQueryDb, revised: bool) -> BigQueryTableSchemaBuilder<'_> {
    let mut labels = vec![("team", "shop")];
    if revised {
        labels.push(("stage", "test"));
    }
    db.fluent()
        .schema()
        .table(SHOP.table(ORDERS))
        .columns(|columns| {
            let mut fields = columns.fields([
                columns.field("order_id").int64().required(),
                columns.field("customer").string(),
                columns.field("note").string_with_max_length(64),
                columns.field("status").string().default_value("'new'"),
                columns.field("price").float64().description("Unit price"),
                columns.field("total").numeric(),
                columns.field("amount").numeric_with(10, 2),
                columns.field("exact").numeric_with(38, 9),
                columns.field("paid").bool(),
                columns.field("payload").bytes(),
                columns.field("digest").bytes_with_max_length(32),
                columns.field("ship_date").date(),
                columns.field("ship_time").time(),
                columns.field("local_at").datetime(),
                columns.field("placed_at").timestamp().required(),
                columns.field("area").geography(),
                columns.field("attributes").json(),
                columns
                    .field("window")
                    .range(BigQueryRangeElementType::Date),
                columns
                    .field("local_window")
                    .range(BigQueryRangeElementType::DateTime),
                columns
                    .field("active")
                    .range(BigQueryRangeElementType::Timestamp),
                columns.field("shipping").record(|shipping| {
                    shipping.fields([
                        shipping.field("city").string(),
                        shipping.field("zip").string_with_max_length(10),
                        shipping.field("coords").record(|coords| {
                            coords.fields([
                                coords.field("lat").float64(),
                                coords.field("lon").float64(),
                            ])
                        }),
                    ])
                }),
                columns.field("tags").string().repeated(),
                columns.field("lines").repeated().record(|lines| {
                    lines.fields([
                        lines.field("sku").string().required(),
                        lines.field("quantity").int64(),
                    ])
                }),
            ]);
            if revised {
                fields.push(columns.field("coupon").string());
            }
            fields
        })
        .primary_key(["order_id"])
        .partition_by_day("placed_at")
        .cluster_by(["customer"])
        .description("Orders")
        .labels(labels.into_iter().collect::<BigQueryLabels>())
}

fn table_request(table: &str) -> (String, String, String) {
    (PROJECT.to_string(), SHOP.to_string(), table.to_string())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_manages_datasets_and_tables() -> BigQueryResult<()> {
    let served = serve().await;
    let db = client(served.addr).await;

    let dataset = db
        .fluent()
        .schema()
        .dataset(SHOP)
        .create()
        .location(EU)
        .description("Shop data")
        .execute()
        .await?;
    assert_eq!(dataset.location, Some(EU));

    let report = orders(&db, false).sync().await?;
    let created = report.created.expect("the sync created the table");
    let table = db.fluent().schema().table(SHOP.table(ORDERS)).get().await?;
    assert_eq!(table.schema.fields, created.columns);
    assert_eq!(table.location, Some(EU));
    assert_eq!(
        table.partitioning,
        Some(BigQueryPartitioning::Time {
            unit: BigQueryPartitionUnit::Day,
            column: Some("placed_at".to_string()),
        })
    );
    assert_eq!(table.clustering, vec!["customer".to_string()]);
    assert_eq!(table.description.as_deref(), Some("Orders"));
    assert_eq!(table.num_rows, Some(0));
    assert!(orders(&db, false).plan().await?.is_empty());

    let tables: Vec<BigQueryTableSummary> = db
        .fluent()
        .schema()
        .dataset(SHOP)
        .tables()
        .stream_all()
        .await?
        .collect()
        .await;
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].reference.table(), &ORDERS);
    let datasets: Vec<BigQueryDatasetSummary> = db
        .fluent()
        .schema()
        .datasets()
        .stream_all()
        .await?
        .collect()
        .await;
    assert_eq!(datasets.len(), 1);
    assert_eq!(datasets[0].location, Some(EU));

    let (project_id, dataset_id, table_id) = table_request(ORDERS.as_str());
    let read = db
        .table_client()
        .get_table(v2::GetTableRequest {
            project_id: project_id.clone(),
            dataset_id: dataset_id.clone(),
            table_id: table_id.clone(),
            ..Default::default()
        })
        .await
        .map_err(BigQueryError::from)?
        .into_inner();
    let report = orders(&db, true).sync().await?;
    assert!(!report.applied.is_empty());
    let mut stale = tonic::Request::new(v2::UpdateOrPatchTableRequest {
        project_id,
        dataset_id,
        table_id,
        table: Some(v2::Table {
            description: Some("Stale".to_string()),
            ..Default::default()
        }),
        autodetect_schema: false,
    });
    stale.metadata_mut().insert(
        "if-match",
        read.etag.parse().expect("an etag is a header value"),
    );
    let refused = db
        .table_client()
        .patch_table(stale)
        .await
        .expect_err("a patch under a stale etag is refused");
    assert_eq!(refused.code(), Code::FailedPrecondition);

    let patched = db.fluent().schema().table(SHOP.table(ORDERS)).get().await?;
    assert_eq!(patched.labels.get("stage"), Some("test"));
    assert_eq!(patched.description.as_deref(), Some("Orders"));
    assert!(patched.schema.fields.iter().any(|f| f.name == "coupon"));

    db.fluent()
        .schema()
        .table(SHOP.table(ORDERS))
        .delete()
        .await?;
    let gone = db.fluent().schema().table(SHOP.table(ORDERS)).get().await;
    assert!(
        matches!(gone, Err(BigQueryError::DataNotFoundError(_))),
        "{gone:?}"
    );
    db.fluent().schema().dataset(SHOP).delete().await?;
    let gone = db.fluent().schema().dataset(SHOP).get().await;
    assert!(
        matches!(gone, Err(BigQueryError::DataNotFoundError(_))),
        "{gone:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn columns_that_differ_only_by_case_are_refused() -> BigQueryResult<()> {
    let served = serve().await;
    let db = client(served.addr).await;
    db.fluent()
        .schema()
        .dataset(SHOP)
        .create()
        .execute()
        .await?;
    let (project_id, dataset_id, table_id) = table_request("cased");
    let column = |name: &str| v2::TableFieldSchema {
        name: name.to_string(),
        r#type: "INT64".to_string(),
        ..Default::default()
    };
    let refused = db
        .table_client()
        .insert_table(v2::InsertTableRequest {
            project_id: project_id.clone(),
            dataset_id: dataset_id.clone(),
            table: Some(v2::Table {
                table_reference: Some(v2::TableReference {
                    project_id,
                    dataset_id,
                    table_id,
                }),
                schema: Some(v2::TableSchema {
                    fields: vec![column("Id"), column("id")],
                    ..Default::default()
                }),
                ..Default::default()
            }),
        })
        .await
        .expect_err("two columns that differ only by case are refused");
    assert_eq!(refused.code(), Code::InvalidArgument);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unserved_method_is_unimplemented_by_name() {
    let served = serve().await;
    let db = client(served.addr).await;
    let query = db
        .job_client()
        .query(v2::PostQueryRequest {
            project_id: PROJECT.to_string(),
            ..Default::default()
        })
        .await
        .expect_err("queries are not served");
    assert_eq!(query.code(), Code::Unimplemented);
    assert!(query
        .message()
        .contains("google.cloud.bigquery.v2.JobService/Query"));
    let patch = db
        .dataset_client()
        .patch_dataset(v2::UpdateOrPatchDatasetRequest::default())
        .await
        .expect_err("dataset patches are not served");
    assert_eq!(patch.code(), Code::Unimplemented);
    assert!(patch
        .message()
        .contains("google.cloud.bigquery.v2.DatasetService/PatchDataset"));
}
