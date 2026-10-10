//! The in-process route through gcloud-sdk's generated clients; the `bigquery` client reaches
//! it once it takes a channel.

use super::support::{client, serve, PROJECT};
use bigquery::{BigQueryDatasetId, BigQueryLocation};
use gcloud_sdk::google::cloud::bigquery::v2;
use gcloud_sdk::google::cloud::bigquery::v2::dataset_service_client::DatasetServiceClient;

const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
const EU: BigQueryLocation = BigQueryLocation::from_static("EU");

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_in_process_channel_serves_what_the_tcp_route_wrote() {
    let served = serve().await;
    let db = client(served.addr).await;
    let created = db
        .fluent()
        .schema()
        .dataset(SHOP)
        .create()
        .location(EU)
        .description("Shop data")
        .execute()
        .await
        .expect("the dataset is created over TCP");

    let dataset = DatasetServiceClient::new(served.engine.channel())
        .get_dataset(v2::GetDatasetRequest {
            project_id: PROJECT.to_string(),
            dataset_id: SHOP.to_string(),
            ..Default::default()
        })
        .await
        .expect("the in-process channel reads the dataset")
        .into_inner();
    assert_eq!(dataset.location, "EU");
    assert_eq!(dataset.description.as_deref(), Some("Shop data"));
    assert_eq!(
        dataset.creation_time,
        created
            .creation_time
            .expect("the dataset has a creation time")
            .as_millisecond()
    );
}
