//! Creates, reads, lists and updates a dataset with its location, table expiration and labels,
//! lists the tables in it, and deletes it: a plain delete is refused while the dataset still
//! holds tables, and the delete with its contents removes them too.
//!
//! Run with `PROJECT_ID=<your-project> cargo run --example datasets-admin`.

use bigquery::errors::BigQueryError;
use bigquery::*;
use futures::TryStreamExt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub fn config_env_var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|error| format!("{name}: {error}"))
}

const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");
const CUSTOMERS: BigQueryTableId = BigQueryTableId::from_static("customers");

/// A dataset name unique to this run, so that concurrent runs never share one.
fn scratch_dataset_id() -> Result<BigQueryDatasetId, Box<dyn std::error::Error + Send + Sync>> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
    Ok(BigQueryDatasetId::new(format!(
        "bigquery_example_datasets_admin_{}_{}",
        now.as_secs(),
        now.subsec_nanos()
    ))?)
}

fn print_labels(labels: &BigQueryLabels) {
    for (key, value) in labels.iter() {
        println!("    {key} = {value}");
    }
}

async fn create_table(
    db: &BigQueryDb,
    dataset: &BigQueryDatasetId,
    table: BigQueryTableId,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    db.fluent()
        .schema()
        .table(dataset.table(table))
        .columns(|columns| {
            columns.fields([
                columns.field("id").int64().required(),
                columns.field("name").string(),
            ])
        })
        .sync()
        .await?;
    Ok(())
}

async fn administer_dataset(
    db: &BigQueryDb,
    dataset: &BigQueryDatasetId,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // The expiration removes the tables even if this process dies before its cleanup.
    let created = db
        .fluent()
        .schema()
        .dataset(dataset.clone())
        .create()
        .location(BigQueryLocation::from_static("EU"))
        .description("Scratch dataset of the datasets-admin example")
        .default_table_expiration(Duration::from_secs(3600))
        .labels([("team", "shop"), ("stage", "example")])
        .execute()
        .await?;
    println!("Created {}", created.reference);
    println!(
        "  location {}, default table expiration {}",
        created
            .location
            .as_ref()
            .map_or("unset".to_string(), BigQueryLocation::to_string),
        created
            .default_table_expiration
            .map_or("unset".to_string(), |expiration| format!(
                "{} s",
                expiration.as_secs()
            ))
    );

    let read = db.fluent().schema().dataset(dataset.clone()).get().await?;
    println!(
        "Read it back: description {:?}, created at {}",
        read.description.as_deref().unwrap_or_default(),
        read.creation_time
            .map_or("unknown".to_string(), |created_at| created_at.to_string())
    );
    print_labels(&read.labels);

    let datasets: Vec<BigQueryDatasetSummary> = db
        .fluent()
        .schema()
        .datasets()
        .stream_all_with_errors()
        .await?
        .try_collect()
        .await?;
    let listed = datasets
        .iter()
        .any(|summary| summary.reference.dataset() == dataset);
    println!(
        "The project has {} datasets; this one is listed: {listed}",
        datasets.len()
    );

    let updated = db
        .fluent()
        .schema()
        .dataset(dataset.clone())
        .update()
        .description("Updated by the datasets-admin example")
        .remove_label("stage")
        .label("owner", "examples")
        .execute()
        .await?;
    println!(
        "Updated: description {:?}, labels now",
        updated.description.as_deref().unwrap_or_default()
    );
    print_labels(&updated.labels);

    create_table(db, dataset, ORDERS).await?;
    create_table(db, dataset, CUSTOMERS).await?;
    let tables: Vec<BigQueryTableSummary> = db
        .fluent()
        .schema()
        .dataset(dataset.clone())
        .tables()
        .stream_all_with_errors()
        .await?
        .try_collect()
        .await?;
    println!("Tables in the dataset:");
    for table in &tables {
        println!(
            "    {} ({})",
            table.reference.table(),
            table
                .table_type
                .as_ref()
                .map_or("unknown type".to_string(), BigQueryTableType::to_string)
        );
    }

    // A plain delete refuses a dataset that still holds tables, and deletes nothing.
    match db.fluent().schema().dataset(dataset.clone()).delete().await {
        Err(BigQueryError::DatabaseError(error)) if error.public.code == "InvalidArgument" => {
            println!("Deleting the dataset with its tables in it was refused: {error}")
        }
        Ok(()) => return Err("a dataset holding tables was deleted".into()),
        Err(error) => return Err(error.into()),
    }

    db.fluent()
        .schema()
        .table(dataset.table(ORDERS))
        .delete()
        .await?;
    println!("Deleted the table {ORDERS}");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter("bigquery=info")
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let db = BigQueryDb::new(&config_env_var("PROJECT_ID")?).await?;

    let dataset = scratch_dataset_id()?;
    let outcome = administer_dataset(&db, &dataset).await;

    let deleted = db
        .fluent()
        .schema()
        .dataset(dataset.clone())
        .dangerously_delete_with_contents()
        .await;
    match &deleted {
        Ok(()) => println!("Deleted the dataset {dataset} with the tables left in it"),
        Err(error) => eprintln!("Failed to delete the scratch dataset {dataset}: {error}"),
    }
    outcome?;
    deleted?;

    match db.fluent().schema().dataset(dataset.clone()).get().await {
        Err(BigQueryError::DataNotFoundError(_)) => println!("Reading it now reports not found"),
        other => return Err(format!("expected the dataset to be gone, got {other:?}").into()),
    }
    Ok(())
}
