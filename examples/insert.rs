//! Inserts rows three ways: a plain batch through the default stream, an exactly-once insert
//! through a committed stream, and an atomic insert through a pending stream. The rows are then
//! read back with a table scan.
//!
//! Run with `PROJECT_ID=<your-project> cargo run --example insert`.

use bigquery::*;
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub fn config_env_var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|error| format!("{name}: {error}"))
}

const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Order {
    order_id: i64,
    customer: String,
    quantity: i64,
    placed_at: BigQueryTimestamp,
}

impl Order {
    fn numbered(order_id: i64) -> Self {
        Order {
            order_id,
            customer: format!("customer-{}", order_id % 4),
            quantity: order_id * 2,
            placed_at: BigQueryTimestamp(BigQueryInstant::now()),
        }
    }
}

/// A dataset name unique to this run, so that concurrent runs never share one.
fn scratch_dataset_id() -> Result<BigQueryDatasetId, Box<dyn std::error::Error + Send + Sync>> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
    Ok(BigQueryDatasetId::new(format!(
        "bigquery_example_insert_{}_{}",
        now.as_secs(),
        now.subsec_nanos()
    ))?)
}

async fn insert_and_read_back(
    db: &BigQueryDb,
    dataset: &BigQueryDatasetId,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    db.fluent()
        .schema()
        .table(dataset.table(ORDERS))
        .columns(|columns| {
            columns.fields([
                columns.field(path!(Order::order_id)).int64().required(),
                columns.field(path!(Order::customer)).string(),
                columns.field(path!(Order::quantity)).int64(),
                columns.field(path!(Order::placed_at)).timestamp(),
            ])
        })
        .sync()
        .await?;
    println!("Created the table {}", dataset.table(ORDERS));

    // The default stream: the cheapest way in, with at-least-once delivery, so a retried
    // batch can store its rows twice.
    let batch: Vec<Order> = (1..=10).map(Order::numbered).collect();
    let summary = db
        .fluent()
        .insert()
        .into(dataset.table(ORDERS))
        .objects(&batch)
        .execute()
        .await?;
    println!(
        "Batch insert: {} rows in {} batches ({} bytes sent)",
        summary.rows_written, summary.batches, summary.bytes_sent
    );

    // A committed stream with offsets: a batch resent after a lost acknowledgement is stored
    // once. Small batches here only to show several of them in flight.
    let exactly_once: Vec<Order> = (11..=20).map(Order::numbered).collect();
    let summary = db
        .fluent()
        .insert()
        .into(dataset.table(ORDERS))
        .objects(&exactly_once)
        .options(BigQueryStreamingWriteOptions::new().with_max_batch_rows(4))
        .exactly_once()
        .execute()
        .await?;
    println!(
        "Exactly-once insert: {} rows in {} batches",
        summary.rows_written, summary.batches
    );
    if let Some(stream) = &summary.stream {
        println!("  through the committed stream {stream}");
    }

    // A pending stream: no row is visible until the commit, and then all of them are.
    let atomic: Vec<Order> = (21..=30).map(Order::numbered).collect();
    let summary = db
        .fluent()
        .insert()
        .into(dataset.table(ORDERS))
        .objects(&atomic)
        .atomic()
        .execute()
        .await?;
    println!("Atomic insert: {} rows", summary.rows_written);
    if let Some(commit_time) = summary.commit_time {
        println!("  all of them visible from the commit at {commit_time}");
    }

    let mut stored: Vec<Order> = db
        .fluent()
        .select()
        .from(dataset.table(ORDERS))
        .obj()
        .query()
        .await?;
    stored.sort_by_key(|order| order.order_id);
    println!("The table holds {} orders:", stored.len());
    for order in &stored {
        println!(
            "  #{:>2} {} x{} at {}",
            order.order_id, order.customer, order.quantity, order.placed_at.0
        );
    }
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
    // The table expiration is a backstop for a run killed before it deletes the dataset.
    db.fluent()
        .schema()
        .dataset(dataset.clone())
        .create()
        .location(BigQueryLocation::from_static("US"))
        .default_table_expiration(Duration::from_secs(3600))
        .execute()
        .await?;
    println!("Created the scratch dataset {dataset}");

    let outcome = insert_and_read_back(&db, &dataset).await;

    let deleted = db
        .fluent()
        .schema()
        .dataset(dataset.clone())
        .dangerously_delete_with_contents()
        .await;
    match &deleted {
        Ok(()) => println!("Deleted the scratch dataset {dataset}"),
        Err(error) => eprintln!("Failed to delete the scratch dataset {dataset}: {error}"),
    }
    outcome?;
    deleted?;
    Ok(())
}
