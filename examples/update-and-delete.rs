//! Updates and deletes rows by primary key with `fluent().update()` and `fluent().delete()`,
//! which BigQuery applies as CDC changes.
//!
//! Run with `PROJECT_ID=<your-project> cargo run --example update-and-delete`.

use bigquery::*;
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub fn config_env_var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|error| format!("{name}: {error}"))
}

const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Order {
    id: i64,
    customer: String,
    status: String,
    note: Option<String>,
}

impl Order {
    fn new(id: i64, customer: &str, status: &str) -> Self {
        Order {
            id,
            customer: customer.to_string(),
            status: status.to_string(),
            note: None,
        }
    }
}

/// A dataset name unique to this run, so that concurrent runs never share one.
fn scratch_dataset_id() -> Result<BigQueryDatasetId, Box<dyn std::error::Error + Send + Sync>> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
    Ok(BigQueryDatasetId::new(format!(
        "bigquery_example_update_and_delete_{}_{}",
        now.as_secs(),
        now.subsec_nanos()
    ))?)
}

/// The table has no `max_staleness`, so a query merges the changes not applied yet and always
/// sees the latest rows.
async fn print_orders(
    db: &BigQueryDb,
    dataset: &BigQueryDatasetId,
    heading: &str,
) -> BigQueryResult<()> {
    let orders: Vec<Order> = db
        .fluent()
        .query(format!(
            "SELECT id, customer, status, note FROM `{}` ORDER BY id",
            dataset.table(ORDERS)
        ))
        .obj()
        .query()
        .await?;
    println!("{heading}:");
    for order in orders {
        println!(
            "  #{} {:<8} {:<8} {}",
            order.id,
            order.customer,
            order.status,
            order.note.as_deref().unwrap_or("-")
        );
    }
    Ok(())
}

async fn update_and_delete(
    db: &BigQueryDb,
    dataset: &BigQueryDatasetId,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Updates and deletes find their row by the primary key.
    db.fluent()
        .schema()
        .table(dataset.table(ORDERS))
        .columns(|columns| {
            columns.fields([
                columns.field(path!(Order::id)).int64().required(),
                columns.field(path!(Order::customer)).string(),
                columns.field(path!(Order::status)).string(),
                columns.field(path!(Order::note)).string(),
            ])
        })
        .primary_key([path!(Order::id)])
        .sync()
        .await?;
    println!("Created the table {} keyed by id", dataset.table(ORDERS));

    // An update of a key that has no row yet inserts it.
    let orders = [
        Order::new(1, "Ada", "placed"),
        Order::new(2, "Grace", "placed"),
        Order::new(3, "Linus", "placed"),
    ];
    db.fluent()
        .update()
        .in_table(dataset.table(ORDERS))
        .objects(&orders)
        .execute()
        .await?;
    print_orders(db, dataset, "After the first updates").await?;

    // An update replaces the whole row, so it carries every column, the unchanged ones too.
    let shipped = Order {
        status: "shipped".to_string(),
        note: Some("left the warehouse".to_string()),
        ..orders[0].clone()
    };
    db.fluent()
        .update()
        .in_table(dataset.table(ORDERS))
        .object(&shipped)
        .execute()
        .await?;

    // A delete needs only the primary key value; the library reads the key's columns from the
    // table.
    db.fluent()
        .delete()
        .from(dataset.table(ORDERS))
        .key(2)
        .execute()
        .await?;
    print_orders(db, dataset, "After shipping #1 and deleting #2").await?;
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

    let outcome = update_and_delete(&db, &dataset).await;

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
