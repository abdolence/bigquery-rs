//! Runs DDL and DML statements and reads what each one did, then estimates a query with a dry
//! run, which bills nothing, before running it.
//!
//! Run with `PROJECT_ID=<your-project> cargo run --example dml-and-dry-run`.

use bigquery::*;
use serde::Deserialize;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub fn config_env_var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|error| format!("{name}: {error}"))
}

const INVENTORY: BigQueryTableId = BigQueryTableId::from_static("inventory");

#[derive(Debug, Deserialize)]
struct InventoryItem {
    sku: String,
    product: String,
    quantity: i64,
}

/// A dataset name unique to this run, so that concurrent runs never share one.
fn scratch_dataset_id() -> Result<BigQueryDatasetId, Box<dyn std::error::Error + Send + Sync>> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
    Ok(BigQueryDatasetId::new(format!(
        "bigquery_example_dml_and_dry_run_{}_{}",
        now.as_secs(),
        now.subsec_nanos()
    ))?)
}

/// Prints the kind of statement and the rows a DML statement changed.
fn print_outcome(action: &str, outcome: &BigQueryQueryOutcome) {
    let statement = outcome
        .statement_type
        .as_ref()
        .map_or("an unreported statement".to_string(), |statement| {
            format!("{statement:?}")
        });
    match outcome.dml_stats {
        Some(changes) => println!(
            "{action}: {statement}, {} inserted, {} updated, {} deleted",
            changes.inserted, changes.updated, changes.deleted
        ),
        None => println!("{action}: {statement}"),
    }
}

/// A byte count BigQuery may leave unreported, as text.
fn byte_count_text(bytes: Option<i64>) -> String {
    bytes.map_or("unreported".to_string(), |bytes| bytes.to_string())
}

async fn change_and_estimate(
    db: &BigQueryDb,
    dataset: &BigQueryDatasetId,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let created = db
        .fluent()
        .query(format!(
            "CREATE TABLE `{INVENTORY}` (sku STRING NOT NULL, product STRING, quantity INT64)"
        ))
        .default_dataset(dataset.clone())
        .execute()
        .await?;
    print_outcome("Created the table", &created);

    let inserted = db
        .fluent()
        .query(format!(
            "INSERT `{INVENTORY}` (sku, product, quantity) \
             SELECT FORMAT('SKU-%03d', number), product, number * 4 \
             FROM UNNEST(['Lingonberry jam', 'Crispbread', 'Cloudberry jam', 'Pickled herring', \
                          'Cinnamon buns', 'Kalles kaviar', 'Västerbotten cheese', 'Coffee']) \
                  AS product WITH OFFSET AS number"
        ))
        .default_dataset(dataset.clone())
        .execute()
        .await?;
    print_outcome("Stocked the shelves", &inserted);

    let restocked = db
        .fluent()
        .query(format!(
            "UPDATE `{INVENTORY}` SET quantity = quantity + @delivered WHERE quantity < @threshold"
        ))
        .default_dataset(dataset.clone())
        .param("delivered", 20)
        .param("threshold", 10)
        .execute()
        .await?;
    print_outcome("Restocked the low items", &restocked);

    let discontinued = db
        .fluent()
        .query(format!(
            "DELETE `{INVENTORY}` WHERE product IN UNNEST(@discontinued)"
        ))
        .default_dataset(dataset.clone())
        .param("discontinued", ["Pickled herring", "Kalles kaviar"])
        .execute()
        .await?;
    print_outcome("Removed the discontinued products", &discontinued);

    let stock_query =
        format!("SELECT sku, product, quantity FROM `{INVENTORY}` ORDER BY quantity DESC");
    let estimate = db
        .fluent()
        .query(stock_query.clone())
        .default_dataset(dataset.clone())
        .dry_run()
        .await?;
    let columns: Vec<String> = estimate
        .schema
        .iter()
        .flat_map(|schema| schema.fields.iter())
        .map(|field| format!("{} {}", field.name, field.field_type))
        .collect();
    println!(
        "The dry run expects {} bytes processed, with the columns {}",
        byte_count_text(estimate.total_bytes_processed),
        columns.join(", ")
    );

    let (stock, stats): (Vec<InventoryItem>, _) = db
        .fluent()
        .query(stock_query)
        .default_dataset(dataset.clone())
        .obj()
        .query_with_stats()
        .await?;
    println!(
        "The query processed {} bytes and billed {} bytes. The stock:",
        byte_count_text(stats.total_bytes_processed),
        byte_count_text(stats.total_bytes_billed),
    );
    for item in &stock {
        println!("  {} {}: {}", item.sku, item.product, item.quantity);
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

    // The expiration removes the tables even if this process dies before its cleanup.
    let dataset = scratch_dataset_id()?;
    db.fluent()
        .schema()
        .dataset(dataset.clone())
        .create()
        .description("Scratch dataset of the dml-and-dry-run example")
        .default_table_expiration(Duration::from_secs(3600))
        .execute()
        .await?;
    println!("Created the scratch dataset {dataset}");

    let outcome = change_and_estimate(&db, &dataset).await;

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
    Ok(deleted?)
}
