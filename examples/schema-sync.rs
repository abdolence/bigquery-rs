//! Declares a table's schema in code and syncs it: the first sync creates the table, the next
//! one, inferred from the struct the example reads the table with, adds columns and renames one
//! in place, and `prune_undeclared()` then drops the column the declaration no longer has. Each
//! step is planned first, which writes nothing.
//!
//! Run with `PROJECT_ID=<your-project> cargo run --example schema-sync`.

use bigquery::*;
use serde::Deserialize;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub fn config_env_var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|error| format!("{name}: {error}"))
}

const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");

#[derive(Deserialize)]
struct Order {
    order_id: i64,
    customer_name: String,
    total: Option<String>,
    status: Option<String>,
    placed_at: Option<jiff::Timestamp>,
}

/// A dataset name unique to this run, so that concurrent runs never share one.
fn scratch_dataset_id() -> Result<BigQueryDatasetId, Box<dyn std::error::Error + Send + Sync>> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
    Ok(BigQueryDatasetId::new(format!(
        "bigquery_example_schema_sync_{}_{}",
        now.as_secs(),
        now.subsec_nanos()
    ))?)
}

/// The first version of the table.
fn orders_first_version<'a>(
    db: &'a BigQueryDb,
    dataset: &BigQueryDatasetId,
) -> BigQueryTableSchemaBuilder<'a> {
    db.fluent()
        .schema()
        .table(dataset.table(ORDERS))
        .columns(|columns| {
            columns.fields([
                columns.field("order_id").int64().required(),
                columns.field("customer").string().required(),
                columns.field("total").numeric(),
                columns.field("note").string(),
            ])
        })
        .description("Orders placed in the shop")
}

/// The second version, inferred from `Order`: `customer` is renamed to `customer_name`, `status`
/// and `placed_at` are added, and `note` is no longer declared. `total` holds NUMERIC text in a
/// `String`, which only `.with(..)` can tell from a STRING.
fn orders_second_version<'a>(
    db: &'a BigQueryDb,
    dataset: &BigQueryDatasetId,
) -> BigQueryTableSchemaBuilder<'a> {
    db.fluent()
        .schema()
        .table(dataset.table(ORDERS))
        .columns(|columns| {
            columns
                .from_type::<Order>()
                .with(path!(Order::customer_name), |customer_name| {
                    customer_name.renamed_from("customer")
                })
                .with(path!(Order::total), |total| total.numeric())
                .with(path!(Order::placed_at), |placed_at| {
                    placed_at.default_value("CURRENT_TIMESTAMP()")
                })
        })
        .description("Orders placed in the shop")
}

async fn sync_orders(
    db: &BigQueryDb,
    dataset: &BigQueryDatasetId,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // A plan reads the table and writes nothing; here the table does not exist yet.
    let plan = orders_first_version(db, dataset).plan().await?;
    println!("{plan}");
    let report = orders_first_version(db, dataset).sync().await?;
    println!("{report}");

    let seeded = db
        .fluent()
        .query(format!(
            "INSERT INTO `{ORDERS}` (order_id, customer, total, note) \
             SELECT order_id, CONCAT('customer-', CAST(MOD(order_id, 4) AS STRING)), \
             CAST(order_id * 2.5 AS NUMERIC), 'seeded' \
             FROM UNNEST(GENERATE_ARRAY(1, 20)) AS order_id"
        ))
        .default_dataset(dataset.clone())
        .execute()
        .await?;
    println!(
        "Inserted {} orders\n",
        seeded.num_dml_affected_rows.unwrap_or_default()
    );

    // Without `prune_undeclared()` the undeclared `note` column is kept and reported as
    // withheld; the rename and the added columns are applied in place, keeping every row.
    let plan = orders_second_version(db, dataset).plan().await?;
    println!("{plan}");
    let report = orders_second_version(db, dataset).sync().await?;
    println!("{report}");

    let plan = orders_second_version(db, dataset)
        .prune_undeclared()
        .plan()
        .await?;
    println!("{plan}");
    let report = orders_second_version(db, dataset)
        .prune_undeclared()
        .sync()
        .await?;
    println!("{report}");

    let plan = orders_second_version(db, dataset)
        .prune_undeclared()
        .plan()
        .await?;
    println!("{plan}");

    let orders: Vec<Order> = db
        .fluent()
        .query(format!(
            "SELECT order_id, customer_name, total, status, placed_at \
             FROM `{ORDERS}` ORDER BY order_id LIMIT 5"
        ))
        .default_dataset(dataset.clone())
        .obj::<Order>()
        .query()
        .await?;
    println!("The first orders after the rename and the drop:");
    for order in &orders {
        println!(
            "  order {}: customer {}, total {}, status {}, placed at {}",
            order.order_id,
            order.customer_name,
            order.total.as_deref().unwrap_or("unset"),
            order.status.as_deref().unwrap_or("unset"),
            order
                .placed_at
                .map_or_else(|| "unset".to_string(), |placed_at| placed_at.to_string())
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
    // The expiration removes the tables even if this process dies before its cleanup.
    db.fluent()
        .schema()
        .dataset(dataset.clone())
        .create()
        .default_table_expiration(Duration::from_secs(3600))
        .execute()
        .await?;
    println!("Created the scratch dataset {dataset}\n");

    let outcome = sync_orders(&db, &dataset).await;

    let deleted = db
        .fluent()
        .schema()
        .dataset(dataset.clone())
        .dangerously_delete_with_contents()
        .await;
    match &deleted {
        Ok(()) => println!("\nDeleted the scratch dataset {dataset}"),
        Err(error) => eprintln!("Failed to delete the scratch dataset {dataset}: {error}"),
    }
    outcome?;
    Ok(deleted?)
}
