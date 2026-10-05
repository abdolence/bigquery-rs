//! Writes the result of a join and aggregate query into a destination table of its own, reads
//! the rows back typed, shows that a second write into the non-empty table is refused by
//! default, and then appends and overwrites explicitly.
//!
//! Run with `PROJECT_ID=<your-project> cargo run --example query-destination-table`.

use bigquery::errors::BigQueryError;
use bigquery::*;
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub fn config_env_var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|error| format!("{name}: {error}"))
}

const CUSTOMERS: BigQueryTableId = BigQueryTableId::from_static("customers");
const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");
const CITY_TOTALS: BigQueryTableId = BigQueryTableId::from_static("city_totals");

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Customer {
    id: i64,
    name: String,
    city: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Order {
    id: i64,
    customer_id: i64,
    total: f64,
}

#[derive(Debug, Deserialize)]
struct CityTotal {
    city: String,
    orders: i64,
    total: f64,
}

/// A dataset name unique to this run, so that concurrent runs never share one.
fn scratch_dataset_id() -> Result<BigQueryDatasetId, Box<dyn std::error::Error + Send + Sync>> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
    Ok(BigQueryDatasetId::new(format!(
        "bigquery_example_query_destination_{}_{}",
        now.as_secs(),
        now.subsec_nanos()
    ))?)
}

async fn create_tables(
    db: &BigQueryDb,
    dataset: &BigQueryDatasetId,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    db.fluent()
        .schema()
        .table(dataset.table(CUSTOMERS))
        .columns(|columns| {
            columns.fields([
                columns.field(path!(Customer::id)).int64().required(),
                columns.field(path!(Customer::name)).string().required(),
                columns.field(path!(Customer::city)).string().required(),
            ])
        })
        .sync()
        .await?;
    db.fluent()
        .schema()
        .table(dataset.table(ORDERS))
        .columns(|columns| {
            columns.fields([
                columns.field(path!(Order::id)).int64().required(),
                columns.field(path!(Order::customer_id)).int64().required(),
                columns.field(path!(Order::total)).float64().required(),
            ])
        })
        .sync()
        .await?;

    let customers = [
        Customer {
            id: 1,
            name: "Astrid".to_string(),
            city: "Malmö".to_string(),
        },
        Customer {
            id: 2,
            name: "Olle".to_string(),
            city: "Göteborg".to_string(),
        },
        Customer {
            id: 3,
            name: "Maja".to_string(),
            city: "Malmö".to_string(),
        },
    ];
    let orders = [
        Order {
            id: 10,
            customer_id: 1,
            total: 120.0,
        },
        Order {
            id: 11,
            customer_id: 1,
            total: 80.5,
        },
        Order {
            id: 12,
            customer_id: 2,
            total: 64.0,
        },
        Order {
            id: 13,
            customer_id: 3,
            total: 230.0,
        },
    ];
    db.fluent()
        .insert()
        .into(dataset.table(CUSTOMERS))
        .objects(&customers)
        .exactly_once()
        .execute()
        .await?;
    db.fluent()
        .insert()
        .into(dataset.table(ORDERS))
        .objects(&orders)
        .exactly_once()
        .execute()
        .await?;
    println!(
        "Inserted {} customers and {} orders",
        customers.len(),
        orders.len()
    );
    Ok(())
}

/// Prints the rows sorted, since rows read back from a table come in no particular order.
fn print_city_totals(heading: &str, mut city_totals: Vec<CityTotal>) {
    city_totals.sort_by(|left, right| (&left.city, left.orders).cmp(&(&right.city, right.orders)));
    println!("{heading}:");
    for city_total in &city_totals {
        println!(
            "  {}: {} orders, {:.2} in total",
            city_total.city, city_total.orders, city_total.total
        );
    }
}

async fn run_queries(
    db: &BigQueryDb,
    dataset: &BigQueryDatasetId,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    create_tables(db, dataset).await?;

    let city_totals_sql = format!(
        "SELECT c.city, COUNT(*) AS orders, SUM(o.total) AS total \
         FROM `{ORDERS}` AS o JOIN `{CUSTOMERS}` AS c ON c.id = o.customer_id \
         WHERE o.total >= @minimum GROUP BY c.city"
    );

    // The result goes into a table of our own, and the rows are read back from it
    let city_totals: Vec<CityTotal> = db
        .fluent()
        .query(city_totals_sql.clone())
        .default_dataset(dataset.clone())
        .param("minimum", 50.0)
        .destination_table(dataset.table(CITY_TOTALS))
        .obj()
        .query()
        .await?;
    print_city_totals(
        "City totals written into the destination table",
        city_totals,
    );

    // The table holds rows now, so the default refuses to write into it again
    match db
        .fluent()
        .query(city_totals_sql.clone())
        .default_dataset(dataset.clone())
        .param("minimum", 50.0)
        .destination_table(dataset.table(CITY_TOTALS))
        .execute()
        .await
    {
        Err(BigQueryError::DataConflictError(error)) => {
            println!("The second write was refused, as expected: {error}")
        }
        other => return Err(format!("expected the second write to be refused: {other:?}").into()),
    }

    // Appending keeps the earlier rows, and the rows read back are the whole table's
    let appended: Vec<CityTotal> = db
        .fluent()
        .query(city_totals_sql.clone())
        .default_dataset(dataset.clone())
        .param("minimum", 100.0)
        .append_to_destination_table(dataset.table(CITY_TOTALS))
        .obj()
        .query()
        .await?;
    print_city_totals("The table after appending the totals over 100", appended);

    // Overwriting replaces every row the table held
    let overwritten: Vec<CityTotal> = db
        .fluent()
        .query(city_totals_sql)
        .default_dataset(dataset.clone())
        .param("minimum", 200.0)
        .dangerously_overwrite_destination_table(dataset.table(CITY_TOTALS))
        .obj()
        .query()
        .await?;
    print_city_totals(
        "The table after overwriting it with the totals over 200",
        overwritten,
    );

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
        .description("Scratch dataset of the query destination table example")
        .default_table_expiration(Duration::from_secs(3600))
        .execute()
        .await?;
    println!("Created the scratch dataset {dataset}");

    let outcome = run_queries(&db, &dataset).await;

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
