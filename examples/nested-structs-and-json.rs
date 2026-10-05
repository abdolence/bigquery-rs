//! Writes and reads nested data: a STRUCT column, an ARRAY<STRUCT> column, and two JSON
//! columns, one held as a `serde_json::Value` and one as a typed struct. A query then reaches
//! into the array with UNNEST and into the JSON with JSON_VALUE.
//!
//! Run with `PROJECT_ID=<your-project> cargo run --example nested-structs-and-json`.

use bigquery::*;
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub fn config_env_var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|error| format!("{name}: {error}"))
}

const CUSTOMERS: BigQueryTableId = BigQueryTableId::from_static("customers");

/// A STRUCT column: its fields are columns of their own, typed and queryable as
/// `address.city`.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Address {
    street: String,
    city: String,
    postcode: Option<String>,
}

/// The element of an ARRAY<STRUCT> column, a REPEATED RECORD in the table schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderLine {
    product: String,
    quantity: i64,
    unit_price_cents: i64,
}

/// A typed document stored in a JSON column: BigQuery holds it as JSON text, and the crate
/// prints it on write and parses it on read.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Preferences {
    newsletter: bool,
    language: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Customer {
    customer_id: i64,
    name: String,
    address: Address,
    order_lines: Vec<OrderLine>,
    preferences: Preferences,
    /// Free-form attributes with no fixed shape, in a JSON column.
    attributes: serde_json::Value,
}

impl Customer {
    fn numbered(customer_id: i64) -> Self {
        let cities = ["Uppsala", "Visby", "Lund"];
        let products = ["kettle", "teapot", "mug", "saucer"];
        let city = cities[customer_id as usize % cities.len()];
        let segment = if customer_id <= 5 {
            "retail"
        } else {
            "wholesale"
        };
        let tags: Vec<String> = (0..customer_id % 3)
            .map(|tag| format!("tag-{tag}"))
            .collect();
        let referred_by: Option<i64> = (customer_id > 1).then_some(customer_id - 1);
        Customer {
            customer_id,
            name: format!("customer-{customer_id}"),
            address: Address {
                street: format!("Storgatan {customer_id}"),
                city: city.to_string(),
                postcode: (customer_id % 2 == 0).then(|| format!("75{customer_id:03}")),
            },
            order_lines: (0..customer_id % 4)
                .map(|line| OrderLine {
                    product: products[line as usize].to_string(),
                    quantity: line + 1,
                    unit_price_cents: 450 * (line + 1),
                })
                .collect(),
            preferences: Preferences {
                newsletter: customer_id % 3 == 0,
                language: if city == "Visby" { "sv" } else { "en" }.to_string(),
            },
            attributes: serde_json::json!({
                "segment": segment,
                "tags": tags,
                "referred_by": referred_by,
            }),
        }
    }
}

/// One row of the UNNEST query.
#[derive(Debug, Deserialize)]
struct PurchasedLine {
    name: String,
    city: String,
    product: String,
    quantity: i64,
}

/// A dataset name unique to this run, so that concurrent runs never share one.
fn scratch_dataset_id() -> Result<BigQueryDatasetId, Box<dyn std::error::Error + Send + Sync>> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
    Ok(BigQueryDatasetId::new(format!(
        "bigquery_example_nested_structs_and_json_{}_{}",
        now.as_secs(),
        now.subsec_nanos()
    ))?)
}

async fn write_and_read_nested(
    db: &BigQueryDb,
    dataset: &BigQueryDatasetId,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    db.fluent()
        .schema()
        .table(dataset.table(CUSTOMERS))
        .columns(|columns| {
            columns.fields([
                columns
                    .field(path!(Customer::customer_id))
                    .int64()
                    .required(),
                columns.field(path!(Customer::name)).string(),
                columns.field(path!(Customer::address)).record(|address| {
                    address.fields([
                        address.field(path!(Address::street)).string(),
                        address.field(path!(Address::city)).string(),
                        address.field(path!(Address::postcode)).string(),
                    ])
                }),
                columns
                    .field(path!(Customer::order_lines))
                    .record(|order_line| {
                        order_line.fields([
                            order_line.field(path!(OrderLine::product)).string(),
                            order_line.field(path!(OrderLine::quantity)).int64(),
                            order_line.field(path!(OrderLine::unit_price_cents)).int64(),
                        ])
                    })
                    .repeated(),
                columns.field(path!(Customer::preferences)).json(),
                columns.field(path!(Customer::attributes)).json(),
            ])
        })
        .sync()
        .await?;
    println!("Created the table {}", dataset.table(CUSTOMERS));

    let customers: Vec<Customer> = (1..=10).map(Customer::numbered).collect();
    let summary = db
        .fluent()
        .insert()
        .into(dataset.table(CUSTOMERS))
        .objects(&customers)
        .execute()
        .await?;
    println!("Inserted {} customers", summary.rows_written);

    let mut stored: Vec<Customer> = db
        .fluent()
        .select()
        .from(dataset.table(CUSTOMERS))
        .obj()
        .query()
        .await?;
    stored.sort_by_key(|customer| customer.customer_id);
    println!("Read back {} customers:", stored.len());
    for customer in &stored {
        let order_total_cents: i64 = customer
            .order_lines
            .iter()
            .map(|line| line.quantity * line.unit_price_cents)
            .sum();
        println!(
            "  #{:>2} {} in {} ({}), {} order lines worth {} cents, newsletter {}, \
             language {}, attributes {}",
            customer.customer_id,
            customer.name,
            customer.address.city,
            customer
                .address
                .postcode
                .as_deref()
                .unwrap_or("no postcode"),
            customer.order_lines.len(),
            order_total_cents,
            customer.preferences.newsletter,
            customer.preferences.language,
            customer.attributes
        );
    }

    // The same columns from SQL: a STRUCT field by its dotted path, the array flattened with
    // UNNEST, and a JSON field read with JSON_VALUE against a query parameter.
    let purchased: Vec<PurchasedLine> = db
        .fluent()
        .query(format!(
            "SELECT customer.name, customer.address.city, line.product, line.quantity \
             FROM `{}` AS customer, UNNEST(customer.order_lines) AS line \
             WHERE JSON_VALUE(customer.attributes, '$.segment') = @segment \
             ORDER BY customer.customer_id, line.product",
            dataset.table(CUSTOMERS)
        ))
        .param("segment", "retail")
        .obj()
        .query()
        .await?;
    println!("Order lines of retail customers, from SQL:");
    for line in &purchased {
        println!(
            "  {} in {} bought {} x {}",
            line.name, line.city, line.quantity, line.product
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

    let outcome = write_and_read_nested(&db, &dataset).await;

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
