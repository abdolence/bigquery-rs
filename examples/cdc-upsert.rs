//! Keeps a table in step with a source of changes through BigQuery CDC: upserts and deletes by
//! primary key, ordered by sequence numbers, through a CDC writer and through the fluent API.
//!
//! Run with `PROJECT_ID=<your-project> cargo run --example cdc-upsert`.

use bigquery::*;
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub fn config_env_var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|error| format!("{name}: {error}"))
}

const CUSTOMERS: BigQueryTableId = BigQueryTableId::from_static("customers");

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Customer {
    customer_id: i64,
    name: String,
    tier: String,
}

impl Customer {
    fn new(customer_id: i64, name: &str, tier: &str) -> Self {
        Customer {
            customer_id,
            name: name.to_string(),
            tier: tier.to_string(),
        }
    }
}

/// A dataset name unique to this run, so that concurrent runs never share one.
fn scratch_dataset_id() -> Result<BigQueryDatasetId, Box<dyn std::error::Error + Send + Sync>> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
    Ok(BigQueryDatasetId::new(format!(
        "bigquery_example_cdc_upsert_{}_{}",
        now.as_secs(),
        now.subsec_nanos()
    ))?)
}

/// The table as a query sees it: BigQuery applies the CDC changes it has received at query
/// time, so the result is current even before its background merge has run.
async fn current_customers(
    db: &BigQueryDb,
    dataset: &BigQueryDatasetId,
) -> BigQueryResult<Vec<Customer>> {
    db.fluent()
        .query(format!(
            "SELECT customer_id, name, tier FROM `{}` ORDER BY customer_id",
            dataset.table(CUSTOMERS)
        ))
        .obj()
        .query()
        .await
}

fn print_customers(heading: &str, customers: &[Customer]) {
    println!("{heading}:");
    for customer in customers {
        println!(
            "  #{} {:<8} {}",
            customer.customer_id, customer.name, customer.tier
        );
    }
}

async fn apply_changes(
    db: &BigQueryDb,
    dataset: &BigQueryDatasetId,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // CDC needs a primary key; BigQuery never enforces it, it only uses it to match changes.
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
                columns.field(path!(Customer::tier)).string(),
            ])
        })
        .primary_key([path!(Customer::customer_id)])
        .sync()
        .await?;
    println!(
        "Created the table {} keyed by customer_id",
        dataset.table(CUSTOMERS)
    );

    let (mut writer, _) = db
        .create_cdc_writer::<Customer>(
            dataset.table(CUSTOMERS),
            BigQueryStreamingWriteOptions::new(),
        )
        .await?;
    // Changes to one key need an order. Without sequence numbers the change BigQuery ingested
    // last wins. With sequence numbers, such as a source database's log position, the highest
    // one wins whatever order the changes arrive in, so a change delivered twice or late cannot
    // overwrite a newer one. This table uses them, and a table that does needs one on every
    // change, the first load included: mixing changes with and without them gives an
    // unpredictable order.
    for (sequence_number, customer) in [
        (1, Customer::new(1, "Ada", "bronze")),
        (2, Customer::new(2, "Grace", "bronze")),
        (3, Customer::new(3, "Linus", "bronze")),
        (4, Customer::new(4, "Barbara", "bronze")),
    ] {
        writer
            .write_change(&BigQueryChange {
                change_type: BigQueryChangeType::Upsert,
                sequence_number: Some(BigQueryChangeSequenceNumber::from(sequence_number)),
                row: customer,
            })
            .await?;
    }
    writer.flush().await?;

    // Barbara's newer change is sent first here and still wins; a delete needs only the key.
    for change in [
        BigQueryChange {
            change_type: BigQueryChangeType::Upsert,
            sequence_number: Some(BigQueryChangeSequenceNumber::from(101)),
            row: Customer::new(2, "Grace", "silver"),
        },
        BigQueryChange {
            change_type: BigQueryChangeType::Delete,
            sequence_number: Some(BigQueryChangeSequenceNumber::from(102)),
            row: Customer::new(3, "", ""),
        },
        BigQueryChange {
            change_type: BigQueryChangeType::Upsert,
            sequence_number: Some(BigQueryChangeSequenceNumber::from(104)),
            row: Customer::new(4, "Barbara", "gold"),
        },
        BigQueryChange {
            change_type: BigQueryChangeType::Upsert,
            sequence_number: Some(BigQueryChangeSequenceNumber::from(103)),
            row: Customer::new(4, "Barbara", "silver"),
        },
    ] {
        writer.write_change(&change).await?;
    }
    let summary = writer.finish().await?;
    println!(
        "CDC writer: {} changes written, {} failed",
        summary.rows_written, summary.rows_failed
    );
    print_customers(
        "After the CDC writer",
        &current_customers(db, dataset).await?,
    );

    // The fluent API does the same in one call: `update()` for rows that share one sequence
    // number, `changes` on an insert for a mix with a number each.
    let upserted = [
        Customer::new(1, "Ada", "gold"),
        Customer::new(5, "Ken", "bronze"),
    ];
    db.fluent()
        .update()
        .in_table(dataset.table(CUSTOMERS))
        .objects(&upserted)
        .sequence_number(105)
        .execute()
        .await?;
    db.fluent()
        .insert()
        .into(dataset.table(CUSTOMERS))
        .changes([
            BigQueryChange {
                change_type: BigQueryChangeType::Delete,
                sequence_number: Some(BigQueryChangeSequenceNumber::from(106)),
                row: Customer::new(2, "", ""),
            },
            BigQueryChange {
                change_type: BigQueryChangeType::Upsert,
                sequence_number: Some(BigQueryChangeSequenceNumber::from(107)),
                row: Customer::new(6, "Margaret", "silver"),
            },
        ])
        .execute()
        .await?;
    print_customers(
        "After the fluent update and changes",
        &current_customers(db, dataset).await?,
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

    let outcome = apply_changes(&db, &dataset).await;

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
