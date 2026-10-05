//! Changes a column to a type BigQuery cannot change in place: the sync refuses it without an
//! opt-in, `recreate_if_empty()` replaces the table while it has no rows, and
//! `dangerously_recreate_with_data_loss()` replaces it once it has some.
//!
//! Run with `PROJECT_ID=<your-project> cargo run --example recreate-table`.

use bigquery::errors::BigQueryError;
use bigquery::*;
use serde::Deserialize;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub fn config_env_var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|error| format!("{name}: {error}"))
}

const PAYMENTS: BigQueryTableId = BigQueryTableId::from_static("payments");

#[derive(Deserialize)]
struct RowCount {
    row_count: i64,
}

/// A dataset name unique to this run, so that concurrent runs never share one.
fn scratch_dataset_id() -> Result<BigQueryDatasetId, Box<dyn std::error::Error + Send + Sync>> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
    Ok(BigQueryDatasetId::new(format!(
        "bigquery_example_recreate_table_{}_{}",
        now.as_secs(),
        now.subsec_nanos()
    ))?)
}

/// `payments` with `amount` of the given type. BigQuery cannot change a column between these
/// types in place, so each change of `amount` needs a recreate.
fn payments_with_amount<'a>(
    db: &'a BigQueryDb,
    dataset: &BigQueryDatasetId,
    amount_type: BigQueryFieldType,
) -> BigQueryTableSchemaBuilder<'a> {
    db.fluent()
        .schema()
        .table(dataset.table(PAYMENTS))
        .columns(|columns| {
            columns.fields([
                columns.field("payment_id").int64().required(),
                columns.field("amount").of_type(amount_type),
            ])
        })
}

async fn count_rows(
    db: &BigQueryDb,
    dataset: &BigQueryDatasetId,
    table: &BigQueryTableId,
) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
    let counts: Vec<RowCount> = db
        .fluent()
        .query(format!("SELECT COUNT(*) AS row_count FROM `{table}`"))
        .default_dataset(dataset.clone())
        .obj::<RowCount>()
        .query()
        .await?;
    Ok(counts.first().map_or(0, |count| count.row_count))
}

async fn recreate_payments(
    db: &BigQueryDb,
    dataset: &BigQueryDatasetId,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let report = payments_with_amount(db, dataset, BigQueryFieldType::Int64)
        .sync()
        .await?;
    println!("{report}");

    // Without a recreate opt-in, a change BigQuery cannot make in place is refused and
    // nothing is written; the error carries the plan.
    let text_amount = BigQueryFieldType::String { max_length: None };
    match payments_with_amount(db, dataset, text_amount.clone())
        .sync()
        .await
    {
        Err(BigQueryError::SchemaChangeRefused(refused)) => {
            println!("Refused without an opt-in:\n{}", refused.plan)
        }
        other => return Err(format!("expected a refusal, got {other:?}").into()),
    }

    // The table holds no rows yet, so `recreate_if_empty()` may replace it.
    let report = payments_with_amount(db, dataset, text_amount)
        .recreate_if_empty()
        .sync()
        .await?;
    println!("{report}");

    let seeded = db
        .fluent()
        .query(format!(
            "INSERT INTO `{PAYMENTS}` (payment_id, amount) \
             SELECT payment_id, CAST(payment_id * 3 AS STRING) \
             FROM UNNEST(GENERATE_ARRAY(1, 25)) AS payment_id"
        ))
        .default_dataset(dataset.clone())
        .execute()
        .await?;
    println!(
        "Inserted {} payments\n",
        seeded.num_dml_affected_rows.unwrap_or_default()
    );

    // Rows inserted by DML count in `num_rows` at once, so the table no longer reads as empty.
    match payments_with_amount(db, dataset, BigQueryFieldType::Float64)
        .recreate_if_empty()
        .sync()
        .await
    {
        Err(BigQueryError::SchemaChangeRefused(refused)) => {
            println!("Refused by recreate_if_empty():\n{}", refused.plan)
        }
        other => return Err(format!("expected a refusal, got {other:?}").into()),
    }

    let plan = payments_with_amount(db, dataset, BigQueryFieldType::Float64)
        .dangerously_recreate_with_data_loss()
        .snapshot_first()
        .plan()
        .await?;
    println!("{plan}");
    let report = payments_with_amount(db, dataset, BigQueryFieldType::Float64)
        .dangerously_recreate_with_data_loss()
        .snapshot_first()
        .sync()
        .await?;
    println!("{report}");

    println!(
        "Rows in {PAYMENTS} after the recreate: {}",
        count_rows(db, dataset, &PAYMENTS).await?
    );
    if let Some(snapshot) = &report.snapshot {
        println!(
            "Rows in the snapshot {snapshot}: {}",
            count_rows(db, dataset, snapshot.table()).await?
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

    let outcome = recreate_payments(&db, &dataset).await;

    // The snapshot the recreate took lives in the dataset and goes with it.
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
