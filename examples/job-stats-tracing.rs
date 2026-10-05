//! Reads what BigQuery reports for a DML statement and a query: the job or query ID, bytes
//! processed and billed, slot time, cache hits and changed rows, from `query_with_stats()` and
//! from the crate's tracing spans.
//!
//! Run with `PROJECT_ID=<your-project> cargo run --example job-stats-tracing`.

use bigquery::*;
use serde::Deserialize;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing_subscriber::fmt::format::FmtSpan;

pub fn config_env_var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|error| format!("{name}: {error}"))
}

const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");

#[derive(Deserialize)]
struct CustomerTotal {
    customer: String,
    order_count: i64,
    total_amount: i64,
}

/// A dataset name unique to this run, so that concurrent runs never share one.
fn scratch_dataset_id() -> Result<BigQueryDatasetId, Box<dyn std::error::Error + Send + Sync>> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
    Ok(BigQueryDatasetId::new(format!(
        "bigquery_example_job_stats_tracing_{}_{}",
        now.as_secs(),
        now.subsec_nanos()
    ))?)
}

/// `value`, or `not reported` for a figure BigQuery left out.
fn reported<T: std::fmt::Display>(value: Option<T>) -> String {
    value.map_or("not reported".to_string(), |value| value.to_string())
}

fn print_stats(title: &str, stats: &BigQueryJobStats) {
    println!("{title}:");
    match (&stats.job, &stats.query_id) {
        (Some(job), _) => println!("  job {}", job.job_id),
        (None, Some(query_id)) => println!("  no job, query ID {query_id}"),
        (None, None) => println!("  no job and no query ID"),
    }
    println!(
        "  statement type: {}",
        reported(stats.statement_type.as_ref())
    );
    println!("  rows in the result: {}", reported(stats.total_rows));
    println!(
        "  bytes processed: {}",
        reported(stats.total_bytes_processed)
    );
    println!("  bytes billed: {}", reported(stats.total_bytes_billed));
    println!("  slot milliseconds: {}", reported(stats.total_slot_ms));
    println!("  answered from the cache: {}", reported(stats.cache_hit));
    if let Some(dml_stats) = &stats.dml_stats {
        println!(
            "  rows inserted {}, updated {}, deleted {}",
            dml_stats.inserted, dml_stats.updated, dml_stats.deleted
        );
    }
    println!();
}

async fn query_with_stats(
    db: &BigQueryDb,
    dataset: &BigQueryDatasetId,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    db.fluent()
        .schema()
        .table(dataset.table(ORDERS))
        .columns(|columns| {
            columns.fields([
                columns.field("order_id").int64().required(),
                columns.field("customer").string().required(),
                columns.field("amount").int64(),
            ])
        })
        .sync()
        .await?;

    let inserted = db
        .fluent()
        .query(format!(
            "INSERT INTO `{ORDERS}` (order_id, customer, amount) \
             SELECT order_id, CONCAT('customer-', CAST(MOD(order_id, 3) AS STRING)), order_id * 10 \
             FROM UNNEST(GENERATE_ARRAY(1, 30)) AS order_id"
        ))
        .default_dataset(dataset.clone())
        .execute()
        .await?;
    print_stats("The INSERT", &inserted);

    // The same query twice: the first runs, the second is answered from the query cache and
    // bills nothing.
    for attempt in ["The first SELECT", "The same SELECT again"] {
        let (totals, stats): (Vec<CustomerTotal>, BigQueryJobStats) = db
            .fluent()
            .query(format!(
                "SELECT customer, COUNT(*) AS order_count, SUM(amount) AS total_amount \
                 FROM `{ORDERS}` GROUP BY customer ORDER BY customer"
            ))
            .default_dataset(dataset.clone())
            .obj::<CustomerTotal>()
            .query_with_stats()
            .await?;
        for total in &totals {
            println!(
                "{}: {} orders, {} in total",
                total.customer, total.order_count, total.total_amount
            );
        }
        print_stats(attempt, &stats);
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // The crate records each query's job, bytes, slots and route as fields of its
    // `BigQuery Query` span at debug level; closing a span prints them.
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter("bigquery=debug")
        .with_span_events(FmtSpan::CLOSE)
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

    let outcome = query_with_stats(&db, &dataset).await;

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
