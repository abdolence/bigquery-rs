//! Reads a table as Arrow record batches: the raw columns first, as BigQuery sent them, then the
//! same batches decoded into typed rows with `BigQueryBatchRows`.
//!
//! Run with `PROJECT_ID=<your-project> cargo run --example record-batches`.

use bigquery::arrow_array::cast::AsArray;
use bigquery::arrow_array::types::Float64Type;
use bigquery::arrow_array::RecordBatch;
use bigquery::*;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub fn config_env_var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|error| format!("{name}: {error}"))
}

const READINGS: BigQueryTableId = BigQueryTableId::from_static("readings");

const STATIONS: [&str; 3] = ["Abisko", "Visby", "Lund"];

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Reading {
    station: String,
    temperature_celsius: f64,
    measured_at: jiff::Timestamp,
}

/// A dataset name unique to this run, so that concurrent runs never share one.
fn scratch_dataset_id() -> Result<BigQueryDatasetId, Box<dyn std::error::Error + Send + Sync>> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
    Ok(BigQueryDatasetId::new(format!(
        "bigquery_example_record_batches_{}_{}",
        now.as_secs(),
        now.subsec_nanos()
    ))?)
}

async fn read_record_batches(
    db: &BigQueryDb,
    dataset: &BigQueryDatasetId,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    db.fluent()
        .schema()
        .table(dataset.table(READINGS))
        .columns(|columns| {
            columns.fields([
                columns.field(path!(Reading::station)).string().required(),
                columns
                    .field(path!(Reading::temperature_celsius))
                    .float64()
                    .required(),
                columns
                    .field(path!(Reading::measured_at))
                    .timestamp()
                    .required(),
            ])
        })
        .sync()
        .await?;

    let first_measurement: jiff::Timestamp = "2026-01-15T06:00:00Z".parse()?;
    let readings: Vec<Reading> = (0..36)
        .map(|hour| Reading {
            station: STATIONS[hour as usize % STATIONS.len()].to_string(),
            temperature_celsius: -12.0 + hour as f64 * 0.75,
            measured_at: first_measurement + jiff::Span::new().hours(hour),
        })
        .collect();
    let written = db
        .fluent()
        .insert()
        .into(dataset.table(READINGS))
        .objects(&readings)
        .exactly_once()
        .execute()
        .await?;
    println!("Inserted {} readings", written.rows_written);

    let batches: Vec<RecordBatch> = db
        .fluent()
        .select()
        .fields(paths!(Reading::{station, temperature_celsius, measured_at}))
        .from(dataset.table(READINGS))
        .record_batches()
        .await?
        .try_collect()
        .await?;

    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    println!("Read {} batches with {total_rows} rows", batches.len());
    if let Some(first_batch) = batches.first() {
        println!("The Arrow schema of the batches:");
        for field in first_batch.schema().fields() {
            println!(
                "  {}: {} (nullable: {})",
                field.name(),
                field.data_type(),
                field.is_nullable()
            );
        }
    }

    // The raw Arrow columns are there for vectorised work that needs no row structs
    let mut temperature_sum = 0.0;
    for batch in &batches {
        let temperatures = batch
            .column_by_name(&path!(Reading::temperature_celsius))
            .ok_or("the batch has no temperature column")?
            .as_primitive_opt::<Float64Type>()
            .ok_or("the temperature column is not FLOAT64")?;
        temperature_sum += temperatures.values().iter().sum::<f64>();
    }
    println!(
        "Mean temperature from the Arrow column: {:.2} °C",
        temperature_sum / total_rows as f64
    );

    // The same batches decoded into rows, with the type mapping the typed reads use
    let mut decoded: Vec<Reading> = Vec::with_capacity(total_rows);
    for batch in &batches {
        for reading in BigQueryBatchRows::<Reading>::new(batch) {
            decoded.push(reading?);
        }
    }
    decoded.sort_by_key(|reading| reading.measured_at);
    println!("The first readings, decoded with BigQueryBatchRows:");
    for reading in decoded.iter().take(5) {
        println!(
            "  {} at {}: {:.2} °C",
            reading.station, reading.measured_at, reading.temperature_celsius
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

    // The expiration removes the tables even if this process dies before its cleanup.
    let dataset = scratch_dataset_id()?;
    db.fluent()
        .schema()
        .dataset(dataset.clone())
        .create()
        .description("Scratch dataset of the record-batches example")
        .default_table_expiration(Duration::from_secs(3600))
        .execute()
        .await?;
    println!("Created the scratch dataset {dataset}");

    let outcome = read_record_batches(&db, &dataset).await;

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
