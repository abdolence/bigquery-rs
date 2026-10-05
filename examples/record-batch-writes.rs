//! Writes Arrow record batches: a batch too large for one request through an exactly once
//! insert, then a buffered writer whose rows become readable only at each flush, and reads the
//! table back as record batches.
//!
//! Run with `PROJECT_ID=<your-project> cargo run --example record-batch-writes`.

use bigquery::arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray};
use bigquery::arrow_schema::{ArrowError, DataType, Field, Schema};
use bigquery::*;
use futures::TryStreamExt;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub fn config_env_var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|error| format!("{name}: {error}"))
}

const READINGS: BigQueryTableId = BigQueryTableId::from_static("readings");

const STATIONS: [&str; 3] = ["Abisko", "Visby", "Lund"];

/// A dataset name unique to this run, so that concurrent runs never share one.
fn scratch_dataset_id() -> Result<BigQueryDatasetId, Box<dyn std::error::Error + Send + Sync>> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
    Ok(BigQueryDatasetId::new(format!(
        "bigquery_example_record_batch_writes_{}_{}",
        now.as_secs(),
        now.subsec_nanos()
    ))?)
}

/// Readings `sequences` as one record batch with the table's columns.
fn readings(sequences: std::ops::Range<i64>) -> Result<RecordBatch, ArrowError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("sequence", DataType::Int64, false),
        Field::new("station", DataType::Utf8, false),
        Field::new("temperature_celsius", DataType::Float64, true),
    ]));
    let stations: StringArray = sequences
        .clone()
        .map(|sequence| Some(STATIONS[sequence as usize % STATIONS.len()]))
        .collect();
    let temperatures: Float64Array = sequences
        .clone()
        .map(|sequence| Some(-5.0 + (sequence % 40) as f64 * 0.5))
        .collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from_iter_values(sequences)),
            Arc::new(stations),
            Arc::new(temperatures),
        ],
    )
}

async fn stored_rows(db: &BigQueryDb, dataset: &BigQueryDatasetId) -> BigQueryResult<usize> {
    let batches: Vec<RecordBatch> = db
        .fluent()
        .select()
        .from(dataset.table(READINGS))
        .record_batches()
        .await?
        .try_collect()
        .await?;
    Ok(batches.iter().map(RecordBatch::num_rows).sum())
}

async fn write_record_batches(
    db: &BigQueryDb,
    dataset: &BigQueryDatasetId,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    db.fluent()
        .schema()
        .table(dataset.table(READINGS))
        .columns(|columns| {
            columns.fields([
                columns.field("sequence").int64().required(),
                columns.field("station").string().required(),
                columns.field("temperature_celsius").float64(),
            ])
        })
        .sync()
        .await?;
    println!("Created the table {}", dataset.table(READINGS));

    // About 25 MB as Arrow IPC, over the 20 MiB request limit, so it goes as slices of
    // the batch, one request each.
    let large = readings(0..1_000_000)?;
    let summary = db
        .fluent()
        .insert()
        .into(dataset.table(READINGS))
        .record_batches([large])
        .exactly_once()
        .execute()
        .await?;
    println!(
        "Exactly once: {} rows in {} requests, {} bytes sent",
        summary.rows_written, summary.batches, summary.bytes_sent
    );
    println!("The table holds {} rows", stored_rows(db, dataset).await?);

    let (mut writer, _) = db
        .create_record_batch_writer_with_options(
            dataset.table(READINGS),
            BigQueryStreamingWriteOptions::new().with_mode(BigQueryWriteMode::Buffered),
        )
        .await?;
    writer.write_batch(&readings(1_000_000..1_000_100)?).await?;
    writer.flush().await?;
    println!(
        "Buffered, acknowledged and not flushed: the table holds {} rows",
        stored_rows(db, dataset).await?
    );
    let flushed = writer.flush_rows().await?;
    println!(
        "Flushed up to offset {flushed:?}: the table holds {} rows",
        stored_rows(db, dataset).await?
    );
    writer.write_batch(&readings(1_000_100..1_000_200)?).await?;
    let summary = writer.finish().await?;
    println!(
        "Buffered writer finished with {} rows: the table holds {} rows",
        summary.rows_written,
        stored_rows(db, dataset).await?
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

    let outcome = write_record_batches(&db, &dataset).await;

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
