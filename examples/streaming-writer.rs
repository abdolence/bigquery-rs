//! Streams rows into a table as they arrive, first through the default stream while a separate
//! task reports each acknowledged batch, then through two pending streams that become visible
//! together in one commit.
//!
//! Run with `PROJECT_ID=<your-project> cargo run --example streaming-writer`.

use bigquery::*;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub fn config_env_var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|error| format!("{name}: {error}"))
}

const READINGS: BigQueryTableId = BigQueryTableId::from_static("sensor_readings");

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SensorReading {
    sensor: String,
    sequence: i64,
    celsius: f64,
    measured_at: BigQueryTimestamp,
}

impl SensorReading {
    fn measured(sensor: &str, sequence: i64) -> Self {
        SensorReading {
            sensor: sensor.to_string(),
            sequence,
            celsius: 18.0 + (sequence % 7) as f64 * 0.5,
            measured_at: BigQueryTimestamp(BigQueryInstant::now()),
        }
    }
}

/// A dataset name unique to this run, so that concurrent runs never share one.
fn scratch_dataset_id() -> Result<BigQueryDatasetId, Box<dyn std::error::Error + Send + Sync>> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
    Ok(BigQueryDatasetId::new(format!(
        "bigquery_example_streaming_writer_{}_{}",
        now.as_secs(),
        now.subsec_nanos()
    ))?)
}

async fn stored_readings(
    db: &BigQueryDb,
    dataset: &BigQueryDatasetId,
) -> BigQueryResult<Vec<SensorReading>> {
    db.fluent()
        .select()
        .from(dataset.table(READINGS))
        .obj()
        .query()
        .await
}

async fn stream_readings(
    db: &BigQueryDb,
    dataset: &BigQueryDatasetId,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    db.fluent()
        .schema()
        .table(dataset.table(READINGS))
        .columns(|columns| {
            columns.fields([
                columns
                    .field(path!(SensorReading::sensor))
                    .string()
                    .required(),
                columns
                    .field(path!(SensorReading::sequence))
                    .int64()
                    .required(),
                columns.field(path!(SensorReading::celsius)).float64(),
                columns.field(path!(SensorReading::measured_at)).timestamp(),
            ])
        })
        .sync()
        .await?;
    println!("Created the table {}", dataset.table(READINGS));

    // A batch goes out once it holds 5 rows or its first row is 200 ms old, whichever comes
    // first; real writers keep the defaults and let the byte limit decide.
    let options = BigQueryStreamingWriteOptions::new()
        .with_max_batch_rows(5)
        .with_max_batch_delay(Duration::from_millis(200));
    let (mut writer, acknowledgements) = db
        .create_streaming_writer_with_options::<SensorReading>(dataset.table(READINGS), options)
        .await?;

    // Reading the acknowledgements is optional; `finish` reports failed rows either way.
    let reporter = tokio::spawn(acknowledgements.for_each(|acknowledgement| async move {
        match acknowledgement {
            Ok(batch) => println!(
                "  batch {} acknowledged: {} rows from row {}",
                batch.batch_index, batch.row_count, batch.first_row
            ),
            Err(error) => eprintln!("  a batch failed: {error}"),
        }
    }));

    println!("Streaming 12 readings through the default stream:");
    for sequence in 1..=12 {
        writer
            .write(&SensorReading::measured("greenhouse", sequence))
            .await?;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let summary = writer.finish().await?;
    reporter.await?;
    println!(
        "Default stream: {} rows written, {} failed, in {} batches",
        summary.rows_written, summary.rows_failed, summary.batches
    );

    // Two pending streams, say from two producers: neither shows a row until the commit, and
    // the commit makes both visible at once.
    let pending_options =
        BigQueryStreamingWriteOptions::new().with_mode(BigQueryWriteMode::Pending);
    let (mut cellar_writer, _) = db
        .create_streaming_writer_with_options::<SensorReading>(
            dataset.table(READINGS),
            pending_options.clone(),
        )
        .await?;
    let (mut attic_writer, _) = db
        .create_streaming_writer_with_options::<SensorReading>(
            dataset.table(READINGS),
            pending_options,
        )
        .await?;
    let cellar_readings: Vec<SensorReading> = (1..=8)
        .map(|sequence| SensorReading::measured("cellar", sequence))
        .collect();
    let attic_readings: Vec<SensorReading> = (1..=8)
        .map(|sequence| SensorReading::measured("attic", sequence))
        .collect();
    cellar_writer.write_all(&cellar_readings).await?;
    attic_writer.write_all(&attic_readings).await?;
    let cellar_stream = cellar_writer.finalize().await?;
    let attic_stream = attic_writer.finalize().await?;
    println!(
        "Pending streams finalized with {} and {} rows",
        cellar_stream.row_count, attic_stream.row_count
    );

    let before_commit = stored_readings(db, dataset).await?;
    println!(
        "Before the commit the table holds {} rows",
        before_commit.len()
    );

    let commit_time = db
        .commit_write_streams(vec![cellar_stream, attic_stream])
        .await?;
    println!("Committed both pending streams at {commit_time}");

    let mut after_commit = stored_readings(db, dataset).await?;
    after_commit
        .sort_by(|left, right| (&left.sensor, left.sequence).cmp(&(&right.sensor, right.sequence)));
    println!(
        "After the commit the table holds {} rows:",
        after_commit.len()
    );
    for reading in &after_commit {
        println!(
            "  {:<10} #{:>2} {:.1} °C",
            reading.sensor, reading.sequence, reading.celsius
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

    let outcome = stream_readings(&db, &dataset).await;

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
