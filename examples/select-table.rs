//! Reads a table through the Storage Read API: every column first, then a projection built
//! with `paths!` and a typed `.filter` that BigQuery applies before the rows leave the server.
//!
//! Run with `PROJECT_ID=<your-project> cargo run --example select-table`.

use bigquery::*;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub fn config_env_var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|error| format!("{name}: {error}"))
}

const RESIDENTS: BigQueryTableId = BigQueryTableId::from_static("residents");

const NAMES: [&str; 10] = [
    "Åsa", "Björn", "Linnéa", "Örjan", "Märta", "Göran", "Saga", "Håkan", "Elsa", "Sören",
];
const COUNTIES: [&str; 4] = ["Skåne", "Stockholm", "Uppsala", "Norrbotten"];

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Resident {
    id: i64,
    name: String,
    county: String,
    birth_year: i64,
    registered_on: jiff::civil::Date,
}

#[derive(Debug, Deserialize)]
struct ResidentSummary {
    name: String,
    county: String,
    birth_year: i64,
}

/// A dataset name unique to this run, so that concurrent runs never share one.
fn scratch_dataset_id() -> Result<BigQueryDatasetId, Box<dyn std::error::Error + Send + Sync>> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
    Ok(BigQueryDatasetId::new(format!(
        "bigquery_example_select_table_{}_{}",
        now.as_secs(),
        now.subsec_nanos()
    ))?)
}

async fn select_from_table(
    db: &BigQueryDb,
    dataset: &BigQueryDatasetId,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    db.fluent()
        .schema()
        .table(dataset.table(RESIDENTS))
        .columns(|columns| {
            columns.fields([
                columns.field(path!(Resident::id)).int64().required(),
                columns.field(path!(Resident::name)).string().required(),
                columns.field(path!(Resident::county)).string().required(),
                columns
                    .field(path!(Resident::birth_year))
                    .int64()
                    .required(),
                columns.field(path!(Resident::registered_on)).date(),
            ])
        })
        .sync()
        .await?;

    let residents: Vec<Resident> = (0..30)
        .map(|id| Resident {
            id,
            name: NAMES[id as usize % NAMES.len()].to_string(),
            county: COUNTIES[id as usize % COUNTIES.len()].to_string(),
            birth_year: 1960 + id * 2,
            registered_on: jiff::civil::date(2020, 1, 1) + jiff::Span::new().days(id * 11),
        })
        .collect();
    let written = db
        .fluent()
        .insert()
        .into(dataset.table(RESIDENTS))
        .objects(&residents)
        .exactly_once()
        .execute()
        .await?;
    println!("Inserted {} residents", written.rows_written);

    let everyone: Vec<Resident> = db
        .fluent()
        .select()
        .from(dataset.table(RESIDENTS))
        .obj()
        .query()
        .await?;
    println!("Read {} residents with every column", everyone.len());
    if let Some(first_resident) = everyone.iter().min_by_key(|resident| resident.id) {
        println!("The first one: {first_resident:?}");
    }

    // Only three columns, and only the rows the filter keeps. The filter is sent to the
    // Storage Read API as the session's row restriction, so BigQuery skips the other rows
    // before they leave the server.
    let born_since = 1990;
    let southern_counties = ["Skåne", "Stockholm"];
    let mut recent_southerners: Vec<ResidentSummary> = db
        .fluent()
        .select()
        .fields(paths!(ResidentSummary::{name, county, birth_year}))
        .from(dataset.table(RESIDENTS))
        .filter(|filter| {
            filter.for_all([
                filter.field(path!(Resident::birth_year)).ge(born_since),
                filter
                    .field(path!(Resident::county))
                    .is_in(southern_counties),
                filter.field(path!(Resident::registered_on)).is_not_null(),
            ])
        })
        .obj()
        .stream_query_with_errors()
        .await?
        .try_collect()
        .await?;
    recent_southerners.sort_by_key(|resident| resident.birth_year);
    println!(
        "Residents of {} born in {born_since} or later:",
        southern_counties.join(" or ")
    );
    for resident in &recent_southerners {
        println!(
            "  {} from {}, born {}",
            resident.name, resident.county, resident.birth_year
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
        .description("Scratch dataset of the select-table example")
        .default_table_expiration(Duration::from_secs(3600))
        .execute()
        .await?;
    println!("Created the scratch dataset {dataset}");

    let outcome = select_from_table(&db, &dataset).await;

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
