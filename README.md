# BigQuery for Rust

Library provides a simple API for Google BigQuery based on gRPC:

- Storage Read API for table scans and large query results;
- Storage Write API for inserts;
- BigQuery v2 API over gRPC for queries, jobs, datasets and tables;
- Retries with jitter for the errors where retrying makes sense, including BigQuery rate limits;
- Full async based on Tokio runtime;
- Google client based on [gcloud-sdk library](https://github.com/abdolence/gcloud-sdk-rs)
  that automatically detects GCE environment or application default accounts for local development;

The library is in early development: at the moment it provides only the client itself. Typed
reads and writes, queries and schema management are coming next.

## Documentation

The book is in the [docs](docs/src/intro.md) directory, and the API reference on
[docs.rs](https://docs.rs/bigquery).

## Quick start

Cargo.toml:

```toml
[dependencies]
bigquery = "0.1"
```

Example code:

```rust,no_run
use bigquery::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Create an instance
    let db = BigQueryDb::new("my-gcp-project-id").await?;

    println!("Connected to {}", db.options().google_project_id);
    Ok(())
}
```

## Google authentication

Looks for credentials in the following places, preferring the first location found:

- A JSON file whose path is specified by the GOOGLE_APPLICATION_CREDENTIALS environment variable.
- A JSON file in a location known to the gcloud command-line tool using `gcloud auth application-default login`.
- On Google Compute Engine, it fetches credentials from the metadata server.

For local development don't confuse `gcloud auth login` with `gcloud auth application-default login`,
since the first authorize only `gcloud` tool to access the Cloud Platform.

## How this library is tested

There are integration tests in the tests directory that run against a real BigQuery project
when `GCP_PROJECT` is set. Each test creates its own scratch dataset and deletes it at the end.
Be aware not to introduce huge reads or writes there.

## Licence

Apache Software License (ASL)

## Author

Abdulla Abdurakhmanov
