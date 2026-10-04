# BigQuery for Rust

Library provides a simple API for Google BigQuery, using gRPC for everything:

- the Storage Read API for table scans and large query results;
- the Storage Write API for inserts;
- the BigQuery v2 API for queries, jobs, datasets and tables.

The v2 API over gRPC is pre-GA and not documented by Google, while the official client
libraries use REST for it. The library was tested against it before choosing it, and it works
for every call the library makes. Be aware Google can change it without notice.

The library is in early development. At the moment it provides the client with both gRPC
channels, error classification and retries. Typed reads, writes, queries and schema management
are coming next.

## Creating a client

```rust,no_run
use bigquery::*;

# async fn example() -> BigQueryResult<()> {
// Uses the application default credentials
let db = BigQueryDb::new("my-gcp-project-id").await?;

// Or detects the project from GCP_PROJECT, PROJECT_ID or GCP_PROJECT_ID,
// the local credentials or the metadata server
let db = BigQueryDb::for_default_project_id().await?;

// Or with options
let db = BigQueryDb::with_options(
    BigQueryDbOptions::new("my-gcp-project-id".to_string()).with_max_retries(5),
)
.await?;
# let _ = db;
# Ok(())
# }
```

Clones of `BigQueryDb` are cheap and share the same channels, so create it once.

Leave `location` unset unless you need it. BigQuery finds the location of a dataset or a job
by itself, and a wrong location fails with `NotFound`.

## Crypto provider error

Depends on your other dependencies you may see the error like:

```text
no process-level CryptoProvider available -- call CryptoProvider::install_default() before this point
```

The TLS crypto providers are not installed by default, so you can choose one. The easiest way to
fix it is to include one, for example:

```toml
[dependencies]
rustls = "0.23"
```

If you have several, you may need to call `CryptoProvider::install_default()` before creating the
client:

```rust,ignore
rustls::crypto::ring::default_provider().install_default().expect("Failed to install rustls crypto provider");
```
