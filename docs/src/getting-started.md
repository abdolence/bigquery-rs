# Getting started

Cargo.toml:

```toml
[dependencies]
bigquery = "0.8"
```

The default feature `tls-roots` uses the native TLS roots of your system. Use
`tls-webpki-roots` instead if you want the bundled Mozilla roots:

```toml
[dependencies]
bigquery = { version = "0.8", default-features = false, features = ["tls-webpki-roots"] }
```

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

## Creating a client

`BigQueryDb` is the client. It opens two authenticated gRPC channels, one to the BigQuery v2 API
for queries, jobs, datasets and tables, and one to the Storage API for reads and writes. Clones
are cheap and share both channels, so create it once and clone it where you need it.

```rust,no_run
use bigquery::*;

# async fn example() -> BigQueryResult<()> {
// Application default credentials, the project given explicitly
let db = BigQueryDb::new("my-gcp-project-id").await?;

// The project detected from GCP_PROJECT, PROJECT_ID or GCP_PROJECT_ID,
// the quota project of the local credentials or the metadata server
let db = BigQueryDb::for_default_project_id().await?;

// With options
let db = BigQueryDb::with_options(
    BigQueryDbOptions::new("my-gcp-project-id".to_string()).with_max_retries(5),
)
.await?;

// Everything else starts from the fluent API
let outcome = db.fluent().query("SELECT 1 AS x").execute().await?;
# let _ = outcome;
# Ok(())
# }
```

`for_default_project_id()` fails with `InvalidParametersError` if no project can be detected.

The project ID is checked only for what would break a resource path: it must not be empty and
must not contain `/` or a control character. Everything else is up to BigQuery.

## Client options

`BigQueryDbOptions` has:

- `google_project_id`: the project that runs the jobs and owns the datasets by default;
- `location`: the location to send with jobs and queries, unset by default, see
  [Locations](#locations);
- `max_retries`: how many times a failed retryable request is sent again, `3` by default;
- `bigquery_api_url`: overrides the v2 API endpoint, `https://bigquery.googleapis.com`;
- `bigquery_storage_api_url`: overrides the Storage API endpoint,
  `https://bigquerystorage.googleapis.com`.

Each has a `with_...` builder method, as in the example above. `db.options()` returns the options
a client was created with.

## Google authentication

Looks for credentials in the following places, preferring the first location found:

- A JSON file whose path is specified by the `GOOGLE_APPLICATION_CREDENTIALS` environment variable;
- A JSON file in a location known to the gcloud command-line tool using
  `gcloud auth application-default login`;
- On Google Compute Engine, it fetches credentials from the metadata server.

For local development don't confuse `gcloud auth login` with `gcloud auth application-default login`,
since the first one authorizes only the `gcloud` tool to access the Cloud Platform.

To use a service account key file directly:

```rust,no_run
use bigquery::*;

# async fn example() -> BigQueryResult<()> {
let db = BigQueryDb::with_options_service_account_key_file(
    BigQueryDbOptions::new("my-gcp-project-id".to_string()),
    "/path/to/service-account.json".into(),
)
.await?;
# let _ = db;
# Ok(())
# }
```

For full control over the OAuth2 scopes and the token source there is
`with_options_token_source`. Its `TokenSourceType` comes from
[gcloud-sdk](https://github.com/abdolence/gcloud-sdk-rs), which the library does not re-export, so
add `gcloud-sdk` to your dependencies to use it:

```rust,no_run
use bigquery::*;

# async fn example(service_account_json: String) -> BigQueryResult<()> {
let db = BigQueryDb::with_options_token_source(
    BigQueryDbOptions::new("my-gcp-project-id".to_string()),
    gcloud_sdk::GCP_DEFAULT_SCOPES.clone(),
    gcloud_sdk::TokenSourceType::Json(service_account_json),
)
.await?;
# let _ = db;
# Ok(())
# }
```

Both channels share one token, so the token source is asked for a new one only when it expires.

The token never reaches the library's logs or spans. The one `info!` line at client creation
logs the project, the two endpoints and the scope names.

## Endpoints

The library uses Google's two global endpoints by default. You can change them, for example for a
Private Service Connect endpoint or a proxy:

```rust,no_run
use bigquery::*;

# async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let options = BigQueryDbOptions::new("my-gcp-project-id".to_string())
    .with_bigquery_api_url("https://bigquery-myendpoint.p.googleapis.com".parse()?)
    .with_bigquery_storage_api_url("https://bigquerystorage-myendpoint.p.googleapis.com".parse()?);
let db = BigQueryDb::with_options(options).await?;
# let _ = db;
# Ok(())
# }
```

The URLs are `url::Url` (re-exported as `bigquery::url`). Creating the client fails with
`InvalidParametersError` for a scheme other than `http` or `https`, or a URL without a host.

There is no emulator support. The BigQuery emulators speak only the REST API, and this library
uses gRPC for everything.

The v2 API over gRPC is pre-GA and not documented by Google. It works for every call the library
makes, and the library's live tests run against it, but be aware Google can change it without
notice.

## Locations

Leave the location unset unless you need it. BigQuery finds the location of a dataset from the
dataset itself and the location of a query from the tables it reads. A wrong location fails with
`DataNotFoundError`, so setting one only adds a way to fail.

You need it when BigQuery cannot find it by itself, for example for a query that reads no table
and should run in a particular region. You can set it for every query and job of a client, or for
one query:

```rust,no_run
use bigquery::*;

const STOCKHOLM: BigQueryLocation = BigQueryLocation::from_static("europe-north2");

# async fn example() -> BigQueryResult<()> {
// For every query and job of this client
let db = BigQueryDb::with_options(
    BigQueryDbOptions::new("my-gcp-project-id".to_string()).with_location(STOCKHOLM),
)
.await?;

// For one query, overriding the client's location
let outcome = db
    .fluent()
    .query("SELECT 1")
    .location(BigQueryLocation::new("EU")?)
    .execute()
    .await?;
# let _ = outcome;
# Ok(())
# }
```

`BigQueryLocation` is just a name, since Google adds regions all the time. It accepts
regions such as `europe-north2` and multi-regions such as `US` and `EU`, and checks only that the
name is not empty. Whether BigQuery knows it is checked by BigQuery. `from_static` checks it at
compile time in a `const`.

The location BigQuery reports for a job is kept in its `BigQueryJobRef`, and the job calls
(`get_job`, `cancel_job`, `delete_job`) send it back, so they find jobs in any region without any
setting.

## Retries

Requests that fail with a retryable error are sent again, up to `max_retries` times. The
retryable errors are `UNAVAILABLE`, `RESOURCE_EXHAUSTED`, `ABORTED`, `INTERNAL`, BigQuery's rate
limit errors and transport errors. Before retry number `n` the client waits a random delay of up to
`2^(n-1)` seconds. Each retry is logged at `warn!` inside the call's span.

A retried query does not run a DML statement twice, since every attempt carries the same
`request_id`. The writer resends batches with its own rules, see [Writing data](./writing-data.md).

## Raw gRPC clients

For the calls the library has no API for, `BigQueryDb` gives you the raw gRPC clients on its
shared channels: `job_client()`, `dataset_client()`, `table_client()`, `model_client()`,
`routine_client()`, `row_access_policy_client()`, `project_client()`, `read_client()` and
`write_client()`.

```rust,no_run
use bigquery::*;

# async fn example(db: BigQueryDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
use gcloud_sdk::google::cloud::bigquery::v2::GetServiceAccountRequest;

let response = db
    .project_client()
    .get_service_account(GetServiceAccountRequest {
        project_id: db.options().google_project_id.clone(),
    })
    .await?;
println!("{}", response.into_inner().email);
# Ok(())
# }
```

Their types come from gcloud-sdk, which the library does not re-export, so they can change in a
patch release.

## Running the examples

All examples available in the [examples](https://github.com/abdolence/bigquery-rs/tree/master/examples) directory.
Each one creates its own scratch dataset and deletes it at the end.

To run an example:

```bash
PROJECT_ID=<your-google-project-id> cargo run --example query
```
