# Getting started

Cargo.toml:

```toml
[dependencies]
bigquery = "0.9"
```

The default feature `tls-roots` uses the native TLS roots of your system. Use
`tls-webpki-roots` instead if you want the bundled Mozilla roots. It needs
`default-features = false`, which also turns off the default feature `auth-default-crypto`, so
enable it again, see [Crypto provider](#crypto-provider):

```toml
[dependencies]
bigquery = { version = "0.9", default-features = false, features = ["tls-webpki-roots", "auth-default-crypto"] }
```

## Crypto provider

Service account keys sign a JWT, and `google-cloud-auth` signs it with a rustls crypto provider.
The default feature `auth-default-crypto` gives it the aws-lc-rs provider, which is built from C
sources.

`tls-webpki-roots` builds aws-lc-rs for rustls already, so `auth-default-crypto` adds no C build
with it. `tls-roots` without `auth-default-crypto` builds no aws-lc-rs.

Without `auth-default-crypto`, install a rustls provider before creating a client, with the same
`rustls` version as `google-cloud-auth` (0.23):

```toml
[dependencies]
bigquery = { version = "0.9", default-features = false, features = ["tls-roots"] }
rustls = { version = "0.23", default-features = false, features = ["ring"] }
```

```rust,ignore
rustls::crypto::ring::default_provider().install_default().expect("Failed to install rustls crypto provider");
```

Without a provider:

- `BigQueryDb::new`, `with_options` and `for_default_project_id` fail with a `SystemError` whose
  code is `CryptoProviderMissing`, when the application default credentials are a service account
  key, used directly or as the source of an impersonation;
- credentials passed to `with_options_auth` are not checked, and the first call that signs with a
  key panics.

### Crypto provider error

Depends on your other dependencies you may see the error like:

```text
no process-level CryptoProvider available -- call CryptoProvider::install_default() before this point
```

It appears when rustls cannot pick a provider by itself, because none or more than one is compiled
into your application. Install one with `CryptoProvider::install_default()` before creating the
client, as above.

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

`BigQueryDb::new`, `with_options` and `for_default_project_id` use the application default
credentials for the `cloud-platform` scope. They look for credentials in the following places,
preferring the first location found:

- A JSON file whose path is specified by the `GOOGLE_APPLICATION_CREDENTIALS` environment variable;
- A JSON file in a location known to the gcloud command-line tool using
  `gcloud auth application-default login`;
- On Google Compute Engine, Cloud Run, GKE, etc., the metadata server.

The JSON file can be a service account key, user credentials, impersonated service account
credentials or a workload identity federation configuration.

For local development don't confuse `gcloud auth login` with `gcloud auth application-default login`,
since the first one authorizes only the `gcloud` tool to access the Cloud Platform.

### Other credentials

`with_options_auth` takes anything that converts into `GoogleAuthHeaders` of
[gcloud-sdk](https://github.com/abdolence/gcloud-sdk-rs):

- `Credentials` built with [google-cloud-auth](https://crates.io/crates/google-cloud-auth): a
  service account key that is not the application default credentials, impersonation, the
  metadata server, workload identity federation, or a `CredentialsProvider` of your own;
- `IDTokenCredentials`, for a server that takes Google ID tokens;
- a `GoogleAuthHeaders`, for example the application default credentials for other scopes.

The library does not re-export gcloud-sdk, so add it to your dependencies, with the same version
the library uses:

```toml
[dependencies]
gcloud-sdk = { version = "0.33", default-features = false }
```

A service account key file:

```rust,no_run
use bigquery::*;
use gcloud_sdk::google_cloud_auth::credentials::service_account;

# async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let key = std::fs::read_to_string("/path/to/service-account.json")?;
let credentials = service_account::Builder::new(serde_json::from_str(&key)?).build()?;
let db = BigQueryDb::with_options_auth(
    BigQueryDbOptions::new("my-gcp-project-id".to_string()),
    credentials,
)
.await?;
# let _ = db;
# Ok(())
# }
```

The application default credentials for other OAuth2 scopes:

```rust,no_run
use bigquery::*;
use gcloud_sdk::GoogleAuthHeaders;

# async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let auth = GoogleAuthHeaders::from_adc_with_scopes(vec![
    "https://www.googleapis.com/auth/bigquery".to_string(),
])
.await?;
let db = BigQueryDb::with_options_auth(
    BigQueryDbOptions::new("my-gcp-project-id".to_string()),
    auth,
)
.await?;
# let _ = db;
# Ok(())
# }
```

Credentials that mint tokens refresh them on the Tokio runtime they are built in, so build them in
the runtime that runs the client.

Both channels share one set of credentials, so a token is minted and refreshed once for both.

The token never reaches the library's logs or spans. The one `info!` line at client creation
logs the project and the two endpoints.

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
