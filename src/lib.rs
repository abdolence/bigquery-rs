//! # BigQuery for Rust
//!
//! A client for Google BigQuery that uses gRPC throughout: the Storage Read API for table scans
//! and large results, the Storage Write API for inserts, and the v2 API for queries, jobs,
//! datasets and tables.
//!
//! ## Example
//!
//! ```rust,no_run
//! use bigquery::*;
//!
//! # async fn example() -> BigQueryResult<()> {
//! let db = BigQueryDb::new("my-project-id").await?;
//! let fluent = db.fluent();
//! # let _ = fluent;
//! # Ok(())
//! # }
//! ```

#![forbid(unsafe_code)]

/// The error types of this crate, built around [`BigQueryError`].
pub mod errors;

mod db;

pub use db::*;

mod fluent_api;

pub use fluent_api::*;

mod types;

pub use types::*;

mod read;

pub use read::*;

mod write;

pub use write::*;

mod query;

pub use query::*;

mod sql;

mod schema;

pub use schema::*;

mod admin;

pub use admin::*;

mod struct_path_macro;

/// The `struct_path` crate the [`path!`] and [`paths!`] macros expand to, for its
/// `StructPath` derive.
pub extern crate struct_path;

pub use arrow_array;
pub use arrow_schema;
pub use jiff;
pub use url;

/// An exact instant in time, used by this library for the times BigQuery reports about a
/// dataset, a table, a job or a write: creation and last modified times, a job's start and end,
/// a table's expiration, a commit time, and a read's snapshot time.
///
/// This is an alias for [`jiff::Timestamp`]. Prefer this alias over naming `jiff::Timestamp`
/// directly, so that your code does not need an explicit `jiff` dependency and stays insulated
/// from changes of the underlying implementation.
///
/// It is not a TIMESTAMP column value: a column in your rows is a [`BigQueryTimestamp`], or a
/// plain `jiff::Timestamp`, as the type mapping describes.
///
/// ```rust
/// use bigquery::*;
///
/// let since: BigQueryInstant = "2026-10-01T00:00:00Z".parse()?;
/// let params = BigQueryListJobsParams::new().with_min_creation_time(since);
/// # let _ = params;
/// # Ok::<(), jiff::Error>(())
/// ```
pub type BigQueryInstant = jiff::Timestamp;

use crate::errors::BigQueryError;

/// The result type of every fallible call in this crate.
pub type BigQueryResult<T> = std::result::Result<T, BigQueryError>;

#[cfg(doctest)]
mod book;
