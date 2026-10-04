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

mod struct_path_macro;

/// The `struct_path` crate the [`path!`] and [`paths!`] macros expand to, for its
/// `StructPath` derive.
pub extern crate struct_path;

pub use arrow_array;
pub use arrow_schema;
pub use jiff;

use crate::errors::BigQueryError;

/// The result type of every fallible call in this crate.
pub type BigQueryResult<T> = std::result::Result<T, BigQueryError>;

#[cfg(doctest)]
mod book;
