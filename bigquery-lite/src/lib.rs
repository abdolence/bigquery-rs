//! A lightweight BigQuery engine: it serves the BigQuery v2 and Storage gRPC APIs that the
//! `bigquery` client calls, and answers them from Apache Iceberg tables under one root.
#![forbid(unsafe_code)]

mod catalog;
mod config;
mod error;
mod ids;
mod proto;
mod runtime;
mod storage;

pub use config::{ByteSize, EngineConfig, Maintenance, StorageOptions, TcpEndpoint};
pub use error::{BackendKind, EngineError, Resource};
pub use ids::{DatasetId, DatasetRef, JobId, Location, ProjectId, TableId, TableRef};
