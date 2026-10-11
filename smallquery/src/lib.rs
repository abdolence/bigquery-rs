//! A lightweight BigQuery engine: it serves the BigQuery v2 and Storage gRPC APIs that the
//! `bigquery` client calls, and answers them from Apache Iceberg tables under one root.
//!
//! An application opens an [`Engine`] over a root directory and reaches it in process through
//! [`Engine::channel`], or over loopback TCP through [`Engine::serve_tcp`] with the client's
//! URL options pointing at the listener.
#![forbid(unsafe_code)]

mod catalog;
mod config;
mod engine;
mod error;
mod ids;
mod proto;
mod query;
mod read;
mod runtime;
mod services;
mod sql;
mod storage;
mod table;
#[cfg(test)]
mod testkit;
mod types;
mod write;

pub use config::{ByteSize, EngineConfig, Maintenance, StorageOptions, TcpEndpoint};
pub use engine::{Engine, TcpServer};
pub use error::{BackendKind, EngineError, JobFailureReason, Resource};
pub use ids::{DatasetId, DatasetRef, JobId, Location, ProjectId, TableId, TableRef};
