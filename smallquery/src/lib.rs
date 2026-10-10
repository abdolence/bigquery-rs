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
mod runtime;
mod services;
mod storage;
mod table;
mod types;

pub use config::{ByteSize, EngineConfig, Maintenance, StorageOptions, TcpEndpoint};
pub use engine::{Engine, TcpServer};
pub use error::{BackendKind, EngineError, Resource};
pub use ids::{DatasetId, DatasetRef, JobId, Location, ProjectId, TableId, TableRef};
