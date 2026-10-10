//! A lightweight BigQuery engine: it serves the BigQuery v2 and Storage gRPC APIs that the
//! `bigquery` client calls, and answers them from Apache Iceberg tables under one root.
#![forbid(unsafe_code)]
