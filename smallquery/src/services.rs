//! The gRPC services the engine registers: the four v2 services and the two Storage services
//! the `bigquery` client calls. A method the engine does not serve answers `Unimplemented`
//! naming the full method, which the generated default stubs would not.

mod datasets;
mod jobs;
mod policies;
mod storage_read;
mod storage_write;
mod tables;

use crate::catalog::EngineCatalog;
use crate::engine::EngineState;
use crate::error::EngineError;
use crate::proto::bigquery_v2::dataset_service_server::DatasetServiceServer;
use crate::proto::bigquery_v2::job_service_server::JobServiceServer;
use crate::proto::bigquery_v2::row_access_policy_service_server::RowAccessPolicyServiceServer;
use crate::proto::bigquery_v2::table_service_server::TableServiceServer;
use crate::proto::storage_v1::big_query_read_server::BigQueryReadServer;
use crate::proto::storage_v1::big_query_write_server::BigQueryWriteServer;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use tonic::service::Routes;
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn, Span};

/// The largest request the services decode: inline query results and Storage Write requests
/// are the large ones, and tonic's 4 MiB default is below both.
const MAX_DECODING_MESSAGE_SIZE: usize = 128 * 1024 * 1024;

/// How a call reached the engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Route {
    InProcess,
    Tcp,
}

impl Display for Route {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Route::InProcess => "in_process",
            Route::Tcp => "tcp",
        })
    }
}

/// What every service of one listener shares.
#[derive(Debug, Clone)]
pub(crate) struct Services {
    state: Arc<EngineState>,
    route: Route,
}

impl Services {
    pub(crate) fn new(state: Arc<EngineState>, route: Route) -> Self {
        Self { state, route }
    }

    fn catalog(&self) -> &Arc<EngineCatalog> {
        &self.state.catalog
    }

    /// Every registered service, ready for a tonic server.
    pub(crate) fn routes(&self) -> Routes {
        Routes::new(
            JobServiceServer::new(jobs::Jobs(self.clone()))
                .max_decoding_message_size(MAX_DECODING_MESSAGE_SIZE),
        )
        .add_service(
            TableServiceServer::new(tables::Tables(self.clone()))
                .max_decoding_message_size(MAX_DECODING_MESSAGE_SIZE),
        )
        .add_service(
            DatasetServiceServer::new(datasets::Datasets(self.clone()))
                .max_decoding_message_size(MAX_DECODING_MESSAGE_SIZE),
        )
        .add_service(
            RowAccessPolicyServiceServer::new(policies::RowAccessPolicies(self.clone()))
                .max_decoding_message_size(MAX_DECODING_MESSAGE_SIZE),
        )
        .add_service(
            BigQueryReadServer::new(storage_read::StorageRead(self.clone()))
                .max_decoding_message_size(MAX_DECODING_MESSAGE_SIZE),
        )
        .add_service(
            BigQueryWriteServer::new(storage_write::StorageWrite(self.clone()))
                .max_decoding_message_size(MAX_DECODING_MESSAGE_SIZE),
        )
    }

    /// The span of one served call. Its name is generic, since a span name is static; the
    /// method is the `otel.name` field, which tracing exporters use as the span's name.
    fn span(&self, method: &'static str, project: &str) -> Span {
        tracing::info_span!(
            "smallquery.rpc",
            otel.name = method,
            smallquery.route = %self.route,
            smallquery.auth = "none",
            smallquery.project = project,
        )
    }
}

/// The answer to a served call, logging a refusal at `info`, since a production log of them
/// shows what a workload needs next, and an engine failure at `warn`.
fn respond<T>(method: &str, result: Result<T, EngineError>) -> Result<Response<T>, Status> {
    match result {
        Ok(value) => Ok(Response::new(value)),
        Err(err) => {
            match &err {
                EngineError::Unsupported { construct, .. } => {
                    info!(
                        method,
                        construct, "Refused a construct the engine does not implement."
                    );
                }
                EngineError::Storage { .. }
                | EngineError::Io(_)
                | EngineError::Iceberg(_)
                | EngineError::CommitOutcomeUnknown { .. } => {
                    warn!(method, %err, "A call failed in the engine.");
                }
                _ => debug!(method, %err, "A call failed."),
            }
            Err(Status::from(err))
        }
    }
}

/// The `Unimplemented` answer of a method the engine does not serve, naming it in full.
fn unimplemented<T>(method: &str) -> Result<T, Status> {
    info!(method, "Refused a method the engine does not serve.");
    Err(Status::from(EngineError::unsupported(
        method,
        "the engine does not serve this method",
    )))
}

/// The `if-match` precondition of a write, as the client sends the etag a read returned.
fn if_match<T>(request: &Request<T>) -> Option<String> {
    request
        .metadata()
        .get("if-match")
        .and_then(|value| value.to_str().ok())
        .map(|etag| etag.trim_matches('"').to_string())
}

/// The project a Storage API name names, `projects/{project}/…`, for a call's span.
fn project_of(name: &str) -> &str {
    name.strip_prefix("projects/")
        .and_then(|rest| rest.split('/').next())
        .unwrap_or_default()
}

/// A protobuf timestamp of `time`, which is never before the Unix epoch on a sane clock and is
/// reported as the epoch if it is.
pub(crate) fn timestamp(time: std::time::SystemTime) -> gcloud_sdk::prost_types::Timestamp {
    let since = time
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    gcloud_sdk::prost_types::Timestamp {
        seconds: i64::try_from(since.as_secs()).unwrap_or(i64::MAX),
        nanos: i32::try_from(since.subsec_nanos()).unwrap_or(0),
    }
}

/// One page of `items` from the offset `page_token` names, and the token of the next page,
/// empty after the last one.
pub(crate) fn page<T>(
    items: Vec<T>,
    max_results: Option<u32>,
    page_token: &str,
) -> Result<(Vec<T>, String), EngineError> {
    let start = match page_token {
        "" => 0,
        token => token.parse::<usize>().map_err(|_| {
            EngineError::invalid("page_token", format!("{token:?} is not a page token"))
        })?,
    };
    let size = max_results
        .filter(|n| *n > 0)
        .map_or(usize::MAX, |n| usize::try_from(n).unwrap_or(usize::MAX));
    let total = items.len();
    let page: Vec<T> = items.into_iter().skip(start).take(size).collect();
    let next = start.saturating_add(page.len());
    let token = if next < total {
        next.to_string()
    } else {
        String::new()
    };
    Ok((page, token))
}
