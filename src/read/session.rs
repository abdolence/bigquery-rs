//! Opening a Storage Read session: the request, the projection and its fallback.

use crate::errors::{
    BigQueryError, BigQueryErrorPublicGenericDetails, BigQuerySchemaMismatchError,
};
use crate::read::projection::intersect;
use crate::{
    BigQueryDb, BigQueryReadCompression, BigQueryReadParams, BigQueryResult, BigQueryTableRef,
};
use gcloud_sdk::google::cloud::bigquery::storage::v1::arrow_serialization_options::CompressionCodec;
use gcloud_sdk::google::cloud::bigquery::storage::v1::read_session::table_read_options::OutputFormatSerializationOptions;
use gcloud_sdk::google::cloud::bigquery::storage::v1::read_session::{
    Schema, TableModifiers, TableReadOptions,
};
use gcloud_sdk::google::cloud::bigquery::storage::v1::{
    ArrowSerializationOptions, CreateReadSessionRequest, DataFormat, ReadSession,
};
use gcloud_sdk::google::cloud::bigquery::v2::get_table_request::TableMetadataView;
use gcloud_sdk::google::cloud::bigquery::v2::GetTableRequest;
use gcloud_sdk::tonic::metadata::MetadataMap;
use tracing::{warn, Span};

/// What BigQuery's `CreateReadSession` says when `selected_fields` names a column it does not
/// know, including for about 30 seconds after the column was added or renamed.
const SELECTED_FIELDS_DO_NOT_EXIST: &str = "selected fields do not exist";

/// An opened session: the Arrow schema message every stream starts from, the streams, and
/// BigQuery's estimates of what reading them all scans.
pub(crate) struct OpenedSession {
    pub schema: Vec<u8>,
    pub streams: Vec<String>,
    pub estimated_bytes_scanned: i64,
    pub estimated_rows: i64,
}

/// How the columns of a session are chosen.
pub(crate) enum Projection {
    /// Every column.
    All,
    /// The target type's top-level field names, intersected with the table's columns, and
    /// every column if the session does not know one of them yet.
    Auto(&'static [&'static str]),
}

/// Opens one session for `params`. Columns given in `params.selected_fields` are sent as they
/// are, and a name the session does not know is a `SchemaMismatchError`; otherwise
/// `projection` decides.
pub(crate) async fn open_session(
    db: &BigQueryDb,
    params: &BigQueryReadParams,
    projection: Projection,
    span: &Span,
) -> BigQueryResult<OpenedSession> {
    validate(params)?;
    if let Some(fields) = &params.selected_fields {
        return create(db, params, fields.clone(), span)
            .await
            .map_err(|err| match err {
                err if selected_fields_lag(&err) => schema_mismatch(&params.table, &err),
                err => err,
            });
    }
    let Projection::Auto(fields) = projection else {
        return create(db, params, Vec::new(), span).await;
    };
    let columns = table_columns(db, &params.table, span).await?;
    let Some(selected) = intersect(fields, &columns) else {
        return create(db, params, Vec::new(), span).await;
    };
    match create(db, params, selected, span).await {
        Err(err) if selected_fields_lag(&err) => {
            span.in_scope(|| {
                warn!(
                    %err,
                    table = %params.table,
                    "The read session does not know a selected column yet, which happens for \
                     about 30 s after a schema change. Reading every column instead.",
                );
            });
            create(db, params, Vec::new(), span).await
        }
        other => other,
    }
}

fn validate(params: &BigQueryReadParams) -> BigQueryResult<()> {
    if let Some(p) = params.sample_percentage {
        if !(p.is_finite() && p > 0.0 && p <= 100.0) {
            return Err(BigQueryError::invalid_parameters(
                "sample_percentage",
                format!("{p} is not a percentage above 0 and up to 100"),
            ));
        }
    }
    Ok(())
}

fn selected_fields_lag(err: &BigQueryError) -> bool {
    matches!(err, BigQueryError::DatabaseError(e)
        if e.public.code == "InvalidArgument" && e.details.contains(SELECTED_FIELDS_DO_NOT_EXIST))
}

fn schema_mismatch(table: &BigQueryTableRef, err: &BigQueryError) -> BigQueryError {
    let details = match err {
        BigQueryError::DatabaseError(e) => e.details.clone(),
        other => other.to_string(),
    };
    BigQueryError::SchemaMismatchError(BigQuerySchemaMismatchError::new(
        BigQueryErrorPublicGenericDetails::new("SCHEMA_MISMATCH".into()),
        table.clone(),
        details,
    ))
}

/// The table's top-level column names, from one `GetTable`.
async fn table_columns(
    db: &BigQueryDb,
    table: &BigQueryTableRef,
    span: &Span,
) -> BigQueryResult<Vec<String>> {
    let request = GetTableRequest {
        project_id: table
            .project()
            .unwrap_or(&db.options().google_project_id)
            .to_string(),
        dataset_id: table.dataset().to_string(),
        table_id: table.table().to_string(),
        view: TableMetadataView::Basic.into(),
        ..Default::default()
    };
    let table = db
        .retry(span, "get the table", &request, &MetadataMap::new(), |r| {
            let mut client = db.table_client();
            async move { client.get_table(r).await }
        })
        .await?;
    Ok(table
        .schema
        .map(|schema| schema.fields.into_iter().map(|f| f.name).collect())
        .unwrap_or_default())
}

fn compression_codec(compression: BigQueryReadCompression) -> CompressionCodec {
    match compression {
        BigQueryReadCompression::None => CompressionCodec::CompressionUnspecified,
        BigQueryReadCompression::Lz4 => CompressionCodec::Lz4Frame,
        BigQueryReadCompression::Zstd => CompressionCodec::Zstd,
    }
}

fn proto_timestamp(t: jiff::Timestamp) -> gcloud_sdk::prost_types::Timestamp {
    let (mut seconds, mut nanos) = (t.as_second(), t.subsec_nanosecond());
    // protobuf's nanos are never negative; jiff's take the sign of the timestamp.
    if nanos < 0 {
        seconds -= 1;
        nanos += 1_000_000_000;
    }
    gcloud_sdk::prost_types::Timestamp { seconds, nanos }
}

/// The stream count asked for when the caller sets none: one per core, since each stream runs
/// on its own task.
fn default_max_stream_count() -> u32 {
    std::thread::available_parallelism()
        .ok()
        .and_then(|n| u32::try_from(n.get()).ok())
        .unwrap_or(1)
}

async fn create(
    db: &BigQueryDb,
    params: &BigQueryReadParams,
    selected_fields: Vec<String>,
    span: &Span,
) -> BigQueryResult<OpenedSession> {
    let project_id = &db.options().google_project_id;
    let options = &params.options;
    let max_stream_count = options
        .max_stream_count
        .unwrap_or_else(default_max_stream_count);
    let request = CreateReadSessionRequest {
        parent: format!("projects/{project_id}"),
        read_session: Some(ReadSession {
            table: params.table.table_path(project_id),
            data_format: DataFormat::Arrow.into(),
            table_modifiers: params.snapshot_time.map(|t| TableModifiers {
                snapshot_time: Some(proto_timestamp(t)),
            }),
            read_options: Some(TableReadOptions {
                selected_fields,
                row_restriction: params.row_restriction.clone().unwrap_or_default(),
                sample_percentage: params.sample_percentage,
                output_format_serialization_options: Some(
                    OutputFormatSerializationOptions::ArrowSerializationOptions(
                        ArrowSerializationOptions {
                            buffer_compression: compression_codec(options.compression).into(),
                            ..Default::default()
                        },
                    ),
                ),
                ..Default::default()
            }),
            ..Default::default()
        }),
        max_stream_count: i32::try_from(max_stream_count).unwrap_or(i32::MAX),
        preferred_min_stream_count: options
            .preferred_min_stream_count
            .map_or(0, |n| i32::try_from(n).unwrap_or(i32::MAX)),
    };
    let session = db
        .retry(
            span,
            "create a read session",
            &request,
            &MetadataMap::new(),
            |r| {
                let mut client = db.read_client();
                async move { client.create_read_session(r).await }
            },
        )
        .await?;
    let schema = match session.schema {
        Some(Schema::ArrowSchema(schema)) => schema.serialized_schema,
        Some(Schema::AvroSchema(_)) => {
            return Err(BigQueryError::invalid_parameters(
                "data_format",
                "The read session answered with an Avro schema to an Arrow request",
            ))
        }
        None => Vec::new(),
    };
    Ok(OpenedSession {
        schema,
        streams: session.streams.into_iter().map(|s| s.name).collect(),
        estimated_bytes_scanned: session.estimated_total_bytes_scanned,
        estimated_rows: session.estimated_row_count,
    })
}
