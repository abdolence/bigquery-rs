//! Tables: get, delete and list, through the v2 `TableService`. Tables are created and changed
//! by the declarative schema API.

use crate::admin::{non_empty, paged, timestamp_ms};
use crate::errors::BigQueryError;
use crate::schema::table_partitioning;
use crate::{
    BigQueryDatasetRef, BigQueryDb, BigQueryPartitioning, BigQueryResult, BigQueryTableRef,
    BigQueryTableSchema,
};
use futures::stream::BoxStream;
use gcloud_sdk::google::cloud::bigquery::v2;
use gcloud_sdk::tonic::metadata::MetadataMap;
use std::collections::BTreeMap;
use tracing::Span;

/// What kind of table a table is.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BigQueryTableType {
    /// A table that stores its rows.
    Table,
    /// A logical view.
    View,
    /// A materialized view.
    MaterializedView,
    /// A table over data outside BigQuery.
    External,
    /// A table snapshot.
    Snapshot,
    /// A type this crate does not know, as BigQuery named it.
    Unrecognised(String),
}

impl From<String> for BigQueryTableType {
    fn from(name: String) -> Self {
        match name.as_str() {
            "TABLE" => Self::Table,
            "VIEW" => Self::View,
            "MATERIALIZED_VIEW" => Self::MaterializedView,
            "EXTERNAL" => Self::External,
            "SNAPSHOT" => Self::Snapshot,
            _ => Self::Unrecognised(name),
        }
    }
}

/// A table as `GetTable` returns it.
#[derive(Debug, Clone, PartialEq)]
pub struct BigQueryTable {
    /// The table, with its project.
    pub reference: BigQueryTableRef,
    /// The kind of table.
    pub table_type: Option<BigQueryTableType>,
    /// The columns, with the v2 API's legacy type names normalised, so it compares with `==`
    /// against a schema from any other source.
    pub schema: BigQueryTableSchema,
    /// The description.
    pub description: Option<String>,
    /// The labels.
    pub labels: BTreeMap<String, String>,
    /// The partitioning.
    pub partitioning: Option<BigQueryPartitioning>,
    /// The clustering columns, most important first.
    pub clustering: Vec<String>,
    /// The rows, as of BigQuery's last count. It lags Storage Write: rows written through
    /// COMMITTED or PENDING streams show after 60 to 70 seconds, default-stream rows can take
    /// more than 5 minutes.
    pub num_rows: Option<u64>,
    /// The logical bytes, with the same lag as `num_rows`.
    pub num_bytes: Option<i64>,
    /// Where the table is stored.
    pub location: Option<String>,
    /// When the table was created.
    pub creation_time: Option<jiff::Timestamp>,
    /// When the table was last changed.
    pub last_modified_time: Option<jiff::Timestamp>,
    /// When BigQuery deletes the table.
    pub expiration_time: Option<jiff::Timestamp>,
}

/// A table as `ListTables` returns it, without its schema and sizes: read one with
/// [`BigQueryTableSchemaBuilder::get`](crate::BigQueryTableSchemaBuilder::get) for those.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BigQueryTableSummary {
    /// The table, with its project.
    pub reference: BigQueryTableRef,
    /// The kind of table.
    pub table_type: Option<BigQueryTableType>,
    /// The labels.
    pub labels: BTreeMap<String, String>,
    /// When the table was created.
    pub creation_time: Option<jiff::Timestamp>,
    /// When BigQuery deletes the table.
    pub expiration_time: Option<jiff::Timestamp>,
}

fn table_reference(reference: Option<v2::TableReference>) -> BigQueryResult<BigQueryTableRef> {
    reference
        .ok_or_else(|| {
            BigQueryError::invalid_parameters("table_reference", "BigQuery returned none")
        })?
        .try_into()
}

/// # Errors
/// [`BigQueryError::InvalidParametersError`] for a missing or invalid table reference or
/// partitioning the crate does not model, and [`BigQueryError::DeserializeError`] for a column
/// type the crate does not handle or a time out of range.
impl TryFrom<v2::Table> for BigQueryTable {
    type Error = BigQueryError;

    fn try_from(table: v2::Table) -> Result<Self, Self::Error> {
        let (partitioning, _) = table_partitioning(&table)?;
        let schema = table
            .schema
            .as_ref()
            .map(BigQueryTableSchema::try_from)
            .transpose()?
            .unwrap_or(BigQueryTableSchema { fields: Vec::new() });
        // Past i64::MAX it is out of range for a timestamp too, and fails as one.
        let last_modified_time = i64::try_from(table.last_modified_time).unwrap_or(i64::MAX);
        Ok(Self {
            reference: table_reference(table.table_reference)?,
            table_type: non_empty(table.r#type).map(Into::into),
            schema,
            description: table.description.and_then(non_empty),
            labels: table.labels.into_iter().collect(),
            partitioning,
            clustering: table.clustering.map(|c| c.fields).unwrap_or_default(),
            num_rows: table.num_rows,
            num_bytes: table.num_bytes,
            location: non_empty(table.location),
            creation_time: timestamp_ms("creation_time", table.creation_time)?,
            last_modified_time: timestamp_ms("last_modified_time", last_modified_time)?,
            expiration_time: timestamp_ms("expiration_time", table.expiration_time.unwrap_or(0))?,
        })
    }
}

/// # Errors
/// [`BigQueryError::InvalidParametersError`] for a missing or invalid table reference, and
/// [`BigQueryError::DeserializeError`] for a time out of range.
impl TryFrom<v2::ListFormatTable> for BigQueryTableSummary {
    type Error = BigQueryError;

    fn try_from(table: v2::ListFormatTable) -> Result<Self, Self::Error> {
        Ok(Self {
            reference: table_reference(table.table_reference)?,
            table_type: non_empty(table.r#type).map(Into::into),
            labels: table.labels.into_iter().collect(),
            creation_time: timestamp_ms("creation_time", table.creation_time)?,
            expiration_time: timestamp_ms("expiration_time", table.expiration_time)?,
        })
    }
}

impl BigQueryDb {
    fn table_request_ids(&self, table: &BigQueryTableRef) -> (String, String, String) {
        (
            table
                .project()
                .unwrap_or(&self.options().google_project_id)
                .to_string(),
            table.dataset().to_string(),
            table.table().to_string(),
        )
    }

    pub(crate) async fn get_table(
        &self,
        table: &BigQueryTableRef,
    ) -> BigQueryResult<BigQueryTable> {
        let (project_id, dataset_id, table_id) = self.table_request_ids(table);
        let request = v2::GetTableRequest {
            project_id,
            dataset_id,
            table_id,
            ..Default::default()
        };
        self.retry(
            &table_span(table),
            "get a table",
            &request,
            &MetadataMap::new(),
            |r| {
                let mut client = self.table_client();
                async move { client.get_table(r).await }
            },
        )
        .await?
        .try_into()
    }

    pub(crate) async fn delete_table(&self, table: &BigQueryTableRef) -> BigQueryResult<()> {
        let (project_id, dataset_id, table_id) = self.table_request_ids(table);
        let request = v2::DeleteTableRequest {
            project_id,
            dataset_id,
            table_id,
        };
        self.retry(
            &table_span(table),
            "delete a table",
            &request,
            &MetadataMap::new(),
            |r| {
                let mut client = self.table_client();
                async move { client.delete_table(r).await }
            },
        )
        .await
    }

    pub(crate) fn list_tables(
        &self,
        dataset: &BigQueryDatasetRef,
        page_size: Option<u32>,
    ) -> BoxStream<'static, BigQueryResult<BigQueryTableSummary>> {
        let db = self.clone();
        let project_id = dataset
            .project()
            .unwrap_or(&self.options().google_project_id)
            .to_string();
        let dataset_id = dataset.dataset().to_string();
        let span = tracing::debug_span!("BigQuery tables", "/bigquery/dataset" = %dataset);
        paged(move |page_token| {
            let db = db.clone();
            let span = span.clone();
            let request = v2::ListTablesRequest {
                project_id: project_id.clone(),
                dataset_id: dataset_id.clone(),
                max_results: page_size,
                page_token,
            };
            async move {
                let page = db
                    .retry(&span, "list tables", &request, &MetadataMap::new(), |r| {
                        let mut client = db.table_client();
                        async move { client.list_tables(r).await }
                    })
                    .await?;
                let tables = page
                    .tables
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<BigQueryResult<_>>()?;
                Ok((tables, page.next_page_token))
            }
        })
    }
}

fn table_span(table: &BigQueryTableRef) -> Span {
    tracing::debug_span!("BigQuery table", "/bigquery/table" = %table)
}
