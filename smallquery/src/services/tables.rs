//! `google.cloud.bigquery.v2.TableService`.

use super::{if_match, page, respond, Services};
use crate::catalog::{Change, LoadedTable};
use crate::error::{EngineError, Resource};
use crate::ids::{DatasetRef, TableRef};
use crate::proto::bigquery_v2::table_service_server::TableService;
use crate::table::{TableDefinition, TableView};
use gcloud_sdk::google::cloud::bigquery::v2;
use tonic::{Request, Response, Status};
use tracing::Instrument;

pub(crate) struct Tables(pub(super) Services);

/// How a write replaces a table's definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Write {
    /// `PatchTable`: what the body sets changes, labels merge.
    Patch,
    /// `UpdateTable`: the body is the whole definition.
    Update,
}

impl Tables {
    /// The table as `GetTable` reports it, in its dataset's location.
    async fn report(
        &self,
        table: &TableRef,
        loaded: &LoadedTable,
        view: TableView,
    ) -> Result<v2::Table, EngineError> {
        let dataset = self.0.catalog().dataset(&table.dataset_ref()).await?;
        loaded.bigquery_table(table, &dataset.location, view)
    }

    async fn get(&self, request: v2::GetTableRequest) -> Result<v2::Table, EngineError> {
        let table = TableRef::parse(&request.project_id, &request.dataset_id, &request.table_id)?;
        let view = match v2::get_table_request::TableMetadataView::try_from(request.view) {
            Ok(v2::get_table_request::TableMetadataView::Basic) => TableView::Basic,
            _ => TableView::Full,
        };
        let loaded = self.0.catalog().load(&table).await?;
        self.report(&table, &loaded, view).await
    }

    async fn insert(&self, request: v2::InsertTableRequest) -> Result<v2::Table, EngineError> {
        let body = request
            .table
            .ok_or_else(|| EngineError::invalid("table", "the request carries no table"))?;
        let reference = body.table_reference.clone().unwrap_or_default();
        for (field, id, requested) in [
            ("project_id", &reference.project_id, &request.project_id),
            ("dataset_id", &reference.dataset_id, &request.dataset_id),
        ] {
            if !id.is_empty() && id != requested {
                return Err(EngineError::invalid(
                    format!("table_reference.{field}"),
                    format!("{id:?} is not the request's {requested:?}"),
                ));
            }
        }
        let table = TableRef::parse(
            &request.project_id,
            &request.dataset_id,
            &reference.table_id,
        )?;
        let definition = TableDefinition::try_from(&body)?;
        let location = self.0.catalog().table_location(&table);
        let metadata = definition.metadata(&table, location, crate::catalog::now_ms())?;
        let loaded = self.0.catalog().create_table(&table, metadata).await?;
        self.report(&table, &loaded, TableView::Full).await
    }

    /// Changes the table as `body` says, when its etag is still `etag`: on a retry after a lost
    /// race the reloaded table has another etag, which fails the precondition as it should.
    async fn write(
        &self,
        request: v2::UpdateOrPatchTableRequest,
        etag: Option<String>,
        write: Write,
    ) -> Result<v2::Table, EngineError> {
        let table = TableRef::parse(&request.project_id, &request.dataset_id, &request.table_id)?;
        if request.autodetect_schema {
            return Err(EngineError::unsupported(
                "autodetect_schema",
                "set on a table write",
            ));
        }
        let body = request
            .table
            .ok_or_else(|| EngineError::invalid("table", "the request carries no table"))?;
        let loaded = self
            .0
            .catalog()
            .commit(&table, |loaded| {
                let current = loaded.version().to_string();
                if let Some(etag) = etag.as_ref().filter(|etag| **etag != current) {
                    return Err(EngineError::Precondition {
                        what: Resource::Table(table.clone()),
                        detail: format!(
                            "the etag {etag:?} is not the table's current etag {current:?}"
                        ),
                    });
                }
                let existing = TableDefinition::try_from(loaded.metadata())?;
                let changed = match write {
                    Write::Patch => existing.patched(&body)?,
                    Write::Update => TableDefinition::try_from(&body)?,
                };
                existing.check_change(&changed)?;
                Ok(Change::Engine(
                    existing.commit(&changed, loaded.metadata())?,
                ))
            })
            .await?;
        self.report(&table, &loaded, TableView::Full).await
    }

    async fn list(&self, request: v2::ListTablesRequest) -> Result<v2::TableList, EngineError> {
        let dataset = DatasetRef::parse(&request.project_id, &request.dataset_id)?;
        self.0.catalog().dataset(&dataset).await?;
        let tables = self
            .0
            .catalog()
            .tables(&dataset)
            .await?
            .into_iter()
            .map(|(id, loaded)| loaded.list_format(&dataset.table(id)))
            .collect::<Result<Vec<_>, _>>()?;
        let total = i32::try_from(tables.len()).ok();
        let (tables, next_page_token) = page(tables, request.max_results, &request.page_token)?;
        Ok(v2::TableList {
            kind: "bigquery#tableList".to_string(),
            next_page_token,
            tables,
            total_items: total,
            ..Default::default()
        })
    }
}

const GET: &str = "google.cloud.bigquery.v2.TableService/GetTable";
const INSERT: &str = "google.cloud.bigquery.v2.TableService/InsertTable";
const PATCH: &str = "google.cloud.bigquery.v2.TableService/PatchTable";
const UPDATE: &str = "google.cloud.bigquery.v2.TableService/UpdateTable";
const DELETE: &str = "google.cloud.bigquery.v2.TableService/DeleteTable";
const LIST: &str = "google.cloud.bigquery.v2.TableService/ListTables";

#[tonic::async_trait]
impl TableService for Tables {
    async fn get_table(
        &self,
        request: Request<v2::GetTableRequest>,
    ) -> Result<Response<v2::Table>, Status> {
        let request = request.into_inner();
        let span = self.0.span(GET, &request.project_id);
        respond(GET, self.get(request).instrument(span).await)
    }

    async fn insert_table(
        &self,
        request: Request<v2::InsertTableRequest>,
    ) -> Result<Response<v2::Table>, Status> {
        let request = request.into_inner();
        let span = self.0.span(INSERT, &request.project_id);
        respond(INSERT, self.insert(request).instrument(span).await)
    }

    async fn patch_table(
        &self,
        request: Request<v2::UpdateOrPatchTableRequest>,
    ) -> Result<Response<v2::Table>, Status> {
        let etag = if_match(&request);
        let request = request.into_inner();
        let span = self.0.span(PATCH, &request.project_id);
        respond(
            PATCH,
            self.write(request, etag, Write::Patch)
                .instrument(span)
                .await,
        )
    }

    async fn update_table(
        &self,
        request: Request<v2::UpdateOrPatchTableRequest>,
    ) -> Result<Response<v2::Table>, Status> {
        let etag = if_match(&request);
        let request = request.into_inner();
        let span = self.0.span(UPDATE, &request.project_id);
        respond(
            UPDATE,
            self.write(request, etag, Write::Update)
                .instrument(span)
                .await,
        )
    }

    async fn delete_table(
        &self,
        request: Request<v2::DeleteTableRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        let span = self.0.span(DELETE, &request.project_id);
        let result = async {
            let table =
                TableRef::parse(&request.project_id, &request.dataset_id, &request.table_id)?;
            self.0.catalog().purge(&table).await
        };
        respond(DELETE, result.instrument(span).await)
    }

    async fn list_tables(
        &self,
        request: Request<v2::ListTablesRequest>,
    ) -> Result<Response<v2::TableList>, Status> {
        let request = request.into_inner();
        let span = self.0.span(LIST, &request.project_id);
        respond(LIST, self.list(request).instrument(span).await)
    }
}
