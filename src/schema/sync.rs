//! `.plan()` and `.sync()` against BigQuery: one `GetTable`, the pure plan, then the writes in
//! the plan's order.

use crate::errors::{BigQueryDataConflictError, BigQueryError, BigQuerySchemaChangeRefusedError};
use crate::schema::ddl::{
    create_sql, drop_and_create_sql, drop_sql, rename_sql, snapshot_sql, table_sql, widen_sql,
};
use crate::schema::diff::{defaults_body, patch_body, plan_table, update_body};
use crate::schema::live::LiveTable;
use crate::schema::plan::ChangeStep;
use crate::{
    BigQueryDatasetRef, BigQueryDb, BigQueryDroppedData, BigQueryPartitioning, BigQueryQueryParams,
    BigQueryQuerySupport, BigQueryRecreate, BigQueryRecreateMethod, BigQueryResult,
    BigQuerySchemaChange, BigQuerySchemaSupport, BigQueryTableDeclaration, BigQueryTableId,
    BigQueryTablePlan, BigQueryTableRef, BigQueryTableSyncReport, BigQueryTableTarget,
};
use async_trait::async_trait;
use gcloud_sdk::google::cloud::bigquery::v2;
use gcloud_sdk::tonic::metadata::{MetadataMap, MetadataValue};
use std::time::Duration;
use tracing::{info, warn, Span};

/// Writes to one table that go out back to back; later ones wait [`PACE`] each. BigQuery's
/// per-table update limit took 5 DDL statements and 7 or 8 patches in a burst (probe 8a to
/// 8c), and both count against one quota.
const BURST: usize = 5;
const PACE: Duration = Duration::from_millis(2200);

struct Pacer {
    sent: usize,
}

impl Pacer {
    async fn next(&mut self) {
        if self.sent >= BURST {
            tokio::time::sleep(PACE).await;
        }
        self.sent += 1;
    }
}

fn schema_span(table: &BigQueryTableRef) -> Span {
    tracing::debug_span!("BigQuery schema", "/bigquery/table" = %table)
}

/// The project, dataset and table IDs of a resolved table, as the v2 requests take them.
struct TableIds {
    project: String,
    dataset: String,
    table: String,
}

impl BigQueryDb {
    /// `table` with the client's project filled in.
    fn resolved(&self, table: &BigQueryTableRef) -> BigQueryResult<BigQueryTableRef> {
        let project = table
            .project()
            .unwrap_or(&self.options().google_project_id)
            .to_string();
        Ok(BigQueryDatasetRef::new(project, table.dataset().clone())?.table(table.table().clone()))
    }

    fn ids(&self, table: &BigQueryTableRef) -> TableIds {
        TableIds {
            project: table
                .project()
                .unwrap_or(&self.options().google_project_id)
                .to_string(),
            dataset: table.dataset().to_string(),
            table: table.table().to_string(),
        }
    }

    async fn get_live_table(
        &self,
        table: &BigQueryTableRef,
        span: &Span,
    ) -> BigQueryResult<Option<LiveTable>> {
        let ids = self.ids(table);
        let request = v2::GetTableRequest {
            project_id: ids.project,
            dataset_id: ids.dataset,
            table_id: ids.table,
            ..Default::default()
        };
        let result = self
            .retry(span, "get a table", &request, &MetadataMap::new(), |r| {
                let mut client = self.table_client();
                async move { client.get_table(r).await }
            })
            .await;
        match result {
            Ok(table) => Ok(Some(LiveTable::try_from(table)?)),
            Err(BigQueryError::DataNotFoundError(_)) => Ok(None),
            Err(err) => Err(err),
        }
    }

    async fn row_access_policies(
        &self,
        table: &BigQueryTableRef,
        span: &Span,
    ) -> BigQueryResult<Vec<String>> {
        let ids = self.ids(table);
        let mut policies = Vec::new();
        let mut page_token = String::new();
        loop {
            let request = v2::ListRowAccessPoliciesRequest {
                project_id: ids.project.clone(),
                dataset_id: ids.dataset.clone(),
                table_id: ids.table.clone(),
                page_token: page_token.clone(),
                page_size: 0,
            };
            let page = self
                .retry(
                    span,
                    "list row access policies",
                    &request,
                    &MetadataMap::new(),
                    |r| {
                        let mut client = self.row_access_policy_client();
                        async move { client.list_row_access_policies(r).await }
                    },
                )
                .await?;
            policies.extend(
                page.row_access_policies
                    .into_iter()
                    .filter_map(|p| p.row_access_policy_reference.map(|r| r.policy_id)),
            );
            if page.next_page_token.is_empty() {
                return Ok(policies);
            }
            page_token = page.next_page_token;
        }
    }

    /// Reads the table and plans, listing the row access policies a recreate would drop.
    async fn planned(
        &self,
        declaration: &BigQueryTableDeclaration,
        span: &Span,
    ) -> BigQueryResult<(BigQueryTablePlan, Option<LiveTable>)> {
        let table = self.resolved(&declaration.table)?;
        let live = self.get_live_table(&table, span).await?;
        let mut plan = plan_table(declaration, table, live.as_ref());
        if let Some(recreate) = &mut plan.recreate {
            recreate.row_access_policies = self.row_access_policies(&plan.table, span).await?;
        }
        Ok((plan, live))
    }

    /// Sends a `PatchTable` or `UpdateTable` guarded by `etag`, and returns the table it wrote.
    ///
    /// A retry after a response lost in transit repeats the same precondition, so a write that
    /// did land the first time reads as a conflict; the next sync then finds nothing to do.
    async fn write_table(
        &self,
        table: &BigQueryTableRef,
        body: v2::Table,
        etag: &str,
        update: bool,
        span: &Span,
    ) -> BigQueryResult<v2::Table> {
        let ids = self.ids(table);
        let request = v2::UpdateOrPatchTableRequest {
            project_id: ids.project,
            dataset_id: ids.dataset,
            table_id: ids.table,
            table: Some(body),
            autodetect_schema: false,
        };
        let mut metadata = MetadataMap::new();
        let precondition = MetadataValue::try_from(etag).map_err(|_| {
            BigQueryError::invalid_parameters(
                "etag",
                format!("GetTable returned an etag that is not a valid header: {etag:?}"),
            )
        })?;
        metadata.insert("if-match", precondition);
        let action = if update {
            "update a table"
        } else {
            "patch a table"
        };
        let result = self
            .retry(span, action, &request, &metadata, |r| {
                let mut client = self.table_client();
                async move {
                    if update {
                        client.update_table(r).await
                    } else {
                        client.patch_table(r).await
                    }
                }
            })
            .await;
        match result {
            Err(BigQueryError::DatabaseError(err)) if err.public.code == "FailedPrecondition" => {
                Err(BigQueryError::DataConflictError(
                    BigQueryDataConflictError::new(
                        err.public,
                        format!(
                            "{table} changed since this sync read it (etag {etag}); nothing after \
                         this write was sent, run the sync again. {}",
                            err.details
                        ),
                    ),
                ))
            }
            other => other,
        }
    }

    async fn run_ddl(&self, sql: String) -> BigQueryResult<()> {
        self.execute_query(BigQueryQueryParams::new(sql))
            .await
            .map(|_| ())
    }

    async fn create_table(
        &self,
        table: &BigQueryTableRef,
        target: &BigQueryTableTarget,
        span: &Span,
    ) -> BigQueryResult<()> {
        let ids = self.ids(table);
        let request = v2::InsertTableRequest {
            project_id: ids.project.clone(),
            dataset_id: ids.dataset.clone(),
            table: Some(new_table(
                v2::TableReference {
                    project_id: ids.project,
                    dataset_id: ids.dataset,
                    table_id: ids.table,
                },
                target,
            )),
        };
        self.retry(span, "create a table", &request, &MetadataMap::new(), |r| {
            let mut client = self.table_client();
            async move { client.insert_table(r).await }
        })
        .await
        .map(|_| ())
    }

    async fn recreate(
        &self,
        table: &BigQueryTableRef,
        recreate: &BigQueryRecreate,
        report: &mut BigQueryTableSyncReport,
    ) -> BigQueryResult<()> {
        let project = &self.options().google_project_id;
        let table_text = table_sql(table, project);
        if recreate.snapshot_first {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_secs());
            let snapshot = BigQueryDatasetRef::new(
                table.project().unwrap_or(project),
                table.dataset().clone(),
            )?
            .table(BigQueryTableId::new(format!(
                "{}_snapshot_{now}",
                table.table()
            ))?);
            self.run_ddl(snapshot_sql(&table_sql(&snapshot, project), &table_text))
                .await?;
            info!(%table, %snapshot, "Took a snapshot before recreating the table.");
            report.snapshot = Some(snapshot);
        }
        let sql = match recreate.method {
            BigQueryRecreateMethod::CreateOrReplace => {
                create_sql(&table_text, &recreate.target, true)?
            }
            BigQueryRecreateMethod::DropAndCreate => {
                drop_and_create_sql(&table_text, &recreate.target)?
            }
        };
        self.run_ddl(sql).await?;
        if recreate.dangerous {
            warn!(
                %table,
                num_rows = recreate.num_rows,
                num_bytes = recreate.num_bytes,
                "Recreated the table and dropped every row it held.",
            );
            report.dropped_data.push(BigQueryDroppedData::Rows {
                num_rows: recreate.num_rows,
                num_bytes: recreate.num_bytes,
            });
        } else {
            info!(%table, "Recreated the empty table.");
        }
        report.recreated = Some(recreate.clone());
        Ok(())
    }

    async fn apply_in_place(
        &self,
        plan: &BigQueryTablePlan,
        live: LiveTable,
        report: &mut BigQueryTableSyncReport,
        span: &Span,
    ) -> BigQueryResult<()> {
        let table = &plan.table;
        let mut pacer = Pacer { sent: 0 };
        let mut latest = live.raw;
        if let Some(body) = patch_body(&latest, &plan.changes) {
            pacer.next().await;
            latest = self
                .write_table(table, body, &latest.etag.clone(), false, span)
                .await?;
            report.applied.extend(
                plan.changes
                    .iter()
                    .filter(|c| c.step() == ChangeStep::Patch)
                    .cloned(),
            );
            info!(%table, "Patched the table.");
        }
        if let Some(body) = defaults_body(&latest, &plan.changes) {
            pacer.next().await;
            latest = self
                .write_table(table, body, &latest.etag.clone(), false, span)
                .await?;
            info!(%table, "Set the default values of the added columns.");
        }
        if let Some(body) = update_body(&latest, &plan.changes) {
            pacer.next().await;
            self.write_table(table, body, &latest.etag.clone(), true, span)
                .await?;
            report.applied.extend(
                plan.changes
                    .iter()
                    .filter(|c| c.step() == ChangeStep::Update)
                    .cloned(),
            );
            info!(%table, "Removed undeclared labels or clustering.");
        }
        let table_text = table_sql(table, &self.options().google_project_id);
        for change in &plan.changes {
            let sql = match change {
                BigQuerySchemaChange::RenameColumn { from, to } => {
                    rename_sql(&table_text, from, to)
                }
                BigQuerySchemaChange::WidenColumn { column, to, .. } => {
                    widen_sql(&table_text, column, to)
                }
                BigQuerySchemaChange::DropColumn { column, .. } => drop_sql(&table_text, column),
                _ => continue,
            };
            pacer.next().await;
            self.run_ddl(sql).await?;
            info!(%table, %change, "Applied a DDL change.");
            if let BigQuerySchemaChange::DropColumn { column, .. } = change {
                warn!(%table, column, "Dropped a column and every value in it.");
                report.dropped_data.push(BigQueryDroppedData::Column {
                    column: column.clone(),
                });
            }
            report.applied.push(change.clone());
        }
        Ok(())
    }
}

/// The `InsertTable` body for `target` at `reference`.
fn new_table(reference: v2::TableReference, target: &BigQueryTableTarget) -> v2::Table {
    let (time_partitioning, range_partitioning) = match &target.partitioning {
        Some(BigQueryPartitioning::Time { unit, column }) => (
            Some(v2::TimePartitioning {
                r#type: unit.name().to_string(),
                expiration_ms: target.partition_expiration_ms,
                field: column.clone(),
            }),
            None,
        ),
        Some(BigQueryPartitioning::Range {
            column,
            start,
            end,
            interval,
        }) => (
            None,
            Some(v2::RangePartitioning {
                field: column.clone(),
                range: Some(v2::range_partitioning::Range {
                    start: start.to_string(),
                    end: end.to_string(),
                    interval: interval.to_string(),
                }),
            }),
        ),
        None => (None, None),
    };
    v2::Table {
        table_reference: Some(reference),
        description: target.description.clone(),
        labels: target.labels.clone().into_iter().collect(),
        schema: Some(v2::TableSchema {
            fields: target
                .columns
                .iter()
                .map(v2::TableFieldSchema::from)
                .collect(),
            ..Default::default()
        }),
        time_partitioning,
        range_partitioning,
        clustering: (!target.clustering.is_empty()).then(|| v2::Clustering {
            fields: target.clustering.clone(),
        }),
        table_constraints: target.primary_key.as_ref().map(|key| v2::TableConstraints {
            primary_key: Some(v2::PrimaryKey {
                columns: key.clone(),
            }),
            foreign_keys: Vec::new(),
        }),
        expiration_time: target.expiration_ms,
        ..Default::default()
    }
}

#[async_trait]
impl BigQuerySchemaSupport for BigQueryDb {
    async fn plan_table_schema(
        &self,
        declaration: BigQueryTableDeclaration,
    ) -> BigQueryResult<BigQueryTablePlan> {
        let span = schema_span(&declaration.table);
        let (plan, _) = self.planned(&declaration, &span).await?;
        Ok(plan)
    }

    async fn sync_table_schema(
        &self,
        declaration: BigQueryTableDeclaration,
    ) -> BigQueryResult<BigQueryTableSyncReport> {
        let span = schema_span(&declaration.table);
        let (plan, live) = self.planned(&declaration, &span).await?;
        if plan.refusal.is_some() {
            return Err(BigQueryError::SchemaChangeRefused(Box::new(
                BigQuerySchemaChangeRefusedError { plan },
            )));
        }
        let mut report = BigQueryTableSyncReport::new(plan.table.clone());
        report.withheld.clone_from(&plan.withheld);
        let result = match (&plan.create, &plan.recreate, live) {
            (Some(target), _, _) => {
                let created = self.create_table(&plan.table, target, &span).await;
                if created.is_ok() {
                    info!(table = %plan.table, "Created the table.");
                    report.created = Some(target.clone());
                }
                created
            }
            (None, Some(recreate), _) => self.recreate(&plan.table, recreate, &mut report).await,
            (None, None, Some(live)) => self.apply_in_place(&plan, live, &mut report, &span).await,
            (None, None, None) => Ok(()),
        };
        if let Err(err) = result {
            if report.created.is_some() || !report.applied.is_empty() || report.recreated.is_some()
            {
                warn!(%err, "The schema sync failed part way; these changes stay applied:\n{report}");
            }
            return Err(err);
        }
        Ok(report)
    }
}
