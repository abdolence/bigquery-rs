//! `.plan()` and `.sync()` against BigQuery: one `GetTable`, the pure plan, then the writes in
//! the plan's order.

use crate::db::if_match;
use crate::db::proto::millis;
use crate::db::TableIds;
use crate::errors::{BigQueryError, BigQuerySchemaChangeRefusedError};
use crate::schema::existing::ExistingTable;
use crate::schema::plan::ChangeStep;
use crate::{
    BigQueryDatasetRef, BigQueryDb, BigQueryDroppedData, BigQueryPartitioning, BigQueryQueryParams,
    BigQueryQuerySupport, BigQueryRecreate, BigQueryRecreateMethod, BigQueryResult,
    BigQuerySchemaChange, BigQueryTableDeclaration, BigQueryTableId, BigQueryTablePlan,
    BigQueryTableRef, BigQueryTableSyncReport, BigQueryTableTarget,
};
use gcloud_sdk::google::cloud::bigquery::v2;
use std::time::Duration;
use tracing::{info, warn, Span};

/// Writes to one table that go out back to back; later ones wait [`PACE`] each. BigQuery's
/// per-table update limit took 5 DDL statements and 7 or 8 patches in a burst, and both count
/// against one quota.
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

impl BigQueryTableRef {
    fn schema_span(&self) -> Span {
        tracing::debug_span!("BigQuery schema", "/bigquery/table" = %self)
    }
}

impl BigQueryDb {
    /// `table` with the client's project filled in.
    fn resolved(&self, table: &BigQueryTableRef) -> BigQueryResult<BigQueryTableRef> {
        let project = table.project_or(&self.options().google_project_id);
        Ok(BigQueryDatasetRef::new(project, table.dataset().clone())?.table(table.table().clone()))
    }

    fn ids(&self, table: &BigQueryTableRef) -> TableIds {
        table.ids(&self.options().google_project_id)
    }

    async fn get_existing_table(
        &self,
        table: &BigQueryTableRef,
        span: &Span,
    ) -> BigQueryResult<Option<ExistingTable>> {
        let ids = self.ids(table);
        let request = v2::GetTableRequest {
            project_id: ids.project,
            dataset_id: ids.dataset,
            table_id: ids.table,
            ..Default::default()
        };
        let result = self
            .retry(span, "get a table", &request, |r| {
                let mut client = self.table_client();
                async move { client.get_table(r).await }
            })
            .await;
        match result {
            Ok(table) => Ok(Some(ExistingTable::try_from(table)?)),
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
                .retry(span, "list row access policies", &request, |r| {
                    let mut client = self.row_access_policy_client();
                    async move { client.list_row_access_policies(r).await }
                })
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
    ) -> BigQueryResult<(BigQueryTablePlan, Option<ExistingTable>)> {
        let table = self.resolved(&declaration.table)?;
        let existing = self.get_existing_table(&table, span).await?;
        let mut plan = declaration.plan(table, existing.as_ref());
        if let Some(recreate) = &mut plan.recreate {
            recreate.row_access_policies = self.row_access_policies(&plan.table, span).await?;
        }
        Ok((plan, existing))
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
        let metadata = if_match("GetTable", etag)?;
        let action = if update {
            "update a table"
        } else {
            "patch a table"
        };
        self.retry_with_metadata(span, action, &request, &metadata, |r| {
            let mut client = self.table_client();
            async move {
                if update {
                    client.update_table(r).await
                } else {
                    client.patch_table(r).await
                }
            }
        })
        .await
        .map_err(|err| {
            err.on_stale_etag(|| {
                format!(
                    "{table} changed since this sync read it (etag {etag}); nothing after this \
                     write was sent, run the sync again."
                )
            })
        })
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
            table: Some(target.insert_body(v2::TableReference {
                project_id: ids.project,
                dataset_id: ids.dataset,
                table_id: ids.table,
            })?),
        };
        self.retry(span, "create a table", &request, |r| {
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
        let table_ddl = table.ddl(project);
        if recreate.snapshot_first {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_secs());
            let snapshot =
                BigQueryDatasetRef::new(table.project_or(project), table.dataset().clone())?.table(
                    BigQueryTableId::new(format!("{}_snapshot_{now}", table.table()))?,
                );
            self.run_ddl(snapshot.ddl(project).snapshot_of(&table_ddl))
                .await?;
            info!(%table, %snapshot, "Took a snapshot before recreating the table.");
            report.snapshot = Some(snapshot);
        }
        let sql = match recreate.method {
            BigQueryRecreateMethod::CreateOrReplace => table_ddl.create(&recreate.target, true)?,
            BigQueryRecreateMethod::DropAndCreate => table_ddl.drop_and_create(&recreate.target)?,
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
        existing: ExistingTable,
        report: &mut BigQueryTableSyncReport,
        span: &Span,
    ) -> BigQueryResult<()> {
        let table = &plan.table;
        let mut pacer = Pacer { sent: 0 };
        let mut latest = existing.raw;
        if let Some(body) = plan.patch_body(&latest)? {
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
        if let Some(body) = plan.defaults_body(&latest) {
            pacer.next().await;
            latest = self
                .write_table(table, body, &latest.etag.clone(), false, span)
                .await?;
            info!(%table, "Set the default values of the added columns.");
        }
        if let Some(body) = plan.update_body(&latest) {
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
        let table_ddl = table.ddl(&self.options().google_project_id);
        for change in &plan.changes {
            let sql = match change {
                BigQuerySchemaChange::RenameColumn { from, to } => {
                    table_ddl.rename_column(from, to)
                }
                BigQuerySchemaChange::WidenColumn { column, to, .. } => {
                    table_ddl.widen_column(column, to)
                }
                BigQuerySchemaChange::DropColumn { column, .. } => table_ddl.drop_column(column),
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

impl BigQueryTableTarget {
    /// The `InsertTable` body for this target at `reference`.
    fn insert_body(&self, reference: v2::TableReference) -> BigQueryResult<v2::Table> {
        let (time_partitioning, range_partitioning) = match &self.partitioning {
            Some(BigQueryPartitioning::Time { unit, column }) => (
                Some(v2::TimePartitioning {
                    r#type: unit.name().to_string(),
                    expiration_ms: self
                        .partition_expiration
                        .map(|d| millis("partition_expiration", d))
                        .transpose()?,
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
        Ok(v2::Table {
            table_reference: Some(reference),
            description: self.description.clone(),
            labels: self.labels.clone().into_iter().collect(),
            schema: Some(v2::TableSchema {
                fields: self
                    .columns
                    .iter()
                    .map(v2::TableFieldSchema::from)
                    .collect(),
                ..Default::default()
            }),
            time_partitioning,
            range_partitioning,
            clustering: (!self.clustering.is_empty()).then(|| v2::Clustering {
                fields: self.clustering.clone(),
            }),
            table_constraints: self.primary_key.as_ref().map(|key| v2::TableConstraints {
                primary_key: Some(v2::PrimaryKey {
                    columns: key.clone(),
                }),
                foreign_keys: Vec::new(),
            }),
            expiration_time: self.expiration.map(|t| t.as_millisecond()),
            ..Default::default()
        })
    }
}

impl BigQueryDb {
    /// Reads the table and reports what [`sync_table_schema`](Self::sync_table_schema) would
    /// do, writing nothing.
    pub(crate) async fn plan_table_schema(
        &self,
        declaration: BigQueryTableDeclaration,
    ) -> BigQueryResult<BigQueryTablePlan> {
        let span = declaration.table.schema_span();
        let (plan, _) = self.planned(&declaration, &span).await?;
        Ok(plan)
    }

    /// Reads the table and applies the plan.
    pub(crate) async fn sync_table_schema(
        &self,
        declaration: BigQueryTableDeclaration,
    ) -> BigQueryResult<BigQueryTableSyncReport> {
        let span = declaration.table.schema_span();
        let (plan, existing) = self.planned(&declaration, &span).await?;
        if plan.refusal.is_some() {
            return Err(BigQueryError::SchemaChangeRefused(Box::new(
                BigQuerySchemaChangeRefusedError { plan },
            )));
        }
        let mut report = BigQueryTableSyncReport::new(plan.table.clone());
        report.withheld.clone_from(&plan.withheld);
        let result = match (&plan.create, &plan.recreate, existing) {
            (Some(target), _, _) => {
                let created = self.create_table(&plan.table, target, &span).await;
                if created.is_ok() {
                    info!(table = %plan.table, "Created the table.");
                    report.created = Some(target.clone());
                }
                created
            }
            (None, Some(recreate), _) => self.recreate(&plan.table, recreate, &mut report).await,
            (None, None, Some(existing)) => {
                self.apply_in_place(&plan, existing, &mut report, &span)
                    .await
            }
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
