//! The dataset and table RPCs: `GetTable`, `InsertTable`, `PatchTable`, `UpdateTable`,
//! `DeleteTable`, `ListTables`, `GetDataset`, `InsertDataset`, `UpdateDataset`,
//! `DeleteDataset` and `ListDatasets`, served from the fake's state.
//!
//! A write that carries an `if-match` precondition is refused with `FailedPrecondition` unless
//! it names the current etag, as BigQuery refuses it.

use crate::db::fake::FakeCall;
use crate::testing::rules::BigQueryFakeRpc;
use crate::testing::server::FakeShared;
use crate::testing::state::{
    fit_to_layout, DatasetKey, FakeDataset, FakeGeneration, FakeState, FakeTable, TableKey,
};
use crate::{BigQueryFieldMode, BigQueryFieldType, BigQueryTableSchema};
use gcloud_sdk::google::cloud::bigquery::v2;
use gcloud_sdk::prost::Message;
use gcloud_sdk::tonic::Status;
use std::num::NonZeroUsize;
use std::sync::Arc;

/// Why an admin call is not applied.
enum AdminRefusal {
    /// What BigQuery answers.
    Status(Status),
    /// What the fake cannot do, answered and reported as unmatched.
    Unsupported(String),
}

impl From<Status> for AdminRefusal {
    fn from(status: Status) -> Self {
        Self::Status(status)
    }
}

type AdminResult<T> = Result<T, AdminRefusal>;

impl FakeShared {
    pub(super) async fn serve_admin(&self, call: FakeCall) {
        match call.method() {
            "GetTable" => self.get_table(call).await,
            "InsertTable" => self.insert_table(call).await,
            "PatchTable" => {
                self.patch_or_update_table(call, BigQueryFakeRpc::PatchTable)
                    .await
            }
            "UpdateTable" => {
                self.patch_or_update_table(call, BigQueryFakeRpc::UpdateTable)
                    .await
            }
            "DeleteTable" => self.delete_table(call).await,
            "ListTables" => self.list_tables(call).await,
            "GetDataset" => self.get_dataset(call).await,
            "InsertDataset" => self.insert_dataset(call).await,
            "UpdateDataset" => self.update_dataset(call).await,
            "DeleteDataset" => self.delete_dataset(call).await,
            "ListDatasets" => self.list_datasets(call).await,
            method => {
                let described = method.to_string();
                self.unmatched(call, &described, &[]);
            }
        }
    }

    /// Answers `call` with `answer`.
    fn answer<M: Message>(&self, call: FakeCall, answer: AdminResult<M>) {
        match answer {
            Ok(message) => call.reply(&message),
            Err(AdminRefusal::Status(status)) => call.fail(status.code(), status.message()),
            Err(AdminRefusal::Unsupported(described)) => self.unmatched(call, &described, &[]),
        }
    }

    /// The table a request names, or `None` once the call is answered as unmatched.
    fn table_key(
        &self,
        call: FakeCall,
        rpc: BigQueryFakeRpc,
        project: &str,
        dataset: &str,
        table: &str,
    ) -> Option<(FakeCall, TableKey)> {
        match TableKey::new(project, dataset, table) {
            Ok(key) => Some((call, key)),
            Err(err) => {
                self.unmatched(call, &format!("{rpc:?} of an invalid table: {err}"), &[]);
                None
            }
        }
    }

    /// The dataset a request names, or `None` once the call is answered as unmatched.
    fn dataset_key(
        &self,
        call: FakeCall,
        rpc: BigQueryFakeRpc,
        project: &str,
        dataset: &str,
    ) -> Option<(FakeCall, DatasetKey)> {
        match DatasetKey::new(project, dataset) {
            Ok(key) => Some((call, key)),
            Err(err) => {
                self.unmatched(call, &format!("{rpc:?} of an invalid dataset: {err}"), &[]);
                None
            }
        }
    }

    /// Answers `GetTable` with the table's schema and figures, or `NotFound`.
    async fn get_table(&self, call: FakeCall) {
        let rpc = BigQueryFakeRpc::GetTable;
        let Some((call, request)) = self.first_request::<v2::GetTableRequest>(call).await else {
            return;
        };
        let Some((call, key)) = self.table_key(
            call,
            rpc,
            &request.project_id,
            &request.dataset_id,
            &request.table_id,
        ) else {
            return;
        };
        let Some(call) = self.unfaulted(call, rpc, Some(&key)).await else {
            return;
        };
        let answer = self.state().table_resource(&key);
        self.answer(call, answer);
    }

    /// Answers `InsertTable` by creating the table in its existing dataset.
    async fn insert_table(&self, call: FakeCall) {
        let rpc = BigQueryFakeRpc::InsertTable;
        let Some((call, request)) = self.first_request::<v2::InsertTableRequest>(call).await else {
            return;
        };
        let body = request.table.unwrap_or_default();
        let table_id = body
            .table_reference
            .as_ref()
            .map(|reference| reference.table_id.clone())
            .unwrap_or_default();
        let Some((call, key)) = self.table_key(
            call,
            rpc,
            &request.project_id,
            &request.dataset_id,
            &table_id,
        ) else {
            return;
        };
        let Some(call) = self.unfaulted(call, rpc, Some(&key)).await else {
            return;
        };
        let answer = self.state().insert_table(&key, body);
        self.answer(call, answer);
    }

    /// Answers `PatchTable` or `UpdateTable` under the call's `if-match` precondition.
    async fn patch_or_update_table(&self, call: FakeCall, rpc: BigQueryFakeRpc) {
        let Some((call, request)) = self
            .first_request::<v2::UpdateOrPatchTableRequest>(call)
            .await
        else {
            return;
        };
        let Some((call, key)) = self.table_key(
            call,
            rpc,
            &request.project_id,
            &request.dataset_id,
            &request.table_id,
        ) else {
            return;
        };
        let Some(call) = self.unfaulted(call, rpc, Some(&key)).await else {
            return;
        };
        let if_match = call.header("if-match");
        let body = request.table.unwrap_or_default();
        let replace = rpc == BigQueryFakeRpc::UpdateTable;
        let answer = self
            .state()
            .write_table(&key, body, if_match.as_deref(), replace);
        self.answer(call, answer);
    }

    /// Answers `DeleteTable`.
    async fn delete_table(&self, call: FakeCall) {
        let rpc = BigQueryFakeRpc::DeleteTable;
        let Some((call, request)) = self.first_request::<v2::DeleteTableRequest>(call).await else {
            return;
        };
        let Some((call, key)) = self.table_key(
            call,
            rpc,
            &request.project_id,
            &request.dataset_id,
            &request.table_id,
        ) else {
            return;
        };
        let Some(call) = self.unfaulted(call, rpc, Some(&key)).await else {
            return;
        };
        let answer = match self.state().tables.remove(&key) {
            Some(_) => Ok(()),
            None => Err(key.not_found().into()),
        };
        self.answer(call, answer);
    }

    /// Answers `ListTables` with one page of the dataset's tables, in ID order.
    async fn list_tables(&self, call: FakeCall) {
        let rpc = BigQueryFakeRpc::ListTables;
        let Some((call, request)) = self.first_request::<v2::ListTablesRequest>(call).await else {
            return;
        };
        let Some((call, key)) =
            self.dataset_key(call, rpc, &request.project_id, &request.dataset_id)
        else {
            return;
        };
        let Some(call) = self.unfaulted(call, rpc, None).await else {
            return;
        };
        let answer = self.state().list_tables(&key, &request);
        self.answer(call, answer);
    }

    /// Answers `GetDataset`.
    async fn get_dataset(&self, call: FakeCall) {
        let rpc = BigQueryFakeRpc::GetDataset;
        let Some((call, request)) = self.first_request::<v2::GetDatasetRequest>(call).await else {
            return;
        };
        let Some((call, key)) =
            self.dataset_key(call, rpc, &request.project_id, &request.dataset_id)
        else {
            return;
        };
        let Some(call) = self.unfaulted(call, rpc, None).await else {
            return;
        };
        let answer = self
            .state()
            .dataset(&key)
            .map(|dataset| dataset.resource(&key))
            .map_err(AdminRefusal::from);
        self.answer(call, answer);
    }

    /// Answers `InsertDataset` by creating the dataset with the settings it carries.
    async fn insert_dataset(&self, call: FakeCall) {
        let rpc = BigQueryFakeRpc::InsertDataset;
        let Some((call, request)) = self.first_request::<v2::InsertDatasetRequest>(call).await
        else {
            return;
        };
        let body = request.dataset.unwrap_or_default();
        let dataset_id = body
            .dataset_reference
            .as_ref()
            .map(|reference| reference.dataset_id.clone())
            .unwrap_or_default();
        let Some((call, key)) = self.dataset_key(call, rpc, &request.project_id, &dataset_id)
        else {
            return;
        };
        let Some(call) = self.unfaulted(call, rpc, None).await else {
            return;
        };
        let answer = self.state().insert_dataset(&key, body);
        self.answer(call, answer);
    }

    /// Answers `UpdateDataset` under the call's `if-match` precondition.
    async fn update_dataset(&self, call: FakeCall) {
        let rpc = BigQueryFakeRpc::UpdateDataset;
        let Some((call, request)) = self
            .first_request::<v2::UpdateOrPatchDatasetRequest>(call)
            .await
        else {
            return;
        };
        let Some((call, key)) =
            self.dataset_key(call, rpc, &request.project_id, &request.dataset_id)
        else {
            return;
        };
        let Some(call) = self.unfaulted(call, rpc, None).await else {
            return;
        };
        let if_match = call.header("if-match");
        let body = request.dataset.unwrap_or_default();
        let answer = self.state().update_dataset(&key, body, if_match.as_deref());
        self.answer(call, answer);
    }

    /// Answers `DeleteDataset`.
    async fn delete_dataset(&self, call: FakeCall) {
        let rpc = BigQueryFakeRpc::DeleteDataset;
        let Some((call, request)) = self.first_request::<v2::DeleteDatasetRequest>(call).await
        else {
            return;
        };
        let Some((call, key)) =
            self.dataset_key(call, rpc, &request.project_id, &request.dataset_id)
        else {
            return;
        };
        let Some(call) = self.unfaulted(call, rpc, None).await else {
            return;
        };
        let answer = self.state().delete_dataset(&key, request.delete_contents);
        self.answer(call, answer);
    }

    /// Answers `ListDatasets` with one page of the project's datasets, in ID order.
    async fn list_datasets(&self, call: FakeCall) {
        let rpc = BigQueryFakeRpc::ListDatasets;
        let Some((call, request)) = self.first_request::<v2::ListDatasetsRequest>(call).await
        else {
            return;
        };
        let Some(call) = self.unfaulted(call, rpc, None).await else {
            return;
        };
        let answer = self.state().list_datasets(&request);
        self.answer(call, answer);
    }
}

impl FakeState {
    /// The dataset at `key`.
    ///
    /// # Errors
    /// `NotFound` if there is none.
    fn dataset(&self, key: &DatasetKey) -> Result<&FakeDataset, Status> {
        self.datasets.get(key).ok_or_else(|| key.not_found())
    }

    fn table_resource(&self, key: &TableKey) -> AdminResult<v2::Table> {
        let table = self.tables.get(key).ok_or_else(|| key.not_found())?;
        Ok(table.resource(key))
    }

    /// Creates the table `body` describes in its existing dataset.
    ///
    /// # Errors
    /// `NotFound` for a dataset that does not exist, `AlreadyExists` for a table that does,
    /// and `InvalidArgument` for a schema the crate cannot read.
    fn insert_table(&mut self, key: &TableKey, mut body: v2::Table) -> AdminResult<v2::Table> {
        self.dataset(&key.dataset)?;
        let schema = table_schema(key, body.schema.take().as_ref())?;
        let generation = self.next_generation();
        let mut table = FakeTable::new(schema, Vec::new(), NonZeroUsize::MIN, generation);
        table.resource = body;
        let resource = table.resource(key);
        self.create_table(key.clone(), table)
            .map_err(|_| key.already_exists())?;
        Ok(resource)
    }

    /// Applies `body` to the table: `replace` takes it as the table's settings, as
    /// `UpdateTable` does, and otherwise only the fields it sets change, as `PatchTable` does.
    ///
    /// # Errors
    /// `NotFound` for a table that does not exist, `FailedPrecondition` for an `if_match`
    /// that is not the table's etag, and `InvalidArgument` for a schema that drops, retypes
    /// or narrows a column or adds a REQUIRED one.
    fn write_table(
        &mut self,
        key: &TableKey,
        mut body: v2::Table,
        if_match: Option<&str>,
        replace: bool,
    ) -> AdminResult<v2::Table> {
        let generation = self.next_generation();
        let table = self.tables.get_mut(key).ok_or_else(|| key.not_found())?;
        table.generation.check(if_match)?;
        if let Some(schema) = body.schema.take() {
            let schema = table_schema(key, Some(&schema))?;
            table.change_schema(key, schema)?;
        }
        if replace {
            table.resource = body;
        } else {
            patch_table(&mut table.resource, body);
        }
        table.generation = generation;
        Ok(table.resource(key))
    }

    /// One page of the tables of the dataset at `key`.
    ///
    /// # Errors
    /// `NotFound` for a dataset that does not exist, and `InvalidArgument` for a page token
    /// the fake did not hand out.
    fn list_tables(
        &self,
        key: &DatasetKey,
        request: &v2::ListTablesRequest,
    ) -> AdminResult<v2::TableList> {
        self.dataset(key)?;
        let tables: Vec<_> = self
            .tables
            .iter()
            .filter(|(table, _)| table.dataset == *key)
            .map(|(table, held)| held.listed(table))
            .collect();
        let total_items = i32::try_from(tables.len()).ok();
        let (tables, next_page_token) = page(tables, request.max_results, &request.page_token)?;
        Ok(v2::TableList {
            kind: "bigquery#tableList".into(),
            next_page_token,
            tables,
            total_items,
            ..Default::default()
        })
    }

    /// Creates the dataset `body` describes.
    ///
    /// # Errors
    /// `AlreadyExists` for a dataset that exists.
    fn insert_dataset(&mut self, key: &DatasetKey, body: v2::Dataset) -> AdminResult<v2::Dataset> {
        let dataset = self.create_dataset(key.clone())?;
        dataset.resource = body;
        Ok(dataset.resource(key))
    }

    /// Replaces the dataset's settings with `body`, its location aside, which cannot change.
    ///
    /// # Errors
    /// `NotFound` for a dataset that does not exist, and `FailedPrecondition` for an
    /// `if_match` that is not the dataset's etag.
    fn update_dataset(
        &mut self,
        key: &DatasetKey,
        mut body: v2::Dataset,
        if_match: Option<&str>,
    ) -> AdminResult<v2::Dataset> {
        let generation = self.next_generation();
        let dataset = self.datasets.get_mut(key).ok_or_else(|| key.not_found())?;
        dataset.generation.check(if_match)?;
        body.location = dataset.resource.location.clone();
        dataset.resource = body;
        dataset.generation = generation;
        Ok(dataset.resource(key))
    }

    /// Deletes the dataset, and with `delete_contents` its tables.
    ///
    /// # Errors
    /// `NotFound` for a dataset that does not exist, and `FailedPrecondition` for one that
    /// holds tables without `delete_contents`. BigQuery documents that refusal as the REST
    /// error `resourceInUse`, HTTP 400.
    fn delete_dataset(&mut self, key: &DatasetKey, delete_contents: bool) -> AdminResult<()> {
        self.dataset(key)?;
        let in_dataset = |table: &TableKey| table.dataset == *key;
        if !delete_contents && self.tables.keys().any(in_dataset) {
            return Err(Status::failed_precondition(format!(
                "Dataset {} is still in use",
                key.legacy_id()
            ))
            .into());
        }
        self.tables.retain(|table, _| !in_dataset(table));
        self.datasets.remove(key);
        Ok(())
    }

    /// One page of the datasets of the request's project. Hidden datasets, whose ID starts
    /// with `_`, are listed only with `all`.
    ///
    /// # Errors
    /// `InvalidArgument` for a page token the fake did not hand out.
    fn list_datasets(&self, request: &v2::ListDatasetsRequest) -> AdminResult<v2::DatasetList> {
        let datasets: Vec<_> = self
            .datasets
            .iter()
            .filter(|(key, _)| key.project == request.project_id)
            .filter(|(key, _)| request.all || !key.dataset.to_string().starts_with('_'))
            .map(|(key, dataset)| dataset.listed(key))
            .collect();
        let (datasets, next_page_token) = page(datasets, request.max_results, &request.page_token)?;
        Ok(v2::DatasetList {
            kind: "bigquery#datasetList".into(),
            next_page_token,
            datasets,
            ..Default::default()
        })
    }
}

impl FakeGeneration {
    /// Checks a write's `if-match` precondition against this generation.
    ///
    /// # Errors
    /// `FailedPrecondition` for a precondition that names another generation.
    fn check(self, if_match: Option<&str>) -> Result<(), Status> {
        match if_match {
            Some(etag) if etag != self.etag() => {
                Err(Status::failed_precondition("Precondition check failed."))
            }
            _ => Ok(()),
        }
    }
}

impl FakeTable {
    /// The table as `ListTables` lists it.
    fn listed(&self, key: &TableKey) -> v2::ListFormatTable {
        let resource = self.resource(key);
        v2::ListFormatTable {
            kind: "bigquery#table".into(),
            id: resource.id,
            table_reference: resource.table_reference,
            friendly_name: resource.friendly_name,
            r#type: resource.r#type,
            time_partitioning: resource.time_partitioning,
            range_partitioning: resource.range_partitioning,
            clustering: resource.clustering,
            labels: resource.labels,
            creation_time: resource.creation_time,
            expiration_time: resource.expiration_time.unwrap_or_default(),
            ..Default::default()
        }
    }

    /// Takes `schema` in place of the table's, as BigQuery allows it in place: every column
    /// kept with its type, a REQUIRED one possibly relaxed to NULLABLE, and new columns that
    /// are not REQUIRED. The rows held read the new columns as NULL.
    ///
    /// # Errors
    /// `InvalidArgument` for a change BigQuery refuses, and an unsupported change for a
    /// nested column added to a table that holds rows.
    fn change_schema(&mut self, key: &TableKey, schema: BigQueryTableSchema) -> AdminResult<()> {
        let refused = |problem: String| {
            AdminRefusal::from(Status::invalid_argument(format!(
                "Provided Schema does not match Table {}. {problem}",
                key.legacy_id()
            )))
        };
        for old in &self.schema.fields {
            let Some(new) = schema
                .fields
                .iter()
                .find(|new| new.name.eq_ignore_ascii_case(&old.name))
            else {
                return Err(refused(format!(
                    "Field {} is missing in new schema",
                    old.name
                )));
            };
            let both_structs = matches!(
                (&old.field_type, &new.field_type),
                (BigQueryFieldType::Struct(_), BigQueryFieldType::Struct(_))
            );
            if old.field_type != new.field_type && !both_structs {
                return Err(refused(format!("Field {} has changed type", old.name)));
            }
            let relaxed =
                old.mode == BigQueryFieldMode::Required && new.mode == BigQueryFieldMode::Nullable;
            if old.mode != new.mode && !relaxed {
                return Err(refused(format!("Field {} has changed mode", old.name)));
            }
        }
        let added_required = schema.fields.iter().find(|new| {
            new.mode == BigQueryFieldMode::Required
                && !self
                    .schema
                    .fields
                    .iter()
                    .any(|old| old.name.eq_ignore_ascii_case(&new.name))
        });
        if let Some(added) = added_required {
            return Err(refused(format!(
                "Cannot add required fields to an existing schema. (field: {})",
                added.name
            )));
        }
        let arrow_schema = Arc::new(schema.arrow_read_schema());
        let unsupported = |problem: String| {
            AdminRefusal::Unsupported(format!("a schema change of {key}, which {problem}"))
        };
        let batches = self
            .batches
            .iter()
            .map(|batch| {
                fit_to_layout(batch, &arrow_schema).map_err(|err| {
                    unsupported(format!(
                        "holds rows, cannot carry them over, as a nested column changed: {err}"
                    ))
                })
            })
            .collect::<AdminResult<Vec<_>>>()?;
        self.schema = schema;
        self.arrow_schema = arrow_schema;
        self.batches = batches;
        Ok(())
    }
}

impl FakeDataset {
    /// The dataset as `ListDatasets` lists it.
    fn listed(&self, key: &DatasetKey) -> v2::ListFormatDataset {
        let resource = self.resource(key);
        v2::ListFormatDataset {
            kind: "bigquery#dataset".into(),
            id: resource.id,
            dataset_reference: resource.dataset_reference,
            labels: resource.labels,
            friendly_name: resource.friendly_name,
            location: resource.location,
            ..Default::default()
        }
    }
}

/// Takes into `settings` what a `PatchTable` body sets of the settings the crate writes:
/// labels are merged, and every other setting the body leaves unset stays as it is.
fn patch_table(settings: &mut v2::Table, body: v2::Table) {
    settings.labels.extend(body.labels);
    settings.description = body.description.or(settings.description.take());
    settings.friendly_name = body.friendly_name.or(settings.friendly_name.take());
    settings.expiration_time = body.expiration_time.or(settings.expiration_time.take());
    settings.time_partitioning = body.time_partitioning.or(settings.time_partitioning.take());
    settings.range_partitioning = body
        .range_partitioning
        .or(settings.range_partitioning.take());
    settings.clustering = body.clustering.or(settings.clustering.take());
    settings.table_constraints = body.table_constraints.or(settings.table_constraints.take());
}

/// The schema of a table body.
///
/// # Errors
/// `InvalidArgument` for a missing schema or one the crate cannot read.
fn table_schema(
    key: &TableKey,
    schema: Option<&v2::TableSchema>,
) -> Result<BigQueryTableSchema, Status> {
    let schema = schema.ok_or_else(|| {
        Status::invalid_argument(format!("The table {} has no schema", key.legacy_id()))
    })?;
    BigQueryTableSchema::try_from(schema).map_err(|err| {
        Status::invalid_argument(format!(
            "The schema of {} is invalid: {err}",
            key.legacy_id()
        ))
    })
}

/// The page of `items` that `page_token` starts, at most `max_results` long, and the token of
/// the next page, empty after the last. A page token is the offset of the page's first item.
///
/// # Errors
/// `InvalidArgument` for a page token that is not such an offset.
fn page<T>(
    items: Vec<T>,
    max_results: Option<u32>,
    page_token: &str,
) -> Result<(Vec<T>, String), Status> {
    let first = match page_token {
        "" => 0,
        token => token
            .parse::<usize>()
            .map_err(|_| Status::invalid_argument(format!("Invalid page token {token:?}")))?,
    };
    let length = max_results
        .and_then(|max| usize::try_from(max).ok())
        .filter(|max| *max > 0)
        .unwrap_or(usize::MAX);
    let end = first.saturating_add(length).min(items.len());
    let next_page_token = if end < items.len() {
        end.to_string()
    } else {
        String::new()
    };
    let page = items
        .into_iter()
        .skip(first)
        .take(end.saturating_sub(first))
        .collect();
    Ok((page, next_page_token))
}

#[cfg(test)]
mod tests {
    use crate::db::if_match;
    use crate::errors::BigQueryError;
    use crate::testing::BigQueryFake;
    use crate::{
        BigQueryDataset, BigQueryDatasetId, BigQueryResult, BigQuerySchemaChange,
        BigQuerySchemaColumn, BigQuerySchemaColumns, BigQuerySchemaColumnsBuilder, BigQueryTableId,
        BigQueryTableRef,
    };
    use futures::TryStreamExt;
    use gcloud_sdk::google::cloud::bigquery::v2;
    use serde::{Deserialize, Serialize};
    use tracing::Span;

    const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
    const ARCHIVE: BigQueryDatasetId = BigQueryDatasetId::from_static("archive");
    const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");
    const CUSTOMERS: BigQueryTableId = BigQueryTableId::from_static("customers");

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Order {
        id: i64,
        customer: Option<String>,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct NotedOrder {
        id: i64,
        customer: Option<String>,
        note: Option<String>,
    }

    fn order_columns(c: BigQuerySchemaColumnsBuilder) -> Vec<BigQuerySchemaColumn> {
        c.fields([
            c.field("id").int64().required(),
            c.field("customer").string(),
        ])
    }

    fn noted_order_columns(c: BigQuerySchemaColumnsBuilder) -> Vec<BigQuerySchemaColumn> {
        c.fields([
            c.field("id").int64().required(),
            c.field("customer").string(),
            c.field("note").string(),
        ])
    }

    async fn dataset_ids(fake: &BigQueryFake) -> BigQueryResult<Vec<BigQueryDatasetId>> {
        fake.db()
            .fluent()
            .schema()
            .datasets()
            .page_size(1)
            .stream_all_with_errors()
            .await?
            .map_ok(|dataset| dataset.reference.dataset().clone())
            .try_collect()
            .await
    }

    async fn table_ids(
        fake: &BigQueryFake,
        dataset: BigQueryDatasetId,
    ) -> BigQueryResult<Vec<BigQueryTableId>> {
        fake.db()
            .fluent()
            .schema()
            .dataset(dataset)
            .tables()
            .stream_all_with_errors()
            .await?
            .map_ok(|table| table.reference.table().clone())
            .try_collect()
            .await
    }

    #[tokio::test]
    async fn a_created_dataset_reads_back_and_is_listed() -> BigQueryResult<()> {
        let fake = BigQueryFake::start().await?;
        let shop = fake.db().fluent().schema().dataset(SHOP);

        let created = shop
            .create()
            .description("The shop")
            .labels([("team", "sales")])
            .execute()
            .await?;
        fake.db()
            .fluent()
            .schema()
            .dataset(ARCHIVE)
            .create()
            .execute()
            .await?;

        assert_eq!(created.reference.project(), Some("fake-project"));
        assert_eq!(created.description.as_deref(), Some("The shop"));
        assert!(created.creation_time.is_some());
        let read: BigQueryDataset = fake.db().fluent().schema().dataset(SHOP).get().await?;
        assert_eq!(read, created);
        assert_eq!(dataset_ids(&fake).await?, [ARCHIVE, SHOP]);
        Ok(())
    }

    #[tokio::test]
    async fn creating_a_dataset_that_exists_is_a_conflict() -> BigQueryResult<()> {
        let fake = BigQueryFake::start().await?;
        fake.create_dataset(SHOP);

        let created = fake
            .db()
            .fluent()
            .schema()
            .dataset(SHOP)
            .create()
            .execute()
            .await;

        assert!(
            matches!(created, Err(BigQueryError::DataConflictError(_))),
            "{created:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_missing_dataset_is_not_found() -> BigQueryResult<()> {
        let fake = BigQueryFake::start().await?;
        let shop = || fake.db().fluent().schema().dataset(SHOP);

        let read = shop().get().await;
        let deleted = shop().delete().await;
        let listed = table_ids(&fake, SHOP).await;

        assert!(
            matches!(read, Err(BigQueryError::DataNotFoundError(_))),
            "{read:?}"
        );
        assert!(
            matches!(deleted, Err(BigQueryError::DataNotFoundError(_))),
            "{deleted:?}"
        );
        assert!(
            matches!(listed, Err(BigQueryError::DataNotFoundError(_))),
            "{listed:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn an_update_changes_the_description_and_labels() -> BigQueryResult<()> {
        let fake = BigQueryFake::start().await?;
        let shop = || fake.db().fluent().schema().dataset(SHOP);
        shop()
            .create()
            .description("The shop")
            .labels([("team", "sales"), ("tier", "gold")])
            .execute()
            .await?;

        let updated = shop()
            .update()
            .clear_description()
            .label("team", "finance")
            .remove_label("tier")
            .execute()
            .await?;

        assert_eq!(updated.description, None);
        assert_eq!(updated.labels, [("team", "finance")].into());
        assert_eq!(shop().get().await?, updated);
        Ok(())
    }

    #[tokio::test]
    async fn a_write_under_a_stale_etag_is_a_conflict() -> BigQueryResult<()> {
        let fake = BigQueryFake::start().await?;
        fake.table(SHOP.table(ORDERS), order_columns).create()?;
        let db = fake.db();
        let stale = if_match("GetTable", "fake-etag-0")?;

        let dataset = v2::UpdateOrPatchDatasetRequest {
            project_id: "fake-project".into(),
            dataset_id: SHOP.to_string(),
            dataset: Some(v2::Dataset::default()),
            ..Default::default()
        };
        let dataset = db
            .retry_with_metadata(&Span::none(), "update", &dataset, &stale, |r| {
                let mut client = db.dataset_client();
                async move { client.update_dataset(r).await }
            })
            .await
            .map_err(|err| err.on_stale_etag(String::new));
        let table = v2::UpdateOrPatchTableRequest {
            project_id: "fake-project".into(),
            dataset_id: SHOP.to_string(),
            table_id: ORDERS.to_string(),
            table: Some(v2::Table::default()),
            autodetect_schema: false,
        };
        let table = db
            .retry_with_metadata(&Span::none(), "patch", &table, &stale, |r| {
                let mut client = db.table_client();
                async move { client.patch_table(r).await }
            })
            .await
            .map_err(|err| err.on_stale_etag(String::new));

        assert!(
            matches!(dataset, Err(BigQueryError::DataConflictError(_))),
            "{dataset:?}"
        );
        assert!(
            matches!(table, Err(BigQueryError::DataConflictError(_))),
            "{table:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn deleting_a_dataset_with_tables_needs_its_contents_deleted() -> BigQueryResult<()> {
        let fake = BigQueryFake::start().await?;
        fake.table(SHOP.table(ORDERS), order_columns).create()?;
        let shop = || fake.db().fluent().schema().dataset(SHOP);

        let refused = shop().delete().await;
        assert!(
            matches!(&refused, Err(BigQueryError::DatabaseError(err))
                if err.public.code == "FailedPrecondition" && !err.retry_possible),
            "{refused:?}"
        );
        assert_eq!(fake.rows::<Order>(SHOP.table(ORDERS))?, []);

        shop().dangerously_delete_with_contents().await?;
        let rows = fake.rows::<Order>(SHOP.table(ORDERS));
        assert!(
            matches!(rows, Err(BigQueryError::DataNotFoundError(_))),
            "{rows:?}"
        );
        assert_eq!(dataset_ids(&fake).await?, []);
        Ok(())
    }

    #[tokio::test]
    async fn tables_are_listed_and_deleted() -> BigQueryResult<()> {
        let fake = BigQueryFake::start().await?;
        fake.table(SHOP.table(ORDERS), order_columns).create()?;
        fake.table(SHOP.table(CUSTOMERS), order_columns).create()?;
        let orders = || fake.db().fluent().schema().table(SHOP.table(ORDERS));

        assert_eq!(table_ids(&fake, SHOP).await?, [CUSTOMERS, ORDERS]);
        orders().delete().await?;

        assert_eq!(table_ids(&fake, SHOP).await?, [CUSTOMERS]);
        let read = orders().get().await;
        assert!(
            matches!(read, Err(BigQueryError::DataNotFoundError(_))),
            "{read:?}"
        );
        let deleted = orders().delete().await;
        assert!(
            matches!(deleted, Err(BigQueryError::DataNotFoundError(_))),
            "{deleted:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn sync_creates_a_table_then_adds_a_column() -> BigQueryResult<()> {
        let fake = BigQueryFake::start().await?;
        fake.create_dataset(SHOP);
        let orders = || fake.db().fluent().schema().table(SHOP.table(ORDERS));

        let created = orders().columns(order_columns).sync().await?;
        assert!(created.created.is_some(), "{created:?}");
        fake.db()
            .fluent()
            .insert()
            .into(SHOP.table(ORDERS))
            .objects(&[Order {
                id: 1,
                customer: Some("Alice".into()),
            }])
            .execute()
            .await?;

        let patched = orders().columns(noted_order_columns).sync().await?;

        assert!(
            matches!(
                &patched.applied[..],
                [BigQuerySchemaChange::AddColumn { .. }]
            ),
            "{patched:?}"
        );
        let table = orders().get().await?;
        let declared = orders().columns(noted_order_columns).plan().await?;
        assert!(declared.changes.is_empty(), "{declared:?}");
        assert_eq!(table.num_rows, Some(1));
        let rows: Vec<NotedOrder> = fake.rows(SHOP.table(ORDERS))?;
        assert_eq!(
            rows,
            [NotedOrder {
                id: 1,
                customer: Some("Alice".into()),
                note: None
            }]
        );
        Ok(())
    }

    #[tokio::test]
    async fn sync_into_a_missing_dataset_is_not_found() -> BigQueryResult<()> {
        let fake = BigQueryFake::start().await?;
        let table: BigQueryTableRef = SHOP.table(ORDERS);

        let synced = fake
            .db()
            .fluent()
            .schema()
            .table(table)
            .columns(order_columns)
            .sync()
            .await;

        assert!(
            matches!(synced, Err(BigQueryError::DataNotFoundError(_))),
            "{synced:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_schema_change_matches_columns_ignoring_case() -> BigQueryResult<()> {
        let fake = BigQueryFake::start().await?;
        let alice = Order {
            id: 1,
            customer: Some("Alice".into()),
        };
        fake.table(SHOP.table(ORDERS), order_columns)
            .rows([alice])
            .create()?;
        let columns = BigQuerySchemaColumnsBuilder;
        let shouted: BigQuerySchemaColumns = columns
            .fields([
                columns.field("ID").int64().required(),
                columns.field("CUSTOMER").string(),
                columns.field("note").string(),
            ])
            .into();
        let schema = shouted.table_schema()?;

        let patched = fake
            .db()
            .table_client()
            .patch_table(v2::UpdateOrPatchTableRequest {
                project_id: "fake-project".to_string(),
                dataset_id: SHOP.to_string(),
                table_id: ORDERS.to_string(),
                table: Some(v2::Table {
                    schema: Some(v2::TableSchema::from(&schema)),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await
            .map_err(BigQueryError::from)?
            .into_inner();

        assert_eq!(patched.num_rows, Some(1));
        Ok(())
    }
}
