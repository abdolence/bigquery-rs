//! The dataset and table RPCs: `GetTable`, `InsertTable`, `PatchTable`, `UpdateTable`,
//! `DeleteTable`, `ListTables`, `GetDataset`, `InsertDataset`, `UpdateDataset`,
//! `DeleteDataset` and `ListDatasets`.

use crate::db::fake::FakeCall;
use crate::testing::rules::BigQueryFakeRpc;
use crate::testing::server::FakeShared;
use crate::testing::state::TableKey;
use gcloud_sdk::google::cloud::bigquery::v2::GetTableRequest;

impl FakeShared {
    pub(super) async fn serve_admin(&self, call: FakeCall) {
        match call.method() {
            "GetTable" => self.get_table(call).await,
            // TODO: serve datasets with their if-match preconditions, and table creation,
            // deletion, listing and additive patches.
            method => {
                let described = method.to_string();
                self.unmatched(call, &described, &[]);
            }
        }
    }

    /// Answers `GetTable` with the table's schema and figures, or `NotFound`.
    async fn get_table(&self, call: FakeCall) {
        let Some((call, request)) = self.first_request::<GetTableRequest>(call).await else {
            return;
        };
        let key = match TableKey::new(&request.project_id, &request.dataset_id, &request.table_id) {
            Ok(key) => key,
            Err(err) => {
                self.unmatched(call, &format!("GetTable of an invalid table: {err}"), &[]);
                return;
            }
        };
        let fault = self.rules().fault(BigQueryFakeRpc::GetTable, Some(&key));
        if let Some(fault) = fault {
            return fault.answer(call).await;
        }
        let table = self
            .state()
            .tables
            .get(&key)
            .map(|table| table.resource(&key));
        match table {
            Some(table) => call.reply(&table),
            None => {
                let status = key.not_found();
                call.fail(status.code(), status.message());
            }
        }
    }
}
