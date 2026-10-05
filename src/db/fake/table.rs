//! Fake answers for the `TableService` and `RowAccessPolicyService` RPCs a schema sync sends.

use super::FakeCall;
use gcloud_sdk::google::cloud::bigquery::v2::{
    GetTableRequest, InsertTableRequest, ListRowAccessPoliciesRequest, TableFieldSchema,
    UpdateOrPatchTableRequest,
};

/// A v2 column as `GetTable` returns it, with BigQuery's type and mode names.
pub(crate) fn v2_field(name: &str, field_type: &str, mode: &str) -> TableFieldSchema {
    TableFieldSchema {
        name: name.into(),
        r#type: field_type.into(),
        mode: mode.into(),
        ..Default::default()
    }
}

impl FakeCall {
    /// Reads a `GetTable` request and logs it as `GetTable ds.t`.
    pub(crate) async fn get_table_request(&mut self) -> GetTableRequest {
        let request: GetTableRequest = self.next_request().await.expect("a GetTable request");
        self.log(format!(
            "GetTable {}.{}",
            request.dataset_id, request.table_id
        ));
        request
    }

    /// Reads a `PatchTable` or `UpdateTable` request and logs it with its precondition, as
    /// `PatchTable if-match=e0`.
    pub(crate) async fn patch_or_update_request(&mut self) -> UpdateOrPatchTableRequest {
        let request: UpdateOrPatchTableRequest = self
            .next_request()
            .await
            .expect("a PatchTable or UpdateTable request");
        let precondition = self.header("if-match").unwrap_or_else(|| "none".into());
        self.log(format!("{} if-match={precondition}", self.method()));
        request
    }

    /// Reads an `InsertTable` request and logs it as `InsertTable ds.t`.
    pub(crate) async fn insert_table_request(&mut self) -> InsertTableRequest {
        let request: InsertTableRequest =
            self.next_request().await.expect("an InsertTable request");
        let table_id = request
            .table
            .as_ref()
            .and_then(|t| t.table_reference.as_ref())
            .map(|r| r.table_id.clone())
            .unwrap_or_default();
        self.log(format!("InsertTable {}.{table_id}", request.dataset_id));
        request
    }

    /// Reads a `ListRowAccessPolicies` request and logs it as `ListRowAccessPolicies ds.t`.
    pub(crate) async fn list_row_access_policies_request(
        &mut self,
    ) -> ListRowAccessPoliciesRequest {
        let request: ListRowAccessPoliciesRequest = self
            .next_request()
            .await
            .expect("a ListRowAccessPolicies request");
        self.log(format!(
            "ListRowAccessPolicies {}.{}",
            request.dataset_id, request.table_id
        ));
        request
    }
}
