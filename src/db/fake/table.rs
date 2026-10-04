//! Fake answers for the `TableService` and `RowAccessPolicyService` RPCs a schema sync sends.

use super::FakeCall;
use gcloud_sdk::google::cloud::bigquery::v2::{
    GetTableRequest, InsertTableRequest, ListRowAccessPoliciesRequest, UpdateOrPatchTableRequest,
};

/// Reads a `GetTable` request and logs it as `GetTable ds.t`.
pub(crate) async fn get_table_request(call: &mut FakeCall) -> GetTableRequest {
    let request: GetTableRequest = call.next_request().await.expect("a GetTable request");
    call.log(format!(
        "GetTable {}.{}",
        request.dataset_id, request.table_id
    ));
    request
}

/// Reads a `PatchTable` or `UpdateTable` request and logs it with its precondition, as
/// `PatchTable if-match=e0`.
pub(crate) async fn patch_or_update_request(call: &mut FakeCall) -> UpdateOrPatchTableRequest {
    let request: UpdateOrPatchTableRequest = call
        .next_request()
        .await
        .expect("a PatchTable or UpdateTable request");
    let precondition = call.header("if-match").unwrap_or_else(|| "none".into());
    call.log(format!("{} if-match={precondition}", call.method()));
    request
}

/// Reads an `InsertTable` request and logs it as `InsertTable ds.t`.
pub(crate) async fn insert_table_request(call: &mut FakeCall) -> InsertTableRequest {
    let request: InsertTableRequest = call.next_request().await.expect("an InsertTable request");
    let table_id = request
        .table
        .as_ref()
        .and_then(|t| t.table_reference.as_ref())
        .map(|r| r.table_id.clone())
        .unwrap_or_default();
    call.log(format!("InsertTable {}.{table_id}", request.dataset_id));
    request
}

/// Reads a `ListRowAccessPolicies` request and logs it as `ListRowAccessPolicies ds.t`.
pub(crate) async fn list_row_access_policies_request(
    call: &mut FakeCall,
) -> ListRowAccessPoliciesRequest {
    let request: ListRowAccessPoliciesRequest = call
        .next_request()
        .await
        .expect("a ListRowAccessPolicies request");
    call.log(format!(
        "ListRowAccessPolicies {}.{}",
        request.dataset_id, request.table_id
    ));
    request
}

#[cfg(test)]
mod tests;
