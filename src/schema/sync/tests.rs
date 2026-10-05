//! `.plan()` and `.sync()` against the fake server: what each sends, in which order, with which
//! precondition, and what it refuses to send.

use crate::db::fake::query::job_reference;
use crate::db::fake::table::v2_field;
use crate::db::fake::{FakeBigQuery, FakeCall};
use crate::db::fake::{ORDERS, SHOP};
use crate::errors::BigQueryError;
use crate::{BigQueryDroppedData, BigQueryRecreateMethod, BigQueryRefusal};
use gcloud_sdk::google::cloud::bigquery::v2::{
    self as v2, ListRowAccessPoliciesResponse, QueryResponse, RowAccessPolicy,
    RowAccessPolicyReference,
};
use gcloud_sdk::tonic::Code;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// `id INTEGER REQUIRED, name STRING, quantity INTEGER, note STRING`, label `owner: billing`, 5
/// rows, etag `e0`.
fn live_table() -> v2::Table {
    v2::Table {
        etag: "e0".into(),
        r#type: "TABLE".into(),
        table_reference: Some(v2::TableReference {
            project_id: "fake-project".into(),
            dataset_id: "shop".into(),
            table_id: "orders".into(),
        }),
        schema: Some(v2::TableSchema {
            fields: vec![
                v2_field("id", "INTEGER", "REQUIRED"),
                v2_field("name", "STRING", "NULLABLE"),
                v2_field("quantity", "INTEGER", "NULLABLE"),
                v2_field("note", "STRING", "NULLABLE"),
            ],
            ..Default::default()
        }),
        labels: HashMap::from([("owner".to_string(), "billing".to_string())]),
        num_rows: Some(5),
        num_bytes: Some(160),
        ..Default::default()
    }
}

#[derive(Clone)]
struct Scenario {
    table: Option<v2::Table>,
    patch_fails: Option<Code>,
    etags: Arc<AtomicUsize>,
}

impl Scenario {
    fn new(table: Option<v2::Table>) -> Self {
        Self {
            table,
            patch_fails: None,
            etags: Arc::new(AtomicUsize::new(1)),
        }
    }

    /// The table a write returns: its body under a fresh etag.
    fn written(&self, body: Option<v2::Table>) -> v2::Table {
        let etag_number = self.etags.fetch_add(1, Ordering::SeqCst);
        v2::Table {
            etag: format!("e{etag_number}"),
            ..body.unwrap_or_default()
        }
    }

    async fn serve(self, mut call: FakeCall) {
        match call.method() {
            "GetTable" => {
                call.get_table_request().await;
                match &self.table {
                    Some(table) => call.reply(table),
                    None => call.fail(Code::NotFound, "Not found: Table fake-project:shop.orders"),
                }
            }
            "PatchTable" | "UpdateTable" => {
                let request = call.patch_or_update_request().await;
                match self.patch_fails {
                    Some(code) if call.method() == "PatchTable" => {
                        call.fail(code, "Precondition check failed.")
                    }
                    _ => {
                        let table = self.written(request.table);
                        call.reply(&table)
                    }
                }
            }
            "InsertTable" => {
                let request = call.insert_table_request().await;
                let table = self.written(request.table);
                call.reply(&table)
            }
            "ListRowAccessPolicies" => {
                call.list_row_access_policies_request().await;
                call.reply(&ListRowAccessPoliciesResponse {
                    row_access_policies: vec![RowAccessPolicy {
                        row_access_policy_reference: Some(RowAccessPolicyReference {
                            policy_id: "rap1".into(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                    next_page_token: String::new(),
                })
            }
            "Query" => {
                call.query_request().await;
                call.reply(&QueryResponse {
                    job_reference: Some(job_reference()),
                    job_complete: Some(true),
                    statement_type: "ALTER_TABLE".into(),
                    ..Default::default()
                })
            }
            other => {
                let message = format!("unexpected {other}");
                call.fail(Code::Unimplemented, &message)
            }
        }
    }
}

async fn start(scenario: Scenario) -> FakeBigQuery {
    FakeBigQuery::start(move |call| scenario.clone().serve(call)).await
}

const TABLE_SQL: &str = "`fake-project`.`shop`.`orders`";

#[tokio::test]
async fn sync_writes_in_fixed_order_with_the_read_etag_as_precondition() {
    let fake = start(Scenario::new(Some(live_table()))).await;
    let report = fake
        .db
        .fluent()
        .schema()
        .table(SHOP.table(ORDERS))
        .columns(|columns| {
            columns.fields([
                columns.field("id").int64().required(),
                columns.field("name2").string().renamed_from("name"),
                columns.field("quantity").numeric(),
                columns.field("status").string().default_value("'new'"),
            ])
        })
        .allow_widening()
        .prune_undeclared()
        .sync()
        .await
        .expect("a sync");
    assert_eq!(
        fake.calls(),
        [
            "GetTable shop.orders".to_string(),
            "PatchTable if-match=e0".into(),
            "PatchTable if-match=e1".into(),
            "UpdateTable if-match=e2".into(),
            format!("Query ALTER TABLE {TABLE_SQL} RENAME COLUMN `name` TO `name2`"),
            format!("Query ALTER TABLE {TABLE_SQL} ALTER COLUMN `quantity` SET DATA TYPE NUMERIC"),
            format!("Query ALTER TABLE {TABLE_SQL} DROP COLUMN `note`"),
        ]
    );
    assert_eq!(report.applied.len(), 5, "{report}");
    assert_eq!(
        report.dropped_data,
        [BigQueryDroppedData::Column {
            column: "note".into()
        }]
    );
}

#[tokio::test]
async fn a_stale_etag_is_a_conflict_and_stops_the_sync() {
    let mut scenario = Scenario::new(Some(live_table()));
    scenario.patch_fails = Some(Code::FailedPrecondition);
    let fake = start(scenario).await;
    let result = fake
        .db
        .fluent()
        .schema()
        .table(SHOP.table(ORDERS))
        .columns(|columns| {
            columns.fields([
                columns.field("id").int64().required(),
                columns.field("name").string(),
                columns.field("quantity").int64(),
                columns.field("new").string(),
            ])
        })
        .prune_undeclared()
        .sync()
        .await;
    match result {
        Err(BigQueryError::DataConflictError(_)) => {}
        other => panic!("expected a conflict, got {other:?}"),
    }
    assert_eq!(
        fake.calls(),
        ["GetTable shop.orders", "PatchTable if-match=e0"]
    );
}

fn refused(result: crate::BigQueryResult<crate::BigQueryTableSyncReport>) -> BigQueryRefusal {
    match result {
        Err(BigQueryError::SchemaChangeRefused(err)) => {
            assert!(!err.plan.impossible.is_empty(), "{}", err.plan);
            err.plan.refusal.clone().expect("a refusal")
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
}

#[tokio::test]
async fn an_impossible_change_without_an_opt_in_writes_nothing() {
    let fake = start(Scenario::new(Some(live_table()))).await;
    let result = fake
        .db
        .fluent()
        .schema()
        .table(SHOP.table(ORDERS))
        .columns(|columns| {
            columns.fields([
                columns.field("id").int64().required(),
                columns.field("name").string(),
                columns.field("quantity").string(),
                columns.field("added").string(),
            ])
        })
        .sync()
        .await;
    assert_eq!(refused(result), BigQueryRefusal::NoRecreateOptIn);
    assert_eq!(fake.calls(), ["GetTable shop.orders"]);
}

#[tokio::test]
async fn recreate_if_empty_refuses_a_table_with_rows() {
    let fake = start(Scenario::new(Some(live_table()))).await;
    let result = fake
        .db
        .fluent()
        .schema()
        .table(SHOP.table(ORDERS))
        .columns(|columns| {
            columns.fields([
                columns.field("id").int64().required(),
                columns.field("quantity").string(),
            ])
        })
        .recreate_if_empty()
        .sync()
        .await;
    assert_eq!(
        refused(result),
        BigQueryRefusal::NotEmpty { num_rows: Some(5) }
    );
    assert_eq!(fake.calls(), ["GetTable shop.orders"]);
}

#[tokio::test]
async fn recreate_if_empty_replaces_an_empty_table_and_lists_its_policies() {
    let mut table = live_table();
    table.num_rows = Some(0);
    let fake = start(Scenario::new(Some(table))).await;
    let report = fake
        .db
        .fluent()
        .schema()
        .table(SHOP.table(ORDERS))
        .columns(|columns| {
            columns.fields([
                columns.field("id").int64().required(),
                columns.field("quantity").string(),
            ])
        })
        .prune_undeclared()
        .recreate_if_empty()
        .sync()
        .await
        .expect("a sync");
    assert_eq!(
        fake.calls(),
        [
            "GetTable shop.orders".to_string(),
            "ListRowAccessPolicies shop.orders".into(),
            format!(
                "Query CREATE OR REPLACE TABLE {TABLE_SQL} (\n  `id` INT64 NOT NULL,\n  \
                 `quantity` STRING\n)"
            ),
        ]
    );
    let recreated = report.recreated.clone().expect("a recreate");
    assert_eq!(recreated.method, BigQueryRecreateMethod::CreateOrReplace);
    assert_eq!(recreated.row_access_policies, ["rap1"]);
    assert!(report.dropped_data.is_empty(), "{report:?}");
}

#[tokio::test]
async fn a_dangerous_partitioning_change_snapshots_then_drops_and_creates() {
    let fake = start(Scenario::new(Some(live_table()))).await;
    let report = fake
        .db
        .fluent()
        .schema()
        .table(SHOP.table(ORDERS))
        .columns(|columns| {
            columns.fields([
                columns.field("id").int64().required(),
                columns.field("day").date(),
            ])
        })
        .partition_by_day("day")
        .prune_undeclared()
        .dangerously_recreate_with_data_loss()
        .snapshot_first()
        .sync()
        .await
        .expect("a sync");
    let calls = fake.calls();
    assert_eq!(calls.len(), 4, "{calls:#?}");
    assert_eq!(
        calls[..2],
        ["GetTable shop.orders", "ListRowAccessPolicies shop.orders"]
    );
    let snapshot = report.snapshot.as_ref().expect("a snapshot");
    assert!(
        snapshot.table().as_str().starts_with("orders_snapshot_"),
        "{snapshot}"
    );
    assert_eq!(
        calls[2],
        format!(
            "Query CREATE SNAPSHOT TABLE `fake-project`.`shop`.`{}` CLONE {TABLE_SQL}",
            snapshot.table()
        )
    );
    assert!(
        calls[3].starts_with(&format!(
            "Query DROP TABLE {TABLE_SQL};\nCREATE TABLE {TABLE_SQL} ("
        )),
        "{}",
        calls[3]
    );
    assert!(calls[3].contains("\nPARTITION BY `day`"), "{}", calls[3]);
    assert_eq!(
        report.dropped_data,
        [BigQueryDroppedData::Rows {
            num_rows: Some(5),
            num_bytes: Some(160)
        }]
    );
}

#[tokio::test]
async fn a_missing_table_is_created_with_insert_table() {
    let fake = start(Scenario::new(None)).await;
    let report = fake
        .db
        .fluent()
        .schema()
        .table(SHOP.table(ORDERS))
        .columns(|columns| columns.fields([columns.field("id").int64().required()]))
        .description("Orders")
        .sync()
        .await
        .expect("a sync");
    assert_eq!(
        fake.calls(),
        ["GetTable shop.orders", "InsertTable shop.orders"]
    );
    let created = report.created.expect("a create");
    assert_eq!(created.description.as_deref(), Some("Orders"));
}

#[tokio::test]
async fn plan_only_reads() {
    let fake = start(Scenario::new(Some(live_table()))).await;
    let plan = fake
        .db
        .fluent()
        .schema()
        .table(SHOP.table(ORDERS))
        .columns(|columns| {
            columns.fields([
                columns.field("id").int64().required(),
                columns.field("name").string(),
                columns.field("quantity").int64(),
                columns.field("new").string(),
            ])
        })
        .prune_undeclared()
        .plan()
        .await
        .expect("a plan");
    assert_eq!(plan.changes.len(), 3, "{plan}");
    assert_eq!(fake.calls(), ["GetTable shop.orders"]);
}

#[tokio::test]
async fn withheld_changes_are_reported_and_not_sent() {
    let fake = start(Scenario::new(Some(live_table()))).await;
    let report = fake
        .db
        .fluent()
        .schema()
        .table(SHOP.table(ORDERS))
        .columns(|columns| {
            columns.fields([
                columns.field("id").int64().required(),
                columns.field("name").string(),
                columns.field("quantity").numeric(),
            ])
        })
        .sync()
        .await
        .expect("a sync");
    assert_eq!(fake.calls(), ["GetTable shop.orders"]);
    assert!(report.applied.is_empty(), "{report}");
    assert_eq!(
        report.withheld.len(),
        3,
        "widen quantity, drop note, remove label owner: {report}"
    );
}

#[tokio::test]
async fn an_invalid_declaration_is_refused_before_any_request() {
    let fake = start(Scenario::new(Some(live_table()))).await;
    for names in [
        vec!["order.total"],
        vec![""],
        vec!["note\u{0}text"],
        vec!["status", "STATUS"],
    ] {
        let result = fake
            .db
            .fluent()
            .schema()
            .table(SHOP.table(ORDERS))
            .columns(|columns| {
                columns.fields(names.iter().map(|name| columns.field(*name).string()))
            })
            .sync()
            .await;
        assert!(
            matches!(result, Err(BigQueryError::InvalidParametersError(_))),
            "{names:?}: {result:?}"
        );
    }
    assert!(fake.calls().is_empty(), "{:?}", fake.calls());
}
