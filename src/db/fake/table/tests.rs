//! `.plan()` and `.sync()` against the fake server: what each sends, in which order, with which
//! precondition, and what it refuses to send.

use super::*;
use crate::db::fake::query::{job_reference, query_request};
use crate::db::fake::FakeBigQuery;
use crate::errors::BigQueryError;
use crate::{
    BigQueryDatasetId, BigQueryDroppedData, BigQueryRecreateMethod, BigQueryRefusal,
    BigQueryTableId, BigQueryTableRef,
};
use gcloud_sdk::google::cloud::bigquery::v2::{
    self as v2, ListRowAccessPoliciesResponse, QueryResponse, RowAccessPolicy,
    RowAccessPolicyReference,
};
use gcloud_sdk::tonic::Code;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

fn orders() -> BigQueryTableRef {
    BigQueryDatasetId::from_static("ds").table(BigQueryTableId::from_static("t"))
}

fn f(name: &str, ty: &str, mode: &str) -> v2::TableFieldSchema {
    v2::TableFieldSchema {
        name: name.into(),
        r#type: ty.into(),
        mode: mode.into(),
        ..Default::default()
    }
}

/// `id INTEGER REQUIRED, name STRING, n INTEGER, x STRING`, label `old: x`, 5 rows, etag `e0`.
fn live_table() -> v2::Table {
    v2::Table {
        etag: "e0".into(),
        r#type: "TABLE".into(),
        table_reference: Some(v2::TableReference {
            project_id: "fake-project".into(),
            dataset_id: "ds".into(),
            table_id: "t".into(),
        }),
        schema: Some(v2::TableSchema {
            fields: vec![
                f("id", "INTEGER", "REQUIRED"),
                f("name", "STRING", "NULLABLE"),
                f("n", "INTEGER", "NULLABLE"),
                f("x", "STRING", "NULLABLE"),
            ],
            ..Default::default()
        }),
        labels: HashMap::from([("old".to_string(), "x".to_string())]),
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
        let n = self.etags.fetch_add(1, Ordering::SeqCst);
        v2::Table {
            etag: format!("e{n}"),
            ..body.unwrap_or_default()
        }
    }

    async fn serve(self, mut call: FakeCall) {
        match call.method() {
            "GetTable" => {
                get_table_request(&mut call).await;
                match &self.table {
                    Some(table) => call.reply(table),
                    None => call.fail(Code::NotFound, "Not found: Table fake-project:ds.t"),
                }
            }
            "PatchTable" | "UpdateTable" => {
                let request = patch_or_update_request(&mut call).await;
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
                let request = insert_table_request(&mut call).await;
                let table = self.written(request.table);
                call.reply(&table)
            }
            "ListRowAccessPolicies" => {
                list_row_access_policies_request(&mut call).await;
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
                query_request(&mut call).await;
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

const TABLE_SQL: &str = "`fake-project`.`ds`.`t`";

#[tokio::test]
async fn sync_writes_in_fixed_order_with_the_read_etag_as_precondition() {
    let fake = start(Scenario::new(Some(live_table()))).await;
    let report = fake
        .db
        .fluent()
        .schema()
        .table(orders())
        .columns(|c| {
            c.fields([
                c.field("id").int64().required(),
                c.field("name2").string().renamed_from("name"),
                c.field("n").numeric(),
                c.field("c").string().default_value("'z'"),
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
            "GetTable ds.t".to_string(),
            "PatchTable if-match=e0".into(),
            "PatchTable if-match=e1".into(),
            "UpdateTable if-match=e2".into(),
            format!("Query ALTER TABLE {TABLE_SQL} RENAME COLUMN `name` TO `name2`"),
            format!("Query ALTER TABLE {TABLE_SQL} ALTER COLUMN `n` SET DATA TYPE NUMERIC"),
            format!("Query ALTER TABLE {TABLE_SQL} DROP COLUMN `x`"),
        ]
    );
    assert_eq!(report.applied.len(), 5, "{report}");
    assert_eq!(
        report.dropped_data,
        [BigQueryDroppedData::Column { column: "x".into() }]
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
        .table(orders())
        .columns(|c| {
            c.fields([
                c.field("id").int64().required(),
                c.field("name").string(),
                c.field("n").int64(),
                c.field("new").string(),
            ])
        })
        .prune_undeclared()
        .sync()
        .await;
    match result {
        Err(BigQueryError::DataConflictError(err)) => {
            assert!(err.details.contains("changed since"), "{err}")
        }
        other => panic!("expected a conflict, got {other:?}"),
    }
    assert_eq!(fake.calls(), ["GetTable ds.t", "PatchTable if-match=e0"]);
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
        .table(orders())
        .columns(|c| {
            c.fields([
                c.field("id").int64().required(),
                c.field("name").string(),
                c.field("n").string(),
                c.field("added").string(),
            ])
        })
        .sync()
        .await;
    assert_eq!(refused(result), BigQueryRefusal::NoRecreateOptIn);
    assert_eq!(fake.calls(), ["GetTable ds.t"]);
}

#[tokio::test]
async fn recreate_if_empty_refuses_a_table_with_rows() {
    let fake = start(Scenario::new(Some(live_table()))).await;
    let result = fake
        .db
        .fluent()
        .schema()
        .table(orders())
        .columns(|c| c.fields([c.field("id").int64().required(), c.field("n").string()]))
        .recreate_if_empty()
        .sync()
        .await;
    assert_eq!(
        refused(result),
        BigQueryRefusal::NotEmpty { num_rows: Some(5) }
    );
    assert_eq!(fake.calls(), ["GetTable ds.t"]);
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
        .table(orders())
        .columns(|c| c.fields([c.field("id").int64().required(), c.field("n").string()]))
        .prune_undeclared()
        .recreate_if_empty()
        .sync()
        .await
        .expect("a sync");
    assert_eq!(
        fake.calls(),
        [
            "GetTable ds.t".to_string(),
            "ListRowAccessPolicies ds.t".into(),
            format!(
                "Query CREATE OR REPLACE TABLE {TABLE_SQL} (\n  `id` INT64 NOT NULL,\n  \
                 `n` STRING\n)"
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
        .table(orders())
        .columns(|c| c.fields([c.field("id").int64().required(), c.field("day").date()]))
        .partition_by_day("day")
        .prune_undeclared()
        .dangerously_recreate_with_data_loss()
        .snapshot_first()
        .sync()
        .await
        .expect("a sync");
    let calls = fake.calls();
    assert_eq!(calls.len(), 4, "{calls:#?}");
    assert_eq!(calls[..2], ["GetTable ds.t", "ListRowAccessPolicies ds.t"]);
    let snapshot = report.snapshot.as_ref().expect("a snapshot");
    assert!(
        snapshot.table().as_str().starts_with("t_snapshot_"),
        "{snapshot}"
    );
    assert_eq!(
        calls[2],
        format!(
            "Query CREATE SNAPSHOT TABLE `fake-project`.`ds`.`{}` CLONE {TABLE_SQL}",
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
        .table(orders())
        .columns(|c| c.fields([c.field("id").int64().required()]))
        .description("Orders")
        .sync()
        .await
        .expect("a sync");
    assert_eq!(fake.calls(), ["GetTable ds.t", "InsertTable ds.t"]);
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
        .table(orders())
        .columns(|c| {
            c.fields([
                c.field("id").int64().required(),
                c.field("name").string(),
                c.field("n").int64(),
                c.field("new").string(),
            ])
        })
        .prune_undeclared()
        .plan()
        .await
        .expect("a plan");
    assert_eq!(plan.changes.len(), 3, "{plan}");
    assert_eq!(fake.calls(), ["GetTable ds.t"]);
}

#[tokio::test]
async fn withheld_changes_are_reported_and_not_sent() {
    let fake = start(Scenario::new(Some(live_table()))).await;
    let report = fake
        .db
        .fluent()
        .schema()
        .table(orders())
        .columns(|c| {
            c.fields([
                c.field("id").int64().required(),
                c.field("name").string(),
                c.field("n").numeric(),
            ])
        })
        .sync()
        .await
        .expect("a sync");
    assert_eq!(fake.calls(), ["GetTable ds.t"]);
    assert!(report.applied.is_empty(), "{report}");
    assert_eq!(
        report.withheld.len(),
        3,
        "widen n, drop x, remove label old: {report}"
    );
}

#[tokio::test]
async fn an_invalid_declaration_is_refused_before_any_request() {
    let fake = start(Scenario::new(Some(live_table()))).await;
    for columns in [vec!["a.b"], vec![""], vec!["a\u{0}b"], vec!["dup", "DUP"]] {
        let result = fake
            .db
            .fluent()
            .schema()
            .table(orders())
            .columns(|c| c.fields(columns.iter().map(|name| c.field(*name).string())))
            .sync()
            .await;
        assert!(
            matches!(result, Err(BigQueryError::InvalidParametersError(_))),
            "{columns:?}: {result:?}"
        );
    }
    assert!(fake.calls().is_empty(), "{:?}", fake.calls());
}
