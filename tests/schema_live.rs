//! Live schema syncs, each on a tiny table of its own in the CI dataset, read back with
//! `GetTable`. Every statement is DDL or table metadata, which bills nothing.
//!
//! A recreate first lists the table's row access policies, which the CI account may not do,
//! so recreates are left to the unit tests against the fake server.

use bigquery::errors::BigQueryError;
use bigquery::*;
use gcloud_sdk::google::cloud::bigquery::v2 as bq;

#[path = "support/common.rs"]
mod common;
use common::*;

const ORDERS: &str = "orders";

/// The table `ORDERS` as `GetTable` returns it.
async fn live_table(scratch: &Scratch) -> TestResult<bq::Table> {
    Ok(scratch
        .db
        .table_client()
        .get_table(bq::GetTableRequest {
            project_id: scratch.project.clone(),
            dataset_id: scratch.dataset.to_string(),
            table_id: scratch.table_id(ORDERS).to_string(),
            ..Default::default()
        })
        .await
        .map_err(BigQueryError::from)?
        .into_inner())
}

async fn live_field(scratch: &Scratch, name: &str) -> TestResult<Option<bq::TableFieldSchema>> {
    Ok(live_table(scratch)
        .await?
        .schema
        .unwrap_or_default()
        .fields
        .into_iter()
        .find(|field| field.name == name))
}

/// `id INT64 REQUIRED, name STRING REQUIRED, amount INT64, note STRING`.
fn base(columns: BigQuerySchemaColumnsBuilder) -> Vec<BigQuerySchemaColumn> {
    columns.fields([
        columns.field("id").int64().required(),
        columns.field("name").string().required(),
        columns.field("amount").int64(),
        columns.field("note").string(),
    ])
}

async fn create_base(scratch: &Scratch) -> TestResult {
    let report = scratch
        .db
        .fluent()
        .schema()
        .table(scratch.table(ORDERS))
        .columns(base)
        .sync()
        .await?;
    assert!(report.created.is_some(), "{report}");
    Ok(())
}

/// `ORDERS` with every column, key, partitioning, clustering and table setting given.
fn full_declaration(scratch: &Scratch) -> BigQueryTableSchemaBuilder<'_> {
    scratch
        .db
        .fluent()
        .schema()
        .table(scratch.table(ORDERS))
        .columns(|columns| {
            columns.fields([
                columns.field("id").int64().required().description("key"),
                columns.field("customer").string_with_max_length(64),
                columns.field("total").numeric_with(10, 2),
                columns
                    .field("ship")
                    .record(|address| address.fields([address.field("city").string()])),
                columns.field("tags").string().repeated(),
                columns.field("placed_at").timestamp(),
            ])
        })
        .primary_key(["id"])
        .partition_by_day("placed_at")
        .cluster_by(["customer"])
        .description("Orders")
        .labels([("team", "shop")])
}

#[tokio::test]
async fn create_then_plan_shows_no_changes() -> TestResult {
    with_scratch(
        "create_then_plan_shows_no_changes",
        async |scratch: &Scratch| {
            let report = full_declaration(scratch).sync().await?;
            assert!(report.created.is_some(), "{report}");
            let plan = full_declaration(scratch).plan().await?;
            assert!(plan.is_empty(), "{plan}");
            Ok(())
        },
    )
    .await
}

#[tokio::test]
async fn add_a_column_and_one_with_a_default() -> TestResult {
    with_scratch(
        "add_a_column_and_one_with_a_default",
        async |scratch: &Scratch| {
            create_base(scratch).await?;
            let report = scratch
                .db
                .fluent()
                .schema()
                .table(scratch.table(ORDERS))
                .columns(|columns| {
                    let mut declared = base(columns);
                    declared.push(columns.field("added").string());
                    declared.push(columns.field("flag").string().default_value("'none'"));
                    declared
                })
                .sync()
                .await?;
            assert_eq!(report.applied.len(), 2, "{report}");
            assert!(live_field(scratch, "added").await?.is_some());
            assert_eq!(
                live_field(scratch, "flag")
                    .await?
                    .and_then(|field| field.default_value_expression),
                Some("'none'".to_string())
            );
            Ok(())
        },
    )
    .await
}

#[tokio::test]
async fn relax_a_column() -> TestResult {
    with_scratch("relax_a_column", async |scratch: &Scratch| {
        create_base(scratch).await?;
        let report = scratch
            .db
            .fluent()
            .schema()
            .table(scratch.table(ORDERS))
            .columns(|columns| {
                columns.fields([
                    columns.field("id").int64().required(),
                    columns.field("name").string(),
                    columns.field("amount").int64(),
                    columns.field("note").string(),
                ])
            })
            .sync()
            .await?;
        assert_eq!(report.applied.len(), 1, "{report}");
        assert_eq!(
            live_field(scratch, "name").await?.map(|field| field.mode),
            Some("NULLABLE".to_string())
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn rename_a_column() -> TestResult {
    with_scratch("rename_a_column", async |scratch: &Scratch| {
        create_base(scratch).await?;
        let report = scratch
            .db
            .fluent()
            .schema()
            .table(scratch.table(ORDERS))
            .columns(|columns| {
                columns.fields([
                    columns.field("id").int64().required(),
                    columns.field("name").string().required(),
                    columns.field("amount").int64(),
                    columns.field("remark").string().renamed_from("note"),
                ])
            })
            .sync()
            .await?;
        assert_eq!(report.applied.len(), 1, "{report}");
        assert!(live_field(scratch, "remark").await?.is_some());
        assert!(live_field(scratch, "note").await?.is_none());
        Ok(())
    })
    .await
}

#[tokio::test]
async fn widen_a_column() -> TestResult {
    with_scratch("widen_a_column", async |scratch: &Scratch| {
        create_base(scratch).await?;
        let report = scratch
            .db
            .fluent()
            .schema()
            .table(scratch.table(ORDERS))
            .columns(|columns| {
                columns.fields([
                    columns.field("id").int64().required(),
                    columns.field("name").string().required(),
                    columns.field("amount").numeric(),
                    columns.field("note").string(),
                ])
            })
            .allow_widening()
            .sync()
            .await?;
        assert_eq!(report.applied.len(), 1, "{report}");
        assert_eq!(
            live_field(scratch, "amount")
                .await?
                .map(|field| field.r#type),
            Some("NUMERIC".to_string())
        );
        Ok(())
    })
    .await
}

/// The base columns without `note`.
fn base_without_note(scratch: &Scratch) -> BigQueryTableSchemaBuilder<'_> {
    scratch
        .db
        .fluent()
        .schema()
        .table(scratch.table(ORDERS))
        .columns(|columns| {
            columns.fields([
                columns.field("id").int64().required(),
                columns.field("name").string().required(),
                columns.field("amount").int64(),
            ])
        })
}

#[tokio::test]
async fn prune_drops_an_undeclared_column() -> TestResult {
    with_scratch(
        "prune_drops_an_undeclared_column",
        async |scratch: &Scratch| {
            create_base(scratch).await?;
            let kept = base_without_note(scratch).sync().await?;
            assert_eq!(kept.withheld.len(), 1, "{kept}");
            assert!(live_field(scratch, "note").await?.is_some());

            let pruned = base_without_note(scratch).prune_undeclared().sync().await?;
            assert_eq!(
                pruned.dropped_data,
                [BigQueryDroppedData::Column {
                    column: "note".into()
                }],
                "{pruned}"
            );
            assert!(live_field(scratch, "note").await?.is_none());
            Ok(())
        },
    )
    .await
}
