//! Live schema syncs, each on a tiny table in its own scratch dataset, read back with
//! `GetTable`. Every statement is DDL or table metadata, which bills nothing.

use bigquery::errors::BigQueryError;
use bigquery::*;
use gcloud_sdk::google::cloud::bigquery::v2 as bq;

#[path = "support/common.rs"]
mod common;
use common::*;

const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");

/// The table `ORDERS` as `GetTable` returns it.
async fn live_table(scratch: &Scratch) -> TestResult<bq::Table> {
    Ok(scratch
        .db
        .table_client()
        .get_table(bq::GetTableRequest {
            project_id: scratch.project.clone(),
            dataset_id: scratch.dataset.to_string(),
            table_id: ORDERS.to_string(),
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
        .table(scratch.dataset.table(ORDERS))
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
        .table(scratch.dataset.table(ORDERS))
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
                .table(scratch.dataset.table(ORDERS))
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
            .table(scratch.dataset.table(ORDERS))
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
            .table(scratch.dataset.table(ORDERS))
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
            .table(scratch.dataset.table(ORDERS))
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
        .table(scratch.dataset.table(ORDERS))
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

/// The base columns with `amount` as STRING, a change only a recreate can make.
fn base_with_string_amount(scratch: &Scratch) -> BigQueryTableSchemaBuilder<'_> {
    scratch
        .db
        .fluent()
        .schema()
        .table(scratch.dataset.table(ORDERS))
        .columns(|columns| {
            columns.fields([
                columns.field("id").int64().required(),
                columns.field("name").string().required(),
                columns.field("amount").string(),
                columns.field("note").string(),
            ])
        })
}

#[tokio::test]
async fn recreate_if_empty_replaces_an_empty_table() -> TestResult {
    with_scratch(
        "recreate_if_empty_replaces_an_empty_table",
        async |scratch: &Scratch| {
            create_base(scratch).await?;
            match base_with_string_amount(scratch).sync().await {
                Err(BigQueryError::SchemaChangeRefused(err)) => {
                    assert_eq!(err.plan.refusal, Some(BigQueryRefusal::NoRecreateOptIn))
                }
                other => panic!("expected a refusal, got {other:?}"),
            }
            let report = base_with_string_amount(scratch)
                .recreate_if_empty()
                .sync()
                .await?;
            let recreated = report.recreated.as_ref().expect("a recreate");
            assert_eq!(recreated.method, BigQueryRecreateMethod::CreateOrReplace);
            assert_eq!(
                live_field(scratch, "amount")
                    .await?
                    .map(|field| field.r#type),
                Some("STRING".to_string())
            );
            Ok(())
        },
    )
    .await
}

/// `id`, `created_at`, `quantity` and `amount`, with `amount` an INT64 or a STRING, and `created_at` and `quantity` with or without
/// defaults; `quantity`'s default ends in a `--` comment.
fn defaults_declaration(
    scratch: &Scratch,
    amount_is_string: bool,
    with_defaults: bool,
) -> BigQueryTableSchemaBuilder<'_> {
    scratch
        .db
        .fluent()
        .schema()
        .table(scratch.dataset.table(ORDERS))
        .columns(move |columns| {
            let mut created_at = columns.field("created_at").timestamp();
            let mut quantity = columns.field("quantity").int64();
            if with_defaults {
                created_at = created_at.default_value("CURRENT_TIMESTAMP()");
                quantity = quantity.default_value("1 -- one");
            }
            let amount = columns.field("amount");
            columns.fields([
                columns.field("id").int64().required(),
                created_at,
                quantity,
                if amount_is_string {
                    amount.string()
                } else {
                    amount.int64()
                },
            ])
        })
}

fn default_expression(field: Option<bq::TableFieldSchema>) -> Option<String> {
    field.and_then(|field| field.default_value_expression)
}

/// A recreate copies the defaults the declaration leaves out from the live table into the
/// `CREATE OR REPLACE`. A live default may end in a `--` comment, which `PatchTable` and
/// `InsertTable` store as given, so the statement only parses if the default is its own operand.
#[tokio::test]
async fn a_recreate_keeps_live_defaults() -> TestResult {
    with_scratch(
        "a_recreate_keeps_live_defaults",
        async |scratch: &Scratch| {
            defaults_declaration(scratch, false, true).sync().await?;
            assert_eq!(
                default_expression(live_field(scratch, "quantity").await?).as_deref(),
                Some("1 -- one")
            );

            let report = defaults_declaration(scratch, true, false)
                .recreate_if_empty()
                .sync()
                .await?;
            assert!(report.recreated.is_some(), "{report}");
            assert_eq!(
                live_field(scratch, "amount")
                    .await?
                    .map(|field| field.r#type),
                Some("STRING".to_string())
            );
            assert_eq!(
                default_expression(live_field(scratch, "created_at").await?).as_deref(),
                Some("CURRENT_TIMESTAMP()")
            );
            assert_eq!(
                default_expression(live_field(scratch, "quantity").await?).as_deref(),
                Some("1")
            );
            Ok(())
        },
    )
    .await
}

/// Corpus values a table or column description can hold, joined: quotes, backslashes,
/// comments, statement terminators, newlines and look-alike quotes.
fn hostile_description() -> String {
    [
        "'; DROP TABLE x; --",
        "' OR '1'='1",
        "`backtick`",
        "\\'",
        "\\\\",
        "\"\"\"",
        "'''",
        "/* comment */",
        "*/ --",
        "#",
        "a\nb\tc",
        "\u{2019} OR \u{2019}1\u{2019}=\u{2019}1",
        "\u{FF07}; DROP TABLE x; --",
        "@other_param ?",
    ]
    .join(" | ")
}

/// The base columns with `amount` as STRING, `description` on the table and on `id`, and `label`
/// as the `team` label, recreated if empty.
fn hostile_declaration<'a>(
    scratch: &'a Scratch,
    description: &str,
    label: &str,
) -> BigQueryTableSchemaBuilder<'a> {
    scratch
        .db
        .fluent()
        .schema()
        .table(scratch.dataset.table(ORDERS))
        .columns(|columns| {
            columns.fields([
                columns
                    .field("id")
                    .int64()
                    .required()
                    .description(description),
                columns.field("name").string().required(),
                columns.field("amount").string(),
                columns.field("note").string(),
            ])
        })
        .description(description)
        .labels([("team", label.to_string())])
        .recreate_if_empty()
}

#[tokio::test]
async fn hostile_description_and_label_stay_literals_in_ddl() -> TestResult {
    with_scratch(
        "hostile_description_and_label_stay_literals_in_ddl",
        async |scratch: &Scratch| {
            create_base(scratch).await?;
            let description = hostile_description();

            // BigQuery's own label rules reject this value; the statement fails as a whole and the
            // table keeps its old schema, so the value never left its literal.
            let hostile_label = hostile_declaration(
                scratch,
                &description,
                "x'), ('team', 'y'); DROP TABLE t; --",
            )
            .sync()
            .await;
            assert!(
                matches!(&hostile_label, Err(BigQueryError::DatabaseError(_))),
                "{hostile_label:?}"
            );
            assert_eq!(
                live_field(scratch, "amount")
                    .await?
                    .map(|field| field.r#type),
                Some("INTEGER".to_string())
            );

            let label = "ünïcödé-ß_1";
            hostile_declaration(scratch, &description, label)
                .sync()
                .await?;
            let table = live_table(scratch).await?;
            assert_eq!(table.description.as_deref(), Some(description.as_str()));
            assert_eq!(table.labels.get("team").map(String::as_str), Some(label));
            assert_eq!(table.labels.len(), 1, "{:?}", table.labels);
            let id = table
                .schema
                .unwrap_or_default()
                .fields
                .into_iter()
                .find(|field| field.name == "id");
            assert_eq!(
                id.and_then(|field| field.description),
                Some(description.clone())
            );
            Ok(())
        },
    )
    .await
}
