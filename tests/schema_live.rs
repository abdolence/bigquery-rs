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
async fn live_table(s: &Scratch) -> TestResult<bq::Table> {
    Ok(s.db
        .table_client()
        .get_table(bq::GetTableRequest {
            project_id: s.project.clone(),
            dataset_id: s.dataset.to_string(),
            table_id: ORDERS.to_string(),
            ..Default::default()
        })
        .await
        .map_err(BigQueryError::from)?
        .into_inner())
}

async fn live_field(s: &Scratch, name: &str) -> TestResult<Option<bq::TableFieldSchema>> {
    Ok(live_table(s)
        .await?
        .schema
        .unwrap_or_default()
        .fields
        .into_iter()
        .find(|f| f.name == name))
}

/// `id INT64 REQUIRED, name STRING REQUIRED, n INT64, x STRING`.
fn base(c: BigQuerySchemaColumnsBuilder) -> Vec<BigQuerySchemaColumn> {
    c.fields([
        c.field("id").int64().required(),
        c.field("name").string().required(),
        c.field("n").int64(),
        c.field("x").string(),
    ])
}

async fn create_base(s: &Scratch) -> TestResult {
    let report =
        s.db.fluent()
            .schema()
            .table(s.dataset.table(ORDERS))
            .columns(base)
            .sync()
            .await?;
    assert!(report.created.is_some(), "{report}");
    Ok(())
}

/// `ORDERS` with every column, key, partitioning, clustering and table setting given.
fn full_declaration(s: &Scratch) -> BigQueryTableSchemaBuilder<'_> {
    s.db.fluent()
        .schema()
        .table(s.dataset.table(ORDERS))
        .columns(|c| {
            c.fields([
                c.field("id").int64().required().description("key"),
                c.field("customer").string_with_max_length(64),
                c.field("total").numeric_with(10, 2),
                c.field("ship")
                    .record(|r| r.fields([r.field("city").string()])),
                c.field("tags").string().repeated(),
                c.field("placed_at").timestamp(),
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
    with_scratch("create_then_plan_shows_no_changes", async |s: &Scratch| {
        let report = full_declaration(s).sync().await?;
        assert!(report.created.is_some(), "{report}");
        let plan = full_declaration(s).plan().await?;
        assert!(plan.is_empty(), "{plan}");
        Ok(())
    })
    .await
}

#[tokio::test]
async fn add_a_column_and_one_with_a_default() -> TestResult {
    with_scratch(
        "add_a_column_and_one_with_a_default",
        async |s: &Scratch| {
            create_base(s).await?;
            let report =
                s.db.fluent()
                    .schema()
                    .table(s.dataset.table(ORDERS))
                    .columns(|c| {
                        let mut columns = base(c);
                        columns.push(c.field("added").string());
                        columns.push(c.field("flag").string().default_value("'none'"));
                        columns
                    })
                    .sync()
                    .await?;
            assert_eq!(report.applied.len(), 2, "{report}");
            assert!(live_field(s, "added").await?.is_some());
            assert_eq!(
                live_field(s, "flag")
                    .await?
                    .and_then(|f| f.default_value_expression),
                Some("'none'".to_string())
            );
            Ok(())
        },
    )
    .await
}

#[tokio::test]
async fn relax_a_column() -> TestResult {
    with_scratch("relax_a_column", async |s: &Scratch| {
        create_base(s).await?;
        let report =
            s.db.fluent()
                .schema()
                .table(s.dataset.table(ORDERS))
                .columns(|c| {
                    c.fields([
                        c.field("id").int64().required(),
                        c.field("name").string(),
                        c.field("n").int64(),
                        c.field("x").string(),
                    ])
                })
                .sync()
                .await?;
        assert_eq!(report.applied.len(), 1, "{report}");
        assert_eq!(
            live_field(s, "name").await?.map(|f| f.mode),
            Some("NULLABLE".to_string())
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn rename_a_column() -> TestResult {
    with_scratch("rename_a_column", async |s: &Scratch| {
        create_base(s).await?;
        let report =
            s.db.fluent()
                .schema()
                .table(s.dataset.table(ORDERS))
                .columns(|c| {
                    c.fields([
                        c.field("id").int64().required(),
                        c.field("name").string().required(),
                        c.field("n").int64(),
                        c.field("x2").string().renamed_from("x"),
                    ])
                })
                .sync()
                .await?;
        assert_eq!(report.applied.len(), 1, "{report}");
        assert!(live_field(s, "x2").await?.is_some());
        assert!(live_field(s, "x").await?.is_none());
        Ok(())
    })
    .await
}

#[tokio::test]
async fn widen_a_column() -> TestResult {
    with_scratch("widen_a_column", async |s: &Scratch| {
        create_base(s).await?;
        let report =
            s.db.fluent()
                .schema()
                .table(s.dataset.table(ORDERS))
                .columns(|c| {
                    c.fields([
                        c.field("id").int64().required(),
                        c.field("name").string().required(),
                        c.field("n").numeric(),
                        c.field("x").string(),
                    ])
                })
                .allow_widening()
                .sync()
                .await?;
        assert_eq!(report.applied.len(), 1, "{report}");
        assert_eq!(
            live_field(s, "n").await?.map(|f| f.r#type),
            Some("NUMERIC".to_string())
        );
        Ok(())
    })
    .await
}

/// The base columns without `x`.
fn base_without_x(s: &Scratch) -> BigQueryTableSchemaBuilder<'_> {
    s.db.fluent()
        .schema()
        .table(s.dataset.table(ORDERS))
        .columns(|c| {
            c.fields([
                c.field("id").int64().required(),
                c.field("name").string().required(),
                c.field("n").int64(),
            ])
        })
}

#[tokio::test]
async fn prune_drops_an_undeclared_column() -> TestResult {
    with_scratch("prune_drops_an_undeclared_column", async |s: &Scratch| {
        create_base(s).await?;
        let kept = base_without_x(s).sync().await?;
        assert_eq!(kept.withheld.len(), 1, "{kept}");
        assert!(live_field(s, "x").await?.is_some());

        let pruned = base_without_x(s).prune_undeclared().sync().await?;
        assert_eq!(
            pruned.dropped_data,
            [BigQueryDroppedData::Column { column: "x".into() }],
            "{pruned}"
        );
        assert!(live_field(s, "x").await?.is_none());
        Ok(())
    })
    .await
}

/// The base columns with `n` as STRING, a change only a recreate can make.
fn base_with_string_n(s: &Scratch) -> BigQueryTableSchemaBuilder<'_> {
    s.db.fluent()
        .schema()
        .table(s.dataset.table(ORDERS))
        .columns(|c| {
            c.fields([
                c.field("id").int64().required(),
                c.field("name").string().required(),
                c.field("n").string(),
                c.field("x").string(),
            ])
        })
}

#[tokio::test]
async fn recreate_if_empty_replaces_an_empty_table() -> TestResult {
    with_scratch(
        "recreate_if_empty_replaces_an_empty_table",
        async |s: &Scratch| {
            create_base(s).await?;
            match base_with_string_n(s).sync().await {
                Err(BigQueryError::SchemaChangeRefused(err)) => {
                    assert_eq!(err.plan.refusal, Some(BigQueryRefusal::NoRecreateOptIn))
                }
                other => panic!("expected a refusal, got {other:?}"),
            }
            let report = base_with_string_n(s).recreate_if_empty().sync().await?;
            let recreated = report.recreated.as_ref().expect("a recreate");
            assert_eq!(recreated.method, BigQueryRecreateMethod::CreateOrReplace);
            assert_eq!(
                live_field(s, "n").await?.map(|f| f.r#type),
                Some("STRING".to_string())
            );
            Ok(())
        },
    )
    .await
}

/// `id`, `ts`, `k` and `n`, with `n` an INT64 or a STRING, and `ts` and `k` with or without
/// defaults; `k`'s default ends in a `--` comment.
fn defaults_declaration(
    s: &Scratch,
    n_is_string: bool,
    with_defaults: bool,
) -> BigQueryTableSchemaBuilder<'_> {
    s.db.fluent()
        .schema()
        .table(s.dataset.table(ORDERS))
        .columns(move |c| {
            let mut ts = c.field("ts").timestamp();
            let mut k = c.field("k").int64();
            if with_defaults {
                ts = ts.default_value("CURRENT_TIMESTAMP()");
                k = k.default_value("1 -- one");
            }
            let n = c.field("n");
            c.fields([
                c.field("id").int64().required(),
                ts,
                k,
                if n_is_string { n.string() } else { n.int64() },
            ])
        })
}

fn default_expression(field: Option<bq::TableFieldSchema>) -> Option<String> {
    field.and_then(|f| f.default_value_expression)
}

/// A recreate copies the defaults the declaration leaves out from the live table into the
/// `CREATE OR REPLACE`. A live default may end in a `--` comment, which `PatchTable` and
/// `InsertTable` store as given, so the statement only parses if the default is its own operand.
#[tokio::test]
async fn a_recreate_keeps_live_defaults() -> TestResult {
    with_scratch("a_recreate_keeps_live_defaults", async |s: &Scratch| {
        defaults_declaration(s, false, true).sync().await?;
        assert_eq!(
            default_expression(live_field(s, "k").await?).as_deref(),
            Some("1 -- one")
        );

        let report = defaults_declaration(s, true, false)
            .recreate_if_empty()
            .sync()
            .await?;
        assert!(report.recreated.is_some(), "{report}");
        assert_eq!(
            live_field(s, "n").await?.map(|f| f.r#type),
            Some("STRING".to_string())
        );
        assert_eq!(
            default_expression(live_field(s, "ts").await?).as_deref(),
            Some("CURRENT_TIMESTAMP()")
        );
        assert_eq!(
            default_expression(live_field(s, "k").await?).as_deref(),
            Some("1")
        );
        Ok(())
    })
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

/// The base columns with `n` as STRING, `description` on the table and on `id`, and `label`
/// as the `team` label, recreated if empty.
fn hostile_declaration<'a>(
    s: &'a Scratch,
    description: &str,
    label: &str,
) -> BigQueryTableSchemaBuilder<'a> {
    s.db.fluent()
        .schema()
        .table(s.dataset.table(ORDERS))
        .columns(|c| {
            c.fields([
                c.field("id").int64().required().description(description),
                c.field("name").string().required(),
                c.field("n").string(),
                c.field("x").string(),
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
        async |s: &Scratch| {
            create_base(s).await?;
            let description = hostile_description();

            // BigQuery's own label rules reject this value; the statement fails as a whole and the
            // table keeps its old schema, so the value never left its literal.
            let hostile_label =
                hostile_declaration(s, &description, "x'), ('team', 'y'); DROP TABLE t; --")
                    .sync()
                    .await;
            assert!(
                matches!(&hostile_label, Err(BigQueryError::DatabaseError(_))),
                "{hostile_label:?}"
            );
            assert_eq!(
                live_field(s, "n").await?.map(|f| f.r#type),
                Some("INTEGER".to_string())
            );

            let label = "ünïcödé-ß_1";
            hostile_declaration(s, &description, label).sync().await?;
            let table = live_table(s).await?;
            assert_eq!(table.description.as_deref(), Some(description.as_str()));
            assert_eq!(table.labels.get("team").map(String::as_str), Some(label));
            assert_eq!(table.labels.len(), 1, "{:?}", table.labels);
            let id = table
                .schema
                .unwrap_or_default()
                .fields
                .into_iter()
                .find(|f| f.name == "id");
            assert_eq!(id.and_then(|f| f.description), Some(description.clone()));
            Ok(())
        },
    )
    .await
}
