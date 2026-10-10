//! The `smq` binary's inspection commands over a root a client wrote through the TCP
//! route.

use super::support::{client, serve, PROJECT};
use bigquery::*;
use std::path::Path;
use std::process::Command;

const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");
const EU: BigQueryLocation = BigQueryLocation::from_static("EU");

/// Runs `smq <args> --root <root>` and returns its standard output, failing the test on a
/// non-zero exit.
fn smq(root: &Path, args: &[&str]) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_smq"))
        .args(args)
        .arg("--root")
        .arg(root)
        .output()
        .expect("the binary runs");
    assert!(
        output.status.success(),
        "smq {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("the output is UTF-8")
}

/// The line of `output` whose first word, after a list item's `-`, is `name`.
fn row<'o>(output: &'o str, name: &str) -> &'o str {
    output
        .lines()
        .find(|line| {
            line.split_whitespace()
                .find(|word| *word != "-")
                .is_some_and(|word| word == name)
        })
        .unwrap_or_else(|| panic!("no row for {name} in:\n{output}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_cli_lists_and_shows_what_a_client_created() -> BigQueryResult<()> {
    let served = serve().await;
    let db = client(served.addr).await;
    db.fluent()
        .schema()
        .dataset(SHOP)
        .create()
        .location(EU)
        .execute()
        .await?;
    db.fluent()
        .schema()
        .table(SHOP.table(ORDERS))
        .columns(|columns| {
            columns.fields([
                columns.field("order_id").int64().required(),
                columns.field("amount").numeric_with(10, 2),
                columns.field("placed_at").timestamp().required(),
                columns
                    .field("lines")
                    .repeated()
                    .record(|lines| lines.fields([lines.field("sku").string().required()])),
            ])
        })
        .partition_by_day("placed_at")
        .cluster_by(["order_id"])
        .sync()
        .await?;

    let root = served.root().to_path_buf();
    let (datasets, tables, shown) = tokio::task::spawn_blocking(move || {
        (
            smq(&root, &["datasets"]),
            smq(&root, &["tables", &format!("{PROJECT}.{SHOP}")]),
            smq(&root, &["show", &format!("{PROJECT}.{SHOP}.{ORDERS}")]),
        )
    })
    .await
    .expect("the commands ran");

    let dataset_row = row(&datasets, "shop");
    assert!(dataset_row.contains(PROJECT), "{dataset_row}");
    assert!(dataset_row.contains("EU"), "{dataset_row}");
    row(&tables, "orders");
    for (column, column_type, mode) in [
        ("order_id", "INT64", "REQUIRED"),
        ("amount", "NUMERIC(10,2)", "NULLABLE"),
        ("placed_at", "TIMESTAMP", "REQUIRED"),
        ("lines", "STRUCT", "REPEATED"),
        ("lines.sku", "STRING", "REQUIRED"),
    ] {
        let column_row = row(&shown, column);
        assert!(column_row.contains(column_type), "{column_row}");
        assert!(column_row.contains(mode), "{column_row}");
    }
    assert!(row(&shown, "Partitioning:").contains("DAY on placed_at"));
    assert!(row(&shown, "Clustering:").contains("order_id"));
    Ok(())
}
