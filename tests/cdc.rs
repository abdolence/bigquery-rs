//! Live CDC: upserts and a delete by primary key through the default stream.

use bigquery::*;
use serde::Serialize;

#[path = "support/common.rs"]
mod common;
use common::*;

#[path = "support/write_support.rs"]
mod write_support;
use write_support::*;

#[derive(Serialize)]
struct Row {
    id: i64,
    name: String,
}

#[tokio::test]
async fn cdc_upsert_then_delete() -> TestResult {
    with_scratch("cdc_upsert_then_delete", async |scratch: &Scratch| {
        let column = |name: &str, field_type, mode| BigQueryFieldSchema {
            name: name.into(),
            field_type,
            mode,
            description: None,
            default_value_expression: None,
        };
        let table = create_table(
            scratch,
            "t",
            vec![
                column("id", BigQueryFieldType::Int64, BigQueryFieldMode::Required),
                column(
                    "name",
                    BigQueryFieldType::String { max_length: None },
                    BigQueryFieldMode::Nullable,
                ),
            ],
            Some("id"),
        )
        .await?;
        let (mut writer, _) = scratch
            .db
            .create_cdc_writer::<Row>(table.clone(), BigQueryStreamingWriteOptions::new())
            .await?;
        let change = |change_type, sequence: u64, id: i64, name: &str| BigQueryChange {
            change_type,
            sequence_number: Some(BigQueryChangeSequenceNumber::from(sequence)),
            row: Row {
                id,
                name: name.into(),
            },
        };
        for c in [
            change(BigQueryChangeType::Upsert, 1, 1, "a"),
            change(BigQueryChangeType::Upsert, 2, 1, "b"),
            change(BigQueryChangeType::Upsert, 3, 2, "c"),
            change(BigQueryChangeType::Delete, 4, 2, ""),
        ] {
            writer.write_change(&c).await?;
        }
        let summary = writer.finish().await?;
        assert_eq!((summary.rows_written, summary.rows_failed), (4, 0));
        scratch
            .db
            .fluent()
            .insert()
            .into(table)
            .object(&Row {
                id: 3,
                name: "d".into(),
            })
            .upsert()
            .execute()
            .await?;
        let (rows, billed) = query_rows(
            scratch,
            &format!(
                "SELECT id, name FROM {} ORDER BY id",
                scratch.table_sql("t")
            ),
        )
        .await?;
        let expected = |id: &str, name: &str| vec![Some(id.to_string()), Some(name.to_string())];
        assert_eq!(rows, [expected("1", "b"), expected("3", "d")]);
        eprintln!("cdc_upsert_then_delete: 5 changes written, {billed} bytes billed");
        Ok(())
    })
    .await
}

#[derive(Serialize)]
struct Order {
    id: i64,
    status: Option<String>,
}

impl Order {
    fn new(id: i64, status: Option<&str>) -> Self {
        Self {
            id,
            status: status.map(str::to_string),
        }
    }
}

#[derive(Serialize)]
struct OrderKey {
    id: i64,
}

/// The table has no `max_staleness`, so a query merges every change at query time and sees
/// them all as soon as `execute` returns.
#[tokio::test]
async fn fluent_update_replaces_whole_rows_and_delete_needs_only_the_key() -> TestResult {
    with_scratch(
        "fluent_update_replaces_whole_rows_and_delete_needs_only_the_key",
        async |scratch: &Scratch| {
            let table = create_table(
                scratch,
                "orders",
                vec![
                    BigQueryFieldSchema {
                        name: "id".into(),
                        field_type: BigQueryFieldType::Int64,
                        mode: BigQueryFieldMode::Required,
                        description: None,
                        default_value_expression: None,
                    },
                    BigQueryFieldSchema {
                        name: "status".into(),
                        field_type: BigQueryFieldType::String { max_length: None },
                        mode: BigQueryFieldMode::Nullable,
                        description: None,
                        default_value_expression: None,
                    },
                ],
                Some("id"),
            )
            .await?;
            scratch
                .db
                .fluent()
                .update()
                .in_table(table.clone())
                .objects(&[
                    Order::new(1, Some("placed")),
                    Order::new(2, Some("placed")),
                    Order::new(3, Some("placed")),
                ])
                .execute()
                .await?;
            scratch
                .db
                .fluent()
                .update()
                .in_table(table.clone())
                .object(&Order::new(1, None))
                .execute()
                .await?;
            let by_object = scratch
                .db
                .fluent()
                .delete()
                .from(table.clone())
                .object(&OrderKey { id: 2 })
                .execute()
                .await?;
            let by_key = scratch
                .db
                .fluent()
                .delete()
                .from(table)
                .key(3)
                .execute()
                .await?;
            assert_eq!((by_object.rows_written, by_object.rows_failed), (1, 0));
            assert_eq!((by_key.rows_written, by_key.rows_failed), (1, 0));

            let (rows, billed) = query_rows(
                scratch,
                &format!(
                    "SELECT id, status FROM {} ORDER BY id",
                    scratch.table_sql("orders")
                ),
            )
            .await?;
            assert_eq!(rows, [vec![Some("1".to_string()), None]]);
            eprintln!("fluent update and delete: 6 changes written, {billed} bytes billed");
            Ok(())
        },
    )
    .await
}
