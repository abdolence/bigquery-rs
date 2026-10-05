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
