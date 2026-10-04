//! Live writes, a handful of rows per test, read back by query.

use bigquery::*;
use serde::Serialize;

mod common;
use common::*;

mod write_support;
use write_support::*;

#[derive(Serialize)]
struct Row {
    id: i64,
    name: String,
    at: BigQueryTimestamp,
}

fn rows(count: i64) -> Vec<Row> {
    (0..count)
        .map(|id| Row {
            id,
            name: format!("row-{id}"),
            at: BigQueryTimestamp(
                jiff::Timestamp::from_second(1_791_000_000 + id).expect("in range"),
            ),
        })
        .collect()
}

fn columns() -> Vec<BigQueryFieldSchema> {
    let column = |name: &str, field_type, mode| BigQueryFieldSchema {
        name: name.into(),
        field_type,
        mode,
        description: None,
        default_value_expression: None,
    };
    vec![
        column("id", BigQueryFieldType::Int64, BigQueryFieldMode::Required),
        column(
            "name",
            BigQueryFieldType::String { max_length: None },
            BigQueryFieldMode::Nullable,
        ),
        column(
            "at",
            BigQueryFieldType::Timestamp,
            BigQueryFieldMode::Nullable,
        ),
    ]
}

/// The single INT64 cell of `sql`, with the bytes billed.
async fn count(scratch: &Scratch, sql: &str) -> TestResult<(i64, i64)> {
    let (rows, billed) = query_rows(scratch, sql).await?;
    let cell = rows
        .first()
        .and_then(|row| row.first().cloned().flatten())
        .ok_or_else(|| format!("{sql} returned no value"))?;
    Ok((cell.parse()?, billed))
}

#[tokio::test]
async fn write_each_mode_then_count() -> TestResult {
    with_scratch("write_each_mode_then_count", async |scratch: &Scratch| {
        let mut billed = 0;
        for (table, mode) in [
            ("t_default", BigQueryWriteMode::Default),
            ("t_committed", BigQueryWriteMode::Committed),
        ] {
            let table_ref = create_table(scratch, table, columns(), None).await?;
            let options = BigQueryStreamingWriteOptions::new()
                .with_mode(mode)
                .with_max_batch_rows(2);
            let (mut writer, _) = scratch
                .db
                .create_streaming_writer_with_options::<Row>(table_ref, options)
                .await?;
            writer.write_all(&rows(5)).await?;
            let summary = writer.finish().await?;
            assert_eq!(
                (summary.rows_written, summary.rows_failed),
                (5, 0),
                "{mode:?}"
            );
            let (n, b) = count(
                scratch,
                &format!("SELECT COUNT(*) FROM {}", table_sql(scratch, table)),
            )
            .await?;
            billed += b;
            assert_eq!(n, 5, "{mode:?}");
        }

        let table_ref = create_table(scratch, "t_pending", columns(), None).await?;
        let (mut writer, _) = scratch
            .db
            .create_streaming_writer_with_options::<Row>(
                table_ref,
                BigQueryStreamingWriteOptions::new().with_mode(BigQueryWriteMode::Pending),
            )
            .await?;
        writer.write_all(&rows(5)).await?;
        let finalized = writer.finalize().await?;
        assert_eq!(finalized.row_count, 5);
        let sql = format!("SELECT COUNT(*) FROM {}", table_sql(scratch, "t_pending"));
        let (before, b) = count(scratch, &sql).await?;
        billed += b;
        assert_eq!(before, 0, "pending rows are invisible before the commit");
        scratch.db.commit_write_streams(vec![finalized]).await?;
        let (after, b) = count(scratch, &sql).await?;
        billed += b;
        assert_eq!(after, 5, "pending rows are visible after the commit");
        eprintln!("write_each_mode_then_count: 15 rows written, {billed} bytes billed");
        Ok(())
    })
    .await
}

/// A resend only happens when an acknowledgement is lost, which a live test cannot cause; the
/// fake server covers that path. This checks the committed stream end to end with several
/// pipelined batches: each row stored once.
#[tokio::test]
async fn committed_resend_writes_no_duplicates() -> TestResult {
    with_scratch(
        "committed_resend_writes_no_duplicates",
        async |scratch: &Scratch| {
            let table = create_table(scratch, "t", columns(), None).await?;
            let summary = scratch
                .db
                .fluent()
                .insert()
                .into(table)
                .objects(&rows(10))
                .options(BigQueryStreamingWriteOptions::new().with_max_batch_rows(2))
                .exactly_once()
                .execute()
                .await?;
            assert_eq!((summary.rows_written, summary.batches), (10, 5));
            let (rows, billed) = query_rows(
                scratch,
                &format!(
                    "SELECT COUNT(*), COUNT(DISTINCT id) FROM {}",
                    table_sql(scratch, "t")
                ),
            )
            .await?;
            assert_eq!(rows, [[Some("10".to_string()), Some("10".to_string())]]);
            eprintln!(
                "committed_resend_writes_no_duplicates: 10 rows written, {billed} bytes billed"
            );
            Ok(())
        },
    )
    .await
}
