//! Live checks of column types whose behaviour depends on the column rather than the Rust
//! field: parameterised NUMERIC and JSON. Rows go in through the crate's writer and come back
//! through a table scan, a handful per test.

use bigquery::*;
use serde::{Deserialize, Serialize};

mod common;
mod read_common;
use common::TestResult;
use read_common::{with_scratch_prefixed, Scratch};

const PREFIX: &str = "bqp4b";

async fn write<T: Serialize + Sync>(s: &Scratch, table: &str, rows: &[T]) -> BigQueryResult<()> {
    s.db.fluent()
        .insert()
        .into((s.project.as_str(), s.dataset.as_str(), table))
        .objects(rows)
        .execute()
        .await
        .map(|_| ())
}

async fn read<T: for<'de> Deserialize<'de> + Send + 'static>(
    s: &Scratch,
    table: &str,
) -> TestResult<Vec<T>> {
    Ok(s.db
        .fluent()
        .select()
        .from((s.dataset.as_str(), table))
        .obj()
        .query()
        .await?)
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Price {
    id: i64,
    amount: String,
}

#[tokio::test]
async fn numeric_with_precision_and_scale_round_trips() -> TestResult {
    with_scratch_prefixed(
        PREFIX,
        "numeric_with_precision_and_scale_round_trips",
        async |s| {
            s.sql("CREATE TABLE prices (id INT64, amount NUMERIC(10, 2))")
                .await?;
            let within = [
                Price {
                    id: 1,
                    amount: "12345678.91".into(),
                },
                Price {
                    id: 2,
                    amount: "-0.5".into(),
                },
            ];
            write(s, "prices", &within).await?;
            let mut got: Vec<Price> = read(s, "prices").await?;
            got.sort_by_key(|p| p.id);
            assert_eq!(got, within);

            // The writer sends every NUMERIC at scale 9, so the excess digits reach BigQuery,
            // which rounds them half away from zero rather than rejecting the row.
            let excess = [
                Price {
                    id: 3,
                    amount: "1.239".into(),
                },
                Price {
                    id: 4,
                    amount: "1.245".into(),
                },
                Price {
                    id: 5,
                    amount: "-1.245".into(),
                },
            ];
            write(s, "prices", &excess).await?;
            let mut stored: Vec<Price> = read::<Price>(s, "prices")
                .await?
                .into_iter()
                .filter(|p| p.id >= 3)
                .collect();
            stored.sort_by_key(|p| p.id);
            eprintln!("LIVE NUMERIC(10, 2) given 1.239, 1.245, -1.245 stores {stored:?}");
            assert_eq!(
                stored,
                [
                    Price {
                        id: 3,
                        amount: "1.24".into()
                    },
                    Price {
                        id: 4,
                        amount: "1.25".into()
                    },
                    Price {
                        id: 5,
                        amount: "-1.25".into()
                    },
                ]
            );
            Ok(())
        },
    )
    .await
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
struct Doc {
    county: String,
    zip: Option<i64>,
    tags: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
struct Rec {
    j: serde_json::Value,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
struct JsonRow {
    id: i64,
    value: Option<serde_json::Value>,
    doc: Doc,
    rec: Rec,
    arr: Vec<serde_json::Value>,
}

fn json_rows() -> Vec<JsonRow> {
    (0..4)
        .map(|id| JsonRow {
            id,
            value: match id {
                0 => Some(serde_json::json!({"län": "Skåne", "n": [1, 2, {"deep": true}]})),
                1 => Some(serde_json::Value::Null),
                2 => None,
                _ => Some(serde_json::json!([id, "x"])),
            },
            doc: Doc {
                county: ["Uppsala", "Gotland"][(id % 2) as usize].into(),
                zip: (id % 2 == 0).then_some(75_000 + id),
                tags: (0..id).map(|t| format!("t{t}")).collect(),
            },
            rec: Rec {
                j: serde_json::json!({"id": id}),
            },
            arr: (0..id).map(|i| serde_json::json!({"i": i})).collect(),
        })
        .collect()
}

#[tokio::test]
async fn json_columns_round_trip_any_serde_shape() -> TestResult {
    with_scratch_prefixed(
        PREFIX,
        "json_columns_round_trip_any_serde_shape",
        async |s| {
            s.sql(
                "CREATE TABLE docs (id INT64, value JSON, doc JSON, rec STRUCT<j JSON>, \
             arr ARRAY<JSON>)",
            )
            .await?;
            let rows = json_rows();
            write(s, "docs", &rows).await?;
            let mut got: Vec<JsonRow> = read(s, "docs").await?;
            got.sort_by_key(|r| r.id);
            assert_eq!(got, rows);
            Ok(())
        },
    )
    .await
}

#[derive(Serialize)]
struct RawJson {
    id: i64,
    value: String,
}

#[tokio::test]
async fn invalid_json_text_is_rejected_by_bigquery() -> TestResult {
    with_scratch_prefixed(
        PREFIX,
        "invalid_json_text_is_rejected_by_bigquery",
        async |s| {
            s.sql("CREATE TABLE raw (id INT64, value JSON)").await?;
            let outcome = write(
                s,
                "raw",
                &[
                    RawJson {
                        id: 1,
                        value: r#"{"ok": true}"#.into(),
                    },
                    RawJson {
                        id: 2,
                        value: "{not json".into(),
                    },
                ],
            )
            .await;
            eprintln!("LIVE invalid JSON text from a String: {outcome:?}");
            let Err(errors::BigQueryError::RowErrors(rejected)) = outcome else {
                panic!("expected row errors, got {outcome:?}");
            };
            let rows: Vec<(u64, &str)> = rejected
                .errors
                .iter()
                .map(|e| (e.row, e.code.as_str()))
                .collect();
            assert_eq!(rows, [(1, "FIELDS_ERROR")]);
            let stored: Vec<serde_json::Value> =
                s.db.fluent()
                    .select()
                    .from((s.dataset.as_str(), "raw"))
                    .obj()
                    .query()
                    .await?;
            assert!(stored.is_empty(), "the batch is rejected whole: {stored:?}");
            Ok(())
        },
    )
    .await
}
