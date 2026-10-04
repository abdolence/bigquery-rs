//! A table scan whose typed filter carries hostile values matches those values literally and
//! leaves the table as it was. It runs only with `GCP_PROJECT` set, on a scratch table of a
//! few dozen rows.

use bigquery::*;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

mod common;
mod read_common;
use common::TestResult;
use read_common::{with_scratch_prefixed, Scratch};

/// The payloads of the crate's own injection corpus that fit a `row_restriction` many times
/// over; the 1 MiB entry is refused before any request, which the unit tests cover.
fn hostile_values() -> Vec<String> {
    [
        "'; DROP TABLE x; --",
        "' OR '1'='1",
        "`backtick`",
        "\\'",
        "\\\\'",
        "\\\\",
        "\\",
        "\"",
        "\"\"\"",
        "'''",
        "/* comment */",
        "*/",
        "--",
        "#",
        "a\nb",
        "a\rb",
        "a\tb",
        "a\0b",
        "\u{2019} OR \u{2019}1\u{2019}=\u{2019}1",
        "\u{FF07}; DROP TABLE x; --",
        "\u{2028}\u{2029}\u{FEFF}\u{202E}",
        "@other_param",
        "?",
        "",
    ]
    .map(String::from)
    .into()
}

/// Rows no hostile value equals, which a value that broke out of its literal into a condition
/// such as `OR TRUE` would match.
const DECOYS: [&str; 4] = ["decoy", "x", "1", "TRUE"];

#[derive(Serialize)]
struct NewRow {
    id: i64,
    s: String,
    b: serde_bytes::ByteBuf,
    ts: BigQueryTimestamp,
}

#[derive(Deserialize, Debug)]
struct ReadRow {
    id: i64,
}

#[derive(Deserialize, Debug, PartialEq)]
struct Count {
    n: i64,
}

async fn ids(
    s: &Scratch,
    filter: impl FnOnce(BigQueryFilterBuilder) -> Option<BigQueryFilter>,
) -> TestResult<BTreeSet<i64>> {
    let table = s.dataset.table(BigQueryTableId::from_static("hostile"));
    let rows: Vec<BigQueryResult<ReadRow>> =
        s.db.fluent()
            .select()
            .from(table)
            .filter(filter)
            .obj::<ReadRow>()
            .stream_query_with_errors()
            .await?
            .collect()
            .await;
    Ok(rows
        .into_iter()
        .map(|r| r.map(|r| r.id))
        .collect::<Result<_, _>>()?)
}

#[tokio::test]
async fn filter_matches_hostile_values_literally() -> TestResult {
    with_scratch_prefixed(
        "bqp4c",
        "filter_matches_hostile_values_literally",
        async |s: &Scratch| {
            let hostile = hostile_values();
            let t0: jiff::Timestamp = "2026-10-04T12:00:00Z".parse()?;
            let mut rows: Vec<NewRow> = hostile
                .iter()
                .zip(1..)
                .map(|(v, id)| NewRow {
                    id,
                    s: v.clone(),
                    b: serde_bytes::ByteBuf::from(v.clone().into_bytes()),
                    ts: BigQueryTimestamp(t0),
                })
                .collect();
            rows.extend(DECOYS.iter().zip(1001..).map(|(v, id)| NewRow {
                id,
                s: v.to_string(),
                b: serde_bytes::ByteBuf::from(v.as_bytes().to_vec()),
                ts: BigQueryTimestamp(t0 - jiff::SignedDuration::from_hours(24)),
            }));
            let total = i64::try_from(rows.len())?;
            let outcome =
                s.db.fluent()
                    .query("CREATE TABLE hostile AS SELECT * FROM UNNEST(@rows)")
                    .default_dataset(BigQueryDatasetRef::new(&s.project, s.dataset.clone())?)
                    .location(BigQueryLocation::from_static("US"))
                    .param("rows", &rows)
                    .execute()
                    .await?;
            eprintln!(
                "LIVE bytes processed by the CREATE TABLE: {:?}",
                outcome.total_bytes_processed
            );

            let all_hostile: BTreeSet<i64> = (1..).take(hostile.len()).collect();
            let decoys: BTreeSet<i64> = (1001..).take(DECOYS.len()).collect();

            let matched = ids(s, |f| {
                f.for_any(hostile.iter().map(|v| f.field("s").eq(v.as_str())))
            })
            .await?;
            assert_eq!(matched, all_hostile, "each value matches its own row only");

            let matched = ids(s, |f| {
                f.for_all([
                    f.field("s").is_in(hostile.iter().map(String::as_str)),
                    f.not(
                        f.field("b")
                            .eq(serde_bytes::Bytes::new(hostile[0].as_bytes())),
                    ),
                    f.field("ts").ge(BigQueryTimestamp(t0)),
                ])
            })
            .await?;
            let expected: BTreeSet<i64> =
                all_hostile.iter().copied().filter(|&id| id != 1).collect();
            assert_eq!(
                matched, expected,
                "IN, NOT over BYTES and a TIMESTAMP bound"
            );

            let matched = ids(s, |f| {
                f.for_any([
                    f.field("s").is_in(Vec::<String>::new()),
                    f.field("s").is_not_in(hostile.iter().map(String::as_str)),
                ])
            })
            .await?;
            assert_eq!(matched, decoys, "an empty IN and a NOT IN of every value");

            let count: Vec<Count> =
                s.db.fluent()
                    .query("SELECT COUNT(*) AS n FROM hostile")
                    .default_dataset(BigQueryDatasetRef::new(&s.project, s.dataset.clone())?)
                    .location(BigQueryLocation::from_static("US"))
                    .obj::<Count>()
                    .query()
                    .await?;
            assert_eq!(count, [Count { n: total }], "the table is intact");
            Ok(())
        },
    )
    .await
}
