//! A table scan's typed filter writes its values into the read session's `row_restriction`
//! as GoogleSQL literals. These tests check that BigQuery parses every literal kind the filter
//! can compare back to the value it was rendered from, and that hostile values match
//! literally and leave the table as it was. They run only with `GCP_PROJECT` set, on scratch
//! tables of a few dozen rows.

use bigquery::*;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[path = "support/common.rs"]
mod common;
#[path = "support/read_common.rs"]
mod read_common;
use common::{with_scratch, Scratch, TestResult, CI_LOCATION};
use read_common::run_sql;

const HOSTILE: &str = "hostile";
const LITERAL_KINDS: &str = "literal_kinds";

/// Every character class the escaper has a rule for: all of C0 and C1, DEL, the invisible
/// format characters, a non-BMP character and the tag block.
fn every_char_class() -> String {
    let mut text: String = (0u32..0x250).filter_map(char::from_u32).collect();
    text.push_str(
        "\u{00AD}\u{061C}\u{200B}\u{200E}\u{2066}\u{2069}\u{FFF9}\u{1F600}\u{E0001}\u{E007F}",
    );
    text
}

/// The payloads of the crate's own injection corpus, every character class, and a 100 KB run
/// of quotes, which still fits a `row_restriction` escaped. The corpus's 1 MiB entry is refused
/// before any request, which the unit tests cover.
fn hostile_values() -> Vec<String> {
    let mut values: Vec<String> = [
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
    .into();
    values.extend(["'".repeat(100_000), every_char_class()]);
    values
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

/// The ids of the rows of `table` that `filter` matches, read through Storage Read.
async fn ids(
    s: &Scratch,
    table: &str,
    filter: impl FnOnce(BigQueryFilterBuilder) -> Option<BigQueryFilter>,
) -> TestResult<BTreeSet<i64>> {
    let rows: Vec<BigQueryResult<ReadRow>> =
        s.db.fluent()
            .select()
            .from(s.table(table))
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
    with_scratch(
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
                    .query(format!(
                        "CREATE TABLE {} AS SELECT * FROM UNNEST(@rows)",
                        s.table_sql(HOSTILE)
                    ))
                    .location(CI_LOCATION)
                    .param("rows", &rows)
                    .execute()
                    .await?;
            eprintln!(
                "LIVE bytes processed by the CREATE TABLE: {:?}",
                outcome.total_bytes_processed
            );

            let all_hostile: BTreeSet<i64> = (1..).take(hostile.len()).collect();
            let decoys: BTreeSet<i64> = (1001..).take(DECOYS.len()).collect();

            let matched = ids(s, HOSTILE, |f| {
                f.for_any(hostile.iter().map(|v| f.field("s").eq(v.as_str())))
            })
            .await?;
            assert_eq!(matched, all_hostile, "each value matches its own row only");

            let matched = ids(s, HOSTILE, |f| {
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

            let matched = ids(s, HOSTILE, |f| {
                f.for_any([
                    f.field("s").is_in(Vec::<String>::new()),
                    f.field("s").is_not_in(hostile.iter().map(String::as_str)),
                ])
            })
            .await?;
            assert_eq!(matched, decoys, "an empty IN and a NOT IN of every value");

            let count: Vec<Count> =
                s.db.fluent()
                    .query(format!(
                        "SELECT COUNT(*) AS n FROM {}",
                        s.table_sql(HOSTILE)
                    ))
                    .location(CI_LOCATION)
                    .obj::<Count>()
                    .query()
                    .await?;
            assert_eq!(count, [Count { n: total }], "the table is intact");
            Ok(())
        },
    )
    .await
}

/// The rows of `literal_kinds`, written as hand-made GoogleSQL so that the stored side shares no code with
/// the crate's literal renderer. Row 1 holds the values the filters look for; row 2 holds the
/// opposite extreme of each kind, which a literal BigQuery read as a different value, or a
/// condition that matches every row, would select too.
const LITERAL_KINDS_SELECT: &str = r#"
SELECT * FROM UNNEST([
  STRUCT(
    1 AS id,
    -9223372036854775808 AS whole,
    CAST('-inf' AS FLOAT64) AS infinity,
    CAST(5e-324 AS FLOAT64) AS magnitude,
    CAST('nan' AS FLOAT64) AS not_a_number,
    TRUE AS flag,
    FROM_HEX('000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f808182838485868788898a8b8c8d8e8f909192939495969798999a9b9c9d9e9fa0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebfc0c1c2c3c4c5c6c7c8c9cacbcccdcecfd0d1d2d3d4d5d6d7d8d9dadbdcdddedfe0e1e2e3e4e5e6e7e8e9eaebecedeeeff0f1f2f3f4f5f6f7f8f9fafbfcfdfeff') AS every_byte,
    NUMERIC '-99999999999999999999999999999.999999999' AS amount,
    BIGNUMERIC '0.00000000000000000000000000000000000001' AS fraction,
    DATE '0001-01-01' AS day,
    TIME '23:59:59.999999' AS clock,
    DATETIME '0001-01-01 23:59:59.999999' AS civil,
    TIMESTAMP '9999-12-30 12:34:56.999999+00' AS instant,
    INTERVAL '-1-2 3 1:2:3.000001' YEAR TO SECOND AS span,
    RANGE(DATE '2024-02-29', NULL) AS period,
    STRUCT(-1 AS number, "' OR '1'='1" AS text) AS pair
  ),
  STRUCT(
    2,
    9223372036854775807,
    CAST('inf' AS FLOAT64),
    CAST(1.7976931348623157e308 AS FLOAT64),
    0.0,
    FALSE,
    b'',
    NUMERIC '99999999999999999999999999999.999999999',
    BIGNUMERIC '0.00000000000000000000000000000000000002',
    DATE '9999-12-31',
    TIME '00:00:00',
    DATETIME '9999-12-31 00:00:00',
    TIMESTAMP '1970-01-01 00:00:00+00',
    INTERVAL '1-2 -3 -1:2:3.000001' YEAR TO SECOND,
    RANGE(DATE '2024-02-29', DATE '2024-03-01'),
    STRUCT(-1, "x")
  )
])"#;

/// The `pair` column's shape, which the filter writes as a STRUCT literal.
#[derive(Serialize)]
struct Pair {
    number: i64,
    text: &'static str,
}

#[tokio::test]
async fn filter_literals_parse_back_to_their_values() -> TestResult {
    with_scratch(
        "filter_literals_parse_back_to_their_values",
        async |scratch: &Scratch| {
            run_sql(
                scratch,
                &format!(
                    "CREATE TABLE {} AS{LITERAL_KINDS_SELECT}",
                    scratch.table_sql(LITERAL_KINDS)
                ),
            )
            .await?;
            let target = BTreeSet::from([1]);
            let opposite = BTreeSet::from([2]);
            let first_day = jiff::civil::date(1, 1, 1);
            let last_microsecond = jiff::civil::time(23, 59, 59, 999_999_000);
            let every_byte: Vec<u8> = (0..=255).collect();

            let matched = ids(scratch, LITERAL_KINDS, |filter| {
                filter.field("whole").eq(i64::MIN)
            })
            .await?;
            assert_eq!(matched, target, "INT64 minimum");
            let matched = ids(scratch, LITERAL_KINDS, |filter| {
                filter.field("whole").eq(i64::MAX)
            })
            .await?;
            assert_eq!(matched, opposite, "INT64 maximum");

            let matched = ids(scratch, LITERAL_KINDS, |filter| {
                filter.field("infinity").eq(f64::NEG_INFINITY)
            })
            .await?;
            assert_eq!(matched, target, "FLOAT64 negative infinity");
            let matched = ids(scratch, LITERAL_KINDS, |filter| {
                filter.field("infinity").eq(f64::INFINITY)
            })
            .await?;
            assert_eq!(matched, opposite, "FLOAT64 infinity");
            let matched = ids(scratch, LITERAL_KINDS, |filter| {
                filter.field("magnitude").eq(5e-324)
            })
            .await?;
            assert_eq!(matched, target, "FLOAT64 smallest subnormal");
            let matched = ids(scratch, LITERAL_KINDS, |filter| {
                filter.field("magnitude").eq(f64::MAX)
            })
            .await?;
            assert_eq!(matched, opposite, "FLOAT64 maximum");
            // NaN equals nothing, itself included, so the NaN literal can only show that it is
            // a FLOAT64 BigQuery accepts.
            let matched = ids(scratch, LITERAL_KINDS, |filter| {
                filter.field("not_a_number").eq(f64::NAN)
            })
            .await?;
            assert_eq!(matched, BTreeSet::new(), "FLOAT64 NaN");

            let matched = ids(scratch, LITERAL_KINDS, |filter| {
                filter.field("flag").eq(true)
            })
            .await?;
            assert_eq!(matched, target, "BOOL true");
            let matched = ids(scratch, LITERAL_KINDS, |filter| {
                filter.field("flag").eq(false)
            })
            .await?;
            assert_eq!(matched, opposite, "BOOL false");

            let matched = ids(scratch, LITERAL_KINDS, |filter| {
                filter
                    .field("every_byte")
                    .eq(serde_bytes::Bytes::new(&every_byte))
            })
            .await?;
            assert_eq!(matched, target, "BYTES of every byte value");

            let matched = ids(scratch, LITERAL_KINDS, |filter| {
                filter
                    .field("amount")
                    .eq(BigQueryDecimal("-99999999999999999999999999999.999999999"))
            })
            .await?;
            assert_eq!(matched, target, "NUMERIC");
            let matched = ids(scratch, LITERAL_KINDS, |filter| {
                filter
                    .field("fraction")
                    .eq(BigQueryDecimal("0.00000000000000000000000000000000000001"))
            })
            .await?;
            assert_eq!(matched, target, "BIGNUMERIC");

            let matched = ids(scratch, LITERAL_KINDS, |filter| {
                filter.field("day").eq(BigQueryDate(first_day))
            })
            .await?;
            assert_eq!(matched, target, "DATE");
            let matched = ids(scratch, LITERAL_KINDS, |filter| {
                filter.field("clock").eq(BigQueryTime(last_microsecond))
            })
            .await?;
            assert_eq!(matched, target, "TIME");
            let matched = ids(scratch, LITERAL_KINDS, |filter| {
                filter
                    .field("civil")
                    .eq(BigQueryDateTime(first_day.to_datetime(last_microsecond)))
            })
            .await?;
            assert_eq!(matched, target, "DATETIME");
            let instant: jiff::Timestamp = "9999-12-30T12:34:56.999999Z".parse()?;
            let matched = ids(scratch, LITERAL_KINDS, |filter| {
                filter.field("instant").eq(BigQueryTimestamp(instant))
            })
            .await?;
            assert_eq!(matched, target, "TIMESTAMP");

            let matched = ids(scratch, LITERAL_KINDS, |filter| {
                filter.field("span").eq(BigQueryInterval {
                    months: -14,
                    days: 3,
                    nanos: 3_723_000_001_000,
                })
            })
            .await?;
            assert_eq!(matched, target, "INTERVAL");

            let matched = ids(scratch, LITERAL_KINDS, |filter| {
                filter.field("period").eq(BigQueryRange {
                    start: Some(BigQueryDate(jiff::civil::date(2024, 2, 29))),
                    end: None,
                })
            })
            .await?;
            assert_eq!(matched, target, "RANGE<DATE> with an unbounded end");

            let matched = ids(scratch, LITERAL_KINDS, |filter| {
                filter.field("pair").eq(Pair {
                    number: -1,
                    text: "' OR '1'='1",
                })
            })
            .await?;
            assert_eq!(matched, target, "STRUCT");
            Ok(())
        },
    )
    .await
}
