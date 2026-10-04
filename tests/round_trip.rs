//! Rust values written as GoogleSQL literals, inserted with DML, and read back through Storage
//! Read into the same Rust types. The round trip through the crate's own writer joins it once
//! the write path is on the branch.

use base64::Engine;
use bigquery::*;
use serde::Deserialize;

mod common;
mod read_common;
use common::TestResult;
use read_common::{with_scratch, Scratch};

#[derive(Deserialize, Debug, PartialEq, Clone)]
struct Home {
    county: String,
    zip: Option<i64>,
}

#[derive(Deserialize, Debug, PartialEq, Clone)]
struct Resident {
    id: i64,
    name: String,
    born: jiff::civil::Date,
    seen: jiff::Timestamp,
    seen_fast: BigQueryTimestamp,
    wakes: BigQueryTime,
    balance: String,
    photo: Vec<u8>,
    tags: Vec<String>,
    home: Option<Home>,
}

fn residents() -> Vec<Resident> {
    let names = [
        "Åsa Lindström",
        "Björn Ö'Hara",
        "Linnéa Ågren",
        "Örjan Näslund",
        "Märta Sjöberg",
        "Göran Ekström",
        "Saga Bäckström",
        "Håkan Åberg",
    ];
    names
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let i = i as i64;
            let seen = jiff::Timestamp::from_microsecond(1_700_000_000_123_456 + i * 86_400_000_000)
                .expect("in range");
            Resident {
                id: i,
                name: name.to_string(),
                born: jiff::civil::date(1950 + i as i16 * 7, 1 + i as i8, 1 + 3 * i as i8),
                seen,
                seen_fast: BigQueryTimestamp(seen),
                wakes: BigQueryTime(jiff::civil::time(5 + i as i8, 30, 0, 250_000_000)),
                // Canonical NUMERIC text: no trailing fractional zeros.
                balance: format!("{}.{:02}", 1000 * i - 3500, i * 6 + 1),
                photo: (0..i as u8 * 3).collect(),
                tags: (0..i % 3).map(|t| format!("län{t}")).collect(),
                home: (i % 4 != 3).then(|| Home {
                    county: ["Skåne", "Uppsala", "Gotland"][(i % 3) as usize].into(),
                    zip: (i % 2 == 0).then_some(10_000 + i),
                }),
            }
        })
        .collect()
}

fn string_literal(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
}

fn timestamp_literal(t: jiff::Timestamp) -> String {
    format!("TIMESTAMP '{}'", t.strftime("%Y-%m-%d %H:%M:%S%.6f+00"))
}

fn row_literal(r: &Resident) -> String {
    let tags: Vec<String> = r.tags.iter().map(|t| string_literal(t)).collect();
    let home = match &r.home {
        Some(h) => format!(
            "STRUCT({} AS county, {} AS zip)",
            string_literal(&h.county),
            h.zip.map_or("CAST(NULL AS INT64)".into(), |z| z.to_string())
        ),
        None => "NULL".into(),
    };
    format!(
        "({}, {}, DATE '{}', {}, {}, TIME '{}', NUMERIC '{}', FROM_BASE64('{}'), ARRAY<STRING>[{}], {})",
        r.id,
        string_literal(&r.name),
        r.born,
        timestamp_literal(r.seen),
        timestamp_literal(r.seen_fast.0),
        r.wakes.0,
        r.balance,
        base64::engine::general_purpose::STANDARD.encode(&r.photo),
        tags.join(", "),
        home
    )
}

#[tokio::test]
async fn round_trip_every_type_through_dml() -> TestResult {
    with_scratch("round_trip_every_type_through_dml", async |s: &Scratch| {
        s.sql(
            "CREATE TABLE residents (id INT64 NOT NULL, name STRING NOT NULL, born DATE NOT NULL, \
             seen TIMESTAMP NOT NULL, seen_fast TIMESTAMP NOT NULL, wakes TIME NOT NULL, \
             balance NUMERIC NOT NULL, photo BYTES NOT NULL, tags ARRAY<STRING>, \
             home STRUCT<county STRING, zip INT64>)",
        )
        .await?;
        let expected = residents();
        let values: Vec<String> = expected.iter().map(row_literal).collect();
        s.sql(&format!("INSERT INTO residents VALUES {}", values.join(", ")))
            .await?;
        let mut got: Vec<Resident> = s
            .db
            .fluent()
            .select()
            .from((s.dataset.as_str(), "residents"))
            .obj()
            .query()
            .await?;
        got.sort_by_key(|r| r.id);
        assert_eq!(got, expected);
        Ok(())
    })
    .await
}
