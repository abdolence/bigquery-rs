//! proptest strategies for values inside BigQuery's range, in the canonical form both codecs'
//! round-trip tests compare: the integers and bytes BigQuery itself stores.

use crate::types::civil::{
    DATE_MAX_DAYS, DATE_MIN_DAYS, MICROS_PER_DAY, TIMESTAMP_MAX_MICROS, TIMESTAMP_MIN_MICROS,
};
use crate::types::interval::BigQueryInterval;
use crate::types::kind::BqKind;
use crate::types::schema::BigQueryRangeElementType;
use arrow_buffer::i256;
use proptest::prelude::*;

/// One value of a column, in BigQuery's own units.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Canonical {
    Int64(i64),
    /// The bits of the `f64`, so that NaN, `-0.0` and subnormals compare exactly.
    Float64(u64),
    Bool(bool),
    String(String),
    Bytes(Vec<u8>),
    /// Days since 1970-01-01.
    Date(i32),
    /// Microseconds since midnight.
    Time(i64),
    /// Civil microseconds since 1970-01-01T00:00:00.
    DateTime(i64),
    /// Microseconds since the epoch.
    Timestamp(i64),
    /// The unscaled value at scale 9.
    Numeric(i128),
    /// The unscaled value at scale 38.
    BigNumeric(i256),
    /// WKT, in the form BigQuery returns it.
    Geography(String),
    Json(serde_json::Value),
    Interval(BigQueryInterval),
    /// The bounds in the element's unit: days for DATE, microseconds otherwise. `None` is
    /// unbounded; when both are set, `start < end`.
    Range {
        element: BigQueryRangeElementType,
        start: Option<i64>,
        end: Option<i64>,
    },
}

/// The largest NUMERIC magnitude, unscaled: 38 nines.
const NUMERIC_MAX_UNSCALED: i128 = 10i128.pow(38) - 1;

/// BigQuery's INTERVAL limits: ±10000 years, ±3660000 days, and a time part that Storage Read
/// can send back as `i64` nanoseconds.
const INTERVAL_MAX_MONTHS: i32 = 120_000;
const INTERVAL_MAX_DAYS: i32 = 3_660_000;
const INTERVAL_MAX_MICROS: i64 = i64::MAX / 1000;

/// Values of `kind` inside BigQuery's range.
///
/// # Panics
/// For [`BqKind::Struct`], whose values are built from their fields' strategies by the test that
/// knows the fields.
pub(crate) fn canonical(kind: BqKind) -> BoxedStrategy<Canonical> {
    match kind {
        BqKind::Int64 => any::<i64>().prop_map(Canonical::Int64).boxed(),
        BqKind::Float64 => any::<f64>()
            .prop_map(|x| Canonical::Float64(x.to_bits()))
            .boxed(),
        BqKind::Bool => any::<bool>().prop_map(Canonical::Bool).boxed(),
        BqKind::String => "\\PC{0,24}".prop_map(Canonical::String).boxed(),
        BqKind::Bytes => proptest::collection::vec(any::<u8>(), 0..32)
            .prop_map(Canonical::Bytes)
            .boxed(),
        BqKind::Date => (DATE_MIN_DAYS..=DATE_MAX_DAYS)
            .prop_map(Canonical::Date)
            .boxed(),
        BqKind::Time => (0..MICROS_PER_DAY).prop_map(Canonical::Time).boxed(),
        BqKind::DateTime => (TIMESTAMP_MIN_MICROS..=TIMESTAMP_MAX_MICROS)
            .prop_map(Canonical::DateTime)
            .boxed(),
        BqKind::Timestamp => (TIMESTAMP_MIN_MICROS..=TIMESTAMP_MAX_MICROS)
            .prop_map(Canonical::Timestamp)
            .boxed(),
        BqKind::Numeric => (-NUMERIC_MAX_UNSCALED..=NUMERIC_MAX_UNSCALED)
            .prop_map(Canonical::Numeric)
            .boxed(),
        BqKind::BigNumeric => any::<[u8; 32]>()
            .prop_map(|bytes| Canonical::BigNumeric(i256::from_le_bytes(bytes)))
            .boxed(),
        BqKind::Geography => (-180i32..=180, -90i32..=90)
            .prop_map(|(lon, lat)| Canonical::Geography(format!("POINT({lon} {lat})")))
            .boxed(),
        BqKind::Json => json_value().prop_map(Canonical::Json).boxed(),
        BqKind::Interval => (
            -INTERVAL_MAX_MONTHS..=INTERVAL_MAX_MONTHS,
            -INTERVAL_MAX_DAYS..=INTERVAL_MAX_DAYS,
            -INTERVAL_MAX_MICROS..=INTERVAL_MAX_MICROS,
        )
            .prop_map(|(months, days, micros)| {
                Canonical::Interval(BigQueryInterval {
                    months,
                    days,
                    nanos: micros * 1000,
                })
            })
            .boxed(),
        BqKind::Range => prop_oneof![
            canonical_range(BigQueryRangeElementType::Date),
            canonical_range(BigQueryRangeElementType::DateTime),
            canonical_range(BigQueryRangeElementType::Timestamp),
        ]
        .boxed(),
        BqKind::Struct => {
            panic!("STRUCT values are built from their fields' strategies, not from a kind")
        }
    }
}

/// RANGE values of one element type, with either end unbounded at times.
pub(crate) fn canonical_range(element: BigQueryRangeElementType) -> BoxedStrategy<Canonical> {
    let bound = match element {
        BigQueryRangeElementType::Date => {
            (i64::from(DATE_MIN_DAYS)..=i64::from(DATE_MAX_DAYS)).boxed()
        }
        BigQueryRangeElementType::DateTime | BigQueryRangeElementType::Timestamp => {
            (TIMESTAMP_MIN_MICROS..=TIMESTAMP_MAX_MICROS).boxed()
        }
    };
    (
        proptest::option::of(bound.clone()),
        proptest::option::of(bound),
    )
        .prop_filter(
            "a bounded range needs start < end",
            |(start, end)| !matches!((start, end), (Some(s), Some(e)) if s == e),
        )
        .prop_map(move |(a, b)| {
            let (start, end) = match (a, b) {
                (Some(a), Some(b)) => (Some(a.min(b)), Some(a.max(b))),
                other => other,
            };
            Canonical::Range {
                element,
                start,
                end,
            }
        })
        .boxed()
}

/// JSON documents without floats, whose text BigQuery may print differently.
fn json_value() -> impl Strategy<Value = serde_json::Value> {
    let leaf = prop_oneof![
        Just(serde_json::Value::Null),
        any::<bool>().prop_map(serde_json::Value::Bool),
        any::<i64>().prop_map(serde_json::Value::from),
        "[a-z0-9 ]{0,8}".prop_map(serde_json::Value::String),
    ];
    leaf.prop_recursive(3, 16, 4, |inner| {
        prop_oneof![
            proptest::collection::vec(inner.clone(), 0..4).prop_map(serde_json::Value::Array),
            proptest::collection::btree_map("[a-z]{1,4}", inner, 0..4)
                .prop_map(|map| serde_json::Value::Object(map.into_iter().collect())),
        ]
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const KINDS: [BqKind; 15] = [
        BqKind::Int64,
        BqKind::Float64,
        BqKind::Bool,
        BqKind::String,
        BqKind::Bytes,
        BqKind::Date,
        BqKind::Time,
        BqKind::DateTime,
        BqKind::Timestamp,
        BqKind::Numeric,
        BqKind::BigNumeric,
        BqKind::Geography,
        BqKind::Json,
        BqKind::Interval,
        BqKind::Range,
    ];

    fn in_element_range(element: BigQueryRangeElementType, v: i64) -> bool {
        match element {
            BigQueryRangeElementType::Date => {
                (i64::from(DATE_MIN_DAYS)..=i64::from(DATE_MAX_DAYS)).contains(&v)
            }
            BigQueryRangeElementType::DateTime | BigQueryRangeElementType::Timestamp => {
                (TIMESTAMP_MIN_MICROS..=TIMESTAMP_MAX_MICROS).contains(&v)
            }
        }
    }

    fn assert_in_range(kind: BqKind, value: &Canonical) {
        let numeric_max = NUMERIC_MAX_UNSCALED;
        let ok = match (kind, value) {
            (BqKind::Int64, Canonical::Int64(_))
            | (BqKind::Float64, Canonical::Float64(_))
            | (BqKind::Bool, Canonical::Bool(_))
            | (BqKind::String, Canonical::String(_))
            | (BqKind::Bytes, Canonical::Bytes(_))
            | (BqKind::BigNumeric, Canonical::BigNumeric(_))
            | (BqKind::Json, Canonical::Json(_)) => true,
            (BqKind::Date, Canonical::Date(d)) => (DATE_MIN_DAYS..=DATE_MAX_DAYS).contains(d),
            (BqKind::Time, Canonical::Time(t)) => (0..MICROS_PER_DAY).contains(t),
            (BqKind::DateTime, Canonical::DateTime(v))
            | (BqKind::Timestamp, Canonical::Timestamp(v)) => {
                (TIMESTAMP_MIN_MICROS..=TIMESTAMP_MAX_MICROS).contains(v)
            }
            (BqKind::Numeric, Canonical::Numeric(v)) => (-numeric_max..=numeric_max).contains(v),
            (BqKind::Geography, Canonical::Geography(wkt)) => {
                wkt.starts_with("POINT(") && wkt.ends_with(')')
            }
            (BqKind::Interval, Canonical::Interval(iv)) => {
                iv.nanos % 1000 == 0
                    && (-120_000..=120_000).contains(&iv.months)
                    && (-3_660_000..=3_660_000).contains(&iv.days)
            }
            (
                BqKind::Range,
                Canonical::Range {
                    element,
                    start,
                    end,
                },
            ) => {
                start.is_none_or(|s| in_element_range(*element, s))
                    && end.is_none_or(|e| in_element_range(*element, e))
                    && match (start, end) {
                        (Some(s), Some(e)) => s < e,
                        _ => true,
                    }
            }
            _ => false,
        };
        assert!(ok, "{kind:?} gave {value:?}");
    }

    proptest! {
        #[test]
        fn testkit_values_stay_in_bigquery_range(
            values in proptest::collection::vec(
                prop_oneof![
                    canonical(BqKind::Int64).prop_map(|v| (BqKind::Int64, v)),
                    canonical(BqKind::Float64).prop_map(|v| (BqKind::Float64, v)),
                    canonical(BqKind::Bool).prop_map(|v| (BqKind::Bool, v)),
                    canonical(BqKind::String).prop_map(|v| (BqKind::String, v)),
                    canonical(BqKind::Bytes).prop_map(|v| (BqKind::Bytes, v)),
                    canonical(BqKind::Date).prop_map(|v| (BqKind::Date, v)),
                    canonical(BqKind::Time).prop_map(|v| (BqKind::Time, v)),
                    canonical(BqKind::DateTime).prop_map(|v| (BqKind::DateTime, v)),
                    canonical(BqKind::Timestamp).prop_map(|v| (BqKind::Timestamp, v)),
                    canonical(BqKind::Numeric).prop_map(|v| (BqKind::Numeric, v)),
                    canonical(BqKind::BigNumeric).prop_map(|v| (BqKind::BigNumeric, v)),
                    canonical(BqKind::Geography).prop_map(|v| (BqKind::Geography, v)),
                    canonical(BqKind::Json).prop_map(|v| (BqKind::Json, v)),
                    canonical(BqKind::Interval).prop_map(|v| (BqKind::Interval, v)),
                    canonical(BqKind::Range).prop_map(|v| (BqKind::Range, v)),
                ],
                KINDS.len()..=4 * KINDS.len(),
            )
        ) {
            for (kind, value) in &values {
                assert_in_range(*kind, value);
            }
        }
    }

    #[test]
    fn range_strategy_keeps_its_element_type() {
        let mut runner = proptest::test_runner::TestRunner::deterministic();
        for element in [
            BigQueryRangeElementType::Date,
            BigQueryRangeElementType::DateTime,
            BigQueryRangeElementType::Timestamp,
        ] {
            for _ in 0..50 {
                let value = canonical_range(element)
                    .new_tree(&mut runner)
                    .expect("valid test input")
                    .current();
                assert!(
                    matches!(value, Canonical::Range { element: got, .. } if got == element),
                    "{element:?} gave {value:?}"
                );
                assert_in_range(BqKind::Range, &value);
            }
        }
    }

    #[test]
    fn bounds_of_the_strategies_reach_bigquery_extremes() {
        let mut runner = proptest::test_runner::TestRunner::deterministic();
        let mut seen_negative_numeric = false;
        for _ in 0..200 {
            if let Canonical::Numeric(v) = canonical(BqKind::Numeric)
                .new_tree(&mut runner)
                .expect("valid test input")
                .current()
            {
                seen_negative_numeric |= v < 0;
            }
        }
        assert!(seen_negative_numeric);
    }
}
