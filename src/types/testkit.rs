//! proptest strategies for values inside BigQuery's range, in the canonical form both codecs'
//! round-trip tests compare: the integers and bytes BigQuery itself stores.

use crate::types::civil::{
    DATE_MAX_DAYS, DATE_MIN_DAYS, MICROS_PER_DAY, TIMESTAMP_MAX_MICROS, TIMESTAMP_MIN_MICROS,
};
use crate::types::interval::BigQueryInterval;
use crate::types::kind::FieldKind;
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

impl Canonical {
    /// Values of `kind` inside BigQuery's range.
    ///
    /// # Panics
    /// For [`FieldKind::Struct`], whose values are built from their fields' strategies by the test that
    /// knows the fields.
    pub(crate) fn strategy(kind: FieldKind) -> BoxedStrategy<Canonical> {
        match kind {
            FieldKind::Int64 => any::<i64>().prop_map(Canonical::Int64).boxed(),
            FieldKind::Float64 => any::<f64>()
                .prop_map(|x| Canonical::Float64(x.to_bits()))
                .boxed(),
            FieldKind::Bool => any::<bool>().prop_map(Canonical::Bool).boxed(),
            FieldKind::String => "\\PC{0,24}".prop_map(Canonical::String).boxed(),
            FieldKind::Bytes => proptest::collection::vec(any::<u8>(), 0..32)
                .prop_map(Canonical::Bytes)
                .boxed(),
            FieldKind::Date => (DATE_MIN_DAYS..=DATE_MAX_DAYS)
                .prop_map(Canonical::Date)
                .boxed(),
            FieldKind::Time => (0..MICROS_PER_DAY).prop_map(Canonical::Time).boxed(),
            FieldKind::DateTime => (TIMESTAMP_MIN_MICROS..=TIMESTAMP_MAX_MICROS)
                .prop_map(Canonical::DateTime)
                .boxed(),
            FieldKind::Timestamp => (TIMESTAMP_MIN_MICROS..=TIMESTAMP_MAX_MICROS)
                .prop_map(Canonical::Timestamp)
                .boxed(),
            FieldKind::Numeric => (-NUMERIC_MAX_UNSCALED..=NUMERIC_MAX_UNSCALED)
                .prop_map(Canonical::Numeric)
                .boxed(),
            FieldKind::BigNumeric => any::<[u8; 32]>()
                .prop_map(|bytes| Canonical::BigNumeric(i256::from_le_bytes(bytes)))
                .boxed(),
            FieldKind::Geography => (-180i32..=180, -90i32..=90)
                .prop_map(|(lon, lat)| Canonical::Geography(format!("POINT({lon} {lat})")))
                .boxed(),
            FieldKind::Json => json_value().prop_map(Canonical::Json).boxed(),
            FieldKind::Interval => (
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
            FieldKind::Range => prop_oneof![
                Canonical::range_strategy(BigQueryRangeElementType::Date),
                Canonical::range_strategy(BigQueryRangeElementType::DateTime),
                Canonical::range_strategy(BigQueryRangeElementType::Timestamp),
            ]
            .boxed(),
            FieldKind::Struct => {
                panic!("STRUCT values are built from their fields' strategies, not from a kind")
            }
        }
    }

    /// RANGE values of one element type, with either end unbounded at times.
    pub(crate) fn range_strategy(element: BigQueryRangeElementType) -> BoxedStrategy<Canonical> {
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

    const KINDS: [FieldKind; 15] = [
        FieldKind::Int64,
        FieldKind::Float64,
        FieldKind::Bool,
        FieldKind::String,
        FieldKind::Bytes,
        FieldKind::Date,
        FieldKind::Time,
        FieldKind::DateTime,
        FieldKind::Timestamp,
        FieldKind::Numeric,
        FieldKind::BigNumeric,
        FieldKind::Geography,
        FieldKind::Json,
        FieldKind::Interval,
        FieldKind::Range,
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

    fn assert_in_range(kind: FieldKind, value: &Canonical) {
        let numeric_max = NUMERIC_MAX_UNSCALED;
        let ok = match (kind, value) {
            (FieldKind::Int64, Canonical::Int64(_))
            | (FieldKind::Float64, Canonical::Float64(_))
            | (FieldKind::Bool, Canonical::Bool(_))
            | (FieldKind::String, Canonical::String(_))
            | (FieldKind::Bytes, Canonical::Bytes(_))
            | (FieldKind::BigNumeric, Canonical::BigNumeric(_))
            | (FieldKind::Json, Canonical::Json(_)) => true,
            (FieldKind::Date, Canonical::Date(d)) => (DATE_MIN_DAYS..=DATE_MAX_DAYS).contains(d),
            (FieldKind::Time, Canonical::Time(t)) => (0..MICROS_PER_DAY).contains(t),
            (FieldKind::DateTime, Canonical::DateTime(v))
            | (FieldKind::Timestamp, Canonical::Timestamp(v)) => {
                (TIMESTAMP_MIN_MICROS..=TIMESTAMP_MAX_MICROS).contains(v)
            }
            (FieldKind::Numeric, Canonical::Numeric(v)) => (-numeric_max..=numeric_max).contains(v),
            (FieldKind::Geography, Canonical::Geography(wkt)) => {
                wkt.starts_with("POINT(") && wkt.ends_with(')')
            }
            (FieldKind::Interval, Canonical::Interval(iv)) => {
                iv.nanos % 1000 == 0
                    && (-120_000..=120_000).contains(&iv.months)
                    && (-3_660_000..=3_660_000).contains(&iv.days)
            }
            (
                FieldKind::Range,
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
                    Canonical::strategy(FieldKind::Int64).prop_map(|v| (FieldKind::Int64, v)),
                    Canonical::strategy(FieldKind::Float64).prop_map(|v| (FieldKind::Float64, v)),
                    Canonical::strategy(FieldKind::Bool).prop_map(|v| (FieldKind::Bool, v)),
                    Canonical::strategy(FieldKind::String).prop_map(|v| (FieldKind::String, v)),
                    Canonical::strategy(FieldKind::Bytes).prop_map(|v| (FieldKind::Bytes, v)),
                    Canonical::strategy(FieldKind::Date).prop_map(|v| (FieldKind::Date, v)),
                    Canonical::strategy(FieldKind::Time).prop_map(|v| (FieldKind::Time, v)),
                    Canonical::strategy(FieldKind::DateTime).prop_map(|v| (FieldKind::DateTime, v)),
                    Canonical::strategy(FieldKind::Timestamp).prop_map(|v| (FieldKind::Timestamp, v)),
                    Canonical::strategy(FieldKind::Numeric).prop_map(|v| (FieldKind::Numeric, v)),
                    Canonical::strategy(FieldKind::BigNumeric).prop_map(|v| (FieldKind::BigNumeric, v)),
                    Canonical::strategy(FieldKind::Geography).prop_map(|v| (FieldKind::Geography, v)),
                    Canonical::strategy(FieldKind::Json).prop_map(|v| (FieldKind::Json, v)),
                    Canonical::strategy(FieldKind::Interval).prop_map(|v| (FieldKind::Interval, v)),
                    Canonical::strategy(FieldKind::Range).prop_map(|v| (FieldKind::Range, v)),
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
                let value = Canonical::range_strategy(element)
                    .new_tree(&mut runner)
                    .expect("valid test input")
                    .current();
                assert!(
                    matches!(value, Canonical::Range { element: got, .. } if got == element),
                    "{element:?} gave {value:?}"
                );
                assert_in_range(FieldKind::Range, &value);
            }
        }
    }

    #[test]
    fn bounds_of_the_strategies_reach_bigquery_extremes() {
        let mut runner = proptest::test_runner::TestRunner::deterministic();
        let mut seen_negative_numeric = false;
        for _ in 0..200 {
            if let Canonical::Numeric(v) = Canonical::strategy(FieldKind::Numeric)
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
