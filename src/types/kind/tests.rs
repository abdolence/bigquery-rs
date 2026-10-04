use super::*;
use crate::types::schema::{BigQueryDecimalParams, BigQueryRangeElementType};
use arrow_schema::{DataType, Field, Fields, IntervalUnit, TimeUnit};
use std::collections::HashMap;

fn ext(name: &str) -> HashMap<String, String> {
    HashMap::from([("ARROW:extension:name".to_string(), name.to_string())])
}

fn f(dt: DataType) -> Field {
    Field::new("x", dt, true)
}

#[test]
fn kind_from_arrow_field_recognises_extension_metadata() {
    let utc = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let range = DataType::Struct(Fields::from(vec![
        Field::new("start", DataType::Date32, true),
        Field::new("end", DataType::Date32, true),
    ]));
    let range_meta = HashMap::from([("google:sqlType".to_string(), "range".to_string())]);
    let cases = [
        (f(DataType::Int64), Some(BqKind::Int64)),
        (f(DataType::Float64), Some(BqKind::Float64)),
        (f(DataType::Boolean), Some(BqKind::Bool)),
        (f(DataType::Utf8), Some(BqKind::String)),
        (
            f(DataType::Utf8).with_metadata(ext("google:sqlType:json")),
            Some(BqKind::Json),
        ),
        (
            f(DataType::Utf8).with_metadata(ext("google:sqlType:geography")),
            Some(BqKind::Geography),
        ),
        (f(DataType::Binary), Some(BqKind::Bytes)),
        (f(DataType::Date32), Some(BqKind::Date)),
        (
            f(DataType::Time64(TimeUnit::Microsecond)),
            Some(BqKind::Time),
        ),
        (
            f(DataType::Timestamp(TimeUnit::Microsecond, None))
                .with_metadata(ext("google:sqlType:datetime")),
            Some(BqKind::DateTime),
        ),
        (f(utc), Some(BqKind::Timestamp)),
        (f(DataType::Decimal128(38, 9)), Some(BqKind::Numeric)),
        (f(DataType::Decimal256(76, 38)), Some(BqKind::BigNumeric)),
        (f(DataType::Decimal128(10, 2)), Some(BqKind::Numeric)),
        (f(DataType::Decimal256(40, 0)), Some(BqKind::BigNumeric)),
        (f(DataType::Decimal128(10, -2)), None),
        (
            f(DataType::Interval(IntervalUnit::MonthDayNano))
                .with_metadata(ext("google:sqlType:interval")),
            Some(BqKind::Interval),
        ),
        (
            f(range.clone()).with_metadata(range_meta),
            Some(BqKind::Range),
        ),
        (f(range), Some(BqKind::Struct)),
        (f(DataType::Float16), None),
        (f(DataType::Int32), None),
        (f(DataType::Timestamp(TimeUnit::Nanosecond, None)), None),
    ];
    for (field, kind) in cases {
        assert_eq!(BqKind::from_arrow(&field), kind, "{:?}", field.data_type());
    }
}

#[test]
fn list_element_kind_comes_from_the_list_field_metadata() {
    let item = Field::new("item", DataType::Utf8, true);
    let list =
        Field::new_list("tags", item.clone(), false).with_metadata(ext("google:sqlType:json"));
    assert_eq!(
        BqKind::from_arrow_list_item(&list, &item),
        Some(BqKind::Json)
    );
    assert_eq!(
        BqKind::from_arrow(&item),
        Some(BqKind::String),
        "the item alone has no metadata"
    );

    let plain = Field::new_list("tags", item.clone(), false);
    assert_eq!(
        BqKind::from_arrow_list_item(&plain, &item),
        Some(BqKind::String)
    );

    let range_item = Field::new(
        "item",
        DataType::Struct(Fields::from(vec![
            Field::new("start", DataType::Date32, true),
            Field::new("end", DataType::Date32, true),
        ])),
        true,
    );
    let ranges = Field::new_list("rs", range_item.clone(), false).with_metadata(HashMap::from([(
        "google:sqlType".to_string(),
        "range".to_string(),
    )]));
    assert_eq!(
        BqKind::from_arrow_list_item(&ranges, &range_item),
        Some(BqKind::Range)
    );
}

#[test]
fn kind_names_packing_and_field_types() {
    assert_eq!(BqKind::Int64.name(), "INT64");
    assert_eq!(BqKind::BigNumeric.name(), "BIGNUMERIC");
    assert_eq!(BqKind::DateTime.name(), "DATETIME");
    let packable: Vec<BqKind> = ALL.into_iter().filter(|k| k.packable()).collect();
    assert_eq!(
        packable,
        [
            BqKind::Int64,
            BqKind::Float64,
            BqKind::Bool,
            BqKind::Date,
            BqKind::Time,
            BqKind::DateTime,
            BqKind::Timestamp
        ]
    );
    assert_eq!(
        BqKind::from(&BigQueryFieldType::Numeric(Some(BigQueryDecimalParams {
            precision: 3,
            scale: 1
        }))),
        BqKind::Numeric
    );
    assert_eq!(
        BqKind::from(&BigQueryFieldType::String {
            max_length: Some(3)
        }),
        BqKind::String
    );
    assert_eq!(
        BqKind::from(&BigQueryFieldType::Range(BigQueryRangeElementType::Date)),
        BqKind::Range
    );
    assert_eq!(
        BqKind::from(&BigQueryFieldType::Struct(vec![])),
        BqKind::Struct
    );
}

const ALL: [BqKind; 16] = [
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
    BqKind::Struct,
];
