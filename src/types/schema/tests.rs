use super::*;
use arrow_schema::{DataType, Field, Fields, IntervalUnit, Schema, TimeUnit};
use std::collections::HashMap;
use storage::table_field_schema::{Mode as StorageMode, Type as StorageType};

fn v2_field(name: &str, ty: &str, mode: &str) -> v2::TableFieldSchema {
    v2::TableFieldSchema {
        name: name.to_string(),
        r#type: ty.to_string(),
        mode: mode.to_string(),
        ..Default::default()
    }
}

fn storage_field(name: &str, ty: StorageType, mode: StorageMode) -> storage::TableFieldSchema {
    storage::TableFieldSchema {
        name: name.to_string(),
        r#type: ty.into(),
        mode: mode.into(),
        ..Default::default()
    }
}

fn v2_schema(fields: Vec<v2::TableFieldSchema>) -> BigQueryResult<BigQueryTableSchema> {
    BigQueryTableSchema::try_from(&v2::TableSchema {
        fields,
        ..Default::default()
    })
}

fn storage_schema(fields: Vec<storage::TableFieldSchema>) -> BigQueryResult<BigQueryTableSchema> {
    BigQueryTableSchema::try_from(&storage::TableSchema { fields })
}

fn field(
    name: &str,
    field_type: BigQueryFieldType,
    mode: BigQueryFieldMode,
) -> BigQueryFieldSchema {
    BigQueryFieldSchema {
        name: name.to_string(),
        field_type,
        mode,
        description: None,
        default_value_expression: None,
    }
}

fn unsupported(result: BigQueryResult<BigQueryTableSchema>) -> String {
    match result {
        Err(BigQueryError::DeserializeError(err)) => {
            assert_eq!(err.kind, BigQueryCodecErrorKind::UnsupportedType, "{err}");
            err.path
        }
        other => panic!("expected UnsupportedType, got {other:?}"),
    }
}

#[test]
fn legacy_and_storage_type_names_normalise_to_one_type() {
    let pairs = [
        (
            "INTEGER",
            "INT64",
            StorageType::Int64,
            BigQueryFieldType::Int64,
        ),
        (
            "FLOAT",
            "FLOAT64",
            StorageType::Double,
            BigQueryFieldType::Float64,
        ),
        (
            "BOOLEAN",
            "BOOL",
            StorageType::Bool,
            BigQueryFieldType::Bool,
        ),
        (
            "DECIMAL",
            "NUMERIC",
            StorageType::Numeric,
            BigQueryFieldType::Numeric(None),
        ),
        (
            "BIGDECIMAL",
            "BIGNUMERIC",
            StorageType::Bignumeric,
            BigQueryFieldType::BigNumeric(None),
        ),
        (
            "STRING",
            "string",
            StorageType::String,
            BigQueryFieldType::String { max_length: None },
        ),
        (
            "BYTES",
            "Bytes",
            StorageType::Bytes,
            BigQueryFieldType::Bytes { max_length: None },
        ),
        ("DATE", "date", StorageType::Date, BigQueryFieldType::Date),
        ("TIME", "TIME", StorageType::Time, BigQueryFieldType::Time),
        (
            "DATETIME",
            "DATETIME",
            StorageType::Datetime,
            BigQueryFieldType::DateTime,
        ),
        (
            "TIMESTAMP",
            "TIMESTAMP",
            StorageType::Timestamp,
            BigQueryFieldType::Timestamp,
        ),
        (
            "GEOGRAPHY",
            "GEOGRAPHY",
            StorageType::Geography,
            BigQueryFieldType::Geography,
        ),
        ("JSON", "JSON", StorageType::Json, BigQueryFieldType::Json),
        (
            "INTERVAL",
            "INTERVAL",
            StorageType::Interval,
            BigQueryFieldType::Interval,
        ),
    ];
    for (legacy, standard, storage_type, expected) in pairs {
        let want = BigQueryTableSchema {
            fields: vec![field("c", expected.clone(), BigQueryFieldMode::Required)],
        };
        assert_eq!(
            v2_schema(vec![v2_field("c", legacy, "REQUIRED")])
                .ok()
                .as_ref(),
            Some(&want),
            "{legacy}"
        );
        assert_eq!(
            v2_schema(vec![v2_field("c", standard, "required")])
                .ok()
                .as_ref(),
            Some(&want),
            "{standard}"
        );
        assert_eq!(
            storage_schema(vec![storage_field(
                "c",
                storage_type,
                StorageMode::Required
            )])
            .ok()
            .as_ref(),
            Some(&want),
            "{storage_type:?}"
        );
    }

    let mut legacy_record = v2_field("rec", "RECORD", "REPEATED");
    legacy_record.fields = vec![v2_field("a", "INTEGER", "NULLABLE")];
    let mut storage_struct = storage_field("rec", StorageType::Struct, StorageMode::Repeated);
    storage_struct.fields = vec![storage_field(
        "a",
        StorageType::Int64,
        StorageMode::Nullable,
    )];
    let mut legacy_range = v2_field("r", "RANGE", "NULLABLE");
    legacy_range.range_element_type = Some(v2::table_field_schema::FieldElementType {
        r#type: "DATETIME".to_string(),
    });
    let mut storage_range = storage_field("r", StorageType::Range, StorageMode::Nullable);
    storage_range.range_element_type = Some(storage::table_field_schema::FieldElementType {
        r#type: StorageType::Datetime.into(),
    });
    let from_v2 = v2_schema(vec![legacy_record, legacy_range]).expect("valid test input");
    let from_storage =
        storage_schema(vec![storage_struct, storage_range]).expect("valid test input");
    assert_eq!(from_v2, from_storage);
    assert_eq!(
        from_v2.fields[0].field_type,
        BigQueryFieldType::Struct(vec![field(
            "a",
            BigQueryFieldType::Int64,
            BigQueryFieldMode::Nullable
        )])
    );
    assert_eq!(
        from_v2.fields[1].field_type,
        BigQueryFieldType::Range(BigQueryRangeElementType::DateTime)
    );

    let mut described = v2_field("s", "STRING", "NULLABLE");
    described.description = Some("a note".to_string());
    described.default_value_expression = Some("'x'".to_string());
    described.max_length = 10;
    let mut storage_described = storage_field("s", StorageType::String, StorageMode::Nullable);
    storage_described.description = "a note".to_string();
    storage_described.default_value_expression = "'x'".to_string();
    storage_described.max_length = 10;
    let from_v2 = v2_schema(vec![described]).expect("valid test input");
    assert_eq!(
        from_v2,
        storage_schema(vec![storage_described]).expect("valid test input")
    );
    assert_eq!(
        from_v2.fields[0].field_type,
        BigQueryFieldType::String {
            max_length: Some(10)
        }
    );
    assert_eq!(from_v2.fields[0].description.as_deref(), Some("a note"));

    let back = v2::TableSchema::from(&from_v2);
    assert_eq!(back.fields[0].r#type, "STRING");
    assert_eq!(back.fields[0].mode, "NULLABLE");
    assert_eq!(back.fields[0].max_length, 10);
    assert_eq!(v2_schema(back.fields).ok(), Some(from_v2));
}

#[test]
fn standard_names_go_out_to_v2_and_read_back_equal() {
    let schema = BigQueryTableSchema {
        fields: vec![
            field("i", BigQueryFieldType::Int64, BigQueryFieldMode::Required),
            field("f", BigQueryFieldType::Float64, BigQueryFieldMode::Nullable),
            field("b", BigQueryFieldType::Bool, BigQueryFieldMode::Repeated),
            field(
                "n",
                BigQueryFieldType::Numeric(Some(BigQueryDecimalParams {
                    precision: 10,
                    scale: 2,
                })),
                BigQueryFieldMode::Nullable,
            ),
            field(
                "r",
                BigQueryFieldType::Range(BigQueryRangeElementType::Timestamp),
                BigQueryFieldMode::Nullable,
            ),
            field(
                "s",
                BigQueryFieldType::Struct(vec![field(
                    "x",
                    BigQueryFieldType::Json,
                    BigQueryFieldMode::Nullable,
                )]),
                BigQueryFieldMode::Nullable,
            ),
        ],
    };
    let v2 = v2::TableSchema::from(&schema);
    let names: Vec<&str> = v2.fields.iter().map(|f| f.r#type.as_str()).collect();
    assert_eq!(
        names,
        ["INT64", "FLOAT64", "BOOL", "NUMERIC", "RANGE", "STRUCT"]
    );
    assert_eq!(v2.fields[3].precision, 10);
    assert_eq!(v2.fields[3].scale, 2);
    assert_eq!(
        v2.fields[4]
            .range_element_type
            .as_ref()
            .map(|e| e.r#type.as_str()),
        Some("TIMESTAMP")
    );
    assert_eq!(BigQueryTableSchema::try_from(&v2).ok(), Some(schema));
}

#[test]
fn empty_mode_is_nullable() {
    let schema = v2_schema(vec![v2_field("c", "INT64", "")]).expect("valid test input");
    assert_eq!(schema.fields[0].mode, BigQueryFieldMode::Nullable);
    let schema = storage_schema(vec![storage_field(
        "c",
        StorageType::Int64,
        StorageMode::Unspecified,
    )])
    .expect("valid test input");
    assert_eq!(schema.fields[0].mode, BigQueryFieldMode::Nullable);
    assert_eq!(
        unsupported(v2_schema(vec![v2_field("c", "INT64", "OPTIONAL")])),
        "c"
    );
}

#[test]
fn unknown_type_name_is_unsupported() {
    assert_eq!(
        unsupported(v2_schema(vec![v2_field("c", "INT", "NULLABLE")])),
        "c"
    );
    let mut nested = v2_field("rec", "RECORD", "NULLABLE");
    nested.fields = vec![v2_field("x", "VARCHAR", "NULLABLE")];
    assert_eq!(unsupported(v2_schema(vec![nested])), "rec.x");
    assert_eq!(
        unsupported(storage_schema(vec![storage_field(
            "c",
            StorageType::Unspecified,
            StorageMode::Nullable
        )])),
        "c"
    );
    let mut no_element = v2_field("r", "RANGE", "NULLABLE");
    no_element.range_element_type = None;
    assert_eq!(unsupported(v2_schema(vec![no_element])), "r");
    let mut bad_element = v2_field("r", "RANGE", "NULLABLE");
    bad_element.range_element_type = Some(v2::table_field_schema::FieldElementType {
        r#type: "INT64".to_string(),
    });
    assert_eq!(unsupported(v2_schema(vec![bad_element])), "r");
}

#[test]
fn numeric_precision_without_scale_equals_scale_zero() {
    let mut p10 = v2_field("n", "NUMERIC", "NULLABLE");
    p10.precision = 10;
    let mut p10s0 = p10.clone();
    p10s0.scale = 0;
    let mut storage_p10 = storage_field("n", StorageType::Numeric, StorageMode::Nullable);
    storage_p10.precision = 10;
    let want = BigQueryFieldType::Numeric(Some(BigQueryDecimalParams {
        precision: 10,
        scale: 0,
    }));
    assert_eq!(
        v2_schema(vec![p10]).expect("valid test input").fields[0].field_type,
        want
    );
    assert_eq!(
        v2_schema(vec![p10s0]).expect("valid test input").fields[0].field_type,
        want
    );
    assert_eq!(
        storage_schema(vec![storage_p10])
            .expect("valid test input")
            .fields[0]
            .field_type,
        want
    );

    let mut no_precision = v2_field("n", "BIGNUMERIC", "NULLABLE");
    no_precision.scale = 5;
    assert_eq!(
        v2_schema(vec![no_precision])
            .expect("valid test input")
            .fields[0]
            .field_type,
        BigQueryFieldType::BigNumeric(None),
        "precision 0 is unset whatever the scale"
    );
    let mut max_on_int = v2_field("i", "INT64", "NULLABLE");
    max_on_int.max_length = 10;
    assert_eq!(
        v2_schema(vec![max_on_int])
            .expect("valid test input")
            .fields[0]
            .field_type,
        BigQueryFieldType::Int64,
        "max_length is kept for STRING and BYTES only"
    );
}

#[test]
fn timestamp_picosecond_precision_is_unsupported() {
    let mut pico = v2_field("t", "TIMESTAMP", "NULLABLE");
    pico.timestamp_precision = Some(12);
    assert_eq!(unsupported(v2_schema(vec![pico])), "t");
    let mut storage_pico = storage_field("t", StorageType::Timestamp, StorageMode::Nullable);
    storage_pico.timestamp_precision = Some(12);
    assert_eq!(unsupported(storage_schema(vec![storage_pico])), "t");
    let mut micro = v2_field("t", "TIMESTAMP", "NULLABLE");
    micro.timestamp_precision = Some(6);
    assert_eq!(
        v2_schema(vec![micro]).expect("valid test input").fields[0].field_type,
        BigQueryFieldType::Timestamp
    );
}

#[test]
fn display_prints_googlesql_type_syntax() {
    let cases = [
        (BigQueryFieldType::Int64, "INT64"),
        (BigQueryFieldType::Float64, "FLOAT64"),
        (BigQueryFieldType::Bool, "BOOL"),
        (BigQueryFieldType::String { max_length: None }, "STRING"),
        (
            BigQueryFieldType::String {
                max_length: Some(10),
            },
            "STRING(10)",
        ),
        (
            BigQueryFieldType::Bytes {
                max_length: Some(3),
            },
            "BYTES(3)",
        ),
        (BigQueryFieldType::Numeric(None), "NUMERIC"),
        (
            BigQueryFieldType::Numeric(Some(BigQueryDecimalParams {
                precision: 10,
                scale: 2,
            })),
            "NUMERIC(10, 2)",
        ),
        (
            BigQueryFieldType::BigNumeric(Some(BigQueryDecimalParams {
                precision: 40,
                scale: 0,
            })),
            "BIGNUMERIC(40, 0)",
        ),
        (BigQueryFieldType::Date, "DATE"),
        (BigQueryFieldType::Time, "TIME"),
        (BigQueryFieldType::DateTime, "DATETIME"),
        (BigQueryFieldType::Timestamp, "TIMESTAMP"),
        (BigQueryFieldType::Geography, "GEOGRAPHY"),
        (BigQueryFieldType::Json, "JSON"),
        (BigQueryFieldType::Interval, "INTERVAL"),
        (
            BigQueryFieldType::Range(BigQueryRangeElementType::Date),
            "RANGE<DATE>",
        ),
        (
            BigQueryFieldType::Range(BigQueryRangeElementType::Timestamp),
            "RANGE<TIMESTAMP>",
        ),
        (
            BigQueryFieldType::Struct(vec![
                field("a", BigQueryFieldType::Int64, BigQueryFieldMode::Required),
                field(
                    "b",
                    BigQueryFieldType::String { max_length: None },
                    BigQueryFieldMode::Repeated,
                ),
            ]),
            "STRUCT<a INT64, b ARRAY<STRING>>",
        ),
    ];
    for (ty, text) in cases {
        assert_eq!(ty.to_string(), text);
    }
}

#[test]
fn arrow_schema_normalises_to_the_same_vocabulary() {
    let ext = |name: &str| HashMap::from([("ARROW:extension:name".to_string(), name.to_string())]);
    let range_meta = HashMap::from([("google:sqlType".to_string(), "range".to_string())]);
    let ts = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let schema = Schema::new(vec![
        Field::new("i", DataType::Int64, false),
        Field::new("j", DataType::Utf8, true).with_metadata(ext("google:sqlType:json")),
        Field::new_list("tags", Field::new("item", DataType::Utf8, true), false)
            .with_metadata(ext("google:sqlType:geography")),
        Field::new(
            "r",
            DataType::Struct(Fields::from(vec![
                Field::new("start", ts.clone(), true),
                Field::new("end", ts, true),
            ])),
            true,
        )
        .with_metadata(range_meta),
        Field::new(
            "s",
            DataType::Struct(Fields::from(vec![Field::new(
                "iv",
                DataType::Interval(IntervalUnit::MonthDayNano),
                true,
            )
            .with_metadata(ext("google:sqlType:interval"))])),
            true,
        ),
    ]);
    let got = BigQueryTableSchema::from_arrow(&schema).expect("valid test input");
    assert_eq!(
        got,
        BigQueryTableSchema {
            fields: vec![
                field("i", BigQueryFieldType::Int64, BigQueryFieldMode::Required),
                field("j", BigQueryFieldType::Json, BigQueryFieldMode::Nullable),
                field(
                    "tags",
                    BigQueryFieldType::Geography,
                    BigQueryFieldMode::Repeated
                ),
                field(
                    "r",
                    BigQueryFieldType::Range(BigQueryRangeElementType::Timestamp),
                    BigQueryFieldMode::Nullable
                ),
                field(
                    "s",
                    BigQueryFieldType::Struct(vec![field(
                        "iv",
                        BigQueryFieldType::Interval,
                        BigQueryFieldMode::Nullable
                    )]),
                    BigQueryFieldMode::Nullable
                ),
            ]
        }
    );
    let bad = Schema::new(vec![Field::new("h", DataType::Float16, true)]);
    assert_eq!(unsupported(BigQueryTableSchema::from_arrow(&bad)), "h");
}
