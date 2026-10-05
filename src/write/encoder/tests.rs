use super::*;
use crate::errors::{BigQueryError, BigQuerySerializationError};
use crate::types::civil;
use crate::types::decimal;
use crate::types::temporal::TAG_DATE;
use crate::types::testkit::field;
use crate::write::descriptor::{CHANGE_SEQUENCE_NUMBER_COLUMN, CHANGE_TYPE_COLUMN};
use crate::{
    BigQueryDate, BigQueryDecimal, BigQueryFieldMode, BigQueryFieldSchema, BigQueryFieldType,
    BigQueryInterval, BigQueryJson, BigQueryRange, BigQueryRangeElementType, BigQueryTableSchema,
    BigQueryTimestamp,
};
use arrow_buffer::i256;
use gcloud_sdk::prost_types::field_descriptor_proto::{Label, Type as ProtoType};
use gcloud_sdk::prost_types::{FileDescriptorProto, FileDescriptorSet};
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor, Value};
use serde::Serialize;

fn nullable(name: &str, field_type: BigQueryFieldType) -> BigQueryFieldSchema {
    field(name, field_type, BigQueryFieldMode::Nullable)
}

const STRING: BigQueryFieldType = BigQueryFieldType::String { max_length: None };
const BYTES: BigQueryFieldType = BigQueryFieldType::Bytes { max_length: None };

fn schema() -> BigQueryTableSchema {
    use BigQueryFieldMode::*;
    use BigQueryFieldType as FieldType;
    BigQueryTableSchema {
        fields: vec![
            field("quantity", FieldType::Int64, Required),
            nullable("price", FieldType::Float64),
            nullable("in_stock", FieldType::Bool),
            nullable("name", STRING),
            nullable("payload", BYTES),
            nullable("order_date", FieldType::Date),
            nullable("pickup_time", FieldType::Time),
            nullable("placed_local", FieldType::DateTime),
            nullable("placed_at", FieldType::Timestamp),
            nullable("total", FieldType::Numeric(None)),
            nullable("big_total", FieldType::BigNumeric(None)),
            nullable("location", FieldType::Geography),
            nullable("attributes", FieldType::Json),
            nullable("lead_time", FieldType::Interval),
            nullable(
                "booking_window",
                FieldType::Range(BigQueryRangeElementType::Date),
            ),
            nullable(
                "line_item",
                FieldType::Struct(vec![
                    nullable("count", FieldType::Int64),
                    field("tags", STRING, Repeated),
                    nullable(
                        "inner",
                        FieldType::Struct(vec![nullable("delivered_on", FieldType::Date)]),
                    ),
                ]),
            ),
            field("lot_numbers", FieldType::Int64, Repeated),
            field(
                "measurements",
                FieldType::Struct(vec![
                    nullable("unit", STRING),
                    nullable("amount", FieldType::Float64),
                ]),
                Repeated,
            ),
        ],
    }
}

fn plan() -> Arc<WritePlan> {
    Arc::new(WritePlan::new(&schema(), false))
}

/// The plan's root message as prost-reflect sees it, to decode what the encoder wrote.
pub(crate) fn message_descriptor(plan: &WritePlan) -> MessageDescriptor {
    let file = FileDescriptorProto {
        name: Some("row.proto".into()),
        syntax: Some("proto2".into()),
        message_type: vec![plan.descriptor().clone()],
        ..Default::default()
    };
    let pool = DescriptorPool::from_file_descriptor_set(FileDescriptorSet { file: vec![file] })
        .expect("the plan's descriptor is a valid proto2 message");
    pool.get_message_by_name(crate::write::descriptor::ROOT_MESSAGE)
        .expect("the root message is in the pool")
}

fn encode<T: Serialize + ?Sized>(plan: &Arc<WritePlan>, row: &T) -> Result<Vec<u8>, CodecError> {
    let mut out = Vec::new();
    Encoder::new(plan.clone()).encode(row, &mut out)?;
    Ok(out)
}

fn decoded<T: Serialize + ?Sized>(plan: &Arc<WritePlan>, row: &T) -> DynamicMessage {
    let bytes = encode(plan, row).expect("the row encodes");
    DynamicMessage::decode(message_descriptor(plan), bytes.as_slice()).expect("the bytes decode")
}

fn encode_error<T: Serialize + ?Sized>(
    plan: &Arc<WritePlan>,
    row: &T,
) -> BigQuerySerializationError {
    match encode(plan, row)
        .expect_err("the row must fail")
        .into_serialize()
    {
        BigQueryError::SerializeError(details) => details,
        other => panic!("expected a serialize error, got {other:?}"),
    }
}

/// The field's value, `None` when the message does not have it.
fn get(message: &DynamicMessage, name: &str) -> Option<Value> {
    message
        .has_field_by_name(name)
        .then(|| {
            message
                .get_field_by_name(name)
                .map(|value| value.into_owned())
        })
        .flatten()
}

fn i64_of(message: &DynamicMessage, name: &str) -> Option<i64> {
    get(message, name).and_then(|value| value.as_i64())
}

fn i32_of(message: &DynamicMessage, name: &str) -> Option<i32> {
    get(message, name).and_then(|value| value.as_i32())
}

fn text_of(message: &DynamicMessage, name: &str) -> Option<String> {
    get(message, name).and_then(|value| value.as_str().map(str::to_string))
}

fn bytes_of(message: &DynamicMessage, name: &str) -> Option<Vec<u8>> {
    get(message, name).and_then(|value| value.as_bytes().map(|bytes| bytes.to_vec()))
}

fn message_of(message: &DynamicMessage, name: &str) -> Option<DynamicMessage> {
    get(message, name).and_then(|value| value.as_message().cloned())
}

fn list_of(message: &DynamicMessage, name: &str) -> Vec<Value> {
    get(message, name)
        .and_then(|value| value.as_list().map(<[Value]>::to_vec))
        .unwrap_or_default()
}

fn le_bytes(value: i256) -> Vec<u8> {
    let (bytes, length) = decimal::decimal_le_bytes(value);
    bytes[..length].to_vec()
}

#[derive(Serialize, Clone)]
struct Inner {
    delivered_on: Option<jiff::civil::Date>,
}

#[derive(Serialize, Clone)]
struct LineItem {
    count: Option<i64>,
    tags: Vec<String>,
    inner: Option<Inner>,
}

#[derive(Serialize, Clone)]
struct Measurement {
    unit: Option<String>,
    amount: Option<f64>,
}

#[derive(Serialize, Clone)]
struct Row {
    quantity: i64,
    price: Option<f64>,
    in_stock: Option<bool>,
    name: Option<String>,
    #[serde(with = "serde_bytes")]
    payload: Vec<u8>,
    order_date: Option<jiff::civil::Date>,
    pickup_time: Option<jiff::civil::Time>,
    placed_local: Option<jiff::civil::DateTime>,
    placed_at: Option<jiff::Timestamp>,
    total: Option<String>,
    big_total: Option<String>,
    location: Option<String>,
    attributes: Option<BigQueryJson<serde_json::Value>>,
    lead_time: Option<BigQueryInterval>,
    booking_window: Option<BigQueryRange<jiff::civil::Date>>,
    line_item: Option<LineItem>,
    lot_numbers: Vec<i64>,
    measurements: Vec<Measurement>,
}

fn date(text: &str) -> jiff::civil::Date {
    text.parse().expect("a valid date")
}

fn full_row() -> Row {
    Row {
        quantity: i64::MIN,
        price: Some(f64::NAN),
        in_stock: Some(true),
        name: Some("héllo 世界 🦀".into()),
        payload: vec![0, 255],
        order_date: Some(date("0001-01-01")),
        pickup_time: Some("23:59:59.999999".parse().expect("a valid time")),
        placed_local: Some(
            "2024-02-29T12:34:56.789012"
                .parse()
                .expect("a valid datetime"),
        ),
        placed_at: Some("1969-07-20T20:17:40.5Z".parse().expect("a valid timestamp")),
        total: Some("-99999999999999999999999999999.999999999".into()),
        big_total: Some(
            "578960446186580977117854925043439539266.34992332820282019728792003956564819967".into(),
        ),
        location: Some("POINT(1 2)".into()),
        attributes: Some(BigQueryJson(serde_json::json!({"count": 1}))),
        lead_time: Some(BigQueryInterval {
            months: -14,
            days: 3,
            nanos: 14_706_000_789_000,
        }),
        booking_window: Some(BigQueryRange {
            start: None,
            end: Some(date("2024-01-01")),
        }),
        line_item: Some(LineItem {
            count: Some(7),
            tags: vec!["gift".into(), "fragile".into()],
            inner: Some(Inner {
                delivered_on: Some(date("2024-02-29")),
            }),
        }),
        lot_numbers: vec![1, -2, 3],
        measurements: vec![
            Measurement {
                unit: Some("kg".into()),
                amount: Some(1.0),
            },
            Measurement {
                unit: None,
                amount: None,
            },
        ],
    }
}

#[test]
fn descriptor_follows_the_table_schema() {
    let plan = plan();
    let descriptor = plan.descriptor();
    assert_eq!(descriptor.name(), "row");
    let by = |name: &str| {
        descriptor
            .field
            .iter()
            .find(|candidate| candidate.name() == name)
            .cloned()
            .unwrap_or_else(|| panic!("no field {name}"))
    };
    assert_eq!(
        (
            by("quantity").number(),
            by("quantity").label(),
            by("quantity").r#type()
        ),
        (1, Label::Required, ProtoType::Int64)
    );
    assert_eq!(
        [
            by("order_date"),
            by("pickup_time"),
            by("placed_local"),
            by("placed_at")
        ]
        .map(|field| field.r#type()),
        [
            ProtoType::Int32,
            ProtoType::Int64,
            ProtoType::Int64,
            ProtoType::Int64
        ]
    );
    assert_eq!(
        [
            by("total"),
            by("big_total"),
            by("lead_time"),
            by("attributes"),
            by("location"),
            by("payload")
        ]
        .map(|field| field.r#type()),
        [
            ProtoType::Bytes,
            ProtoType::Bytes,
            ProtoType::String,
            ProtoType::String,
            ProtoType::String,
            ProtoType::Bytes
        ]
    );
    assert_eq!(
        (
            by("price").r#type(),
            by("in_stock").r#type(),
            by("price").label()
        ),
        (ProtoType::Double, ProtoType::Bool, Label::Optional)
    );
    assert_eq!(
        (
            by("lot_numbers").label(),
            by("measurements").label(),
            by("measurements").number()
        ),
        (Label::Repeated, Label::Repeated, 18)
    );
    let nested = |type_name: &str| {
        descriptor
            .nested_type
            .iter()
            .find(|name| name.name() == type_name)
            .cloned()
            .unwrap_or_else(|| panic!("no nested type {type_name}"))
    };
    let names = |message: &gcloud_sdk::prost_types::DescriptorProto| {
        message
            .field
            .iter()
            .map(|field| (field.name().to_string(), field.r#type()))
            .collect::<Vec<_>>()
    };
    let line_item = nested(by("line_item").type_name());
    assert_eq!(
        names(&line_item),
        [
            ("count".to_string(), ProtoType::Int64),
            ("tags".to_string(), ProtoType::String),
            ("inner".to_string(), ProtoType::Message)
        ]
    );
    let inner_type = line_item
        .field
        .iter()
        .find(|field| field.name() == "inner")
        .map(|field| field.type_name().to_string())
        .unwrap_or_default();
    assert_eq!(
        names(&nested(&inner_type)),
        [("delivered_on".to_string(), ProtoType::Int32)]
    );
    assert_eq!(
        names(&nested(by("booking_window").type_name())),
        [
            ("start".to_string(), ProtoType::Int32),
            ("end".to_string(), ProtoType::Int32)
        ]
    );
    // Every nested type sits flat in the root, which prost-reflect resolves.
    assert!(descriptor
        .nested_type
        .iter()
        .all(|name| name.nested_type.is_empty()));
    message_descriptor(&plan);
}

#[test]
fn every_type_encodes_to_its_accepted_wire_form() {
    let plan = plan();
    let got = decoded(&plan, &full_row());
    assert_eq!(i64_of(&got, "quantity"), Some(i64::MIN));
    assert!(get(&got, "price")
        .and_then(|value| value.as_f64())
        .is_some_and(f64::is_nan));
    assert_eq!(
        get(&got, "in_stock").and_then(|value| value.as_bool()),
        Some(true)
    );
    assert_eq!(text_of(&got, "name").as_deref(), Some("héllo 世界 🦀"));
    assert_eq!(bytes_of(&got, "payload"), Some(vec![0, 255]));
    assert_eq!(i32_of(&got, "order_date"), Some(civil::DATE_MIN_DAYS));
    assert_eq!(
        i64_of(&got, "pickup_time"),
        Some(civil::pack_time(86_399_999_999))
    );
    let local_micros = civil::parse_datetime("2024-02-29T12:34:56.789012").expect("valid");
    assert_eq!(
        i64_of(&got, "placed_local"),
        Some(civil::pack_datetime(local_micros))
    );
    assert_eq!(
        i64_of(&got, "placed_at"),
        Some(civil::parse_timestamp("1969-07-20T20:17:40.5Z").expect("valid"))
    );
    assert_eq!(
        bytes_of(&got, "total"),
        Some(le_bytes(
            decimal::parse_numeric("-99999999999999999999999999999.999999999").expect("valid")
        ))
    );
    assert_eq!(bytes_of(&got, "big_total"), Some(le_bytes(i256::MAX)));
    assert_eq!(text_of(&got, "location").as_deref(), Some("POINT(1 2)"));
    assert_eq!(
        text_of(&got, "attributes").as_deref(),
        Some(r#"{"count":1}"#)
    );
    assert_eq!(
        text_of(&got, "lead_time").as_deref(),
        Some("-1-2 3 4:5:6.000789")
    );
    let rng = message_of(&got, "booking_window").expect("rng is set");
    assert_eq!(
        (i32_of(&rng, "start"), i32_of(&rng, "end")),
        (None, Some(19723))
    );
    let line_item = message_of(&got, "line_item").expect("line_item is set");
    assert_eq!(i64_of(&line_item, "count"), Some(7));
    assert_eq!(
        list_of(&line_item, "tags"),
        [
            Value::String("gift".into()),
            Value::String("fragile".into())
        ]
    );
    let inner = message_of(&line_item, "inner").expect("inner is set");
    assert_eq!(i32_of(&inner, "delivered_on"), Some(19782));
    assert_eq!(
        list_of(&got, "lot_numbers"),
        [Value::I64(1), Value::I64(-2), Value::I64(3)]
    );
    let measurements = list_of(&got, "measurements");
    assert_eq!(measurements.len(), 2);
    let first = measurements[0].as_message().expect("a message");
    assert_eq!(
        (
            text_of(first, "unit").as_deref(),
            get(first, "amount").and_then(|value| value.as_f64())
        ),
        (Some("kg"), Some(1.0))
    );
    let second = measurements[1].as_message().expect("a message");
    assert_eq!((get(second, "unit"), get(second, "amount")), (None, None));
}

#[test]
fn repeated_scalars_are_packed() {
    #[derive(Serialize)]
    struct LotNumbers {
        quantity: i64,
        lot_numbers: Vec<i64>,
    }
    let bytes = encode(
        &plan(),
        &LotNumbers {
            quantity: 1,
            lot_numbers: vec![1, 2, 3],
        },
    )
    .expect("encodes");
    // key(1, varint) 1, then key(17, length-delimited) = 0x8a 0x01, length 3, 1 2 3.
    assert_eq!(bytes, [0x08, 0x01, 0x8a, 0x01, 0x03, 0x01, 0x02, 0x03]);
}

#[test]
fn none_is_absent_and_a_required_none_is_an_error_naming_the_field() {
    #[derive(Serialize)]
    struct Optional {
        quantity: Option<i64>,
        name: Option<String>,
        line_item: Option<LineItem>,
    }
    let plan = plan();
    let got = decoded(
        &plan,
        &Optional {
            quantity: Some(1),
            name: None,
            line_item: None,
        },
    );
    assert_eq!(
        (
            i64_of(&got, "quantity"),
            get(&got, "name"),
            get(&got, "line_item")
        ),
        (Some(1), None, None)
    );
    let error = encode_error(
        &plan,
        &Optional {
            quantity: None,
            name: None,
            line_item: None,
        },
    );
    assert_eq!(
        (error.kind, error.path.as_str()),
        (BigQueryCodecErrorKind::NullForRequired, "quantity")
    );
    #[derive(Serialize)]
    struct Missing {
        name: String,
    }
    let error = encode_error(&plan, &Missing { name: "Ada".into() });
    assert_eq!(
        (error.kind, error.path.as_str()),
        (BigQueryCodecErrorKind::MissingRequiredField, "quantity")
    );
}

#[test]
fn errors_name_the_field_path() {
    let plan = plan();
    #[derive(Serialize)]
    struct BadTag {
        quantity: i64,
        line_item: BadLineItem,
    }
    #[derive(Serialize)]
    struct BadLineItem {
        tags: (String, i64),
    }
    let error = encode_error(
        &plan,
        &BadTag {
            quantity: 1,
            line_item: BadLineItem {
                tags: ("gift".into(), 5),
            },
        },
    );
    assert_eq!(
        (error.kind, error.path.as_str()),
        (BigQueryCodecErrorKind::TypeMismatch, "line_item.tags[1]")
    );
    #[derive(Serialize)]
    struct Unknown {
        quantity: i64,
        line_item: UnknownInner,
    }
    #[derive(Serialize)]
    struct UnknownInner {
        inner: UnknownLeaf,
    }
    #[derive(Serialize)]
    struct UnknownLeaf {
        discount: i64,
    }
    let error = encode_error(
        &plan,
        &Unknown {
            quantity: 1,
            line_item: UnknownInner {
                inner: UnknownLeaf { discount: 2 },
            },
        },
    );
    assert_eq!(
        (error.kind, error.path.as_str()),
        (
            BigQueryCodecErrorKind::UnknownField,
            "line_item.inner.discount"
        )
    );
    #[derive(Serialize)]
    struct BadMeasurement {
        amount: &'static str,
    }
    #[derive(Serialize)]
    struct BadMeasurements {
        quantity: i64,
        measurements: Vec<BadMeasurement>,
    }
    let error = encode_error(
        &plan,
        &BadMeasurements {
            quantity: 1,
            measurements: vec![BadMeasurement { amount: "heavy" }],
        },
    );
    assert_eq!(error.path, "measurements[0].amount");
    #[derive(Serialize)]
    struct BadDate {
        quantity: i64,
        order_date: &'static str,
    }
    let error = encode_error(
        &plan,
        &BadDate {
            quantity: 1,
            order_date: "2023-02-29",
        },
    );
    assert_eq!(
        (error.kind, error.path.as_str()),
        (BigQueryCodecErrorKind::InvalidText, "order_date")
    );
}

#[test]
fn integer_forms_of_temporal_types_match_the_read_side() {
    #[derive(Serialize)]
    struct Ints {
        quantity: i64,
        order_date: i32,
        pickup_time: i64,
        placed_local: i64,
        placed_at: i64,
    }
    let local_micros = civil::parse_datetime("2024-02-29T12:34:56.789012").expect("valid");
    let got = decoded(
        &plan(),
        &Ints {
            quantity: 0,
            order_date: 19782,
            pickup_time: 45_296_000_000,
            placed_local: local_micros,
            placed_at: civil::TIMESTAMP_MAX_MICROS,
        },
    );
    assert_eq!(i32_of(&got, "order_date"), Some(19782));
    assert_eq!(
        i64_of(&got, "pickup_time"),
        Some(civil::pack_time(45_296_000_000))
    );
    assert_eq!(
        i64_of(&got, "placed_local"),
        Some(civil::pack_datetime(local_micros))
    );
    assert_eq!(i64_of(&got, "placed_at"), Some(civil::TIMESTAMP_MAX_MICROS));
    #[derive(Serialize)]
    struct OutOfDay {
        quantity: i64,
        pickup_time: i64,
    }
    let error = encode_error(
        &plan(),
        &OutOfDay {
            quantity: 0,
            pickup_time: civil::MICROS_PER_DAY,
        },
    );
    assert_eq!(
        (error.kind, error.path.as_str()),
        (BigQueryCodecErrorKind::OutOfRange, "pickup_time")
    );
    #[derive(Serialize)]
    struct PastMax {
        quantity: i64,
        placed_at: i64,
    }
    let error = encode_error(
        &plan(),
        &PastMax {
            quantity: 0,
            placed_at: civil::TIMESTAMP_MAX_MICROS + 1,
        },
    );
    assert_eq!(
        (error.kind, error.path.as_str()),
        (BigQueryCodecErrorKind::OutOfRange, "placed_at")
    );
}

#[test]
fn wrappers_and_alternate_forms() {
    #[derive(Debug, PartialEq)]
    struct Cents(i64);
    impl std::fmt::Display for Cents {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "{}.{:02}", self.0 / 100, self.0 % 100)
        }
    }
    #[derive(Serialize)]
    #[serde(rename_all = "lowercase")]
    enum Color {
        Red,
    }
    #[derive(Serialize)]
    struct Wrappers {
        quantity: u8,
        total: BigQueryDecimal<Cents>,
        big_total: f64,
        placed_at: &'static str,
        placed_local: &'static str,
        lead_time: &'static str,
        attributes: &'static str,
        name: Color,
        payload: [u8; 2],
        lot_numbers: std::collections::VecDeque<i32>,
        line_item: std::collections::BTreeMap<String, i64>,
    }
    let row = Wrappers {
        quantity: 3,
        total: BigQueryDecimal(Cents(12345)),
        big_total: 0.5,
        placed_at: "9999-12-31 23:59:59.999999+00:00",
        placed_local: "2024-02-29 12:34:56",
        lead_time: "1-2 3 4:5:6",
        attributes: r#"{"unit":[1,2]}"#,
        name: Color::Red,
        payload: *b"ab",
        lot_numbers: [4, 5].into(),
        line_item: [("count".to_string(), 9)].into(),
    };
    let got = decoded(&plan(), &row);
    assert_eq!(i64_of(&got, "quantity"), Some(3));
    assert_eq!(
        bytes_of(&got, "total"),
        Some(le_bytes(i256::from_i128(123_450_000_000)))
    );
    assert_eq!(
        bytes_of(&got, "big_total"),
        Some(le_bytes(i256::from_i128(5 * 10i128.pow(37))))
    );
    assert_eq!(i64_of(&got, "placed_at"), Some(civil::TIMESTAMP_MAX_MICROS));
    assert_eq!(
        i64_of(&got, "placed_local"),
        Some(civil::pack_datetime(
            civil::parse_datetime("2024-02-29T12:34:56").expect("valid")
        ))
    );
    assert_eq!(text_of(&got, "lead_time").as_deref(), Some("1-2 3 4:5:6"));
    assert_eq!(
        text_of(&got, "attributes").as_deref(),
        Some(r#"{"unit":[1,2]}"#)
    );
    assert_eq!(text_of(&got, "name").as_deref(), Some("red"));
    assert_eq!(bytes_of(&got, "payload"), Some(b"ab".to_vec()));
    assert_eq!(list_of(&got, "lot_numbers"), [Value::I64(4), Value::I64(5)]);
    let line_item = message_of(&got, "line_item").expect("line_item is set");
    assert_eq!(i64_of(&line_item, "count"), Some(9));
    #[derive(Serialize)]
    struct WholeNumeric {
        quantity: i64,
        total: i64,
        big_total: i64,
    }
    let got = decoded(
        &plan(),
        &WholeNumeric {
            quantity: 0,
            total: -7,
            big_total: 2,
        },
    );
    assert_eq!(
        bytes_of(&got, "total"),
        Some(le_bytes(i256::from_i128(-7_000_000_000)))
    );
    assert_eq!(
        bytes_of(&got, "big_total"),
        Some(le_bytes(
            i256::from_i128(2 * 10i128.pow(19)).wrapping_mul(i256::from_i128(10i128.pow(19)))
        ))
    );
}

#[test]
fn skipped_reordered_and_flattened_fields() {
    #[derive(Serialize)]
    struct Sparse {
        #[serde(skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        quantity: i64,
    }
    #[derive(Serialize)]
    struct Flat {
        quantity: i64,
        #[serde(flatten)]
        rest: FlatRest,
    }
    #[derive(Serialize)]
    struct FlatRest {
        name: String,
        in_stock: bool,
    }
    let plan = plan();
    let descriptor = message_descriptor(&plan);
    let mut encoder = Encoder::new(plan.clone());
    let rows = [
        Sparse {
            name: Some("Ada".into()),
            quantity: 1,
        },
        Sparse {
            name: None,
            quantity: 2,
        },
        Sparse {
            name: Some("Grace".into()),
            quantity: 3,
        },
    ];
    for (k, row) in rows.iter().enumerate() {
        let mut out = Vec::new();
        encoder.encode(row, &mut out).expect("encodes");
        let got = DynamicMessage::decode(descriptor.clone(), out.as_slice()).expect("decodes");
        assert_eq!(
            (i64_of(&got, "quantity"), text_of(&got, "name").is_some()),
            (Some(k as i64 + 1), k != 1)
        );
    }
    let mut out = Vec::new();
    encoder
        .encode(
            &Flat {
                quantity: 9,
                rest: FlatRest {
                    name: "Linus".into(),
                    in_stock: false,
                },
            },
            &mut out,
        )
        .expect("encodes");
    let got = DynamicMessage::decode(descriptor, out.as_slice()).expect("decodes");
    assert_eq!(
        (
            i64_of(&got, "quantity"),
            text_of(&got, "name").as_deref(),
            get(&got, "in_stock").and_then(|value| value.as_bool())
        ),
        (Some(9), Some("Linus"), Some(false))
    );
}

#[test]
fn a_null_array_element_is_an_error() {
    #[derive(Serialize)]
    struct WithNullElement {
        quantity: i64,
        lot_numbers: Vec<Option<i64>>,
    }
    let error = encode_error(
        &plan(),
        &WithNullElement {
            quantity: 1,
            lot_numbers: vec![Some(1), None],
        },
    );
    assert_eq!(
        (error.kind, error.path.as_str()),
        (BigQueryCodecErrorKind::NullArrayElement, "lot_numbers[1]")
    );
}

#[test]
fn nested_lengths_over_127_bytes_are_backpatched() {
    let plan = plan();
    let mut long = full_row();
    if let Some(line_item) = long.line_item.as_mut() {
        line_item.tags = vec!["gift".repeat(200), "fragile ".repeat(2_500)];
    }
    long.measurements = (0..300)
        .map(|index| Measurement {
            unit: Some(format!("k{index}")),
            amount: Some(f64::from(index)),
        })
        .collect();
    long.lot_numbers = (0..100).map(|index| index * 1_000_000_007).collect();
    let got = decoded(&plan, &long);
    let line_item = message_of(&got, "line_item").expect("line_item is set");
    let tags = list_of(&line_item, "tags");
    assert_eq!(tags[1].as_str().map(str::len), Some(20_000));
    assert_eq!(
        message_of(&line_item, "inner").and_then(|message| i32_of(&message, "delivered_on")),
        Some(19782)
    );
    let measurements = list_of(&got, "measurements");
    assert_eq!(measurements.len(), 300);
    assert_eq!(
        measurements[299]
            .as_message()
            .and_then(|message| text_of(message, "unit"))
            .as_deref(),
        Some("k299")
    );
    assert_eq!(list_of(&got, "lot_numbers").len(), 100);
    assert_eq!(text_of(&got, "location").as_deref(), Some("POINT(1 2)"));
}

#[test]
fn float64_rejects_integers() {
    #[derive(Serialize)]
    struct IntegerPrice {
        quantity: i64,
        price: i64,
    }
    let error = encode_error(
        &plan(),
        &IntegerPrice {
            quantity: 1,
            price: 2,
        },
    );
    assert_eq!(
        (error.kind, error.path.as_str()),
        (BigQueryCodecErrorKind::TypeMismatch, "price")
    );
    #[derive(Serialize)]
    struct NarrowPrice {
        quantity: i64,
        price: f32,
    }
    let got = decoded(
        &plan(),
        &NarrowPrice {
            quantity: 1,
            price: 0.5,
        },
    );
    assert_eq!(
        get(&got, "price").and_then(|value| value.as_f64()),
        Some(0.5)
    );
}

#[test]
fn bytes_rejects_strings() {
    #[derive(Serialize)]
    struct TextPayload {
        quantity: i64,
        payload: &'static str,
    }
    let error = encode_error(
        &plan(),
        &TextPayload {
            quantity: 1,
            payload: "AQI=",
        },
    );
    assert_eq!(
        (error.kind, error.path.as_str()),
        (BigQueryCodecErrorKind::TypeMismatch, "payload")
    );
}

#[test]
fn string_rejects_bytes() {
    #[derive(Serialize)]
    struct ByteBufName {
        quantity: i64,
        #[serde(with = "serde_bytes")]
        name: Vec<u8>,
    }
    let error = encode_error(
        &plan(),
        &ByteBufName {
            quantity: 1,
            name: b"ok".to_vec(),
        },
    );
    assert_eq!(
        (error.kind, error.path.as_str()),
        (BigQueryCodecErrorKind::TypeMismatch, "name")
    );
    #[derive(Serialize)]
    struct BytesName {
        quantity: i64,
        name: Vec<u8>,
    }
    let error = encode_error(
        &plan(),
        &BytesName {
            quantity: 1,
            name: b"ok".to_vec(),
        },
    );
    assert_eq!(
        (error.kind, error.path.as_str()),
        (BigQueryCodecErrorKind::TypeMismatch, "name")
    );
}

#[test]
fn date_before_year_one_is_out_of_range() {
    #[derive(Serialize)]
    struct Jiff {
        quantity: i64,
        order_date: jiff::civil::Date,
    }
    #[derive(Serialize)]
    struct Wrapped {
        quantity: i64,
        order_date: BigQueryDate,
    }
    #[derive(Serialize)]
    struct Days {
        quantity: i64,
        order_date: i32,
    }
    #[derive(Serialize)]
    struct Text {
        quantity: i64,
        order_date: &'static str,
    }
    #[derive(Serialize)]
    struct Local {
        quantity: i64,
        placed_local: jiff::civil::DateTime,
    }
    let plan = plan();
    for year in [0, -1, -9999] {
        let date = jiff::civil::date(year, 12, 31);
        let error = encode_error(
            &plan,
            &Jiff {
                quantity: 0,
                order_date: date,
            },
        );
        assert_eq!(
            (error.kind, error.path.as_str()),
            (BigQueryCodecErrorKind::OutOfRange, "order_date"),
            "{date}"
        );
        let error = encode_error(
            &plan,
            &Wrapped {
                quantity: 0,
                order_date: BigQueryDate(date),
            },
        );
        assert_eq!(
            (error.kind, error.path.as_str()),
            (BigQueryCodecErrorKind::OutOfRange, "order_date"),
            "{date}"
        );
        let error = encode_error(
            &plan,
            &Local {
                quantity: 0,
                placed_local: date.at(1, 2, 3, 0),
            },
        );
        assert_eq!(
            (error.kind, error.path.as_str()),
            (BigQueryCodecErrorKind::OutOfRange, "placed_local"),
            "{date}"
        );
    }
    let error = encode_error(
        &plan,
        &Days {
            quantity: 0,
            order_date: civil::DATE_MIN_DAYS - 1,
        },
    );
    assert_eq!(
        (error.kind, error.path.as_str()),
        (BigQueryCodecErrorKind::OutOfRange, "order_date")
    );
    let error = encode_error(
        &plan,
        &Text {
            quantity: 0,
            order_date: "0000-12-31",
        },
    );
    assert_eq!(
        (error.kind, error.path.as_str()),
        (BigQueryCodecErrorKind::OutOfRange, "order_date")
    );
    let got = decoded(
        &plan,
        &Jiff {
            quantity: 0,
            order_date: date("0001-01-01"),
        },
    );
    assert_eq!(i32_of(&got, "order_date"), Some(civil::DATE_MIN_DAYS));
}

#[test]
fn interval_beyond_storage_read_range_is_refused() {
    #[derive(Serialize)]
    struct Text {
        quantity: i64,
        lead_time: &'static str,
    }
    #[derive(Serialize)]
    struct Parts {
        quantity: i64,
        lead_time: BigQueryInterval,
    }
    let plan = plan();
    let error = encode_error(
        &plan,
        &Text {
            quantity: 0,
            lead_time: "0-0 0 2562048:0:0",
        },
    );
    assert_eq!(
        (error.kind, error.path.as_str()),
        (BigQueryCodecErrorKind::OutOfRange, "lead_time")
    );
    let error = encode_error(
        &plan,
        &Parts {
            quantity: 0,
            lead_time: BigQueryInterval {
                months: 0,
                days: 0,
                nanos: 1_500,
            },
        },
    );
    assert_eq!(
        (error.kind, error.path.as_str()),
        (BigQueryCodecErrorKind::OutOfRange, "lead_time")
    );
    let got = decoded(
        &plan,
        &Text {
            quantity: 0,
            lead_time: "0-0 0 2562047:0:0",
        },
    );
    assert_eq!(
        text_of(&got, "lead_time").as_deref(),
        Some("0-0 0 2562047:0:0")
    );
}

#[test]
fn temporal_wrapper_on_another_temporal_column_is_type_mismatch() {
    #[derive(Serialize)]
    struct DateOnTimestamp {
        quantity: i64,
        placed_at: BigQueryDate,
    }
    #[derive(Serialize)]
    struct TimestampOnDate {
        quantity: i64,
        order_date: BigQueryTimestamp,
    }
    #[derive(Serialize)]
    struct DateOnInt {
        quantity: BigQueryDate,
    }
    let plan = plan();
    let error = encode_error(
        &plan,
        &DateOnTimestamp {
            quantity: 0,
            placed_at: BigQueryDate(date("2024-01-01")),
        },
    );
    assert_eq!(
        (error.kind, error.path.as_str()),
        (BigQueryCodecErrorKind::TypeMismatch, "placed_at")
    );
    let error = encode_error(
        &plan,
        &TimestampOnDate {
            quantity: 0,
            order_date: BigQueryTimestamp(jiff::Timestamp::UNIX_EPOCH),
        },
    );
    assert_eq!(
        (error.kind, error.path.as_str()),
        (BigQueryCodecErrorKind::TypeMismatch, "order_date")
    );
    let error = encode_error(
        &plan,
        &DateOnInt {
            quantity: BigQueryDate(date("2024-01-01")),
        },
    );
    assert_eq!(
        (error.kind, error.path.as_str()),
        (BigQueryCodecErrorKind::TypeMismatch, "quantity")
    );
}

/// Has the DATE wrapper's serde name and fails if asked for its text form.
struct NoTextDate(i32);

struct NoTextInner(i32);

impl Serialize for NoTextDate {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_newtype_struct(TAG_DATE, &NoTextInner(self.0))
    }
}

impl Serialize for NoTextInner {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            Err(serde::ser::Error::custom("the text form was asked for"))
        } else {
            serializer.serialize_i32(self.0)
        }
    }
}

/// Writes text to a human-readable serializer and bytes to any other, as `uuid` does.
struct UuidLike;

impl Serialize for UuidLike {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            serializer.serialize_str("67e55044-10b1-426f-9247-bb680e5fe0c8")
        } else {
            serializer.serialize_bytes(&[0x67; 16])
        }
    }
}

#[test]
fn temporal_wrappers_write_integers_without_text() {
    #[derive(Serialize)]
    struct WithoutText {
        quantity: i64,
        order_date: NoTextDate,
        booking_window: BigQueryRange<NoTextDate>,
        name: UuidLike,
    }
    let got = decoded(
        &plan(),
        &WithoutText {
            quantity: 0,
            order_date: NoTextDate(19782),
            booking_window: BigQueryRange {
                start: Some(NoTextDate(1)),
                end: None,
            },
            name: UuidLike,
        },
    );
    assert_eq!(i32_of(&got, "order_date"), Some(19782));
    let rng = message_of(&got, "booking_window").expect("rng is set");
    assert_eq!(
        (i32_of(&rng, "start"), i32_of(&rng, "end")),
        (Some(1), None)
    );
    assert_eq!(
        text_of(&got, "name").as_deref(),
        Some("67e55044-10b1-426f-9247-bb680e5fe0c8")
    );
    #[derive(Serialize)]
    struct WrappedDate {
        quantity: i64,
        order_date: BigQueryDate,
        #[serde(with = "crate::serialize_as_timestamp")]
        placed_at: jiff::Timestamp,
    }
    let timestamp: jiff::Timestamp = "2024-02-29T12:34:56.789012345Z".parse().expect("valid");
    let got = decoded(
        &plan(),
        &WrappedDate {
            quantity: 0,
            order_date: BigQueryDate(date("2024-02-29")),
            placed_at: timestamp,
        },
    );
    assert_eq!(i32_of(&got, "order_date"), Some(19782));
    assert_eq!(
        i64_of(&got, "placed_at"),
        Some(civil::parse_timestamp("2024-02-29T12:34:56.789012Z").expect("valid"))
    );
}

#[test]
fn cdc_pseudo_columns_follow_the_row() {
    let plan = Arc::new(WritePlan::new(&schema(), true));
    let names: Vec<&str> = plan
        .descriptor()
        .field
        .iter()
        .map(|field| field.name())
        .collect();
    assert_eq!(
        &names[names.len() - 2..],
        [CHANGE_TYPE_COLUMN, CHANGE_SEQUENCE_NUMBER_COLUMN]
    );
    let descriptor = message_descriptor(&plan);
    #[derive(Serialize)]
    struct Customer {
        quantity: i64,
        name: &'static str,
    }
    let mut encoder = Encoder::new(plan.clone());
    let sequence: BigQueryChangeSequenceNumber = "1F/A".parse().expect("valid");
    let mut out = Vec::new();
    encoder
        .encode_change(
            &Customer {
                quantity: 4,
                name: "Ada",
            },
            BigQueryChangeType::Upsert,
            Some(&sequence),
            &mut out,
        )
        .expect("encodes");
    let got = DynamicMessage::decode(descriptor.clone(), out.as_slice()).expect("decodes");
    assert_eq!(
        (
            i64_of(&got, "quantity"),
            text_of(&got, "name").as_deref(),
            text_of(&got, CHANGE_TYPE_COLUMN).as_deref(),
            text_of(&got, CHANGE_SEQUENCE_NUMBER_COLUMN).as_deref()
        ),
        (Some(4), Some("Ada"), Some("UPSERT"), Some("1F/A"))
    );
    let mut out = Vec::new();
    encoder
        .encode_change(
            &Customer {
                quantity: 5,
                name: "Grace",
            },
            BigQueryChangeType::Delete,
            None,
            &mut out,
        )
        .expect("encodes");
    let got = DynamicMessage::decode(descriptor, out.as_slice()).expect("decodes");
    assert_eq!(
        (
            text_of(&got, CHANGE_TYPE_COLUMN).as_deref(),
            get(&got, CHANGE_SEQUENCE_NUMBER_COLUMN)
        ),
        (Some("DELETE"), None)
    );
    #[derive(Serialize)]
    struct Spoof {
        quantity: i64,
        #[serde(rename = "_CHANGE_TYPE")]
        change: &'static str,
    }
    let error = match encoder
        .encode_change(
            &Spoof {
                quantity: 1,
                change: "DELETE",
            },
            BigQueryChangeType::Upsert,
            None,
            &mut Vec::new(),
        )
        .expect_err("a row cannot write the pseudo-columns itself")
        .into_serialize()
    {
        BigQueryError::SerializeError(details) => details,
        other => panic!("expected a serialize error, got {other:?}"),
    };
    assert_eq!(
        (error.kind, error.path.as_str()),
        (BigQueryCodecErrorKind::UnknownField, CHANGE_TYPE_COLUMN)
    );
}

#[test]
fn json_column_prints_any_shape_but_a_string_as_json() {
    use BigQueryFieldMode::*;
    let plan = Arc::new(WritePlan::new(
        &BigQueryTableSchema {
            fields: vec![
                nullable("attributes", BigQueryFieldType::Json),
                field("attribute_list", BigQueryFieldType::Json, Repeated),
            ],
        },
        false,
    ));
    #[derive(Serialize)]
    struct Doc {
        count: i64,
        tags: Vec<&'static str>,
    }
    #[derive(Serialize)]
    struct JsonColumns<Attributes: Serialize, AttributeList: Serialize> {
        attributes: Attributes,
        attribute_list: AttributeList,
    }
    let json_columns = |message: DynamicMessage| {
        (
            text_of(&message, "attributes"),
            list_of(&message, "attribute_list"),
        )
    };
    let text = |text: &str| Value::String(text.into());

    let doc = Doc {
        count: 1,
        tags: vec!["gift"],
    };
    assert_eq!(
        json_columns(decoded(
            &plan,
            &JsonColumns {
                attributes: &doc,
                attribute_list: [&doc]
            }
        )),
        (
            Some(r#"{"count":1,"tags":["gift"]}"#.into()),
            vec![text(r#"{"count":1,"tags":["gift"]}"#)]
        )
    );
    let value = serde_json::json!({"unit": [1, null]});
    assert_eq!(
        json_columns(decoded(
            &plan,
            &JsonColumns {
                attributes: &value,
                attribute_list: vec![serde_json::json!(true), serde_json::json!(2)]
            }
        )),
        (
            Some(r#"{"unit":[1,null]}"#.into()),
            vec![text("true"), text("2")]
        )
    );
    let map = std::collections::BTreeMap::from([("discount", 1.5)]);
    assert_eq!(
        json_columns(decoded(
            &plan,
            &JsonColumns {
                attributes: &map,
                attribute_list: [vec![1, 2]]
            }
        )),
        (Some(r#"{"discount":1.5}"#.into()), vec![text("[1,2]")])
    );
    assert_eq!(
        json_columns(decoded(
            &plan,
            &JsonColumns {
                attributes: r#"{"raw": true}"#,
                attribute_list: ["[ 1 ]"]
            }
        )),
        (Some(r#"{"raw": true}"#.into()), vec![text("[ 1 ]")]),
        "a string is the JSON text as it is"
    );
    assert_eq!(
        json_columns(decoded(
            &plan,
            &JsonColumns {
                attributes: Some(serde_json::Value::Null),
                attribute_list: [(); 0]
            }
        )),
        (Some("null".into()), vec![]),
        "JSON null is text, apart from SQL NULL"
    );
    assert_eq!(
        json_columns(decoded(
            &plan,
            &JsonColumns {
                attributes: None::<serde_json::Value>,
                attribute_list: [(); 0]
            }
        )),
        (None, vec![])
    );
    assert_eq!(
        json_columns(decoded(
            &plan,
            &JsonColumns {
                attributes: BigQueryJson(&doc),
                attribute_list: [BigQueryJson("name")]
            }
        )),
        (
            Some(r#"{"count":1,"tags":["gift"]}"#.into()),
            vec![text(r#""name""#)]
        )
    );
}
