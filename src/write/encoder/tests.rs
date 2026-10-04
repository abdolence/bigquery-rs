use super::*;
use crate::errors::{BigQueryError, BigQuerySerializationError};
use crate::types::civil;
use crate::types::decimal;
use crate::types::temporal::TAG_DATE;
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

pub(crate) fn field(
    name: &str,
    field_type: BigQueryFieldType,
    mode: BigQueryFieldMode,
) -> BigQueryFieldSchema {
    BigQueryFieldSchema {
        name: name.into(),
        field_type,
        mode,
        description: None,
        default_value_expression: None,
    }
}

fn nullable(name: &str, field_type: BigQueryFieldType) -> BigQueryFieldSchema {
    field(name, field_type, BigQueryFieldMode::Nullable)
}

const STRING: BigQueryFieldType = BigQueryFieldType::String { max_length: None };
const BYTES: BigQueryFieldType = BigQueryFieldType::Bytes { max_length: None };

fn schema() -> BigQueryTableSchema {
    use BigQueryFieldMode::*;
    use BigQueryFieldType as T;
    BigQueryTableSchema {
        fields: vec![
            field("i", T::Int64, Required),
            nullable("f", T::Float64),
            nullable("b", T::Bool),
            nullable("s", STRING),
            nullable("y", BYTES),
            nullable("d", T::Date),
            nullable("t", T::Time),
            nullable("dt", T::DateTime),
            nullable("ts", T::Timestamp),
            nullable("num", T::Numeric(None)),
            nullable("big", T::BigNumeric(None)),
            nullable("geo", T::Geography),
            nullable("js", T::Json),
            nullable("iv", T::Interval),
            nullable("rng", T::Range(BigQueryRangeElementType::Date)),
            nullable(
                "rec",
                T::Struct(vec![
                    nullable("a", T::Int64),
                    field("tags", STRING, Repeated),
                    nullable("inner", T::Struct(vec![nullable("x", T::Date)])),
                ]),
            ),
            field("arr", T::Int64, Repeated),
            field(
                "recs",
                T::Struct(vec![nullable("k", STRING), nullable("v", T::Float64)]),
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

fn enc<T: Serialize + ?Sized>(plan: &Arc<WritePlan>, row: &T) -> Result<Vec<u8>, CodecError> {
    let mut out = Vec::new();
    Encoder::new(plan.clone()).encode(row, &mut out)?;
    Ok(out)
}

fn decoded<T: Serialize + ?Sized>(plan: &Arc<WritePlan>, row: &T) -> DynamicMessage {
    let bytes = enc(plan, row).expect("the row encodes");
    DynamicMessage::decode(message_descriptor(plan), bytes.as_slice()).expect("the bytes decode")
}

fn err<T: Serialize + ?Sized>(plan: &Arc<WritePlan>, row: &T) -> BigQuerySerializationError {
    match enc(plan, row)
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
        .then(|| message.get_field_by_name(name).map(|v| v.into_owned()))
        .flatten()
}

fn i64_of(message: &DynamicMessage, name: &str) -> Option<i64> {
    get(message, name).and_then(|v| v.as_i64())
}

fn i32_of(message: &DynamicMessage, name: &str) -> Option<i32> {
    get(message, name).and_then(|v| v.as_i32())
}

fn str_of(message: &DynamicMessage, name: &str) -> Option<String> {
    get(message, name).and_then(|v| v.as_str().map(str::to_string))
}

fn bytes_of(message: &DynamicMessage, name: &str) -> Option<Vec<u8>> {
    get(message, name).and_then(|v| v.as_bytes().map(|b| b.to_vec()))
}

fn msg_of(message: &DynamicMessage, name: &str) -> Option<DynamicMessage> {
    get(message, name).and_then(|v| v.as_message().cloned())
}

fn list_of(message: &DynamicMessage, name: &str) -> Vec<Value> {
    get(message, name)
        .and_then(|v| v.as_list().map(<[Value]>::to_vec))
        .unwrap_or_default()
}

fn le(v: i256) -> Vec<u8> {
    let (bytes, n) = decimal::decimal_le_bytes(v);
    bytes[..n].to_vec()
}

#[derive(Serialize, Clone)]
struct Inner {
    x: Option<jiff::civil::Date>,
}

#[derive(Serialize, Clone)]
struct Rec {
    a: Option<i64>,
    tags: Vec<String>,
    inner: Option<Inner>,
}

#[derive(Serialize, Clone)]
struct Kv {
    k: Option<String>,
    v: Option<f64>,
}

#[derive(Serialize, Clone)]
struct Row {
    i: i64,
    f: Option<f64>,
    b: Option<bool>,
    s: Option<String>,
    #[serde(with = "serde_bytes")]
    y: Vec<u8>,
    d: Option<jiff::civil::Date>,
    t: Option<jiff::civil::Time>,
    dt: Option<jiff::civil::DateTime>,
    ts: Option<jiff::Timestamp>,
    num: Option<String>,
    big: Option<String>,
    geo: Option<String>,
    js: Option<BigQueryJson<serde_json::Value>>,
    iv: Option<BigQueryInterval>,
    rng: Option<BigQueryRange<jiff::civil::Date>>,
    rec: Option<Rec>,
    arr: Vec<i64>,
    recs: Vec<Kv>,
}

fn date(s: &str) -> jiff::civil::Date {
    s.parse().expect("a valid date")
}

fn full_row() -> Row {
    Row {
        i: i64::MIN,
        f: Some(f64::NAN),
        b: Some(true),
        s: Some("héllo 世界 🦀".into()),
        y: vec![0, 255],
        d: Some(date("0001-01-01")),
        t: Some("23:59:59.999999".parse().expect("a valid time")),
        dt: Some(
            "2024-02-29T12:34:56.789012"
                .parse()
                .expect("a valid datetime"),
        ),
        ts: Some("1969-07-20T20:17:40.5Z".parse().expect("a valid timestamp")),
        num: Some("-99999999999999999999999999999.999999999".into()),
        big: Some(
            "578960446186580977117854925043439539266.34992332820282019728792003956564819967".into(),
        ),
        geo: Some("POINT(1 2)".into()),
        js: Some(BigQueryJson(serde_json::json!({"a": 1}))),
        iv: Some(BigQueryInterval {
            months: -14,
            days: 3,
            nanos: 14_706_000_789_000,
        }),
        rng: Some(BigQueryRange {
            start: None,
            end: Some(date("2024-01-01")),
        }),
        rec: Some(Rec {
            a: Some(7),
            tags: vec!["x".into(), "y".into()],
            inner: Some(Inner {
                x: Some(date("2024-02-29")),
            }),
        }),
        arr: vec![1, -2, 3],
        recs: vec![
            Kv {
                k: Some("a".into()),
                v: Some(1.0),
            },
            Kv { k: None, v: None },
        ],
    }
}

#[test]
fn descriptor_follows_the_table_schema() {
    let plan = plan();
    let d = plan.descriptor();
    assert_eq!(d.name(), "row");
    let by = |n: &str| {
        d.field
            .iter()
            .find(|x| x.name() == n)
            .cloned()
            .unwrap_or_else(|| panic!("no field {n}"))
    };
    assert_eq!(
        (by("i").number(), by("i").label(), by("i").r#type()),
        (1, Label::Required, ProtoType::Int64)
    );
    assert_eq!(
        [by("d"), by("t"), by("dt"), by("ts")].map(|f| f.r#type()),
        [
            ProtoType::Int32,
            ProtoType::Int64,
            ProtoType::Int64,
            ProtoType::Int64
        ]
    );
    assert_eq!(
        [by("num"), by("big"), by("iv"), by("js"), by("geo"), by("y")].map(|f| f.r#type()),
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
        (by("f").r#type(), by("b").r#type(), by("f").label()),
        (ProtoType::Double, ProtoType::Bool, Label::Optional)
    );
    assert_eq!(
        (by("arr").label(), by("recs").label(), by("recs").number()),
        (Label::Repeated, Label::Repeated, 18)
    );
    let nested = |type_name: &str| {
        d.nested_type
            .iter()
            .find(|n| n.name() == type_name)
            .cloned()
            .unwrap_or_else(|| panic!("no nested type {type_name}"))
    };
    let names = |m: &gcloud_sdk::prost_types::DescriptorProto| {
        m.field
            .iter()
            .map(|f| (f.name().to_string(), f.r#type()))
            .collect::<Vec<_>>()
    };
    let rec = nested(by("rec").type_name());
    assert_eq!(
        names(&rec),
        [
            ("a".to_string(), ProtoType::Int64),
            ("tags".to_string(), ProtoType::String),
            ("inner".to_string(), ProtoType::Message)
        ]
    );
    let inner_type = rec
        .field
        .iter()
        .find(|f| f.name() == "inner")
        .map(|f| f.type_name().to_string())
        .unwrap_or_default();
    assert_eq!(
        names(&nested(&inner_type)),
        [("x".to_string(), ProtoType::Int32)]
    );
    assert_eq!(
        names(&nested(by("rng").type_name())),
        [
            ("start".to_string(), ProtoType::Int32),
            ("end".to_string(), ProtoType::Int32)
        ]
    );
    // Every nested type sits flat in the root, which prost-reflect resolves.
    assert!(d.nested_type.iter().all(|n| n.nested_type.is_empty()));
    message_descriptor(&plan);
}

#[test]
fn every_type_encodes_to_its_probe_confirmed_wire_form() {
    let plan = plan();
    let got = decoded(&plan, &full_row());
    assert_eq!(i64_of(&got, "i"), Some(i64::MIN));
    assert!(get(&got, "f")
        .and_then(|v| v.as_f64())
        .is_some_and(f64::is_nan));
    assert_eq!(get(&got, "b").and_then(|v| v.as_bool()), Some(true));
    assert_eq!(str_of(&got, "s").as_deref(), Some("héllo 世界 🦀"));
    assert_eq!(bytes_of(&got, "y"), Some(vec![0, 255]));
    assert_eq!(i32_of(&got, "d"), Some(civil::DATE_MIN_DAYS));
    assert_eq!(i64_of(&got, "t"), Some(civil::pack_time(86_399_999_999)));
    let dt = civil::parse_datetime("2024-02-29T12:34:56.789012").expect("valid");
    assert_eq!(i64_of(&got, "dt"), Some(civil::pack_datetime(dt)));
    assert_eq!(
        i64_of(&got, "ts"),
        Some(civil::parse_timestamp("1969-07-20T20:17:40.5Z").expect("valid"))
    );
    assert_eq!(
        bytes_of(&got, "num"),
        Some(le(decimal::parse_numeric(
            "-99999999999999999999999999999.999999999"
        )
        .expect("valid")))
    );
    assert_eq!(bytes_of(&got, "big"), Some(le(i256::MAX)));
    assert_eq!(str_of(&got, "geo").as_deref(), Some("POINT(1 2)"));
    assert_eq!(str_of(&got, "js").as_deref(), Some(r#"{"a":1}"#));
    assert_eq!(str_of(&got, "iv").as_deref(), Some("-1-2 3 4:5:6.000789"));
    let rng = msg_of(&got, "rng").expect("rng is set");
    assert_eq!(
        (i32_of(&rng, "start"), i32_of(&rng, "end")),
        (None, Some(19723))
    );
    let rec = msg_of(&got, "rec").expect("rec is set");
    assert_eq!(i64_of(&rec, "a"), Some(7));
    assert_eq!(
        list_of(&rec, "tags"),
        [Value::String("x".into()), Value::String("y".into())]
    );
    let inner = msg_of(&rec, "inner").expect("inner is set");
    assert_eq!(i32_of(&inner, "x"), Some(19782));
    assert_eq!(
        list_of(&got, "arr"),
        [Value::I64(1), Value::I64(-2), Value::I64(3)]
    );
    let recs = list_of(&got, "recs");
    assert_eq!(recs.len(), 2);
    let first = recs[0].as_message().expect("a message");
    assert_eq!(
        (
            str_of(first, "k").as_deref(),
            get(first, "v").and_then(|v| v.as_f64())
        ),
        (Some("a"), Some(1.0))
    );
    let second = recs[1].as_message().expect("a message");
    assert_eq!((get(second, "k"), get(second, "v")), (None, None));
}

#[test]
fn repeated_scalars_are_packed() {
    #[derive(Serialize)]
    struct A {
        i: i64,
        arr: Vec<i64>,
    }
    let bytes = enc(
        &plan(),
        &A {
            i: 1,
            arr: vec![1, 2, 3],
        },
    )
    .expect("encodes");
    // key(1, varint) 1, then key(17, length-delimited) = 0x8a 0x01, length 3, 1 2 3.
    assert_eq!(bytes, [0x08, 0x01, 0x8a, 0x01, 0x03, 0x01, 0x02, 0x03]);
}

#[test]
fn none_is_absent_and_a_required_none_is_an_error_naming_the_field() {
    #[derive(Serialize)]
    struct N {
        i: Option<i64>,
        s: Option<String>,
        rec: Option<Rec>,
    }
    let plan = plan();
    let got = decoded(
        &plan,
        &N {
            i: Some(1),
            s: None,
            rec: None,
        },
    );
    assert_eq!(
        (i64_of(&got, "i"), get(&got, "s"), get(&got, "rec")),
        (Some(1), None, None)
    );
    let e = err(
        &plan,
        &N {
            i: None,
            s: None,
            rec: None,
        },
    );
    assert_eq!(
        (e.kind, e.path.as_str()),
        (BigQueryCodecErrorKind::NullForRequired, "i")
    );
    #[derive(Serialize)]
    struct Missing {
        s: String,
    }
    let e = err(&plan, &Missing { s: "x".into() });
    assert_eq!(
        (e.kind, e.path.as_str()),
        (BigQueryCodecErrorKind::MissingRequiredField, "i")
    );
}

#[test]
fn errors_name_the_field_path() {
    let plan = plan();
    #[derive(Serialize)]
    struct BadTag {
        i: i64,
        rec: BadRec,
    }
    #[derive(Serialize)]
    struct BadRec {
        tags: (String, i64),
    }
    let e = err(
        &plan,
        &BadTag {
            i: 1,
            rec: BadRec {
                tags: ("a".into(), 5),
            },
        },
    );
    assert_eq!(
        (e.kind, e.path.as_str()),
        (BigQueryCodecErrorKind::TypeMismatch, "rec.tags[1]")
    );
    #[derive(Serialize)]
    struct Unknown {
        i: i64,
        rec: UnknownInner,
    }
    #[derive(Serialize)]
    struct UnknownInner {
        inner: UnknownLeaf,
    }
    #[derive(Serialize)]
    struct UnknownLeaf {
        z: i64,
    }
    let e = err(
        &plan,
        &Unknown {
            i: 1,
            rec: UnknownInner {
                inner: UnknownLeaf { z: 2 },
            },
        },
    );
    assert_eq!(
        (e.kind, e.path.as_str()),
        (BigQueryCodecErrorKind::UnknownField, "rec.inner.z")
    );
    #[derive(Serialize)]
    struct BadKv {
        v: &'static str,
    }
    #[derive(Serialize)]
    struct BadRecs {
        i: i64,
        recs: Vec<BadKv>,
    }
    let e = err(
        &plan,
        &BadRecs {
            i: 1,
            recs: vec![BadKv { v: "x" }],
        },
    );
    assert_eq!(e.path, "recs[0].v");
    #[derive(Serialize)]
    struct BadDate {
        i: i64,
        d: &'static str,
    }
    let e = err(
        &plan,
        &BadDate {
            i: 1,
            d: "2023-02-29",
        },
    );
    assert_eq!(
        (e.kind, e.path.as_str()),
        (BigQueryCodecErrorKind::InvalidText, "d")
    );
}

#[test]
fn integer_forms_of_temporal_types_match_the_read_side() {
    #[derive(Serialize)]
    struct Ints {
        i: i64,
        d: i32,
        t: i64,
        dt: i64,
        ts: i64,
    }
    let dt = civil::parse_datetime("2024-02-29T12:34:56.789012").expect("valid");
    let got = decoded(
        &plan(),
        &Ints {
            i: 0,
            d: 19782,
            t: 45_296_000_000,
            dt,
            ts: civil::TIMESTAMP_MAX_MICROS,
        },
    );
    assert_eq!(i32_of(&got, "d"), Some(19782));
    assert_eq!(i64_of(&got, "t"), Some(civil::pack_time(45_296_000_000)));
    assert_eq!(i64_of(&got, "dt"), Some(civil::pack_datetime(dt)));
    assert_eq!(i64_of(&got, "ts"), Some(civil::TIMESTAMP_MAX_MICROS));
    #[derive(Serialize)]
    struct OutOfDay {
        i: i64,
        t: i64,
    }
    let e = err(
        &plan(),
        &OutOfDay {
            i: 0,
            t: civil::MICROS_PER_DAY,
        },
    );
    assert_eq!(
        (e.kind, e.path.as_str()),
        (BigQueryCodecErrorKind::OutOfRange, "t")
    );
    #[derive(Serialize)]
    struct PastMax {
        i: i64,
        ts: i64,
    }
    let e = err(
        &plan(),
        &PastMax {
            i: 0,
            ts: civil::TIMESTAMP_MAX_MICROS + 1,
        },
    );
    assert_eq!(
        (e.kind, e.path.as_str()),
        (BigQueryCodecErrorKind::OutOfRange, "ts")
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
    struct W {
        i: u8,
        num: BigQueryDecimal<Cents>,
        big: f64,
        ts: &'static str,
        dt: &'static str,
        iv: &'static str,
        js: &'static str,
        s: Color,
        y: [u8; 2],
        arr: std::collections::VecDeque<i32>,
        rec: std::collections::BTreeMap<String, i64>,
    }
    let row = W {
        i: 3,
        num: BigQueryDecimal(Cents(12345)),
        big: 0.5,
        ts: "9999-12-31 23:59:59.999999+00:00",
        dt: "2024-02-29 12:34:56",
        iv: "1-2 3 4:5:6",
        js: r#"{"k":[1,2]}"#,
        s: Color::Red,
        y: *b"ab",
        arr: [4, 5].into(),
        rec: [("a".to_string(), 9)].into(),
    };
    let got = decoded(&plan(), &row);
    assert_eq!(i64_of(&got, "i"), Some(3));
    assert_eq!(
        bytes_of(&got, "num"),
        Some(le(i256::from_i128(123_450_000_000)))
    );
    assert_eq!(
        bytes_of(&got, "big"),
        Some(le(i256::from_i128(5 * 10i128.pow(37))))
    );
    assert_eq!(i64_of(&got, "ts"), Some(civil::TIMESTAMP_MAX_MICROS));
    assert_eq!(
        i64_of(&got, "dt"),
        Some(civil::pack_datetime(
            civil::parse_datetime("2024-02-29T12:34:56").expect("valid")
        ))
    );
    assert_eq!(str_of(&got, "iv").as_deref(), Some("1-2 3 4:5:6"));
    assert_eq!(str_of(&got, "js").as_deref(), Some(r#"{"k":[1,2]}"#));
    assert_eq!(str_of(&got, "s").as_deref(), Some("red"));
    assert_eq!(bytes_of(&got, "y"), Some(b"ab".to_vec()));
    assert_eq!(list_of(&got, "arr"), [Value::I64(4), Value::I64(5)]);
    let rec = msg_of(&got, "rec").expect("rec is set");
    assert_eq!(i64_of(&rec, "a"), Some(9));
    #[derive(Serialize)]
    struct WholeNumeric {
        i: i64,
        num: i64,
        big: i64,
    }
    let got = decoded(
        &plan(),
        &WholeNumeric {
            i: 0,
            num: -7,
            big: 2,
        },
    );
    assert_eq!(
        bytes_of(&got, "num"),
        Some(le(i256::from_i128(-7_000_000_000)))
    );
    assert_eq!(
        bytes_of(&got, "big"),
        Some(le(
            i256::from_i128(2 * 10i128.pow(19)).wrapping_mul(i256::from_i128(10i128.pow(19)))
        ))
    );
}

#[test]
fn skipped_reordered_and_flattened_fields() {
    #[derive(Serialize)]
    struct Sparse {
        #[serde(skip_serializing_if = "Option::is_none")]
        s: Option<String>,
        i: i64,
    }
    #[derive(Serialize)]
    struct Flat {
        i: i64,
        #[serde(flatten)]
        rest: FlatRest,
    }
    #[derive(Serialize)]
    struct FlatRest {
        s: String,
        b: bool,
    }
    let plan = plan();
    let descriptor = message_descriptor(&plan);
    let mut e = Encoder::new(plan.clone());
    let rows = [
        Sparse {
            s: Some("a".into()),
            i: 1,
        },
        Sparse { s: None, i: 2 },
        Sparse {
            s: Some("c".into()),
            i: 3,
        },
    ];
    for (k, row) in rows.iter().enumerate() {
        let mut out = Vec::new();
        e.encode(row, &mut out).expect("encodes");
        let got = DynamicMessage::decode(descriptor.clone(), out.as_slice()).expect("decodes");
        assert_eq!(
            (i64_of(&got, "i"), str_of(&got, "s").is_some()),
            (Some(k as i64 + 1), k != 1)
        );
    }
    let mut out = Vec::new();
    e.encode(
        &Flat {
            i: 9,
            rest: FlatRest {
                s: "z".into(),
                b: false,
            },
        },
        &mut out,
    )
    .expect("encodes");
    let got = DynamicMessage::decode(descriptor, out.as_slice()).expect("decodes");
    assert_eq!(
        (
            i64_of(&got, "i"),
            str_of(&got, "s").as_deref(),
            get(&got, "b").and_then(|v| v.as_bool())
        ),
        (Some(9), Some("z"), Some(false))
    );
}

#[test]
fn a_null_array_element_is_an_error() {
    #[derive(Serialize)]
    struct A {
        i: i64,
        arr: Vec<Option<i64>>,
    }
    let e = err(
        &plan(),
        &A {
            i: 1,
            arr: vec![Some(1), None],
        },
    );
    assert_eq!(
        (e.kind, e.path.as_str()),
        (BigQueryCodecErrorKind::NullArrayElement, "arr[1]")
    );
}

#[test]
fn nested_lengths_over_127_bytes_are_backpatched() {
    let plan = plan();
    let mut long = full_row();
    if let Some(rec) = long.rec.as_mut() {
        rec.tags = vec!["x".repeat(200), "y".repeat(20_000)];
    }
    long.recs = (0..300)
        .map(|i| Kv {
            k: Some(format!("k{i}")),
            v: Some(f64::from(i)),
        })
        .collect();
    long.arr = (0..100).map(|i| i * 1_000_000_007).collect();
    let got = decoded(&plan, &long);
    let rec = msg_of(&got, "rec").expect("rec is set");
    let tags = list_of(&rec, "tags");
    assert_eq!(tags[1].as_str().map(str::len), Some(20_000));
    assert_eq!(
        msg_of(&rec, "inner").and_then(|m| i32_of(&m, "x")),
        Some(19782)
    );
    let recs = list_of(&got, "recs");
    assert_eq!(recs.len(), 300);
    assert_eq!(
        recs[299]
            .as_message()
            .and_then(|m| str_of(m, "k"))
            .as_deref(),
        Some("k299")
    );
    assert_eq!(list_of(&got, "arr").len(), 100);
    assert_eq!(str_of(&got, "geo").as_deref(), Some("POINT(1 2)"));
}

#[test]
fn float64_rejects_integers() {
    #[derive(Serialize)]
    struct A {
        i: i64,
        f: i64,
    }
    let e = err(&plan(), &A { i: 1, f: 2 });
    assert_eq!(
        (e.kind, e.path.as_str()),
        (BigQueryCodecErrorKind::TypeMismatch, "f")
    );
    #[derive(Serialize)]
    struct B {
        i: i64,
        f: f32,
    }
    let got = decoded(&plan(), &B { i: 1, f: 0.5 });
    assert_eq!(get(&got, "f").and_then(|v| v.as_f64()), Some(0.5));
}

#[test]
fn bytes_rejects_strings() {
    #[derive(Serialize)]
    struct A {
        i: i64,
        y: &'static str,
    }
    let e = err(&plan(), &A { i: 1, y: "AQI=" });
    assert_eq!(
        (e.kind, e.path.as_str()),
        (BigQueryCodecErrorKind::TypeMismatch, "y")
    );
}

#[test]
fn string_rejects_bytes() {
    #[derive(Serialize)]
    struct A {
        i: i64,
        #[serde(with = "serde_bytes")]
        s: Vec<u8>,
    }
    let e = err(
        &plan(),
        &A {
            i: 1,
            s: b"ok".to_vec(),
        },
    );
    assert_eq!(
        (e.kind, e.path.as_str()),
        (BigQueryCodecErrorKind::TypeMismatch, "s")
    );
    #[derive(Serialize)]
    struct B {
        i: i64,
        s: Vec<u8>,
    }
    let e = err(
        &plan(),
        &B {
            i: 1,
            s: b"ok".to_vec(),
        },
    );
    assert_eq!(
        (e.kind, e.path.as_str()),
        (BigQueryCodecErrorKind::TypeMismatch, "s")
    );
}

#[test]
fn date_before_year_one_is_out_of_range() {
    #[derive(Serialize)]
    struct Jiff {
        i: i64,
        d: jiff::civil::Date,
    }
    #[derive(Serialize)]
    struct Wrapped {
        i: i64,
        d: BigQueryDate,
    }
    #[derive(Serialize)]
    struct Days {
        i: i64,
        d: i32,
    }
    #[derive(Serialize)]
    struct Text {
        i: i64,
        d: &'static str,
    }
    #[derive(Serialize)]
    struct Local {
        i: i64,
        dt: jiff::civil::DateTime,
    }
    let plan = plan();
    for year in [0, -1, -9999] {
        let d = jiff::civil::date(year, 12, 31);
        let e = err(&plan, &Jiff { i: 0, d });
        assert_eq!(
            (e.kind, e.path.as_str()),
            (BigQueryCodecErrorKind::OutOfRange, "d"),
            "{d}"
        );
        let e = err(
            &plan,
            &Wrapped {
                i: 0,
                d: BigQueryDate(d),
            },
        );
        assert_eq!(
            (e.kind, e.path.as_str()),
            (BigQueryCodecErrorKind::OutOfRange, "d"),
            "{d}"
        );
        let e = err(
            &plan,
            &Local {
                i: 0,
                dt: d.at(1, 2, 3, 0),
            },
        );
        assert_eq!(
            (e.kind, e.path.as_str()),
            (BigQueryCodecErrorKind::OutOfRange, "dt"),
            "{d}"
        );
    }
    let e = err(
        &plan,
        &Days {
            i: 0,
            d: civil::DATE_MIN_DAYS - 1,
        },
    );
    assert_eq!(
        (e.kind, e.path.as_str()),
        (BigQueryCodecErrorKind::OutOfRange, "d")
    );
    let e = err(
        &plan,
        &Text {
            i: 0,
            d: "0000-12-31",
        },
    );
    assert_eq!(
        (e.kind, e.path.as_str()),
        (BigQueryCodecErrorKind::OutOfRange, "d")
    );
    let got = decoded(
        &plan,
        &Jiff {
            i: 0,
            d: date("0001-01-01"),
        },
    );
    assert_eq!(i32_of(&got, "d"), Some(civil::DATE_MIN_DAYS));
}

#[test]
fn interval_beyond_storage_read_range_is_refused() {
    #[derive(Serialize)]
    struct Text {
        i: i64,
        iv: &'static str,
    }
    #[derive(Serialize)]
    struct Parts {
        i: i64,
        iv: BigQueryInterval,
    }
    let plan = plan();
    let e = err(
        &plan,
        &Text {
            i: 0,
            iv: "0-0 0 2562048:0:0",
        },
    );
    assert_eq!(
        (e.kind, e.path.as_str()),
        (BigQueryCodecErrorKind::OutOfRange, "iv")
    );
    let e = err(
        &plan,
        &Parts {
            i: 0,
            iv: BigQueryInterval {
                months: 0,
                days: 0,
                nanos: 1_500,
            },
        },
    );
    assert_eq!(
        (e.kind, e.path.as_str()),
        (BigQueryCodecErrorKind::OutOfRange, "iv")
    );
    let got = decoded(
        &plan,
        &Text {
            i: 0,
            iv: "0-0 0 2562047:0:0",
        },
    );
    assert_eq!(str_of(&got, "iv").as_deref(), Some("0-0 0 2562047:0:0"));
}

#[test]
fn temporal_wrapper_on_another_temporal_column_is_type_mismatch() {
    #[derive(Serialize)]
    struct DateOnTimestamp {
        i: i64,
        ts: BigQueryDate,
    }
    #[derive(Serialize)]
    struct TimestampOnDate {
        i: i64,
        d: BigQueryTimestamp,
    }
    #[derive(Serialize)]
    struct DateOnInt {
        i: BigQueryDate,
    }
    let plan = plan();
    let e = err(
        &plan,
        &DateOnTimestamp {
            i: 0,
            ts: BigQueryDate(date("2024-01-01")),
        },
    );
    assert_eq!(
        (e.kind, e.path.as_str()),
        (BigQueryCodecErrorKind::TypeMismatch, "ts")
    );
    let e = err(
        &plan,
        &TimestampOnDate {
            i: 0,
            d: BigQueryTimestamp(jiff::Timestamp::UNIX_EPOCH),
        },
    );
    assert_eq!(
        (e.kind, e.path.as_str()),
        (BigQueryCodecErrorKind::TypeMismatch, "d")
    );
    let e = err(
        &plan,
        &DateOnInt {
            i: BigQueryDate(date("2024-01-01")),
        },
    );
    assert_eq!(
        (e.kind, e.path.as_str()),
        (BigQueryCodecErrorKind::TypeMismatch, "i")
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
    struct A {
        i: i64,
        d: NoTextDate,
        rng: BigQueryRange<NoTextDate>,
        s: UuidLike,
    }
    let got = decoded(
        &plan(),
        &A {
            i: 0,
            d: NoTextDate(19782),
            rng: BigQueryRange {
                start: Some(NoTextDate(1)),
                end: None,
            },
            s: UuidLike,
        },
    );
    assert_eq!(i32_of(&got, "d"), Some(19782));
    let rng = msg_of(&got, "rng").expect("rng is set");
    assert_eq!(
        (i32_of(&rng, "start"), i32_of(&rng, "end")),
        (Some(1), None)
    );
    assert_eq!(
        str_of(&got, "s").as_deref(),
        Some("67e55044-10b1-426f-9247-bb680e5fe0c8")
    );
    #[derive(Serialize)]
    struct B {
        i: i64,
        d: BigQueryDate,
        #[serde(with = "crate::serialize_as_timestamp")]
        ts: jiff::Timestamp,
    }
    let ts: jiff::Timestamp = "2024-02-29T12:34:56.789012345Z".parse().expect("valid");
    let got = decoded(
        &plan(),
        &B {
            i: 0,
            d: BigQueryDate(date("2024-02-29")),
            ts,
        },
    );
    assert_eq!(i32_of(&got, "d"), Some(19782));
    assert_eq!(
        i64_of(&got, "ts"),
        Some(civil::parse_timestamp("2024-02-29T12:34:56.789012Z").expect("valid"))
    );
}

#[test]
fn cdc_pseudo_columns_follow_the_row() {
    let plan = Arc::new(WritePlan::new(&schema(), true));
    let names: Vec<&str> = plan.descriptor().field.iter().map(|f| f.name()).collect();
    assert_eq!(
        &names[names.len() - 2..],
        [CHANGE_TYPE_COLUMN, CHANGE_SEQUENCE_NUMBER_COLUMN]
    );
    let descriptor = message_descriptor(&plan);
    #[derive(Serialize)]
    struct A {
        i: i64,
        s: &'static str,
    }
    let mut encoder = Encoder::new(plan.clone());
    let sequence: BigQueryChangeSequenceNumber = "1F/A".parse().expect("valid");
    let mut out = Vec::new();
    encoder
        .encode_change(
            &A { i: 4, s: "x" },
            BigQueryChangeType::Upsert,
            Some(&sequence),
            &mut out,
        )
        .expect("encodes");
    let got = DynamicMessage::decode(descriptor.clone(), out.as_slice()).expect("decodes");
    assert_eq!(
        (
            i64_of(&got, "i"),
            str_of(&got, "s").as_deref(),
            str_of(&got, CHANGE_TYPE_COLUMN).as_deref(),
            str_of(&got, CHANGE_SEQUENCE_NUMBER_COLUMN).as_deref()
        ),
        (Some(4), Some("x"), Some("UPSERT"), Some("1F/A"))
    );
    let mut out = Vec::new();
    encoder
        .encode_change(
            &A { i: 5, s: "y" },
            BigQueryChangeType::Delete,
            None,
            &mut out,
        )
        .expect("encodes");
    let got = DynamicMessage::decode(descriptor, out.as_slice()).expect("decodes");
    assert_eq!(
        (
            str_of(&got, CHANGE_TYPE_COLUMN).as_deref(),
            get(&got, CHANGE_SEQUENCE_NUMBER_COLUMN)
        ),
        (Some("DELETE"), None)
    );
    #[derive(Serialize)]
    struct Spoof {
        i: i64,
        #[serde(rename = "_CHANGE_TYPE")]
        change: &'static str,
    }
    let e = match encoder
        .encode_change(
            &Spoof {
                i: 1,
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
        (e.kind, e.path.as_str()),
        (BigQueryCodecErrorKind::UnknownField, CHANGE_TYPE_COLUMN)
    );
}

#[test]
fn json_column_prints_any_shape_but_a_string_as_json() {
    use BigQueryFieldMode::*;
    let plan = Arc::new(WritePlan::new(
        &BigQueryTableSchema {
            fields: vec![
                nullable("js", BigQueryFieldType::Json),
                field("ajs", BigQueryFieldType::Json, Repeated),
            ],
        },
        false,
    ));
    #[derive(Serialize)]
    struct Doc {
        a: i64,
        b: Vec<&'static str>,
    }
    #[derive(Serialize)]
    struct Row<J: Serialize, A: Serialize> {
        js: J,
        ajs: A,
    }
    let js_of = |m: DynamicMessage| (str_of(&m, "js"), list_of(&m, "ajs"));
    let text = |s: &str| Value::String(s.into());

    let doc = Doc { a: 1, b: vec!["x"] };
    assert_eq!(
        js_of(decoded(
            &plan,
            &Row {
                js: &doc,
                ajs: [&doc]
            }
        )),
        (
            Some(r#"{"a":1,"b":["x"]}"#.into()),
            vec![text(r#"{"a":1,"b":["x"]}"#)]
        )
    );
    let value = serde_json::json!({"k": [1, null]});
    assert_eq!(
        js_of(decoded(
            &plan,
            &Row {
                js: &value,
                ajs: vec![serde_json::json!(true), serde_json::json!(2)]
            }
        )),
        (
            Some(r#"{"k":[1,null]}"#.into()),
            vec![text("true"), text("2")]
        )
    );
    let map = std::collections::BTreeMap::from([("z", 1.5)]);
    assert_eq!(
        js_of(decoded(
            &plan,
            &Row {
                js: &map,
                ajs: [vec![1, 2]]
            }
        )),
        (Some(r#"{"z":1.5}"#.into()), vec![text("[1,2]")])
    );
    assert_eq!(
        js_of(decoded(
            &plan,
            &Row {
                js: r#"{"raw": true}"#,
                ajs: ["[ 1 ]"]
            }
        )),
        (Some(r#"{"raw": true}"#.into()), vec![text("[ 1 ]")]),
        "a string is the JSON text as it is"
    );
    assert_eq!(
        js_of(decoded(
            &plan,
            &Row {
                js: Some(serde_json::Value::Null),
                ajs: [(); 0]
            }
        )),
        (Some("null".into()), vec![]),
        "JSON null is text, apart from SQL NULL"
    );
    assert_eq!(
        js_of(decoded(
            &plan,
            &Row {
                js: None::<serde_json::Value>,
                ajs: [(); 0]
            }
        )),
        (None, vec![])
    );
    assert_eq!(
        js_of(decoded(
            &plan,
            &Row {
                js: BigQueryJson(&doc),
                ajs: [BigQueryJson("s")]
            }
        )),
        (Some(r#"{"a":1,"b":["x"]}"#.into()), vec![text(r#""s""#)])
    );
}
