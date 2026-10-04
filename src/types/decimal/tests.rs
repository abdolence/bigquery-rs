use super::*;

fn s<F: Fn(&mut String)>(f: F) -> String {
    let mut out = String::new();
    f(&mut out);
    out
}

fn kind<T: std::fmt::Debug>(result: Result<T, CodecError>) -> BigQueryCodecErrorKind {
    match result.map_err(CodecError::into_serialize) {
        Err(crate::errors::BigQueryError::SerializeError(err)) => err.kind,
        other => panic!("expected a codec error, got {other:?}"),
    }
}

const I256_MAX_AT_38: &str =
    "578960446186580977117854925043439539266.34992332820282019728792003956564819967";
const I256_MIN_AT_38: &str =
    "-578960446186580977117854925043439539266.34992332820282019728792003956564819968";

#[test]
fn decimals_format_trimmed_and_parse_at_scale() {
    assert_eq!(s(|o| fmt_decimal_i128(123_450_000_000, 9, o)), "123.45");
    assert_eq!(s(|o| fmt_decimal_i128(-1, 9, o)), "-0.000000001");
    assert_eq!(s(|o| fmt_decimal_i128(0, 9, o)), "0");
    assert_eq!(s(|o| fmt_decimal_i128(5_000_000_000, 9, o)), "5");
    assert_eq!(
        s(|o| fmt_decimal_i128(i128::pow(10, 38) - 1, 9, o)),
        "99999999999999999999999999999.999999999"
    );
    assert_eq!(s(|o| fmt_decimal_i256(i256::MAX, 38, o)), I256_MAX_AT_38);
    assert_eq!(s(|o| fmt_decimal_i256(i256::MIN, 38, o)), I256_MIN_AT_38);

    assert_eq!(
        parse_numeric("123.45").ok(),
        Some(i256::from_i128(123_450_000_000))
    );
    assert_eq!(
        parse_numeric("-0.000000001").ok(),
        Some(i256::from_i128(-1))
    );
    assert_eq!(
        parse_numeric("+7").ok(),
        Some(i256::from_i128(7_000_000_000))
    );
    assert_eq!(parse_numeric(".5").ok(), Some(i256::from_i128(500_000_000)));
    assert_eq!(
        kind(parse_numeric("0.0000000001")),
        BigQueryCodecErrorKind::OutOfRange,
        "more digits than the scale"
    );
    assert_eq!(
        kind(parse_numeric("100000000000000000000000000000")),
        BigQueryCodecErrorKind::OutOfRange,
        "30 integer digits"
    );
    assert_eq!(
        parse_numeric("99999999999999999999999999999.999999999").ok(),
        Some(i256::from_i128(i128::pow(10, 38) - 1))
    );
    assert_eq!(
        kind(parse_numeric("1e5")),
        BigQueryCodecErrorKind::InvalidText
    );
    assert_eq!(kind(parse_numeric("")), BigQueryCodecErrorKind::InvalidText);
    assert_eq!(
        kind(parse_numeric("-")),
        BigQueryCodecErrorKind::InvalidText
    );
    assert_eq!(parse_bignumeric(I256_MIN_AT_38).ok(), Some(i256::MIN));
    assert_eq!(parse_bignumeric(I256_MAX_AT_38).ok(), Some(i256::MAX));
    assert_eq!(
        kind(parse_bignumeric(
            "578960446186580977117854925043439539266.34992332820282019728792003956564819968"
        )),
        BigQueryCodecErrorKind::OutOfRange
    );
    assert_eq!(
        parse_decimal("123.45", 9).ok(),
        Some(i256::from_i128(123_450_000_000))
    );

    assert_eq!(
        decimal_from_f64(1.5, 9).ok(),
        Some(i256::from_i128(1_500_000_000))
    );
    assert_eq!(
        decimal_from_f64(0.1234567894, 9).ok(),
        Some(i256::from_i128(123_456_789))
    );
    assert_eq!(
        kind(decimal_from_f64(f64::NAN, 9)),
        BigQueryCodecErrorKind::OutOfRange
    );
    assert_eq!(
        kind(decimal_from_f64(f64::INFINITY, 9)),
        BigQueryCodecErrorKind::OutOfRange
    );

    assert_eq!(
        decimal_to_i64_exact(i256::from_i128(-42_000_000_000), 9).ok(),
        Some(-42)
    );
    assert_eq!(
        kind(decimal_to_i64_exact(i256::from_i128(1_500_000_000), 9)),
        BigQueryCodecErrorKind::OutOfRange,
        "a fractional value into an integer"
    );
    assert_eq!(
        kind(decimal_to_i64_exact(
            i256::from_i128(i128::from(i64::MAX) * 1_000_000_000 + 1_000_000_000),
            9
        )),
        BigQueryCodecErrorKind::OutOfRange
    );
}

#[test]
fn decimal_wire_bytes_are_minimal_twos_complement() {
    let b = |v: i256| {
        let (buf, n) = decimal_le_bytes(v);
        buf[..n].to_vec()
    };
    assert_eq!(b(i256::from_i128(0)), vec![0]);
    assert_eq!(b(i256::from_i128(127)), vec![127]);
    assert_eq!(b(i256::from_i128(128)), vec![128, 0]);
    assert_eq!(b(i256::from_i128(-1)), vec![0xff]);
    assert_eq!(b(i256::from_i128(-129)), vec![0x7f, 0xff]);
    assert_eq!(
        b(i256::from_i128(123_456_789_000)),
        vec![0x08, 0x1a, 0x99, 0xbe, 0x1c]
    );
    assert_eq!(b(i256::MAX).len(), 32);
    for v in [0, 127, 128, -1, -129, 123_456_789_000, i128::MIN, i128::MAX] {
        let v = i256::from_i128(v);
        assert_eq!(decimal_from_le_bytes(&b(v)), v);
    }
    assert_eq!(decimal_from_le_bytes(&b(i256::MIN)), i256::MIN);
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
struct Price {
    #[serde(with = "serialize_as_decimal")]
    amount: i128,
    #[serde(with = "serialize_as_optional_decimal")]
    discount: Option<f64>,
}

#[test]
fn decimal_wrapper_is_its_display_text() {
    let text = serde_json::to_string(&BigQueryDecimal(12345i64)).expect("valid test input");
    assert_eq!(text, r#""12345""#);
    assert_eq!(
        serde_json::from_str::<BigQueryDecimal<i64>>(&text).expect("valid test input"),
        BigQueryDecimal(12345)
    );
    assert!(serde_json::from_str::<BigQueryDecimal<i64>>(r#""1.5""#).is_err());

    let price = Price {
        amount: -7,
        discount: Some(0.5),
    };
    let text = serde_json::to_string(&price).expect("valid test input");
    assert_eq!(text, r#"{"amount":"-7","discount":"0.5"}"#);
    assert_eq!(
        serde_json::from_str::<Price>(&text).expect("valid test input"),
        price
    );
    let none = Price {
        discount: None,
        ..price
    };
    let text = serde_json::to_string(&none).expect("valid test input");
    assert_eq!(text, r#"{"amount":"-7","discount":null}"#);
    assert_eq!(
        serde_json::from_str::<Price>(&text).expect("valid test input"),
        none
    );
}
