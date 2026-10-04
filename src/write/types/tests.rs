use super::*;
use crate::errors::BigQueryCodecErrorKind;

#[test]
fn sequence_number_format_is_checked() {
    for valid in ["0", "1F", "ffffffffffffffff", "A/B/C/D", "0/123abc"] {
        let parsed: BigQueryChangeSequenceNumber = valid.parse().expect(valid);
        assert_eq!(parsed.as_str(), valid);
    }
    for invalid in [
        "",
        "/",
        "1/",
        "/1",
        "1//2",
        "g",
        "0x1",
        "-1",
        "1 2",
        "11111111111111111",
        "1/2/3/4/5",
    ] {
        match invalid.parse::<BigQueryChangeSequenceNumber>() {
            Err(BigQueryError::SerializeError(err)) => {
                assert_eq!(err.kind, BigQueryCodecErrorKind::InvalidText, "{invalid:?}");
                assert!(
                    err.message.contains(invalid),
                    "{invalid:?}: {}",
                    err.message
                );
            }
            other => panic!("{invalid:?} must be InvalidText, got {other:?}"),
        }
    }
    assert_eq!(BigQueryChangeSequenceNumber::from(255).as_str(), "FF");
}

#[test]
fn an_empty_trace_id_is_refused() {
    let err = BigQueryTraceId::new("").expect_err("an empty trace ID");
    assert!(err.to_string().contains("trace_id"), "{err}");
    assert_eq!(
        BigQueryTraceId::new("app:1.0")
            .expect("a trace ID")
            .as_str(),
        "app:1.0"
    );
}
