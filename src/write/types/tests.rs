use super::*;
use crate::errors::BigQueryCodecErrorKind;

#[test]
fn a_sequence_number_is_sent_as_written_and_only_empty_is_refused() {
    for text in ["1F", "A/B/C/D", "g", "1/2/3/4/5", "11111111111111111"] {
        let parsed: BigQueryChangeSequenceNumber = text.parse().expect(text);
        assert_eq!(parsed.as_str(), text);
    }
    match "".parse::<BigQueryChangeSequenceNumber>() {
        Err(BigQueryError::SerializeError(err)) => {
            assert_eq!(err.kind, BigQueryCodecErrorKind::InvalidText);
        }
        other => panic!("an empty sequence number must be InvalidText, got {other:?}"),
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
