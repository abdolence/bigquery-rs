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
