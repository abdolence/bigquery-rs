use super::*;
use crate::errors::BigQueryInvalidParametersError;
use proptest::prelude::*;

/// The field and the message of an invalid-parameters error.
fn invalid<T: std::fmt::Debug>(result: BigQueryResult<T>) -> (String, String) {
    match result {
        Err(BigQueryError::InvalidParametersError(BigQueryInvalidParametersError { public })) => {
            (public.field, public.error)
        }
        other => panic!("expected an invalid-parameters error, got {other:?}"),
    }
}

/// Each class of ID the crate rejects, for both ID types: the ones that would change how a
/// resource path or SQL text is built from the ID.
#[test]
fn ids_that_could_reshape_a_path_or_sql_are_rejected() {
    let over_limit = "é".repeat(513);
    let cases = [
        ("", "empty"),
        (over_limit.as_str(), "over 1,024 bytes"),
        ("ord\0ers", "NUL"),
        ("ord\ners", "newline"),
        ("ord\rers", "carriage return"),
        ("ord\ters", "tab"),
        ("ord\u{7f}ers", "DEL"),
        ("ord\u{85}ers", "C1 control"),
        ("ord`ers", "backtick"),
        ("ord'ers", "single quote"),
        ("ord\"ers", "double quote"),
        ("ord\\ers", "backslash"),
        ("shop.orders", "part separator"),
        ("shop/orders", "path separator"),
        ("orders$20250101", "partition decorator"),
        ("orders@1700000000000", "snapshot decorator"),
    ];
    for (id, class) in cases {
        assert_eq!(
            invalid(BigQueryDatasetId::new(id)).0,
            "dataset_id",
            "{class}: {id:?}"
        );
        assert_eq!(
            invalid(BigQueryTableId::new(id)).0,
            "table_id",
            "{class}: {id:?}"
        );
    }
}

/// Anything else is BigQuery's to accept or reject, so the crate lets it through.
#[test]
fn other_ids_are_left_to_bigquery() {
    let at_limit = "é".repeat(512);
    for id in [
        "orders",
        "shop-eu",
        "table 01",
        "étudiant-01",
        "00_お客様",
        "ord😀ers",
        "a+b;c",
        at_limit.as_str(),
    ] {
        assert!(BigQueryDatasetId::new(id).is_ok(), "{id:?}");
        assert!(BigQueryTableId::new(id).is_ok(), "{id:?}");
    }
}

#[test]
fn an_invalid_character_error_names_the_character_and_its_offset() {
    let (_, message) = invalid(BigQueryTableId::new("ord`ers"));
    assert!(message.contains("'`' at byte 3"), "{message}");

    let (_, message) = invalid(BigQueryDatasetId::new("sh\nop"));
    assert!(!message.contains('\n'), "{message:?}");
    assert!(message.contains("byte 2"), "{message}");
}

#[test]
fn an_oversized_id_error_reports_its_length_not_the_value() {
    let (_, message) = invalid(BigQueryTableId::new("x".repeat(10_000)));
    assert!(message.contains("10000"), "{message}");
    assert!(!message.contains(&"x".repeat(10)), "{message}");
}

const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
static ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");

#[test]
fn from_static_agrees_with_new() {
    assert_eq!(
        SHOP,
        BigQueryDatasetId::new("shop").expect("valid test input")
    );
    assert_eq!(
        ORDERS,
        BigQueryTableId::new("orders").expect("valid test input")
    );
}

#[test]
#[should_panic(expected = "must not contain")]
fn from_static_panics_at_run_time_on_a_bad_literal() {
    let _ = BigQueryDatasetId::from_static("shop.eu");
}

#[test]
fn deserializing_validates() {
    let shop: BigQueryDatasetId = serde_json::from_str("\"shop\"").expect("valid test input");
    assert_eq!(shop, "shop");
    assert!(serde_json::from_str::<BigQueryDatasetId>("\"shop.eu\"").is_err());
    assert!(serde_json::from_str::<BigQueryTableId>("\"a.b\"").is_err());
    assert_eq!(
        serde_json::to_string(&ORDERS).expect("valid test input"),
        "\"orders\""
    );
}

proptest! {
    /// The byte walk in the const check rejects exactly the characters `char` names.
    #[test]
    fn the_id_check_agrees_with_char(id in any::<String>()) {
        let expected = !id.is_empty()
            && id.len() <= 1024
            && !id.chars().any(|c| c.is_control() || "`'\"\\./$@".contains(c));
        prop_assert_eq!(check_id(&id).is_ok(), expected);
    }
}
