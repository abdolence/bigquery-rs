use super::*;
use serde_json::{json, Value};

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Doc {
    a: i64,
    b: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Row {
    #[serde(with = "serialize_as_json")]
    doc: Doc,
    #[serde(with = "serialize_as_optional_json")]
    maybe: Option<Value>,
}

#[test]
fn json_wrapper_is_the_json_text_as_a_string() {
    let value = BigQueryJson(json!({"k": [1, null]}));
    let text = serde_json::to_string(&value).expect("valid test input");
    assert_eq!(text, r#""{\"k\":[1,null]}""#);
    assert_eq!(
        serde_json::from_str::<BigQueryJson<Value>>(&text).expect("valid test input"),
        value
    );

    let null = BigQueryJson(Value::Null);
    let text = serde_json::to_string(&Some(null.clone())).expect("valid test input");
    assert_eq!(text, r#""null""#, "JSON null is text, apart from SQL NULL");
    assert_eq!(
        serde_json::from_str::<Option<BigQueryJson<Value>>>(&text).expect("valid test input"),
        Some(null)
    );
    assert_eq!(
        serde_json::from_str::<Option<BigQueryJson<Value>>>("null").expect("valid test input"),
        None
    );
    assert!(serde_json::from_str::<BigQueryJson<Doc>>(r#""{\"a\":\"x\"}""#).is_err());
}

#[test]
fn json_with_modules_match_the_wrapper() {
    let row = Row {
        doc: Doc {
            a: 1,
            b: vec!["x".to_string()],
        },
        maybe: None,
    };
    let text = serde_json::to_string(&row).expect("valid test input");
    assert_eq!(text, r#"{"doc":"{\"a\":1,\"b\":[\"x\"]}","maybe":null}"#);
    assert_eq!(
        serde_json::from_str::<Row>(&text).expect("valid test input"),
        row
    );
    let with_value = Row {
        maybe: Some(json!(2)),
        ..row
    };
    let text = serde_json::to_string(&with_value).expect("valid test input");
    assert_eq!(
        serde_json::from_str::<Row>(&text).expect("valid test input"),
        with_value
    );
}
