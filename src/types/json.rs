use serde::de::{DeserializeOwned, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt::Formatter;
use std::marker::PhantomData;

pub(crate) const TAG_JSON: &str = "BigQueryJson";

/// A JSON value carried as its JSON text, whatever the serde format.
///
/// The crate's codecs need no wrapper on a JSON column: a `String` field is the JSON text as
/// it is, and any other shape, such as `serde_json::Value` or a typed document, is parsed on
/// read and printed on write. This wrapper is for the two cases that rule leaves out: a
/// `BigQueryJson<String>` holds a JSON string parsed into a `String`, not the text, and in other
/// serde formats the value is the JSON text as a string.
///
/// In an `Option`, SQL NULL is `None`. JSON `null` is `Some(BigQueryJson(Value::Null))` for a
/// `T` that can hold `null`, such as `serde_json::Value`, so the two stay apart; for any other
/// `T`, such as a typed struct, the crate's decoder reads JSON `null` as `None`, the same as it
/// does for a bare `Option<T>` field. Text that does not parse into `T` fails its row with
/// [`Custom`](crate::errors::BigQueryCodecErrorKind::Custom).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct BigQueryJson<T>(pub T);

fn serialize_json<T: Serialize + ?Sized, S: Serializer>(
    value: &T,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let text = serde_json::to_string(value).map_err(serde::ser::Error::custom)?;
    serializer.serialize_newtype_struct(TAG_JSON, &text)
}

fn deserialize_json<'de, T: DeserializeOwned, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<T, D::Error> {
    deserializer.deserialize_newtype_struct(TAG_JSON, JsonVisitor(PhantomData))
}

impl<T: Serialize> Serialize for BigQueryJson<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serialize_json(&self.0, serializer)
    }
}

impl<'de, T: DeserializeOwned> Deserialize<'de> for BigQueryJson<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserialize_json(deserializer).map(BigQueryJson)
    }
}

struct JsonVisitor<T>(PhantomData<T>);

impl<'de, T: DeserializeOwned> Visitor<'de> for JsonVisitor<T> {
    type Value = T;

    fn expecting(&self, f: &mut Formatter) -> std::fmt::Result {
        f.write_str("JSON text")
    }

    fn visit_newtype_struct<D: Deserializer<'de>>(self, deserializer: D) -> Result<T, D::Error> {
        deserializer.deserialize_str(self)
    }

    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<T, E> {
        serde_json::from_str(v).map_err(E::custom)
    }
}

/// `#[serde(with = "bigquery::serialize_as_json")]` for a bare field of a JSON column, the same
/// as [`BigQueryJson`].
pub mod serialize_as_json {
    use serde::de::DeserializeOwned;
    use serde::{Deserializer, Serialize, Serializer};

    /// Serializes `value` as [`BigQueryJson`](crate::BigQueryJson) does.
    pub fn serialize<T: Serialize, S: Serializer>(
        value: &T,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        super::serialize_json(value, serializer)
    }

    /// Deserializes a value as [`BigQueryJson`](crate::BigQueryJson) does.
    pub fn deserialize<'de, T: DeserializeOwned, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<T, D::Error> {
        super::deserialize_json(deserializer)
    }
}

/// `#[serde(with = "bigquery::serialize_as_optional_json")]` for an `Option` field of a JSON
/// column, the same as `Option<BigQueryJson<T>>`.
pub mod serialize_as_optional_json {
    use crate::BigQueryJson;
    use serde::de::DeserializeOwned;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    /// Serializes `value` as `Option<BigQueryJson<T>>` does.
    pub fn serialize<T: Serialize, S: Serializer>(
        value: &Option<T>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        value.as_ref().map(BigQueryJson).serialize(serializer)
    }

    /// Deserializes a value as `Option<BigQueryJson<T>>` does.
    pub fn deserialize<'de, T: DeserializeOwned, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<T>, D::Error> {
        Option::<BigQueryJson<T>>::deserialize(deserializer).map(|json| json.map(|json| json.0))
    }
}

#[cfg(test)]
mod tests {
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
}
