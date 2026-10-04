//! The columns a typed read selects by itself: the target's top-level serde fields that are
//! columns of the table.

use crate::errors::BigQueryCodecErrorKind;
use crate::types::error::CodecError;
use serde::de::{DeserializeOwned, Visitor};
use std::cell::Cell;

/// The names in `T`'s top-level serde `fields` list, aliases included, or `None` when `T` is
/// not a plain struct: a struct with `flatten`, a map, a dynamic value or a tuple has no list
/// that names every column it reads.
pub(crate) fn struct_fields<T: DeserializeOwned>() -> Option<&'static [&'static str]> {
    let found = Cell::new(None);
    // The probe always ends the deserialization with an error once it has the list.
    let _ = T::deserialize(FieldsProbe { found: &found });
    found.get()
}

/// The table's columns that `fields` names, in table order; `None` when there is nothing to
/// project, since an empty `selected_fields` reads every column.
pub(crate) fn intersect(fields: &[&str], columns: &[String]) -> Option<Vec<String>> {
    let selected: Vec<String> = columns
        .iter()
        .filter(|c| fields.contains(&c.as_str()))
        .cloned()
        .collect();
    (!selected.is_empty()).then_some(selected)
}

struct FieldsProbe<'f> {
    found: &'f Cell<Option<&'static [&'static str]>>,
}

fn stop() -> CodecError {
    CodecError::new(BigQueryCodecErrorKind::Custom, "projection probe")
}

impl<'de> serde::Deserializer<'de> for FieldsProbe<'_> {
    type Error = CodecError;

    fn deserialize_any<V: Visitor<'de>>(self, _v: V) -> Result<V::Value, CodecError> {
        Err(stop())
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        _v: V,
    ) -> Result<V::Value, CodecError> {
        self.found.set(Some(fields));
        Err(stop())
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        v: V,
    ) -> Result<V::Value, CodecError> {
        v.visit_newtype_struct(self)
    }

    fn deserialize_option<V: Visitor<'de>>(self, v: V) -> Result<V::Value, CodecError> {
        v.visit_some(self)
    }

    serde::forward_to_deserialize_any! {
        <W: Visitor<'de>>
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string bytes byte_buf
        unit unit_struct seq tuple tuple_struct map enum identifier ignored_any
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use std::collections::HashMap;

    #[test]
    fn plain_structs_name_their_fields_and_other_shapes_none() {
        #[derive(Deserialize)]
        #[allow(dead_code, reason = "only its serde fields list is read")]
        struct Row {
            id: i64,
            #[serde(alias = "user_name")]
            name: String,
        }
        #[derive(Deserialize)]
        #[allow(dead_code, reason = "only its serde fields list is read")]
        struct Flat {
            id: i64,
            #[serde(flatten)]
            rest: HashMap<String, String>,
        }
        assert_eq!(
            struct_fields::<Row>(),
            Some(&["id", "name", "user_name"][..])
        );
        assert_eq!(struct_fields::<Flat>(), None);
        assert_eq!(struct_fields::<HashMap<String, i64>>(), None);
        assert_eq!(struct_fields::<serde_json::Value>(), None);
        assert_eq!(struct_fields::<(i64, String)>(), None);
    }
}
