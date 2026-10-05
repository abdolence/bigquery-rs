use crate::errors::BigQueryError;
use crate::BigQueryResult;
use serde::ser::{Error, Impossible, SerializeMap, SerializeTuple};
use serde::{Serialize, Serializer};

/// A primary key value as a row of just the key columns. For a key of one column the value is
/// that column, written as it is, so the row encoder applies its usual type rules (a `Vec<u8>`
/// for BYTES, say). For a key of several columns the value is a tuple, or a newtype over one,
/// with one element per column in the key's column order.
pub(crate) struct BigQueryKeyRow<'c, K> {
    columns: &'c [String],
    key: K,
}

impl<'c, K: Serialize> BigQueryKeyRow<'c, K> {
    /// Pairs `key` with the table's key `columns`.
    ///
    /// # Errors
    /// [`BigQueryError::InvalidParametersError`] for the field `key` when a key of several
    /// columns is not a tuple with one element per column. A value for a key of one column is
    /// checked only by the row encoder, at the write.
    pub(crate) fn new(columns: &'c [String], key: K) -> BigQueryResult<Self> {
        let row = Self { columns, key };
        serde_json::to_writer(std::io::sink(), &row)
            .map_err(|error| BigQueryError::invalid_parameters("key", error.to_string()))?;
        Ok(row)
    }
}

impl<K: Serialize> Serialize for BigQueryKeyRow<'_, K> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.columns.len()))?;
        match self.columns {
            [column] => map.serialize_entry(column, &self.key)?,
            columns => self.key.serialize(CompositeKeyColumns {
                columns,
                map: &mut map,
            })?,
        }
        map.end()
    }
}

/// Writes the elements of a tuple key as map entries named by the key's columns, for a key of
/// two or more columns.
struct CompositeKeyColumns<'c, 'm, M> {
    columns: &'c [String],
    map: &'m mut M,
}

impl<M: SerializeMap> CompositeKeyColumns<'_, '_, M> {
    fn arity_error(&self, values: usize) -> M::Error {
        M::Error::custom(format!(
            "the primary key has {} columns ({}), the key value has {values}",
            self.columns.len(),
            self.columns.join(", ")
        ))
    }

    fn unsupported(&self, what: &str) -> M::Error {
        M::Error::custom(format!(
            "the primary key has {} columns ({}), so its value is a tuple, not {what}",
            self.columns.len(),
            self.columns.join(", ")
        ))
    }
}

macro_rules! one_value {
    ($($method:ident: $type:ty),* $(,)?) => {
        $(fn $method(self, _value: $type) -> Result<(), M::Error> {
            Err(self.arity_error(1))
        })*
    };
}

impl<'c, 'm, M: SerializeMap> Serializer for CompositeKeyColumns<'c, 'm, M> {
    type Ok = ();
    type Error = M::Error;
    type SerializeSeq = Impossible<(), M::Error>;
    type SerializeTuple = KeyTupleColumns<'c, 'm, M>;
    type SerializeTupleStruct = Impossible<(), M::Error>;
    type SerializeTupleVariant = Impossible<(), M::Error>;
    type SerializeMap = Impossible<(), M::Error>;
    type SerializeStruct = Impossible<(), M::Error>;
    type SerializeStructVariant = Impossible<(), M::Error>;

    one_value! {
        serialize_bool: bool,
        serialize_i8: i8,
        serialize_i16: i16,
        serialize_i32: i32,
        serialize_i64: i64,
        serialize_i128: i128,
        serialize_u8: u8,
        serialize_u16: u16,
        serialize_u32: u32,
        serialize_u64: u64,
        serialize_u128: u128,
        serialize_f32: f32,
        serialize_f64: f64,
        serialize_char: char,
        serialize_str: &str,
        serialize_bytes: &[u8],
    }

    fn serialize_none(self) -> Result<(), M::Error> {
        Err(self.unsupported("None"))
    }

    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<(), M::Error> {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<(), M::Error> {
        Err(self.unsupported("a unit"))
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<(), M::Error> {
        Err(self.unsupported("a unit struct"))
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
    ) -> Result<(), M::Error> {
        Err(self.arity_error(1))
    }

    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<(), M::Error> {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<(), M::Error> {
        Err(self.unsupported("an enum variant"))
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, M::Error> {
        Err(self.unsupported("a sequence"))
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, M::Error> {
        if len != self.columns.len() {
            return Err(self.arity_error(len));
        }
        Ok(KeyTupleColumns {
            columns: self.columns.iter(),
            map: self.map,
        })
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, M::Error> {
        Err(self.unsupported("a tuple struct"))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, M::Error> {
        Err(self.unsupported("an enum variant"))
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, M::Error> {
        Err(self.unsupported("a map"))
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, M::Error> {
        Err(self.unsupported("a struct; pass a struct with .object(..)"))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, M::Error> {
        Err(self.unsupported("an enum variant"))
    }
}

/// The elements of a tuple key, each written under the next key column. The tuple's length
/// was checked against the columns before the first element.
struct KeyTupleColumns<'c, 'm, M> {
    columns: std::slice::Iter<'c, String>,
    map: &'m mut M,
}

impl<M: SerializeMap> SerializeTuple for KeyTupleColumns<'_, '_, M> {
    type Ok = ();
    type Error = M::Error;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), M::Error> {
        let column = self
            .columns
            .next()
            .ok_or_else(|| M::Error::custom("a tuple key wrote more elements than its length"))?;
        self.map.serialize_entry(column, value)
    }

    fn end(self) -> Result<(), M::Error> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    fn key_columns(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| name.to_string()).collect()
    }

    fn key_row<K: Serialize>(columns: &[String], key: K) -> Value {
        let row = BigQueryKeyRow::new(columns, key).expect("the key fits the columns");
        serde_json::to_value(&row).expect("a key row serializes to JSON")
    }

    #[derive(Serialize)]
    struct LineKey((i64, &'static str));

    #[test]
    fn a_newtype_over_a_tuple_fills_a_composite_key() {
        let columns = key_columns(&["order_id", "line"]);
        assert_eq!(
            key_row(&columns, LineKey((7, "line-1"))),
            json!({"order_id": 7, "line": "line-1"})
        );
    }

    #[test]
    fn a_one_column_key_is_written_as_given_whatever_its_shape() {
        let columns = key_columns(&["checksum"]);
        assert_eq!(key_row(&columns, vec![1u8, 2]), json!({"checksum": [1, 2]}));
        assert_eq!(
            key_row(&columns, [7u8; 16]),
            json!({"checksum": vec![7; 16]})
        );
        assert_eq!(key_row(&columns, (42,)), json!({"checksum": [42]}));
    }
}
