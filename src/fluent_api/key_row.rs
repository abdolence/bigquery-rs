use crate::errors::BigQueryError;
use crate::BigQueryResult;
use serde::ser::{Error, Impossible, SerializeMap, SerializeTuple};
use serde::{Serialize, Serializer};

/// A primary key value as a row of just the key columns: a tuple gives one column per
/// element, in the key's column order, and any other value is the key's only column.
pub(crate) struct BigQueryKeyRow<'c, K> {
    columns: &'c [String],
    key: K,
}

impl<'c, K: Serialize> BigQueryKeyRow<'c, K> {
    /// Pairs `key` with the table's key `columns`.
    ///
    /// # Errors
    /// [`BigQueryError::InvalidParametersError`] for the field `key` when `key` does not have
    /// one value per key column.
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
        self.key.serialize(KeyColumns {
            columns: self.columns,
            map: &mut map,
        })?;
        map.end()
    }
}

/// Writes a key value's parts as map entries named by the key columns.
struct KeyColumns<'c, 'm, M> {
    columns: &'c [String],
    map: &'m mut M,
}

impl<M: SerializeMap> KeyColumns<'_, '_, M> {
    fn arity_error(&self, values: usize) -> M::Error {
        M::Error::custom(format!(
            "the primary key has {} columns ({}), the key value has {values}",
            self.columns.len(),
            self.columns.join(", ")
        ))
    }

    fn single<T: Serialize + ?Sized>(self, value: &T) -> Result<(), M::Error> {
        match self.columns {
            [column] => self.map.serialize_entry(column, value),
            _ => Err(self.arity_error(1)),
        }
    }

    fn unsupported(what: &str) -> M::Error {
        M::Error::custom(format!(
            "a primary key value is a single value or a tuple, not {what}"
        ))
    }
}

macro_rules! single_column {
    ($($method:ident: $type:ty),* $(,)?) => {
        $(fn $method(self, value: $type) -> Result<(), M::Error> {
            self.single(&value)
        })*
    };
}

impl<'c, 'm, M: SerializeMap> Serializer for KeyColumns<'c, 'm, M> {
    type Ok = ();
    type Error = M::Error;
    type SerializeSeq = Impossible<(), M::Error>;
    type SerializeTuple = KeyTupleColumns<'c, 'm, M>;
    type SerializeTupleStruct = Impossible<(), M::Error>;
    type SerializeTupleVariant = Impossible<(), M::Error>;
    type SerializeMap = Impossible<(), M::Error>;
    type SerializeStruct = Impossible<(), M::Error>;
    type SerializeStructVariant = Impossible<(), M::Error>;

    single_column! {
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
        self.single(&Option::<()>::None)
    }

    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<(), M::Error> {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<(), M::Error> {
        Err(Self::unsupported("a unit"))
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<(), M::Error> {
        Err(Self::unsupported("a unit struct"))
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
    ) -> Result<(), M::Error> {
        self.single(variant)
    }

    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<(), M::Error> {
        self.single(value)
    }

    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<(), M::Error> {
        Err(Self::unsupported("an enum variant with data"))
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, M::Error> {
        Err(Self::unsupported("a sequence"))
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
        Err(Self::unsupported("a tuple struct"))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, M::Error> {
        Err(Self::unsupported("an enum variant with data"))
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, M::Error> {
        Err(Self::unsupported("a map"))
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, M::Error> {
        Err(Self::unsupported(
            "a struct; pass a struct with .object(..)",
        ))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, M::Error> {
        Err(Self::unsupported("an enum variant with data"))
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
