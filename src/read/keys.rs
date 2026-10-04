//! How struct fields are handed to a serde-derived visitor: by position or by name.
//!
//! A derived `Deserialize` takes field keys as `visit_u64(i)` or as the field name. Position
//! keys are the fast path, and they are only correct when the struct's `fields` list numbers
//! exactly its fields. serde derive also lists every alias there while numbering only the
//! fields, so a struct with an alias whose names are all columns would get its values shifted
//! into the wrong fields. A probe on the first row of each batch tells the two apart: the last
//! position of `fields` is offered first. Without an alias it is the last field, and derive asks
//! for its value with the field's own type. With an alias it lies beyond derive's numbering, so
//! derive either ignores it, asking for the value as `IgnoredAny`, or, under
//! `deny_unknown_fields`, rejects the key. Either answer switches the plan to name keys and
//! marks the row to be decoded again.
//!
//! The probe relies on serde derive's numbering, which is stable but not a documented contract;
//! the attribute tests in `decoder` pin it. A struct whose last field is itself `IgnoredAny`
//! looks aliased and is read with name keys, which is correct and slightly slower.

use crate::read::decoder::{Node, ValueDe};
use crate::errors::BigQueryCodecErrorKind;
use crate::types::error::CodecError;
use serde::de::value::{BorrowedStrDeserializer, U64Deserializer};
use serde::de::{DeserializeSeed, MapAccess, Visitor};
use std::cell::Cell;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum KeyMode {
    /// Keys as `visit_u64(i)`, `i` the position in `fields`.
    Index,
    /// Keys as the field name.
    Name,
}

/// One struct target resolved against the columns of one node of a batch.
pub(crate) struct Plan {
    mode: Cell<KeyMode>,
    /// `(position in fields, column)`, in `fields` order, for every name that is a column.
    keys: Vec<(u32, u32)>,
    /// A column that no name in `fields` matches. It is offered by name until the target
    /// ignores it once, so that `deny_unknown_fields` rejects every row of a batch with extra
    /// columns, while any other target pays for it on one row only.
    unknown: Option<u32>,
    unknown_ignored: Cell<bool>,
    probed: Cell<bool>,
}

impl Plan {
    /// Matches `fields` to the node's column names. Position keys are chosen only when every
    /// name in `fields` is a column: a name without one is an alias or a defaulted field, and
    /// both shift derive's numbering away from `fields`.
    pub(crate) fn resolve(fields: &'static [&'static str], columns: &[&str]) -> Plan {
        let found: Vec<Option<u32>> = fields
            .iter()
            .map(|name| {
                columns
                    .iter()
                    .position(|column| column == name)
                    .and_then(|c| u32::try_from(c).ok())
            })
            .collect();
        let mode = if found.iter().all(Option::is_some) {
            KeyMode::Index
        } else {
            KeyMode::Name
        };
        let keys = found
            .iter()
            .enumerate()
            .filter_map(|(j, c)| Some((u32::try_from(j).ok()?, (*c)?)))
            .collect();
        let unknown = (0..columns.len())
            .filter_map(|c| u32::try_from(c).ok())
            .find(|c| !found.contains(&Some(*c)));
        Plan {
            mode: Cell::new(mode),
            keys,
            unknown,
            unknown_ignored: Cell::new(false),
            probed: Cell::new(false),
        }
    }

    pub(crate) fn mode(&self) -> KeyMode {
        self.mode.get()
    }
}

/// The keys of one struct value, in the order the plan gives them.
pub(crate) struct FieldMap<'n, 'a> {
    node: &'n Node<'a>,
    plan: &'n Plan,
    fields: &'static [&'static str],
    row: usize,
    /// The next entry of `order` to hand out.
    next: usize,
    order: Order,
    pending: Option<Pending>,
}

/// Which keys a [`FieldMap`] hands out and in what order.
#[derive(Clone, Copy)]
enum Order {
    /// The plan's keys in `fields` order.
    Plain { offer_unknown: bool },
    /// The last key first as the alias probe, then the others.
    Probe { offer_unknown: bool },
}

#[derive(Clone, Copy)]
enum Pending {
    Field(u32),
    Probe(u32),
    Unknown(u32),
}

impl<'n, 'a> FieldMap<'n, 'a> {
    pub(crate) fn new(
        node: &'n Node<'a>,
        plan: &'n Plan,
        fields: &'static [&'static str],
        row: usize,
    ) -> Self {
        let offer_unknown = plan.unknown.is_some() && !plan.unknown_ignored.get();
        let order = if plan.mode() == KeyMode::Index && !plan.probed.replace(true) {
            Order::Probe { offer_unknown }
        } else {
            Order::Plain { offer_unknown }
        };
        FieldMap {
            node,
            plan,
            fields,
            row,
            next: 0,
            order,
            pending: None,
        }
    }

    /// The entry at `step`: the unknown column first when it is offered, then the keys.
    fn entry(&self, step: usize) -> Option<Pending> {
        let (offer_unknown, probe) = match self.order {
            Order::Plain { offer_unknown } => (offer_unknown, false),
            Order::Probe { offer_unknown } => (offer_unknown, true),
        };
        let keys = &self.plan.keys;
        let last = keys.len().checked_sub(1);
        // The probe goes before the unknown column, so that a probe answer is never hidden
        // behind an `unknown field` error.
        let mut step = step;
        if probe {
            if step == 0 {
                return last.map(|l| Pending::Probe(l as u32));
            }
            step -= 1;
        }
        if offer_unknown {
            if step == 0 {
                return self.plan.unknown.map(Pending::Unknown);
            }
            step -= 1;
        }
        let index = if probe {
            // The probed key is the last one and was already handed out.
            (step + 1 < keys.len()).then_some(step)?
        } else {
            (step < keys.len()).then_some(step)?
        };
        Some(Pending::Field(index as u32))
    }

    fn remaining(&self) -> usize {
        let mut total = self.plan.keys.len();
        if matches!(
            self.order,
            Order::Plain {
                offer_unknown: true
            } | Order::Probe {
                offer_unknown: true
            }
        ) {
            total += 1;
        }
        total.saturating_sub(self.next)
    }

    fn mark_aliased(&self) {
        self.plan.mode.set(KeyMode::Name);
        self.node.request_redo();
    }
}

impl<'a> MapAccess<'a> for FieldMap<'_, 'a> {
    type Error = CodecError;

    fn next_key_seed<K: DeserializeSeed<'a>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, CodecError> {
        let Some(entry) = self.entry(self.next) else {
            return Ok(None);
        };
        self.next += 1;
        self.pending = Some(entry);
        match entry {
            Pending::Field(k) => {
                let (j, _) = self.plan.keys[k as usize];
                match self.plan.mode() {
                    KeyMode::Index => seed.deserialize(U64Deserializer::new(u64::from(j))),
                    KeyMode::Name => {
                        seed.deserialize(BorrowedStrDeserializer::new(self.fields[j as usize]))
                    }
                }
                .map(Some)
            }
            Pending::Probe(k) => {
                let (j, _) = self.plan.keys[k as usize];
                let key = seed.deserialize(U64Deserializer::<CodecError>::new(u64::from(j)));
                if key.is_err() {
                    self.mark_aliased();
                }
                key.map(Some)
            }
            Pending::Unknown(c) => seed
                .deserialize(BorrowedStrDeserializer::new(self.node.name(c as usize)))
                .map(Some),
        }
    }

    fn next_value_seed<S: DeserializeSeed<'a>>(&mut self, seed: S) -> Result<S::Value, CodecError> {
        let entry = self.pending.take().ok_or_else(|| {
            CodecError::new(
                BigQueryCodecErrorKind::Custom,
                "a struct value was requested before its key",
            )
        })?;
        let (c, value) = match entry {
            Pending::Field(k) => {
                let c = self.plan.keys[k as usize].1 as usize;
                let value = self
                    .node
                    .col(c)
                    .and_then(|col| seed.deserialize(ValueDe::new(col, self.row)));
                (c, value)
            }
            Pending::Probe(k) => {
                let c = self.plan.keys[k as usize].1 as usize;
                let value = self.node.col(c).and_then(|col| {
                    seed.deserialize(ProbeValue {
                        map: self,
                        value: ValueDe::new(col, self.row),
                    })
                });
                (c, value)
            }
            Pending::Unknown(c) => {
                let c = c as usize;
                let value = seed.deserialize(Unknown { map: self, c });
                (c, value)
            }
        };
        value.map_err(|e| e.at_field(self.node.name(c)))
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.remaining())
    }
}

/// The value of the probe key. Asked for as `IgnoredAny`, the key was out of derive's
/// numbering; asked for as anything else, it was the last field, and the column is read.
struct ProbeValue<'m, 'n, 'a> {
    map: &'m FieldMap<'n, 'a>,
    value: ValueDe<'n, 'a>,
}

macro_rules! forward_to_value {
    ($($method:ident),*) => {
        $(
            fn $method<V: Visitor<'a>>(self, v: V) -> Result<V::Value, CodecError> {
                serde::Deserializer::$method(self.value, v)
            }
        )*
    };
}

impl<'a> serde::Deserializer<'a> for ProbeValue<'_, '_, 'a> {
    type Error = CodecError;

    forward_to_value!(
        deserialize_any,
        deserialize_bool,
        deserialize_i8,
        deserialize_i16,
        deserialize_i32,
        deserialize_i64,
        deserialize_i128,
        deserialize_u8,
        deserialize_u16,
        deserialize_u32,
        deserialize_u64,
        deserialize_u128,
        deserialize_f32,
        deserialize_f64,
        deserialize_char,
        deserialize_str,
        deserialize_string,
        deserialize_bytes,
        deserialize_byte_buf,
        deserialize_option,
        deserialize_unit,
        deserialize_seq,
        deserialize_map,
        deserialize_identifier
    );

    fn deserialize_ignored_any<V: Visitor<'a>>(self, _v: V) -> Result<V::Value, CodecError> {
        self.map.mark_aliased();
        Err(CodecError::new(
            BigQueryCodecErrorKind::Custom,
            "the struct's field list holds an alias; the row is read again with name keys",
        ))
    }

    fn deserialize_unit_struct<V: Visitor<'a>>(
        self,
        name: &'static str,
        v: V,
    ) -> Result<V::Value, CodecError> {
        self.value.deserialize_unit_struct(name, v)
    }

    fn deserialize_newtype_struct<V: Visitor<'a>>(
        self,
        name: &'static str,
        v: V,
    ) -> Result<V::Value, CodecError> {
        self.value.deserialize_newtype_struct(name, v)
    }

    fn deserialize_tuple<V: Visitor<'a>>(self, len: usize, v: V) -> Result<V::Value, CodecError> {
        self.value.deserialize_tuple(len, v)
    }

    fn deserialize_tuple_struct<V: Visitor<'a>>(
        self,
        name: &'static str,
        len: usize,
        v: V,
    ) -> Result<V::Value, CodecError> {
        self.value.deserialize_tuple_struct(name, len, v)
    }

    fn deserialize_struct<V: Visitor<'a>>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        v: V,
    ) -> Result<V::Value, CodecError> {
        self.value.deserialize_struct(name, fields, v)
    }

    fn deserialize_enum<V: Visitor<'a>>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        v: V,
    ) -> Result<V::Value, CodecError> {
        self.value.deserialize_enum(name, variants, v)
    }
}

/// The value of the unknown column: answered without decoding when the target ignores it,
/// which is what derive does with a key it does not know.
struct Unknown<'m, 'n, 'a> {
    map: &'m FieldMap<'n, 'a>,
    c: usize,
}

impl<'a> serde::Deserializer<'a> for Unknown<'_, '_, 'a> {
    type Error = CodecError;

    fn deserialize_any<V: Visitor<'a>>(self, v: V) -> Result<V::Value, CodecError> {
        let col = self.map.node.col(self.c)?;
        ValueDe::new(col, self.map.row).deserialize_any(v)
    }

    fn deserialize_ignored_any<V: Visitor<'a>>(self, v: V) -> Result<V::Value, CodecError> {
        self.map.plan.unknown_ignored.set(true);
        v.visit_unit()
    }

    serde::forward_to_deserialize_any! {
        <W: Visitor<'a>>
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string bytes byte_buf
        option unit unit_struct newtype_struct seq tuple tuple_struct map struct enum identifier
    }
}

