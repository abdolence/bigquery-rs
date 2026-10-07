//! Schema inference from a row type's `Deserialize` impl.
//!
//! The type is never given a row. Each run drives `T::deserialize` with a [`Node`] that walks a
//! route of field names down to one field, answers what that field asks for with a value of the
//! kind it asked for, and records the request: an `i64` is INT64, an `Option` NULLABLE, a
//! struct a RECORD whose fields get runs of their own. One field per run keeps a field without
//! an answer, such as a `serde_json::Value`, from hiding the fields after it.
//!
//! Some fields take more than one run:
//!
//! - a field that reads a string is offered BigQuery's text for each temporal type, the text the
//!   decoder hands a string target, and takes the type whose text it reads;
//! - an enum is offered each of its variants, to tell a unit-variant enum from one with data;
//! - serde derive lists a field's aliases next to its name in `fields`. Once a field's value is
//!   produced, its key is offered a second time, and serde's `duplicate field` names the field
//!   the key belongs to. A struct with a field whose value cannot be produced has its fields
//!   counted by position instead, the way the decoder's key plan tells an aliased struct.

use crate::errors::{BigQuerySchemaInferenceError, BigQuerySchemaInferenceErrorKind};
use crate::schema::declaration::{BigQuerySchemaColumn, BigQuerySchemaColumns, ColumnKind};
use crate::sql::dotted_path;
use crate::types::civil;
use crate::types::decimal::TAG_DECIMAL;
use crate::types::interval::TAG_INTERVAL;
use crate::types::json::TAG_JSON;
use crate::types::range::TAG_RANGE;
use crate::types::temporal::{TAG_DATE, TAG_DATETIME, TAG_TIME, TAG_TIMESTAMP};
use crate::{BigQueryFieldMode, BigQueryFieldType, BigQueryRangeElementType};
use serde::de::value::{BorrowedStrDeserializer, U64Deserializer};
use serde::de::{
    self, DeserializeOwned, DeserializeSeed, EnumAccess, MapAccess, SeqAccess, VariantAccess,
    Visitor,
};
use serde::Deserializer;
use std::cell::{Cell, RefCell};
use std::fmt::{Display, Formatter};
use std::marker::PhantomData;

impl BigQuerySchemaColumns {
    pub(super) fn infer<T: DeserializeOwned>() -> Self {
        let mut explorer = Explorer::<T> {
            ancestors: Vec::new(),
            row: PhantomData,
        };
        let root = explorer.run(&[], Offer::FIRST);
        let shape = match root.answer {
            Some(Answer::Struct(shape)) if root.wrappers.is_empty() => Ok(shape),
            Some(Answer::Uninferred(
                kind @ (BigQuerySchemaInferenceErrorKind::UnknownKeys
                | BigQuerySchemaInferenceErrorKind::DynamicValue),
            )) => Err(kind),
            _ => Err(BigQuerySchemaInferenceErrorKind::NotAStruct),
        };
        match shape.and_then(|shape| explorer.record(&[], "", shape)) {
            Ok(columns) => columns.into(),
            Err(kind) => Self {
                columns: Vec::new(),
                problem: Some(BigQuerySchemaInferenceError::new(String::new(), kind)),
            },
        }
    }
}

/// Drives `T::deserialize` once per question and builds the columns from the answers.
struct Explorer<T> {
    /// The structs being explored, outermost first, so that a struct inside itself is refused
    /// instead of explored forever.
    ancestors: Vec<StructShape>,
    row: PhantomData<fn() -> T>,
}

/// A struct as serde derive names it, with every field name and alias in `fields`.
#[derive(Clone, Copy, Debug, PartialEq)]
struct StructShape {
    name: &'static str,
    fields: &'static [&'static str],
}

/// One name of a struct's `fields` list, traced.
struct TracedName {
    identity: Identity,
    column: Option<BigQuerySchemaColumn>,
}

impl<T: DeserializeOwned> Explorer<T> {
    fn run(&self, route: &[Key], offer: Offer) -> Trace {
        let run = Run {
            route,
            offer,
            trace: RefCell::default(),
            repeated: Cell::new(false),
        };
        // A run ends in an error once it has its answer, or in a value nobody needs: either
        // way what it found is in the trace.
        let _ = T::deserialize(Node {
            run: &run,
            depth: 0,
        });
        run.trace.into_inner()
    }

    /// The columns of the struct at `route`, its fields named once each.
    fn record(
        &mut self,
        route: &[Key],
        path: &str,
        shape: StructShape,
    ) -> Result<Vec<BigQuerySchemaColumn>, BigQuerySchemaInferenceErrorKind> {
        if self.ancestors.contains(&shape) {
            return Err(BigQuerySchemaInferenceErrorKind::Recursive);
        }
        self.ancestors.push(shape);
        let traced: Vec<TracedName> = shape
            .fields
            .iter()
            .map(|name| self.name(route, path, name))
            .collect();
        self.ancestors.pop();

        if traced.iter().any(|name| name.identity == Identity::Unknown) {
            let aliases = traced
                .iter()
                .filter(|name| matches!(name.identity, Identity::AliasOf(_)))
                .count();
            match self.numbered_fields(route, shape) {
                // A hand-written impl that does not number its fields: its names are taken
                // as its fields, since such an impl is not where serde's aliases come from.
                0 => {}
                numbered if shape.fields.len() - numbered == aliases => {}
                _ => return Err(BigQuerySchemaInferenceErrorKind::UnresolvedAlias),
            }
        }
        Ok(traced
            .into_iter()
            .filter(|name| !matches!(name.identity, Identity::AliasOf(_)))
            .filter_map(|name| name.column)
            .collect())
    }

    /// How many fields serde derive numbers in the struct at `route`: the first position it
    /// ignores or refuses as a key. Aliases are in `fields` and not numbered.
    fn numbered_fields(&self, route: &[Key], shape: StructShape) -> usize {
        let mut probe = route.to_vec();
        (0..shape.fields.len())
            .find(|position| {
                probe.truncate(route.len());
                probe.push(Key::Position(*position as u64));
                matches!(self.run(&probe, Offer::FIRST).answer, Some(Answer::Ignored))
            })
            .unwrap_or(shape.fields.len())
    }

    fn name(&mut self, route: &[Key], path: &str, name: &'static str) -> TracedName {
        let mut route = route.to_vec();
        route.push(Key::Name(name));
        let path = dotted_path(path, name);
        let field = self.field(&route);
        let column = self
            .column(&route, &path, field.wrappers, field.answer)
            .map(|(kind, mode)| BigQuerySchemaColumn::inferred(name.to_string(), kind, mode));
        TracedName {
            identity: field.identity,
            column,
        }
    }

    /// The field at the end of `route`, with every offer it needs settled.
    fn field(&self, route: &[Key]) -> Trace {
        let mut first = self.run(route, Offer::FIRST);
        let mut identity = first.identity;
        let mut offered = |offer: Offer| {
            let trace = self.run(route, offer);
            identity = identity.or(trace.identity);
            trace.accepted
        };
        first.answer = match first.answer {
            Some(Answer::Text) if first.accepted => Some(Answer::Column(STRING)),
            Some(Answer::Text) => Some(
                // jiff::Timestamp reads only TIMESTAMP text and civil::Time reads TIME text and
                // the time of DATETIME text. civil::Date and civil::DateTime parse with one
                // parser, so they read the same texts and no text tells them apart. Any other
                // pattern, such as jiff::Span reading `00:00:00`, is a type with a text form
                // of its own, which only a STRING column holds.
                match (
                    offered(Offer::text(TextSample::Timestamp)),
                    offered(Offer::text(TextSample::DateTime)),
                    offered(Offer::text(TextSample::Date)),
                    offered(Offer::text(TextSample::Time)),
                ) {
                    (true, false, false, false) => Answer::Column(BigQueryFieldType::Timestamp),
                    (false, true, false, true) => Answer::Column(BigQueryFieldType::Time),
                    (false, true, true, false) => {
                        Answer::Uninferred(BigQuerySchemaInferenceErrorKind::DateOrDateTime)
                    }
                    _ => Answer::Column(STRING),
                },
            ),
            Some(Answer::Enum { variants }) => {
                let unit =
                    first.accepted && (1..variants).all(|variant| offered(Offer::variant(variant)));
                Some(if unit {
                    Answer::Column(STRING)
                } else {
                    Answer::Uninferred(BigQuerySchemaInferenceErrorKind::EnumWithData)
                })
            }
            other => other,
        };
        first.identity = identity;
        first
    }

    /// The column kind and mode of a field shaped `wrappers` around `answer`; `None` for a name
    /// the struct does not read.
    fn column(
        &mut self,
        route: &[Key],
        path: &str,
        mut wrappers: Vec<Wrapper>,
        answer: Option<Answer>,
    ) -> Option<(ColumnKind, BigQueryFieldMode)> {
        let answer = match answer {
            Some(Answer::Byte) if wrappers.last() == Some(&Wrapper::Sequence) => {
                wrappers.pop();
                Answer::Column(BigQueryFieldType::Bytes { max_length: None })
            }
            Some(Answer::Byte) => Answer::Column(BigQueryFieldType::Int64),
            Some(Answer::Ignored) => return None,
            Some(answer) => answer,
            None => Answer::Uninferred(BigQuerySchemaInferenceErrorKind::Inconsistent),
        };
        let mode = match BigQueryFieldMode::try_from(wrappers.as_slice()) {
            Ok(mode) => mode,
            Err(kind) => {
                return Some((
                    ColumnKind::uninferred(path, kind),
                    BigQueryFieldMode::Repeated,
                ))
            }
        };
        let kind = match answer {
            Answer::Column(field_type) => ColumnKind::Type(field_type),
            Answer::Struct(shape) if shape.name == TAG_RANGE => {
                let mut start = route.to_vec();
                start.push(Key::Name("start"));
                match self.field(&start).answer {
                    Some(Answer::Column(BigQueryFieldType::Date)) => {
                        ColumnKind::Type(BigQueryFieldType::Range(BigQueryRangeElementType::Date))
                    }
                    Some(Answer::Column(BigQueryFieldType::DateTime)) => ColumnKind::Type(
                        BigQueryFieldType::Range(BigQueryRangeElementType::DateTime),
                    ),
                    Some(Answer::Column(BigQueryFieldType::Timestamp)) => ColumnKind::Type(
                        BigQueryFieldType::Range(BigQueryRangeElementType::Timestamp),
                    ),
                    Some(Answer::Uninferred(BigQuerySchemaInferenceErrorKind::DateOrDateTime)) => {
                        ColumnKind::uninferred(
                            path,
                            BigQuerySchemaInferenceErrorKind::DateOrDateTime,
                        )
                    }
                    _ => {
                        ColumnKind::uninferred(path, BigQuerySchemaInferenceErrorKind::RangeElement)
                    }
                }
            }
            Answer::Struct(shape) => match self.record(route, path, shape) {
                Ok(fields) => ColumnKind::Record(fields),
                Err(kind) => ColumnKind::uninferred(path, kind),
            },
            Answer::Uninferred(kind) => ColumnKind::uninferred(path, kind),
            Answer::Byte | Answer::Text | Answer::Enum { .. } | Answer::Ignored => {
                ColumnKind::uninferred(path, BigQuerySchemaInferenceErrorKind::Inconsistent)
            }
        };
        Some((kind, mode))
    }
}

impl ColumnKind {
    fn uninferred(path: &str, kind: BigQuerySchemaInferenceErrorKind) -> Self {
        ColumnKind::Uninferred(BigQuerySchemaInferenceError::new(path.to_string(), kind))
    }
}

const STRING: BigQueryFieldType = BigQueryFieldType::String { max_length: None };

/// The mode the `Option`s and sequences around a value give its column, outermost first.
impl TryFrom<&[Wrapper]> for BigQueryFieldMode {
    type Error = BigQuerySchemaInferenceErrorKind;

    fn try_from(wrappers: &[Wrapper]) -> Result<Self, Self::Error> {
        wrappers
            .iter()
            .try_fold(BigQueryFieldMode::Required, |mode, wrapper| {
                match (mode, wrapper) {
                    (BigQueryFieldMode::Repeated, Wrapper::Sequence) => {
                        Err(BigQuerySchemaInferenceErrorKind::NestedArray)
                    }
                    (BigQueryFieldMode::Repeated, Wrapper::Optional) => {
                        Err(BigQuerySchemaInferenceErrorKind::NullableArrayElement)
                    }
                    (_, Wrapper::Sequence) => Ok(BigQueryFieldMode::Repeated),
                    (_, Wrapper::Optional) => Ok(BigQueryFieldMode::Nullable),
                }
            })
    }
}

/// One step of a route: a struct field by name, or by the position serde derive numbers it.
#[derive(Clone, Copy, Debug)]
enum Key {
    Name(&'static str),
    Position(u64),
}

/// What a run offers the field at the end of its route when the field asks for text or for
/// an enum variant.
#[derive(Clone, Copy, Debug)]
struct Offer {
    text: TextSample,
    variant: usize,
}

impl Offer {
    const FIRST: Offer = Offer {
        text: TextSample::Any,
        variant: 0,
    };

    fn text(text: TextSample) -> Self {
        Self {
            text,
            ..Self::FIRST
        }
    }

    fn variant(variant: usize) -> Self {
        Self {
            variant,
            ..Self::FIRST
        }
    }
}

/// The texts a field that reads a string is offered: any text, then the text the decoder hands
/// a string target for each temporal column type.
#[derive(Clone, Copy, Debug)]
enum TextSample {
    Any,
    Timestamp,
    DateTime,
    Date,
    Time,
}

impl TextSample {
    fn text(self) -> String {
        let mut text = String::new();
        let printed = match self {
            TextSample::Any => {
                text.push('x');
                Ok(())
            }
            TextSample::Timestamp => civil::fmt_timestamp(0, &mut text),
            TextSample::DateTime => civil::fmt_datetime(0, &mut text),
            TextSample::Date => civil::fmt_date(0, &mut text),
            TextSample::Time => civil::fmt_time(0, &mut text),
        };
        printed.expect("the epoch is inside the range of every BigQuery temporal type");
        text
    }
}

/// One run of `T::deserialize`.
struct Run<'a> {
    route: &'a [Key],
    offer: Offer,
    trace: RefCell<Trace>,
    /// Set once the field's value is produced and its key offered again.
    repeated: Cell<bool>,
}

/// What a run found at the end of its route.
#[derive(Debug, Default)]
struct Trace {
    /// The `Option`s and sequences around the field's value, outermost first.
    wrappers: Vec<Wrapper>,
    answer: Option<Answer>,
    /// Whether the field took the offered text, or the offered variant as a unit variant.
    accepted: bool,
    identity: Identity,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Wrapper {
    Optional,
    Sequence,
}

#[derive(Debug)]
enum Answer {
    Column(BigQueryFieldType),
    /// A `u8`: BYTES as the element of a sequence, INT64 anywhere else.
    Byte,
    /// A string, whose column depends on the texts it reads.
    Text,
    /// An enum, whose column depends on whether its variants carry data.
    Enum {
        variants: usize,
    },
    Struct(StructShape),
    /// The value of a key the struct does not read.
    Ignored,
    Uninferred(BigQuerySchemaInferenceErrorKind),
}

/// Whose name a key of `fields` is, as serde's `duplicate field` told it.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
enum Identity {
    #[default]
    Unknown,
    /// The key is the field's own name.
    Own,
    /// The key is an alias of the field with this name.
    AliasOf(&'static str),
}

impl Identity {
    fn or(self, other: Identity) -> Identity {
        match self {
            Identity::Unknown => other,
            known => known,
        }
    }
}

/// How a run ends.
#[derive(Debug)]
enum TraceError {
    /// The run has its answer, or the type refused what it was offered.
    Stop,
    /// serde's `duplicate field`, naming the field the repeated key belongs to.
    DuplicateField(&'static str),
}

impl Display for TraceError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TraceError::Stop => f.write_str("schema inference run stopped"),
            TraceError::DuplicateField(field) => write!(f, "duplicate field `{field}`"),
        }
    }
}

impl std::error::Error for TraceError {}

impl de::Error for TraceError {
    fn custom<M: Display>(_message: M) -> Self {
        TraceError::Stop
    }

    fn duplicate_field(field: &'static str) -> Self {
        TraceError::DuplicateField(field)
    }
}

/// A value inside one run: on its way down the route while `depth` is short of it, the field
/// the run asks about once it is not.
#[derive(Clone, Copy)]
struct Node<'r> {
    run: &'r Run<'r>,
    depth: usize,
}

impl Node<'_> {
    fn at_field(self) -> bool {
        self.depth == self.run.route.len()
    }

    /// Records `answer` for the field. A node still on its way down the route was a struct on
    /// an earlier run, so any other request means the type answers differently now.
    fn answer(self, answer: Answer) -> Result<(), TraceError> {
        let mut trace = self.run.trace.borrow_mut();
        if self.at_field() {
            trace.answer = Some(answer);
            Ok(())
        } else {
            trace.answer = Some(Answer::Uninferred(
                BigQuerySchemaInferenceErrorKind::Inconsistent,
            ));
            Err(TraceError::Stop)
        }
    }

    fn wrap(self, wrapper: Wrapper) {
        if self.at_field() {
            self.run.trace.borrow_mut().wrappers.push(wrapper);
        }
    }
}

impl<'de> Deserializer<'de> for Node<'_> {
    type Error = TraceError;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, TraceError> {
        self.answer(Answer::Uninferred(
            BigQuerySchemaInferenceErrorKind::DynamicValue,
        ))?;
        visitor.visit_unit()
    }

    fn deserialize_bool<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, TraceError> {
        self.answer(Answer::Column(BigQueryFieldType::Bool))?;
        visitor.visit_bool(false)
    }

    fn deserialize_i8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, TraceError> {
        self.answer(Answer::Column(BigQueryFieldType::Int64))?;
        visitor.visit_i8(1)
    }

    fn deserialize_i16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, TraceError> {
        self.answer(Answer::Column(BigQueryFieldType::Int64))?;
        visitor.visit_i16(1)
    }

    fn deserialize_i32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, TraceError> {
        self.answer(Answer::Column(BigQueryFieldType::Int64))?;
        visitor.visit_i32(1)
    }

    fn deserialize_i64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, TraceError> {
        self.answer(Answer::Column(BigQueryFieldType::Int64))?;
        visitor.visit_i64(1)
    }

    fn deserialize_i128<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, TraceError> {
        self.answer(Answer::Column(BigQueryFieldType::Int64))?;
        visitor.visit_i128(1)
    }

    fn deserialize_u8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, TraceError> {
        self.answer(Answer::Byte)?;
        visitor.visit_u8(1)
    }

    fn deserialize_u16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, TraceError> {
        self.answer(Answer::Column(BigQueryFieldType::Int64))?;
        visitor.visit_u16(1)
    }

    fn deserialize_u32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, TraceError> {
        self.answer(Answer::Column(BigQueryFieldType::Int64))?;
        visitor.visit_u32(1)
    }

    fn deserialize_u64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, TraceError> {
        self.answer(Answer::Column(BigQueryFieldType::Int64))?;
        visitor.visit_u64(1)
    }

    fn deserialize_u128<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, TraceError> {
        self.answer(Answer::Column(BigQueryFieldType::Int64))?;
        visitor.visit_u128(1)
    }

    fn deserialize_f32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, TraceError> {
        self.answer(Answer::Column(BigQueryFieldType::Float64))?;
        visitor.visit_f32(0.0)
    }

    fn deserialize_f64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, TraceError> {
        self.answer(Answer::Column(BigQueryFieldType::Float64))?;
        visitor.visit_f64(0.0)
    }

    fn deserialize_char<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, TraceError> {
        self.answer(Answer::Column(STRING))?;
        visitor.visit_char('x')
    }

    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, TraceError> {
        self.answer(Answer::Text)?;
        let taken = visitor.visit_str(&self.run.offer.text.text());
        self.run.trace.borrow_mut().accepted = taken.is_ok();
        taken
    }

    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, TraceError> {
        self.deserialize_str(visitor)
    }

    fn deserialize_bytes<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, TraceError> {
        self.answer(Answer::Column(BigQueryFieldType::Bytes {
            max_length: None,
        }))?;
        visitor.visit_bytes(&[])
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, TraceError> {
        self.deserialize_bytes(visitor)
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, TraceError> {
        self.wrap(Wrapper::Optional);
        visitor.visit_some(self)
    }

    fn deserialize_unit<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, TraceError> {
        self.answer(Answer::Uninferred(
            BigQuerySchemaInferenceErrorKind::NoColumnType,
        ))?;
        visitor.visit_unit()
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, TraceError> {
        self.deserialize_unit(visitor)
    }

    /// The crate's wrappers are named newtype structs; any other newtype is its inner value.
    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, TraceError> {
        if !self.at_field() {
            return visitor.visit_newtype_struct(self);
        }
        let temporal = match name {
            TAG_TIMESTAMP => Some(BigQueryFieldType::Timestamp),
            TAG_DATE => Some(BigQueryFieldType::Date),
            TAG_TIME => Some(BigQueryFieldType::Time),
            TAG_DATETIME => Some(BigQueryFieldType::DateTime),
            _ => None,
        };
        if let Some(field_type) = temporal {
            self.answer(Answer::Column(field_type))?;
            // The wrappers' visitors take the column's integer, as the decoder hands it.
            return visitor.visit_i64(0);
        }
        match name {
            TAG_DECIMAL => {
                self.answer(Answer::Column(BigQueryFieldType::Numeric(None)))?;
                visitor.visit_str("0")
            }
            TAG_JSON => {
                self.answer(Answer::Column(BigQueryFieldType::Json))?;
                visitor.visit_str("null")
            }
            _ => visitor.visit_newtype_struct(self),
        }
    }

    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, TraceError> {
        self.wrap(Wrapper::Sequence);
        visitor.visit_seq(OneElement(Some(self)))
    }

    /// An array of `u8` is BYTES; any other tuple or array has no column type.
    fn deserialize_tuple<V: Visitor<'de>>(
        self,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, TraceError> {
        self.answer(Answer::Column(BigQueryFieldType::Bytes {
            max_length: None,
        }))?;
        visitor.visit_seq(ByteArray {
            run: self.run,
            left: len,
        })
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _len: usize,
        _visitor: V,
    ) -> Result<V::Value, TraceError> {
        self.answer(Answer::Uninferred(
            BigQuerySchemaInferenceErrorKind::NoColumnType,
        ))?;
        Err(TraceError::Stop)
    }

    fn deserialize_map<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, TraceError> {
        self.answer(Answer::Uninferred(
            BigQuerySchemaInferenceErrorKind::UnknownKeys,
        ))?;
        visitor.visit_map(NoEntries)
    }

    /// On the way down, the route's next key. At the field, the struct itself, produced with
    /// no entries, which succeeds when every field of it is optional.
    fn deserialize_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, TraceError> {
        if self.at_field() {
            if name == TAG_INTERVAL {
                self.answer(Answer::Column(BigQueryFieldType::Interval))?;
                return Err(TraceError::Stop);
            }
            self.answer(Answer::Struct(StructShape { name, fields }))?;
            return visitor.visit_map(NoEntries);
        }
        let key = self.run.route[self.depth];
        match visitor.visit_map(RouteMap {
            node: self,
            state: RouteState::Key,
        }) {
            Err(TraceError::DuplicateField(field)) => {
                if let (true, Key::Name(name)) = (self.run.repeated.get(), key) {
                    self.run.trace.borrow_mut().identity = if field == name {
                        Identity::Own
                    } else {
                        Identity::AliasOf(field)
                    };
                }
                Err(TraceError::Stop)
            }
            other => other,
        }
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, TraceError> {
        self.answer(Answer::Enum {
            variants: variants.len(),
        })?;
        match variants.get(self.run.offer.variant) {
            Some(variant) => visitor.visit_enum(VariantOffer {
                run: self.run,
                variant,
            }),
            None => {
                self.answer(Answer::Uninferred(
                    BigQuerySchemaInferenceErrorKind::Inconsistent,
                ))?;
                Err(TraceError::Stop)
            }
        }
    }

    fn deserialize_identifier<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, TraceError> {
        self.answer(Answer::Uninferred(
            BigQuerySchemaInferenceErrorKind::NoColumnType,
        ))?;
        Err(TraceError::Stop)
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, TraceError> {
        self.answer(Answer::Ignored)?;
        visitor.visit_unit()
    }
}

/// A sequence of one element, enough to see what its elements ask for.
struct OneElement<'r>(Option<Node<'r>>);

impl<'de> SeqAccess<'de> for OneElement<'_> {
    type Error = TraceError;

    fn next_element_seed<S: DeserializeSeed<'de>>(
        &mut self,
        seed: S,
    ) -> Result<Option<S::Value>, TraceError> {
        self.0.take().map(|node| seed.deserialize(node)).transpose()
    }
}

/// The elements of a tuple or array, each of which must be a `u8`.
struct ByteArray<'r> {
    run: &'r Run<'r>,
    left: usize,
}

impl<'de> SeqAccess<'de> for ByteArray<'_> {
    type Error = TraceError;

    fn next_element_seed<S: DeserializeSeed<'de>>(
        &mut self,
        seed: S,
    ) -> Result<Option<S::Value>, TraceError> {
        if self.left == 0 {
            return Ok(None);
        }
        self.left -= 1;
        seed.deserialize(ByteElement(self.run)).map(Some)
    }
}

/// One element of a [`ByteArray`].
struct ByteElement<'r>(&'r Run<'r>);

impl<'de> Deserializer<'de> for ByteElement<'_> {
    type Error = TraceError;

    fn deserialize_u8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, TraceError> {
        visitor.visit_u8(1)
    }

    fn deserialize_any<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, TraceError> {
        self.0.trace.borrow_mut().answer = Some(Answer::Uninferred(
            BigQuerySchemaInferenceErrorKind::NoColumnType,
        ));
        Err(TraceError::Stop)
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u16 u32 u64 u128 f32 f64 char str string bytes byte_buf
        option unit unit_struct newtype_struct seq tuple tuple_struct map struct enum
        identifier ignored_any
    }
}

/// The entries of a struct on the route: its one key, the value under it, and the key once
/// more after a value at the field, for serde to name the field the key belongs to.
struct RouteMap<'r> {
    node: Node<'r>,
    state: RouteState,
}

#[derive(Clone, Copy, PartialEq)]
enum RouteState {
    Key,
    Value,
    Again,
    Done,
}

impl<'de> MapAccess<'de> for RouteMap<'_> {
    type Error = TraceError;

    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, TraceError> {
        match self.state {
            RouteState::Key => self.state = RouteState::Value,
            RouteState::Again => self.state = RouteState::Done,
            RouteState::Value | RouteState::Done => return Err(TraceError::Stop),
        }
        let key = match self.node.run.route[self.node.depth] {
            Key::Name(name) => seed.deserialize(BorrowedStrDeserializer::new(name)),
            Key::Position(position) => seed.deserialize(U64Deserializer::new(position)),
        };
        if key.is_err() && self.node.depth + 1 == self.node.run.route.len() {
            self.node.run.trace.borrow_mut().answer = Some(Answer::Ignored);
        }
        key.map(Some)
    }

    fn next_value_seed<S: DeserializeSeed<'de>>(
        &mut self,
        seed: S,
    ) -> Result<S::Value, TraceError> {
        if self.state != RouteState::Value {
            return Err(TraceError::Stop);
        }
        let value = Node {
            run: self.node.run,
            depth: self.node.depth + 1,
        };
        let produced = seed.deserialize(value);
        self.state = RouteState::Done;
        if produced.is_ok()
            && value.at_field()
            && matches!(self.node.run.route[self.node.depth], Key::Name(_))
        {
            self.state = RouteState::Again;
            self.node.run.repeated.set(true);
        }
        produced
    }
}

/// A map or struct with no entries.
struct NoEntries;

impl<'de> MapAccess<'de> for NoEntries {
    type Error = TraceError;

    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        _seed: K,
    ) -> Result<Option<K::Value>, TraceError> {
        Ok(None)
    }

    fn next_value_seed<S: DeserializeSeed<'de>>(
        &mut self,
        _seed: S,
    ) -> Result<S::Value, TraceError> {
        Err(TraceError::Stop)
    }
}

/// One variant of an enum, offered by name; taking it as a unit variant is accepting it.
struct VariantOffer<'r> {
    run: &'r Run<'r>,
    variant: &'static str,
}

impl<'de> EnumAccess<'de> for VariantOffer<'_> {
    type Error = TraceError;
    type Variant = Self;

    fn variant_seed<S: DeserializeSeed<'de>>(
        self,
        seed: S,
    ) -> Result<(S::Value, Self), TraceError> {
        let variant = seed.deserialize(BorrowedStrDeserializer::new(self.variant))?;
        Ok((variant, self))
    }
}

impl<'de> VariantAccess<'de> for VariantOffer<'_> {
    type Error = TraceError;

    fn unit_variant(self) -> Result<(), TraceError> {
        self.run.trace.borrow_mut().accepted = true;
        Ok(())
    }

    fn newtype_variant_seed<S: DeserializeSeed<'de>>(
        self,
        _seed: S,
    ) -> Result<S::Value, TraceError> {
        Err(TraceError::Stop)
    }

    fn tuple_variant<V: Visitor<'de>>(
        self,
        _len: usize,
        _visitor: V,
    ) -> Result<V::Value, TraceError> {
        Err(TraceError::Stop)
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        _fields: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, TraceError> {
        Err(TraceError::Stop)
    }
}

#[cfg(test)]
mod tests {
    use crate::db::fake::{ORDERS, SHOP};
    use crate::errors::{
        BigQueryError, BigQuerySchemaInferenceError, BigQuerySchemaInferenceErrorKind,
    };
    use crate::schema::declaration::{
        BigQuerySchemaColumns, BigQueryTableDeclarationDraft, ColumnKind,
    };
    use crate::{
        BigQueryDate, BigQueryDecimal, BigQueryFieldMode, BigQueryInterval, BigQueryJson,
        BigQueryRange, BigQueryRangeElementType, BigQuerySchemaColumn,
        BigQuerySchemaColumnsBuilder, BigQueryTableDeclaration, BigQueryTimestamp,
    };
    use serde::de::DeserializeOwned;
    use serde::Deserialize;
    use std::collections::HashMap;

    const COLUMNS: BigQuerySchemaColumnsBuilder = BigQuerySchemaColumnsBuilder;

    fn by_hand(columns: Vec<BigQuerySchemaColumn>) -> BigQuerySchemaColumns {
        columns.into()
    }

    fn uninferred(
        name: &str,
        path: &str,
        kind: BigQuerySchemaInferenceErrorKind,
        mode: BigQueryFieldMode,
    ) -> BigQuerySchemaColumn {
        BigQuerySchemaColumn::inferred(
            name.into(),
            ColumnKind::Uninferred(BigQuerySchemaInferenceError::new(path.into(), kind)),
            mode,
        )
    }

    /// The path and kind of the error a declaration of `T`'s inferred columns is refused with.
    fn refusal_of<T: DeserializeOwned>() -> (String, BigQuerySchemaInferenceErrorKind) {
        let mut draft = BigQueryTableDeclarationDraft::new(SHOP.table(ORDERS));
        draft.columns = COLUMNS.from_type::<T>();
        match BigQueryTableDeclaration::try_from(draft) {
            Err(BigQueryError::SchemaInferenceError(error)) => (error.path, error.kind),
            other => panic!("expected a schema inference error, got {other:?}"),
        }
    }

    #[derive(Deserialize)]
    #[allow(dead_code, reason = "only its serde shape is read")]
    struct Address {
        city: String,
        street: Option<String>,
    }

    #[test]
    fn scalars_and_modes_follow_the_type_mapping() {
        #[derive(Deserialize)]
        enum Status {
            Placed,
            Shipped,
        }
        #[derive(Deserialize)]
        #[allow(dead_code, reason = "only its serde shape is read")]
        struct Row {
            id: i64,
            quantity: u8,
            ratio: Option<f32>,
            paid: bool,
            initial: char,
            status: Status,
            tags: Vec<String>,
            labels: Option<Vec<String>>,
            payload: Vec<u8>,
            digest: [u8; 4],
            #[serde(with = "serde_bytes")]
            blob: Vec<u8>,
            chunks: Vec<Vec<u8>>,
        }
        assert_eq!(
            COLUMNS.from_type::<Row>(),
            by_hand(vec![
                COLUMNS.field("id").int64().required(),
                COLUMNS.field("quantity").int64().required(),
                COLUMNS.field("ratio").float64(),
                COLUMNS.field("paid").bool().required(),
                COLUMNS.field("initial").string().required(),
                COLUMNS.field("status").string().required(),
                COLUMNS.field("tags").string().repeated(),
                COLUMNS.field("labels").string().repeated(),
                COLUMNS.field("payload").bytes().required(),
                COLUMNS.field("digest").bytes().required(),
                COLUMNS.field("blob").bytes().required(),
                COLUMNS.field("chunks").bytes().repeated(),
            ])
        );
    }

    #[test]
    fn a_struct_is_a_record_of_its_fields() {
        #[derive(Deserialize)]
        #[allow(dead_code, reason = "only its serde shape is read")]
        struct Item {
            sku: String,
            quantity: i64,
        }
        #[derive(Deserialize)]
        #[allow(dead_code, reason = "only its serde shape is read")]
        struct Order {
            shipping: Option<Address>,
            billing: Address,
            items: Vec<Item>,
        }
        let address = |record: BigQuerySchemaColumnsBuilder| {
            record.fields([
                record.field("city").string().required(),
                record.field("street").string(),
            ])
        };
        assert_eq!(
            COLUMNS.from_type::<Order>(),
            by_hand(vec![
                COLUMNS.field("shipping").record(address),
                COLUMNS.field("billing").record(address).required(),
                COLUMNS
                    .field("items")
                    .record(|item| {
                        item.fields([
                            item.field("sku").string().required(),
                            item.field("quantity").int64().required(),
                        ])
                    })
                    .repeated(),
            ])
        );
    }

    #[test]
    fn serde_names_skips_and_defaults_are_followed() {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        #[allow(dead_code, reason = "only its serde shape is read")]
        struct Row {
            order_id: i64,
            #[serde(rename = "Customer")]
            customer_name: String,
            #[serde(skip)]
            cached: Option<String>,
            #[serde(default)]
            retry_count: i64,
        }
        assert_eq!(
            COLUMNS.from_type::<Row>(),
            by_hand(vec![
                COLUMNS.field("orderId").int64().required(),
                COLUMNS.field("Customer").string().required(),
                COLUMNS.field("retryCount").int64().required(),
            ])
        );
    }

    #[test]
    fn an_alias_is_not_a_column() {
        #[derive(Deserialize)]
        #[allow(dead_code, reason = "only its serde shape is read")]
        struct Row {
            // serde lists a field's names sorted, so `customer` comes before the field's own
            // name and `zone` after it.
            #[serde(alias = "customer")]
            customer_name: String,
            #[serde(alias = "zone")]
            region: Option<String>,
            shipping: Address,
        }
        assert_eq!(
            COLUMNS.from_type::<Row>(),
            by_hand(vec![
                COLUMNS.field("customer_name").string().required(),
                COLUMNS.field("region").string(),
                COLUMNS
                    .field("shipping")
                    .record(|address| {
                        address.fields([
                            address.field("city").string().required(),
                            address.field("street").string(),
                        ])
                    })
                    .required(),
            ])
        );
    }

    #[test]
    fn wrappers_and_with_modules_are_their_own_types() {
        #[derive(Deserialize)]
        #[allow(dead_code, reason = "only its serde shape is read")]
        struct Row {
            placed_at: BigQueryTimestamp,
            placed_on: Option<BigQueryDate>,
            #[serde(with = "crate::serialize_as_time")]
            opens: jiff::civil::Time,
            #[serde(with = "crate::serialize_as_optional_datetime")]
            local: Option<jiff::civil::DateTime>,
            total: BigQueryDecimal<String>,
            #[serde(with = "crate::serialize_as_optional_decimal")]
            discount: Option<String>,
            document: BigQueryJson<serde_json::Value>,
            wait: BigQueryInterval,
            valid: Option<BigQueryRange<BigQueryDate>>,
            window: BigQueryRange<BigQueryTimestamp>,
        }
        assert_eq!(
            COLUMNS.from_type::<Row>(),
            by_hand(vec![
                COLUMNS.field("placed_at").timestamp().required(),
                COLUMNS.field("placed_on").date(),
                COLUMNS.field("opens").time().required(),
                COLUMNS.field("local").datetime(),
                COLUMNS.field("total").numeric().required(),
                COLUMNS.field("discount").numeric(),
                COLUMNS.field("document").json().required(),
                COLUMNS.field("wait").interval().required(),
                COLUMNS.field("valid").range(BigQueryRangeElementType::Date),
                COLUMNS
                    .field("window")
                    .range(BigQueryRangeElementType::Timestamp)
                    .required(),
            ])
        );
    }

    #[test]
    fn text_fields_take_the_type_whose_bigquery_text_they_read() {
        #[derive(Deserialize)]
        #[allow(dead_code, reason = "only its serde shape is read")]
        struct Row {
            name: String,
            placed_at: jiff::Timestamp,
            opens: Option<jiff::civil::Time>,
            link: url::Url,
            placed_on: jiff::civil::Date,
            local: Option<jiff::civil::DateTime>,
        }
        assert_eq!(
            COLUMNS.from_type::<Row>(),
            by_hand(vec![
                COLUMNS.field("name").string().required(),
                COLUMNS.field("placed_at").timestamp().required(),
                COLUMNS.field("opens").time(),
                COLUMNS.field("link").string().required(),
                uninferred(
                    "placed_on",
                    "placed_on",
                    BigQuerySchemaInferenceErrorKind::DateOrDateTime,
                    BigQueryFieldMode::Required,
                ),
                uninferred(
                    "local",
                    "local",
                    BigQuerySchemaInferenceErrorKind::DateOrDateTime,
                    BigQueryFieldMode::Nullable,
                ),
            ])
        );
    }

    #[test]
    fn with_replaces_a_nested_column_and_keeps_the_others() {
        #[derive(Deserialize)]
        #[allow(dead_code, reason = "only its serde shape is read")]
        struct Order {
            id: i64,
            shipping: Option<Address>,
        }
        let columns = COLUMNS
            .from_type::<Order>()
            .with("shipping.city", |city| city.string_with_max_length(64))
            .with("id", |id| id.description("The order number"));
        assert_eq!(
            columns,
            by_hand(vec![
                COLUMNS
                    .field("id")
                    .int64()
                    .required()
                    .description("The order number"),
                COLUMNS.field("shipping").record(|address| {
                    address.fields([
                        address.field("city").string_with_max_length(64).required(),
                        address.field("street").string(),
                    ])
                }),
            ])
        );
    }

    #[test]
    fn with_on_a_record_settles_its_fields() {
        #[derive(Deserialize)]
        #[allow(dead_code, reason = "only its serde shape is read")]
        struct Payload {
            kind: String,
            attributes: serde_json::Value,
        }
        #[derive(Deserialize)]
        #[allow(dead_code, reason = "only its serde shape is read")]
        struct Event {
            payload: Payload,
            context: Option<Payload>,
        }
        let columns = COLUMNS
            .from_type::<Event>()
            .with("payload", |payload| payload.json())
            .with("context", |context| {
                context.record(|fields| fields.fields([fields.field("kind").string()]))
            });
        assert_eq!(
            columns,
            by_hand(vec![
                COLUMNS.field("payload").json().required(),
                COLUMNS
                    .field("context")
                    .record(|fields| fields.fields([fields.field("kind").string()])),
            ])
        );
    }

    #[test]
    fn inferred_columns_match_the_schema_doc_example() {
        #[derive(Deserialize)]
        #[allow(dead_code, reason = "only its serde shape is read")]
        struct Order {
            id: i64,
            customer: String,
            placed_at: jiff::Timestamp,
        }
        assert_eq!(
            COLUMNS.from_type::<Order>(),
            by_hand(vec![
                COLUMNS.field("id").int64().required(),
                COLUMNS.field("customer").string().required(),
                COLUMNS.field("placed_at").timestamp().required(),
            ])
        );
    }

    #[test]
    fn a_field_without_one_column_type_is_uninferred_at_its_path() {
        use BigQueryFieldMode::{Repeated, Required};
        use BigQuerySchemaInferenceErrorKind::*;

        #[derive(Deserialize)]
        #[allow(dead_code, reason = "only its serde shape is read")]
        enum Shape {
            Point,
            Circle { radius: f64 },
        }
        #[derive(Deserialize)]
        #[allow(dead_code, reason = "only its serde shape is read")]
        struct Category {
            name: String,
            children: Vec<Category>,
        }
        #[derive(Deserialize)]
        #[allow(dead_code, reason = "only its serde shape is read")]
        struct Payload {
            attributes: serde_json::Value,
        }
        #[derive(Deserialize)]
        #[allow(dead_code, reason = "only its serde shape is read")]
        struct Moved {
            // A RECORD's value cannot be produced without its fields, so serde never names the
            // field `address` belongs to.
            #[serde(alias = "address")]
            shipping: Address,
        }
        #[derive(Deserialize)]
        #[allow(dead_code, reason = "only its serde shape is read")]
        struct Row {
            attributes: serde_json::Value,
            labels: HashMap<String, String>,
            shape: Shape,
            matrix: Vec<Vec<i64>>,
            readings: Vec<Option<i64>>,
            point: (i64, i64),
            span: BigQueryRange<i64>,
            category: Category,
            payload: Option<Payload>,
            moved: Moved,
        }
        assert_eq!(
            COLUMNS.from_type::<Row>(),
            by_hand(vec![
                uninferred("attributes", "attributes", DynamicValue, Required),
                uninferred("labels", "labels", UnknownKeys, Required),
                uninferred("shape", "shape", EnumWithData, Required),
                uninferred("matrix", "matrix", NestedArray, Repeated),
                uninferred("readings", "readings", NullableArrayElement, Repeated),
                uninferred("point", "point", NoColumnType, Required),
                uninferred("span", "span", RangeElement, Required),
                COLUMNS
                    .field("category")
                    .record(|category| {
                        vec![
                            category.field("name").string().required(),
                            uninferred("children", "category.children", Recursive, Repeated),
                        ]
                    })
                    .required(),
                COLUMNS.field("payload").record(|_| {
                    vec![uninferred(
                        "attributes",
                        "payload.attributes",
                        DynamicValue,
                        Required,
                    )]
                }),
                uninferred("moved", "moved", UnresolvedAlias, Required),
            ])
        );
    }

    #[test]
    fn a_row_type_that_is_not_a_struct_is_refused() {
        use BigQuerySchemaInferenceErrorKind::*;

        #[derive(Deserialize)]
        #[allow(dead_code, reason = "only its serde shape is read")]
        struct Flattened {
            id: i64,
            #[serde(flatten)]
            rest: HashMap<String, String>,
        }
        assert_eq!(
            [
                refusal_of::<Flattened>(),
                refusal_of::<HashMap<String, i64>>(),
                refusal_of::<serde_json::Value>(),
                refusal_of::<(i64, String)>(),
            ],
            [
                (String::new(), UnknownKeys),
                (String::new(), UnknownKeys),
                (String::new(), DynamicValue),
                (String::new(), NotAStruct),
            ]
        );
    }
}
