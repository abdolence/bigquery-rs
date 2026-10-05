//! A table schema compiled for writing: the proto2 descriptor Storage Write is sent in
//! `writer_schema`, and per field the precomputed key bytes and the wire form Storage Write
//! accepts for its BigQuery type.

use crate::types::kind::FieldKind;
use crate::{
    BigQueryFieldMode, BigQueryFieldSchema, BigQueryFieldType, BigQueryRangeElementType,
    BigQueryTableSchema,
};
use gcloud_sdk::prost::encoding::{encode_key, key_len, WireType};
use gcloud_sdk::prost_types::field_descriptor_proto::{Label, Type as ProtoType};
use gcloud_sdk::prost_types::{DescriptorProto, FieldDescriptorProto};
use std::collections::HashMap;

/// The name of the root message in every descriptor the writer sends.
pub(crate) const ROOT_MESSAGE: &str = "row";

/// The CDC pseudo-column that says whether a row is an upsert or a delete.
pub(crate) const CHANGE_TYPE_COLUMN: &str = "_CHANGE_TYPE";
/// The CDC pseudo-column that orders changes to one primary key.
pub(crate) const CHANGE_SEQUENCE_NUMBER_COLUMN: &str = "_CHANGE_SEQUENCE_NUMBER";

/// One protobuf field key, varint-encoded once at compile time.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Key {
    bytes: [u8; 5],
    len: u8,
}

impl Key {
    fn new(number: u32, wire_type: WireType) -> Key {
        let mut bytes = [0u8; 5];
        let len = key_len(number);
        encode_key(number, wire_type, &mut &mut bytes[..len]);
        Key {
            bytes,
            len: len as u8,
        }
    }

    #[inline]
    pub(crate) fn get(&self) -> &[u8] {
        &self.bytes[..usize::from(self.len)]
    }
}

/// How one field of a message is written.
#[derive(Debug)]
pub(crate) struct FieldPlan {
    pub(crate) kind: FieldKind,
    pub(crate) required: bool,
    pub(crate) repeated: bool,
    /// The key of one occurrence, with the element's wire type.
    pub(crate) key: Key,
    /// The key of a packed run, wire type 2.
    pub(crate) packed_key: Key,
    /// The message of a STRUCT or RANGE field.
    pub(crate) nested: Option<Box<MessagePlan>>,
}

/// How one message is written: the row, a STRUCT or a RANGE.
#[derive(Debug)]
pub(crate) struct MessagePlan {
    /// The index of this message's key cache in the encoder.
    pub(crate) id: usize,
    pub(crate) names: Vec<String>,
    pub(crate) fields: Vec<FieldPlan>,
    by_name: HashMap<String, usize>,
    /// Bit `i` set for each REQUIRED field `i < 64`; a REQUIRED field beyond 64 is left to
    /// BigQuery's own per-row check.
    pub(crate) required: u64,
}

impl MessagePlan {
    pub(crate) fn index_of(&self, name: &str) -> Option<usize> {
        self.by_name.get(name).copied()
    }
}

/// The keys of the two CDC pseudo-columns, which follow the table's own fields.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CdcKeys {
    pub(crate) change_type: Key,
    pub(crate) sequence_number: Key,
}

/// A table schema compiled for writing, once per schema the writer sees.
///
/// Every batch is encoded against exactly one plan and sent with that plan's descriptor
/// whenever the connection last saw another, so the plan's identity (its `Arc`) is what the
/// writer compares, never the schema.
#[derive(Debug)]
pub(crate) struct WritePlan {
    schema: BigQueryTableSchema,
    descriptor: DescriptorProto,
    pub(crate) root: MessagePlan,
    pub(crate) messages: usize,
    pub(crate) cdc: Option<CdcKeys>,
}

impl FieldKind {
    /// The proto type each BigQuery type is written as. Storage Write refuses FLOAT64 or BOOL as
    /// a string and TIMESTAMP as a double or as text ending in `+00`, so none of those forms is
    /// used.
    fn proto_type(self) -> ProtoType {
        match self {
            FieldKind::Int64 | FieldKind::Time | FieldKind::DateTime | FieldKind::Timestamp => {
                ProtoType::Int64
            }
            FieldKind::Float64 => ProtoType::Double,
            FieldKind::Bool => ProtoType::Bool,
            FieldKind::Date => ProtoType::Int32,
            FieldKind::Bytes | FieldKind::Numeric | FieldKind::BigNumeric => ProtoType::Bytes,
            FieldKind::String | FieldKind::Geography | FieldKind::Json | FieldKind::Interval => {
                ProtoType::String
            }
            FieldKind::Struct | FieldKind::Range => ProtoType::Message,
        }
    }

    fn wire_type(self) -> WireType {
        match self.proto_type() {
            ProtoType::Int64 | ProtoType::Int32 | ProtoType::Bool => WireType::Varint,
            ProtoType::Double => WireType::SixtyFourBit,
            _ => WireType::LengthDelimited,
        }
    }
}

impl BigQueryRangeElementType {
    fn field_named(self, name: &str) -> BigQueryFieldSchema {
        let field_type = match self {
            BigQueryRangeElementType::Date => BigQueryFieldType::Date,
            BigQueryRangeElementType::DateTime => BigQueryFieldType::DateTime,
            BigQueryRangeElementType::Timestamp => BigQueryFieldType::Timestamp,
        };
        BigQueryFieldSchema {
            name: name.to_string(),
            field_type,
            mode: BigQueryFieldMode::Nullable,
            description: None,
            default_value_expression: None,
        }
    }
}

/// The field number of the `index`-th field: schema order, from 1.
fn field_number(index: usize) -> u32 {
    u32::try_from(index + 1).unwrap_or(u32::MAX)
}

struct Compiler {
    nested: Vec<DescriptorProto>,
    messages: usize,
}

impl Compiler {
    fn compile(
        &mut self,
        fields: &[BigQueryFieldSchema],
    ) -> (MessagePlan, Vec<FieldDescriptorProto>) {
        let id = self.messages;
        self.messages += 1;
        let mut plan = MessagePlan {
            id,
            names: Vec::with_capacity(fields.len()),
            fields: Vec::with_capacity(fields.len()),
            by_name: HashMap::with_capacity(fields.len()),
            required: 0,
        };
        let mut descriptors = Vec::with_capacity(fields.len());
        for (i, field) in fields.iter().enumerate() {
            let kind = FieldKind::from(&field.field_type);
            let number = field_number(i);
            let mut descriptor = FieldDescriptorProto {
                name: Some(field.name.clone()),
                number: i32::try_from(number).ok(),
                label: Some(
                    match field.mode {
                        BigQueryFieldMode::Required => Label::Required,
                        BigQueryFieldMode::Repeated => Label::Repeated,
                        BigQueryFieldMode::Nullable => Label::Optional,
                    }
                    .into(),
                ),
                r#type: Some(kind.proto_type().into()),
                ..Default::default()
            };
            let children = match &field.field_type {
                BigQueryFieldType::Struct(children) => Some(children.clone()),
                BigQueryFieldType::Range(element) => Some(vec![
                    element.field_named("start"),
                    element.field_named("end"),
                ]),
                _ => None,
            };
            let nested = children.map(|children| {
                let (nested, nested_fields) = self.compile(&children);
                // Storage Write wants every nested type flattened into the root, so names only
                // have to be unique there; numbering them keeps them valid identifiers whatever
                // the column names are.
                let type_name = format!("{ROOT_MESSAGE}_{}", nested.id);
                self.nested.push(DescriptorProto {
                    name: Some(type_name.clone()),
                    field: nested_fields,
                    ..Default::default()
                });
                descriptor.type_name = Some(type_name);
                Box::new(nested)
            });
            let required = field.mode == BigQueryFieldMode::Required;
            if required && i < 64 {
                plan.required |= 1 << i;
            }
            plan.by_name.insert(field.name.clone(), i);
            plan.names.push(field.name.clone());
            plan.fields.push(FieldPlan {
                kind,
                required,
                repeated: field.mode == BigQueryFieldMode::Repeated,
                key: Key::new(number, kind.wire_type()),
                packed_key: Key::new(number, WireType::LengthDelimited),
                nested,
            });
            descriptors.push(descriptor);
        }
        (plan, descriptors)
    }
}

fn pseudo_column(name: &str, number: u32) -> FieldDescriptorProto {
    FieldDescriptorProto {
        name: Some(name.to_string()),
        number: i32::try_from(number).ok(),
        label: Some(Label::Optional.into()),
        r#type: Some(ProtoType::String.into()),
        ..Default::default()
    }
}

impl WritePlan {
    /// Compiles `schema`. With `cdc`, the descriptor also declares `_CHANGE_TYPE` and
    /// `_CHANGE_SEQUENCE_NUMBER` after the table's own fields, as optional strings.
    pub(crate) fn new(schema: &BigQueryTableSchema, cdc: bool) -> WritePlan {
        let mut compiler = Compiler {
            nested: Vec::new(),
            messages: 0,
        };
        let (root, mut fields) = compiler.compile(&schema.fields);
        let cdc = cdc.then(|| {
            let change_type = field_number(schema.fields.len());
            let sequence_number = change_type.saturating_add(1);
            fields.push(pseudo_column(CHANGE_TYPE_COLUMN, change_type));
            fields.push(pseudo_column(
                CHANGE_SEQUENCE_NUMBER_COLUMN,
                sequence_number,
            ));
            CdcKeys {
                change_type: Key::new(change_type, WireType::LengthDelimited),
                sequence_number: Key::new(sequence_number, WireType::LengthDelimited),
            }
        });
        let descriptor = DescriptorProto {
            name: Some(ROOT_MESSAGE.to_string()),
            field: fields,
            nested_type: compiler.nested,
            ..Default::default()
        };
        WritePlan {
            schema: schema.clone(),
            descriptor,
            root,
            messages: compiler.messages,
            cdc,
        }
    }

    /// The descriptor sent in `ProtoSchema.proto_descriptor`.
    pub(crate) fn descriptor(&self) -> &DescriptorProto {
        &self.descriptor
    }

    /// The schema this plan was compiled from.
    pub(crate) fn schema(&self) -> &BigQueryTableSchema {
        &self.schema
    }
}

/// Whether `new` relaxes a column `old` has as REQUIRED to NULLABLE, at any depth. An open
/// connection can go on rejecting NULLs for such a column for seconds to minutes after the
/// change, while a fresh one accepts them, so this is what makes a writer reconnect.
pub(crate) fn relaxes_a_required_field(
    old: &[BigQueryFieldSchema],
    new: &[BigQueryFieldSchema],
) -> bool {
    old.iter().any(|before| {
        new.iter()
            .find(|after| after.name == before.name)
            .is_some_and(|after| {
                let relaxed = before.mode == BigQueryFieldMode::Required
                    && after.mode == BigQueryFieldMode::Nullable;
                let nested = match (&before.field_type, &after.field_type) {
                    (BigQueryFieldType::Struct(b), BigQueryFieldType::Struct(a)) => {
                        relaxes_a_required_field(b, a)
                    }
                    _ => false,
                };
                relaxed || nested
            })
    })
}
