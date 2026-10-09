//! The schemas the fake serves: the columns a test declares, checked, and the Arrow layout a
//! read session or an inline query result has for them.

use crate::types::kind::{ARROW_EXTENSION_NAME, GOOGLE_SQL_TYPE};
use crate::{
    BigQueryDecimalParams, BigQueryFieldMode, BigQueryFieldSchema, BigQueryFieldType,
    BigQueryRangeElementType, BigQueryResult, BigQuerySchemaColumns, BigQueryTableSchema,
};
use arrow_schema::{DataType, Field, Fields, IntervalUnit, Schema, TimeUnit};
use std::collections::HashMap;

/// NUMERIC's precision and scale in Arrow when the column declares none.
const NUMERIC_DEFAULT: BigQueryDecimalParams = BigQueryDecimalParams {
    precision: 38,
    scale: 9,
};
/// BIGNUMERIC's precision and scale in Arrow when the column declares none.
const BIGNUMERIC_DEFAULT: BigQueryDecimalParams = BigQueryDecimalParams {
    precision: 76,
    scale: 38,
};

impl BigQuerySchemaColumns {
    /// The table schema these columns declare, checked as `.plan()` and `.sync()` check them.
    ///
    /// # Errors
    /// As [`checked`](Self::checked).
    pub(super) fn table_schema(self) -> BigQueryResult<BigQueryTableSchema> {
        Ok(BigQueryTableSchema {
            fields: self
                .checked()?
                .into_iter()
                .map(|column| column.field)
                .collect(),
        })
    }
}

impl BigQueryTableSchema {
    /// The Arrow schema BigQuery sends for a table of this schema, in a read session or an
    /// inline query result: the layout
    /// [`FieldKind::from_arrow`](crate::types::kind::FieldKind::from_arrow) reads back.
    pub(crate) fn arrow_read_schema(&self) -> Schema {
        Schema::new(
            self.fields
                .iter()
                .map(BigQueryFieldSchema::arrow_read_field)
                .collect::<Vec<_>>(),
        )
    }
}

impl BigQueryFieldSchema {
    /// The Arrow field of this column. A REPEATED column is a non-null `List` of `item`, with
    /// the element's metadata on the list field, as BigQuery sends it.
    fn arrow_read_field(&self) -> Field {
        let (data_type, metadata) = self.field_type.arrow_read_type();
        match self.mode {
            BigQueryFieldMode::Repeated => Field::new_list(
                self.name.clone(),
                Field::new("item", data_type, true),
                false,
            ),
            BigQueryFieldMode::Nullable => Field::new(self.name.clone(), data_type, true),
            BigQueryFieldMode::Required => Field::new(self.name.clone(), data_type, false),
        }
        .with_metadata(metadata)
    }
}

impl BigQueryFieldType {
    /// The Arrow type of one value of this type, and the field metadata that tells the types
    /// sharing an Arrow type apart.
    pub(super) fn arrow_read_type(&self) -> (DataType, HashMap<String, String>) {
        let extension =
            |name: &str| HashMap::from([(ARROW_EXTENSION_NAME.to_string(), name.to_string())]);
        match self {
            BigQueryFieldType::Int64 => (DataType::Int64, HashMap::new()),
            BigQueryFieldType::Float64 => (DataType::Float64, HashMap::new()),
            BigQueryFieldType::Bool => (DataType::Boolean, HashMap::new()),
            BigQueryFieldType::String { .. } => (DataType::Utf8, HashMap::new()),
            BigQueryFieldType::Bytes { .. } => (DataType::Binary, HashMap::new()),
            BigQueryFieldType::Json => (DataType::Utf8, extension("google:sqlType:json")),
            BigQueryFieldType::Geography => {
                let mut metadata = extension("google:sqlType:geography");
                metadata.insert(
                    "ARROW:extension:metadata".to_string(),
                    r#"{"encoding": "WKT"}"#.to_string(),
                );
                (DataType::Utf8, metadata)
            }
            BigQueryFieldType::Date => (DataType::Date32, HashMap::new()),
            BigQueryFieldType::Time => (DataType::Time64(TimeUnit::Microsecond), HashMap::new()),
            BigQueryFieldType::DateTime => (
                DataType::Timestamp(TimeUnit::Microsecond, None),
                extension("google:sqlType:datetime"),
            ),
            BigQueryFieldType::Timestamp => (
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                HashMap::new(),
            ),
            // BigQuery declares scales of at most 38, which fit Arrow's i8.
            BigQueryFieldType::Numeric(params) => {
                let params = params.unwrap_or(NUMERIC_DEFAULT);
                let scale = i8::try_from(params.scale).unwrap_or(i8::MAX);
                (
                    DataType::Decimal128(params.precision, scale),
                    HashMap::new(),
                )
            }
            BigQueryFieldType::BigNumeric(params) => {
                let params = params.unwrap_or(BIGNUMERIC_DEFAULT);
                let scale = i8::try_from(params.scale).unwrap_or(i8::MAX);
                (
                    DataType::Decimal256(params.precision, scale),
                    HashMap::new(),
                )
            }
            BigQueryFieldType::Interval => (
                DataType::Interval(IntervalUnit::MonthDayNano),
                extension("google:sqlType:interval"),
            ),
            BigQueryFieldType::Range(element) => {
                let bound = element.arrow_read_type();
                (
                    DataType::Struct(Fields::from(vec![
                        Field::new("start", bound.clone(), true),
                        Field::new("end", bound, true),
                    ])),
                    HashMap::from([(GOOGLE_SQL_TYPE.to_string(), "range".to_string())]),
                )
            }
            BigQueryFieldType::Struct(fields) => (
                DataType::Struct(
                    fields
                        .iter()
                        .map(BigQueryFieldSchema::arrow_read_field)
                        .collect(),
                ),
                HashMap::new(),
            ),
        }
    }
}

impl BigQueryRangeElementType {
    /// The Arrow type of a bound of a RANGE of this element.
    fn arrow_read_type(self) -> DataType {
        match self {
            BigQueryRangeElementType::Date => DataType::Date32,
            BigQueryRangeElementType::DateTime => DataType::Timestamp(TimeUnit::Microsecond, None),
            BigQueryRangeElementType::Timestamp => {
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::testkit::field;
    use proptest::prelude::*;

    fn mode() -> impl Strategy<Value = BigQueryFieldMode> {
        prop_oneof![
            Just(BigQueryFieldMode::Nullable),
            Just(BigQueryFieldMode::Required),
            Just(BigQueryFieldMode::Repeated),
        ]
    }

    /// Every type, nested STRUCTs included, without the parameters Arrow cannot carry.
    fn field_type() -> impl Strategy<Value = BigQueryFieldType> {
        let leaf = prop_oneof![
            Just(BigQueryFieldType::Int64),
            Just(BigQueryFieldType::Float64),
            Just(BigQueryFieldType::Bool),
            Just(BigQueryFieldType::String { max_length: None }),
            Just(BigQueryFieldType::Bytes { max_length: None }),
            Just(BigQueryFieldType::Date),
            Just(BigQueryFieldType::Time),
            Just(BigQueryFieldType::DateTime),
            Just(BigQueryFieldType::Timestamp),
            Just(BigQueryFieldType::Numeric(None)),
            Just(BigQueryFieldType::BigNumeric(None)),
            Just(BigQueryFieldType::Geography),
            Just(BigQueryFieldType::Json),
            Just(BigQueryFieldType::Interval),
            Just(BigQueryFieldType::Range(BigQueryRangeElementType::Date)),
            Just(BigQueryFieldType::Range(BigQueryRangeElementType::DateTime)),
            Just(BigQueryFieldType::Range(
                BigQueryRangeElementType::Timestamp
            )),
        ];
        leaf.prop_recursive(2, 12, 4, |inner| {
            columns(inner).prop_map(BigQueryFieldType::Struct)
        })
    }

    fn columns(
        field_type: impl Strategy<Value = BigQueryFieldType>,
    ) -> impl Strategy<Value = Vec<BigQueryFieldSchema>> {
        proptest::collection::vec((field_type, mode()), 1..5).prop_map(|columns| {
            columns
                .into_iter()
                .enumerate()
                .map(|(index, (field_type, mode))| {
                    field(&format!("column_{index}"), field_type, mode)
                })
                .collect()
        })
    }

    proptest! {
        #[test]
        fn the_read_layout_reads_back_as_its_schema(fields in columns(field_type())) {
            let schema = BigQueryTableSchema { fields };
            let read_back = BigQueryTableSchema::from_arrow(&schema.arrow_read_schema());
            prop_assert_eq!(read_back.ok(), Some(schema));
        }
    }
}
