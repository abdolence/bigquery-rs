use crate::types::schema::BigQueryFieldType;
use arrow_schema::Metadata;
use arrow_schema::{DataType, IntervalUnit, TimeUnit};

pub(crate) const ARROW_EXTENSION_NAME: &str = "ARROW:extension:name";
pub(crate) const GOOGLE_SQL_TYPE: &str = "google:sqlType";

/// A BigQuery column type as both codecs see it: [`BigQueryFieldType`] without its parameters,
/// element types or fields.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum FieldKind {
    Int64,
    Float64,
    Bool,
    String,
    Bytes,
    Date,
    Time,
    DateTime,
    Timestamp,
    Numeric,
    BigNumeric,
    Geography,
    Json,
    Interval,
    Range,
    Struct,
}

impl FieldKind {
    /// Classifies a column of a Storage Read or inline query Arrow schema. `None` means the
    /// Arrow type is not one BigQuery sends. A `List` is not a kind: classify its element with
    /// [`FieldKind::from_arrow_list_item`].
    pub(crate) fn from_arrow(field: &arrow_schema::Field) -> Option<FieldKind> {
        classify(field.data_type(), field.metadata())
    }

    /// Classifies the element of a REPEATED column. BigQuery puts the element's metadata on the
    /// list field and none on `item`, so the type comes from `item` and the metadata from
    /// `list`.
    pub(crate) fn from_arrow_list_item(
        list: &arrow_schema::Field,
        item: &arrow_schema::Field,
    ) -> Option<FieldKind> {
        classify(item.data_type(), list.metadata())
    }

    /// The GoogleSQL name, as in `INT64`.
    pub(crate) fn name(self) -> &'static str {
        match self {
            FieldKind::Int64 => "INT64",
            FieldKind::Float64 => "FLOAT64",
            FieldKind::Bool => "BOOL",
            FieldKind::String => "STRING",
            FieldKind::Bytes => "BYTES",
            FieldKind::Date => "DATE",
            FieldKind::Time => "TIME",
            FieldKind::DateTime => "DATETIME",
            FieldKind::Timestamp => "TIMESTAMP",
            FieldKind::Numeric => "NUMERIC",
            FieldKind::BigNumeric => "BIGNUMERIC",
            FieldKind::Geography => "GEOGRAPHY",
            FieldKind::Json => "JSON",
            FieldKind::Interval => "INTERVAL",
            FieldKind::Range => "RANGE",
            FieldKind::Struct => "STRUCT",
        }
    }

    /// Whether a REPEATED column of this kind is written as one packed run.
    pub(crate) fn packable(self) -> bool {
        matches!(
            self,
            FieldKind::Int64
                | FieldKind::Float64
                | FieldKind::Bool
                | FieldKind::Date
                | FieldKind::Time
                | FieldKind::DateTime
                | FieldKind::Timestamp
        )
    }
}

fn classify(data_type: &DataType, metadata: &Metadata) -> Option<FieldKind> {
    let extension = metadata.get(ARROW_EXTENSION_NAME).map(String::as_str);
    Some(match data_type {
        DataType::Int64 => FieldKind::Int64,
        DataType::Float64 => FieldKind::Float64,
        DataType::Boolean => FieldKind::Bool,
        DataType::Utf8 => match extension {
            Some("google:sqlType:json") => FieldKind::Json,
            Some("google:sqlType:geography") => FieldKind::Geography,
            _ => FieldKind::String,
        },
        DataType::Binary => FieldKind::Bytes,
        DataType::Date32 => FieldKind::Date,
        DataType::Time64(TimeUnit::Microsecond) => FieldKind::Time,
        DataType::Timestamp(TimeUnit::Microsecond, None) => FieldKind::DateTime,
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => FieldKind::Timestamp,
        // A `NUMERIC(P, S)` column arrives at its own precision and scale, not at (38, 9).
        DataType::Decimal128(_, scale) if *scale >= 0 => FieldKind::Numeric,
        DataType::Decimal256(_, scale) if *scale >= 0 => FieldKind::BigNumeric,
        DataType::Interval(IntervalUnit::MonthDayNano) => FieldKind::Interval,
        DataType::Struct(_)
            if metadata.get(GOOGLE_SQL_TYPE).map(String::as_str) == Some("range") =>
        {
            FieldKind::Range
        }
        DataType::Struct(_) => FieldKind::Struct,
        _ => return None,
    })
}

impl From<&BigQueryFieldType> for FieldKind {
    fn from(field_type: &BigQueryFieldType) -> Self {
        match field_type {
            BigQueryFieldType::Int64 => FieldKind::Int64,
            BigQueryFieldType::Float64 => FieldKind::Float64,
            BigQueryFieldType::Numeric(_) => FieldKind::Numeric,
            BigQueryFieldType::BigNumeric(_) => FieldKind::BigNumeric,
            BigQueryFieldType::Bool => FieldKind::Bool,
            BigQueryFieldType::String { .. } => FieldKind::String,
            BigQueryFieldType::Bytes { .. } => FieldKind::Bytes,
            BigQueryFieldType::Date => FieldKind::Date,
            BigQueryFieldType::Time => FieldKind::Time,
            BigQueryFieldType::DateTime => FieldKind::DateTime,
            BigQueryFieldType::Timestamp => FieldKind::Timestamp,
            BigQueryFieldType::Geography => FieldKind::Geography,
            BigQueryFieldType::Json => FieldKind::Json,
            BigQueryFieldType::Interval => FieldKind::Interval,
            BigQueryFieldType::Range(_) => FieldKind::Range,
            BigQueryFieldType::Struct(_) => FieldKind::Struct,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::schema::{BigQueryDecimalParams, BigQueryRangeElementType};
    use arrow_schema::{DataType, Field, Fields, IntervalUnit, TimeUnit};
    use std::collections::HashMap;

    fn extension_metadata(name: &str) -> HashMap<String, String> {
        HashMap::from([("ARROW:extension:name".to_string(), name.to_string())])
    }

    fn field_of(dt: DataType) -> Field {
        Field::new("x", dt, true)
    }

    #[test]
    fn kind_from_arrow_field_recognises_extension_metadata() {
        let utc = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        let range = DataType::Struct(Fields::from(vec![
            Field::new("start", DataType::Date32, true),
            Field::new("end", DataType::Date32, true),
        ]));
        let range_meta = HashMap::from([("google:sqlType".to_string(), "range".to_string())]);
        let cases = [
            (field_of(DataType::Int64), Some(FieldKind::Int64)),
            (field_of(DataType::Float64), Some(FieldKind::Float64)),
            (field_of(DataType::Boolean), Some(FieldKind::Bool)),
            (field_of(DataType::Utf8), Some(FieldKind::String)),
            (
                field_of(DataType::Utf8).with_metadata(extension_metadata("google:sqlType:json")),
                Some(FieldKind::Json),
            ),
            (
                field_of(DataType::Utf8)
                    .with_metadata(extension_metadata("google:sqlType:geography")),
                Some(FieldKind::Geography),
            ),
            (field_of(DataType::Binary), Some(FieldKind::Bytes)),
            (field_of(DataType::Date32), Some(FieldKind::Date)),
            (
                field_of(DataType::Time64(TimeUnit::Microsecond)),
                Some(FieldKind::Time),
            ),
            (
                field_of(DataType::Timestamp(TimeUnit::Microsecond, None))
                    .with_metadata(extension_metadata("google:sqlType:datetime")),
                Some(FieldKind::DateTime),
            ),
            (field_of(utc), Some(FieldKind::Timestamp)),
            (
                field_of(DataType::Decimal128(38, 9)),
                Some(FieldKind::Numeric),
            ),
            (
                field_of(DataType::Decimal256(76, 38)),
                Some(FieldKind::BigNumeric),
            ),
            (
                field_of(DataType::Decimal128(10, 2)),
                Some(FieldKind::Numeric),
            ),
            (
                field_of(DataType::Decimal256(40, 0)),
                Some(FieldKind::BigNumeric),
            ),
            (field_of(DataType::Decimal128(10, -2)), None),
            (
                field_of(DataType::Interval(IntervalUnit::MonthDayNano))
                    .with_metadata(extension_metadata("google:sqlType:interval")),
                Some(FieldKind::Interval),
            ),
            (
                field_of(range.clone()).with_metadata(range_meta),
                Some(FieldKind::Range),
            ),
            (field_of(range), Some(FieldKind::Struct)),
            (field_of(DataType::Float16), None),
            (field_of(DataType::Int32), None),
            (
                field_of(DataType::Timestamp(TimeUnit::Nanosecond, None)),
                None,
            ),
        ];
        for (field, kind) in cases {
            assert_eq!(
                FieldKind::from_arrow(&field),
                kind,
                "{:?}",
                field.data_type()
            );
        }
    }

    #[test]
    fn list_element_kind_comes_from_the_list_field_metadata() {
        let item = Field::new("item", DataType::Utf8, true);
        let list = Field::new_list("tags", item.clone(), false)
            .with_metadata(extension_metadata("google:sqlType:json"));
        assert_eq!(
            FieldKind::from_arrow_list_item(&list, &item),
            Some(FieldKind::Json)
        );
        assert_eq!(
            FieldKind::from_arrow(&item),
            Some(FieldKind::String),
            "the item alone has no metadata"
        );

        let plain = Field::new_list("tags", item.clone(), false);
        assert_eq!(
            FieldKind::from_arrow_list_item(&plain, &item),
            Some(FieldKind::String)
        );

        let range_item = Field::new(
            "item",
            DataType::Struct(Fields::from(vec![
                Field::new("start", DataType::Date32, true),
                Field::new("end", DataType::Date32, true),
            ])),
            true,
        );
        let ranges = Field::new_list("rs", range_item.clone(), false).with_metadata(HashMap::from(
            [("google:sqlType".to_string(), "range".to_string())],
        ));
        assert_eq!(
            FieldKind::from_arrow_list_item(&ranges, &range_item),
            Some(FieldKind::Range)
        );
    }

    #[test]
    fn kind_names_packing_and_field_types() {
        assert_eq!(FieldKind::Int64.name(), "INT64");
        assert_eq!(FieldKind::BigNumeric.name(), "BIGNUMERIC");
        assert_eq!(FieldKind::DateTime.name(), "DATETIME");
        let packable: Vec<FieldKind> = ALL.into_iter().filter(|k| k.packable()).collect();
        assert_eq!(
            packable,
            [
                FieldKind::Int64,
                FieldKind::Float64,
                FieldKind::Bool,
                FieldKind::Date,
                FieldKind::Time,
                FieldKind::DateTime,
                FieldKind::Timestamp
            ]
        );
        assert_eq!(
            FieldKind::from(&BigQueryFieldType::Numeric(Some(BigQueryDecimalParams {
                precision: 3,
                scale: 1
            }))),
            FieldKind::Numeric
        );
        assert_eq!(
            FieldKind::from(&BigQueryFieldType::String {
                max_length: Some(3)
            }),
            FieldKind::String
        );
        assert_eq!(
            FieldKind::from(&BigQueryFieldType::Range(BigQueryRangeElementType::Date)),
            FieldKind::Range
        );
        assert_eq!(
            FieldKind::from(&BigQueryFieldType::Struct(vec![])),
            FieldKind::Struct
        );
    }

    const ALL: [FieldKind; 16] = [
        FieldKind::Int64,
        FieldKind::Float64,
        FieldKind::Bool,
        FieldKind::String,
        FieldKind::Bytes,
        FieldKind::Date,
        FieldKind::Time,
        FieldKind::DateTime,
        FieldKind::Timestamp,
        FieldKind::Numeric,
        FieldKind::BigNumeric,
        FieldKind::Geography,
        FieldKind::Json,
        FieldKind::Interval,
        FieldKind::Range,
        FieldKind::Struct,
    ];
}
