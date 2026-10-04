use crate::types::schema::BigQueryFieldType;
use arrow_schema::Metadata;
use arrow_schema::{DataType, IntervalUnit, TimeUnit};

pub(crate) const ARROW_EXTENSION_NAME: &str = "ARROW:extension:name";
pub(crate) const GOOGLE_SQL_TYPE: &str = "google:sqlType";

/// A BigQuery column type as both codecs see it: [`BigQueryFieldType`] without its parameters,
/// element types or fields.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum BqKind {
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

impl BqKind {
    /// Classifies a column of a Storage Read or inline query Arrow schema. `None` means the
    /// Arrow type is not one BigQuery sends. A `List` is not a kind: classify its element with
    /// [`BqKind::from_arrow_list_item`].
    pub(crate) fn from_arrow(field: &arrow_schema::Field) -> Option<BqKind> {
        classify(field.data_type(), field.metadata())
    }

    /// Classifies the element of a REPEATED column. BigQuery puts the element's metadata on the
    /// list field and none on `item`, so the type comes from `item` and the metadata from
    /// `list`.
    pub(crate) fn from_arrow_list_item(
        list: &arrow_schema::Field,
        item: &arrow_schema::Field,
    ) -> Option<BqKind> {
        classify(item.data_type(), list.metadata())
    }

    /// The GoogleSQL name, as in `INT64`.
    pub(crate) fn name(self) -> &'static str {
        match self {
            BqKind::Int64 => "INT64",
            BqKind::Float64 => "FLOAT64",
            BqKind::Bool => "BOOL",
            BqKind::String => "STRING",
            BqKind::Bytes => "BYTES",
            BqKind::Date => "DATE",
            BqKind::Time => "TIME",
            BqKind::DateTime => "DATETIME",
            BqKind::Timestamp => "TIMESTAMP",
            BqKind::Numeric => "NUMERIC",
            BqKind::BigNumeric => "BIGNUMERIC",
            BqKind::Geography => "GEOGRAPHY",
            BqKind::Json => "JSON",
            BqKind::Interval => "INTERVAL",
            BqKind::Range => "RANGE",
            BqKind::Struct => "STRUCT",
        }
    }

    /// Whether a REPEATED column of this kind is written as one packed run.
    pub(crate) fn packable(self) -> bool {
        matches!(
            self,
            BqKind::Int64
                | BqKind::Float64
                | BqKind::Bool
                | BqKind::Date
                | BqKind::Time
                | BqKind::DateTime
                | BqKind::Timestamp
        )
    }

    /// Whether the temporal wrappers' integer form applies to this kind.
    pub(crate) fn is_temporal(self) -> bool {
        matches!(
            self,
            BqKind::Date | BqKind::Time | BqKind::DateTime | BqKind::Timestamp
        )
    }
}

fn classify(data_type: &DataType, metadata: &Metadata) -> Option<BqKind> {
    let ext = metadata.get(ARROW_EXTENSION_NAME).map(String::as_str);
    Some(match data_type {
        DataType::Int64 => BqKind::Int64,
        DataType::Float64 => BqKind::Float64,
        DataType::Boolean => BqKind::Bool,
        DataType::Utf8 => match ext {
            Some("google:sqlType:json") => BqKind::Json,
            Some("google:sqlType:geography") => BqKind::Geography,
            _ => BqKind::String,
        },
        DataType::Binary => BqKind::Bytes,
        DataType::Date32 => BqKind::Date,
        DataType::Time64(TimeUnit::Microsecond) => BqKind::Time,
        DataType::Timestamp(TimeUnit::Microsecond, None) => BqKind::DateTime,
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => BqKind::Timestamp,
        DataType::Decimal128(38, 9) => BqKind::Numeric,
        DataType::Decimal256(76, 38) => BqKind::BigNumeric,
        DataType::Interval(IntervalUnit::MonthDayNano) => BqKind::Interval,
        DataType::Struct(_)
            if metadata.get(GOOGLE_SQL_TYPE).map(String::as_str) == Some("range") =>
        {
            BqKind::Range
        }
        DataType::Struct(_) => BqKind::Struct,
        _ => return None,
    })
}

impl From<&BigQueryFieldType> for BqKind {
    fn from(field_type: &BigQueryFieldType) -> Self {
        match field_type {
            BigQueryFieldType::Int64 => BqKind::Int64,
            BigQueryFieldType::Float64 => BqKind::Float64,
            BigQueryFieldType::Numeric(_) => BqKind::Numeric,
            BigQueryFieldType::BigNumeric(_) => BqKind::BigNumeric,
            BigQueryFieldType::Bool => BqKind::Bool,
            BigQueryFieldType::String { .. } => BqKind::String,
            BigQueryFieldType::Bytes { .. } => BqKind::Bytes,
            BigQueryFieldType::Date => BqKind::Date,
            BigQueryFieldType::Time => BqKind::Time,
            BigQueryFieldType::DateTime => BqKind::DateTime,
            BigQueryFieldType::Timestamp => BqKind::Timestamp,
            BigQueryFieldType::Geography => BqKind::Geography,
            BigQueryFieldType::Json => BqKind::Json,
            BigQueryFieldType::Interval => BqKind::Interval,
            BigQueryFieldType::Range(_) => BqKind::Range,
            BigQueryFieldType::Struct(_) => BqKind::Struct,
        }
    }
}

#[cfg(test)]
mod tests;
