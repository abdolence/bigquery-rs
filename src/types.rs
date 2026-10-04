//! The BigQuery type vocabulary and the conversions both codecs share.
//!
//! Every BigQuery type has one set of Rust serde forms, and the read and write paths accept
//! exactly that set, so whatever one direction writes the other gives back in the same form.
//! The crate-private items here are what the codecs call; the public items are the wrappers,
//! the `with` modules and the schema types.

pub(crate) mod civil;
pub(crate) mod decimal;
pub(crate) mod error;
pub(crate) mod interval;
pub(crate) mod json;
pub(crate) mod kind;
pub(crate) mod range;
pub(crate) mod schema;
pub(crate) mod temporal;
#[cfg(test)]
pub(crate) mod testkit;

pub use decimal::{serialize_as_decimal, serialize_as_optional_decimal, BigQueryDecimal};
pub use interval::BigQueryInterval;
pub use json::{serialize_as_json, serialize_as_optional_json, BigQueryJson};
pub use range::BigQueryRange;
pub use schema::{
    BigQueryDecimalParams, BigQueryFieldMode, BigQueryFieldSchema, BigQueryFieldType,
    BigQueryRangeElementType, BigQueryTableSchema,
};
pub use temporal::{
    serialize_as_date, serialize_as_datetime, serialize_as_optional_date,
    serialize_as_optional_datetime, serialize_as_optional_time, serialize_as_optional_timestamp,
    serialize_as_time, serialize_as_timestamp, BigQueryDate, BigQueryDateTime, BigQueryTime,
    BigQueryTimestamp,
};
