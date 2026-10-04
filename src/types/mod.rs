//! The BigQuery type vocabulary and the conversions both codecs share.
//!
//! Every BigQuery type has one set of Rust serde forms, and the read and write paths accept
//! exactly that set, so whatever one direction writes the other gives back in the same form.
//! The crate-private items here are what the codecs call; the public items are the wrappers,
//! the `with` modules and the schema types.

// The codecs, the query parameters and the schema operations are the callers of most
// crate-private items here, and they land after this module.
#[allow(dead_code)]
pub(crate) mod civil;
#[allow(dead_code)]
pub(crate) mod decimal;
#[allow(dead_code)]
pub(crate) mod error;
#[allow(dead_code)]
pub(crate) mod interval;
#[allow(dead_code)]
pub(crate) mod json;
#[allow(dead_code)]
pub(crate) mod kind;
#[allow(dead_code)]
pub(crate) mod range;
pub(crate) mod schema;
#[allow(dead_code)]
pub(crate) mod temporal;
#[cfg(test)]
#[allow(dead_code)]
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
