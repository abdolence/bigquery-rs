use serde::{Deserialize, Serialize};

pub(crate) const TAG_RANGE: &str = "BigQueryRange";

/// A RANGE value of DATE, DATETIME or TIMESTAMP.
///
/// `T` is any Rust form of the element type, the temporal wrappers included. A `None` end is
/// unbounded, except in a REQUIRED RANGE column, where BigQuery sends an unbounded end as the
/// epoch and the read path cannot tell it from a real epoch bound.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BigQueryRange<T> {
    /// The inclusive start, `None` when unbounded.
    pub start: Option<T>,
    /// The exclusive end, `None` when unbounded.
    pub end: Option<T>,
}
