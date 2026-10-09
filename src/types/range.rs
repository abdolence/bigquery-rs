use serde::{Deserialize, Serialize};

pub(crate) const TAG_RANGE: &str = "BigQueryRange";

/// A RANGE value of DATE, DATETIME or TIMESTAMP.
///
/// `T` is any Rust form of the element type, the temporal wrappers included. A `None` bound is
/// unbounded.
///
/// A REQUIRED RANGE column carries no validity for its bounds, and BigQuery sends an unbounded
/// bound of it as the epoch (1970-01-01, at 00:00:00 for DATETIME and TIMESTAMP). Since BigQuery keeps
/// `start < end`, the read path reads an epoch bound as `None` when the pair would break that
/// order otherwise: an epoch end at or below the start, an epoch start at or above the end.
/// Two cases cannot be told from a real epoch bound and read as the epoch: an unbounded start
/// with an end after the epoch, and an unbounded end with a start before it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BigQueryRange<T> {
    /// The inclusive start, `None` when unbounded.
    pub start: Option<T>,
    /// The exclusive end, `None` when unbounded.
    pub end: Option<T>,
}
