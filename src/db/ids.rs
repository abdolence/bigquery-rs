//! Validated newtypes for BigQuery dataset and table IDs.
//!
//! Both types follow the same shape: [`new`](BigQueryDatasetId::new) and the `TryFrom`/`FromStr`
//! impls validate at run time, and `const fn from_static` validates a literal at compile time.
//! Both share one rule function. The project ID is not here: it is one value for every Google
//! Cloud product, and stays a plain `String`.
//!
//! The rule is deliberately narrow. It rejects only what would change how the crate builds a
//! resource path (`projects/{p}/datasets/{d}/tables/{t}`), a dotted reference (`p.ds.t`) or SQL
//! text from the ID. Everything else, Unicode and BigQuery's own naming rules included, is the
//! server's to accept or reject.

use crate::errors::BigQueryError;
use crate::{BigQueryResult, BigQueryTableRef};
use serde::{Deserialize, Serialize, Serializer};
use std::borrow::{Borrow, Cow};
use std::fmt::{Display, Formatter};
use std::str::FromStr;

/// The most a dataset ID or a table ID may hold, in UTF-8 bytes; BigQuery's own limit for both.
const MAX_ID_BYTES: usize = 1024;

/// The ASCII characters an ID may not contain besides control characters: quotes and the
/// backslash would end or escape a quoted SQL identifier or string, `.` and `/` separate the parts
/// of a dotted reference and a resource path, and `$` and `@` start BigQuery's partition and
/// snapshot decorators.
const FORBIDDEN_ASCII: &[u8] = b"`'\"\\./$@";

/// The one rule an ID fails, if any.
#[derive(Clone, Copy)]
enum IdViolation {
    Empty,
    TooLong {
        len: usize,
    },
    /// The character starting at byte `at` is not allowed.
    InvalidChar {
        at: usize,
    },
}

impl IdViolation {
    /// The rule as a phrase, the same for both callers.
    ///
    /// `from_static` panics with it bare: formatting is not `const` on stable Rust, and the
    /// failing `const` item already points `rustc` at the bad literal. The run-time path adds
    /// the field name and the offending part.
    const fn rule(self) -> &'static str {
        match self {
            Self::Empty => "must not be empty",
            Self::TooLong { .. } => "must be at most 1,024 UTF-8 bytes",
            Self::InvalidChar { .. } => {
                "must not contain control characters or any of ` ' \" \\ . / $ @"
            }
        }
    }

    /// The error for `id` under the field name `field`.
    fn into_error(self, field: &'static str, id: &str) -> BigQueryError {
        let rule = self.rule();
        let error = match self {
            Self::Empty => rule.to_string(),
            // The value is unbounded here, so the message gives its length, not the value.
            Self::TooLong { len } => format!("{rule}, was {len} bytes"),
            Self::InvalidChar { at } => {
                let ch = id[at..]
                    .chars()
                    .next()
                    .expect("`at` is the start of the character the check rejected");
                format!(
                    "{rule}; got {ch:?} at byte {at} of \"{}\"",
                    id.escape_debug()
                )
            }
        };
        BigQueryError::invalid_parameters(field, error)
    }
}

/// Checks a dataset or table ID: not empty, at most [`MAX_ID_BYTES`], and free of control
/// characters (as [`char::is_control`]) and of [`FORBIDDEN_ASCII`].
///
/// `str` methods are not `const fn`, so this walks the bytes by hand; that is what lets
/// `from_static` run at compile time. Every forbidden character but the C1 controls is ASCII. The
/// C1 controls, U+0080 to U+009F, are the only characters encoded as `0xC2` followed by
/// `0x80..=0x9F`; `id` is valid UTF-8, so a `0xC2` byte always starts a character and needs no
/// further decoding.
const fn check_id(id: &str) -> Result<(), IdViolation> {
    let bytes = id.as_bytes();
    let len = bytes.len();
    if len == 0 {
        return Err(IdViolation::Empty);
    }
    if len > MAX_ID_BYTES {
        return Err(IdViolation::TooLong { len });
    }
    let mut i = 0;
    while i < len {
        let b = bytes[i];
        let c1_control = b == 0xC2 && i + 1 < len && bytes[i + 1] >= 0x80 && bytes[i + 1] <= 0x9F;
        if b.is_ascii_control() || c1_control || is_forbidden_ascii(b) {
            return Err(IdViolation::InvalidChar { at: i });
        }
        i += 1;
    }
    Ok(())
}

/// Whether `b` is in [`FORBIDDEN_ASCII`]; a loop because slice methods are not `const fn`.
const fn is_forbidden_ascii(b: u8) -> bool {
    let mut i = 0;
    while i < FORBIDDEN_ASCII.len() {
        if FORBIDDEN_ASCII[i] == b {
            return true;
        }
        i += 1;
    }
    false
}

/// A BigQuery dataset ID, checked only for what would let it reshape a path or a query.
///
/// An ID is rejected when it is empty, longer than 1,024 UTF-8 bytes, or holds a control
/// character or any of `` ` ``, `'`, `"`, `\`, `.`, `/`, `$` and `@`. Quotes and the backslash
/// would end or escape a quoted identifier in SQL text, `.` and `/` separate the parts of
/// `p.ds.t` and `projects/{p}/datasets/{d}`, and `$` and `@` start BigQuery's partition and
/// snapshot decorators. The full naming rules are BigQuery's to enforce: an ID that passes here
/// can still be refused by the server, at the call that sends it.
///
/// Declare the datasets an application knows up front as constants, and build tables in them
/// with [`table`](Self::table):
///
/// ```rust
/// use bigquery::{BigQueryDatasetId, BigQueryTableId};
///
/// const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
/// const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");
///
/// let orders = SHOP.table(ORDERS);
/// assert_eq!(orders.to_string(), "shop.orders");
/// ```
///
/// A name that arrives at run time goes through [`new`](Self::new), `parse()` or `try_into()`;
/// deserializing validates the same way:
///
/// ```rust
/// use bigquery::BigQueryDatasetId;
///
/// let shop = BigQueryDatasetId::new("shop_eu")?;
/// assert_eq!(shop, "shop_eu");
///
/// let err = BigQueryDatasetId::new("shop.eu").unwrap_err();
/// assert!(err.to_string().contains("dataset_id"));
/// # Ok::<(), bigquery::errors::BigQueryError>(())
/// ```
///
/// Dataset IDs are case-sensitive, and one that starts with `_` is a hidden dataset.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize)]
#[serde(try_from = "String")]
pub struct BigQueryDatasetId(Cow<'static, str>);

impl BigQueryDatasetId {
    /// Validates `id` and wraps it.
    ///
    /// # Errors
    /// [`BigQueryError::InvalidParametersError`] for the field `dataset_id` if `id` breaks the
    /// rule in the [type docs](Self); the message names the first rejected character and its
    /// byte offset.
    pub fn new(id: impl Into<String>) -> BigQueryResult<Self> {
        let id = id.into();
        match check_id(&id) {
            Ok(()) => Ok(Self(Cow::Owned(id))),
            Err(violation) => Err(violation.into_error("dataset_id", &id)),
        }
    }

    /// Validates `id` at compile time and wraps it without allocating.
    ///
    /// In a `const` or `static` item an invalid literal fails the build:
    ///
    /// ```compile_fail
    /// use bigquery::BigQueryDatasetId;
    /// const BAD: BigQueryDatasetId = BigQueryDatasetId::from_static("shop.eu");
    /// ```
    ///
    /// # Panics
    /// On an invalid `id` when called at run time, under the same rules as [`new`](Self::new).
    pub const fn from_static(id: &'static str) -> Self {
        match check_id(id) {
            Ok(()) => Self(Cow::Borrowed(id)),
            Err(violation) => panic!("{}", violation.rule()),
        }
    }

    /// The table `table` in this dataset, in the client's project.
    pub fn table(&self, table: BigQueryTableId) -> BigQueryTableRef {
        BigQueryTableRef::new(None, self.clone(), table)
    }

    /// The ID. The same as [`as_str`](Self::as_str).
    pub fn value(&self) -> &str {
        &self.0
    }

    /// The ID. The same as [`value`](Self::value).
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for BigQueryDatasetId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Borrow<str> for BigQueryDatasetId {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl Display for BigQueryDatasetId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self.0.as_ref(), f)
    }
}

impl TryFrom<&str> for BigQueryDatasetId {
    type Error = BigQueryError;

    fn try_from(id: &str) -> Result<Self, Self::Error> {
        Self::new(id)
    }
}

impl TryFrom<String> for BigQueryDatasetId {
    type Error = BigQueryError;

    fn try_from(id: String) -> Result<Self, Self::Error> {
        Self::new(id)
    }
}

impl FromStr for BigQueryDatasetId {
    type Err = BigQueryError;

    fn from_str(id: &str) -> Result<Self, Self::Err> {
        Self::new(id)
    }
}

impl PartialEq<str> for BigQueryDatasetId {
    fn eq(&self, other: &str) -> bool {
        self.0.as_ref() == other
    }
}

impl PartialEq<BigQueryDatasetId> for str {
    fn eq(&self, other: &BigQueryDatasetId) -> bool {
        self == other.0.as_ref()
    }
}

impl PartialEq<&str> for BigQueryDatasetId {
    fn eq(&self, other: &&str) -> bool {
        self.0.as_ref() == *other
    }
}

impl PartialEq<BigQueryDatasetId> for &str {
    fn eq(&self, other: &BigQueryDatasetId) -> bool {
        *self == other.0.as_ref()
    }
}

impl Serialize for BigQueryDatasetId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

/// A BigQuery table ID, checked only for what would let it reshape a path or a query.
///
/// The rule is the one [`BigQueryDatasetId`] applies: an ID is rejected when it is empty, longer
/// than 1,024 UTF-8 bytes, or holds a control character or any of `` ` ``, `'`, `"`, `\`, `.`,
/// `/`, `$` and `@`. Rejecting `$` and `@` keeps a plain table ID from silently becoming a
/// partition decorator such as `orders$20250101` or a snapshot decorator such as
/// `orders@1700000000000`. Unicode, spaces and dashes pass; the full naming rules are BigQuery's
/// to enforce, and an ID it refuses fails at the call that sends it.
///
/// ```rust
/// use bigquery::BigQueryTableId;
///
/// const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");
/// assert_eq!(ORDERS, "orders");
///
/// assert!(BigQueryTableId::new("étudiant-01").is_ok());
/// assert!(BigQueryTableId::new("table 01").is_ok());
///
/// let err = BigQueryTableId::new("orders`; DROP").unwrap_err();
/// assert!(err.to_string().contains("table_id"));
/// ```
///
/// Table IDs are case-sensitive.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize)]
#[serde(try_from = "String")]
pub struct BigQueryTableId(Cow<'static, str>);

impl BigQueryTableId {
    /// Validates `id` and wraps it.
    ///
    /// # Errors
    /// [`BigQueryError::InvalidParametersError`] for the field `table_id` if `id` breaks the
    /// rule in the [type docs](Self); the message names the first rejected character and its
    /// byte offset.
    pub fn new(id: impl Into<String>) -> BigQueryResult<Self> {
        let id = id.into();
        match check_id(&id) {
            Ok(()) => Ok(Self(Cow::Owned(id))),
            Err(violation) => Err(violation.into_error("table_id", &id)),
        }
    }

    /// Validates `id` at compile time and wraps it without allocating.
    ///
    /// In a `const` or `static` item an invalid literal fails the build:
    ///
    /// ```compile_fail
    /// use bigquery::BigQueryTableId;
    /// const BAD: BigQueryTableId = BigQueryTableId::from_static("shop.orders");
    /// ```
    ///
    /// # Panics
    /// On an invalid `id` when called at run time, under the same rules as [`new`](Self::new).
    pub const fn from_static(id: &'static str) -> Self {
        match check_id(id) {
            Ok(()) => Self(Cow::Borrowed(id)),
            Err(violation) => panic!("{}", violation.rule()),
        }
    }

    /// The ID. The same as [`as_str`](Self::as_str).
    pub fn value(&self) -> &str {
        &self.0
    }

    /// The ID. The same as [`value`](Self::value).
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for BigQueryTableId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Borrow<str> for BigQueryTableId {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl Display for BigQueryTableId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self.0.as_ref(), f)
    }
}

impl TryFrom<&str> for BigQueryTableId {
    type Error = BigQueryError;

    fn try_from(id: &str) -> Result<Self, Self::Error> {
        Self::new(id)
    }
}

impl TryFrom<String> for BigQueryTableId {
    type Error = BigQueryError;

    fn try_from(id: String) -> Result<Self, Self::Error> {
        Self::new(id)
    }
}

impl FromStr for BigQueryTableId {
    type Err = BigQueryError;

    fn from_str(id: &str) -> Result<Self, Self::Err> {
        Self::new(id)
    }
}

impl PartialEq<str> for BigQueryTableId {
    fn eq(&self, other: &str) -> bool {
        self.0.as_ref() == other
    }
}

impl PartialEq<BigQueryTableId> for str {
    fn eq(&self, other: &BigQueryTableId) -> bool {
        self == other.0.as_ref()
    }
}

impl PartialEq<&str> for BigQueryTableId {
    fn eq(&self, other: &&str) -> bool {
        self.0.as_ref() == *other
    }
}

impl PartialEq<BigQueryTableId> for &str {
    fn eq(&self, other: &BigQueryTableId) -> bool {
        *self == other.0.as_ref()
    }
}

impl Serialize for BigQueryTableId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

#[cfg(test)]
mod tests;
