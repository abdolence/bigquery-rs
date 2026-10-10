//! The engine's identifiers, validated by BigQuery's own naming rules, since the engine is the
//! server that enforces them. Every ID is also one path segment of the storage layout, which
//! the rules guarantee: none of them admits `/`, `.` as a whole segment, or a control character.

use crate::error::EngineError;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::str::FromStr;
use unicode_properties::{GeneralCategory, GeneralCategoryGroup, UnicodeGeneralCategory};

/// A Google Cloud project ID: 6 to 30 characters of `a-z`, `0-9` and `-` that start with a
/// letter, optionally behind a domain scope, as in `example.com:my-project`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String")]
pub struct ProjectId(String);

impl ProjectId {
    /// The ID as the storage layout and the wire spell it.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for ProjectId {
    type Error = EngineError;

    fn try_from(id: String) -> Result<Self, Self::Error> {
        let invalid = |rule: &str| EngineError::invalid(format!("project ID {id:?}"), rule);
        let (domain, project) = match id.split_once(':') {
            Some((domain, project)) => (Some(domain), project),
            None => (None, id.as_str()),
        };
        if let Some(domain) = domain {
            if domain.is_empty()
                || !domain
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'.' || b == b'-')
            {
                return Err(invalid(
                    "a domain scope holds lowercase letters, digits, `.` and `-` only",
                ));
            }
        }
        if !(6..=30).contains(&project.len()) {
            return Err(invalid("a project ID is 6 to 30 characters long"));
        }
        if !project.starts_with(|c: char| c.is_ascii_lowercase()) {
            return Err(invalid("a project ID starts with a lowercase letter"));
        }
        if !project
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        {
            return Err(invalid(
                "a project ID holds lowercase letters, digits and `-` only",
            ));
        }
        Ok(Self(id))
    }
}

impl FromStr for ProjectId {
    type Err = EngineError;

    fn from_str(id: &str) -> Result<Self, Self::Err> {
        Self::try_from(id.to_string())
    }
}

impl Display for ProjectId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A dataset ID: 1 to 1,024 characters of `A-Z`, `a-z`, `0-9` and `_`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String")]
pub struct DatasetId(String);

impl DatasetId {
    /// The ID as the storage layout and the wire spell it.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for DatasetId {
    type Error = EngineError;

    fn try_from(id: String) -> Result<Self, Self::Error> {
        if !(1..=1024).contains(&id.len()) {
            return Err(EngineError::invalid(
                format!("dataset ID {id:?}"),
                "a dataset ID is 1 to 1,024 characters long",
            ));
        }
        if !id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
            return Err(EngineError::invalid(
                format!("dataset ID {id:?}"),
                "a dataset ID holds letters, digits and `_` only",
            ));
        }
        Ok(Self(id))
    }
}

impl FromStr for DatasetId {
    type Err = EngineError;

    fn from_str(id: &str) -> Result<Self, Self::Err> {
        Self::try_from(id.to_string())
    }
}

impl Display for DatasetId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A table ID: up to 1,024 bytes of UTF-8 in the Unicode categories L (letter), M (mark), N
/// (number), Pc (connector), Pd (dash) and Zs (space). `.` and `/` are in none of them.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String")]
pub struct TableId(String);

impl TableId {
    /// The ID as the storage layout and the wire spell it.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for TableId {
    type Error = EngineError;

    fn try_from(id: String) -> Result<Self, Self::Error> {
        if !(1..=1024).contains(&id.len()) {
            return Err(EngineError::invalid(
                format!("table ID {id:?}"),
                "a table ID is 1 to 1,024 bytes of UTF-8",
            ));
        }
        let allowed = |c: char| {
            matches!(
                c.general_category_group(),
                GeneralCategoryGroup::Letter
                    | GeneralCategoryGroup::Mark
                    | GeneralCategoryGroup::Number
            ) || matches!(
                c.general_category(),
                GeneralCategory::ConnectorPunctuation
                    | GeneralCategory::DashPunctuation
                    | GeneralCategory::SpaceSeparator
            )
        };
        if let Some(c) = id.chars().find(|c| !allowed(*c)) {
            return Err(EngineError::invalid(
                format!("table ID {id:?}"),
                format!(
                    "{c:?} is not a letter, mark, number, connector, dash or space, the \
                     characters a table ID holds"
                ),
            ));
        }
        Ok(Self(id))
    }
}

impl FromStr for TableId {
    type Err = EngineError;

    fn from_str(id: &str) -> Result<Self, Self::Err> {
        Self::try_from(id.to_string())
    }
}

impl Display for TableId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A job ID: 1 to 1,024 characters of `A-Z`, `a-z`, `0-9`, `_` and `-`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String")]
pub struct JobId(String);

impl JobId {
    /// The ID as the wire spells it.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for JobId {
    type Error = EngineError;

    fn try_from(id: String) -> Result<Self, Self::Error> {
        if !(1..=1024).contains(&id.len())
            || !id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        {
            return Err(EngineError::invalid(
                format!("job ID {id:?}"),
                "a job ID is 1 to 1,024 characters of letters, digits, `_` and `-`",
            ));
        }
        Ok(Self(id))
    }
}

impl FromStr for JobId {
    type Err = EngineError;

    fn from_str(id: &str) -> Result<Self, Self::Err> {
        Self::try_from(id.to_string())
    }
}

impl Display for JobId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Where a dataset's data lives, as in `EU` or `us-central1`: non-empty, without control
/// characters. Two locations are equal when they differ only in ASCII case, as BigQuery
/// compares them; each is reported as it was given.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(try_from = "String")]
pub struct Location(Cow<'static, str>);

impl Location {
    /// A location from a literal, checked when the constant is evaluated.
    ///
    /// # Panics
    /// When `location` is empty or holds an ASCII control character; in a `const` that is a
    /// compile error.
    pub const fn from_static(location: &'static str) -> Self {
        let bytes = location.as_bytes();
        assert!(!bytes.is_empty(), "a location is not empty");
        let mut i = 0;
        while i < bytes.len() {
            assert!(
                !bytes[i].is_ascii_control(),
                "a location holds no control character"
            );
            i += 1;
        }
        Self(Cow::Borrowed(location))
    }

    /// The location as it was given.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl PartialEq for Location {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq_ignore_ascii_case(&other.0)
    }
}

impl Eq for Location {}

impl Hash for Location {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for byte in self.0.bytes() {
            state.write_u8(byte.to_ascii_lowercase());
        }
    }
}

impl TryFrom<String> for Location {
    type Error = EngineError;

    fn try_from(location: String) -> Result<Self, Self::Error> {
        if location.is_empty() || location.chars().any(char::is_control) {
            return Err(EngineError::invalid(
                format!("location {location:?}"),
                "a location is not empty and holds no control character",
            ));
        }
        Ok(Self(Cow::Owned(location)))
    }
}

impl FromStr for Location {
    type Err = EngineError;

    fn from_str(location: &str) -> Result<Self, Self::Err> {
        Self::try_from(location.to_string())
    }
}

impl Display for Location {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A dataset of a project.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DatasetRef {
    /// The project.
    pub project: ProjectId,
    /// The dataset.
    pub dataset: DatasetId,
}

impl DatasetRef {
    /// The table `table` of this dataset.
    pub fn table(&self, table: TableId) -> TableRef {
        TableRef {
            project: self.project.clone(),
            dataset: self.dataset.clone(),
            table,
        }
    }
}

/// `project:dataset`, BigQuery's legacy form, which its error messages use.
impl Display for DatasetRef {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.project, self.dataset)
    }
}

/// A table of a dataset.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TableRef {
    /// The project.
    pub project: ProjectId,
    /// The dataset.
    pub dataset: DatasetId,
    /// The table.
    pub table: TableId,
}

impl TableRef {
    /// The dataset that holds the table.
    pub fn dataset_ref(&self) -> DatasetRef {
        DatasetRef {
            project: self.project.clone(),
            dataset: self.dataset.clone(),
        }
    }
}

/// `project:dataset.table`, BigQuery's legacy form, which its error messages use.
impl Display for TableRef {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}.{}", self.project, self.dataset, self.table)
    }
}
