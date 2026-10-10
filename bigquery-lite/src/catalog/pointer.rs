//! The catalog pointers of one table name: `v<version>` objects, one per commit, each created
//! only if absent. The newest one is the table's state: a live pointer names the current
//! metadata file, a tombstone says the table was dropped. Numbering never restarts, so a name
//! is never reused for another commit unless pruning freed it, which the commit protocol
//! detects by reading the predecessor back.

use crate::storage::{ObjectIdentity, StoredObject};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// A pointer's version: 0 for a name's first commit, one more for each commit on top. It is the
/// table's etag.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct PointerVersion(u64);

impl PointerVersion {
    pub(crate) const FIRST: Self = Self(0);

    /// The version a commit on top of this one creates.
    pub(crate) fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }

    /// The object name: `v` and the version zero-padded to ten digits, so that every
    /// backend's listing order is the numeric one. Readers parse the number instead of trusting
    /// that order.
    pub(crate) fn object_name(self) -> String {
        format!("v{:010}", self.0)
    }

    /// The version an object name holds, for names written by [`PointerVersion::object_name`];
    /// staging files and anything else in the directory are `None`.
    pub(crate) fn from_object_name(name: &str) -> Option<Self> {
        let digits = name.strip_prefix('v')?;
        if digits.len() < 10 || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        digits.parse().ok().map(Self)
    }

    pub(crate) fn number(self) -> u64 {
        self.0
    }

    #[cfg(test)]
    pub(crate) const fn new(version: u64) -> Self {
        Self(version)
    }
}

/// The version as the table's etag.
impl Display for PointerVersion {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The content of a pointer object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum PointerBody {
    Live(LivePointer),
    Tombstone(Tombstone),
}

/// A pointer to a table's metadata file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LivePointer {
    pub metadata_location: String,
    /// The metadata location of the pointer this one was created on; `None` for a name's
    /// first pointer and for a table created on a tombstone.
    pub previous: Option<String>,
    pub committed_at_ms: i64,
}

/// The pointer a drop creates, so that a dropped name keeps its numbering.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Tombstone {
    /// The metadata location of the dropped table.
    pub previous: String,
    pub dropped_at_ms: i64,
}

impl PointerBody {
    /// The metadata location the next pointer names as its `previous`.
    pub(crate) fn metadata_location(&self) -> Option<&str> {
        match self {
            PointerBody::Live(live) => Some(&live.metadata_location),
            PointerBody::Tombstone(_) => None,
        }
    }

    /// The metadata file the pointer is about: the current one of a live pointer, the dropped
    /// table's of a tombstone.
    pub(crate) fn metadata_file(&self) -> &str {
        match self {
            PointerBody::Live(live) => &live.metadata_location,
            PointerBody::Tombstone(tombstone) => &tombstone.previous,
        }
    }
}

/// A pointer as one read returned it.
#[derive(Debug, Clone)]
pub(crate) struct PointerHead {
    pub version: PointerVersion,
    pub identity: ObjectIdentity,
    pub body: PointerBody,
}

impl PointerHead {
    /// The pointer `object` holds.
    ///
    /// # Errors
    /// The JSON error when the object is not a pointer.
    pub(crate) fn read(
        version: PointerVersion,
        object: StoredObject,
    ) -> Result<Self, serde_json::Error> {
        Ok(Self {
            version,
            identity: object.identity,
            body: serde_json::from_slice(&object.bytes)?,
        })
    }

    /// Whether `object`, read back later, is still this pointer: the same object with the same
    /// content.
    pub(crate) fn is(&self, object: &StoredObject) -> bool {
        object.identity == self.identity
            && serde_json::from_slice::<PointerBody>(&object.bytes)
                .is_ok_and(|body| body == self.body)
    }
}
