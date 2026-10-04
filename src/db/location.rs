//! The location a job runs in or a dataset is stored in.

use crate::errors::BigQueryError;
use crate::BigQueryResult;
use serde::{Deserialize, Serialize, Serializer};
use std::borrow::Cow;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

/// A BigQuery location, such as `US`, `EU` or `europe-west1`.
///
/// The set of locations is open and grows as Google adds regions, so this is a name rather than
/// an enum. It is checked only for being non-empty: whether BigQuery knows the location is
/// BigQuery's to say, at the call that sends it. BigQuery compares locations ignoring case;
/// this type keeps the name as given.
///
/// ```rust
/// use bigquery::BigQueryLocation;
///
/// const EU: BigQueryLocation = BigQueryLocation::from_static("EU");
/// assert_eq!(EU.to_string(), "EU");
///
/// let region: BigQueryLocation = "europe-west1".parse()?;
/// assert_eq!(region.to_string(), "europe-west1");
/// assert!(BigQueryLocation::new("").is_err());
/// # Ok::<(), bigquery::errors::BigQueryError>(())
/// ```
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize)]
#[serde(try_from = "String")]
pub struct BigQueryLocation(Cow<'static, str>);

impl BigQueryLocation {
    /// Checks `location` and wraps it.
    ///
    /// # Errors
    /// [`BigQueryError::InvalidParametersError`] for the field `location` if it is empty.
    pub fn new(location: impl Into<String>) -> BigQueryResult<Self> {
        let location = location.into();
        if location.is_empty() {
            return Err(BigQueryError::invalid_parameters(
                "location",
                "must not be empty",
            ));
        }
        Ok(Self(Cow::Owned(location)))
    }

    /// Checks `location` at compile time and wraps it without allocating.
    ///
    /// # Panics
    /// On an empty `location` when called at run time; in a `const` item it fails the build.
    pub const fn from_static(location: &'static str) -> Self {
        assert!(!location.is_empty(), "a location must not be empty");
        Self(Cow::Borrowed(location))
    }

    /// A location BigQuery reported, kept as it came; an empty one is no location.
    pub(crate) fn reported(location: String) -> Option<Self> {
        (!location.is_empty()).then_some(Self(Cow::Owned(location)))
    }

    /// The name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for BigQueryLocation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self.0.as_ref(), f)
    }
}

impl TryFrom<&str> for BigQueryLocation {
    type Error = BigQueryError;

    fn try_from(location: &str) -> Result<Self, Self::Error> {
        Self::new(location)
    }
}

impl TryFrom<String> for BigQueryLocation {
    type Error = BigQueryError;

    fn try_from(location: String) -> Result<Self, Self::Error> {
        Self::new(location)
    }
}

impl FromStr for BigQueryLocation {
    type Err = BigQueryError;

    fn from_str(location: &str) -> Result<Self, Self::Err> {
        Self::new(location)
    }
}

impl Serialize for BigQueryLocation {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_location_is_refused_everywhere_it_can_be_built() {
        match BigQueryLocation::new("") {
            Err(BigQueryError::InvalidParametersError(err)) => {
                assert_eq!(err.public.field, "location")
            }
            other => panic!("expected an invalid location, got {other:?}"),
        }
        assert!("".parse::<BigQueryLocation>().is_err());
        assert!(serde_json::from_str::<BigQueryLocation>(r#""""#).is_err());
        assert_eq!(
            serde_json::from_str::<BigQueryLocation>(r#""EU""#).expect("a location"),
            BigQueryLocation::from_static("EU")
        );
    }
}
