//! The labels of a dataset, a table or a job.

use serde::{Deserialize, Serialize};
use std::collections::{btree_map, BTreeMap};
use std::fmt::{Debug, Formatter};

/// The labels of a dataset, a table or a job: keys and values, ordered by key.
///
/// Neither keys nor values are checked here. BigQuery's label rules (lowercase, length, the
/// characters allowed) are BigQuery's to enforce, at the call that sends them. Serde reads
/// and writes the labels as a map, and `{:?}` prints them as one.
///
/// A builder that takes labels accepts anything that converts, so an array of pairs works:
///
/// ```rust
/// use bigquery::BigQueryLabels;
///
/// let mut labels = BigQueryLabels::from([("team", "shop"), ("env", "prod")]);
/// labels.insert("env", "dev");
/// assert_eq!(labels.get("env"), Some("dev"));
/// assert_eq!(labels.iter().collect::<Vec<_>>(), [("env", "dev"), ("team", "shop")]);
///
/// let collected: BigQueryLabels = vec![("team".to_string(), "shop")].into_iter().collect();
/// assert_eq!(collected.len(), 1);
/// ```
#[derive(Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BigQueryLabels(BTreeMap<String, String>);

impl BigQueryLabels {
    /// No labels.
    pub fn new() -> Self {
        Self::default()
    }

    /// The value of the label `key`.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }

    /// Sets the label `key` to `value`, and returns the value it replaced.
    pub fn insert(&mut self, key: impl Into<String>, value: impl Into<String>) -> Option<String> {
        self.0.insert(key.into(), value.into())
    }

    /// Removes the label `key`, and returns its value.
    pub fn remove(&mut self, key: &str) -> Option<String> {
        self.0.remove(key)
    }

    /// The labels, ordered by key.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> + '_ {
        self.0.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }

    /// How many labels there are.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether there are no labels.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl Debug for BigQueryLabels {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.0, f)
    }
}

/// The owning iterator of [`BigQueryLabels`], ordered by key.
#[derive(Debug)]
pub struct BigQueryLabelsIntoIter(btree_map::IntoIter<String, String>);

impl Iterator for BigQueryLabelsIntoIter {
    type Item = (String, String);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl IntoIterator for BigQueryLabels {
    type Item = (String, String);
    type IntoIter = BigQueryLabelsIntoIter;

    fn into_iter(self) -> Self::IntoIter {
        BigQueryLabelsIntoIter(self.0.into_iter())
    }
}

impl<K: Into<String>, V: Into<String>> FromIterator<(K, V)> for BigQueryLabels {
    fn from_iter<I: IntoIterator<Item = (K, V)>>(labels: I) -> Self {
        Self(
            labels
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        )
    }
}

impl<K: Into<String>, V: Into<String>, const N: usize> From<[(K, V); N]> for BigQueryLabels {
    fn from(labels: [(K, V); N]) -> Self {
        labels.into_iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn builder_takes(labels: impl Into<BigQueryLabels>) -> BigQueryLabels {
        labels.into()
    }

    #[test]
    fn labels_convert_from_pairs_and_serialize_as_a_map() {
        let labels = builder_takes([("team", "shop"), ("env", "prod")]);
        assert_eq!(labels.get("team"), Some("shop"));
        assert_eq!(labels.len(), 2);

        let json = serde_json::to_string(&labels).expect("a string map serializes");
        assert_eq!(json, r#"{"env":"prod","team":"shop"}"#);
        assert_eq!(
            serde_json::from_str::<BigQueryLabels>(&json).expect("a string map deserializes"),
            labels
        );
        assert_eq!(format!("{labels:?}"), r#"{"env": "prod", "team": "shop"}"#);
    }
}
