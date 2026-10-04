use crate::errors::BigQueryError;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

/// A table, by project, dataset and table ID.
///
/// Every call that takes a table takes `impl Into<BigQueryTableRef>`, so `("ds", "t")`,
/// `("project", "ds", "t")` and their `String` forms work as they are, and `"ds.t".parse()`
/// or `"project.ds.t".parse()` build one from text. The tuples are not checked; a table that
/// cannot be named fails at the call that sends it.
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct BigQueryTableRef {
    /// The project that owns the dataset; `None` is the client's
    /// [`google_project_id`](crate::BigQueryDbOptions::google_project_id).
    pub project_id: Option<String>,
    /// The dataset ID.
    pub dataset_id: String,
    /// The table ID.
    pub table_id: String,
}

impl BigQueryTableRef {
    /// The resource name `projects/{p}/datasets/{d}/tables/{t}` that the Storage API takes,
    /// with `default_project_id` for an unset project.
    #[allow(dead_code, reason = "the read and write sessions call it")]
    pub(crate) fn table_path(&self, default_project_id: &str) -> String {
        format!(
            "projects/{}/datasets/{}/tables/{}",
            self.project_id.as_deref().unwrap_or(default_project_id),
            self.dataset_id,
            self.table_id
        )
    }
}

/// `(dataset, table)` in the client's project.
impl From<(&str, &str)> for BigQueryTableRef {
    fn from((dataset_id, table_id): (&str, &str)) -> Self {
        Self {
            project_id: None,
            dataset_id: dataset_id.to_string(),
            table_id: table_id.to_string(),
        }
    }
}

/// `(project, dataset, table)`.
impl From<(&str, &str, &str)> for BigQueryTableRef {
    fn from((project_id, dataset_id, table_id): (&str, &str, &str)) -> Self {
        Self {
            project_id: Some(project_id.to_string()),
            dataset_id: dataset_id.to_string(),
            table_id: table_id.to_string(),
        }
    }
}

/// `(dataset, table)` in the client's project.
impl From<(String, String)> for BigQueryTableRef {
    fn from((dataset_id, table_id): (String, String)) -> Self {
        Self {
            project_id: None,
            dataset_id,
            table_id,
        }
    }
}

/// `(project, dataset, table)`.
impl From<(String, String, String)> for BigQueryTableRef {
    fn from((project_id, dataset_id, table_id): (String, String, String)) -> Self {
        Self {
            project_id: Some(project_id),
            dataset_id,
            table_id,
        }
    }
}

/// Parses `dataset.table` or `project.dataset.table`. A domain-scoped project such as
/// `example.com:project` keeps its dot, since the dataset and the table are the last two parts.
///
/// # Errors
/// [`BigQueryError::InvalidParametersError`] when the text has fewer than two parts or an empty
/// one.
impl FromStr for BigQueryTableRef {
    type Err = BigQueryError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let invalid = || {
            BigQueryError::invalid_parameters(
                "table",
                format!("`{s}` is not `dataset.table` or `project.dataset.table`"),
            )
        };
        let mut parts = s.rsplitn(3, '.');
        let (Some(table_id), Some(dataset_id)) = (parts.next(), parts.next()) else {
            return Err(invalid());
        };
        let project_id = parts.next();
        if table_id.is_empty() || dataset_id.is_empty() || project_id == Some("") {
            return Err(invalid());
        }
        Ok(Self {
            project_id: project_id.map(str::to_string),
            dataset_id: dataset_id.to_string(),
            table_id: table_id.to_string(),
        })
    }
}

/// `project.dataset.table`, or `dataset.table` when the project is unset: the text
/// [`FromStr`] reads back.
impl Display for BigQueryTableRef {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(project_id) = &self.project_id {
            write!(f, "{project_id}.")?;
        }
        write!(f, "{}.{}", self.dataset_id, self.table_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(project: Option<&str>, dataset: &str, table: &str) -> BigQueryTableRef {
        BigQueryTableRef {
            project_id: project.map(str::to_string),
            dataset_id: dataset.to_string(),
            table_id: table.to_string(),
        }
    }

    #[test]
    fn tuples_build_table_refs() {
        assert_eq!(BigQueryTableRef::from(("ds", "t")), table(None, "ds", "t"));
        assert_eq!(
            BigQueryTableRef::from(("p", "ds", "t")),
            table(Some("p"), "ds", "t")
        );
        assert_eq!(
            BigQueryTableRef::from(("ds".to_string(), "t".to_string())),
            table(None, "ds", "t")
        );
        assert_eq!(
            BigQueryTableRef::from(("p".to_string(), "ds".to_string(), "t".to_string())),
            table(Some("p"), "ds", "t")
        );
    }

    #[test]
    fn table_refs_parse_from_dotted_text() {
        assert_eq!(
            "ds.t".parse::<BigQueryTableRef>().ok(),
            Some(table(None, "ds", "t"))
        );
        assert_eq!(
            "p.ds.t".parse::<BigQueryTableRef>().ok(),
            Some(table(Some("p"), "ds", "t"))
        );
        assert_eq!(
            "example.com:p.ds.t".parse::<BigQueryTableRef>().ok(),
            Some(table(Some("example.com:p"), "ds", "t")),
            "a domain-scoped project keeps its dot"
        );
        for bad in ["t", "", "ds.", ".t", "p..t", "p.ds.t."] {
            assert!(
                matches!(
                    bad.parse::<BigQueryTableRef>(),
                    Err(BigQueryError::InvalidParametersError(_))
                ),
                "{bad:?}"
            );
        }
        for text in ["ds.t", "p.ds.t"] {
            assert_eq!(
                text.parse::<BigQueryTableRef>()
                    .expect("valid test input")
                    .to_string(),
                text
            );
        }
    }

    #[test]
    fn table_path_falls_back_to_the_db_project() {
        assert_eq!(
            table(None, "ds", "t").table_path("default-p"),
            "projects/default-p/datasets/ds/tables/t"
        );
        assert_eq!(
            table(Some("p"), "ds", "t").table_path("default-p"),
            "projects/p/datasets/ds/tables/t"
        );
    }
}
