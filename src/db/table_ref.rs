use crate::errors::BigQueryError;
use crate::{BigQueryDatasetId, BigQueryResult, BigQueryTableId};
use gcloud_sdk::google::cloud::bigquery::v2::{DatasetReference, TableReference};
use std::fmt::{Display, Formatter};
use std::str::FromStr;

/// Checks the one rule a project ID has here: that it is not empty. Project IDs are shared by
/// every Google Cloud product, so their full rules are left to the server.
fn check_project_id(project: String) -> BigQueryResult<String> {
    if project.is_empty() {
        return Err(BigQueryError::invalid_parameters(
            "project_id",
            "must not be empty",
        ));
    }
    Ok(project)
}

/// A dataset, by project and dataset ID.
///
/// A [`BigQueryDatasetId`] converts into one in the client's project; [`new`](Self::new) names
/// another project:
///
/// ```rust
/// use bigquery::{BigQueryDatasetId, BigQueryDatasetRef, BigQueryTableId};
///
/// const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
/// const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");
///
/// let orders = BigQueryDatasetRef::new("acme-prod", SHOP)?.table(ORDERS);
/// assert_eq!(orders.to_string(), "acme-prod.shop.orders");
/// # Ok::<(), bigquery::errors::BigQueryError>(())
/// ```
#[derive(Debug, Eq, PartialEq, Clone, Hash)]
pub struct BigQueryDatasetRef {
    project: Option<String>,
    dataset: BigQueryDatasetId,
}

impl BigQueryDatasetRef {
    /// The dataset `dataset` in the project `project`.
    ///
    /// # Errors
    /// [`BigQueryError::InvalidParametersError`] for the field `project_id` if `project` is
    /// empty.
    pub fn new(project: impl Into<String>, dataset: BigQueryDatasetId) -> BigQueryResult<Self> {
        Ok(Self {
            project: Some(check_project_id(project.into())?),
            dataset,
        })
    }

    /// The table `table` in this dataset.
    pub fn table(&self, table: BigQueryTableId) -> BigQueryTableRef {
        BigQueryTableRef::new(self.project.clone(), self.dataset.clone(), table)
    }

    /// The project that owns the dataset; `None` is the client's
    /// [`google_project_id`](crate::BigQueryDbOptions::google_project_id).
    pub fn project(&self) -> Option<&str> {
        self.project.as_deref()
    }

    /// The dataset ID.
    pub fn dataset(&self) -> &BigQueryDatasetId {
        &self.dataset
    }
}

/// The dataset in the client's project.
impl From<BigQueryDatasetId> for BigQueryDatasetRef {
    fn from(dataset: BigQueryDatasetId) -> Self {
        Self {
            project: None,
            dataset,
        }
    }
}

/// Parses `dataset` or `project.dataset`. A domain-scoped project such as `example.com:project`
/// keeps its dot, since the dataset is the last part.
///
/// # Errors
/// [`BigQueryError::InvalidParametersError`] naming the part that is invalid: `project_id` or
/// `dataset_id`.
impl FromStr for BigQueryDatasetRef {
    type Err = BigQueryError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.rsplit_once('.') {
            Some((project, dataset)) => Self::new(project, dataset.parse()?),
            None => Ok(Self::from(s.parse::<BigQueryDatasetId>()?)),
        }
    }
}

/// A dataset the v2 API names, such as a listed one.
///
/// # Errors
/// [`BigQueryError::InvalidParametersError`] naming the part that is empty or invalid.
impl TryFrom<DatasetReference> for BigQueryDatasetRef {
    type Error = BigQueryError;

    fn try_from(dataset: DatasetReference) -> Result<Self, Self::Error> {
        Self::new(dataset.project_id, dataset.dataset_id.try_into()?)
    }
}

/// `project.dataset`, or `dataset` when the project is unset: the text [`FromStr`] reads back.
impl Display for BigQueryDatasetRef {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(project) = &self.project {
            write!(f, "{project}.")?;
        }
        write!(f, "{}", self.dataset)
    }
}

/// A table, by project, dataset and table ID.
///
/// Every call that takes a table takes `impl Into<BigQueryTableRef>`. Build one from validated
/// IDs, or parse it from `dataset.table` or `project.dataset.table`:
///
/// ```rust
/// use bigquery::{BigQueryDatasetId, BigQueryTableId, BigQueryTableRef};
///
/// const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
/// const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");
///
/// let orders = SHOP.table(ORDERS);
/// assert_eq!(orders.project(), None);
///
/// let parsed: BigQueryTableRef = "acme-prod.shop.orders".parse()?;
/// assert_eq!(parsed.project(), Some("acme-prod"));
/// assert_eq!(parsed.dataset(), "shop");
/// assert_eq!(parsed.table(), "orders");
/// # Ok::<(), bigquery::errors::BigQueryError>(())
/// ```
#[derive(Debug, Eq, PartialEq, Clone, Hash)]
pub struct BigQueryTableRef {
    project: Option<String>,
    dataset: BigQueryDatasetId,
    table: BigQueryTableId,
}

impl BigQueryTableRef {
    /// Callers have checked that `project` is not empty.
    pub(crate) fn new(
        project: Option<String>,
        dataset: BigQueryDatasetId,
        table: BigQueryTableId,
    ) -> Self {
        Self {
            project,
            dataset,
            table,
        }
    }

    /// The project that owns the dataset; `None` is the client's
    /// [`google_project_id`](crate::BigQueryDbOptions::google_project_id).
    pub fn project(&self) -> Option<&str> {
        self.project.as_deref()
    }

    /// The dataset ID.
    pub fn dataset(&self) -> &BigQueryDatasetId {
        &self.dataset
    }

    /// The table ID.
    pub fn table(&self) -> &BigQueryTableId {
        &self.table
    }

    /// The resource name `projects/{p}/datasets/{d}/tables/{t}` that the Storage API takes,
    /// with `default_project_id` for an unset project.
    pub(crate) fn table_path(&self, default_project_id: &str) -> String {
        format!(
            "projects/{}/datasets/{}/tables/{}",
            self.project.as_deref().unwrap_or(default_project_id),
            self.dataset,
            self.table
        )
    }
}

/// A table the v2 API names, such as a query job's destination table.
///
/// # Errors
/// [`BigQueryError::InvalidParametersError`] naming the part that is empty or invalid.
impl TryFrom<TableReference> for BigQueryTableRef {
    type Error = BigQueryError;

    fn try_from(table: TableReference) -> Result<Self, Self::Error> {
        Ok(Self::new(
            Some(check_project_id(table.project_id)?),
            table.dataset_id.try_into()?,
            table.table_id.try_into()?,
        ))
    }
}

/// Parses `dataset.table` or `project.dataset.table`. A domain-scoped project such as
/// `example.com:project` keeps its dot, since the dataset and the table are the last two parts.
///
/// # Errors
/// [`BigQueryError::InvalidParametersError`] for the field `table` when the text has fewer than
/// two parts, and otherwise naming the part that is invalid: `project_id`, `dataset_id` or
/// `table_id`.
impl FromStr for BigQueryTableRef {
    type Err = BigQueryError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let Some((dataset, table)) = s.rsplit_once('.') else {
            return Err(BigQueryError::invalid_parameters(
                "table",
                format!(
                    "\"{}\" is not `dataset.table` or `project.dataset.table`",
                    s.escape_debug()
                ),
            ));
        };
        let dataset: BigQueryDatasetRef = dataset.parse()?;
        Ok(dataset.table(table.parse()?))
    }
}

/// `project.dataset.table`, or `dataset.table` when the project is unset: the text
/// [`FromStr`] reads back.
impl Display for BigQueryTableRef {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(project) = &self.project {
            write!(f, "{project}.")?;
        }
        write!(f, "{}.{}", self.dataset, self.table)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::BigQueryInvalidParametersError;

    const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
    const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");

    /// The field an invalid-parameters error names.
    fn invalid_field<T: std::fmt::Debug>(result: BigQueryResult<T>) -> String {
        match result {
            Err(BigQueryError::InvalidParametersError(BigQueryInvalidParametersError {
                public,
            })) => public.field,
            other => panic!("expected an invalid-parameters error, got {other:?}"),
        }
    }

    #[test]
    fn table_refs_parse_and_print_back() {
        for text in ["shop.orders", "acme-prod.shop.orders"] {
            let parsed: BigQueryTableRef = text.parse().expect("valid test input");
            assert_eq!(parsed.to_string(), text);
        }
        let parsed: BigQueryTableRef = "shop.orders".parse().expect("valid test input");
        assert_eq!(parsed, SHOP.table(ORDERS));
    }

    #[test]
    fn a_domain_scoped_project_keeps_its_dot() {
        let parsed: BigQueryTableRef = "example.com:proj.shop.orders"
            .parse()
            .expect("valid test input");
        assert_eq!(parsed.project(), Some("example.com:proj"));
        assert_eq!(parsed.dataset(), "shop");
        assert_eq!(parsed.table(), "orders");
        assert_eq!(parsed.to_string(), "example.com:proj.shop.orders");

        let dataset: BigQueryDatasetRef = "example.com:proj.shop".parse().expect("valid input");
        assert_eq!(dataset.project(), Some("example.com:proj"));
    }

    #[test]
    fn a_parse_error_names_the_bad_part() {
        let cases = [
            ("orders", "table"),
            ("", "table"),
            ("shop.", "table_id"),
            ("shop.ord`ers", "table_id"),
            (".orders", "dataset_id"),
            ("sh/op.orders", "dataset_id"),
            ("p..orders", "dataset_id"),
            (".shop.orders", "project_id"),
            ("p.shop.orders.", "table_id"),
        ];
        for (text, field) in cases {
            assert_eq!(
                invalid_field(text.parse::<BigQueryTableRef>()),
                field,
                "{text:?}"
            );
        }
    }

    #[test]
    fn a_dataset_ref_rejects_an_empty_project() {
        assert_eq!(
            invalid_field(BigQueryDatasetRef::new("", SHOP)),
            "project_id"
        );
    }

    #[test]
    fn a_dataset_ref_builds_tables_in_its_project() {
        let other = BigQueryDatasetRef::new("acme-prod", SHOP).expect("valid test input");
        assert_eq!(other.table(ORDERS).to_string(), "acme-prod.shop.orders");
        assert_eq!(
            BigQueryDatasetRef::from(SHOP).table(ORDERS),
            SHOP.table(ORDERS)
        );
    }

    #[test]
    fn a_v2_table_reference_is_validated() {
        let good = TableReference {
            project_id: "p".into(),
            dataset_id: "shop".into(),
            table_id: "orders".into(),
        };
        let table = BigQueryTableRef::try_from(good.clone()).expect("valid test input");
        assert_eq!(table.to_string(), "p.shop.orders");

        let bad = TableReference {
            dataset_id: "sh/op".into(),
            ..good
        };
        assert_eq!(invalid_field(BigQueryTableRef::try_from(bad)), "dataset_id");
    }

    #[test]
    fn table_path_falls_back_to_the_db_project() {
        assert_eq!(
            SHOP.table(ORDERS).table_path("default-p"),
            "projects/default-p/datasets/shop/tables/orders"
        );
        let other = BigQueryDatasetRef::new("p", SHOP).expect("valid test input");
        assert_eq!(
            other.table(ORDERS).table_path("default-p"),
            "projects/p/datasets/shop/tables/orders"
        );
    }
}
