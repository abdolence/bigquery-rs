//! Datasets: one `<dataset>.json` per dataset under its project's catalog directory, written
//! with replace semantics. Within one process a dataset's changes are serialized; across
//! processes the last writer wins.

use super::{now_ms, EngineCatalog};
use crate::error::{EngineError, Resource};
use crate::ids::{DatasetId, DatasetRef, Location, ProjectId};
use crate::storage::{Created, ListedKind};
use crate::types::Labels;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// A dataset as the catalog stores it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DatasetEntry {
    /// Recorded at creation and never changed.
    pub location: Location,
    pub description: Option<String>,
    pub labels: Labels,
    pub default_table_expiration_ms: Option<i64>,
    /// Counts the dataset's changes; it is the dataset's etag.
    pub generation: u64,
    pub creation_time_ms: i64,
    pub last_modified_time_ms: i64,
}

/// What a new dataset holds; the catalog adds the generation and the times.
#[derive(Debug, Clone)]
pub(crate) struct NewDataset {
    pub location: Location,
    pub description: Option<String>,
    pub labels: Labels,
    pub default_table_expiration_ms: Option<i64>,
}

impl DatasetEntry {
    /// The dataset's etag.
    pub(crate) fn etag(&self) -> String {
        self.generation.to_string()
    }
}

impl EngineCatalog {
    /// Creates the dataset.
    ///
    /// # Errors
    /// [`EngineError::AlreadyExists`] when it exists.
    pub(crate) async fn create_dataset(
        &self,
        dataset: &DatasetRef,
        new: NewDataset,
    ) -> Result<DatasetEntry, EngineError> {
        let now = now_ms();
        let entry = DatasetEntry {
            location: new.location,
            description: new.description,
            labels: new.labels,
            default_table_expiration_ms: new.default_table_expiration_ms,
            generation: 1,
            creation_time_ms: now,
            last_modified_time_ms: now,
        };
        let path = Self::dataset_file(dataset);
        match self
            .backend
            .create(&path, Self::dataset_json(&entry)?)
            .await?
        {
            Created::Created(_) => Ok(entry),
            Created::AlreadyExists => Err(EngineError::AlreadyExists {
                what: Resource::Dataset(dataset.clone()),
            }),
        }
    }

    /// The dataset.
    ///
    /// # Errors
    /// [`EngineError::NotFound`] when it does not exist.
    pub(crate) async fn dataset(&self, dataset: &DatasetRef) -> Result<DatasetEntry, EngineError> {
        let bytes = self
            .backend
            .read(&Self::dataset_file(dataset))
            .await?
            .ok_or_else(|| EngineError::NotFound {
                what: Resource::Dataset(dataset.clone()),
            })?;
        serde_json::from_slice(&bytes).map_err(|err| {
            EngineError::invalid(format!("catalog entry of {dataset}"), err.to_string())
        })
    }

    /// Applies `change` to the dataset and stores it under the next generation, when its
    /// current etag is `etag` or `etag` is `None`.
    ///
    /// # Errors
    /// [`EngineError::NotFound`], [`EngineError::Precondition`] for a stale etag, and whatever
    /// `change` refuses.
    pub(crate) async fn update_dataset(
        &self,
        dataset: &DatasetRef,
        etag: Option<&str>,
        change: impl FnOnce(&mut DatasetEntry) -> Result<(), EngineError>,
    ) -> Result<DatasetEntry, EngineError> {
        let lock = self.locks.dataset(dataset);
        let _guard = lock.write().await;
        let mut entry = self.dataset(dataset).await?;
        if let Some(etag) = etag.filter(|etag| *etag != entry.etag()) {
            return Err(EngineError::Precondition {
                what: Resource::Dataset(dataset.clone()),
                detail: format!(
                    "the etag {etag:?} is not the dataset's current etag {:?}",
                    entry.etag()
                ),
            });
        }
        change(&mut entry)?;
        entry.generation += 1;
        entry.last_modified_time_ms = now_ms();
        self.backend
            .write(&Self::dataset_file(dataset), Self::dataset_json(&entry)?)
            .await?;
        Ok(entry)
    }

    /// Deletes the dataset. With `delete_contents` every table is dropped and purged first,
    /// leaving its tombstone, so a dataset created again under the name continues each table's
    /// numbering; without it a dataset that holds a table is refused.
    ///
    /// # Errors
    /// [`EngineError::NotFound`], and [`EngineError::Precondition`] for a non-empty dataset
    /// without `delete_contents`.
    pub(crate) async fn delete_dataset(
        &self,
        dataset: &DatasetRef,
        delete_contents: bool,
    ) -> Result<(), EngineError> {
        let lock = self.locks.dataset(dataset);
        let _guard = lock.write().await;
        self.dataset(dataset).await?;
        let tables = self.table_names(dataset).await?;
        if !delete_contents && !tables.is_empty() {
            return Err(EngineError::Precondition {
                what: Resource::Dataset(dataset.clone()),
                detail: format!(
                    "the dataset holds {} tables; delete it with its contents",
                    tables.len()
                ),
            });
        }
        for table in tables {
            match self.purge(&dataset.table(table)).await {
                Err(EngineError::NotFound { .. }) | Ok(()) => {}
                Err(err) => return Err(err),
            }
        }
        self.backend.delete(&Self::dataset_file(dataset)).await?;
        Ok(())
    }

    /// The datasets of `project`, by name.
    pub(crate) async fn datasets(
        &self,
        project: &ProjectId,
    ) -> Result<Vec<(DatasetId, DatasetEntry)>, EngineError> {
        let mut datasets = Vec::new();
        for listed in self.backend.list(&Self::project_dir(project)).await? {
            let ListedKind::Object { .. } = listed.kind else {
                continue;
            };
            let Some(Ok(id)) = listed
                .name
                .strip_suffix(".json")
                .map(str::parse::<DatasetId>)
            else {
                continue;
            };
            let dataset = DatasetRef {
                project: project.clone(),
                dataset: id.clone(),
            };
            match self.dataset(&dataset).await {
                Ok(entry) => datasets.push((id, entry)),
                Err(EngineError::NotFound { .. }) => {}
                Err(err) => return Err(err),
            }
        }
        datasets.sort_by(|(a, _), (b, _)| a.cmp(b));
        Ok(datasets)
    }

    /// The projects that hold a dataset's catalog directory.
    pub(crate) async fn projects(&self) -> Result<Vec<ProjectId>, EngineError> {
        let mut projects: Vec<ProjectId> = self
            .backend
            .list(&Self::catalog_dir())
            .await?
            .into_iter()
            .filter(|listed| matches!(listed.kind, ListedKind::Directory))
            .filter_map(|listed| listed.name.parse().ok())
            .collect();
        projects.sort();
        Ok(projects)
    }

    fn dataset_json(entry: &DatasetEntry) -> Result<Bytes, EngineError> {
        serde_json::to_vec(entry)
            .map(Bytes::from)
            .map_err(|err| EngineError::invalid("dataset entry", err.to_string()))
    }
}
