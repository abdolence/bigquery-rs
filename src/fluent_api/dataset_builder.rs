//! Builders for the plain dataset and table calls under
//! [`BigQuerySchemaBuilder`](crate::BigQuerySchemaBuilder): create, get, update, delete and
//! list.

use crate::admin::{logging_errors, DatasetEdits, DatasetSettings, LabelEdit};
use crate::{
    BigQueryDataset, BigQueryDatasetRef, BigQueryDatasetSummary, BigQueryDb, BigQueryResult,
    BigQueryTableSummary,
};
use crate::{BigQueryLabels, BigQueryLocation};
use futures::stream::BoxStream;
use std::time::Duration;

/// One dataset, from [`BigQuerySchemaBuilder::dataset`](crate::BigQuerySchemaBuilder::dataset).
/// End it with one of its calls.
#[derive(Clone, Debug)]
pub struct BigQueryDatasetBuilder<'a> {
    db: &'a BigQueryDb,
    dataset: BigQueryDatasetRef,
}

impl<'a> BigQueryDatasetBuilder<'a> {
    pub(crate) fn new(db: &'a BigQueryDb, dataset: BigQueryDatasetRef) -> Self {
        Self { db, dataset }
    }

    /// Starts creating the dataset. Continue with its settings, then
    /// [`execute`](BigQueryDatasetCreateBuilder::execute).
    #[inline]
    pub fn create(self) -> BigQueryDatasetCreateBuilder<'a> {
        BigQueryDatasetCreateBuilder {
            db: self.db,
            dataset: self.dataset,
            settings: DatasetSettings::default(),
        }
    }

    /// Reads the dataset.
    ///
    /// # Errors
    /// [`DataNotFoundError`](crate::errors::BigQueryError::DataNotFoundError) for a dataset that
    /// does not exist.
    pub async fn get(self) -> BigQueryResult<BigQueryDataset> {
        self.db.get_dataset(&self.dataset).await
    }

    /// Starts changing the dataset's description and labels. Continue with the changes, then
    /// [`execute`](BigQueryDatasetUpdateBuilder::execute).
    #[inline]
    pub fn update(self) -> BigQueryDatasetUpdateBuilder<'a> {
        BigQueryDatasetUpdateBuilder {
            db: self.db,
            dataset: self.dataset,
            edits: DatasetEdits::default(),
        }
    }

    /// Deletes the dataset if it holds no tables, views, models or routines.
    ///
    /// # Errors
    /// What BigQuery returns for a dataset that is not empty, with nothing deleted, and
    /// [`DataNotFoundError`](crate::errors::BigQueryError::DataNotFoundError) for one that does
    /// not exist. A retry after a lost response can report the dataset it deleted as not found.
    pub async fn delete(self) -> BigQueryResult<()> {
        self.db.delete_dataset(&self.dataset, false).await
    }

    /// **Deletes every table in the dataset with every row in it**, and its views, models and
    /// routines, then the dataset. Nothing is kept for an undo.
    ///
    /// # Errors
    /// As for [`delete`](Self::delete), less the refusal of a dataset that is not empty.
    pub async fn dangerously_delete_with_contents(self) -> BigQueryResult<()> {
        self.db.delete_dataset(&self.dataset, true).await
    }

    /// Starts listing the dataset's tables, views and other table-like objects.
    #[inline]
    pub fn tables(self) -> BigQueryTableListBuilder<'a> {
        BigQueryTableListBuilder {
            db: self.db,
            dataset: self.dataset,
            page_size: None,
        }
    }
}

/// A new dataset's settings, from [`BigQueryDatasetBuilder::create`].
#[derive(Clone, Debug)]
pub struct BigQueryDatasetCreateBuilder<'a> {
    db: &'a BigQueryDb,
    dataset: BigQueryDatasetRef,
    settings: DatasetSettings,
}

impl BigQueryDatasetCreateBuilder<'_> {
    /// Where the dataset's data is stored, such as `US`, `EU` or `europe-west2`. It cannot
    /// change later. Unset, BigQuery picks `US`.
    #[inline]
    pub fn location(mut self, location: BigQueryLocation) -> Self {
        self.settings.location = Some(location);
        self
    }

    /// The description.
    #[inline]
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.settings.description = Some(description.into());
        self
    }

    /// How long after its creation BigQuery deletes a table created in the dataset, unless the
    /// table sets its own expiration.
    #[inline]
    pub fn default_table_expiration(mut self, expiration: Duration) -> Self {
        self.settings.default_table_expiration = Some(expiration);
        self
    }

    /// The labels. Each call replaces the labels of a previous one.
    #[inline]
    pub fn labels(mut self, labels: impl Into<BigQueryLabels>) -> Self {
        self.settings.labels = labels.into();
        self
    }

    /// Creates the dataset and returns it as BigQuery stored it.
    ///
    /// # Errors
    /// [`DataConflictError`](crate::errors::BigQueryError::DataConflictError) for a dataset that
    /// already exists. A retry after a lost response reports the dataset it created this way.
    /// [`InvalidParametersError`](crate::errors::BigQueryError::InvalidParametersError) for a
    /// default table expiration beyond `i64` milliseconds.
    pub async fn execute(self) -> BigQueryResult<BigQueryDataset> {
        self.db.create_dataset(&self.dataset, self.settings).await
    }
}

/// Changes to a dataset, from [`BigQueryDatasetBuilder::update`]. What it does not change stays
/// as the dataset has it.
#[derive(Clone, Debug)]
pub struct BigQueryDatasetUpdateBuilder<'a> {
    db: &'a BigQueryDb,
    dataset: BigQueryDatasetRef,
    edits: DatasetEdits,
}

impl BigQueryDatasetUpdateBuilder<'_> {
    /// Sets the description.
    #[inline]
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.edits.description = Some(Some(description.into()));
        self
    }

    /// Removes the description.
    #[inline]
    pub fn clear_description(mut self) -> Self {
        self.edits.description = Some(None);
        self
    }

    /// Replaces every label with these. Label changes apply in the order they are made.
    #[inline]
    pub fn labels(mut self, labels: impl Into<BigQueryLabels>) -> Self {
        self.edits.labels.push(LabelEdit::ReplaceAll(labels.into()));
        self
    }

    /// Adds the label `key`, or changes its value.
    #[inline]
    pub fn label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.edits
            .labels
            .push(LabelEdit::Set(key.into(), value.into()));
        self
    }

    /// Removes the label `key`, if the dataset has it.
    #[inline]
    pub fn remove_label(mut self, key: impl Into<String>) -> Self {
        self.edits.labels.push(LabelEdit::Remove(key.into()));
        self
    }

    /// Reads the dataset, makes the changes, and writes it back with `UpdateDataset` under the
    /// etag it read as an `if-match` precondition. Only the metadata is written; the access
    /// list stays as it is.
    ///
    /// # Errors
    /// [`DataConflictError`](crate::errors::BigQueryError::DataConflictError) when the dataset
    /// changed between the read and the write, nothing written; run it again.
    pub async fn execute(self) -> BigQueryResult<BigQueryDataset> {
        self.db.update_dataset(&self.dataset, self.edits).await
    }
}

/// A listing of the datasets in a project, from
/// [`BigQuerySchemaBuilder::datasets`](crate::BigQuerySchemaBuilder::datasets).
#[derive(Clone, Debug)]
pub struct BigQueryDatasetListBuilder<'a> {
    db: &'a BigQueryDb,
    project: Option<String>,
    page_size: Option<u32>,
}

impl<'a> BigQueryDatasetListBuilder<'a> {
    pub(crate) fn new(db: &'a BigQueryDb) -> Self {
        Self {
            db,
            project: None,
            page_size: None,
        }
    }

    /// Lists the datasets of `project` rather than of the client's.
    #[inline]
    pub fn project(mut self, project: impl Into<String>) -> Self {
        self.project = Some(project.into());
        self
    }

    /// How many datasets one `ListDatasets` call returns; unset is BigQuery's default.
    #[inline]
    pub fn page_size(mut self, page_size: u32) -> Self {
        self.page_size = Some(page_size);
        self
    }

    /// Streams the datasets, fetching each page as the stream reaches it. A failed page is
    /// logged and ends the stream; see [`stream_all_with_errors`](Self::stream_all_with_errors)
    /// to see it.
    pub async fn stream_all<'b>(self) -> BigQueryResult<BoxStream<'b, BigQueryDatasetSummary>> {
        Ok(logging_errors(
            self.stream_all_with_errors().await?,
            "datasets",
        ))
    }

    /// Like [`stream_all`](Self::stream_all), with a failed page as the stream's last item.
    pub async fn stream_all_with_errors<'b>(
        self,
    ) -> BigQueryResult<BoxStream<'b, BigQueryResult<BigQueryDatasetSummary>>> {
        Ok(self.db.list_datasets(self.project, self.page_size))
    }
}

/// A listing of one dataset's tables, from [`BigQueryDatasetBuilder::tables`].
#[derive(Clone, Debug)]
pub struct BigQueryTableListBuilder<'a> {
    db: &'a BigQueryDb,
    dataset: BigQueryDatasetRef,
    page_size: Option<u32>,
}

impl BigQueryTableListBuilder<'_> {
    /// How many tables one `ListTables` call returns; unset is BigQuery's default.
    #[inline]
    pub fn page_size(mut self, page_size: u32) -> Self {
        self.page_size = Some(page_size);
        self
    }

    /// Streams the tables, fetching each page as the stream reaches it. A failed page is logged
    /// and ends the stream; see [`stream_all_with_errors`](Self::stream_all_with_errors) to see
    /// it.
    pub async fn stream_all<'b>(self) -> BigQueryResult<BoxStream<'b, BigQueryTableSummary>> {
        Ok(logging_errors(
            self.stream_all_with_errors().await?,
            "tables",
        ))
    }

    /// Like [`stream_all`](Self::stream_all), with a failed page as the stream's last item.
    pub async fn stream_all_with_errors<'b>(
        self,
    ) -> BigQueryResult<BoxStream<'b, BigQueryResult<BigQueryTableSummary>>> {
        Ok(self.db.list_tables(&self.dataset, self.page_size))
    }
}
