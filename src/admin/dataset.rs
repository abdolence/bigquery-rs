//! Datasets: create, get, update, delete and list, through the v2 `DatasetService`.

use crate::admin::{duration_ms, non_empty, paged, timestamp_ms};
use crate::errors::{BigQueryDataConflictError, BigQueryError};
use crate::BigQueryInstant;
use crate::{BigQueryDatasetRef, BigQueryDb, BigQueryResult};
use crate::{BigQueryLabels, BigQueryLocation};
use futures::stream::BoxStream;
use gcloud_sdk::google::cloud::bigquery::v2;
use gcloud_sdk::tonic::metadata::{MetadataMap, MetadataValue};
use std::time::Duration;
use tracing::Span;

/// A dataset as `GetDataset` returns it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BigQueryDataset {
    /// The dataset, with its project.
    pub reference: BigQueryDatasetRef,
    /// Where its data is stored, such as `US` or `europe-west2`.
    pub location: Option<BigQueryLocation>,
    /// The display name.
    pub friendly_name: Option<String>,
    /// The description.
    pub description: Option<String>,
    /// The labels.
    pub labels: BigQueryLabels,
    /// How long a new table lives unless it sets its own expiration.
    pub default_table_expiration: Option<Duration>,
    /// How long a partition of a new partitioned table is kept, unless the table sets its own.
    pub default_partition_expiration: Option<Duration>,
    /// When the dataset was created.
    pub creation_time: Option<BigQueryInstant>,
    /// When the dataset or one of its tables was last changed.
    pub last_modified_time: Option<BigQueryInstant>,
}

/// A dataset as `ListDatasets` returns it, which is less than [`BigQueryDataset`]: read one with
/// [`BigQueryDatasetBuilder::get`](crate::BigQueryDatasetBuilder::get) for the rest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BigQueryDatasetSummary {
    /// The dataset, with its project.
    pub reference: BigQueryDatasetRef,
    /// Where its data is stored.
    pub location: Option<BigQueryLocation>,
    /// The display name.
    pub friendly_name: Option<String>,
    /// The labels.
    pub labels: BigQueryLabels,
}

/// # Errors
/// [`BigQueryError::InvalidParametersError`] for a missing or invalid dataset reference, and
/// [`BigQueryError::DeserializeError`] for a time or a duration out of range.
impl TryFrom<v2::Dataset> for BigQueryDataset {
    type Error = BigQueryError;

    fn try_from(dataset: v2::Dataset) -> Result<Self, Self::Error> {
        Ok(Self {
            reference: dataset_reference(dataset.dataset_reference)?,
            location: BigQueryLocation::reported(dataset.location),
            friendly_name: dataset.friendly_name.and_then(non_empty),
            description: dataset.description.and_then(non_empty),
            labels: dataset.labels.into_iter().collect(),
            default_table_expiration: duration_ms(
                "default_table_expiration_ms",
                dataset.default_table_expiration_ms,
            )?,
            default_partition_expiration: duration_ms(
                "default_partition_expiration_ms",
                dataset.default_partition_expiration_ms,
            )?,
            creation_time: timestamp_ms("creation_time", dataset.creation_time)?,
            last_modified_time: timestamp_ms("last_modified_time", dataset.last_modified_time)?,
        })
    }
}

/// # Errors
/// [`BigQueryError::InvalidParametersError`] for a missing or invalid dataset reference.
impl TryFrom<v2::ListFormatDataset> for BigQueryDatasetSummary {
    type Error = BigQueryError;

    fn try_from(dataset: v2::ListFormatDataset) -> Result<Self, Self::Error> {
        Ok(Self {
            reference: dataset_reference(dataset.dataset_reference)?,
            location: BigQueryLocation::reported(dataset.location),
            friendly_name: dataset.friendly_name.and_then(non_empty),
            labels: dataset.labels.into_iter().collect(),
        })
    }
}

fn dataset_reference(
    reference: Option<v2::DatasetReference>,
) -> BigQueryResult<BigQueryDatasetRef> {
    reference
        .ok_or_else(|| {
            BigQueryError::invalid_parameters("dataset_reference", "BigQuery returned none")
        })?
        .try_into()
}

/// The settings a new dataset is created with.
#[derive(Debug, Clone, Default)]
pub(crate) struct DatasetSettings {
    pub location: Option<BigQueryLocation>,
    pub description: Option<String>,
    pub labels: BigQueryLabels,
}

/// One change to a dataset's labels, applied in the order the caller made them.
#[derive(Debug, Clone)]
pub(crate) enum LabelEdit {
    ReplaceAll(BigQueryLabels),
    Set(String, String),
    Remove(String),
}

/// The changes an update makes to a dataset; what it leaves `None` or empty stays as it is.
#[derive(Debug, Clone, Default)]
pub(crate) struct DatasetEdits {
    /// `Some(None)` removes the description.
    pub description: Option<Option<String>>,
    pub labels: Vec<LabelEdit>,
}

impl DatasetEdits {
    fn apply(&self, dataset: &mut v2::Dataset) {
        if let Some(description) = &self.description {
            // `UpdateDataset` keeps a description that is left out, so a removal is sent as
            // empty, which reads back as none.
            dataset.description = Some(description.clone().unwrap_or_default());
        }
        for edit in &self.labels {
            match edit {
                LabelEdit::ReplaceAll(labels) => {
                    dataset.labels = labels.clone().into_iter().collect();
                }
                LabelEdit::Set(key, value) => {
                    dataset.labels.insert(key.clone(), value.clone());
                }
                LabelEdit::Remove(key) => {
                    dataset.labels.remove(key);
                }
            }
        }
    }
}

fn dataset_span(dataset: &BigQueryDatasetRef) -> Span {
    tracing::debug_span!("BigQuery dataset", "/bigquery/dataset" = %dataset)
}

impl BigQueryDb {
    fn dataset_project(&self, dataset: &BigQueryDatasetRef) -> String {
        dataset
            .project()
            .unwrap_or(&self.options().google_project_id)
            .to_string()
    }

    pub(crate) async fn create_dataset(
        &self,
        dataset: &BigQueryDatasetRef,
        settings: DatasetSettings,
    ) -> BigQueryResult<BigQueryDataset> {
        let project_id = self.dataset_project(dataset);
        let request = v2::InsertDatasetRequest {
            project_id: project_id.clone(),
            dataset: Some(v2::Dataset {
                dataset_reference: Some(v2::DatasetReference {
                    dataset_id: dataset.dataset().to_string(),
                    project_id,
                }),
                location: settings.location.map(|l| l.to_string()).unwrap_or_default(),
                description: settings.description,
                labels: settings.labels.into_iter().collect(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let span = dataset_span(dataset);
        self.retry(
            &span,
            "create a dataset",
            &request,
            &MetadataMap::new(),
            |r| {
                let mut client = self.dataset_client();
                async move { client.insert_dataset(r).await }
            },
        )
        .await?
        .try_into()
    }

    async fn get_raw_dataset(
        &self,
        dataset: &BigQueryDatasetRef,
        span: &Span,
    ) -> BigQueryResult<v2::Dataset> {
        let request = v2::GetDatasetRequest {
            project_id: self.dataset_project(dataset),
            dataset_id: dataset.dataset().to_string(),
            dataset_view: v2::get_dataset_request::DatasetView::Metadata.into(),
            ..Default::default()
        };
        self.retry(span, "get a dataset", &request, &MetadataMap::new(), |r| {
            let mut client = self.dataset_client();
            async move { client.get_dataset(r).await }
        })
        .await
    }

    pub(crate) async fn get_dataset(
        &self,
        dataset: &BigQueryDatasetRef,
    ) -> BigQueryResult<BigQueryDataset> {
        self.get_raw_dataset(dataset, &dataset_span(dataset))
            .await?
            .try_into()
    }

    pub(crate) async fn update_dataset(
        &self,
        dataset: &BigQueryDatasetRef,
        edits: DatasetEdits,
    ) -> BigQueryResult<BigQueryDataset> {
        let span = dataset_span(dataset);
        let mut body = self.get_raw_dataset(dataset, &span).await?;
        let etag = body.etag.clone();
        edits.apply(&mut body);
        let precondition = MetadataValue::try_from(etag.as_str()).map_err(|_| {
            BigQueryError::invalid_parameters(
                "etag",
                format!("GetDataset returned an etag that is not a valid header: {etag:?}"),
            )
        })?;
        let mut metadata = MetadataMap::new();
        metadata.insert("if-match", precondition);
        let request = v2::UpdateOrPatchDatasetRequest {
            project_id: self.dataset_project(dataset),
            dataset_id: dataset.dataset().to_string(),
            dataset: Some(body),
            update_mode: v2::update_or_patch_dataset_request::UpdateMode::UpdateMetadata.into(),
            ..Default::default()
        };
        let result = self
            .retry(&span, "update a dataset", &request, &metadata, |r| {
                let mut client = self.dataset_client();
                async move { client.update_dataset(r).await }
            })
            .await;
        match result {
            Ok(updated) => updated.try_into(),
            Err(BigQueryError::DatabaseError(err)) if err.public.code == "FailedPrecondition" => {
                Err(BigQueryError::DataConflictError(
                    BigQueryDataConflictError::new(
                        err.public,
                        format!(
                            "{dataset} changed since this update read it (etag {etag}); nothing \
                             was written, run the update again. {}",
                            err.details
                        ),
                    ),
                ))
            }
            Err(err) => Err(err),
        }
    }

    pub(crate) async fn delete_dataset(
        &self,
        dataset: &BigQueryDatasetRef,
        delete_contents: bool,
    ) -> BigQueryResult<()> {
        let request = v2::DeleteDatasetRequest {
            project_id: self.dataset_project(dataset),
            dataset_id: dataset.dataset().to_string(),
            delete_contents,
        };
        let span = dataset_span(dataset);
        self.retry(
            &span,
            "delete a dataset",
            &request,
            &MetadataMap::new(),
            |r| {
                let mut client = self.dataset_client();
                async move { client.delete_dataset(r).await }
            },
        )
        .await
    }

    pub(crate) fn list_datasets(
        &self,
        project: Option<String>,
        page_size: Option<u32>,
    ) -> BoxStream<'static, BigQueryResult<BigQueryDatasetSummary>> {
        let db = self.clone();
        let project_id = project.unwrap_or_else(|| self.options().google_project_id.clone());
        let span = tracing::debug_span!("BigQuery datasets", "/bigquery/project" = %project_id);
        paged(move |page_token| {
            let db = db.clone();
            let span = span.clone();
            let request = v2::ListDatasetsRequest {
                project_id: project_id.clone(),
                max_results: page_size,
                page_token,
                ..Default::default()
            };
            async move {
                let page = db
                    .retry(&span, "list datasets", &request, &MetadataMap::new(), |r| {
                        let mut client = db.dataset_client();
                        async move { client.list_datasets(r).await }
                    })
                    .await?;
                let datasets = page
                    .datasets
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<BigQueryResult<_>>()?;
                Ok((datasets, page.next_page_token))
            }
        })
    }
}
