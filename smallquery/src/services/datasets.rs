//! `google.cloud.bigquery.v2.DatasetService`.

use super::{if_match, page, respond, unimplemented, Services};
use crate::catalog::{DatasetEntry, NewDataset};
use crate::error::EngineError;
use crate::ids::{DatasetRef, Location, ProjectId};
use crate::proto::bigquery_v2::dataset_service_server::DatasetService;
use gcloud_sdk::google::cloud::bigquery::v2;
use tonic::{Request, Response, Status};
use tracing::Instrument;

pub(crate) struct Datasets(pub(super) Services);

impl DatasetEntry {
    /// The dataset as `GetDataset` reports it.
    fn v2_dataset(&self, dataset: &DatasetRef) -> v2::Dataset {
        v2::Dataset {
            kind: "bigquery#dataset".to_string(),
            etag: self.etag(),
            id: dataset.to_string(),
            dataset_reference: Some(dataset.into()),
            description: self.description.clone(),
            default_table_expiration_ms: self.default_table_expiration_ms,
            labels: self.labels.clone().into_iter().collect(),
            creation_time: self.creation_time_ms,
            last_modified_time: self.last_modified_time_ms,
            location: self.location.to_string(),
            r#type: "DEFAULT".to_string(),
            ..Default::default()
        }
    }

    /// The dataset as `ListDatasets` reports it.
    fn list_format(&self, dataset: &DatasetRef) -> v2::ListFormatDataset {
        v2::ListFormatDataset {
            kind: "bigquery#dataset".to_string(),
            id: dataset.to_string(),
            dataset_reference: Some(dataset.into()),
            labels: self.labels.clone().into_iter().collect(),
            location: self.location.to_string(),
            r#type: "DEFAULT".to_string(),
            ..Default::default()
        }
    }
}

impl From<&DatasetRef> for v2::DatasetReference {
    fn from(dataset: &DatasetRef) -> Self {
        v2::DatasetReference {
            dataset_id: dataset.dataset.to_string(),
            project_id: dataset.project.to_string(),
        }
    }
}

/// The settable fields of a dataset body the engine does not implement, and whether each is
/// set.
fn unsupported_fields(body: &v2::Dataset) -> [(&'static str, bool); 14] {
    [
        (
            "friendly_name",
            body.friendly_name.as_ref().is_some_and(|v| !v.is_empty()),
        ),
        (
            "default_partition_expiration_ms",
            body.default_partition_expiration_ms
                .is_some_and(|ms| ms > 0),
        ),
        ("dataset access entries", !body.access.is_empty()),
        (
            "default_encryption_configuration",
            body.default_encryption_configuration.is_some(),
        ),
        ("linked datasets", body.linked_dataset_source.is_some()),
        (
            "external datasets",
            body.external_dataset_reference.is_some(),
        ),
        (
            "external_catalog_dataset_options",
            body.external_catalog_dataset_options.is_some(),
        ),
        (
            "case-insensitive datasets",
            body.is_case_insensitive == Some(true),
        ),
        (
            "default_collation",
            body.default_collation
                .as_ref()
                .is_some_and(|v| !v.is_empty()),
        ),
        ("default_rounding_mode", body.default_rounding_mode != 0),
        (
            "max_time_travel_hours",
            body.max_time_travel_hours.is_some(),
        ),
        ("storage_billing_model", body.storage_billing_model != 0),
        ("restrictions", body.restrictions.is_some()),
        ("resource_tags", !body.resource_tags.is_empty()),
    ]
}

impl Datasets {
    async fn insert(&self, request: v2::InsertDatasetRequest) -> Result<v2::Dataset, EngineError> {
        let body = request
            .dataset
            .ok_or_else(|| EngineError::invalid("dataset", "the request carries no dataset"))?;
        EngineError::refuse_unsupported("dataset", &unsupported_fields(&body))?;
        let reference = body.dataset_reference.clone().unwrap_or_default();
        let project_id = match reference.project_id.as_str() {
            "" => request.project_id.as_str(),
            project_id => project_id,
        };
        if project_id != request.project_id {
            return Err(EngineError::invalid(
                "dataset_reference.project_id",
                format!(
                    "{project_id:?} is not the request's project {:?}",
                    request.project_id
                ),
            ));
        }
        let dataset = DatasetRef::parse(project_id, &reference.dataset_id)?;
        let location = match body.location.as_str() {
            "" => self.0.catalog().settings().default_location.clone(),
            location => location.parse::<Location>()?,
        };
        let entry = self
            .0
            .catalog()
            .create_dataset(
                &dataset,
                NewDataset {
                    location,
                    description: body.description.filter(|d| !d.is_empty()),
                    labels: body.labels.into_iter().collect(),
                    default_table_expiration_ms: body
                        .default_table_expiration_ms
                        .filter(|ms| *ms > 0),
                },
            )
            .await?;
        Ok(entry.v2_dataset(&dataset))
    }

    async fn update(
        &self,
        request: v2::UpdateOrPatchDatasetRequest,
        etag: Option<String>,
    ) -> Result<v2::Dataset, EngineError> {
        let dataset = DatasetRef::parse(&request.project_id, &request.dataset_id)?;
        let body = request
            .dataset
            .ok_or_else(|| EngineError::invalid("dataset", "the request carries no dataset"))?;
        EngineError::refuse_unsupported("dataset", &unsupported_fields(&body))?;
        let location = Some(body.location.as_str())
            .filter(|location| !location.is_empty())
            .map(str::parse::<Location>)
            .transpose()?;
        let entry = self
            .0
            .catalog()
            .update_dataset(&dataset, etag.as_deref(), |entry| {
                if let Some(location) = location.filter(|location| *location != entry.location) {
                    return Err(EngineError::invalid(
                        format!("location of {dataset}"),
                        format!(
                            "{location} is not the dataset's location {}; a dataset's location \
                             cannot change",
                            entry.location
                        ),
                    ));
                }
                if let Some(description) = body.description {
                    entry.description = Some(description).filter(|d| !d.is_empty());
                }
                entry.labels = body.labels.into_iter().collect();
                entry.default_table_expiration_ms =
                    body.default_table_expiration_ms.filter(|ms| *ms > 0);
                Ok(())
            })
            .await?;
        Ok(entry.v2_dataset(&dataset))
    }

    async fn list(&self, request: v2::ListDatasetsRequest) -> Result<v2::DatasetList, EngineError> {
        if !request.filter.is_empty() {
            return Err(EngineError::unsupported(
                "the ListDatasets filter",
                format!("{:?}", request.filter),
            ));
        }
        let project = request.project_id.parse::<ProjectId>()?;
        let datasets: Vec<v2::ListFormatDataset> = self
            .0
            .catalog()
            .datasets(&project)
            .await?
            .into_iter()
            .filter(|(id, _)| request.all || !id.as_str().starts_with('_'))
            .map(|(id, entry)| {
                entry.list_format(&DatasetRef {
                    project: project.clone(),
                    dataset: id,
                })
            })
            .collect();
        let (datasets, next_page_token) = page(datasets, request.max_results, &request.page_token)?;
        Ok(v2::DatasetList {
            kind: "bigquery#datasetList".to_string(),
            next_page_token,
            datasets,
            ..Default::default()
        })
    }
}

const GET: &str = "google.cloud.bigquery.v2.DatasetService/GetDataset";
const INSERT: &str = "google.cloud.bigquery.v2.DatasetService/InsertDataset";
const PATCH: &str = "google.cloud.bigquery.v2.DatasetService/PatchDataset";
const UPDATE: &str = "google.cloud.bigquery.v2.DatasetService/UpdateDataset";
const DELETE: &str = "google.cloud.bigquery.v2.DatasetService/DeleteDataset";
const LIST: &str = "google.cloud.bigquery.v2.DatasetService/ListDatasets";
const UNDELETE: &str = "google.cloud.bigquery.v2.DatasetService/UndeleteDataset";

#[tonic::async_trait]
impl DatasetService for Datasets {
    async fn get_dataset(
        &self,
        request: Request<v2::GetDatasetRequest>,
    ) -> Result<Response<v2::Dataset>, Status> {
        let request = request.into_inner();
        let span = self.0.span(GET, &request.project_id);
        let result = async {
            let dataset = DatasetRef::parse(&request.project_id, &request.dataset_id)?;
            Ok(self
                .0
                .catalog()
                .dataset(&dataset)
                .await?
                .v2_dataset(&dataset))
        };
        respond(GET, result.instrument(span).await)
    }

    async fn insert_dataset(
        &self,
        request: Request<v2::InsertDatasetRequest>,
    ) -> Result<Response<v2::Dataset>, Status> {
        let request = request.into_inner();
        let span = self.0.span(INSERT, &request.project_id);
        respond(INSERT, self.insert(request).instrument(span).await)
    }

    async fn patch_dataset(
        &self,
        _request: Request<v2::UpdateOrPatchDatasetRequest>,
    ) -> Result<Response<v2::Dataset>, Status> {
        unimplemented(PATCH)
    }

    async fn update_dataset(
        &self,
        request: Request<v2::UpdateOrPatchDatasetRequest>,
    ) -> Result<Response<v2::Dataset>, Status> {
        let etag = if_match(&request);
        let request = request.into_inner();
        let span = self.0.span(UPDATE, &request.project_id);
        respond(UPDATE, self.update(request, etag).instrument(span).await)
    }

    async fn delete_dataset(
        &self,
        request: Request<v2::DeleteDatasetRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        let span = self.0.span(DELETE, &request.project_id);
        let result = async {
            let dataset = DatasetRef::parse(&request.project_id, &request.dataset_id)?;
            self.0
                .catalog()
                .delete_dataset(&dataset, request.delete_contents)
                .await
        };
        respond(DELETE, result.instrument(span).await)
    }

    async fn list_datasets(
        &self,
        request: Request<v2::ListDatasetsRequest>,
    ) -> Result<Response<v2::DatasetList>, Status> {
        let request = request.into_inner();
        let span = self.0.span(LIST, &request.project_id);
        respond(LIST, self.list(request).instrument(span).await)
    }

    async fn undelete_dataset(
        &self,
        _request: Request<v2::UndeleteDatasetRequest>,
    ) -> Result<Response<v2::Dataset>, Status> {
        unimplemented(UNDELETE)
    }
}
