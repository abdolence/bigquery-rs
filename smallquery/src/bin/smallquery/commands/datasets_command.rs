use super::{call_failed, counted, open_existing_engine, INSPECTION_GRACE};
use crate::args::EngineRoot;
use anyhow::Context;
use console::{pad_str, Alignment, Style, Term};
use gcloud_sdk::google::cloud::bigquery::v2;
use gcloud_sdk::google::cloud::bigquery::v2::dataset_service_client::DatasetServiceClient;
use smallquery::ProjectId;
use tonic::transport::Channel;

pub async fn command_datasets(
    term: &Term,
    root: EngineRoot,
    project: Option<ProjectId>,
) -> anyhow::Result<()> {
    let projects = match project {
        Some(project) => vec![project.to_string()],
        None => catalog_projects(&root)?,
    };
    let engine = open_existing_engine(&root).await?;
    let mut client = DatasetServiceClient::new(engine.channel());
    let mut rows = Vec::new();
    for project in &projects {
        for dataset in list_datasets(&mut client, project).await? {
            let id = dataset
                .dataset_reference
                .map(|reference| reference.dataset_id)
                .unwrap_or_default();
            rows.push((project.clone(), id, dataset.location));
        }
    }
    engine.shutdown(INSPECTION_GRACE).await;

    let bold_style = Style::new().bold();
    let highlighted = bold_style.clone().white();
    let dimmed_style = Style::new().dim();
    let id_width = rows
        .iter()
        .map(|(_, id, _)| id.len())
        .max()
        .unwrap_or(0)
        .max(7)
        + 2;
    let project_width = rows
        .iter()
        .map(|(project, ..)| project.len())
        .max()
        .unwrap_or(0)
        .max(7)
        + 2;
    if !rows.is_empty() {
        term.write_line(&format!(
            "\n  {} {} {}",
            dimmed_style.apply_to(pad_str("Dataset", id_width, Alignment::Left, None)),
            dimmed_style.apply_to(pad_str("Project", project_width, Alignment::Left, None)),
            dimmed_style.apply_to("Location"),
        ))?;
        for (project, id, location) in &rows {
            term.write_line(&format!(
                "- {} {} {}",
                highlighted.apply_to(pad_str(id, id_width, Alignment::Left, None)),
                pad_str(project, project_width, Alignment::Left, None),
                location,
            ))?;
        }
        term.write_line("")?;
    }
    term.write_line(&format!(
        "{} in {}.",
        highlighted.apply_to(counted(rows.len(), "dataset")),
        highlighted.apply_to(counted(projects.len(), "project"))
    ))?;
    Ok(())
}

/// Every dataset of `project`, across pages.
async fn list_datasets(
    client: &mut DatasetServiceClient<Channel>,
    project: &str,
) -> anyhow::Result<Vec<v2::ListFormatDataset>> {
    let mut datasets = Vec::new();
    let mut page_token = String::new();
    loop {
        let page = client
            .list_datasets(v2::ListDatasetsRequest {
                project_id: project.to_string(),
                page_token: page_token.clone(),
                ..Default::default()
            })
            .await
            .map_err(call_failed)
            .with_context(|| format!("Cannot list the datasets of {project}"))?
            .into_inner();
        datasets.extend(page.datasets);
        if page.next_page_token.is_empty() {
            return Ok(datasets);
        }
        page_token = page.next_page_token;
    }
}

/// The projects of a local root. The v2 API has no call that lists projects, so they are read
/// from the catalog's layout, where a project is a directory that its first dataset creates.
fn catalog_projects(root: &EngineRoot) -> anyhow::Result<Vec<String>> {
    let Some(dir) = root.local_dir() else {
        anyhow::bail!(
            "{} is not a local root; name the project with --project",
            root.url()
        );
    };
    let catalog = dir.join("_smallquery").join("catalog");
    if !catalog.is_dir() {
        return Ok(Vec::new());
    }
    let mut projects = Vec::new();
    for entry in
        std::fs::read_dir(&catalog).with_context(|| format!("Cannot read {}", catalog.display()))?
    {
        let entry = entry.with_context(|| format!("Cannot read {}", catalog.display()))?;
        if entry.file_type()?.is_dir() {
            if let Some(name) = entry.file_name().to_str() {
                projects.push(name.to_string());
            }
        }
    }
    projects.sort();
    Ok(projects)
}
