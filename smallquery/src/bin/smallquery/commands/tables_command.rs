use super::{call_failed, counted, open_existing_engine, INSPECTION_GRACE};
use crate::args::EngineRoot;
use anyhow::Context;
use console::{pad_str, Alignment, Style, Term};
use gcloud_sdk::google::cloud::bigquery::v2;
use gcloud_sdk::google::cloud::bigquery::v2::table_service_client::TableServiceClient;
use smallquery::DatasetRef;

pub async fn command_tables(
    term: &Term,
    root: EngineRoot,
    dataset: DatasetRef,
) -> anyhow::Result<()> {
    let engine = open_existing_engine(&root).await?;
    let mut client = TableServiceClient::new(engine.channel());
    let mut tables = Vec::new();
    let mut page_token = String::new();
    loop {
        let page = client
            .list_tables(v2::ListTablesRequest {
                project_id: dataset.project.to_string(),
                dataset_id: dataset.dataset.to_string(),
                page_token: page_token.clone(),
                ..Default::default()
            })
            .await
            .map_err(call_failed)
            .with_context(|| format!("Cannot list the tables of {dataset}"))?
            .into_inner();
        tables.extend(page.tables);
        if page.next_page_token.is_empty() {
            break;
        }
        page_token = page.next_page_token;
    }
    engine.shutdown(INSPECTION_GRACE).await;

    let bold_style = Style::new().bold();
    let highlighted = bold_style.clone().white();
    let dimmed_style = Style::new().dim();
    let rows: Vec<(String, String)> = tables
        .into_iter()
        .map(|table| {
            let id = table
                .table_reference
                .map(|reference| reference.table_id)
                .unwrap_or_default();
            (id, table.r#type)
        })
        .collect();
    let id_width = rows
        .iter()
        .map(|(id, _)| id.len())
        .max()
        .unwrap_or(0)
        .max(5)
        + 2;
    if !rows.is_empty() {
        term.write_line(&format!(
            "\n  {} {}",
            dimmed_style.apply_to(pad_str("Table", id_width, Alignment::Left, None)),
            dimmed_style.apply_to("Type"),
        ))?;
        for (id, table_type) in &rows {
            term.write_line(&format!(
                "- {} {}",
                highlighted.apply_to(pad_str(id, id_width, Alignment::Left, None)),
                table_type,
            ))?;
        }
        term.write_line("")?;
    }
    term.write_line(&format!(
        "{} in {}.",
        highlighted.apply_to(counted(rows.len(), "table")),
        bold_style.apply_to(&dataset)
    ))?;
    Ok(())
}
