use super::{call_failed, open_existing_engine, INSPECTION_GRACE};
use crate::args::EngineRoot;
use anyhow::Context;
use console::{pad_str, Alignment, Style, Term};
use gcloud_sdk::google::cloud::bigquery::v2;
use gcloud_sdk::google::cloud::bigquery::v2::table_service_client::TableServiceClient;
use smallquery::TableRef;

pub async fn command_show(
    term: &Term,
    root: EngineRoot,
    table_ref: TableRef,
) -> anyhow::Result<()> {
    let engine = open_existing_engine(&root).await?;
    let table = TableServiceClient::new(engine.channel())
        .get_table(v2::GetTableRequest {
            project_id: table_ref.project.to_string(),
            dataset_id: table_ref.dataset.to_string(),
            table_id: table_ref.table.to_string(),
            ..Default::default()
        })
        .await
        .map_err(call_failed)
        .with_context(|| format!("Cannot read {table_ref}"))?
        .into_inner();
    engine.shutdown(INSPECTION_GRACE).await;

    let bold_style = Style::new().bold();
    let highlighted = bold_style.clone().white();
    let dimmed_style = Style::new().dim();
    let mut columns = Vec::new();
    if let Some(schema) = &table.schema {
        ColumnRow::collect(&schema.fields, "", &mut columns);
    }
    let name_width = columns
        .iter()
        .map(|c| c.name.len())
        .max()
        .unwrap_or(0)
        .max(6)
        + 2;
    let type_width = columns
        .iter()
        .map(|c| c.column_type.len())
        .max()
        .unwrap_or(0)
        .max(4)
        + 2;

    term.write_line(&format!("\n{}", bold_style.apply_to(&table_ref)))?;
    if let Some(description) = table.description.as_deref().filter(|d| !d.is_empty()) {
        term.write_line(description)?;
    }
    term.write_line(&format!(
        "\n  {} {} {}",
        dimmed_style.apply_to(pad_str("Column", name_width, Alignment::Left, None)),
        dimmed_style.apply_to(pad_str("Type", type_width, Alignment::Left, None)),
        dimmed_style.apply_to("Mode"),
    ))?;
    for column in &columns {
        term.write_line(&format!(
            "  {} {} {}",
            highlighted.apply_to(pad_str(&column.name, name_width, Alignment::Left, None)),
            pad_str(&column.column_type, type_width, Alignment::Left, None),
            column.mode,
        ))?;
    }
    term.write_line("")?;
    for (label, value) in [
        ("Partitioning:", partitioning(&table)),
        ("Clustering:", clustering(&table)),
        ("Max staleness:", none_if_empty(&table.max_staleness)),
        ("Location:", none_if_empty(&table.location)),
        ("Pointer version:", none_if_empty(&table.etag)),
    ] {
        term.write_line(&format!(
            "{} {}",
            dimmed_style.apply_to(pad_str(label, 17, Alignment::Left, None)),
            highlighted.apply_to(value)
        ))?;
    }
    Ok(())
}

/// One column of the schema, a nested one named by its dotted path.
struct ColumnRow {
    name: String,
    column_type: String,
    mode: String,
}

impl ColumnRow {
    /// Appends `fields` and every field nested in them, depth first, in schema order.
    fn collect(fields: &[v2::TableFieldSchema], parent: &str, rows: &mut Vec<ColumnRow>) {
        for field in fields {
            let name = if parent.is_empty() {
                field.name.clone()
            } else {
                format!("{parent}.{}", field.name)
            };
            rows.push(ColumnRow {
                name: name.clone(),
                column_type: column_type(field),
                mode: if field.mode.is_empty() {
                    "NULLABLE".to_string()
                } else {
                    field.mode.clone()
                },
            });
            Self::collect(&field.fields, &name, rows);
        }
    }
}

/// The type as BigQuery's DDL writes it, with its parameters.
fn column_type(field: &v2::TableFieldSchema) -> String {
    let base = field.r#type.as_str();
    if let Some(element) = &field.range_element_type {
        return format!("{base}<{}>", element.r#type);
    }
    match (field.max_length, field.precision, field.scale) {
        (length, ..) if length > 0 => format!("{base}({length})"),
        (_, precision, 0) if precision > 0 => format!("{base}({precision})"),
        (_, precision, scale) if precision > 0 => format!("{base}({precision},{scale})"),
        _ => base.to_string(),
    }
}

fn partitioning(table: &v2::Table) -> String {
    if let Some(time) = &table.time_partitioning {
        let column = time.field.as_deref().unwrap_or("_PARTITIONTIME");
        return match time.expiration_ms {
            Some(expiration_ms) => {
                format!(
                    "{} on {column}, partitions expire after {expiration_ms} ms",
                    time.r#type
                )
            }
            None => format!("{} on {column}", time.r#type),
        };
    }
    if let Some(range) = &table.range_partitioning {
        return match &range.range {
            Some(bounds) => format!(
                "RANGE on {} from {} to {} by {}",
                range.field, bounds.start, bounds.end, bounds.interval
            ),
            None => format!("RANGE on {}", range.field),
        };
    }
    "none".to_string()
}

fn clustering(table: &v2::Table) -> String {
    table
        .clustering
        .as_ref()
        .filter(|clustering| !clustering.fields.is_empty())
        .map_or_else(
            || "none".to_string(),
            |clustering| clustering.fields.join(", "),
        )
}

fn none_if_empty(value: &str) -> String {
    if value.is_empty() {
        "none".to_string()
    } else {
        value.to_string()
    }
}
