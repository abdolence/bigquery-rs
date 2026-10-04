//! The DDL `.sync()` runs, rendered only through `src/sql`: every name a quoted identifier and
//! every description, label and option value an escaped literal. The one exception is a
//! column's default value, which is a trusted SQL expression by contract, declared or copied
//! from the live table, and is written as one parenthesized operand.
//!
//! Numbers in the text (type parameters, range bounds) come from integer fields, never from
//! caller text.

use crate::errors::BigQueryError;
use crate::sql::{quote_identifier, SqlLiteral, TextLiteralKind};
use crate::types::civil::fmt_timestamp;
use crate::{
    BigQueryFieldMode, BigQueryFieldSchema, BigQueryFieldType, BigQueryPartitionUnit,
    BigQueryPartitioning, BigQueryResult, BigQueryTableRef, BigQueryTableTarget,
};

/// The table as a quoted path, with `default_project` for an unset project.
pub(crate) fn table_sql(table: &BigQueryTableRef, default_project: &str) -> String {
    format!(
        "{}.{}.{}",
        quote_identifier(table.project().unwrap_or(default_project)),
        quote_identifier(table.dataset().as_str()),
        quote_identifier(table.table().as_str())
    )
}

/// `ALTER TABLE .. RENAME COLUMN`.
pub(crate) fn rename_sql(table: &str, from: &str, to: &str) -> String {
    format!(
        "ALTER TABLE {table} RENAME COLUMN {} TO {}",
        quote_identifier(from),
        quote_identifier(to)
    )
}

/// `ALTER TABLE .. ALTER COLUMN .. SET DATA TYPE`.
pub(crate) fn widen_sql(table: &str, column: &str, to: &BigQueryFieldType) -> String {
    format!(
        "ALTER TABLE {table} ALTER COLUMN {} SET DATA TYPE {}",
        quote_identifier(column),
        type_sql(to)
    )
}

/// `ALTER TABLE .. DROP COLUMN`.
pub(crate) fn drop_sql(table: &str, column: &str) -> String {
    format!(
        "ALTER TABLE {table} DROP COLUMN {}",
        quote_identifier(column)
    )
}

/// `CREATE SNAPSHOT TABLE .. CLONE ..`.
pub(crate) fn snapshot_sql(snapshot: &str, source: &str) -> String {
    format!("CREATE SNAPSHOT TABLE {snapshot} CLONE {source}")
}

/// `DROP TABLE` and `CREATE TABLE` as one script, so the table is never missing between two
/// jobs.
pub(crate) fn drop_and_create_sql(
    table: &str,
    target: &BigQueryTableTarget,
) -> BigQueryResult<String> {
    Ok(format!(
        "DROP TABLE {table};\n{};",
        create_sql(table, target, false)?
    ))
}

/// `CREATE [OR REPLACE] TABLE` with everything `target` holds, so that `CREATE OR REPLACE`
/// restates the partitioning and clustering it must keep (recreate probe 1C2).
///
/// # Errors
/// [`BigQueryError::InvalidParametersError`] when the partitioning column is not a column of
/// `target` of a type the partitioning can use, since the `PARTITION BY` expression depends on
/// that type.
pub(crate) fn create_sql(
    table: &str,
    target: &BigQueryTableTarget,
    or_replace: bool,
) -> BigQueryResult<String> {
    let mut items: Vec<String> = target.columns.iter().map(column_sql).collect();
    if let Some(key) = &target.primary_key {
        items.push(format!("PRIMARY KEY ({}) NOT ENFORCED", identifiers(key)));
    }
    let mut sql = format!(
        "CREATE {}TABLE {table} (\n  {}\n)",
        if or_replace { "OR REPLACE " } else { "" },
        items.join(",\n  ")
    );
    if let Some(partitioning) = &target.partitioning {
        sql.push_str("\nPARTITION BY ");
        sql.push_str(&partition_sql(partitioning, &target.columns)?);
    }
    if !target.clustering.is_empty() {
        sql.push_str("\nCLUSTER BY ");
        sql.push_str(&identifiers(&target.clustering));
    }
    let mut options = Vec::new();
    if let Some(description) = &target.description {
        options.push(format!("description={}", SqlLiteral::string(description)));
    }
    if !target.labels.is_empty() {
        let labels: Vec<String> = target
            .labels
            .iter()
            .map(|(k, v)| format!("({}, {})", SqlLiteral::string(k), SqlLiteral::string(v)))
            .collect();
        options.push(format!("labels=[{}]", labels.join(", ")));
    }
    if let Some(ms) = target.expiration_ms {
        let mut text = String::new();
        fmt_timestamp(ms.saturating_mul(1000), &mut text);
        options.push(format!(
            "expiration_timestamp={}",
            SqlLiteral::text(TextLiteralKind::Timestamp, &text)
        ));
    }
    if let Some(ms) = target.partition_expiration_ms {
        let days = ms as f64 / 86_400_000.0;
        options.push(format!(
            "partition_expiration_days={}",
            SqlLiteral::float64(days)
        ));
    }
    if !options.is_empty() {
        sql.push_str(&format!("\nOPTIONS({})", options.join(", ")));
    }
    Ok(sql)
}

fn identifiers(names: &[String]) -> String {
    names
        .iter()
        .map(|n| quote_identifier(n))
        .collect::<Vec<_>>()
        .join(", ")
}

/// A type with its parameters; a STRUCT with its fields quoted and their modes and
/// descriptions.
fn type_sql(field_type: &BigQueryFieldType) -> String {
    match field_type {
        BigQueryFieldType::Struct(fields) => {
            let fields: Vec<String> = fields.iter().map(field_sql).collect();
            format!("STRUCT<{}>", fields.join(", "))
        }
        // Every other type's `Display` is its GoogleSQL syntax with integer parameters only.
        other => other.to_string(),
    }
}

/// A field of a STRUCT. BigQuery has no default value for a nested field, so none is written.
fn field_sql(field: &BigQueryFieldSchema) -> String {
    let mut sql = format!(
        "{} {}",
        quote_identifier(&field.name),
        moded_type_sql(field)
    );
    push_not_null_and_options(&mut sql, field);
    sql
}

fn moded_type_sql(field: &BigQueryFieldSchema) -> String {
    match field.mode {
        BigQueryFieldMode::Repeated => format!("ARRAY<{}>", type_sql(&field.field_type)),
        _ => type_sql(&field.field_type),
    }
}

fn push_not_null_and_options(sql: &mut String, field: &BigQueryFieldSchema) {
    if field.mode == BigQueryFieldMode::Required {
        sql.push_str(" NOT NULL");
    }
    if let Some(description) = &field.description {
        sql.push_str(&format!(
            " OPTIONS(description={})",
            SqlLiteral::string(description)
        ));
    }
}

/// A top-level column: type, `DEFAULT`, `NOT NULL`, `OPTIONS`, in the order GoogleSQL's
/// `column_schema` takes them.
fn column_sql(field: &BigQueryFieldSchema) -> String {
    let mut sql = format!(
        "{} {}",
        quote_identifier(&field.name),
        moded_type_sql(field)
    );
    if let Some(default) = &field.default_value_expression {
        // One operand whatever the text holds: the newline ends a `--` or `#` comment before
        // the closing parenthesis, and a `;`, an unbalanced `)` or an unclosed `/*` becomes a
        // syntax error in this statement rather than a second one. BigQuery stores the
        // expression without the parentheses and the comment, so the next plan compares equal.
        sql.push_str(" DEFAULT (");
        sql.push_str(default);
        sql.push_str("\n)");
    }
    push_not_null_and_options(&mut sql, field);
    sql
}

fn partition_sql(
    partitioning: &BigQueryPartitioning,
    columns: &[BigQueryFieldSchema],
) -> BigQueryResult<String> {
    let (unit, column) = match partitioning {
        BigQueryPartitioning::Range {
            column,
            start,
            end,
            interval,
        } => {
            return Ok(format!(
                "RANGE_BUCKET({}, GENERATE_ARRAY({start}, {end}, {interval}))",
                quote_identifier(column)
            ))
        }
        BigQueryPartitioning::Time { unit, column: None } => {
            return Ok(match unit {
                BigQueryPartitionUnit::Day => "_PARTITIONDATE".to_string(),
                unit => format!("TIMESTAMP_TRUNC(_PARTITIONTIME, {unit})"),
            })
        }
        BigQueryPartitioning::Time {
            unit,
            column: Some(column),
        } => (*unit, column),
    };
    let field_type = columns
        .iter()
        .find(|c| c.name.eq_ignore_ascii_case(column))
        .map(|c| &c.field_type);
    let name = quote_identifier(column);
    Ok(match (field_type, unit) {
        (Some(BigQueryFieldType::Date), BigQueryPartitionUnit::Day) => name,
        (Some(BigQueryFieldType::Date), unit) => format!("DATE_TRUNC({name}, {unit})"),
        (
            Some(BigQueryFieldType::Timestamp | BigQueryFieldType::DateTime),
            BigQueryPartitionUnit::Day,
        ) => {
            format!("DATE({name})")
        }
        (Some(BigQueryFieldType::Timestamp), unit) => format!("TIMESTAMP_TRUNC({name}, {unit})"),
        (Some(BigQueryFieldType::DateTime), unit) => format!("DATETIME_TRUNC({name}, {unit})"),
        (other, _) => {
            return Err(BigQueryError::invalid_parameters(
                "partition_by",
                format!(
                    "the partitioning column `{column}` is {}, not DATE, TIMESTAMP or DATETIME",
                    other.map_or_else(|| "not a column".to_string(), ToString::to_string)
                ),
            ))
        }
    })
}

#[cfg(test)]
mod tests;
