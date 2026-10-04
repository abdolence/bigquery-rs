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
use std::fmt::{self, Display, Formatter};

/// A table as the quoted path DDL names it, and the statements on it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DdlTable(String);

impl Display for DdlTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl BigQueryTableRef {
    /// The table as a quoted path, with `default_project` for an unset project.
    pub(crate) fn ddl(&self, default_project: &str) -> DdlTable {
        DdlTable(format!(
            "{}.{}.{}",
            quote_identifier(self.project_or(default_project)),
            quote_identifier(self.dataset().as_str()),
            quote_identifier(self.table().as_str())
        ))
    }
}

impl DdlTable {
    /// `ALTER TABLE .. RENAME COLUMN`.
    pub(crate) fn rename_column(&self, from: &str, to: &str) -> String {
        format!(
            "ALTER TABLE {self} RENAME COLUMN {} TO {}",
            quote_identifier(from),
            quote_identifier(to)
        )
    }

    /// `ALTER TABLE .. ALTER COLUMN .. SET DATA TYPE`.
    pub(crate) fn widen_column(&self, column: &str, to: &BigQueryFieldType) -> String {
        format!(
            "ALTER TABLE {self} ALTER COLUMN {} SET DATA TYPE {}",
            quote_identifier(column),
            to.ddl()
        )
    }

    /// `ALTER TABLE .. DROP COLUMN`.
    pub(crate) fn drop_column(&self, column: &str) -> String {
        format!(
            "ALTER TABLE {self} DROP COLUMN {}",
            quote_identifier(column)
        )
    }

    /// `CREATE SNAPSHOT TABLE` of `source` under this name.
    pub(crate) fn snapshot_of(&self, source: &DdlTable) -> String {
        format!("CREATE SNAPSHOT TABLE {self} CLONE {source}")
    }

    /// `DROP TABLE` and `CREATE TABLE` as one script, so the table is never missing between two
    /// jobs.
    pub(crate) fn drop_and_create(&self, target: &BigQueryTableTarget) -> BigQueryResult<String> {
        Ok(format!(
            "DROP TABLE {self};\n{};",
            self.create(target, false)?
        ))
    }

    /// `CREATE [OR REPLACE] TABLE` with everything `target` holds, since `CREATE OR REPLACE`
    /// must restate the partitioning and clustering the table keeps.
    ///
    /// # Errors
    /// [`BigQueryError::InvalidParametersError`] when the partitioning column is not a column
    /// of `target` of a type the partitioning can use, since the `PARTITION BY` expression
    /// depends on that type.
    pub(crate) fn create(
        &self,
        target: &BigQueryTableTarget,
        or_replace: bool,
    ) -> BigQueryResult<String> {
        let mut items: Vec<String> = target
            .columns
            .iter()
            .map(BigQueryFieldSchema::column_ddl)
            .collect();
        if let Some(key) = &target.primary_key {
            items.push(format!("PRIMARY KEY ({}) NOT ENFORCED", identifiers(key)));
        }
        let mut sql = format!(
            "CREATE {}TABLE {self} (\n  {}\n)",
            if or_replace { "OR REPLACE " } else { "" },
            items.join(",\n  ")
        );
        if let Some(partitioning) = &target.partitioning {
            sql.push_str("\nPARTITION BY ");
            sql.push_str(&partitioning.ddl(&target.columns)?);
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
        if let Some(at) = target.expiration {
            let mut text = String::new();
            fmt_timestamp(at.as_microsecond(), &mut text);
            options.push(format!(
                "expiration_timestamp={}",
                SqlLiteral::text(TextLiteralKind::Timestamp, &text)
            ));
        }
        if let Some(expiration) = target.partition_expiration {
            let days = expiration.as_millis() as f64 / 86_400_000.0;
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
}

fn identifiers(names: &[String]) -> String {
    names
        .iter()
        .map(|n| quote_identifier(n))
        .collect::<Vec<_>>()
        .join(", ")
}

impl BigQueryFieldType {
    /// The type with its parameters; a STRUCT with its fields quoted and their modes and
    /// descriptions.
    fn ddl(&self) -> String {
        match self {
            BigQueryFieldType::Struct(fields) => {
                let fields: Vec<String> =
                    fields.iter().map(BigQueryFieldSchema::field_ddl).collect();
                format!("STRUCT<{}>", fields.join(", "))
            }
            // Every other type's `Display` is its GoogleSQL syntax with integer parameters only.
            other => other.to_string(),
        }
    }
}

impl BigQueryFieldSchema {
    /// The field inside a STRUCT. BigQuery has no default value for a nested field, so none is
    /// written.
    fn field_ddl(&self) -> String {
        let mut sql = format!("{} {}", quote_identifier(&self.name), self.moded_type_ddl());
        self.push_not_null_and_options(&mut sql);
        sql
    }

    fn moded_type_ddl(&self) -> String {
        match self.mode {
            BigQueryFieldMode::Repeated => format!("ARRAY<{}>", self.field_type.ddl()),
            _ => self.field_type.ddl(),
        }
    }

    fn push_not_null_and_options(&self, sql: &mut String) {
        if self.mode == BigQueryFieldMode::Required {
            sql.push_str(" NOT NULL");
        }
        if let Some(description) = &self.description {
            sql.push_str(&format!(
                " OPTIONS(description={})",
                SqlLiteral::string(description)
            ));
        }
    }

    /// The field as a top-level column: type, `DEFAULT`, `NOT NULL`, `OPTIONS`, in the order
    /// GoogleSQL's `column_schema` takes them.
    fn column_ddl(&self) -> String {
        let mut sql = format!("{} {}", quote_identifier(&self.name), self.moded_type_ddl());
        if let Some(default) = &self.default_value_expression {
            // One operand whatever the text holds: the newline ends a `--` or `#` comment before
            // the closing parenthesis, and a `;`, an unbalanced `)` or an unclosed `/*` becomes a
            // syntax error in this statement rather than a second one. BigQuery stores the
            // expression without the parentheses and the comment, so the next plan compares
            // equal.
            sql.push_str(" DEFAULT (");
            sql.push_str(default);
            sql.push_str("\n)");
        }
        self.push_not_null_and_options(&mut sql);
        sql
    }
}

impl BigQueryPartitioning {
    /// The `PARTITION BY` expression, which for a column depends on its type among `columns`.
    fn ddl(&self, columns: &[BigQueryFieldSchema]) -> BigQueryResult<String> {
        let (unit, column) = match self {
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
            (Some(BigQueryFieldType::Timestamp), unit) => {
                format!("TIMESTAMP_TRUNC({name}, {unit})")
            }
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
}

#[cfg(test)]
mod tests;
