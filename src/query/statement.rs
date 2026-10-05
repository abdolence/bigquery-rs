use std::fmt::{Display, Formatter};

/// The kind of statement a query ran, as BigQuery names it in `statement_type`.
///
/// The names are the ones the v2 API documents for `JobStatistics2.statement_type`; a name it
/// adds later reads as [`Other`](Self::Other) with BigQuery's text, so
/// [`as_str`](Self::as_str) gives back what BigQuery sent for every value.
///
/// ```rust
/// use bigquery::BigQueryStatementType;
///
/// assert_eq!(BigQueryStatementType::from("CREATE_TABLE"), BigQueryStatementType::CreateTable);
/// let future = BigQueryStatementType::from("UNDROP_SCHEMA");
/// assert_eq!(future, BigQueryStatementType::Other("UNDROP_SCHEMA".into()));
/// assert_eq!(future.as_str(), "UNDROP_SCHEMA");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum BigQueryStatementType {
    /// A `SELECT` statement.
    Select,
    /// An `ASSERT` statement.
    Assert,
    /// An `INSERT` statement.
    Insert,
    /// An `UPDATE` statement.
    Update,
    /// A `DELETE` statement.
    Delete,
    /// A `MERGE` statement.
    Merge,
    /// A `CREATE TABLE` statement.
    CreateTable,
    /// A `CREATE TABLE ... AS SELECT` statement.
    CreateTableAsSelect,
    /// A `CREATE VIEW` statement.
    CreateView,
    /// A `CREATE MODEL` statement.
    CreateModel,
    /// A `CREATE MATERIALIZED VIEW` statement.
    CreateMaterializedView,
    /// A `CREATE FUNCTION` statement.
    CreateFunction,
    /// A `CREATE TABLE FUNCTION` statement.
    CreateTableFunction,
    /// A `CREATE PROCEDURE` statement.
    CreateProcedure,
    /// A `CREATE ROW ACCESS POLICY` statement.
    CreateRowAccessPolicy,
    /// A `CREATE SCHEMA` statement.
    CreateSchema,
    /// A `CREATE SNAPSHOT TABLE` statement.
    CreateSnapshotTable,
    /// A `CREATE SEARCH INDEX` statement.
    CreateSearchIndex,
    /// A `CREATE EXTERNAL TABLE` statement.
    CreateExternalTable,
    /// A `DROP TABLE` statement.
    DropTable,
    /// A `DROP EXTERNAL TABLE` statement.
    DropExternalTable,
    /// A `DROP VIEW` statement.
    DropView,
    /// A `DROP MODEL` statement.
    DropModel,
    /// A `DROP MATERIALIZED VIEW` statement.
    DropMaterializedView,
    /// A `DROP FUNCTION` statement.
    DropFunction,
    /// A `DROP TABLE FUNCTION` statement.
    DropTableFunction,
    /// A `DROP PROCEDURE` statement.
    DropProcedure,
    /// A `DROP SEARCH INDEX` statement.
    DropSearchIndex,
    /// A `DROP SCHEMA` statement.
    DropSchema,
    /// A `DROP SNAPSHOT TABLE` statement.
    DropSnapshotTable,
    /// A `DROP [ALL] ROW ACCESS POLICY` statement.
    DropRowAccessPolicy,
    /// An `ALTER TABLE` statement.
    AlterTable,
    /// An `ALTER VIEW` statement.
    AlterView,
    /// An `ALTER MATERIALIZED VIEW` statement.
    AlterMaterializedView,
    /// An `ALTER SCHEMA` statement.
    AlterSchema,
    /// A `TRUNCATE TABLE` statement.
    TruncateTable,
    /// A multi-statement script.
    Script,
    /// An `EXPORT DATA` statement.
    ExportData,
    /// An `EXPORT MODEL` statement.
    ExportModel,
    /// A `LOAD DATA` statement.
    LoadData,
    /// A `CALL` statement.
    Call,
    /// A statement type this crate does not know, as BigQuery named it.
    Other(String),
}

impl BigQueryStatementType {
    /// The name as BigQuery writes it, such as `CREATE_TABLE`.
    pub fn as_str(&self) -> &str {
        match self {
            Self::Select => "SELECT",
            Self::Assert => "ASSERT",
            Self::Insert => "INSERT",
            Self::Update => "UPDATE",
            Self::Delete => "DELETE",
            Self::Merge => "MERGE",
            Self::CreateTable => "CREATE_TABLE",
            Self::CreateTableAsSelect => "CREATE_TABLE_AS_SELECT",
            Self::CreateView => "CREATE_VIEW",
            Self::CreateModel => "CREATE_MODEL",
            Self::CreateMaterializedView => "CREATE_MATERIALIZED_VIEW",
            Self::CreateFunction => "CREATE_FUNCTION",
            Self::CreateTableFunction => "CREATE_TABLE_FUNCTION",
            Self::CreateProcedure => "CREATE_PROCEDURE",
            Self::CreateRowAccessPolicy => "CREATE_ROW_ACCESS_POLICY",
            Self::CreateSchema => "CREATE_SCHEMA",
            Self::CreateSnapshotTable => "CREATE_SNAPSHOT_TABLE",
            Self::CreateSearchIndex => "CREATE_SEARCH_INDEX",
            Self::CreateExternalTable => "CREATE_EXTERNAL_TABLE",
            Self::DropTable => "DROP_TABLE",
            Self::DropExternalTable => "DROP_EXTERNAL_TABLE",
            Self::DropView => "DROP_VIEW",
            Self::DropModel => "DROP_MODEL",
            Self::DropMaterializedView => "DROP_MATERIALIZED_VIEW",
            Self::DropFunction => "DROP_FUNCTION",
            Self::DropTableFunction => "DROP_TABLE_FUNCTION",
            Self::DropProcedure => "DROP_PROCEDURE",
            Self::DropSearchIndex => "DROP_SEARCH_INDEX",
            Self::DropSchema => "DROP_SCHEMA",
            Self::DropSnapshotTable => "DROP_SNAPSHOT_TABLE",
            Self::DropRowAccessPolicy => "DROP_ROW_ACCESS_POLICY",
            Self::AlterTable => "ALTER_TABLE",
            Self::AlterView => "ALTER_VIEW",
            Self::AlterMaterializedView => "ALTER_MATERIALIZED_VIEW",
            Self::AlterSchema => "ALTER_SCHEMA",
            Self::TruncateTable => "TRUNCATE_TABLE",
            Self::Script => "SCRIPT",
            Self::ExportData => "EXPORT_DATA",
            Self::ExportModel => "EXPORT_MODEL",
            Self::LoadData => "LOAD_DATA",
            Self::Call => "CALL",
            Self::Other(name) => name,
        }
    }

    fn known(name: &str) -> Option<Self> {
        Some(match name {
            "SELECT" => Self::Select,
            "ASSERT" => Self::Assert,
            "INSERT" => Self::Insert,
            "UPDATE" => Self::Update,
            "DELETE" => Self::Delete,
            "MERGE" => Self::Merge,
            "CREATE_TABLE" => Self::CreateTable,
            "CREATE_TABLE_AS_SELECT" => Self::CreateTableAsSelect,
            "CREATE_VIEW" => Self::CreateView,
            "CREATE_MODEL" => Self::CreateModel,
            "CREATE_MATERIALIZED_VIEW" => Self::CreateMaterializedView,
            "CREATE_FUNCTION" => Self::CreateFunction,
            "CREATE_TABLE_FUNCTION" => Self::CreateTableFunction,
            "CREATE_PROCEDURE" => Self::CreateProcedure,
            "CREATE_ROW_ACCESS_POLICY" => Self::CreateRowAccessPolicy,
            "CREATE_SCHEMA" => Self::CreateSchema,
            "CREATE_SNAPSHOT_TABLE" => Self::CreateSnapshotTable,
            "CREATE_SEARCH_INDEX" => Self::CreateSearchIndex,
            "CREATE_EXTERNAL_TABLE" => Self::CreateExternalTable,
            "DROP_TABLE" => Self::DropTable,
            "DROP_EXTERNAL_TABLE" => Self::DropExternalTable,
            "DROP_VIEW" => Self::DropView,
            "DROP_MODEL" => Self::DropModel,
            "DROP_MATERIALIZED_VIEW" => Self::DropMaterializedView,
            "DROP_FUNCTION" => Self::DropFunction,
            "DROP_TABLE_FUNCTION" => Self::DropTableFunction,
            "DROP_PROCEDURE" => Self::DropProcedure,
            "DROP_SEARCH_INDEX" => Self::DropSearchIndex,
            "DROP_SCHEMA" => Self::DropSchema,
            "DROP_SNAPSHOT_TABLE" => Self::DropSnapshotTable,
            "DROP_ROW_ACCESS_POLICY" => Self::DropRowAccessPolicy,
            "ALTER_TABLE" => Self::AlterTable,
            "ALTER_VIEW" => Self::AlterView,
            "ALTER_MATERIALIZED_VIEW" => Self::AlterMaterializedView,
            "ALTER_SCHEMA" => Self::AlterSchema,
            "TRUNCATE_TABLE" => Self::TruncateTable,
            "SCRIPT" => Self::Script,
            "EXPORT_DATA" => Self::ExportData,
            "EXPORT_MODEL" => Self::ExportModel,
            "LOAD_DATA" => Self::LoadData,
            "CALL" => Self::Call,
            _ => return None,
        })
    }
}

impl From<&str> for BigQueryStatementType {
    fn from(name: &str) -> Self {
        Self::known(name).unwrap_or_else(|| Self::Other(name.to_string()))
    }
}

impl From<String> for BigQueryStatementType {
    fn from(name: String) -> Self {
        Self::known(&name).unwrap_or(Self::Other(name))
    }
}

impl Display for BigQueryStatementType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statement_types_map_known_names_and_keep_unknown_ones() {
        let known = [
            "SELECT",
            "ASSERT",
            "INSERT",
            "UPDATE",
            "DELETE",
            "MERGE",
            "CREATE_TABLE",
            "CREATE_TABLE_AS_SELECT",
            "CREATE_VIEW",
            "CREATE_MODEL",
            "CREATE_MATERIALIZED_VIEW",
            "CREATE_FUNCTION",
            "CREATE_TABLE_FUNCTION",
            "CREATE_PROCEDURE",
            "CREATE_ROW_ACCESS_POLICY",
            "CREATE_SCHEMA",
            "CREATE_SNAPSHOT_TABLE",
            "CREATE_SEARCH_INDEX",
            "CREATE_EXTERNAL_TABLE",
            "DROP_TABLE",
            "DROP_EXTERNAL_TABLE",
            "DROP_VIEW",
            "DROP_MODEL",
            "DROP_MATERIALIZED_VIEW",
            "DROP_FUNCTION",
            "DROP_TABLE_FUNCTION",
            "DROP_PROCEDURE",
            "DROP_SEARCH_INDEX",
            "DROP_SCHEMA",
            "DROP_SNAPSHOT_TABLE",
            "DROP_ROW_ACCESS_POLICY",
            "ALTER_TABLE",
            "ALTER_VIEW",
            "ALTER_MATERIALIZED_VIEW",
            "ALTER_SCHEMA",
            "TRUNCATE_TABLE",
            "SCRIPT",
            "EXPORT_DATA",
            "EXPORT_MODEL",
            "LOAD_DATA",
            "CALL",
        ];
        for name in known {
            let parsed = BigQueryStatementType::from(name);
            assert!(
                !matches!(parsed, BigQueryStatementType::Other(_)),
                "{name} reads as {parsed:?}"
            );
            assert_eq!(parsed.as_str(), name);
            assert_eq!(BigQueryStatementType::from(name.to_string()), parsed);
        }
        let unknown = BigQueryStatementType::from("UNDROP_SCHEMA".to_string());
        assert_eq!(
            unknown,
            BigQueryStatementType::Other("UNDROP_SCHEMA".into())
        );
        assert_eq!(unknown.to_string(), "UNDROP_SCHEMA");
    }
}
