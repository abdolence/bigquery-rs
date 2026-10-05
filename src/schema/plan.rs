//! The plan `.plan()` returns and `.sync()` follows, and the report of what `.sync()` did.

use crate::BigQueryLabels;
use crate::{
    BigQueryFieldMode, BigQueryFieldSchema, BigQueryFieldType, BigQueryInstant,
    BigQueryPartitioning, BigQueryTableRef,
};
use std::fmt::{self, Display, Formatter};
use std::time::Duration;

/// One difference between a declaration and the table, and how it is applied.
///
/// Column paths are dotted for nested fields, as in `shipping.city`, and name the column as the
/// table has it when the change is made: a change sent before a rename names the old column.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BigQuerySchemaChange {
    /// `PatchTable`: a new NULLABLE or REPEATED column or nested field. With a default value,
    /// the column is added first and the default set by a second patch; existing rows stay
    /// NULL.
    AddColumn {
        /// Where the column goes.
        path: String,
        /// The column as declared.
        field: BigQueryFieldSchema,
    },
    /// `PatchTable`: REQUIRED to NULLABLE.
    RelaxColumn {
        /// The column.
        path: String,
    },
    /// `PatchTable`: a column description.
    SetColumnDescription {
        /// The column.
        path: String,
        /// The description the table has.
        from: Option<String>,
        /// The declared description.
        to: String,
    },
    /// `PatchTable`: a default value on an existing column. Existing rows keep their values.
    SetColumnDefault {
        /// The column.
        path: String,
        /// The default the table has.
        from: Option<String>,
        /// The declared default expression.
        to: String,
    },
    /// `PatchTable`: the table description.
    SetDescription {
        /// The description the table has.
        from: Option<String>,
        /// The declared description.
        to: String,
    },
    /// `PatchTable`: a label added or changed.
    SetLabel {
        /// The label key.
        key: String,
        /// The value the table has.
        from: Option<String>,
        /// The declared value.
        to: String,
    },
    /// `PatchTable`: the table expiration.
    SetExpiration {
        /// The expiration the table has.
        from: Option<BigQueryInstant>,
        /// The declared expiration.
        to: BigQueryInstant,
    },
    /// `PatchTable`: the partition expiration.
    SetPartitionExpiration {
        /// The expiration the table has.
        from: Option<Duration>,
        /// The declared expiration.
        to: Duration,
    },
    /// `PatchTable`: clustering added or changed. New data is clustered the new way; existing
    /// data is reclustered by BigQuery in the background.
    SetClustering {
        /// The clustering the table has, empty for none.
        from: Vec<String>,
        /// The declared clustering.
        to: Vec<String>,
    },
    /// `PatchTable`: the primary key added or changed (`NOT ENFORCED`).
    SetPrimaryKey {
        /// The key the table has.
        from: Option<Vec<String>>,
        /// The declared key.
        to: Vec<String>,
    },
    /// `PatchTable`: an undeclared primary key removed (`prune_undeclared()`).
    RemovePrimaryKey {
        /// The key the table has.
        from: Vec<String>,
    },
    /// `UpdateTable`: an undeclared label removed (`prune_undeclared()`).
    RemoveLabel {
        /// The label key.
        key: String,
        /// The value it had.
        value: String,
    },
    /// `UpdateTable`: undeclared clustering removed (`prune_undeclared()`).
    RemoveClustering {
        /// The clustering the table has.
        from: Vec<String>,
    },
    /// DDL `RENAME COLUMN`. The values are kept, but for writers it is a drop of the old name.
    RenameColumn {
        /// The name the table has.
        from: String,
        /// The declared name.
        to: String,
    },
    /// DDL `ALTER COLUMN SET DATA TYPE` to a wider type (`allow_widening()`).
    WidenColumn {
        /// The column, by its name after any rename.
        column: String,
        /// The type the table has.
        from: BigQueryFieldType,
        /// The declared type.
        to: BigQueryFieldType,
    },
    /// DDL `DROP COLUMN` of an undeclared column (`prune_undeclared()`). **Data loss**: every
    /// value of the column is deleted.
    DropColumn {
        /// The column.
        column: String,
        /// Its type.
        field_type: BigQueryFieldType,
    },
    /// Impossible in place: a type change that is not a widening, a widening of a nested
    /// field, or a change between a RECORD and another type.
    ChangeColumnType {
        /// The column.
        path: String,
        /// The type the table has.
        from: BigQueryFieldType,
        /// The declared type.
        to: BigQueryFieldType,
    },
    /// Impossible in place: NULLABLE to REQUIRED, or any change to or from REPEATED.
    ChangeColumnMode {
        /// The column.
        path: String,
        /// The mode the table has.
        from: BigQueryFieldMode,
        /// The declared mode.
        to: BigQueryFieldMode,
    },
    /// Impossible in place: a new REQUIRED column or nested field.
    AddRequiredColumn {
        /// Where the column goes.
        path: String,
        /// The column as declared.
        field: BigQueryFieldSchema,
    },
    /// Impossible in place: dropping an undeclared nested field (`prune_undeclared()`).
    DropNestedField {
        /// The field.
        path: String,
        /// Its type.
        field_type: BigQueryFieldType,
    },
    /// Impossible in place: adding, changing or (with `prune_undeclared()`) removing the
    /// partitioning.
    ChangePartitioning {
        /// The partitioning the table has.
        from: Option<BigQueryPartitioning>,
        /// The declared partitioning.
        to: Option<BigQueryPartitioning>,
    },
}

/// How `.sync()` applies a change, which fixes the order it is applied in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum ChangeStep {
    Patch,
    Update,
    Rename,
    Widen,
    Drop,
    Impossible,
}

impl BigQuerySchemaChange {
    pub(crate) fn step(&self) -> ChangeStep {
        use BigQuerySchemaChange::*;
        match self {
            AddColumn { .. }
            | RelaxColumn { .. }
            | SetColumnDescription { .. }
            | SetColumnDefault { .. }
            | SetDescription { .. }
            | SetLabel { .. }
            | SetExpiration { .. }
            | SetPartitionExpiration { .. }
            | SetClustering { .. }
            | SetPrimaryKey { .. }
            | RemovePrimaryKey { .. } => ChangeStep::Patch,
            RemoveLabel { .. } | RemoveClustering { .. } => ChangeStep::Update,
            RenameColumn { .. } => ChangeStep::Rename,
            WidenColumn { .. } => ChangeStep::Widen,
            DropColumn { .. } => ChangeStep::Drop,
            ChangeColumnType { .. }
            | ChangeColumnMode { .. }
            | AddRequiredColumn { .. }
            | DropNestedField { .. }
            | ChangePartitioning { .. } => ChangeStep::Impossible,
        }
    }
}

fn debug_or_none<T: fmt::Debug>(value: &Option<T>) -> String {
    value
        .as_ref()
        .map_or_else(|| "none".to_string(), |v| format!("{v:?}"))
}

fn list(columns: &[String]) -> String {
    if columns.is_empty() {
        return "none".into();
    }
    let quoted: Vec<String> = columns.iter().map(|c| format!("`{c}`")).collect();
    quoted.join(", ")
}

impl Display for BigQuerySchemaChange {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        use BigQuerySchemaChange::*;
        match self {
            AddColumn { path, field } => {
                write!(
                    f,
                    "[patch] add column `{path}` {} {}",
                    field.field_type, field.mode
                )?;
                if let Some(default) = &field.default_value_expression {
                    write!(
                        f,
                        ", then DEFAULT {default} in a second patch (existing rows stay NULL)"
                    )?;
                }
                Ok(())
            }
            RelaxColumn { path } => write!(f, "[patch] relax `{path}` REQUIRED to NULLABLE"),
            SetColumnDescription { path, from, to } => write!(
                f,
                "[patch] description of `{path}`: {} to {to:?}",
                debug_or_none(from)
            ),
            SetColumnDefault { path, from, to } => {
                write!(
                    f,
                    "[patch] default of `{path}`: {} to {to}",
                    debug_or_none(from)
                )
            }
            SetDescription { from, to } => {
                write!(
                    f,
                    "[patch] table description: {} to {to:?}",
                    debug_or_none(from)
                )
            }
            SetLabel { key, from, to } => {
                write!(
                    f,
                    "[patch] label {key:?}: {} to {to:?}",
                    debug_or_none(from)
                )
            }
            SetExpiration { from, to } => write!(
                f,
                "[patch] table expiration: {} to {to}",
                from.map_or_else(|| "none".to_string(), |t| t.to_string()),
            ),
            SetPartitionExpiration { from, to } => write!(
                f,
                "[patch] partition expiration: {} to {} ms",
                from.map_or_else(|| "none".to_string(), |d| format!("{} ms", d.as_millis())),
                to.as_millis()
            ),
            SetClustering { from, to } => {
                write!(f, "[patch] clustering: {} to {}", list(from), list(to))
            }
            SetPrimaryKey { from, to } => write!(
                f,
                "[patch] primary key: {} to {}",
                from.as_deref().map_or_else(|| "none".to_string(), list),
                list(to)
            ),
            RemovePrimaryKey { from } => {
                write!(f, "[patch] remove undeclared primary key {}", list(from))
            }
            RemoveLabel { key, value } => {
                write!(f, "[update] remove undeclared label {key:?} = {value:?}")
            }
            RemoveClustering { from } => {
                write!(f, "[update] remove undeclared clustering {}", list(from))
            }
            RenameColumn { from, to } => write!(
                f,
                "[ddl] RENAME COLUMN `{from}` TO `{to}` (writers still sending `{from}` lose it)"
            ),
            WidenColumn { column, from, to } => {
                write!(f, "[ddl] widen `{column}` from {from} to {to}")
            }
            DropColumn { column, field_type } => write!(
                f,
                "[ddl] DROP COLUMN `{column}` {field_type}: DATA LOSS, every value in it is \
                 deleted"
            ),
            ChangeColumnType { path, from, to } => {
                write!(f, "change the type of `{path}` from {from} to {to}")
            }
            ChangeColumnMode { path, from, to } => {
                write!(f, "change the mode of `{path}` from {} to {}", from, to)
            }
            AddRequiredColumn { path, field } => write!(
                f,
                "add REQUIRED column `{path}` {} to an existing table",
                field.field_type
            ),
            DropNestedField { path, field_type } => {
                write!(f, "drop the nested field `{path}` {field_type}: DATA LOSS")
            }
            ChangePartitioning { from, to } => write!(
                f,
                "change the partitioning from {} to {}",
                from.as_ref()
                    .map_or_else(|| "none".to_string(), ToString::to_string),
                to.as_ref()
                    .map_or_else(|| "none".to_string(), ToString::to_string)
            ),
        }
    }
}

/// What a withheld change waits for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum BigQueryWithheldReason {
    /// The table has something the declaration does not; `prune_undeclared()` would remove it.
    PruneUndeclared,
    /// The declared type is wider than the column's; `allow_widening()` would widen it.
    AllowWidening,
}

impl Display for BigQueryWithheldReason {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            BigQueryWithheldReason::PruneUndeclared => "needs prune_undeclared()",
            BigQueryWithheldReason::AllowWidening => "needs allow_widening()",
        })
    }
}

/// A change `.sync()` leaves out until the declaration opts in to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BigQueryWithheldChange {
    /// The change.
    pub change: BigQuerySchemaChange,
    /// The opt-in it needs.
    pub reason: BigQueryWithheldReason,
}

impl Display for BigQueryWithheldChange {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.change, self.reason)
    }
}

/// A whole table as `.sync()` creates or recreates it: what is declared, plus whatever the
/// table already has that the declaration leaves undeclared and is not pruned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BigQueryTableTarget {
    /// The columns, in table order.
    pub columns: Vec<BigQueryFieldSchema>,
    /// The primary key (`NOT ENFORCED`).
    pub primary_key: Option<Vec<String>>,
    /// The partitioning.
    pub partitioning: Option<BigQueryPartitioning>,
    /// The partition expiration.
    pub partition_expiration: Option<Duration>,
    /// The clustering columns, empty for none.
    pub clustering: Vec<String>,
    /// The table description.
    pub description: Option<String>,
    /// The labels.
    pub labels: BigQueryLabels,
    /// The table expiration.
    pub expiration: Option<BigQueryInstant>,
}

impl Display for BigQueryTableTarget {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let columns: Vec<String> = self
            .columns
            .iter()
            .map(|c| format!("`{}` {} {}", c.name, c.field_type, c.mode))
            .collect();
        write!(f, "columns {}", columns.join(", "))?;
        if let Some(key) = &self.primary_key {
            write!(f, "; primary key {}", list(key))?;
        }
        if let Some(partitioning) = &self.partitioning {
            write!(f, "; partitioned {partitioning}")?;
        }
        if !self.clustering.is_empty() {
            write!(f, "; clustered by {}", list(&self.clustering))?;
        }
        if !self.labels.is_empty() {
            write!(f, "; labels {:?}", self.labels)?;
        }
        Ok(())
    }
}

/// How a table is recreated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum BigQueryRecreateMethod {
    /// `CREATE OR REPLACE TABLE`, when the partitioning and clustering stay as they are. It
    /// keeps the table's IAM bindings.
    CreateOrReplace,
    /// `DROP TABLE` and `CREATE TABLE` in one script, when the partitioning or clustering
    /// changes, which `CREATE OR REPLACE` cannot do. It loses the table's IAM bindings.
    DropAndCreate,
}

/// A recreate `.sync()` runs because a change is impossible in place and the declaration opts
/// in to recreating.
///
/// **Every row is lost.** `num_rows` and `num_bytes` come from `GetTable`, which lags Storage
/// Write: rows written through COMMITTED or PENDING streams show after 60 to 70 s, and rows on
/// the default stream can take more than 5 minutes. So a table written seconds ago can read as
/// empty, and the numbers here can be lower than what is lost.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BigQueryRecreate {
    /// How the table is recreated.
    pub method: BigQueryRecreateMethod,
    /// The changes that are impossible in place.
    pub reasons: Vec<BigQuerySchemaChange>,
    /// The table as it is recreated.
    pub target: BigQueryTableTarget,
    /// The table's `num_rows` from `GetTable`.
    pub num_rows: Option<u64>,
    /// The table's `num_bytes` from `GetTable`.
    pub num_bytes: Option<i64>,
    /// The row access policies on the table, all of which every recreate drops.
    pub row_access_policies: Vec<String>,
    /// Whether this is `dangerously_recreate_with_data_loss()` rather than
    /// `recreate_if_empty()`.
    pub dangerous: bool,
    /// Whether a `CREATE SNAPSHOT TABLE` runs first (`snapshot_first()`).
    pub snapshot_first: bool,
}

impl Display for BigQueryRecreate {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let method = match self.method {
            BigQueryRecreateMethod::CreateOrReplace => "CREATE OR REPLACE TABLE",
            BigQueryRecreateMethod::DropAndCreate => "DROP TABLE + CREATE TABLE",
        };
        writeln!(
            f,
            "  recreate with {method}{}, because:",
            if self.dangerous {
                " (dangerously_recreate_with_data_loss)"
            } else {
                " (recreate_if_empty)"
            }
        )?;
        for reason in &self.reasons {
            writeln!(f, "    {reason}")?;
        }
        writeln!(
            f,
            "    DATA LOSS: every row, num_rows {} and num_bytes {} per GetTable, which lags \
             Storage Write by a minute or more",
            self.num_rows
                .map_or_else(|| "not reported".to_string(), |n| n.to_string()),
            self.num_bytes
                .map_or_else(|| "not reported".to_string(), |n| n.to_string()),
        )?;
        if self.row_access_policies.is_empty() {
            writeln!(f, "    loses: row access policies (none on the table)")?;
        } else {
            writeln!(
                f,
                "    loses: row access policies {}",
                list(&self.row_access_policies)
            )?;
        }
        match self.method {
            BigQueryRecreateMethod::CreateOrReplace => writeln!(f, "    keeps: table IAM")?,
            BigQueryRecreateMethod::DropAndCreate => writeln!(f, "    loses: table IAM")?,
        }
        if self.snapshot_first {
            writeln!(
                f,
                "    snapshot first: CREATE SNAPSHOT TABLE, which misses rows still in the \
                 streaming buffer"
            )?;
        }
        writeln!(
            f,
            "    stop writers and readers first: open writers keep getting acks for lost rows"
        )?;
        writeln!(f, "    table as recreated: {}", self.target)
    }
}

/// Why `.sync()` refuses a plan, writing nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum BigQueryRefusal {
    /// A change is impossible in place and the declaration has no recreate opt-in.
    NoRecreateOptIn,
    /// `recreate_if_empty()` is set, but `GetTable` does not report `num_rows == 0`.
    NotEmpty {
        /// The table's `num_rows`.
        num_rows: Option<u64>,
    },
}

impl Display for BigQueryRefusal {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            BigQueryRefusal::NoRecreateOptIn => f.write_str(
                "no recreate opt-in: recreate_if_empty() or dangerously_recreate_with_data_loss()",
            ),
            BigQueryRefusal::NotEmpty { num_rows } => write!(
                f,
                "recreate_if_empty() needs num_rows == 0, GetTable reports {}",
                num_rows.map_or_else(|| "nothing".to_string(), |n| n.to_string())
            ),
        }
    }
}

/// What `.sync()` would do to one table, as `.plan()` returns it.
///
/// Exactly one of these holds: the table is missing and `create` is set; or a change is
/// impossible in place and either `recreate` (an opt-in applies) or `refusal` (`.sync()` writes
/// nothing) is set; or `changes` lists the in-place changes in the order `.sync()` sends them.
/// Every undeclared item and every widening the declaration does not opt in to is in
/// `withheld`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BigQueryTablePlan {
    /// The table, with its project resolved.
    pub table: BigQueryTableRef,
    /// The table to create, when it does not exist.
    pub create: Option<BigQueryTableTarget>,
    /// The in-place changes, in write order: `PatchTable`, the default-value `PatchTable`,
    /// `UpdateTable`, then DDL renames, widenings and drops.
    pub changes: Vec<BigQuerySchemaChange>,
    /// Changes left out until the declaration opts in.
    pub withheld: Vec<BigQueryWithheldChange>,
    /// The recreate `.sync()` runs instead of `changes`.
    pub recreate: Option<BigQueryRecreate>,
    /// Changes that are impossible in place, when `.sync()` refuses them.
    pub impossible: Vec<BigQuerySchemaChange>,
    /// Why `.sync()` refuses this plan.
    pub refusal: Option<BigQueryRefusal>,
}

impl BigQueryTablePlan {
    /// Whether `.sync()` would write nothing and nothing is withheld.
    pub fn is_empty(&self) -> bool {
        self.create.is_none()
            && self.changes.is_empty()
            && self.withheld.is_empty()
            && self.recreate.is_none()
            && self.impossible.is_empty()
    }
}

fn write_section<T: Display>(f: &mut Formatter<'_>, label: &str, items: &[T]) -> fmt::Result {
    if items.is_empty() {
        return Ok(());
    }
    writeln!(f, "  {label}: {}", items.len())?;
    for item in items {
        writeln!(f, "    {item}")?;
    }
    Ok(())
}

impl Display for BigQueryTablePlan {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if self.is_empty() {
            return writeln!(f, "BigQuery table plan for {}: no changes", self.table);
        }
        writeln!(f, "BigQuery table plan for {}:", self.table)?;
        if let Some(target) = &self.create {
            writeln!(f, "  create: {target}")?;
        }
        write_section(f, "changes, in write order", &self.changes)?;
        write_section(f, "withheld", &self.withheld)?;
        if let Some(recreate) = &self.recreate {
            write!(f, "{recreate}")?;
        }
        write_section(f, "impossible in place", &self.impossible)?;
        if let Some(refusal) = &self.refusal {
            writeln!(f, "  refused, sync writes nothing: {refusal}")?;
        }
        Ok(())
    }
}

/// Data a `.sync()` deleted.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum BigQueryDroppedData {
    /// A column dropped by `prune_undeclared()`, with every value in it.
    Column {
        /// The column.
        column: String,
    },
    /// Every row of a recreated table. The counts are `GetTable`'s and lag Storage Write.
    Rows {
        /// `num_rows` before the recreate.
        num_rows: Option<u64>,
        /// `num_bytes` before the recreate.
        num_bytes: Option<i64>,
    },
}

impl Display for BigQueryDroppedData {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            BigQueryDroppedData::Column { column } => write!(f, "column `{column}`"),
            BigQueryDroppedData::Rows {
                num_rows,
                num_bytes,
            } => write!(
                f,
                "all rows: num_rows {}, num_bytes {} (GetTable lags Storage Write)",
                num_rows.map_or_else(|| "not reported".to_string(), |n| n.to_string()),
                num_bytes.map_or_else(|| "not reported".to_string(), |n| n.to_string()),
            ),
        }
    }
}

/// What `.sync()` did to one table.
///
/// After a recreate, writers opened before it lose rows silently and new writers can get
/// `NotFound` for minutes; the report does not promise that the table is writable yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BigQueryTableSyncReport {
    /// The table, with its project resolved.
    pub table: BigQueryTableRef,
    /// The table created, when it did not exist.
    pub created: Option<BigQueryTableTarget>,
    /// The in-place changes applied, in the order they were sent.
    pub applied: Vec<BigQuerySchemaChange>,
    /// Changes left out until the declaration opts in.
    pub withheld: Vec<BigQueryWithheldChange>,
    /// The recreate that ran.
    pub recreated: Option<BigQueryRecreate>,
    /// The snapshot `snapshot_first()` took before the recreate.
    pub snapshot: Option<BigQueryTableRef>,
    /// Data this sync deleted.
    pub dropped_data: Vec<BigQueryDroppedData>,
}

impl BigQueryTableSyncReport {
    pub(crate) fn new(table: BigQueryTableRef) -> Self {
        Self {
            table,
            created: None,
            applied: Vec::new(),
            withheld: Vec::new(),
            recreated: None,
            snapshot: None,
            dropped_data: Vec::new(),
        }
    }
}

impl Display for BigQueryTableSyncReport {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        writeln!(f, "BigQuery table sync report for {}:", self.table)?;
        if let Some(target) = &self.created {
            writeln!(f, "  created: {target}")?;
        }
        write_section(f, "applied", &self.applied)?;
        write_section(f, "withheld", &self.withheld)?;
        if let Some(snapshot) = &self.snapshot {
            writeln!(f, "  snapshot: {snapshot}")?;
        }
        if let Some(recreate) = &self.recreated {
            write!(f, "{recreate}")?;
        }
        write_section(f, "dropped_data (DATA LOSS)", &self.dropped_data)?;
        if self.created.is_none()
            && self.applied.is_empty()
            && self.withheld.is_empty()
            && self.recreated.is_none()
        {
            writeln!(f, "  no changes")?;
        }
        Ok(())
    }
}
