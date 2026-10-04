//! The plan as a pure function of a declaration and the table `GetTable` returned, and the
//! request bodies that carry it out.
//!
//! Nothing here performs I/O, so every class of change BigQuery was measured on is a unit
//! test.

use crate::db::proto::millis;
use crate::schema::declaration::DeclaredColumn;
use crate::schema::live::LiveTable;
use crate::schema::plan::ChangeStep;
use crate::BigQueryLabels;
use crate::{
    BigQueryDecimalParams, BigQueryFieldMode, BigQueryFieldSchema, BigQueryFieldType,
    BigQueryRecreate, BigQueryRecreateMethod, BigQueryRecreatePolicy, BigQueryRefusal,
    BigQueryResult, BigQuerySchemaChange, BigQueryTableDeclaration, BigQueryTablePlan,
    BigQueryTableRef, BigQueryTableTarget, BigQueryWithheldChange, BigQueryWithheldReason,
};
use gcloud_sdk::google::cloud::bigquery::v2;
use std::collections::HashSet;

impl BigQueryTableDeclaration {
    /// What `.sync()` would do to `table`, declared as `declaration`, given its current state;
    /// `None` when it does not exist. `row_access_policies` of a recreate are left empty for the
    /// caller to list.
    pub(crate) fn plan(
        &self,
        table: BigQueryTableRef,
        live: Option<&LiveTable>,
    ) -> BigQueryTablePlan {
        let mut plan = BigQueryTablePlan {
            table,
            create: None,
            changes: Vec::new(),
            withheld: Vec::new(),
            recreate: None,
            impossible: Vec::new(),
            refusal: None,
        };
        let Some(live) = live else {
            plan.create = Some(self.target(None));
            return plan;
        };

        let mut diff = Diff {
            declaration: self,
            changes: Vec::new(),
            withheld: Vec::new(),
            impossible: Vec::new(),
        };
        diff.columns(&live.schema.fields);
        diff.settings(live);
        // A stable sort keeps the column changes ahead of the table settings within a step.
        diff.changes.sort_by_key(BigQuerySchemaChange::step);
        plan.withheld = diff.withheld;

        if diff.impossible.is_empty() {
            plan.changes = diff.changes;
            return plan;
        }
        let dangerous = match self.recreate {
            Some(BigQueryRecreatePolicy::DangerouslyWithDataLoss) => true,
            Some(BigQueryRecreatePolicy::IfEmpty) if live.num_rows == Some(0) => false,
            Some(BigQueryRecreatePolicy::IfEmpty) => {
                plan.changes = diff.changes;
                plan.impossible = diff.impossible;
                plan.refusal = Some(BigQueryRefusal::NotEmpty {
                    num_rows: live.num_rows,
                });
                return plan;
            }
            None => {
                plan.changes = diff.changes;
                plan.impossible = diff.impossible;
                plan.refusal = Some(BigQueryRefusal::NoRecreateOptIn);
                return plan;
            }
        };
        let target = self.target(Some(live));
        let same_partitioning = match (&target.partitioning, &live.partitioning) {
            (Some(a), Some(b)) => a.same_as(b),
            (None, None) => true,
            _ => false,
        };
        // `CREATE OR REPLACE` must restate the partitioning and clustering exactly.
        let method = if same_partitioning && same_columns(&target.clustering, &live.clustering) {
            BigQueryRecreateMethod::CreateOrReplace
        } else {
            BigQueryRecreateMethod::DropAndCreate
        };
        plan.recreate = Some(BigQueryRecreate {
            method,
            reasons: diff.impossible,
            target,
            num_rows: live.num_rows,
            num_bytes: live.num_bytes,
            row_access_policies: Vec::new(),
            dangerous,
            snapshot_first: self.snapshot_first,
        });
        plan
    }
}

/// Column names compare as BigQuery compares them, ignoring case.
fn same_name(a: &str, b: &str) -> bool {
    a.eq_ignore_ascii_case(b)
}

fn same_columns(a: &[String], b: &[String]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(a, b)| same_name(a, b))
}

fn find<'f>(fields: &'f [BigQueryFieldSchema], name: &str) -> Option<&'f BigQueryFieldSchema> {
    fields.iter().find(|f| same_name(&f.name, name))
}

fn join(prefix: &str, name: &str) -> String {
    if prefix.is_empty() {
        name.to_string()
    } else {
        format!("{prefix}.{name}")
    }
}

impl DeclaredColumn {
    /// The live column a declared top-level column matches: the one with its name, or else the
    /// one it is renamed from.
    fn matched<'f>(&self, live: &'f [BigQueryFieldSchema]) -> Option<&'f BigQueryFieldSchema> {
        find(live, &self.field.name)
            .or_else(|| self.renamed_from.as_deref().and_then(|old| find(live, old)))
    }
}

fn decimal_widens(from: Option<BigQueryDecimalParams>, to: Option<BigQueryDecimalParams>) -> bool {
    match (from, to) {
        (Some(_), None) => true,
        (Some(a), Some(b)) => {
            b.scale >= a.scale
                && i32::from(b.precision) - i32::from(b.scale)
                    >= i32::from(a.precision) - i32::from(a.scale)
        }
        (None, _) => false,
    }
}

fn length_widens(from: Option<u64>, to: Option<u64>) -> bool {
    match (from, to) {
        (Some(_), None) => true,
        (Some(a), Some(b)) => b > a,
        (None, _) => false,
    }
}

impl BigQueryFieldType {
    /// Whether `ALTER COLUMN SET DATA TYPE` takes this type to `to`.
    ///
    /// BigQuery takes INT64 to NUMERIC and a longer `STRING(n)`, and refuses INT64 to
    /// FLOAT64, so FLOAT64 is never a widening target here. The other pairs follow
    /// GoogleSQL's assignability of the exact numeric types and of type parameters.
    fn widens_to(&self, to: &BigQueryFieldType) -> bool {
        use BigQueryFieldType::*;
        match (self, to) {
            (Int64, Numeric(None) | BigNumeric(None)) => true,
            (Numeric(None), BigNumeric(None)) => true,
            (Numeric(a), Numeric(b)) => decimal_widens(*a, *b),
            (BigNumeric(a), BigNumeric(b)) => decimal_widens(*a, *b),
            (String { max_length: a }, String { max_length: b }) => length_widens(*a, *b),
            (Bytes { max_length: a }, Bytes { max_length: b }) => length_widens(*a, *b),
            _ => false,
        }
    }
}

struct Diff<'a> {
    declaration: &'a BigQueryTableDeclaration,
    changes: Vec<BigQuerySchemaChange>,
    withheld: Vec<BigQueryWithheldChange>,
    impossible: Vec<BigQuerySchemaChange>,
}

impl Diff<'_> {
    /// An undeclared item: removed with `prune_undeclared()`, kept and reported without.
    fn undeclared(&mut self, change: BigQuerySchemaChange) {
        if !self.declaration.prune {
            self.withheld.push(BigQueryWithheldChange {
                change,
                reason: BigQueryWithheldReason::PruneUndeclared,
            });
        } else if change.step() == ChangeStep::Impossible {
            self.impossible.push(change);
        } else {
            self.changes.push(change);
        }
    }

    fn add(&mut self, path: String, field: &BigQueryFieldSchema) {
        let field = field.clone();
        if field.mode == BigQueryFieldMode::Required {
            self.impossible
                .push(BigQuerySchemaChange::AddRequiredColumn { path, field });
        } else {
            self.changes
                .push(BigQuerySchemaChange::AddColumn { path, field });
        }
    }

    fn columns(&mut self, live: &[BigQueryFieldSchema]) {
        let declaration = self.declaration;
        let mut matched_names = HashSet::new();
        for column in &declaration.columns {
            let Some(current) = column.matched(live) else {
                self.add(column.field.name.clone(), &column.field);
                continue;
            };
            matched_names.insert(current.name.to_ascii_lowercase());
            if !same_name(&current.name, &column.field.name) {
                self.changes.push(BigQuerySchemaChange::RenameColumn {
                    from: current.name.clone(),
                    to: column.field.name.clone(),
                });
            }
            self.compare(
                &current.name,
                Some(&column.field.name),
                &column.field,
                current,
            );
        }
        for current in live {
            if !matched_names.contains(&current.name.to_ascii_lowercase()) {
                self.undeclared(BigQuerySchemaChange::DropColumn {
                    column: current.name.clone(),
                    field_type: current.field_type.clone(),
                });
            }
        }
    }

    /// Compares one declared column or field with the live one at `path`. `ddl_name` is the
    /// top-level column's name after any rename, `None` for a nested field, which no DDL can
    /// alter.
    fn compare(
        &mut self,
        path: &str,
        ddl_name: Option<&str>,
        declared: &BigQueryFieldSchema,
        current: &BigQueryFieldSchema,
    ) {
        match (&declared.field_type, &current.field_type) {
            (
                BigQueryFieldType::Struct(declared_fields),
                BigQueryFieldType::Struct(live_fields),
            ) => self.nested(path, declared_fields, live_fields),
            (to, from) if to == from => {}
            (to, from) if ddl_name.is_some() && from.widens_to(to) => {
                let change = BigQuerySchemaChange::WidenColumn {
                    column: ddl_name.unwrap_or(path).to_string(),
                    from: from.clone(),
                    to: to.clone(),
                };
                if self.declaration.allow_widening {
                    self.changes.push(change);
                } else {
                    self.withheld.push(BigQueryWithheldChange {
                        change,
                        reason: BigQueryWithheldReason::AllowWidening,
                    });
                }
            }
            (to, from) => self
                .impossible
                .push(BigQuerySchemaChange::ChangeColumnType {
                    path: path.to_string(),
                    from: from.clone(),
                    to: to.clone(),
                }),
        }
        match (declared.mode, current.mode) {
            (to, from) if to == from => {}
            (BigQueryFieldMode::Nullable, BigQueryFieldMode::Required) => {
                self.changes.push(BigQuerySchemaChange::RelaxColumn {
                    path: path.to_string(),
                })
            }
            (to, from) => self
                .impossible
                .push(BigQuerySchemaChange::ChangeColumnMode {
                    path: path.to_string(),
                    from,
                    to,
                }),
        }
        if let Some(to) = &declared.description {
            if current.description.as_deref().unwrap_or("") != to {
                self.changes
                    .push(BigQuerySchemaChange::SetColumnDescription {
                        path: path.to_string(),
                        from: current.description.clone(),
                        to: to.clone(),
                    });
            }
        }
        if let Some(to) = &declared.default_value_expression {
            if current.default_value_expression.as_ref() != Some(to) {
                self.changes.push(BigQuerySchemaChange::SetColumnDefault {
                    path: path.to_string(),
                    from: current.default_value_expression.clone(),
                    to: to.clone(),
                });
            }
        }
    }

    fn nested(
        &mut self,
        prefix: &str,
        declared: &[BigQueryFieldSchema],
        live: &[BigQueryFieldSchema],
    ) {
        for field in declared {
            let path = join(prefix, &field.name);
            match find(live, &field.name) {
                Some(current) => self.compare(&path, None, field, current),
                None => self.add(path, field),
            }
        }
        for current in live {
            if find(declared, &current.name).is_none() {
                self.undeclared(BigQuerySchemaChange::DropNestedField {
                    path: join(prefix, &current.name),
                    field_type: current.field_type.clone(),
                });
            }
        }
    }

    fn settings(&mut self, live: &LiveTable) {
        let declaration = self.declaration;
        if let Some(to) = &declaration.description {
            if live.description.as_deref().unwrap_or("") != to {
                self.changes.push(BigQuerySchemaChange::SetDescription {
                    from: live.description.clone(),
                    to: to.clone(),
                });
            }
        }
        for (key, to) in declaration.labels.iter() {
            let from = live.labels.get(key);
            if from != Some(to) {
                self.changes.push(BigQuerySchemaChange::SetLabel {
                    key: key.to_string(),
                    from: from.map(str::to_string),
                    to: to.to_string(),
                });
            }
        }
        for (key, value) in live.labels.iter() {
            if declaration.labels.get(key).is_none() {
                self.undeclared(BigQuerySchemaChange::RemoveLabel {
                    key: key.to_string(),
                    value: value.to_string(),
                });
            }
        }
        if let Some(to) = declaration.expiration {
            if live.expiration != Some(to) {
                self.changes.push(BigQuerySchemaChange::SetExpiration {
                    from: live.expiration,
                    to,
                });
            }
        }
        match (&declaration.partitioning, &live.partitioning) {
            (Some(declared), Some(current)) if declared.same_as(current) => {
                if let Some(to) = declaration.partition_expiration {
                    if live.partition_expiration != Some(to) {
                        self.changes
                            .push(BigQuerySchemaChange::SetPartitionExpiration {
                                from: live.partition_expiration,
                                to,
                            });
                    }
                }
            }
            (Some(declared), current) => {
                self.impossible
                    .push(BigQuerySchemaChange::ChangePartitioning {
                        from: current.clone(),
                        to: Some(declared.clone()),
                    })
            }
            (None, Some(current)) => self.undeclared(BigQuerySchemaChange::ChangePartitioning {
                from: Some(current.clone()),
                to: None,
            }),
            (None, None) => {}
        }
        match &declaration.clustering {
            Some(to) if !same_columns(to, &live.clustering) => {
                self.changes.push(BigQuerySchemaChange::SetClustering {
                    from: live.clustering.clone(),
                    to: to.clone(),
                })
            }
            Some(_) => {}
            None if !live.clustering.is_empty() => {
                self.undeclared(BigQuerySchemaChange::RemoveClustering {
                    from: live.clustering.clone(),
                })
            }
            None => {}
        }
        match (&declaration.primary_key, &live.primary_key) {
            (Some(to), current)
                if current
                    .as_ref()
                    .is_none_or(|current| !same_columns(to, current)) =>
            {
                self.changes.push(BigQuerySchemaChange::SetPrimaryKey {
                    from: current.clone(),
                    to: to.clone(),
                })
            }
            (None, Some(current)) => self.undeclared(BigQuerySchemaChange::RemovePrimaryKey {
                from: current.clone(),
            }),
            _ => {}
        }
    }
}

impl BigQueryFieldSchema {
    /// This declared field with what it leaves undeclared taken from `current`: descriptions, defaults and,
    /// unless pruning, nested fields.
    fn merged_with(
        &self,
        current: Option<&BigQueryFieldSchema>,
        prune: bool,
    ) -> BigQueryFieldSchema {
        let mut field = self.clone();
        let Some(current) = current else {
            return field;
        };
        if field.description.is_none() {
            field.description.clone_from(&current.description);
        }
        if field.default_value_expression.is_none() {
            field
                .default_value_expression
                .clone_from(&current.default_value_expression);
        }
        if let (
            BigQueryFieldType::Struct(declared_fields),
            BigQueryFieldType::Struct(live_fields),
        ) = (&self.field_type, &current.field_type)
        {
            let mut fields: Vec<BigQueryFieldSchema> = declared_fields
                .iter()
                .map(|f| f.merged_with(find(live_fields, &f.name), prune))
                .collect();
            if !prune {
                fields.extend(
                    live_fields
                        .iter()
                        .filter(|f| find(declared_fields, &f.name).is_none())
                        .cloned(),
                );
            }
            field.field_type = BigQueryFieldType::Struct(fields);
        }
        field
    }
}

impl BigQueryTableDeclaration {
    /// The whole table as `.sync()` creates or recreates it: the declaration, plus what it leaves
    /// undeclared and does not prune.
    fn target(&self, live: Option<&LiveTable>) -> BigQueryTableTarget {
        let prune = self.prune;
        let live_fields = live.map_or(&[][..], |l| &l.schema.fields[..]);
        let mut columns: Vec<BigQueryFieldSchema> = self
            .columns
            .iter()
            .map(|c| c.field.merged_with(c.matched(live_fields), prune))
            .collect();
        let mut labels = BigQueryLabels::new();
        let mut target = BigQueryTableTarget {
            columns: Vec::new(),
            primary_key: self.primary_key.clone(),
            partitioning: self.partitioning.clone(),
            partition_expiration: self.partition_expiration,
            clustering: self.clustering.clone().unwrap_or_default(),
            description: self.description.clone(),
            labels: BigQueryLabels::new(),
            expiration: self.expiration,
        };
        if let Some(live) = live {
            if !prune {
                let declared_live: HashSet<String> = self
                    .columns
                    .iter()
                    .filter_map(|c| c.matched(live_fields))
                    .map(|f| f.name.to_ascii_lowercase())
                    .collect();
                columns.extend(
                    live_fields
                        .iter()
                        .filter(|f| !declared_live.contains(&f.name.to_ascii_lowercase()))
                        .cloned(),
                );
                labels.clone_from(&live.labels);
                if target.primary_key.is_none() {
                    target.primary_key.clone_from(&live.primary_key);
                }
                if target.partitioning.is_none() {
                    target.partitioning.clone_from(&live.partitioning);
                }
                if self.clustering.is_none() {
                    target.clustering.clone_from(&live.clustering);
                }
            }
            let same_partitioning = match (&target.partitioning, &live.partitioning) {
                (Some(a), Some(b)) => a.same_as(b),
                _ => false,
            };
            if target.partition_expiration.is_none() && same_partitioning {
                target.partition_expiration = live.partition_expiration;
            }
            if target.description.is_none() {
                target.description.clone_from(&live.description);
            }
            if target.expiration.is_none() {
                target.expiration = live.expiration;
            }
        }
        for (key, value) in self.labels.iter() {
            labels.insert(key, value);
        }
        target.columns = columns;
        target.labels = labels;
        target
    }
}

/// The field at the dotted `path`, matched ignoring case as the diff matched it.
fn field_at<'f>(
    fields: &'f mut [v2::TableFieldSchema],
    path: &str,
) -> Option<&'f mut v2::TableFieldSchema> {
    let (head, rest) = match path.split_once('.') {
        Some((head, rest)) => (head, Some(rest)),
        None => (path, None),
    };
    let field = fields.iter_mut().find(|f| same_name(&f.name, head))?;
    match rest {
        Some(rest) => field_at(&mut field.fields, rest),
        None => Some(field),
    }
}

/// Appends `field` at `path`, inside the record its parent path names.
fn add_field(schema: &mut v2::TableSchema, path: &str, field: v2::TableFieldSchema) {
    match path.rsplit_once('.') {
        Some((parent, _)) => {
            if let Some(parent) = field_at(&mut schema.fields, parent) {
                parent.fields.push(field);
            }
        }
        None => schema.fields.push(field),
    }
}

impl BigQueryTablePlan {
    /// The first `PatchTable` body: every `[patch]` change, with new columns added without their
    /// default value, which BigQuery refuses in the same step.
    ///
    /// A schema in the body replaces the whole schema, so it is the live one with the changes
    /// edited in, every column and description included, and everything the crate does
    /// not model, such as policy tags, carried back as it was.
    ///
    /// # Errors
    /// [`BigQueryError::InvalidParametersError`] for a partition expiration whose milliseconds do
    /// not fit the request field.
    pub(crate) fn patch_body(&self, live: &v2::Table) -> BigQueryResult<Option<v2::Table>> {
        let changes = &self.changes;
        let mut body = v2::Table::default();
        let mut schema = live.schema.clone().unwrap_or_default();
        let mut schema_edited = false;
        let mut any = false;
        let foreign_keys = || {
            live.table_constraints
                .as_ref()
                .map(|c| c.foreign_keys.clone())
                .unwrap_or_default()
        };
        for change in changes.iter().filter(|c| c.step() == ChangeStep::Patch) {
            any = true;
            if matches!(
                change,
                BigQuerySchemaChange::AddColumn { .. }
                    | BigQuerySchemaChange::RelaxColumn { .. }
                    | BigQuerySchemaChange::SetColumnDescription { .. }
                    | BigQuerySchemaChange::SetColumnDefault { .. }
            ) {
                schema_edited = true;
            }
            match change {
                BigQuerySchemaChange::AddColumn { path, field } => {
                    let mut field = v2::TableFieldSchema::from(field);
                    field.default_value_expression = None;
                    add_field(&mut schema, path, field);
                }
                BigQuerySchemaChange::RelaxColumn { path } => {
                    if let Some(field) = field_at(&mut schema.fields, path) {
                        field.mode = "NULLABLE".into();
                    }
                }
                BigQuerySchemaChange::SetColumnDescription { path, to, .. } => {
                    if let Some(field) = field_at(&mut schema.fields, path) {
                        field.description = Some(to.clone());
                    }
                }
                BigQuerySchemaChange::SetColumnDefault { path, to, .. } => {
                    if let Some(field) = field_at(&mut schema.fields, path) {
                        field.default_value_expression = Some(to.clone());
                    }
                }
                BigQuerySchemaChange::SetDescription { to, .. } => {
                    body.description = Some(to.clone())
                }
                BigQuerySchemaChange::SetLabel { key, to, .. } => {
                    body.labels.insert(key.clone(), to.clone());
                }
                BigQuerySchemaChange::SetExpiration { to, .. } => {
                    body.expiration_time = Some(to.as_millisecond());
                }
                BigQuerySchemaChange::SetPartitionExpiration { to, .. } => {
                    body.time_partitioning = Some(v2::TimePartitioning {
                        expiration_ms: Some(millis("partition_expiration", *to)?),
                        ..live.time_partitioning.clone().unwrap_or_default()
                    });
                }
                BigQuerySchemaChange::SetClustering { to, .. } => {
                    body.clustering = Some(v2::Clustering { fields: to.clone() });
                }
                BigQuerySchemaChange::SetPrimaryKey { to, .. } => {
                    body.table_constraints = Some(v2::TableConstraints {
                        primary_key: Some(v2::PrimaryKey {
                            columns: to.clone(),
                        }),
                        foreign_keys: foreign_keys(),
                    });
                }
                BigQuerySchemaChange::RemovePrimaryKey { .. } => {
                    body.table_constraints = Some(v2::TableConstraints {
                        primary_key: None,
                        foreign_keys: foreign_keys(),
                    });
                }
                _ => {}
            }
        }
        body.schema = schema_edited.then_some(schema);
        Ok(any.then_some(body))
    }

    /// The second `PatchTable` body, setting the defaults of the columns the first one added; built
    /// from the table the first patch returned. `None` when no added column has a default.
    pub(crate) fn defaults_body(&self, latest: &v2::Table) -> Option<v2::Table> {
        let changes = &self.changes;
        let mut schema = latest.schema.clone().unwrap_or_default();
        let mut any = false;
        for change in changes {
            if let BigQuerySchemaChange::AddColumn { path, field } = change {
                if let Some(default) = &field.default_value_expression {
                    if let Some(field) = field_at(&mut schema.fields, path) {
                        field.default_value_expression = Some(default.clone());
                        any = true;
                    }
                }
            }
        }
        any.then(|| v2::Table {
            schema: Some(schema),
            ..Default::default()
        })
    }

    /// The `UpdateTable` body: the whole of `latest`, which must be the table as it is now, without
    /// the removed labels and clustering. Update clears whatever its body leaves out.
    pub(crate) fn update_body(&self, latest: &v2::Table) -> Option<v2::Table> {
        let changes = &self.changes;
        let mut body = latest.clone();
        let mut any = false;
        for change in changes {
            match change {
                BigQuerySchemaChange::RemoveLabel { key, .. } => {
                    body.labels.remove(key);
                    any = true;
                }
                BigQuerySchemaChange::RemoveClustering { .. } => {
                    body.clustering = None;
                    any = true;
                }
                _ => {}
            }
        }
        any.then_some(body)
    }
}

#[cfg(test)]
mod tests;
