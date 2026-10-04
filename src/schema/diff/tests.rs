//! One test per row of the probe's "Confirmed diff classes" table
//! (`docs/schema-probe-2026-10.md`), plus the normalisation, rename, recreate and request-body
//! rules the plan depends on.

use super::*;
use crate::schema::declaration::BigQueryTableDeclarationDraft;
use crate::{
    BigQueryDatasetId, BigQueryFieldMode, BigQueryFieldSchema, BigQueryFieldType,
    BigQueryPartitionUnit, BigQueryPartitioning, BigQueryRecreateMethod, BigQueryRecreatePolicy,
    BigQueryRefusal, BigQuerySchemaColumn, BigQuerySchemaColumnsBuilder, BigQueryTableId,
    BigQueryWithheldChange, BigQueryWithheldReason,
};
use std::collections::HashMap;

const C: BigQuerySchemaColumnsBuilder = BigQuerySchemaColumnsBuilder;

fn table_ref() -> BigQueryTableRef {
    BigQueryDatasetId::from_static("ds").table(BigQueryTableId::from_static("t"))
}

fn f(name: &str, ty: &str, mode: &str) -> v2::TableFieldSchema {
    v2::TableFieldSchema {
        name: name.into(),
        r#type: ty.into(),
        mode: mode.into(),
        ..Default::default()
    }
}

fn rec(name: &str, fields: Vec<v2::TableFieldSchema>) -> v2::TableFieldSchema {
    v2::TableFieldSchema {
        fields,
        ..f(name, "RECORD", "NULLABLE")
    }
}

/// The probe's `t_patch` table: `id INT64 REQUIRED, name STRING REQUIRED, rec RECORD{a INT64},
/// n INT64, s STRING`, in the legacy type names and with the empty mode `GetTable` returns for
/// DDL-created columns on `n`.
fn base_fields() -> Vec<v2::TableFieldSchema> {
    vec![
        f("id", "INTEGER", "REQUIRED"),
        f("name", "STRING", "REQUIRED"),
        rec("rec", vec![f("a", "INTEGER", "NULLABLE")]),
        f("n", "INTEGER", ""),
        f("s", "STRING", "NULLABLE"),
    ]
}

fn base_columns() -> Vec<BigQuerySchemaColumn> {
    vec![
        C.field("id").int64().required(),
        C.field("name").string().required(),
        C.field("rec").record(|r| r.fields([r.field("a").int64()])),
        C.field("n").int64(),
        C.field("s").string(),
    ]
}

fn raw(fields: Vec<v2::TableFieldSchema>) -> v2::Table {
    v2::Table {
        etag: "e0".into(),
        r#type: "TABLE".into(),
        schema: Some(v2::TableSchema {
            fields,
            ..Default::default()
        }),
        num_rows: Some(3),
        num_bytes: Some(96),
        ..Default::default()
    }
}

fn declare(
    columns: Vec<BigQuerySchemaColumn>,
    set: impl FnOnce(&mut BigQueryTableDeclarationDraft),
) -> BigQueryTableDeclaration {
    let mut draft = BigQueryTableDeclarationDraft::new(table_ref());
    draft.columns = columns;
    set(&mut draft);
    BigQueryTableDeclaration::try_from(draft).expect("a valid declaration")
}

fn plan_against(declaration: &BigQueryTableDeclaration, table: v2::Table) -> BigQueryTablePlan {
    let live = LiveTable::try_from(table).expect("a table the crate models");
    plan_table(declaration, table_ref(), Some(&live))
}

fn plan(columns: Vec<BigQuerySchemaColumn>, table: v2::Table) -> BigQueryTablePlan {
    plan_against(&declare(columns, |_| {}), table)
}

fn with(
    mut columns: Vec<BigQuerySchemaColumn>,
    extra: BigQuerySchemaColumn,
) -> Vec<BigQuerySchemaColumn> {
    columns.push(extra);
    columns
}

fn field(
    name: &str,
    field_type: BigQueryFieldType,
    mode: BigQueryFieldMode,
) -> BigQueryFieldSchema {
    BigQueryFieldSchema {
        name: name.into(),
        field_type,
        mode,
        description: None,
        default_value_expression: None,
    }
}

const STRING: BigQueryFieldType = BigQueryFieldType::String { max_length: None };

fn assert_only_change(plan: &BigQueryTablePlan, change: BigQuerySchemaChange) {
    assert_eq!(plan.changes, vec![change], "{plan}");
    assert!(plan.withheld.is_empty(), "{plan}");
    assert!(
        plan.impossible.is_empty() && plan.refusal.is_none(),
        "{plan}"
    );
}

fn assert_impossible(plan: &BigQueryTablePlan, change: BigQuerySchemaChange) {
    assert_eq!(plan.impossible, vec![change], "{plan}");
    assert_eq!(
        plan.refusal,
        Some(BigQueryRefusal::NoRecreateOptIn),
        "{plan}"
    );
    assert!(plan.recreate.is_none(), "{plan}");
}

#[test]
fn legacy_type_names_and_an_empty_mode_are_no_change() {
    let plan = plan(base_columns(), raw(base_fields()));
    assert!(plan.is_empty(), "{plan}");
}

#[test]
fn a_type_parameter_is_part_of_the_type() {
    let mut fields = base_fields();
    fields[4].max_length = 10;
    let mut columns = base_columns();
    columns[4] = C.field("s").string_with_max_length(10);
    let same = plan(columns, raw(fields.clone()));
    assert!(same.is_empty(), "{same}");

    let mut columns = base_columns();
    columns[4] = C.field("s").string();
    let unbounded = plan(columns, raw(fields));
    assert_eq!(
        unbounded.withheld,
        vec![BigQueryWithheldChange {
            change: BigQuerySchemaChange::WidenColumn {
                column: "s".into(),
                from: BigQueryFieldType::String {
                    max_length: Some(10)
                },
                to: STRING,
            },
            reason: BigQueryWithheldReason::AllowWidening,
        }],
        "{unbounded}"
    );
}

#[test]
fn add_nullable_column_is_a_patch() {
    let plan = plan(
        with(base_columns(), C.field("c_null").string()),
        raw(base_fields()),
    );
    assert_only_change(
        &plan,
        BigQuerySchemaChange::AddColumn {
            path: "c_null".into(),
            field: field("c_null", STRING, BigQueryFieldMode::Nullable),
        },
    );
}

#[test]
fn add_repeated_column_is_a_patch() {
    let plan = plan(
        with(base_columns(), C.field("c_rep").int64().repeated()),
        raw(base_fields()),
    );
    assert_only_change(
        &plan,
        BigQuerySchemaChange::AddColumn {
            path: "c_rep".into(),
            field: field(
                "c_rep",
                BigQueryFieldType::Int64,
                BigQueryFieldMode::Repeated,
            ),
        },
    );
}

#[test]
fn add_nullable_field_inside_an_existing_record_is_a_patch() {
    let mut columns = base_columns();
    columns[2] = C
        .field("rec")
        .record(|r| r.fields([r.field("a").int64(), r.field("b").string()]));
    let plan = plan(columns, raw(base_fields()));
    assert_only_change(
        &plan,
        BigQuerySchemaChange::AddColumn {
            path: "rec.b".into(),
            field: field("b", STRING, BigQueryFieldMode::Nullable),
        },
    );
}

#[test]
fn add_record_column_is_a_patch() {
    let plan = plan(
        with(
            base_columns(),
            C.field("addr")
                .record(|r| r.fields([r.field("city").string()])),
        ),
        raw(base_fields()),
    );
    assert_only_change(
        &plan,
        BigQuerySchemaChange::AddColumn {
            path: "addr".into(),
            field: field(
                "addr",
                BigQueryFieldType::Struct(vec![field("city", STRING, BigQueryFieldMode::Nullable)]),
                BigQueryFieldMode::Nullable,
            ),
        },
    );
}

#[test]
fn add_column_with_default_is_added_first_and_defaulted_by_a_second_patch() {
    let plan = plan(
        with(
            base_columns(),
            C.field("c_def").string().default_value("'x'"),
        ),
        raw(base_fields()),
    );
    let mut added = field("c_def", STRING, BigQueryFieldMode::Nullable);
    added.default_value_expression = Some("'x'".into());
    assert_only_change(
        &plan,
        BigQuerySchemaChange::AddColumn {
            path: "c_def".into(),
            field: added,
        },
    );

    let first = patch_body(&raw(base_fields()), &plan.changes).expect("a first patch");
    let first_fields = &first.schema.as_ref().expect("a schema").fields;
    let c_def = first_fields.last().expect("the added column");
    assert_eq!(c_def.name, "c_def");
    assert_eq!(
        c_def.default_value_expression, None,
        "1h: rejected in one step"
    );

    let second = defaults_body(&first, &plan.changes).expect("a second patch");
    let second_fields = &second.schema.as_ref().expect("a schema").fields;
    assert_eq!(second_fields.len(), first_fields.len());
    assert_eq!(
        second_fields
            .last()
            .and_then(|c| c.default_value_expression.as_deref()),
        Some("'x'")
    );
}

#[test]
fn set_default_on_existing_column_is_a_patch() {
    let mut columns = base_columns();
    columns[4] = C.field("s").string().default_value("'dflt'");
    let plan = plan(columns, raw(base_fields()));
    assert_only_change(
        &plan,
        BigQuerySchemaChange::SetColumnDefault {
            path: "s".into(),
            from: None,
            to: "'dflt'".into(),
        },
    );
    assert_eq!(defaults_body(&raw(base_fields()), &plan.changes), None);
    let body = patch_body(&raw(base_fields()), &plan.changes).expect("a patch");
    assert_eq!(
        body.schema.expect("a schema").fields[4]
            .default_value_expression
            .as_deref(),
        Some("'dflt'")
    );
}

#[test]
fn relax_required_to_nullable_is_a_patch() {
    let mut columns = base_columns();
    columns[1] = C.field("name").string();
    let plan = plan(columns, raw(base_fields()));
    assert_only_change(
        &plan,
        BigQuerySchemaChange::RelaxColumn {
            path: "name".into(),
        },
    );
    let body = patch_body(&raw(base_fields()), &plan.changes).expect("a patch");
    assert_eq!(body.schema.expect("a schema").fields[1].mode, "NULLABLE");
}

#[test]
fn column_description_patch_carries_every_column_and_its_description() {
    let mut fields = base_fields();
    fields[0].description = Some("id-desc".into());
    fields[2].fields[0].description = Some("a-desc".into());
    fields[3].policy_tags = Some(v2::table_field_schema::PolicyTagList {
        names: vec!["projects/p/locations/us/taxonomies/1/policyTags/2".into()],
    });
    let mut columns = base_columns();
    columns[4] = C.field("s").string().description("s-desc");
    let plan = plan(columns, raw(fields.clone()));
    assert_only_change(
        &plan,
        BigQuerySchemaChange::SetColumnDescription {
            path: "s".into(),
            from: None,
            to: "s-desc".into(),
        },
    );
    let body = patch_body(&raw(fields.clone()), &plan.changes).expect("a patch");
    let mut expected = fields;
    expected[4].description = Some("s-desc".into());
    assert_eq!(body.schema.expect("a schema").fields, expected, "2k, 3c");
}

#[test]
fn table_description_is_a_patch_without_a_schema() {
    let plan = plan_against(
        &declare(base_columns(), |d| d.description = Some("Orders".into())),
        raw(base_fields()),
    );
    assert_only_change(
        &plan,
        BigQuerySchemaChange::SetDescription {
            from: None,
            to: "Orders".into(),
        },
    );
    let body = patch_body(&raw(base_fields()), &plan.changes).expect("a patch");
    assert_eq!(body.description.as_deref(), Some("Orders"));
    assert_eq!(body.schema, None, "1f: a body with only the description");
}

#[test]
fn add_or_change_label_is_a_patch_of_that_label_only() {
    let mut table = raw(base_fields());
    table.labels = HashMap::from([("env".to_string(), "dev".to_string())]);
    let plan = plan_against(
        &declare(base_columns(), |d| {
            d.labels = [("env", "prod"), ("team", "shop")]
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .into();
        }),
        table.clone(),
    );
    assert_eq!(
        plan.changes,
        vec![
            BigQuerySchemaChange::SetLabel {
                key: "env".into(),
                from: Some("dev".into()),
                to: "prod".into()
            },
            BigQuerySchemaChange::SetLabel {
                key: "team".into(),
                from: None,
                to: "shop".into()
            },
        ],
        "{plan}"
    );
    let body = patch_body(&table, &plan.changes).expect("a patch");
    assert_eq!(body.labels.len(), 2);
}

#[test]
fn remove_label_is_an_update_with_prune_and_withheld_without() {
    let mut table = raw(base_fields());
    table.labels = HashMap::from([("old".to_string(), "x".to_string())]);
    let removal = BigQuerySchemaChange::RemoveLabel {
        key: "old".into(),
        value: "x".into(),
    };
    let kept = plan(base_columns(), table.clone());
    assert_eq!(
        kept.withheld,
        vec![BigQueryWithheldChange {
            change: removal.clone(),
            reason: BigQueryWithheldReason::PruneUndeclared
        }],
        "{kept}"
    );
    assert!(kept.changes.is_empty(), "{kept}");

    let pruned = plan_against(&declare(base_columns(), |d| d.prune = true), table.clone());
    assert_only_change(&pruned, removal);
    assert_eq!(patch_body(&table, &pruned.changes), None);
    let body = update_body(&table, &pruned.changes).expect("an update");
    assert!(body.labels.is_empty());
    assert_eq!(
        body.schema, table.schema,
        "3g: Update starts from the whole body"
    );
    assert_eq!(body.etag, "e0");
}

#[test]
fn table_expiration_is_a_patch() {
    let at = jiff::Timestamp::from_second(1_900_000_000).expect("a timestamp");
    let plan = plan_against(
        &declare(base_columns(), |d| d.expiration = Some(at)),
        raw(base_fields()),
    );
    assert_only_change(
        &plan,
        BigQuerySchemaChange::SetExpiration {
            from: None,
            to: at.as_millisecond(),
        },
    );
}

fn partitioned(table: &mut v2::Table) {
    table
        .schema
        .as_mut()
        .expect("a schema")
        .fields
        .push(f("ts", "TIMESTAMP", "NULLABLE"));
    table.time_partitioning = Some(v2::TimePartitioning {
        r#type: "DAY".into(),
        expiration_ms: None,
        field: Some("ts".into()),
    });
}

fn day_on_ts() -> BigQueryPartitioning {
    BigQueryPartitioning::Time {
        unit: BigQueryPartitionUnit::Day,
        column: Some("ts".into()),
    }
}

#[test]
fn partition_expiration_is_a_patch_restating_the_partitioning() {
    let mut table = raw(base_fields());
    partitioned(&mut table);
    let plan = plan_against(
        &declare(with(base_columns(), C.field("ts").timestamp()), |d| {
            d.partitioning = Some(day_on_ts());
            d.partition_expiration = Some(std::time::Duration::from_secs(86_400));
        }),
        table.clone(),
    );
    assert_only_change(
        &plan,
        BigQuerySchemaChange::SetPartitionExpiration {
            from: None,
            to: 86_400_000,
        },
    );
    let body = patch_body(&table, &plan.changes).expect("a patch");
    assert_eq!(
        body.time_partitioning,
        Some(v2::TimePartitioning {
            r#type: "DAY".into(),
            expiration_ms: Some(86_400_000),
            field: Some("ts".into()),
        }),
        "2j: same type and field"
    );
}

#[test]
fn add_or_change_clustering_is_a_patch() {
    let mut table = raw(base_fields());
    table.clustering = Some(v2::Clustering {
        fields: vec!["s".into()],
    });
    let plan = plan_against(
        &declare(base_columns(), |d| {
            d.clustering = Some(vec!["n".into(), "s".into()])
        }),
        table.clone(),
    );
    assert_only_change(
        &plan,
        BigQuerySchemaChange::SetClustering {
            from: vec!["s".into()],
            to: vec!["n".into(), "s".into()],
        },
    );
    let body = patch_body(&table, &plan.changes).expect("a patch");
    assert_eq!(body.clustering.expect("clustering").fields, ["n", "s"]);
}

#[test]
fn remove_clustering_is_an_update_with_prune() {
    let mut table = raw(base_fields());
    table.clustering = Some(v2::Clustering {
        fields: vec!["s".into()],
    });
    let plan = plan_against(&declare(base_columns(), |d| d.prune = true), table.clone());
    assert_only_change(
        &plan,
        BigQuerySchemaChange::RemoveClustering {
            from: vec!["s".into()],
        },
    );
    let body = update_body(&table, &plan.changes).expect("an update");
    assert_eq!(body.clustering, None, "1m': Patch rejects an empty list");
}

#[test]
fn add_primary_key_is_a_patch_keeping_foreign_keys() {
    let mut table = raw(base_fields());
    let foreign = v2::ForeignKey {
        name: "fk".into(),
        ..Default::default()
    };
    table.table_constraints = Some(v2::TableConstraints {
        primary_key: None,
        foreign_keys: vec![foreign.clone()],
    });
    let plan = plan_against(
        &declare(base_columns(), |d| d.primary_key = Some(vec!["id".into()])),
        table.clone(),
    );
    assert_only_change(
        &plan,
        BigQuerySchemaChange::SetPrimaryKey {
            from: None,
            to: vec!["id".into()],
        },
    );
    let body = patch_body(&table, &plan.changes).expect("a patch");
    assert_eq!(
        body.table_constraints,
        Some(v2::TableConstraints {
            primary_key: Some(v2::PrimaryKey {
                columns: vec!["id".into()]
            }),
            foreign_keys: vec![foreign],
        })
    );
}

#[test]
fn remove_primary_key_is_a_patch_with_prune() {
    let mut table = raw(base_fields());
    table.table_constraints = Some(v2::TableConstraints {
        primary_key: Some(v2::PrimaryKey {
            columns: vec!["id".into()],
        }),
        foreign_keys: Vec::new(),
    });
    let plan = plan_against(&declare(base_columns(), |d| d.prune = true), table.clone());
    assert_only_change(
        &plan,
        BigQuerySchemaChange::RemovePrimaryKey {
            from: vec!["id".into()],
        },
    );
    let body = patch_body(&table, &plan.changes).expect("a patch");
    assert_eq!(
        body.table_constraints,
        Some(v2::TableConstraints {
            primary_key: None,
            foreign_keys: Vec::new()
        }),
        "1o"
    );
}

#[test]
fn drop_column_is_ddl_with_prune_and_withheld_without() {
    let mut columns = base_columns();
    columns.remove(4);
    let drop = BigQuerySchemaChange::DropColumn {
        column: "s".into(),
        field_type: STRING,
    };
    let kept = plan(columns.clone(), raw(base_fields()));
    assert_eq!(
        kept.withheld,
        vec![BigQueryWithheldChange {
            change: drop.clone(),
            reason: BigQueryWithheldReason::PruneUndeclared
        }],
        "{kept}"
    );
    assert!(kept.changes.is_empty());
    let pruned = plan_against(&declare(columns, |d| d.prune = true), raw(base_fields()));
    assert_only_change(&pruned, drop);
    assert_eq!(patch_body(&raw(base_fields()), &pruned.changes), None, "2k");
}

#[test]
fn drop_nested_field_is_impossible_with_prune_and_withheld_without() {
    let mut columns = base_columns();
    columns[2] = C.field("rec").record(|r| r.fields([r.field("b").string()]));
    let mut fields = base_fields();
    fields[2].fields.push(f("b", "STRING", "NULLABLE"));
    let drop = BigQuerySchemaChange::DropNestedField {
        path: "rec.a".into(),
        field_type: BigQueryFieldType::Int64,
    };
    let kept = plan(columns.clone(), raw(fields.clone()));
    assert_eq!(
        kept.withheld,
        vec![BigQueryWithheldChange {
            change: drop.clone(),
            reason: BigQueryWithheldReason::PruneUndeclared
        }],
        "{kept}"
    );
    let pruned = plan_against(&declare(columns, |d| d.prune = true), raw(fields));
    assert_impossible(&pruned, drop);
}

#[test]
fn widening_is_ddl_with_allow_widening_and_withheld_without() {
    let mut columns = base_columns();
    columns[3] = C.field("n").numeric();
    let widen = BigQuerySchemaChange::WidenColumn {
        column: "n".into(),
        from: BigQueryFieldType::Int64,
        to: BigQueryFieldType::Numeric(None),
    };
    let kept = plan(columns.clone(), raw(base_fields()));
    assert_eq!(
        kept.withheld,
        vec![BigQueryWithheldChange {
            change: widen.clone(),
            reason: BigQueryWithheldReason::AllowWidening
        }],
        "{kept}"
    );
    let widened = plan_against(
        &declare(columns, |d| d.allow_widening = true),
        raw(base_fields()),
    );
    assert_only_change(&widened, widen);
    assert_eq!(
        patch_body(&raw(base_fields()), &widened.changes),
        None,
        "2b"
    );
}

#[test]
fn widening_string_length_is_ddl() {
    let mut fields = base_fields();
    fields[4].max_length = 10;
    let mut columns = base_columns();
    columns[4] = C.field("s").string_with_max_length(20);
    let plan = plan_against(&declare(columns, |d| d.allow_widening = true), raw(fields));
    assert_only_change(
        &plan,
        BigQuerySchemaChange::WidenColumn {
            column: "s".into(),
            from: BigQueryFieldType::String {
                max_length: Some(10),
            },
            to: BigQueryFieldType::String {
                max_length: Some(20),
            },
        },
    );
}

#[test]
fn rename_is_ddl_and_a_no_op_once_done() {
    let mut columns = base_columns();
    columns[4] = C.field("s2").string().renamed_from("s");
    let rename = plan(columns.clone(), raw(base_fields()));
    assert_only_change(
        &rename,
        BigQuerySchemaChange::RenameColumn {
            from: "s".into(),
            to: "s2".into(),
        },
    );
    assert_eq!(patch_body(&raw(base_fields()), &rename.changes), None, "2f");

    let mut renamed = base_fields();
    renamed[4].name = "s2".into();
    let done = plan(columns.clone(), raw(renamed));
    assert!(done.is_empty(), "{done}");

    let mut missing = base_fields();
    missing.remove(4);
    let added = plan(columns, raw(missing));
    assert_only_change(
        &added,
        BigQuerySchemaChange::AddColumn {
            path: "s2".into(),
            field: field("s2", STRING, BigQueryFieldMode::Nullable),
        },
    );
}

#[test]
fn narrowing_is_impossible() {
    let mut fields = base_fields();
    fields[3].r#type = "NUMERIC".into();
    let plan_numeric = plan(base_columns(), raw(fields));
    assert_impossible(
        &plan_numeric,
        BigQuerySchemaChange::ChangeColumnType {
            path: "n".into(),
            from: BigQueryFieldType::Numeric(None),
            to: BigQueryFieldType::Int64,
        },
    );

    let mut fields = base_fields();
    fields[4].max_length = 20;
    let mut columns = base_columns();
    columns[4] = C.field("s").string_with_max_length(5);
    let plan_string = plan_against(&declare(columns, |d| d.allow_widening = true), raw(fields));
    assert_impossible(
        &plan_string,
        BigQuerySchemaChange::ChangeColumnType {
            path: "s".into(),
            from: BigQueryFieldType::String {
                max_length: Some(20),
            },
            to: BigQueryFieldType::String {
                max_length: Some(5),
            },
        },
    );
}

#[test]
fn other_type_changes_are_impossible_even_with_allow_widening() {
    for (declared, to) in [
        (C.field("n").string(), STRING),
        (C.field("n").float64(), BigQueryFieldType::Float64),
    ] {
        let mut columns = base_columns();
        columns[3] = declared;
        let plan = plan_against(
            &declare(columns, |d| d.allow_widening = true),
            raw(base_fields()),
        );
        assert_impossible(
            &plan,
            BigQuerySchemaChange::ChangeColumnType {
                path: "n".into(),
                from: BigQueryFieldType::Int64,
                to,
            },
        );
    }
}

#[test]
fn nullable_to_required_or_repeated_is_impossible() {
    for (declared, mode) in [
        (
            C.field("s").string().required(),
            BigQueryFieldMode::Required,
        ),
        (
            C.field("s").string().repeated(),
            BigQueryFieldMode::Repeated,
        ),
    ] {
        let mut columns = base_columns();
        columns[4] = declared;
        let plan = plan(columns, raw(base_fields()));
        assert_impossible(
            &plan,
            BigQuerySchemaChange::ChangeColumnMode {
                path: "s".into(),
                from: BigQueryFieldMode::Nullable,
                to: mode,
            },
        );
    }
}

#[test]
fn adding_a_required_column_is_impossible() {
    let plan = plan(
        with(base_columns(), C.field("r").string().required()),
        raw(base_fields()),
    );
    assert_impossible(
        &plan,
        BigQuerySchemaChange::AddRequiredColumn {
            path: "r".into(),
            field: field("r", STRING, BigQueryFieldMode::Required),
        },
    );
}

#[test]
fn adding_or_changing_partitioning_is_impossible() {
    let columns = with(base_columns(), C.field("ts").timestamp());
    let mut unpartitioned = raw(base_fields());
    unpartitioned
        .schema
        .as_mut()
        .expect("a schema")
        .fields
        .push(f("ts", "TIMESTAMP", "NULLABLE"));
    let add = plan_against(
        &declare(columns.clone(), |d| d.partitioning = Some(day_on_ts())),
        unpartitioned,
    );
    assert_impossible(
        &add,
        BigQuerySchemaChange::ChangePartitioning {
            from: None,
            to: Some(day_on_ts()),
        },
    );

    let month = BigQueryPartitioning::Time {
        unit: BigQueryPartitionUnit::Month,
        column: Some("ts".into()),
    };
    let mut table = raw(base_fields());
    partitioned(&mut table);
    let change = plan_against(
        &declare(columns, |d| d.partitioning = Some(month.clone())),
        table,
    );
    assert_impossible(
        &change,
        BigQuerySchemaChange::ChangePartitioning {
            from: Some(day_on_ts()),
            to: Some(month),
        },
    );
}

#[test]
fn changes_are_planned_in_write_order() {
    let mut table = raw(base_fields());
    table.labels = HashMap::from([("old".to_string(), "x".to_string())]);
    let columns = vec![
        C.field("id").int64().required(),
        C.field("name2").string().required().renamed_from("name"),
        C.field("rec").record(|r| r.fields([r.field("a").int64()])),
        C.field("n").numeric(),
        C.field("c").string(),
    ];
    let plan = plan_against(
        &declare(columns, |d| {
            d.prune = true;
            d.allow_widening = true;
            d.description = Some("d".into());
        }),
        table,
    );
    let steps: Vec<String> = plan
        .changes
        .iter()
        .map(|c| {
            c.to_string()
                .split(' ')
                .take(3)
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect();
    assert_eq!(
        steps,
        [
            "[patch] add column",
            "[patch] table description:",
            "[update] remove undeclared",
            "[ddl] RENAME COLUMN",
            "[ddl] widen `n`",
            "[ddl] DROP COLUMN",
        ],
        "{plan}"
    );
}

#[test]
fn a_missing_table_is_created_as_declared() {
    let declaration = declare(base_columns(), |d| {
        d.description = Some("Orders".into());
        d.labels = [("team".to_string(), "shop".to_string())].into();
    });
    let plan = plan_table(&declaration, table_ref(), None);
    let target = plan.create.expect("a create");
    assert_eq!(target.columns.len(), 5);
    assert_eq!(target.columns[2].name, "rec");
    assert_eq!(target.description.as_deref(), Some("Orders"));
    assert_eq!(target.labels.len(), 1);
    assert!(plan.changes.is_empty());
}

fn impossible_columns() -> Vec<BigQuerySchemaColumn> {
    let mut columns = base_columns();
    columns[3] = C.field("n").string();
    columns
}

#[test]
fn recreate_if_empty_recreates_an_empty_table_with_create_or_replace() {
    let mut table = raw(base_fields());
    table.num_rows = Some(0);
    let plan = plan_against(
        &declare(impossible_columns(), |d| {
            d.recreate = Some(BigQueryRecreatePolicy::IfEmpty)
        }),
        table,
    );
    let recreate = plan.recreate.as_ref().expect("a recreate");
    assert_eq!(recreate.method, BigQueryRecreateMethod::CreateOrReplace);
    assert!(!recreate.dangerous);
    assert_eq!(recreate.target.columns[3].field_type, STRING);
    assert!(plan.changes.is_empty() && plan.refusal.is_none(), "{plan}");
}

#[test]
fn recreate_if_empty_refuses_a_table_with_rows() {
    let plan = plan_against(
        &declare(impossible_columns(), |d| {
            d.recreate = Some(BigQueryRecreatePolicy::IfEmpty)
        }),
        raw(base_fields()),
    );
    assert_eq!(
        plan.refusal,
        Some(BigQueryRefusal::NotEmpty { num_rows: Some(3) }),
        "{plan}"
    );
    assert!(plan.recreate.is_none());
}

#[test]
fn dangerous_recreate_with_a_partitioning_change_drops_and_creates() {
    let mut table = raw(base_fields());
    partitioned(&mut table);
    let month = BigQueryPartitioning::Time {
        unit: BigQueryPartitionUnit::Month,
        column: Some("ts".into()),
    };
    let plan = plan_against(
        &declare(with(base_columns(), C.field("ts").timestamp()), |d| {
            d.partitioning = Some(month.clone());
            d.recreate = Some(BigQueryRecreatePolicy::DangerouslyWithDataLoss);
        }),
        table,
    );
    let recreate = plan.recreate.as_ref().expect("a recreate");
    assert_eq!(
        recreate.method,
        BigQueryRecreateMethod::DropAndCreate,
        "1A2"
    );
    assert!(recreate.dangerous);
    assert_eq!(recreate.num_rows, Some(3));
    assert_eq!(recreate.target.partitioning, Some(month));
}

#[test]
fn a_recreate_keeps_undeclared_columns_and_settings_unless_pruned() {
    let mut table = raw(base_fields());
    table.labels = HashMap::from([("old".to_string(), "x".to_string())]);
    table.description = Some("kept".into());
    let mut columns = impossible_columns();
    columns.remove(4);
    let dangerous = |prune: bool| {
        declare(columns.clone(), move |d| {
            d.prune = prune;
            d.recreate = Some(BigQueryRecreatePolicy::DangerouslyWithDataLoss);
        })
    };
    let kept = plan_against(&dangerous(false), table.clone());
    let target = &kept.recreate.as_ref().expect("a recreate").target;
    assert_eq!(target.columns.last().map(|c| c.name.as_str()), Some("s"));
    assert_eq!(target.labels.get("old").map(String::as_str), Some("x"));
    assert_eq!(target.description.as_deref(), Some("kept"));

    let pruned = plan_against(&dangerous(true), table);
    let target = &pruned.recreate.as_ref().expect("a recreate").target;
    assert_eq!(target.columns.len(), 4);
    assert!(target.labels.is_empty());
}
