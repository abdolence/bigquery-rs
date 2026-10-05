//! Each value a statement carries stays one token: a statement rendered with a hostile value,
//! once its quoted tokens are decoded, is the statement rendered with a plain one.

use super::*;
use crate::sql::tests::{injection_corpus, pieces, Piece};
use crate::types::testkit::field;
use crate::BigQueryLabels;
use crate::{
    BigQueryDatasetId, BigQueryDatasetRef, BigQueryDecimalParams, BigQueryFieldMode,
    BigQueryFieldSchema, BigQueryPartitionUnit, BigQueryPartitioning, BigQueryTableId,
};

/// Fails unless `hostile` is `plain` with each quoted token holding `plain_value` holding
/// `hostile_value` instead, decoded: the hostile value changed no other token.
fn assert_same_statement(hostile: &str, plain: &str, hostile_value: &str, plain_value: &str) {
    let swapped: Vec<Piece> = pieces(hostile)
        .into_iter()
        .map(|piece| match piece {
            Piece::String(s) if s == hostile_value => Piece::String(plain_value.into()),
            Piece::Identifier(s) if s == hostile_value => Piece::Identifier(plain_value.into()),
            other => other,
        })
        .collect();
    assert_eq!(swapped, pieces(plain), "{hostile_value:.80?}");
}

fn target(columns: Vec<BigQueryFieldSchema>) -> BigQueryTableTarget {
    BigQueryTableTarget {
        columns,
        primary_key: None,
        partitioning: None,
        partition_expiration: None,
        clustering: Vec::new(),
        description: None,
        labels: BigQueryLabels::new(),
        expiration: None,
    }
}

const STRING: BigQueryFieldType = BigQueryFieldType::String { max_length: None };

fn hostile_target(value: &str) -> BigQueryTableTarget {
    let mut id = field("id", BigQueryFieldType::Int64, BigQueryFieldMode::Required);
    id.description = Some(value.to_string());
    let mut target = target(vec![id]);
    target.description = Some(value.to_string());
    target.labels = BigQueryLabels::from([(value, value)]);
    target
}

fn orders() -> BigQueryTableRef {
    BigQueryDatasetRef::new("acme-prod", BigQueryDatasetId::from_static("shop"))
        .expect("a project")
        .table(BigQueryTableId::from_static("orders"))
}

#[test]
fn table_path_quotes_each_part_whatever_the_project_holds() {
    let accepted: Vec<(String, BigQueryTableRef)> = injection_corpus()
        .into_iter()
        .filter_map(|p| {
            let dataset =
                BigQueryDatasetRef::new(p.clone(), BigQueryDatasetId::from_static("shop"));
            Some((
                p,
                dataset.ok()?.table(BigQueryTableId::from_static("orders")),
            ))
        })
        .collect();
    assert!(
        accepted.iter().any(|(p, _)| p == "`backtick`"),
        "a backtick project must reach the renderer, or this test checks nothing"
    );
    for (project, table) in accepted {
        let sql = table.ddl("unused").to_string();
        assert_eq!(
            pieces(&sql),
            [
                Piece::Identifier(project.clone()),
                Piece::Text(".".into()),
                Piece::Identifier("shop".into()),
                Piece::Text(".".into()),
                Piece::Identifier("orders".into()),
            ],
            "{project:.80?}"
        );
    }
    let unset =
        BigQueryDatasetId::from_static("shop").table(BigQueryTableId::from_static("orders"));
    assert_eq!(
        unset.ddl("client-project").to_string(),
        "`client-project`.`shop`.`orders`"
    );
}

#[test]
fn hostile_descriptions_and_labels_render_as_single_literals_that_parse_back() {
    let table = orders().ddl("acme-prod");
    let plain = table.create(&hostile_target("plain"), true).expect("DDL");
    for value in injection_corpus() {
        let sql = table.create(&hostile_target(&value), true).expect("DDL");
        assert_same_statement(&sql, &plain, &value, "plain");
    }
}

#[test]
fn hostile_column_names_render_as_single_identifiers() {
    let table = orders().ddl("acme-prod");
    let create = |name: &str| {
        let target = target(vec![field(name, STRING, BigQueryFieldMode::Nullable)]);
        table.create(&target, false).expect("DDL")
    };
    let widen = |name: &str| table.widen_column(name, &BigQueryFieldType::Numeric(None));
    for name in injection_corpus().into_iter().filter(|n| !n.is_empty()) {
        assert_same_statement(&create(&name), &create("c"), &name, "c");
        let rename = table.rename_column(&name, &name);
        assert_same_statement(&rename, &table.rename_column("c", "c"), &name, "c");
        assert_same_statement(&widen(&name), &widen("c"), &name, "c");
        let drop = table.drop_column(&name);
        assert_same_statement(&drop, &table.drop_column("c"), &name, "c");
    }
}

#[test]
fn create_or_replace_restates_columns_key_partitioning_clustering_and_options() {
    let mut id = field("id", BigQueryFieldType::Int64, BigQueryFieldMode::Required);
    id.description = Some("key".into());
    let mut city = field("city", STRING, BigQueryFieldMode::Required);
    city.description = Some("c".into());
    let mut total = field(
        "total",
        BigQueryFieldType::Numeric(Some(BigQueryDecimalParams {
            precision: 10,
            scale: 2,
        })),
        BigQueryFieldMode::Nullable,
    );
    total.default_value_expression = Some("0".into());
    let mut target = target(vec![
        id,
        field(
            "ts",
            BigQueryFieldType::Timestamp,
            BigQueryFieldMode::Nullable,
        ),
        field("tags", STRING, BigQueryFieldMode::Repeated),
        field(
            "addr",
            BigQueryFieldType::Struct(vec![city]),
            BigQueryFieldMode::Nullable,
        ),
        total,
    ]);
    target.primary_key = Some(vec!["id".into()]);
    target.partitioning = Some(BigQueryPartitioning::Time {
        unit: BigQueryPartitionUnit::Day,
        column: Some("ts".into()),
    });
    target.partition_expiration = Some(std::time::Duration::from_secs(86_400));
    target.clustering = vec!["id".into()];
    target.description = Some("Orders".into());
    target.labels = BigQueryLabels::from([("team", "shop")]);
    target.expiration = Some(jiff::Timestamp::from_second(1_900_000_000).expect("a timestamp"));

    let sql = orders()
        .ddl("acme-prod")
        .create(&target, true)
        .expect("DDL");
    assert_eq!(
        sql,
        "CREATE OR REPLACE TABLE `acme-prod`.`shop`.`orders` (\n  \
         `id` INT64 NOT NULL OPTIONS(description='key'),\n  \
         `ts` TIMESTAMP,\n  \
         `tags` ARRAY<STRING>,\n  \
         `addr` STRUCT<`city` STRING NOT NULL OPTIONS(description='c')>,\n  \
         `total` NUMERIC(10, 2) DEFAULT (0\n),\n  \
         PRIMARY KEY (`id`) NOT ENFORCED\n)\n\
         PARTITION BY DATE(`ts`)\n\
         CLUSTER BY `id`\n\
         OPTIONS(description='Orders', labels=[('team', 'shop')], \
         expiration_timestamp=TIMESTAMP '2030-03-17T17:46:40Z', partition_expiration_days=1.0)"
    );
}

#[test]
fn drop_and_create_is_one_script() {
    let target = target(vec![field(
        "id",
        BigQueryFieldType::Int64,
        BigQueryFieldMode::Nullable,
    )]);
    let table = orders().ddl("acme-prod");
    let sql = table.drop_and_create(&target).expect("DDL");
    assert_eq!(
        sql,
        "DROP TABLE `acme-prod`.`shop`.`orders`;\nCREATE TABLE `acme-prod`.`shop`.`orders` (\n  `id` INT64\n);"
    );
}

#[test]
fn partition_expression_follows_the_column_type() {
    let cases = [
        (
            BigQueryFieldType::Date,
            BigQueryPartitionUnit::Day,
            "PARTITION BY `c`",
        ),
        (
            BigQueryFieldType::Date,
            BigQueryPartitionUnit::Month,
            "PARTITION BY DATE_TRUNC(`c`, MONTH)",
        ),
        (
            BigQueryFieldType::Timestamp,
            BigQueryPartitionUnit::Day,
            "PARTITION BY DATE(`c`)",
        ),
        (
            BigQueryFieldType::Timestamp,
            BigQueryPartitionUnit::Hour,
            "PARTITION BY TIMESTAMP_TRUNC(`c`, HOUR)",
        ),
        (
            BigQueryFieldType::DateTime,
            BigQueryPartitionUnit::Year,
            "PARTITION BY DATETIME_TRUNC(`c`, YEAR)",
        ),
    ];
    let table = orders().ddl("acme-prod");
    for (field_type, unit, expected) in cases {
        let mut target = target(vec![field(
            "c",
            field_type.clone(),
            BigQueryFieldMode::Nullable,
        )]);
        target.partitioning = Some(BigQueryPartitioning::Time {
            unit,
            column: Some("c".into()),
        });
        let sql = table.create(&target, false).expect("DDL");
        assert!(
            sql.contains(&format!("\n{expected}")),
            "{field_type} {unit}: {sql}"
        );
    }

    let mut ingestion = target(vec![field("c", STRING, BigQueryFieldMode::Nullable)]);
    ingestion.partitioning = Some(BigQueryPartitioning::Time {
        unit: BigQueryPartitionUnit::Day,
        column: None,
    });
    let sql = table.create(&ingestion, false).expect("DDL");
    assert!(sql.contains("\nPARTITION BY _PARTITIONDATE"), "{sql}");

    let mut range = target(vec![field(
        "c",
        BigQueryFieldType::Int64,
        BigQueryFieldMode::Nullable,
    )]);
    range.partitioning = Some(BigQueryPartitioning::Range {
        column: "c".into(),
        start: -10,
        end: 100,
        interval: 5,
    });
    let sql = table.create(&range, false).expect("DDL");
    assert!(
        sql.contains("\nPARTITION BY RANGE_BUCKET(`c`, GENERATE_ARRAY(-10, 100, 5))"),
        "{sql}"
    );

    let mut unknown = target(vec![field("c", STRING, BigQueryFieldMode::Nullable)]);
    unknown.partitioning = Some(BigQueryPartitioning::Time {
        unit: BigQueryPartitionUnit::Day,
        column: Some("c".into()),
    });
    assert!(
        table.create(&unknown, false).is_err(),
        "STRING cannot partition"
    );
}

#[test]
fn alter_statements_name_the_table_and_the_column() {
    let table = orders().ddl("acme-prod");
    assert_eq!(
        table.rename_column("a", "b"),
        "ALTER TABLE `acme-prod`.`shop`.`orders` RENAME COLUMN `a` TO `b`"
    );
    assert_eq!(
        table.widen_column(
            "s",
            &BigQueryFieldType::String {
                max_length: Some(20)
            }
        ),
        "ALTER TABLE `acme-prod`.`shop`.`orders` ALTER COLUMN `s` SET DATA TYPE STRING(20)"
    );
    assert_eq!(
        table.drop_column("d"),
        "ALTER TABLE `acme-prod`.`shop`.`orders` DROP COLUMN `d`"
    );
    assert_eq!(
        BigQueryDatasetRef::new("acme-prod", BigQueryDatasetId::from_static("shop"))
            .expect("a project")
            .table(BigQueryTableId::from_static("orders_snapshot"))
            .ddl("acme-prod")
            .snapshot_of(&table),
        "CREATE SNAPSHOT TABLE `acme-prod`.`shop`.`orders_snapshot` CLONE `acme-prod`.`shop`.`orders`"
    );
}

/// `sql` with every `--`, `#` and `/* */` comment outside a quoted token removed, which is what
/// BigQuery's parser sees.
fn without_comments(sql: &str) -> String {
    let mut out = String::new();
    let mut chars = sql.chars().peekable();
    let mut quote = None;
    while let Some(c) = chars.next() {
        if let Some(q) = quote {
            out.push(c);
            if c == '\\' {
                out.extend(chars.next());
            } else if c == q {
                quote = None;
            }
            continue;
        }
        match (c, chars.peek()) {
            ('`' | '\'' | '"', _) => {
                quote = Some(c);
                out.push(c);
            }
            ('#', _) | ('-', Some('-')) => while chars.next_if(|&d| d != '\n').is_some() {},
            ('/', Some('*')) => {
                chars.next();
                while let Some(d) = chars.next() {
                    if d == '*' && chars.next_if_eq(&'/').is_some() {
                        break;
                    }
                }
            }
            _ => out.push(c),
        }
    }
    out
}

#[test]
fn a_default_is_one_parenthesized_operand() {
    let table = orders().ddl("acme-prod");
    let rendered = |expression: &str, code: &str| {
        let mut quantity = field(
            "quantity",
            BigQueryFieldType::Int64,
            BigQueryFieldMode::Nullable,
        );
        quantity.default_value_expression = Some(expression.into());
        let target = target(vec![
            quantity,
            field(
                "amount",
                BigQueryFieldType::Int64,
                BigQueryFieldMode::Nullable,
            ),
        ]);
        let sql = table.create(&target, false).expect("DDL");
        let parsed = without_comments(&sql).replacen(code, "E", 1);
        pieces(&parsed.split_whitespace().collect::<Vec<_>>().join(" "))
    };
    let plain = rendered("0", "0");
    for (expression, code) in [
        ("CURRENT_TIMESTAMP()", "CURRENT_TIMESTAMP()"),
        ("1 -- x", "1"),
        ("1 # x", "1"),
        ("1 /* x */", "1"),
    ] {
        assert_eq!(rendered(expression, code), plain, "{expression:?}");
    }
    assert!(
        plain.contains(&Piece::Text(" INT64 DEFAULT (E ), ".into())),
        "{plain:?}"
    );
}

#[test]
fn a_nested_field_name_stays_one_identifier() {
    let table = orders().ddl("acme-prod");
    let nested = |name: &str| {
        let inner = field(name, STRING, BigQueryFieldMode::Nullable);
        let outer = field(
            name,
            BigQueryFieldType::Struct(vec![inner]),
            BigQueryFieldMode::Repeated,
        );
        BigQueryFieldType::Struct(vec![outer])
    };
    let create = |name: &str| {
        let target = target(vec![field("s", nested(name), BigQueryFieldMode::Nullable)]);
        table.create(&target, false).expect("DDL")
    };
    let widen = |name: &str| table.widen_column("s", &nested(name));
    for name in injection_corpus().into_iter().filter(|n| !n.is_empty()) {
        assert_same_statement(&create(&name), &create("c"), &name, "c");
        assert_same_statement(&widen(&name), &widen("c"), &name, "c");
    }
}

#[test]
fn hostile_key_clustering_and_partitioning_columns_render_as_single_identifiers() {
    let table = orders().ddl("acme-prod");
    // `None` stands for range partitioning on an INT64 column.
    let partitionings = [
        (BigQueryFieldType::Date, Some(BigQueryPartitionUnit::Day)),
        (BigQueryFieldType::Date, Some(BigQueryPartitionUnit::Month)),
        (
            BigQueryFieldType::Timestamp,
            Some(BigQueryPartitionUnit::Day),
        ),
        (
            BigQueryFieldType::Timestamp,
            Some(BigQueryPartitionUnit::Hour),
        ),
        (
            BigQueryFieldType::DateTime,
            Some(BigQueryPartitionUnit::Day),
        ),
        (
            BigQueryFieldType::DateTime,
            Some(BigQueryPartitionUnit::Year),
        ),
        (BigQueryFieldType::Int64, None),
    ];
    for (field_type, unit) in partitionings {
        let create = |name: &str| {
            let mut target = target(vec![field(
                name,
                field_type.clone(),
                BigQueryFieldMode::Required,
            )]);
            target.primary_key = Some(vec![name.into()]);
            target.clustering = vec![name.into()];
            target.partitioning = Some(match unit {
                Some(unit) => BigQueryPartitioning::Time {
                    unit,
                    column: Some(name.into()),
                },
                None => BigQueryPartitioning::Range {
                    column: name.into(),
                    start: 0,
                    end: 10,
                    interval: 1,
                },
            });
            table.create(&target, false).expect("DDL")
        };
        let plain = create("c");
        for name in injection_corpus().into_iter().filter(|n| !n.is_empty()) {
            assert_same_statement(&create(&name), &plain, &name, "c");
        }
    }
}
