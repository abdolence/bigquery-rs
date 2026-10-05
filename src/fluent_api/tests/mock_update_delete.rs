//! The update and delete builders, through the inserts `mock_write` records.

use crate::db::fake::{ORDERS, SHOP};
use crate::fluent_api::tests::mock_write::{take_inserts, RecordedInsert};
use crate::fluent_api::tests::mockdb::MockDatabase;
use crate::fluent_api::BigQueryExprBuilder;
use crate::{
    BigQueryChangeSequenceNumber, BigQueryChangeType, BigQueryStreamingWriteOptions,
    BigQueryWriteMode,
};
use serde::Serialize;
use serde_json::json;

#[derive(Serialize)]
struct Order {
    id: i64,
    status: String,
}

impl Order {
    fn new(id: i64, status: &str) -> Self {
        Self {
            id,
            status: status.to_string(),
        }
    }
}

#[derive(Serialize)]
struct OrderKey {
    id: i64,
}

fn the_only_insert() -> RecordedInsert {
    let mut inserts = take_inserts();
    assert_eq!(inserts.len(), 1, "one write per execute");
    inserts.remove(0)
}

#[tokio::test]
async fn update_writes_every_row_as_an_upsert_in_order() {
    let db = MockDatabase;
    let orders = [Order::new(1, "placed"), Order::new(1, "shipped")];
    take_inserts();

    let summary = BigQueryExprBuilder::new(&db)
        .update()
        .in_table(SHOP.table(ORDERS))
        .objects(&orders)
        .options(BigQueryStreamingWriteOptions::new().with_max_batch_rows(7))
        .execute()
        .await
        .expect("the update runs");

    assert_eq!(summary.rows_written, 2);
    let insert = the_only_insert();
    assert_eq!(insert.params.table, SHOP.table(ORDERS));
    assert_eq!(insert.params.options.mode, BigQueryWriteMode::Default);
    assert_eq!(insert.params.options.max_batch_rows, Some(7));
    assert_eq!(
        insert.rows,
        [
            json!({"id": 1, "status": "placed"}),
            json!({"id": 1, "status": "shipped"})
        ]
    );
    assert_eq!(
        insert.changes,
        Some(vec![
            (BigQueryChangeType::Upsert, None),
            (BigQueryChangeType::Upsert, None)
        ])
    );
}

#[tokio::test]
async fn update_of_one_row_carries_its_sequence_number() {
    let db = MockDatabase;
    take_inserts();

    BigQueryExprBuilder::new(&db)
        .update()
        .in_table(SHOP.table(ORDERS))
        .object(&Order::new(42, "shipped"))
        .sequence_number(BigQueryChangeSequenceNumber::from(31))
        .execute()
        .await
        .expect("the update runs");

    assert_eq!(
        the_only_insert().changes,
        Some(vec![(BigQueryChangeType::Upsert, Some("1F".to_string()))])
    );
}

#[tokio::test]
async fn delete_writes_only_the_key_as_a_delete() {
    let db = MockDatabase;
    take_inserts();

    BigQueryExprBuilder::new(&db)
        .delete()
        .from(SHOP.table(ORDERS))
        .object(&OrderKey { id: 42 })
        .sequence_number(32)
        .execute()
        .await
        .expect("the delete runs");
    BigQueryExprBuilder::new(&db)
        .delete()
        .from(SHOP.table(ORDERS))
        .objects([OrderKey { id: 1 }, OrderKey { id: 2 }])
        .execute()
        .await
        .expect("the delete runs");

    let inserts = take_inserts();
    assert_eq!(
        inserts
            .iter()
            .map(|insert| &insert.rows)
            .collect::<Vec<_>>(),
        [
            &vec![json!({"id": 42})],
            &vec![json!({"id": 1}), json!({"id": 2})]
        ]
    );
    assert_eq!(
        inserts
            .into_iter()
            .map(|insert| insert.changes)
            .collect::<Vec<_>>(),
        [
            Some(vec![(BigQueryChangeType::Delete, Some("20".to_string()))]),
            Some(vec![
                (BigQueryChangeType::Delete, None),
                (BigQueryChangeType::Delete, None)
            ])
        ]
    );
}
