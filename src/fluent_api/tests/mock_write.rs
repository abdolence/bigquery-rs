//! `BigQueryWriteSupport` for `MockDatabase`: records each insert for the builder tests.

use crate::fluent_api::tests::mockdb::MockDatabase;
use crate::fluent_api::BigQueryExprBuilder;
use crate::{
    BigQueryChange, BigQueryChangeSequenceNumber, BigQueryChangeType, BigQueryDatasetId,
    BigQueryDatasetRef, BigQueryInsertParams, BigQueryResult, BigQueryStreamingWriteOptions,
    BigQueryTableId, BigQueryWriteMode, BigQueryWriteSummary, BigQueryWriteSupport,
};
use async_trait::async_trait;
use serde::Serialize;
use std::cell::RefCell;

const DS: BigQueryDatasetId = BigQueryDatasetId::from_static("ds");
const T: BigQueryTableId = BigQueryTableId::from_static("t");

/// One insert as the mock saw it: rows as JSON, and for CDC each change's type and sequence.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RecordedInsert {
    pub(crate) params: BigQueryInsertParams,
    pub(crate) rows: Vec<serde_json::Value>,
    pub(crate) changes: Option<Vec<(BigQueryChangeType, Option<String>)>>,
}

thread_local! {
    static INSERTS: RefCell<Vec<RecordedInsert>> = const { RefCell::new(Vec::new()) };
}

/// Every insert recorded on this thread since the last call.
pub(crate) fn take_inserts() -> Vec<RecordedInsert> {
    INSERTS.with(|inserts| inserts.take())
}

fn json<T: Serialize>(row: &T) -> serde_json::Value {
    serde_json::to_value(row).expect("a test row serializes to JSON")
}

fn summary(rows: usize) -> BigQueryWriteSummary {
    BigQueryWriteSummary {
        rows_written: rows as u64,
        rows_failed: 0,
        batches: 1,
        bytes_sent: 0,
        stream: None,
        commit_time: None,
    }
}

#[async_trait]
impl BigQueryWriteSupport for MockDatabase {
    async fn insert_objects<T, I>(
        &self,
        params: BigQueryInsertParams,
        rows: I,
    ) -> BigQueryResult<BigQueryWriteSummary>
    where
        T: Serialize + Send + Sync,
        I: IntoIterator<Item = T> + Send,
        I::IntoIter: Send,
    {
        let rows: Vec<serde_json::Value> = rows.into_iter().map(|row| json(&row)).collect();
        let count = rows.len();
        INSERTS.with(|inserts| {
            inserts.borrow_mut().push(RecordedInsert {
                params,
                rows,
                changes: None,
            })
        });
        Ok(summary(count))
    }

    async fn insert_changes<T, I>(
        &self,
        params: BigQueryInsertParams,
        changes: I,
    ) -> BigQueryResult<BigQueryWriteSummary>
    where
        T: Serialize + Send + Sync,
        I: IntoIterator<Item = BigQueryChange<T>> + Send,
        I::IntoIter: Send,
    {
        let (rows, kinds): (Vec<_>, Vec<_>) = changes
            .into_iter()
            .map(|change| {
                (
                    json(&change.row),
                    (
                        change.change_type,
                        change.sequence_number.map(|s| s.as_str().to_string()),
                    ),
                )
            })
            .unzip();
        let count = rows.len();
        INSERTS.with(|inserts| {
            inserts.borrow_mut().push(RecordedInsert {
                params,
                rows,
                changes: Some(kinds),
            })
        });
        Ok(summary(count))
    }
}

#[derive(Serialize)]
struct Row {
    id: i64,
}

#[tokio::test]
async fn insert_chain_passes_mode_and_rows() {
    let db = MockDatabase;
    let rows = [Row { id: 1 }, Row { id: 2 }];
    let ids = |insert: &RecordedInsert| -> Vec<i64> {
        insert
            .rows
            .iter()
            .filter_map(|row| row["id"].as_i64())
            .collect()
    };
    take_inserts();

    let summary = BigQueryExprBuilder::new(&db)
        .insert()
        .into(DS.table(T))
        .objects(&rows)
        .execute()
        .await
        .expect("the insert runs");
    assert_eq!(summary.rows_written, 2);
    BigQueryExprBuilder::new(&db)
        .insert()
        .into(
            BigQueryDatasetRef::new("p", DS)
                .expect("valid test input")
                .table(T),
        )
        .object(&rows[0])
        .exactly_once()
        .execute()
        .await
        .expect("the insert runs");
    BigQueryExprBuilder::new(&db)
        .insert()
        .into(DS.table(T))
        .objects(rows.iter())
        .options(BigQueryStreamingWriteOptions::new().with_max_batch_rows(7))
        .atomic()
        .execute()
        .await
        .expect("the insert runs");
    BigQueryExprBuilder::new(&db)
        .insert()
        .into(DS.table(T))
        .objects(&rows)
        .upsert()
        .execute()
        .await
        .expect("the insert runs");
    BigQueryExprBuilder::new(&db)
        .insert()
        .into(DS.table(T))
        .changes(vec![BigQueryChange {
            change_type: BigQueryChangeType::Delete,
            sequence_number: Some(BigQueryChangeSequenceNumber::from(10)),
            row: Row { id: 2 },
        }])
        .execute()
        .await
        .expect("the insert runs");

    let inserts = take_inserts();
    assert_eq!(inserts.len(), 5);
    let modes: Vec<BigQueryWriteMode> = inserts.iter().map(|i| i.params.options.mode).collect();
    assert_eq!(
        modes,
        [
            BigQueryWriteMode::Default,
            BigQueryWriteMode::Committed,
            BigQueryWriteMode::Pending,
            BigQueryWriteMode::Default,
            BigQueryWriteMode::Default
        ]
    );
    assert_eq!(inserts[0].params.table, DS.table(T));
    assert_eq!(
        inserts[1].params.table,
        BigQueryDatasetRef::new("p", DS)
            .expect("valid test input")
            .table(T)
    );
    assert_eq!(inserts[2].params.options.max_batch_rows, Some(7));
    assert_eq!(
        inserts.iter().map(ids).collect::<Vec<_>>(),
        [vec![1, 2], vec![1], vec![1, 2], vec![1, 2], vec![2]]
    );
    assert_eq!(inserts[0].changes, None);
    assert_eq!(
        inserts[3].changes,
        Some(vec![
            (BigQueryChangeType::Upsert, None),
            (BigQueryChangeType::Upsert, None)
        ])
    );
    assert_eq!(
        inserts[4].changes,
        Some(vec![(BigQueryChangeType::Delete, Some("A".to_string()))])
    );

    let refused = BigQueryExprBuilder::new(&db)
        .insert()
        .into(DS.table(T))
        .objects(&rows)
        .upsert()
        .exactly_once()
        .execute()
        .await;
    assert!(
        matches!(
            refused,
            Err(crate::errors::BigQueryError::InvalidParametersError(_))
        ),
        "{refused:?}"
    );
    assert!(take_inserts().is_empty());
}
