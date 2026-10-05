use crate::{
    BigQueryChange, BigQueryChangeSequenceNumber, BigQueryChangeType, BigQueryInsertParams,
    BigQueryResult, BigQueryWriteSummary, BigQueryWriteSupport,
};
use serde::Serialize;

/// Rows that all become CDC changes of one type, as the update and delete builders write them.
#[derive(Clone, Debug)]
pub(crate) struct BigQueryRowChanges<I> {
    pub(crate) params: BigQueryInsertParams,
    pub(crate) change_type: BigQueryChangeType,
    /// Given to every row; the builders set it only when there is a single row, since rows
    /// with one key and one sequence number have no defined order.
    pub(crate) sequence_number: Option<BigQueryChangeSequenceNumber>,
    pub(crate) rows: I,
}

impl<I> BigQueryRowChanges<I>
where
    I: IntoIterator + Send,
    I::Item: Serialize + Send + Sync,
    I::IntoIter: Send,
{
    pub(crate) fn new(
        params: BigQueryInsertParams,
        change_type: BigQueryChangeType,
        rows: I,
    ) -> Self {
        Self {
            params,
            change_type,
            sequence_number: None,
            rows,
        }
    }

    pub(crate) async fn execute<D: BigQueryWriteSupport>(
        self,
        db: &D,
    ) -> BigQueryResult<BigQueryWriteSummary> {
        let changes = self.rows.into_iter().map(|row| BigQueryChange {
            change_type: self.change_type,
            sequence_number: self.sequence_number.clone(),
            row,
        });
        db.insert_changes(self.params, changes).await
    }
}
