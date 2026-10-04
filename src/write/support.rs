//! [`BigQueryWriteSupport`] for [`BigQueryDb`]: one writer per insert.

use crate::write::connection::{FinishKind, Finished};
use crate::write::writer::WriterCore;
use crate::{
    BigQueryChange, BigQueryDb, BigQueryInsertParams, BigQueryResult, BigQueryWriteSummary,
    BigQueryWriteSupport,
};
use async_trait::async_trait;
use serde::Serialize;

/// The summary of a finished insert, or the first failed batch's error.
fn outcome(finished: Finished) -> BigQueryResult<BigQueryWriteSummary> {
    match finished.first_error {
        Some(err) => Err(err),
        None => Ok(finished.summary),
    }
}

#[async_trait]
impl BigQueryWriteSupport for BigQueryDb {
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
        let (mut core, _responses) =
            WriterCore::open(self, params.table, params.options, false).await?;
        for row in rows {
            if let Err(err) = core
                .write_with(|encoder, out| encoder.encode(&row, out))
                .await
            {
                core.abandon();
                return Err(err);
            }
        }
        outcome(core.finish(FinishKind::Close).await?)
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
        let (mut core, _responses) =
            WriterCore::open(self, params.table, params.options, true).await?;
        for change in changes {
            let written = core
                .write_with(|encoder, out| {
                    encoder.encode_change(
                        &change.row,
                        change.change_type,
                        change.sequence_number.as_ref(),
                        out,
                    )
                })
                .await;
            if let Err(err) = written {
                core.abandon();
                return Err(err);
            }
        }
        outcome(core.finish(FinishKind::Close).await?)
    }
}
