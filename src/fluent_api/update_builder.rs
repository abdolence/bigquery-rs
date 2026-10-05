use crate::fluent_api::row_changes::BigQueryRowChanges;
use crate::{
    BigQueryChangeSequenceNumber, BigQueryChangeType, BigQueryInsertParams, BigQueryResult,
    BigQueryStreamingWriteOptions, BigQueryTableRef, BigQueryWriteSummary, BigQueryWriteSupport,
};
use serde::Serialize;

/// The first stage of an update, from
/// [`BigQueryExprBuilder::update`](crate::BigQueryExprBuilder::update).
#[derive(Clone, Debug)]
pub struct BigQueryUpdateInitialBuilder<'a, D>
where
    D: BigQueryWriteSupport,
{
    db: &'a D,
}

impl<'a, D> BigQueryUpdateInitialBuilder<'a, D>
where
    D: BigQueryWriteSupport,
{
    pub(crate) fn new(db: &'a D) -> Self {
        Self { db }
    }

    /// The table to update. It needs a primary key.
    #[inline]
    pub fn in_table(self, table: impl Into<BigQueryTableRef>) -> BigQueryUpdateTableBuilder<'a, D> {
        BigQueryUpdateTableBuilder {
            db: self.db,
            params: BigQueryInsertParams::new(table.into()),
        }
    }
}

/// An update with its table; continue with the rows to write.
#[derive(Clone, Debug)]
pub struct BigQueryUpdateTableBuilder<'a, D>
where
    D: BigQueryWriteSupport,
{
    db: &'a D,
    params: BigQueryInsertParams,
}

impl<'a, D> BigQueryUpdateTableBuilder<'a, D>
where
    D: BigQueryWriteSupport,
{
    /// Writes one row: it replaces the row with its primary key, or is inserted if there is
    /// none.
    #[inline]
    pub fn object<'o, T: Serialize + Sync>(
        self,
        row: &'o T,
    ) -> BigQueryUpdateObjBuilder<'a, D, std::iter::Once<&'o T>> {
        self.objects(std::iter::once(row))
    }

    /// Writes every row of `rows`, in order, each as [`object`](Self::object) does. Two rows
    /// with one key apply in this order.
    #[inline]
    pub fn objects<I>(self, rows: I) -> BigQueryUpdateObjBuilder<'a, D, I>
    where
        I: IntoIterator + Send,
        I::Item: Serialize + Send + Sync,
        I::IntoIter: Send,
    {
        BigQueryUpdateObjBuilder {
            db: self.db,
            changes: BigQueryRowChanges::new(self.params, BigQueryChangeType::Upsert, rows),
        }
    }
}

/// An update of whole rows by primary key, written as CDC upserts through the table's default
/// stream.
#[derive(Clone, Debug)]
pub struct BigQueryUpdateObjBuilder<'a, D, I>
where
    D: BigQueryWriteSupport,
{
    db: &'a D,
    changes: BigQueryRowChanges<I>,
}

impl<'a, 'o, D, T> BigQueryUpdateObjBuilder<'a, D, std::iter::Once<&'o T>>
where
    D: BigQueryWriteSupport,
{
    /// Orders this update against other changes to the same key: the highest sequence number
    /// wins, whenever it arrives. Once a key has changes with sequence numbers, every later
    /// change to it needs one. For many rows, each with its own number, use
    /// [`changes`](crate::BigQueryInsertTableBuilder::changes) on an insert.
    #[inline]
    pub fn sequence_number(
        mut self,
        sequence_number: impl Into<BigQueryChangeSequenceNumber>,
    ) -> Self {
        self.changes.sequence_number = Some(sequence_number.into());
        self
    }
}

impl<'a, D, I> BigQueryUpdateObjBuilder<'a, D, I>
where
    D: BigQueryWriteSupport,
    I: IntoIterator + Send,
    I::Item: Serialize + Send + Sync,
    I::IntoIter: Send,
{
    /// Replaces the writer options. The mode must stay
    /// [`Default`](crate::BigQueryWriteMode::Default).
    #[inline]
    pub fn options(mut self, options: BigQueryStreamingWriteOptions) -> Self {
        self.changes.params.options = options;
        self
    }

    /// Opens a CDC writer, writes every row as an upsert and finishes it. BigQuery applies the
    /// changes after they are written, as the table's `max_staleness` allows.
    ///
    /// # Errors
    /// The first failed batch's error, a row that does not serialize, or
    /// [`BigQueryError::InvalidParametersError`](crate::errors::BigQueryError::InvalidParametersError)
    /// for a mode other than the default.
    pub async fn execute(self) -> BigQueryResult<BigQueryWriteSummary> {
        self.changes.execute(self.db).await
    }
}
