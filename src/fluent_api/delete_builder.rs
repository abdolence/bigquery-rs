use crate::fluent_api::row_changes::BigQueryRowChanges;
use crate::{
    BigQueryChangeSequenceNumber, BigQueryChangeType, BigQueryInsertParams, BigQueryResult,
    BigQueryStreamingWriteOptions, BigQueryTableRef, BigQueryWriteSummary, BigQueryWriteSupport,
};
use serde::Serialize;

/// The first stage of a delete, from
/// [`BigQueryExprBuilder::delete`](crate::BigQueryExprBuilder::delete).
#[derive(Clone, Debug)]
pub struct BigQueryDeleteInitialBuilder<'a, D>
where
    D: BigQueryWriteSupport,
{
    db: &'a D,
}

impl<'a, D> BigQueryDeleteInitialBuilder<'a, D>
where
    D: BigQueryWriteSupport,
{
    pub(crate) fn new(db: &'a D) -> Self {
        Self { db }
    }

    /// The table to delete from. It needs a primary key.
    #[inline]
    pub fn from(self, table: impl Into<BigQueryTableRef>) -> BigQueryDeleteTableBuilder<'a, D> {
        BigQueryDeleteTableBuilder {
            db: self.db,
            params: BigQueryInsertParams::new(table.into()),
        }
    }
}

/// A delete with its table; continue with the keys of the rows to delete.
#[derive(Clone, Debug)]
pub struct BigQueryDeleteTableBuilder<'a, D>
where
    D: BigQueryWriteSupport,
{
    db: &'a D,
    params: BigQueryInsertParams,
}

impl<'a, D> BigQueryDeleteTableBuilder<'a, D>
where
    D: BigQueryWriteSupport,
{
    /// Deletes the row with `key`'s primary key. Only the key columns matter, so `key` can be
    /// a struct of just those fields, as long as every column it leaves out is `NULLABLE`: a
    /// row without a `REQUIRED` column does not serialize.
    #[inline]
    pub fn object<'o, T: Serialize + Sync>(
        self,
        key: &'o T,
    ) -> BigQueryDeleteObjBuilder<'a, D, std::iter::Once<&'o T>> {
        self.objects(std::iter::once(key))
    }

    /// Deletes the row of every key in `keys`, each as [`object`](Self::object) does.
    #[inline]
    pub fn objects<I>(self, keys: I) -> BigQueryDeleteObjBuilder<'a, D, I>
    where
        I: IntoIterator + Send,
        I::Item: Serialize + Send + Sync,
        I::IntoIter: Send,
    {
        BigQueryDeleteObjBuilder {
            db: self.db,
            changes: BigQueryRowChanges::new(self.params, BigQueryChangeType::Delete, keys),
        }
    }
}

/// A delete of rows by primary key, written as CDC deletes through the table's default stream.
#[derive(Clone, Debug)]
pub struct BigQueryDeleteObjBuilder<'a, D, I>
where
    D: BigQueryWriteSupport,
{
    db: &'a D,
    changes: BigQueryRowChanges<I>,
}

impl<'a, 'o, D, T> BigQueryDeleteObjBuilder<'a, D, std::iter::Once<&'o T>>
where
    D: BigQueryWriteSupport,
{
    /// Orders this delete against other changes to the same key, as
    /// [`BigQueryUpdateObjBuilder::sequence_number`](crate::BigQueryUpdateObjBuilder::sequence_number)
    /// does for an update.
    #[inline]
    pub fn sequence_number(
        mut self,
        sequence_number: impl Into<BigQueryChangeSequenceNumber>,
    ) -> Self {
        self.changes.sequence_number = Some(sequence_number.into());
        self
    }
}

impl<'a, D, I> BigQueryDeleteObjBuilder<'a, D, I>
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

    /// Opens a CDC writer, writes every key as a delete and finishes it. BigQuery applies the
    /// deletes after they are written, as the table's `max_staleness` allows.
    ///
    /// # Errors
    /// The first failed batch's error, a key that does not serialize, or
    /// [`BigQueryError::InvalidParametersError`](crate::errors::BigQueryError::InvalidParametersError)
    /// for a mode other than the default.
    pub async fn execute(self) -> BigQueryResult<BigQueryWriteSummary> {
        self.changes.execute(self.db).await
    }
}
