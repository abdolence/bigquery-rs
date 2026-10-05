use crate::errors::BigQueryError;
use crate::fluent_api::key_row::BigQueryKeyRow;
use crate::fluent_api::row_changes::BigQueryRowChanges;
use crate::{
    BigQueryChangeSequenceNumber, BigQueryChangeType, BigQueryInsertParams, BigQueryResult,
    BigQueryStreamingWriteOptions, BigQueryTableRef, BigQueryTableSupport, BigQueryWriteSummary,
    BigQueryWriteSupport,
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

/// A delete with its table; continue with the keys of the rows to delete, as plain values or
/// as rows.
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

    /// Deletes the row with the primary key `key`: a plain value such as `42` for a key of one
    /// column, or a tuple such as `(42, "line-1")` with one value per key column, in the
    /// key's column order. The key's columns come from the table's metadata, read once at
    /// [`execute`](BigQueryDeleteKeysBuilder::execute).
    #[inline]
    pub fn key<K: Serialize + Send + Sync>(
        self,
        key: K,
    ) -> BigQueryDeleteKeysBuilder<'a, D, std::iter::Once<K>> {
        self.keys(std::iter::once(key))
    }

    /// Deletes the row of every primary key in `keys`, each as [`key`](Self::key) does.
    #[inline]
    pub fn keys<I>(self, keys: I) -> BigQueryDeleteKeysBuilder<'a, D, I>
    where
        I: IntoIterator + Send,
        I::Item: Serialize + Send + Sync,
        I::IntoIter: Send,
    {
        BigQueryDeleteKeysBuilder {
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

impl<'a, D, I> BigQueryDeleteObjBuilder<'a, D, I>
where
    D: BigQueryWriteSupport,
    I: IntoIterator + Send,
    I::Item: Serialize + Send + Sync,
    I::IntoIter: Send,
{
    /// Orders the deletes against other changes to the same keys, as
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

/// A delete of rows by primary key values, written as CDC deletes of rows that hold only the
/// key columns.
#[derive(Clone, Debug)]
pub struct BigQueryDeleteKeysBuilder<'a, D, I>
where
    D: BigQueryWriteSupport,
{
    db: &'a D,
    changes: BigQueryRowChanges<I>,
}

impl<'a, D, I> BigQueryDeleteKeysBuilder<'a, D, I>
where
    D: BigQueryWriteSupport + BigQueryTableSupport + Sync,
    I: IntoIterator + Send,
    I::Item: Serialize + Send + Sync,
    I::IntoIter: Send,
{
    /// Orders the deletes against other changes to the same keys, as
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

    /// Replaces the writer options. The mode must stay
    /// [`Default`](crate::BigQueryWriteMode::Default).
    #[inline]
    pub fn options(mut self, options: BigQueryStreamingWriteOptions) -> Self {
        self.changes.params.options = options;
        self
    }

    /// Reads the table's primary key columns with one `GetTable`, then opens a CDC writer,
    /// writes every key as a delete and finishes it.
    ///
    /// # Errors
    /// [`BigQueryError::InvalidParametersError`] before any write, for the field `table` when
    /// the table has no primary key, and for the field `key` when a key does not have one
    /// value per key column. Otherwise the failure to read the table, or as
    /// [`BigQueryDeleteObjBuilder::execute`].
    pub async fn execute(self) -> BigQueryResult<BigQueryWriteSummary> {
        let BigQueryRowChanges {
            params,
            change_type,
            sequence_number,
            rows: keys,
        } = self.changes;
        let columns = self.db.primary_key_columns(&params.table).await?;
        if columns.is_empty() {
            return Err(BigQueryError::invalid_parameters(
                "table",
                format!("{} has no primary key to delete by", params.table),
            ));
        }
        let rows = keys
            .into_iter()
            .map(|key| BigQueryKeyRow::new(&columns, key))
            .collect::<BigQueryResult<Vec<_>>>()?;
        BigQueryRowChanges {
            params,
            change_type,
            sequence_number,
            rows,
        }
        .execute(self.db)
        .await
    }
}
