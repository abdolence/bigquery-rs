use crate::errors::BigQueryError;
use crate::{
    BigQueryChange, BigQueryChangeType, BigQueryInsertParams, BigQueryResult,
    BigQueryStreamingWriteOptions, BigQueryTableRef, BigQueryWriteMode, BigQueryWriteSummary,
    BigQueryWriteSupport,
};
use serde::Serialize;

/// The first stage of an insert, from
/// [`BigQueryExprBuilder::insert`](crate::BigQueryExprBuilder::insert).
#[derive(Clone, Debug)]
pub struct BigQueryInsertInitialBuilder<'a, D>
where
    D: BigQueryWriteSupport,
{
    db: &'a D,
}

impl<'a, D> BigQueryInsertInitialBuilder<'a, D>
where
    D: BigQueryWriteSupport,
{
    pub(crate) fn new(db: &'a D) -> Self {
        Self { db }
    }

    /// The table to write.
    #[inline]
    pub fn into(self, table: impl Into<BigQueryTableRef>) -> BigQueryInsertTableBuilder<'a, D> {
        BigQueryInsertTableBuilder {
            db: self.db,
            params: BigQueryInsertParams::new(table.into()),
        }
    }
}

/// An insert with its table; continue with the rows or the changes to write.
#[derive(Clone, Debug)]
pub struct BigQueryInsertTableBuilder<'a, D>
where
    D: BigQueryWriteSupport,
{
    db: &'a D,
    params: BigQueryInsertParams,
}

impl<'a, D> BigQueryInsertTableBuilder<'a, D>
where
    D: BigQueryWriteSupport,
{
    /// Writes one row.
    #[inline]
    pub fn object<'o, T: Serialize + Sync>(
        self,
        row: &'o T,
    ) -> BigQueryInsertObjBuilder<'a, D, std::iter::Once<&'o T>> {
        self.objects(std::iter::once(row))
    }

    /// Writes every row of `rows`, in order.
    #[inline]
    pub fn objects<I>(self, rows: I) -> BigQueryInsertObjBuilder<'a, D, I>
    where
        I: IntoIterator + Send,
        I::Item: Serialize + Send + Sync,
        I::IntoIter: Send,
    {
        BigQueryInsertObjBuilder {
            db: self.db,
            params: self.params,
            rows,
            upsert: false,
        }
    }

    /// Writes CDC changes, each an upsert or a delete with an optional sequence number. The
    /// table needs a primary key.
    #[inline]
    pub fn changes<T, I>(self, changes: I) -> BigQueryInsertChangesBuilder<'a, D, I>
    where
        T: Serialize + Send + Sync,
        I: IntoIterator<Item = BigQueryChange<T>> + Send,
        I::IntoIter: Send,
    {
        BigQueryInsertChangesBuilder {
            db: self.db,
            params: self.params,
            changes,
        }
    }
}

/// An insert of rows. The default writes through the table's default stream, at least once.
#[derive(Clone, Debug)]
pub struct BigQueryInsertObjBuilder<'a, D, I>
where
    D: BigQueryWriteSupport,
{
    db: &'a D,
    params: BigQueryInsertParams,
    rows: I,
    upsert: bool,
}

impl<'a, D, I> BigQueryInsertObjBuilder<'a, D, I>
where
    D: BigQueryWriteSupport,
    I: IntoIterator + Send,
    I::Item: Serialize + Send + Sync,
    I::IntoIter: Send,
{
    /// Writes through a committed stream with offsets: each row exactly once, even when a
    /// request is sent again.
    #[inline]
    pub fn exactly_once(mut self) -> Self {
        self.params.options.mode = BigQueryWriteMode::Committed;
        self
    }

    /// Writes through a pending stream: every row becomes visible at one commit, or none
    /// does.
    #[inline]
    pub fn atomic(mut self) -> Self {
        self.params.options.mode = BigQueryWriteMode::Pending;
        self
    }

    /// Writes the rows as CDC upserts by primary key. Only the default stream takes CDC, so
    /// together with [`exactly_once`](Self::exactly_once) or [`atomic`](Self::atomic) the
    /// insert fails.
    #[inline]
    pub fn upsert(mut self) -> Self {
        self.upsert = true;
        self
    }

    /// Replaces the writer options, the mode included; call it before
    /// [`exactly_once`](Self::exactly_once) or [`atomic`](Self::atomic) to keep those.
    #[inline]
    pub fn options(mut self, options: BigQueryStreamingWriteOptions) -> Self {
        self.params.options = options;
        self
    }

    /// Opens a writer, writes every row and finishes it.
    ///
    /// Each call pays one `GetWriteStream` or `CreateWriteStream` round trip, about 300 ms,
    /// so many small inserts should share a
    /// [`BigQueryStreamingWriter`](crate::BigQueryStreamingWriter).
    ///
    /// # Errors
    /// The first failed batch's error; in pending mode nothing is committed then. A row that
    /// does not serialize stops the insert with
    /// [`BigQueryError::SerializeError`]; on the default and committed streams the batches
    /// before it may be written. [`BigQueryError::InvalidParametersError`] for
    /// [`upsert`](Self::upsert) with another mode than the default.
    pub async fn execute(self) -> BigQueryResult<BigQueryWriteSummary> {
        if !self.upsert {
            return self.db.insert_objects(self.params, self.rows).await;
        }
        if self.params.options.mode != BigQueryWriteMode::Default {
            return Err(BigQueryError::invalid_parameters(
                "upsert",
                format!(
                    "CDC upserts go through the default stream only, not {:?}",
                    self.params.options.mode
                ),
            ));
        }
        let changes = self.rows.into_iter().map(|row| BigQueryChange {
            change_type: BigQueryChangeType::Upsert,
            sequence_number: None,
            row,
        });
        self.db.insert_changes(self.params, changes).await
    }
}

/// An insert of CDC changes through the default stream.
#[derive(Clone, Debug)]
pub struct BigQueryInsertChangesBuilder<'a, D, I>
where
    D: BigQueryWriteSupport,
{
    db: &'a D,
    params: BigQueryInsertParams,
    changes: I,
}

impl<'a, D, T, I> BigQueryInsertChangesBuilder<'a, D, I>
where
    D: BigQueryWriteSupport,
    T: Serialize + Send + Sync,
    I: IntoIterator<Item = BigQueryChange<T>> + Send,
    I::IntoIter: Send,
{
    /// Replaces the writer options. The mode must stay
    /// [`Default`](BigQueryWriteMode::Default).
    #[inline]
    pub fn options(mut self, options: BigQueryStreamingWriteOptions) -> Self {
        self.params.options = options;
        self
    }

    /// Opens a CDC writer, writes every change and finishes it.
    ///
    /// # Errors
    /// The first failed batch's error, a change that does not serialize, or
    /// [`BigQueryError::InvalidParametersError`] for a mode other than the default.
    pub async fn execute(self) -> BigQueryResult<BigQueryWriteSummary> {
        self.db.insert_changes(self.params, self.changes).await
    }
}
