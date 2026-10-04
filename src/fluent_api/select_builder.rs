use crate::{
    BigQueryReadOptions, BigQueryReadParams, BigQueryReadSupport, BigQueryResult, BigQueryTableRef,
};
use arrow_array::RecordBatch;
use futures::stream::BoxStream;
use serde::de::DeserializeOwned;
use std::marker::PhantomData;

/// The first stage of a table read, from
/// [`BigQueryExprBuilder::select`](crate::BigQueryExprBuilder::select): pick the columns, or
/// name the table.
#[derive(Clone, Debug)]
pub struct BigQuerySelectInitialBuilder<'a, D>
where
    D: BigQueryReadSupport,
{
    db: &'a D,
    selected_fields: Option<Vec<String>>,
}

impl<'a, D> BigQuerySelectInitialBuilder<'a, D>
where
    D: BigQueryReadSupport,
{
    pub(crate) fn new(db: &'a D) -> Self {
        Self {
            db,
            selected_fields: None,
        }
    }

    /// Reads only these columns, sent to BigQuery as they are: a top-level column by its name,
    /// a STRUCT subfield as `rec.field`. [`paths!`](crate::paths!) builds them from a struct's
    /// fields. A name the table does not have fails the read with
    /// [`SchemaMismatchError`](crate::errors::BigQueryError::SchemaMismatchError); BigQuery
    /// also reports a column added or renamed in the last 30 seconds or so that way.
    ///
    /// Without it, a typed read selects the columns its type's fields name, and a record batch
    /// read takes every column.
    pub fn fields<I>(self, fields: I) -> Self
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        Self {
            selected_fields: Some(fields.into_iter().map(|f| f.as_ref().to_string()).collect()),
            ..self
        }
    }

    /// Names the table to read: `("ds", "t")`, `("project", "ds", "t")` or a parsed
    /// [`BigQueryTableRef`].
    pub fn from(self, table: impl Into<BigQueryTableRef>) -> BigQuerySelectBuilder<'a, D> {
        let params =
            BigQueryReadParams::new(table.into()).opt_selected_fields(self.selected_fields);
        BigQuerySelectBuilder {
            db: self.db,
            params,
        }
    }
}

/// A table read with its table named: add a filter or session settings, then pick a
/// terminal, [`obj`](Self::obj) for typed rows or [`record_batches`](Self::record_batches)
/// for raw Arrow.
#[derive(Clone, Debug)]
pub struct BigQuerySelectBuilder<'a, D>
where
    D: BigQueryReadSupport,
{
    db: &'a D,
    params: BigQueryReadParams,
}

impl<'a, D> BigQuerySelectBuilder<'a, D>
where
    D: BigQueryReadSupport,
{
    /// Reads only the rows that match a GoogleSQL condition such as `n > 10`, sent as the
    /// session's `row_restriction`. It may name any column, selected or not.
    pub fn filter(self, row_restriction: impl Into<String>) -> Self {
        Self {
            params: self.params.with_row_restriction(row_restriction.into()),
            ..self
        }
    }

    /// Reads the table as it was at this time instead of now.
    pub fn snapshot_time(self, snapshot_time: jiff::Timestamp) -> Self {
        Self {
            params: self.params.with_snapshot_time(snapshot_time),
            ..self
        }
    }

    /// Reads a random sample of about this percentage of the table, above 0 and up to 100.
    /// Another value fails the terminal with
    /// [`InvalidParametersError`](crate::errors::BigQueryError::InvalidParametersError).
    pub fn sample_percentage(self, percentage: f64) -> Self {
        Self {
            params: self.params.with_sample_percentage(percentage),
            ..self
        }
    }

    /// Sets how the read session is opened: stream counts and compression.
    pub fn options(self, options: BigQueryReadOptions) -> Self {
        Self {
            params: self.params.with_options(options),
            ..self
        }
    }

    /// Reads the rows as `T`, with serde.
    pub fn obj<T>(self) -> BigQuerySelectObjBuilder<'a, D, T>
    where
        T: DeserializeOwned + Send + 'static,
    {
        BigQuerySelectObjBuilder {
            db: self.db,
            params: self.params,
            _target: PhantomData,
        }
    }

    /// Streams the record batches as BigQuery sent them, after IPC decode and decompression.
    /// A read stream that fails for good is one `Err` item, and then the stream ends.
    pub async fn record_batches<'b>(
        self,
    ) -> BigQueryResult<BoxStream<'b, BigQueryResult<RecordBatch>>> {
        self.db.stream_read_record_batches(self.params).await
    }
}

/// A typed table read: pick a terminal.
///
/// Rows are decoded on each read stream's task, so `T` is `Send + 'static` and cannot borrow
/// from the batch. Rows from different streams arrive in no particular order.
#[derive(Clone, Debug)]
pub struct BigQuerySelectObjBuilder<'a, D, T>
where
    D: BigQueryReadSupport,
{
    db: &'a D,
    params: BigQueryReadParams,
    _target: PhantomData<fn() -> T>,
}

impl<'a, D, T> BigQuerySelectObjBuilder<'a, D, T>
where
    D: BigQueryReadSupport,
    T: DeserializeOwned + Send + 'static,
{
    /// Reads every row into a `Vec`, failing on the first row or stream error.
    pub async fn query(self) -> BigQueryResult<Vec<T>> {
        self.db.read_obj(self.params).await
    }

    /// Streams the rows. A row that fails to decode is logged at `error!` and skipped. A read
    /// stream that fails for good is logged too and ends the stream, so a stream that ends
    /// does not mean every row was read; use
    /// [`stream_query_with_errors`](Self::stream_query_with_errors) to tell the two apart.
    pub async fn stream_query<'b>(self) -> BigQueryResult<BoxStream<'b, T>> {
        self.db.stream_read_obj(self.params).await
    }

    /// Streams the rows, yielding every failure as an `Err` item. A row that fails to decode
    /// is one `Err(DeserializeError)` and the stream goes on; a read stream that fails for good
    /// is one `Err`, and then the stream ends.
    pub async fn stream_query_with_errors<'b>(
        self,
    ) -> BigQueryResult<BoxStream<'b, BigQueryResult<T>>> {
        self.db.stream_read_obj_with_errors(self.params).await
    }
}
