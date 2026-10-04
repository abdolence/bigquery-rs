use crate::errors::BigQueryError;
use crate::BigQueryInstant;
use crate::{
    BigQueryFilter, BigQueryFilterBuilder, BigQueryReadOptions, BigQueryReadParams,
    BigQueryReadSupport, BigQueryResult, BigQueryTableRef,
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

    /// Names the table to read, such as `SHOP.table(ORDERS)`; see [`BigQueryTableRef`].
    pub fn from(self, table: impl Into<BigQueryTableRef>) -> BigQuerySelectBuilder<'a, D> {
        let params =
            BigQueryReadParams::new(table.into()).opt_selected_fields(self.selected_fields);
        BigQuerySelectBuilder {
            db: self.db,
            params,
            filter_failure: None,
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
    /// Why the last `filter` could not be rendered, returned by the terminal.
    filter_failure: Option<BigQueryError>,
}

impl<'a, D> BigQuerySelectBuilder<'a, D>
where
    D: BigQueryReadSupport,
{
    /// Reads only the rows that match a typed condition, built by the closure from a
    /// [`BigQueryFilterBuilder`]:
    ///
    /// ```rust,no_run
    /// # use bigquery::*;
    /// # async fn example(db: &BigQueryDb, city: &str) -> BigQueryResult<()> {
    /// #[derive(serde::Deserialize)]
    /// struct Person {
    ///     name: String,
    ///     city: String,
    ///     year: i64,
    /// }
    ///
    /// let people: Vec<Person> = db
    ///     .fluent()
    ///     .select()
    ///     .from(BigQueryDatasetId::from_static("shop").table(BigQueryTableId::from_static("people")))
    ///     .filter(|f| {
    ///         f.for_all([
    ///             f.field(path!(Person::city)).eq(city),
    ///             f.field(path!(Person::year)).ge(2010),
    ///         ])
    ///     })
    ///     .obj()
    ///     .query()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Values are written into the session's `row_restriction` as escaped GoogleSQL literals
    /// and column names as quoted identifiers, so a value cannot change the condition it is
    /// in. A condition may name any column, selected or not. A closure that returns `None`
    /// reads every row. This and [`filter_sql`](Self::filter_sql) set the same restriction;
    /// the last call wins.
    ///
    /// The Storage Read API documents the restriction as "SQL text filtering statement,
    /// similar to a WHERE clause in a query", without aggregates and at most 1 MB long, a limit BigQuery enforces
    /// ([`TableReadOptions`](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#tablereadoptions)).
    /// The builder only writes comparisons, `IS [NOT] NULL`, `[NOT] IN` lists, `AND`, `OR`
    /// and `NOT`, all within that. A column path with an
    /// empty segment or a control character, or a value without a literal form fails the
    /// terminal with
    /// [`InvalidParametersError`](crate::errors::BigQueryError::InvalidParametersError) or
    /// [`SerializeError`](crate::errors::BigQueryError::SerializeError), before any request.
    pub fn filter<FN>(self, filter: FN) -> Self
    where
        FN: FnOnce(BigQueryFilterBuilder) -> Option<BigQueryFilter>,
    {
        let (row_restriction, filter_failure) =
            match filter(BigQueryFilterBuilder::new()).map(BigQueryFilter::into_row_restriction) {
                None => (None, None),
                Some(Ok(sql)) => (Some(sql), None),
                Some(Err(failure)) => (None, Some(failure)),
            };
        Self {
            params: BigQueryReadParams {
                row_restriction,
                ..self.params
            },
            filter_failure,
            ..self
        }
    }

    /// Reads only the rows that match GoogleSQL condition text such as `n > 10`, sent as the
    /// session's `row_restriction` exactly as given.
    ///
    /// **Trusted input only.** The text is SQL: a value spliced into it can rewrite the
    /// condition, so never build it from user input. Use [`filter`](Self::filter) for any
    /// condition that carries a value. This and `filter` set the same restriction; the last
    /// call wins.
    pub fn filter_sql(self, row_restriction: impl Into<String>) -> Self {
        Self {
            params: self.params.with_row_restriction(row_restriction.into()),
            filter_failure: None,
            ..self
        }
    }

    /// Reads the table as it was at this time instead of now.
    pub fn snapshot_time(self, snapshot_time: BigQueryInstant) -> Self {
        Self {
            params: self.params.with_snapshot_time(snapshot_time),
            ..self
        }
    }

    /// Reads a random sample of about this percentage of the table, above 0 and up to 100.
    /// BigQuery refuses another value when the session opens.
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
            filter_failure: self.filter_failure,
            _target: PhantomData,
        }
    }

    /// Streams the record batches as BigQuery sent them, after IPC decode and decompression.
    /// A read stream that fails for good is one `Err` item, and then the stream ends.
    pub async fn record_batches<'b>(
        self,
    ) -> BigQueryResult<BoxStream<'b, BigQueryResult<RecordBatch>>> {
        if let Some(failure) = self.filter_failure {
            return Err(failure);
        }
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
    filter_failure: Option<BigQueryError>,
    _target: PhantomData<fn() -> T>,
}

impl<'a, D, T> BigQuerySelectObjBuilder<'a, D, T>
where
    D: BigQueryReadSupport,
    T: DeserializeOwned + Send + 'static,
{
    fn checked_params(self) -> BigQueryResult<BigQueryReadParams> {
        match self.filter_failure {
            Some(failure) => Err(failure),
            None => Ok(self.params),
        }
    }

    /// Reads every row into a `Vec`, failing on the first row or stream error.
    pub async fn query(self) -> BigQueryResult<Vec<T>> {
        let db = self.db;
        db.read_obj(self.checked_params()?).await
    }

    /// Streams the rows. A row that fails to decode is logged at `error!` and skipped; the log
    /// line names the error's kind, row and field path and leaves out its message, which can
    /// hold the cell's text. A read stream that fails for good is logged too and ends the
    /// stream, so a stream that ends does not mean every row was read; use
    /// [`stream_query_with_errors`](Self::stream_query_with_errors) to tell the two apart and to
    /// get each error in full.
    pub async fn stream_query<'b>(self) -> BigQueryResult<BoxStream<'b, T>> {
        let db = self.db;
        db.stream_read_obj(self.checked_params()?).await
    }

    /// Streams the rows, yielding every failure as an `Err` item. A row that fails to decode
    /// is one `Err(DeserializeError)` and the stream goes on; a read stream that fails for good
    /// is one `Err`, and then the stream ends.
    pub async fn stream_query_with_errors<'b>(
        self,
    ) -> BigQueryResult<BoxStream<'b, BigQueryResult<T>>> {
        let db = self.db;
        db.stream_read_obj_with_errors(self.checked_params()?).await
    }
}
