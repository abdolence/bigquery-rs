//! Builders for declaring one table's schema and settings and reconciling the table with them.
//!
//! The entry point is [`BigQueryExprBuilder::schema`](crate::BigQueryExprBuilder::schema).

use crate::schema::BigQueryTableDeclarationDraft;
use crate::{
    BigQueryDatasetBuilder, BigQueryDatasetListBuilder, BigQueryDatasetRef, BigQueryDb,
    BigQueryPartitionUnit, BigQueryPartitioning, BigQueryRecreatePolicy, BigQueryResult,
    BigQuerySchemaColumn, BigQuerySchemaColumnsBuilder, BigQuerySchemaSupport, BigQueryTable,
    BigQueryTableDeclaration, BigQueryTablePlan, BigQueryTableRef, BigQueryTableSyncReport,
};
use std::time::Duration;

/// The schema namespace, from [`BigQueryExprBuilder::schema`](crate::BigQueryExprBuilder::schema).
///
/// Tables are the one kind of schema object it declares so far; datasets have the plain calls
/// of [`dataset`](Self::dataset) and [`datasets`](Self::datasets).
#[derive(Clone, Debug)]
pub struct BigQuerySchemaBuilder<'a, D>
where
    D: BigQuerySchemaSupport,
{
    db: &'a D,
}

impl<'a, D> BigQuerySchemaBuilder<'a, D>
where
    D: BigQuerySchemaSupport,
{
    pub(crate) fn new(db: &'a D) -> Self {
        Self { db }
    }

    /// Names the table this statement owns.
    ///
    /// One statement owns one table: `.prune_undeclared()` never reaches anything outside it.
    /// Several tables need several statements.
    #[inline]
    pub fn table(self, table: impl Into<BigQueryTableRef>) -> BigQueryTableSchemaBuilder<'a, D> {
        BigQueryTableSchemaBuilder {
            db: self.db,
            draft: BigQueryTableDeclarationDraft::new(table.into()),
        }
    }
}

impl<'a> BigQuerySchemaBuilder<'a, BigQueryDb> {
    /// Names a dataset for one plain call: create, get, update, delete, or list its tables.
    ///
    /// ```rust,no_run
    /// # use bigquery::*;
    /// # async fn example(db: BigQueryDb) -> BigQueryResult<()> {
    /// const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
    ///
    /// let shop = db
    ///     .fluent()
    ///     .schema()
    ///     .dataset(SHOP)
    ///     .create()
    ///     .location("EU")
    ///     .labels([("team", "shop")])
    ///     .execute()
    ///     .await?;
    /// println!("created {} in {:?}", shop.reference, shop.location);
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    pub fn dataset(self, dataset: impl Into<BigQueryDatasetRef>) -> BigQueryDatasetBuilder<'a> {
        BigQueryDatasetBuilder::new(self.db, dataset.into())
    }

    /// Starts listing the datasets of the client's project.
    #[inline]
    pub fn datasets(self) -> BigQueryDatasetListBuilder<'a> {
        BigQueryDatasetListBuilder::new(self.db)
    }
}

/// One table's declaration. End it with [`plan`](Self::plan) or [`sync`](Self::sync), or with
/// one of the plain calls [`get`](Self::get) and [`delete`](Self::delete), which act on the
/// table and ignore whatever the chain declared.
///
/// What the chain declares is what `.sync()` makes the table hold; what it leaves undeclared
/// (a column, a label, clustering, the primary key, the partitioning, a description or a
/// default value) is kept as the table has it, and reported unless it is a description, a
/// default or an expiration.
///
/// # Rolling out a change
///
/// Changes that keep running writers working go in before the deploy of the code that needs
/// them: adding columns, relaxing REQUIRED, descriptions, labels, clustering. New writer
/// connections accept the new schema about a second after the sync, and open ones get
/// `updated_schema` after about 7 seconds. A writer should reconnect before it sends NULL into
/// a relaxed column.
///
/// Drops and renames go in after every writer has stopped sending the column:
/// [`prune_undeclared`](Self::prune_undeclared) in a second sync once the rollout is done. For
/// about 9 seconds after a drop, an open writer's values for the column are accepted and lost
/// silently, then the connection fails with `Input schema has more fields than BigQuery
/// schema`.
///
/// Readers that pass `selected_fields` with a new or renamed column name fail with `The
/// following selected fields do not exist` for about 30 seconds after the sync; readers without
/// `selected_fields` see the change at once.
#[derive(Clone, Debug)]
pub struct BigQueryTableSchemaBuilder<'a, D>
where
    D: BigQuerySchemaSupport,
{
    db: &'a D,
    draft: BigQueryTableDeclarationDraft,
}

impl<'a, D> BigQueryTableSchemaBuilder<'a, D>
where
    D: BigQuerySchemaSupport,
{
    /// Declares the columns, in table order. Each call replaces the columns of a previous one.
    ///
    /// ```rust
    /// # use bigquery::*;
    /// # fn declare(c: BigQuerySchemaColumnsBuilder) -> Vec<BigQuerySchemaColumn> {
    /// struct Order {
    ///     id: i64,
    ///     note: Option<String>,
    /// }
    ///
    /// c.fields([
    ///     c.field(path!(Order::id)).int64().required(),
    ///     c.field(path!(Order::note)).string().description("free text"),
    /// ])
    /// # }
    /// ```
    #[inline]
    pub fn columns<F>(mut self, columns: F) -> Self
    where
        F: FnOnce(BigQuerySchemaColumnsBuilder) -> Vec<BigQuerySchemaColumn>,
    {
        self.draft.columns = columns(BigQuerySchemaColumnsBuilder);
        self
    }

    /// The primary key, `NOT ENFORCED` as every BigQuery key is: BigQuery uses it to plan
    /// queries and never checks it.
    #[inline]
    pub fn primary_key<I>(mut self, columns: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<String>,
    {
        self.draft.primary_key = Some(columns.into_iter().map(Into::into).collect());
        self
    }

    /// Any partitioning. A table's partitioning cannot change in place: adding or changing it
    /// is impossible in place, see [`recreate_if_empty`](Self::recreate_if_empty).
    #[inline]
    pub fn partition_by(mut self, partitioning: BigQueryPartitioning) -> Self {
        self.draft.partitioning = Some(partitioning);
        self
    }

    /// Partitions by hour on a declared TIMESTAMP or DATETIME column.
    #[inline]
    pub fn partition_by_hour(self, column: impl Into<String>) -> Self {
        self.partition_on(BigQueryPartitionUnit::Hour, column)
    }

    /// Partitions by day on a declared DATE, TIMESTAMP or DATETIME column.
    #[inline]
    pub fn partition_by_day(self, column: impl Into<String>) -> Self {
        self.partition_on(BigQueryPartitionUnit::Day, column)
    }

    /// Partitions by month on a declared DATE, TIMESTAMP or DATETIME column.
    #[inline]
    pub fn partition_by_month(self, column: impl Into<String>) -> Self {
        self.partition_on(BigQueryPartitionUnit::Month, column)
    }

    /// Partitions by year on a declared DATE, TIMESTAMP or DATETIME column.
    #[inline]
    pub fn partition_by_year(self, column: impl Into<String>) -> Self {
        self.partition_on(BigQueryPartitionUnit::Year, column)
    }

    fn partition_on(self, unit: BigQueryPartitionUnit, column: impl Into<String>) -> Self {
        self.partition_by(BigQueryPartitioning::Time {
            unit,
            column: Some(column.into()),
        })
    }

    /// How long a partition is kept after its partition time. Changes in place.
    #[inline]
    pub fn partition_expiration(mut self, expiration: Duration) -> Self {
        self.draft.partition_expiration = Some(expiration);
        self
    }

    /// The clustering columns, most important first. Clustering changes in place.
    #[inline]
    pub fn cluster_by<I>(mut self, columns: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<String>,
    {
        self.draft.clustering = Some(columns.into_iter().map(Into::into).collect());
        self
    }

    /// The table description, sent as a value, never as SQL.
    #[inline]
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.draft.description = Some(description.into());
        self
    }

    /// The declared labels. Labels the table has and this does not declare are kept, unless
    /// [`prune_undeclared`](Self::prune_undeclared) is set. Each call replaces the labels of a
    /// previous one.
    #[inline]
    pub fn labels<I, K, V>(mut self, labels: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.draft.labels = labels
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();
        self
    }

    /// When the table expires and BigQuery deletes it, at millisecond precision.
    #[inline]
    pub fn expiration(mut self, at: jiff::Timestamp) -> Self {
        self.draft.expiration = Some(at);
        self
    }

    /// Widens columns whose declared type is wider than the table's, such as INT64 to NUMERIC
    /// or `STRING(10)` to `STRING(20)`, with `ALTER COLUMN SET DATA TYPE`. Without it a
    /// widening is reported as withheld. Top-level columns only; widening a nested field is
    /// impossible in place.
    #[inline]
    pub fn allow_widening(mut self) -> Self {
        self.draft.allow_widening = true;
        self
    }

    /// **Destructive.** Also removes what the table has and this statement does not declare:
    /// undeclared columns are dropped with every value in them, and undeclared labels,
    /// clustering and primary key are removed. Without it they are kept and reported as
    /// withheld.
    ///
    /// An undeclared nested field or partitioning cannot be removed in place, so with this set
    /// it makes the plan need a recreate.
    ///
    /// Run it after the rollout: an open writer still sending a dropped column has its values
    /// accepted and lost silently for about 9 seconds before it fails.
    #[inline]
    pub fn prune_undeclared(mut self) -> Self {
        self.draft.prune = true;
        self
    }

    /// Lets `.sync()` recreate the table when a change is impossible in place, but only when
    /// `GetTable` reports `num_rows == 0`; otherwise `.sync()` refuses as without it.
    ///
    /// `num_rows` lags Storage Write: rows written through COMMITTED or PENDING streams show
    /// after 60 to 70 seconds, and rows on the default stream can take more than 5 minutes, so
    /// a table written seconds ago can read as empty and be recreated, its rows lost. Use it
    /// for tables that are new or being experimented on, with no writer running. Nothing guards
    /// the recreate against concurrent changes: BigQuery takes no precondition on it.
    ///
    /// See [`dangerously_recreate_with_data_loss`](Self::dangerously_recreate_with_data_loss)
    /// for what a recreate keeps and loses. The last of the two calls wins.
    #[inline]
    pub fn recreate_if_empty(mut self) -> Self {
        self.draft.recreate = Some(BigQueryRecreatePolicy::IfEmpty);
        self
    }

    /// **Deletes every row.** Lets `.sync()` recreate the table when a change is impossible in
    /// place, whatever it holds. No data is migrated.
    ///
    /// The table is recreated with `CREATE OR REPLACE TABLE` when its partitioning and
    /// clustering stay as they are, which keeps the table's IAM bindings, and otherwise with
    /// `DROP TABLE` and `CREATE TABLE` in one script, which loses them. Both drop every row
    /// access policy. `.plan()` lists what will be lost; the counts it gives come from
    /// `GetTable` and lag Storage Write as described at
    /// [`recreate_if_empty`](Self::recreate_if_empty).
    ///
    /// Stop writers and readers first. A writer connection opened before the recreate keeps
    /// getting acks for about 4 to 7 seconds for rows that are lost, then waits about 120
    /// seconds for `DeadlineExceeded`; new writers can get `NotFound` for up to a few minutes.
    /// A read session opened before it keeps returning the old rows. Nothing guards the
    /// recreate against concurrent changes. The last of this and `recreate_if_empty()` wins.
    #[inline]
    pub fn dangerously_recreate_with_data_loss(mut self) -> Self {
        self.draft.recreate = Some(BigQueryRecreatePolicy::DangerouslyWithDataLoss);
        self
    }

    /// Takes a `CREATE SNAPSHOT TABLE` of the table before a recreate, as a cheap undo, named
    /// `<table>_snapshot_<unix seconds>` in the same dataset. The snapshot misses rows still in
    /// the streaming buffer, which is every row written through Storage Write in roughly the
    /// last minute or more. It is never deleted by the crate. Needs a recreate opt-in.
    #[inline]
    pub fn snapshot_first(mut self) -> Self {
        self.draft.snapshot_first = true;
        self
    }

    /// Reports what [`sync`](Self::sync) would do, writing nothing.
    ///
    /// # Errors
    /// [`InvalidParametersError`](crate::errors::BigQueryError::InvalidParametersError) for a
    /// declaration the crate cannot diff or render, before any request; otherwise what
    /// `GetTable` returned.
    pub async fn plan(self) -> BigQueryResult<BigQueryTablePlan> {
        let declaration = BigQueryTableDeclaration::try_from(self.draft)?;
        self.db.plan_table_schema(declaration).await
    }

    /// Makes the table match the declaration.
    ///
    /// It reads the table, plans, and writes in a fixed order: one `PatchTable` with every
    /// additive change, a second one for the default values of added columns, an `UpdateTable`
    /// for removed labels and clustering, then one DDL statement at a time for renames,
    /// widenings and drops. Patch and Update carry the etag the sync read as an `if-match`
    /// precondition. More than five writes to one table are spaced about 2 seconds apart, under
    /// BigQuery's per-table update rate limit, and rate-limit errors are retried.
    ///
    /// When a change is impossible in place and no recreate opt-in applies, it writes nothing.
    ///
    /// # Errors
    /// - [`SchemaChangeRefused`](crate::errors::BigQueryError::SchemaChangeRefused) with the
    ///   plan when it refuses, nothing written;
    /// - [`DataConflictError`](crate::errors::BigQueryError::DataConflictError) when the table
    ///   changed between the read and a patch or update; run the sync again. Changes sent before
    ///   it stay applied, as do those of any failure later in the sync;
    /// - [`InvalidParametersError`](crate::errors::BigQueryError::InvalidParametersError) as for
    ///   [`plan`](Self::plan).
    pub async fn sync(self) -> BigQueryResult<BigQueryTableSyncReport> {
        let declaration = BigQueryTableDeclaration::try_from(self.draft)?;
        self.db.sync_table_schema(declaration).await
    }
}

impl BigQueryTableSchemaBuilder<'_, BigQueryDb> {
    /// Reads the table: its schema, sizes and settings. Views and other table-like objects
    /// read as well.
    ///
    /// # Errors
    /// [`DataNotFoundError`](crate::errors::BigQueryError::DataNotFoundError) for a table that
    /// does not exist.
    pub async fn get(self) -> BigQueryResult<BigQueryTable> {
        self.db.get_table(&self.draft.table).await
    }

    /// **Deletes the table with every row in it.** Nothing is kept for an undo, and open
    /// writers to it fail.
    ///
    /// # Errors
    /// [`DataNotFoundError`](crate::errors::BigQueryError::DataNotFoundError) for a table that
    /// does not exist. A retry after a lost response can report the table it deleted this way.
    pub async fn delete(self) -> BigQueryResult<()> {
        self.db.delete_table(&self.draft.table).await
    }
}
