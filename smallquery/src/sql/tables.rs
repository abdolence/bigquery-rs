//! The tables a statement names, as DataFusion sees them: each registered under its three-part
//! name, project as catalog and dataset as schema, and read at the snapshot that was current
//! when the statement loaded it, so that one statement reads one snapshot of each table and
//! never reloads the catalog mid-flight.
//!
//! A table is registered with the wire metadata of each column's type on its schema, and its
//! scans relabel their batches to that schema: a JSON, GEOGRAPHY or RANGE column a query passes
//! through then keeps its BigQuery type up to the output step, which reads it from the metadata.
//! Iceberg's own scan stays the leaf of the plan, so projection and filter pushdown reach it
//! unchanged.

use crate::catalog::LoadedTable;
use crate::error::EngineError;
use crate::ids::TableRef;
use crate::types::Columns;
use arrow::array::{ArrayRef, RecordBatch, RecordBatchOptions};
use arrow::compute::cast;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::memory::{MemoryCatalogProvider, MemorySchemaProvider};
use datafusion::catalog::{CatalogProvider, Session, TableProvider};
use datafusion::common::Result as DataFusionResult;
use datafusion::datasource::TableType;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use iceberg_datafusion::IcebergStaticTableProvider;
use std::any::Any;
use std::sync::Arc;

/// Registers `loaded` as `table` in `session`, read at its current snapshot.
///
/// # Errors
/// The errors of building the provider and of DataFusion's registration.
pub(crate) async fn register_table(
    session: &SessionContext,
    table: &TableRef,
    loaded: &LoadedTable,
) -> Result<(), EngineError> {
    let provider = loaded.table_provider().await?;
    let project = table.project.as_str();
    let catalog = match session.catalog(project) {
        Some(catalog) => catalog,
        None => {
            let catalog: Arc<dyn CatalogProvider> = Arc::new(MemoryCatalogProvider::new());
            session.register_catalog(project, catalog.clone());
            catalog
        }
    };
    let dataset = table.dataset.as_str();
    let schema = match catalog.schema(dataset) {
        Some(schema) => schema,
        None => {
            let schema = Arc::new(MemorySchemaProvider::new());
            catalog.register_schema(dataset, schema.clone())?;
            schema
        }
    };
    schema.register_table(table.table.to_string(), provider)?;
    Ok(())
}

impl LoadedTable {
    /// A provider of this table's rows at its current snapshot, every column with the wire
    /// metadata of its BigQuery type.
    async fn table_provider(&self) -> Result<Arc<dyn TableProvider>, EngineError> {
        let metadata = self.metadata();
        let (provider, schema) = match metadata.current_snapshot() {
            Some(snapshot) => (
                IcebergStaticTableProvider::try_new_from_table_snapshot(
                    self.table.clone(),
                    snapshot.snapshot_id(),
                )
                .await?,
                snapshot.schema(metadata)?,
            ),
            None => (
                IcebergStaticTableProvider::try_new_from_table(self.table.clone()).await?,
                metadata.current_schema().clone(),
            ),
        };
        let engine = provider.schema();
        let annotated = Columns::from_iceberg(&schema, metadata.properties())?.annotate(&engine);
        Ok(Arc::new(AnnotatedTable {
            inner: provider,
            schema: Arc::new(annotated),
        }))
    }
}

/// Iceberg's static provider with the wire metadata of every column on its schema.
#[derive(Debug)]
struct AnnotatedTable {
    inner: IcebergStaticTableProvider,
    schema: SchemaRef,
}

#[async_trait]
impl TableProvider for AnnotatedTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Iceberg's scan of no columns yields batches without a row count, which Arrow refuses,
        // so a scan that needs only the rows, as `COUNT(*)` does, reads the first column and
        // keeps its row counts.
        let first = vec![0];
        let scanned = match projection {
            Some(projection) if projection.is_empty() => Some(&first),
            projection => projection,
        };
        let input = self.inner.scan(state, scanned, filters, limit).await?;
        let schema = match projection {
            Some(projection) => Arc::new(self.schema.project(projection)?),
            None => self.schema.clone(),
        };
        Ok(Arc::new(AnnotatedScan::new(input, schema)))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }
}

/// An Iceberg scan whose batches are relabeled to the annotated schema: the values are the
/// scan's, the field metadata the schema's.
#[derive(Debug)]
struct AnnotatedScan {
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    /// Whether the scan's orderings carry over: they do while every column keeps its type, and
    /// a nested column whose fields gained metadata has another type to DataFusion.
    keeps_order: bool,
}

impl AnnotatedScan {
    /// The relabeling of `input`, whose partitioning it keeps, and its orderings when every
    /// column keeps its type.
    fn new(input: Arc<dyn ExecutionPlan>, schema: SchemaRef) -> Self {
        let (equivalences, keeps_order) = match input
            .properties()
            .equivalence_properties()
            .clone()
            .with_new_schema(schema.clone())
        {
            Ok(equivalences) => (equivalences, true),
            Err(_) => (EquivalenceProperties::new(schema.clone()), false),
        };
        let properties = Arc::new(
            input
                .properties()
                .as_ref()
                .clone()
                .with_eq_properties(equivalences),
        );
        Self {
            input,
            schema,
            properties,
            keeps_order,
        }
    }
}

impl DisplayAs for AnnotatedScan {
    fn fmt_as(&self, _format: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("AnnotatedScan")
    }
}

impl ExecutionPlan for AnnotatedScan {
    fn name(&self) -> &str {
        "AnnotatedScan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    /// Relabeling is per batch, so a repartition of the scan below it buys nothing; the
    /// partitioning is the scan's, and the plan's properties hold only while they agree.
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![self.keeps_order]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let input = children
            .into_iter()
            .next()
            .unwrap_or_else(|| self.input.clone());
        Ok(Arc::new(AnnotatedScan::new(input, self.schema.clone())))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let schema = self.schema.clone();
        let batches = self.input.execute(partition, context)?;
        let relabeled = batches.map(move |batch| relabel(&batch?, &schema));
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            relabeled,
        )))
    }
}

/// `batch` in `schema`: a column whose nested fields carry metadata of their own is cast to its
/// annotated type, which keeps its values; the other columns are taken as they are. A schema of
/// no columns keeps the batch's row count only.
fn relabel(batch: &RecordBatch, schema: &SchemaRef) -> DataFusionResult<RecordBatch> {
    if schema.fields().is_empty() {
        return Ok(RecordBatch::try_new_with_options(
            schema.clone(),
            Vec::new(),
            &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
        )?);
    }
    let columns = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(column, field)| {
            if column.data_type() == field.data_type() {
                Ok(column.clone())
            } else {
                cast(column, field.data_type())
            }
        })
        .collect::<Result<Vec<ArrayRef>, _>>()?;
    Ok(RecordBatch::try_new(schema.clone(), columns)?)
}
