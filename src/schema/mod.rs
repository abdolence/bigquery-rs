//! Declarative table schemas: `db.fluent().schema().table(..)`, with a read-only `.plan()` and a
//! `.sync()` that applies it.
//!
//! The diff follows the rules BigQuery was measured to apply to each kind of change: additive
//! changes go through one `PatchTable`, removals of labels and clustering through `UpdateTable`, drops,
//! renames and widenings through DDL, and changes that are impossible in place are refused
//! unless the declaration opts in to recreating the table.

mod declaration;
pub(crate) use declaration::BigQueryTableDeclarationDraft;
pub use declaration::*;

mod plan;
pub use plan::*;

mod live;
pub(crate) use live::table_partitioning;

mod diff;

mod ddl;

mod sync;
