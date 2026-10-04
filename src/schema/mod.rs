//! Declarative table schemas: `db.fluent().schema().table(..)`, with a read-only `.plan()` and a
//! `.sync()` that applies it.
//!
//! The diff follows BigQuery's measured rules for which changes it allows and how
//! (`docs/schema-probe-2026-10.md`, `docs/recreate-probe-2026-10.md`): additive changes go
//! through one `PatchTable`, removals of labels and clustering through `UpdateTable`, drops,
//! renames and widenings through DDL, and changes that are impossible in place are refused
//! unless the declaration opts in to recreating the table.

mod declaration;
pub(crate) use declaration::BigQueryTableDeclarationDraft;
pub use declaration::*;

mod plan;
pub use plan::*;

mod live;

mod diff;

mod ddl;

mod sync;
