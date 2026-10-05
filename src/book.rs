//! Compiles the code in the book chapters as doctests, so an example cannot go stale without a
//! test failing. Only reachable through `cfg(doctest)`, which rustdoc sets while collecting
//! doctests and never while building the crate.

#[doc = include_str!("../docs/src/intro.md")]
mod intro {}

#[doc = include_str!("../docs/src/getting-started.md")]
mod getting_started {}

#[doc = include_str!("../docs/src/queries.md")]
mod queries {}

#[doc = include_str!("../docs/src/reading-tables.md")]
mod reading_tables {}

#[doc = include_str!("../docs/src/table-reads-or-queries.md")]
mod table_reads_or_queries {}

#[doc = include_str!("../docs/src/writing-data.md")]
mod writing_data {}

#[doc = include_str!("../docs/src/cdc.md")]
mod cdc {}

#[doc = include_str!("../docs/src/schema-management.md")]
mod schema_management {}

#[doc = include_str!("../docs/src/admin.md")]
mod admin {}

#[doc = include_str!("../docs/src/types.md")]
mod type_mapping {}

#[doc = include_str!("../docs/src/observability.md")]
mod observability {}

/// The README's quick start, compiled so it cannot drift from the API it demonstrates.
#[doc = include_str!("../README.md")]
mod readme {}
