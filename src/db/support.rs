//! Crate-private "support" traits: the operations the fluent builders are generic over, so
//! that a mock database can stand in for [`BigQueryDb`](crate::BigQueryDb) in their tests.
//!
//! They are declared as `pub trait` inside this private module. `pub(crate) trait` would trip
//! the `private_bounds` lint wherever a public builder uses them as a bound, and the gates run
//! clippy with `-Dwarnings`. A `pub trait` in a private module has the same reachability: it
//! cannot be named, implemented or called from outside this crate.

mod query;
mod read;
mod table;
mod write;

pub use query::*;
pub use read::*;
pub use table::*;
pub use write::*;
