//! GoogleSQL queries and their jobs.
//!
//! A query runs through `JobService.Query` and asks for its result as Arrow. A result that
//! comes complete in that first response is decoded from it; a larger one, or one whose job
//! outlived the first call, is read through the Storage Read API from the job's destination
//! table, with the read path's decoder in both cases.

mod ids;
pub use ids::*;

mod sql_file;
pub use sql_file::BigQuerySql;

mod statement;
pub use statement::BigQueryStatementType;

mod types;
pub use types::*;

mod jobs;
mod params;
mod routing;
mod support;

#[cfg(test)]
pub(crate) use params::{infer_param, typed_param, ParamLabel};
pub(crate) use params::{literal_of, parameter_mode, ParamList};

#[cfg(test)]
mod tests;
