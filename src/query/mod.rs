//! GoogleSQL queries and their jobs.
//!
//! A query runs through `JobService.Query` and asks for its result as Arrow. A result that
//! comes complete in that first response is decoded from it; a larger one, or one whose job
//! outlived the first call, is read through the Storage Read API from the job's destination
//! table, with the read path's decoder in both cases.

mod types;
pub use types::*;

mod jobs;
mod params;
mod routing;
mod support;

pub(crate) use params::{infer_param, struct_params, typed_param, ParamFailure, ParamLabel};
