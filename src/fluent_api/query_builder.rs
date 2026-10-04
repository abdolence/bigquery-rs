use crate::{BigQueryQueryParams, BigQueryQuerySupport};

/// A query being built, from [`BigQueryExprBuilder::query`](crate::BigQueryExprBuilder::query).
#[derive(Clone, Debug)]
pub struct BigQueryQueryBuilder<'a, D>
where
    D: BigQueryQuerySupport,
{
    #[allow(dead_code, reason = "read by the query terminals")]
    db: &'a D,
    #[allow(dead_code, reason = "read by the query terminals")]
    params: BigQueryQueryParams,
}

impl<'a, D> BigQueryQueryBuilder<'a, D>
where
    D: BigQueryQuerySupport,
{
    pub(crate) fn new(db: &'a D, params: BigQueryQueryParams) -> Self {
        Self { db, params }
    }
}
