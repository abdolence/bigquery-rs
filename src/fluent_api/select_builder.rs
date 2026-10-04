use crate::BigQueryReadSupport;

/// The first stage of a table read, from
/// [`BigQueryExprBuilder::select`](crate::BigQueryExprBuilder::select).
#[derive(Clone, Debug)]
pub struct BigQuerySelectInitialBuilder<'a, D>
where
    D: BigQueryReadSupport,
{
    #[allow(dead_code, reason = "read by the read stages built from here")]
    db: &'a D,
}

impl<'a, D> BigQuerySelectInitialBuilder<'a, D>
where
    D: BigQueryReadSupport,
{
    pub(crate) fn new(db: &'a D) -> Self {
        Self { db }
    }
}
