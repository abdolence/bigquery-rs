use crate::BigQueryWriteSupport;

/// The first stage of an insert, from
/// [`BigQueryExprBuilder::insert`](crate::BigQueryExprBuilder::insert).
#[derive(Clone, Debug)]
pub struct BigQueryInsertInitialBuilder<'a, D>
where
    D: BigQueryWriteSupport,
{
    #[allow(dead_code, reason = "read by the insert stages built from here")]
    db: &'a D,
}

impl<'a, D> BigQueryInsertInitialBuilder<'a, D>
where
    D: BigQueryWriteSupport,
{
    pub(crate) fn new(db: &'a D) -> Self {
        Self { db }
    }
}
