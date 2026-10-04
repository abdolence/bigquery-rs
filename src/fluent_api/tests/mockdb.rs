/// A database that the fluent builders' tests stand in for
/// [`BigQueryDb`](crate::BigQueryDb), through the support traits implemented in the sibling
/// `mock_*` modules.
#[derive(Clone, Debug)]
#[allow(
    dead_code,
    reason = "the builder tests construct it once the support traits are mocked"
)]
pub struct MockDatabase;
