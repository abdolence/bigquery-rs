//! The rules a test registers, how a call finds the one that answers it, and the faults a rule
//! can answer with.

use crate::errors::BigQueryError;
use crate::testing::state::TableKey;
use crate::{BigQueryDmlStats, BigQueryResult, BigQueryStatementType, BigQueryTableSchema};
use arrow_array::RecordBatch;
use gcloud_sdk::google::cloud::bigquery::v2::{QueryParameter, QueryParameterValue};
use gcloud_sdk::tonic::Code;
use std::fmt::{Debug, Display, Formatter};
use std::num::NonZeroUsize;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// A registered rule, to read how many calls it answered. Clones share one counter.
///
/// Every rule a [`BigQueryFake`](super::BigQueryFake) registers stays registered until the fake
/// is dropped.
#[derive(Clone, Debug)]
pub struct BigQueryFakeRule {
    counter: Arc<RuleCounter>,
}

#[derive(Debug)]
struct RuleCounter {
    /// What the rule matches, as the unmatched report and `verify` list it.
    description: String,
    /// The calls the rule answers before it stops matching, if limited.
    times: Option<NonZeroUsize>,
    calls: AtomicUsize,
}

impl BigQueryFakeRule {
    fn new(description: String, times: Option<NonZeroUsize>) -> Self {
        Self {
            counter: Arc::new(RuleCounter {
                description,
                times,
                calls: AtomicUsize::new(0),
            }),
        }
    }

    /// A rule that answers every call it matches.
    pub(super) fn unlimited(description: String) -> Self {
        Self::new(description, None)
    }

    /// A rule that answers `times` calls if set, and every call it matches if not.
    ///
    /// # Errors
    /// [`BigQueryError::InvalidParametersError`] for the field `times` for `times(0)`, a rule
    /// that could never answer.
    pub(super) fn limited(description: String, times: Option<usize>) -> BigQueryResult<Self> {
        match times.map(NonZeroUsize::new) {
            Some(None) => Err(BigQueryError::invalid_parameters(
                "times",
                format!("{description} would answer no call; leave times unset or pass 1 or more"),
            )),
            Some(times) => Ok(Self::new(description, times)),
            None => Ok(Self::unlimited(description)),
        }
    }

    /// The RPCs this rule answered, retries included. For
    /// [`reject_rows`](super::BigQueryFake::reject_rows), the append requests it rejected.
    pub fn calls(&self) -> usize {
        self.counter.calls.load(Ordering::SeqCst)
    }

    /// Counts one call if the rule has calls left, and says whether it did.
    fn claim(&self) -> bool {
        let times = self.counter.times;
        self.counter
            .calls
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |calls| {
                times
                    .is_none_or(|times| calls < times.get())
                    .then_some(calls + 1)
            })
            .is_ok()
    }

    /// Counts one call whatever the rule's limit.
    #[allow(dead_code, reason = "used by the Storage Write RPCs")]
    pub(super) fn count(&self) {
        self.counter.calls.fetch_add(1, Ordering::SeqCst);
    }

    /// The rule as the unmatched report lists it: `description (answered n of times(m))`.
    fn listed(&self) -> String {
        let calls = self.calls();
        match self.counter.times {
            Some(times) => format!(
                "{} (answered {calls} of times({times}))",
                self.counter.description
            ),
            None => format!("{} (answered {calls})", self.counter.description),
        }
    }

    /// Why this rule fails `verify`: it was limited to `times(n)` and answered another number
    /// of calls.
    fn shortfall(&self) -> Option<String> {
        let times = self.counter.times?.get();
        let calls = self.calls();
        (calls != times).then(|| {
            format!(
                "{} answered {calls} of its times({times}) calls",
                self.counter.description
            )
        })
    }
}

/// How a scripted call fails.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum BigQueryFakeFault {
    /// Ends the call with a gRPC status before its first response message, as BigQuery does
    /// for a refused request. The client maps it as it maps BigQuery's own: `NotFound` is a
    /// [`DataNotFoundError`](crate::errors::BigQueryError::DataNotFoundError), `AlreadyExists`
    /// a [`DataConflictError`](crate::errors::BigQueryError::DataConflictError), `Aborted`,
    /// `Unavailable`, `ResourceExhausted`, `Internal`, and `PermissionDenied` or
    /// `InvalidArgument` with "exceeded rate limits" in the message a retryable
    /// [`DatabaseError`](crate::errors::BigQueryError::DatabaseError), and anything else one
    /// that is not retried.
    ///
    /// On `AppendRows` it is instead the in-band error of the append request that drew it.
    Status {
        /// The gRPC code.
        code: BigQueryFakeCode,
        /// The status message.
        message: String,
    },
    /// Closes the connection without answering, as a response lost in transit. The client
    /// sees a transport error, which it retries. Every other call on the same connection is
    /// lost with it.
    ConnectionDropped,
    /// Never answers, as a call stuck in flight. The client waits until its own timeout, if
    /// it has one.
    Hang,
}

impl BigQueryFakeFault {
    /// A [`Status`](Self::Status) fault.
    pub fn status(code: BigQueryFakeCode, message: impl Into<String>) -> Self {
        Self::Status {
            code,
            message: message.into(),
        }
    }
}

/// The gRPC status codes a fault can answer with: every code but `OK`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BigQueryFakeCode {
    /// `CANCELLED`.
    Cancelled,
    /// `UNKNOWN`.
    Unknown,
    /// `INVALID_ARGUMENT`.
    InvalidArgument,
    /// `DEADLINE_EXCEEDED`.
    DeadlineExceeded,
    /// `NOT_FOUND`.
    NotFound,
    /// `ALREADY_EXISTS`.
    AlreadyExists,
    /// `PERMISSION_DENIED`.
    PermissionDenied,
    /// `RESOURCE_EXHAUSTED`.
    ResourceExhausted,
    /// `FAILED_PRECONDITION`.
    FailedPrecondition,
    /// `ABORTED`.
    Aborted,
    /// `OUT_OF_RANGE`.
    OutOfRange,
    /// `UNIMPLEMENTED`.
    Unimplemented,
    /// `INTERNAL`.
    Internal,
    /// `UNAVAILABLE`.
    Unavailable,
    /// `DATA_LOSS`.
    DataLoss,
    /// `UNAUTHENTICATED`.
    Unauthenticated,
}

impl BigQueryFakeCode {
    /// The code on the wire. A method rather than `From`, which would put the transport's
    /// type in the public API.
    pub(crate) fn grpc_code(self) -> Code {
        match self {
            BigQueryFakeCode::Cancelled => Code::Cancelled,
            BigQueryFakeCode::Unknown => Code::Unknown,
            BigQueryFakeCode::InvalidArgument => Code::InvalidArgument,
            BigQueryFakeCode::DeadlineExceeded => Code::DeadlineExceeded,
            BigQueryFakeCode::NotFound => Code::NotFound,
            BigQueryFakeCode::AlreadyExists => Code::AlreadyExists,
            BigQueryFakeCode::PermissionDenied => Code::PermissionDenied,
            BigQueryFakeCode::ResourceExhausted => Code::ResourceExhausted,
            BigQueryFakeCode::FailedPrecondition => Code::FailedPrecondition,
            BigQueryFakeCode::Aborted => Code::Aborted,
            BigQueryFakeCode::OutOfRange => Code::OutOfRange,
            BigQueryFakeCode::Unimplemented => Code::Unimplemented,
            BigQueryFakeCode::Internal => Code::Internal,
            BigQueryFakeCode::Unavailable => Code::Unavailable,
            BigQueryFakeCode::DataLoss => Code::DataLoss,
            BigQueryFakeCode::Unauthenticated => Code::Unauthenticated,
        }
    }
}

/// The RPCs a fault can target. `Query` and `InsertJob` are not among them: a query rule's
/// [`fails`](super::BigQueryFakeQueryBuilder::fails) scripts their failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum BigQueryFakeRpc {
    /// `JobService.GetQueryResults`, which waits for a query job.
    GetQueryResults,
    /// `JobService.GetJob`.
    GetJob,
    /// `JobService.CancelJob`.
    CancelJob,
    /// `BigQueryRead.CreateReadSession`.
    CreateReadSession,
    /// `BigQueryRead.ReadRows`.
    ReadRows,
    /// `BigQueryWrite.GetWriteStream`, which opens the default stream.
    GetWriteStream,
    /// `BigQueryWrite.CreateWriteStream`.
    CreateWriteStream,
    /// `BigQueryWrite.AppendRows`.
    AppendRows,
    /// `BigQueryWrite.FlushRows`.
    FlushRows,
    /// `BigQueryWrite.FinalizeWriteStream`.
    FinalizeWriteStream,
    /// `BigQueryWrite.BatchCommitWriteStreams`.
    BatchCommitWriteStreams,
    /// `TableService.GetTable`.
    GetTable,
    /// `TableService.InsertTable`.
    InsertTable,
    /// `TableService.PatchTable`.
    PatchTable,
    /// `TableService.UpdateTable`.
    UpdateTable,
    /// `TableService.DeleteTable`.
    DeleteTable,
    /// `TableService.ListTables`.
    ListTables,
    /// `DatasetService.GetDataset`.
    GetDataset,
    /// `DatasetService.InsertDataset`.
    InsertDataset,
    /// `DatasetService.UpdateDataset`.
    UpdateDataset,
    /// `DatasetService.DeleteDataset`.
    DeleteDataset,
    /// `DatasetService.ListDatasets`.
    ListDatasets,
}

impl BigQueryFakeRpc {
    /// Whether a call of this RPC belongs to one table, which a fault can be narrowed to.
    pub(super) fn names_a_table(self) -> bool {
        match self {
            BigQueryFakeRpc::CreateReadSession
            | BigQueryFakeRpc::ReadRows
            | BigQueryFakeRpc::GetWriteStream
            | BigQueryFakeRpc::CreateWriteStream
            | BigQueryFakeRpc::AppendRows
            | BigQueryFakeRpc::FlushRows
            | BigQueryFakeRpc::FinalizeWriteStream
            | BigQueryFakeRpc::BatchCommitWriteStreams
            | BigQueryFakeRpc::GetTable
            | BigQueryFakeRpc::InsertTable
            | BigQueryFakeRpc::PatchTable
            | BigQueryFakeRpc::UpdateTable
            | BigQueryFakeRpc::DeleteTable => true,
            BigQueryFakeRpc::GetQueryResults
            | BigQueryFakeRpc::GetJob
            | BigQueryFakeRpc::CancelJob
            | BigQueryFakeRpc::ListTables
            | BigQueryFakeRpc::GetDataset
            | BigQueryFakeRpc::InsertDataset
            | BigQueryFakeRpc::UpdateDataset
            | BigQueryFakeRpc::DeleteDataset
            | BigQueryFakeRpc::ListDatasets => false,
        }
    }
}

/// The `error_result` of a failed query job, which the client reads back as
/// [`BigQueryError::JobError`](crate::errors::BigQueryError::JobError).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BigQueryFakeJobFailure {
    /// BigQuery's short error code, such as `invalidQuery`.
    pub reason: String,
    /// The error message.
    pub message: String,
}

/// Which statements a query rule matches.
pub(super) enum SqlMatcher {
    /// The SQL text, compared exactly.
    Exact(String),
    /// A test's predicate over the SQL text.
    Matching(Box<dyn Fn(&str) -> bool + Send + Sync>),
}

impl SqlMatcher {
    /// Whether `sql` matches.
    ///
    /// # Errors
    /// A panic of the test's predicate, as an internal failure of the fake: a server task that
    /// panics leaves its client waiting for an answer.
    fn matches(&self, sql: &str) -> Result<bool, String> {
        match self {
            SqlMatcher::Exact(expected) => Ok(expected == sql),
            SqlMatcher::Matching(predicate) => catch_unwind(AssertUnwindSafe(|| predicate(sql)))
                .map_err(|_| format!("the query_matching predicate panicked on {sql:?}")),
        }
    }
}

/// `query "SQL"` or `query_matching(..)`.
impl Display for SqlMatcher {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SqlMatcher::Exact(sql) => write!(f, "query {sql:?}"),
            SqlMatcher::Matching(_) => f.write_str("query_matching(..)"),
        }
    }
}

impl Debug for SqlMatcher {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

/// What a query rule answers with.
#[derive(Debug)]
pub(super) enum QueryAnswer {
    /// A result of `schema`, whose rows are `rows`.
    Rows {
        schema: BigQueryTableSchema,
        rows: RecordBatch,
    },
    /// A DML statement and the rows it changed.
    Dml {
        statement_type: BigQueryStatementType,
        stats: BigQueryDmlStats,
    },
    /// A statement without rows or DML counts, such as DDL.
    Statement(BigQueryStatementType),
    /// A refused call.
    Fault(BigQueryFakeFault),
    /// A job that runs and fails.
    JobFailure(BigQueryFakeJobFailure),
}

/// A query rule's answer, and the figures every purpose of the call reports.
#[derive(Debug)]
pub(super) struct QueryReply {
    pub answer: QueryAnswer,
    pub bytes_processed: Option<i64>,
}

/// One query rule.
pub(super) struct QueryRule {
    pub sql: SqlMatcher,
    /// `None` answers a call whatever its parameters; otherwise named parameters must equal
    /// these as a set and positional ones in order.
    pub parameters: Option<Vec<QueryParameter>>,
    pub reply: Arc<QueryReply>,
    pub rule: BigQueryFakeRule,
}

impl QueryRule {
    fn matches(&self, sql: &str, parameters: &[QueryParameter]) -> Result<bool, String> {
        if !self.sql.matches(sql)? {
            return Ok(false);
        }
        let Some(expected) = &self.parameters else {
            return Ok(true);
        };
        let (expected_named, expected_positional): (Vec<_>, Vec<_>) = expected
            .iter()
            .partition(|parameter| !parameter.name.is_empty());
        let (named, positional): (Vec<_>, Vec<_>) = parameters
            .iter()
            .partition(|parameter| !parameter.name.is_empty());
        Ok(expected_positional == positional
            && expected_named.len() == named.len()
            && expected_named
                .iter()
                .all(|parameter| named.contains(parameter)))
    }
}

/// One rule for read sessions.
#[allow(dead_code, reason = "used by the Storage Read RPCs")]
pub(super) struct ReadRule {
    pub table: TableKey,
    /// `None` matches a session without a row restriction.
    pub row_restriction: Option<String>,
    pub rows: RecordBatch,
    pub rule: BigQueryFakeRule,
}

/// One fault rule.
pub(super) struct FaultRule {
    pub rpc: BigQueryFakeRpc,
    /// `None` matches every call of `rpc`.
    pub table: Option<TableKey>,
    pub fault: BigQueryFakeFault,
    pub rule: BigQueryFakeRule,
}

/// The indexes, within an appended batch, of the rows a `reject_rows` predicate refuses, or
/// the error decoding them into the test's type.
pub(super) type RowRejection =
    Box<dyn Fn(&RecordBatch) -> BigQueryResult<Vec<usize>> + Send + Sync>;

/// One `reject_rows` rule.
#[allow(dead_code, reason = "used by the Storage Write RPCs")]
pub(super) struct RejectRule {
    pub table: TableKey,
    /// The message of each row error.
    pub reason: String,
    pub rejects: RowRejection,
    pub rule: BigQueryFakeRule,
}

/// Every rule a test registered, in registration order per kind.
#[derive(Default)]
pub(super) struct FakeRules {
    queries: Vec<QueryRule>,
    reads: Vec<ReadRule>,
    faults: Vec<FaultRule>,
    rejections: Vec<Arc<RejectRule>>,
}

impl FakeRules {
    pub(super) fn add_query(&mut self, rule: QueryRule) {
        self.queries.push(rule);
    }

    pub(super) fn add_read(&mut self, rule: ReadRule) {
        self.reads.push(rule);
    }

    pub(super) fn add_fault(&mut self, rule: FaultRule) {
        self.faults.push(rule);
    }

    pub(super) fn add_rejection(&mut self, rule: RejectRule) {
        self.rejections.push(Arc::new(rule));
    }

    /// The reply of the first query rule with calls left that matches, counting the call.
    ///
    /// # Errors
    /// An internal failure of the fake, such as a panicking `query_matching` predicate.
    pub(super) fn answer_query(
        &self,
        sql: &str,
        parameters: &[QueryParameter],
    ) -> Result<Option<Arc<QueryReply>>, String> {
        for rule in &self.queries {
            if rule.matches(sql, parameters)? && rule.rule.claim() {
                return Ok(Some(rule.reply.clone()));
            }
        }
        Ok(None)
    }

    /// The rows of the first read rule with calls left for `table` and exactly
    /// `row_restriction`, counting the call.
    #[allow(dead_code, reason = "used by the Storage Read RPCs")]
    pub(super) fn answer_read(
        &self,
        table: &TableKey,
        row_restriction: Option<&str>,
    ) -> Option<RecordBatch> {
        self.reads
            .iter()
            .find(|rule| {
                rule.table == *table
                    && rule.row_restriction.as_deref() == row_restriction
                    && rule.rule.claim()
            })
            .map(|rule| rule.rows.clone())
    }

    /// The fault of the first fault rule with calls left for `rpc` on `table`, counting the
    /// call. Faults are tried before any other rule and before state.
    pub(super) fn fault(
        &self,
        rpc: BigQueryFakeRpc,
        table: Option<&TableKey>,
    ) -> Option<BigQueryFakeFault> {
        self.faults
            .iter()
            .find(|rule| {
                rule.rpc == rpc
                    && rule.table.as_ref().is_none_or(|only| Some(only) == table)
                    && rule.rule.claim()
            })
            .map(|rule| rule.fault.clone())
    }

    /// The `reject_rows` rules of `table`, in registration order, to apply outside the lock.
    #[allow(dead_code, reason = "used by the Storage Write RPCs")]
    pub(super) fn rejections(&self, table: &TableKey) -> Vec<Arc<RejectRule>> {
        self.rejections
            .iter()
            .filter(|rule| rule.table == *table)
            .cloned()
            .collect()
    }

    /// The query rules, as the unmatched report lists them.
    pub(super) fn describe_queries(&self) -> Vec<String> {
        Self::describe(self.queries.iter().map(|rule| &rule.rule))
    }

    /// The read rules, as the unmatched report lists them.
    pub(super) fn describe_reads(&self) -> Vec<String> {
        Self::describe(self.reads.iter().map(|rule| &rule.rule))
    }

    fn describe<'r>(rules: impl Iterator<Item = &'r BigQueryFakeRule>) -> Vec<String> {
        rules.map(BigQueryFakeRule::listed).collect()
    }

    /// Every `times(n)` rule that did not answer exactly n calls.
    pub(super) fn shortfalls(&self) -> Vec<String> {
        let queries = self.queries.iter().map(|rule| &rule.rule);
        let reads = self.reads.iter().map(|rule| &rule.rule);
        let faults = self.faults.iter().map(|rule| &rule.rule);
        queries
            .chain(reads)
            .chain(faults)
            .filter_map(BigQueryFakeRule::shortfall)
            .collect()
    }
}

/// Query parameters as the unmatched report shows them: `@name = value` or `?n = value`, with
/// arrays in brackets and structs in braces.
pub(super) struct ShownParameters<'p>(pub &'p [QueryParameter]);

impl Display for ShownParameters<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.0.is_empty() {
            return f.write_str("no parameters");
        }
        for (index, parameter) in self.0.iter().enumerate() {
            if index > 0 {
                f.write_str(", ")?;
            }
            if parameter.name.is_empty() {
                write!(f, "?{} = ", index + 1)?;
            } else {
                write!(f, "@{} = ", parameter.name)?;
            }
            match &parameter.parameter_value {
                Some(value) => ShownValue(value).fmt(f)?,
                None => f.write_str("NULL")?,
            }
        }
        Ok(())
    }
}

struct ShownValue<'v>(&'v QueryParameterValue);

impl Display for ShownValue<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let value = self.0;
        if let Some(scalar) = &value.value {
            return write!(f, "{scalar:?}");
        }
        if !value.array_values.is_empty() {
            f.write_str("[")?;
            for (index, element) in value.array_values.iter().enumerate() {
                if index > 0 {
                    f.write_str(", ")?;
                }
                ShownValue(element).fmt(f)?;
            }
            return f.write_str("]");
        }
        if !value.struct_values.is_empty() {
            let mut fields: Vec<_> = value.struct_values.iter().collect();
            fields.sort_by_key(|(name, _)| *name);
            f.write_str("{")?;
            for (index, (name, field)) in fields.into_iter().enumerate() {
                if index > 0 {
                    f.write_str(", ")?;
                }
                write!(f, "{name}: ")?;
                ShownValue(field).fmt(f)?;
            }
            return f.write_str("}");
        }
        if let Some(range) = &value.range_value {
            let bound = |bound: &Option<Box<QueryParameterValue>>| {
                bound.as_deref().map_or_else(
                    || "UNBOUNDED".to_string(),
                    |bound| ShownValue(bound).to_string(),
                )
            };
            return write!(f, "[{}, {})", bound(&range.start), bound(&range.end));
        }
        f.write_str("NULL")
    }
}
