//! Starting and stopping the fake, sending each call to the area that serves it, and the
//! report of what went wrong.

use crate::db::fake::{FakeCall, FakeServer};
use crate::db::RetryBackoff;
use crate::errors::BigQueryError;
use crate::testing::rules::{BigQueryFakeFault, BigQueryFakeRpc, FakeRules};
use crate::testing::state::{FakeState, TableKey};
use crate::testing::BigQueryFake;
use crate::{BigQueryDbOptions, BigQueryResult};
use gcloud_sdk::prost::Message;
use gcloud_sdk::tonic::{Code, Status};
use std::fmt::{Display, Formatter};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

/// The project of [`BigQueryFake::start`].
const FAKE_PROJECT: &str = "fake-project";

/// What the server's calls share with the [`BigQueryFake`] that owns them. Each lock is held
/// only between two `.await`s, and never together with another.
pub(super) struct FakeShared {
    /// The client's project, which table keys resolve against.
    project: String,
    state: Mutex<FakeState>,
    rules: Mutex<FakeRules>,
    problems: Mutex<Vec<FakeProblem>>,
}

/// Something `verify` reports.
#[derive(Debug)]
enum FakeProblem {
    /// A call no rule or state could answer.
    Unmatched(String),
    /// A failure of the fake itself.
    Internal(String),
}

/// Why a unary call is not answered with its response.
pub(super) enum FakeRefusal {
    /// What BigQuery answers.
    Status(Status),
    /// A failure of the fake itself, answered with `Internal` and reported by `verify`.
    Internal(String),
}

impl From<Status> for FakeRefusal {
    fn from(status: Status) -> Self {
        Self::Status(status)
    }
}

impl FakeShared {
    fn new(project: String) -> Self {
        Self {
            project,
            state: Mutex::default(),
            rules: Mutex::default(),
            problems: Mutex::default(),
        }
    }

    /// The client's project.
    pub(super) fn project(&self) -> &str {
        &self.project
    }

    /// The state. A poisoned lock is taken as it is: the panic that poisoned it fails the test
    /// by itself, and `verify` must still be able to report from it.
    pub(super) fn state(&self) -> MutexGuard<'_, FakeState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The rules, taken as [`state`](Self::state) is.
    pub(super) fn rules(&self) -> MutexGuard<'_, FakeRules> {
        self.rules.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn problems(&self) -> MutexGuard<'_, Vec<FakeProblem>> {
        self.problems.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The call's first request message. A call without one, or with one that is not an `M`,
    /// is answered as unmatched, and `None` is returned.
    pub(super) async fn first_request<M: Message + Default>(
        &self,
        mut call: FakeCall,
    ) -> Option<(FakeCall, M)> {
        match call.try_next_request::<M>().await {
            Ok(Some(request)) => Some((call, request)),
            Ok(None) => {
                let described = format!("{} without a request message", call.method());
                self.unmatched(call, &described, &[]);
                None
            }
            Err(error) => {
                let described = format!("{} whose request does not decode: {error}", call.method());
                self.unmatched(call, &described, &[]);
                None
            }
        }
    }

    /// The call back unless a fault of `rpc` on `table` answered it. Faults answer before
    /// every other rule and before the state.
    pub(super) async fn unfaulted(
        &self,
        call: FakeCall,
        rpc: BigQueryFakeRpc,
        table: Option<&TableKey>,
    ) -> Option<FakeCall> {
        let fault = self.rules().answer_fault(rpc, table);
        match fault {
            Some(fault) => {
                fault.answer(call).await;
                None
            }
            None => Some(call),
        }
    }

    /// Answers a call no rule or state answers with `Unimplemented`, which the client does not
    /// retry, and records it for `verify`. `described` is the call, and `rules` the rules
    /// that could have answered it.
    pub(super) fn unmatched(&self, call: FakeCall, described: &str, rules: &[String]) {
        let rules = if rules.is_empty() {
            "none".to_string()
        } else {
            rules.join("; ")
        };
        let problem = format!("no rule answers {described}; rules: {rules}");
        let message = format!("bigquery fake: {problem}");
        // Recorded before the answer, so a test that verifies as soon as its call returns sees it.
        self.problems().push(FakeProblem::Unmatched(problem));
        call.fail(Code::Unimplemented, &message);
    }

    /// Answers a call the fake failed to serve with `Internal` and records the failure for
    /// `verify`.
    pub(super) fn internal(&self, call: FakeCall, failure: &str) {
        self.problems()
            .push(FakeProblem::Internal(failure.to_string()));
        call.fail(Code::Internal, &format!("bigquery fake: {failure}"));
    }

    /// Answers a unary `call` with `answer`.
    pub(super) fn answer<M: Message>(&self, call: FakeCall, answer: Result<M, FakeRefusal>) {
        match answer {
            Ok(message) => call.reply(&message),
            Err(FakeRefusal::Status(status)) => call.fail(status.code(), status.message()),
            Err(FakeRefusal::Internal(failure)) => self.internal(call, &failure),
        }
    }

    /// Every problem `verify` reports, empty when there is none.
    fn report(&self) -> FakeReport {
        let mut lines: Vec<String> = self
            .problems()
            .iter()
            .map(|problem| match problem {
                FakeProblem::Unmatched(problem) => problem.clone(),
                FakeProblem::Internal(failure) => format!("internal failure: {failure}"),
            })
            .collect();
        lines.extend(self.rules().shortfalls());
        FakeReport { lines }
    }
}

impl BigQueryFakeFault {
    /// Answers `call` with this fault.
    pub(super) async fn answer(self, call: FakeCall) {
        match self {
            BigQueryFakeFault::Status { code, message } => call.fail(code.grpc_code(), &message),
            BigQueryFakeFault::ConnectionDropped => call.drop_connection().await,
            BigQueryFakeFault::Hang => call.hang().await,
        }
    }
}

/// The problems `verify` reports, one per line.
struct FakeReport {
    lines: Vec<String>,
}

impl Display for FakeReport {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "bigquery fake: {} problem(s):", self.lines.len())?;
        for line in &self.lines {
            write!(f, "\n  - {line}")?;
        }
        Ok(())
    }
}

impl FakeShared {
    /// Sends `call` to the area that serves its RPC. An RPC no area serves is unmatched.
    async fn dispatch(self: Arc<Self>, call: FakeCall) {
        match call.method() {
            "Query" | "InsertJob" | "GetQueryResults" | "GetJob" | "CancelJob" => {
                self.serve_query(call).await;
            }
            "CreateReadSession" | "ReadRows" => self.serve_read(call).await,
            "GetWriteStream"
            | "CreateWriteStream"
            | "AppendRows"
            | "FlushRows"
            | "FinalizeWriteStream"
            | "BatchCommitWriteStreams" => self.serve_write(call).await,
            "GetTable" | "InsertTable" | "PatchTable" | "UpdateTable" | "DeleteTable"
            | "ListTables" | "GetDataset" | "InsertDataset" | "UpdateDataset" | "DeleteDataset"
            | "ListDatasets" => self.serve_admin(call).await,
            method => {
                let described = method.to_string();
                self.unmatched(call, &described, &[]);
            }
        }
    }
}

impl BigQueryFake {
    /// Starts a fake on a free loopback port, with a client for the project `fake-project`.
    ///
    /// # Errors
    /// [`BigQueryError::SystemError`] if no loopback port can be bound.
    pub async fn start() -> BigQueryResult<Self> {
        Self::start_with(BigQueryDbOptions::new(FAKE_PROJECT.to_string())).await
    }

    /// Starts a fake on a free loopback port, with a client that keeps the project, location
    /// and `max_retries` of `options` and connects both its endpoints to the fake.
    ///
    /// Retries wait no backoff, so a test that scripts failures does not wait out the delays
    /// meant for a real backend.
    ///
    /// # Errors
    /// [`BigQueryError::SystemError`] if no loopback port can be bound, and
    /// [`BigQueryError::InvalidParametersError`] for an invalid project.
    pub async fn start_with(options: BigQueryDbOptions) -> BigQueryResult<Self> {
        let shared = Arc::new(FakeShared::new(options.google_project_id.clone()));
        let served = shared.clone();
        let server = FakeServer::bind(move |call| served.clone().dispatch(call))
            .await
            .map_err(|err| {
                BigQueryError::system(
                    "FAKE_SERVER_UNAVAILABLE",
                    format!("the BigQuery fake cannot bind a loopback port: {err}"),
                )
            })?;
        let db = server.client(options, RetryBackoff::Immediate).await?;
        Ok(Self {
            db,
            shared,
            _server: server,
        })
    }

    /// Checks that the fake answered every call as scripted.
    ///
    /// `Drop` calls it, so a test needs it only to check at a point of its own choosing.
    ///
    /// # Panics
    /// With every call that no rule answered, every internal failure of the fake, and every
    /// rule limited with `times(n)` that did not answer exactly n calls.
    pub fn verify(&self) {
        let report = self.shared.report();
        if !report.lines.is_empty() {
            panic!("{report}");
        }
    }
}

/// Stops the server, and calls [`verify`](BigQueryFake::verify) unless the thread is already
/// panicking, when the problems are logged with `tracing::error!` instead, so that the first
/// panic is the one the test reports.
impl Drop for BigQueryFake {
    fn drop(&mut self) {
        if std::thread::panicking() {
            let report = self.shared.report();
            if !report.lines.is_empty() {
                tracing::error!(%report, "The BigQuery fake was dropped during a panic");
            }
        } else {
            self.verify();
        }
    }
}
