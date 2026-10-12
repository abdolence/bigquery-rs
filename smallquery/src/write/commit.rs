//! Group commit: the appends of one table that arrive while a commit of the table runs form the
//! next commit.
//!
//! A request writes its data files without any lock, then joins its table's queue. When no
//! commit of the table is running, its arrival starts one: a task that drains the queue, waits
//! `commit_window` for more, lands every queued request's files in one `fast_append` through the
//! catalog's commit loop, answers every member in queue order, and goes again until the queue is
//! empty. Under no contention a group is one request and its latency one commit; under load the
//! commit rate is bounded by the commit's own latency, not by the request rate.
//!
//! A request's files are in exactly one group, so a request lands whole or not at all, and a
//! failed group fails every member with its error: `Aborted` past the retry deadline, which the
//! client retries, or an unknown outcome, which nobody retries.
//!
//! A change of a write stream's state is a member too, with the files a flush or a batch commit
//! adds: the change is checked on the table the commit loaded, after the changes of the members
//! before it, and lands in the same commit as the files, or the member is refused and its files
//! left out.

use crate::catalog::{Change, EngineCatalog, EngineCommit};
use crate::error::{EngineError, Resource, StreamProblem};
use crate::ids::{StreamId, TableRef, WriteStreamId, WriteStreamName};
use crate::storage::{StoredStream, StreamChange};
use crate::write::{WriteDisposition, WrittenFiles};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;
use tokio::sync::oneshot;
use tracing::debug;

/// One request waiting in its table's queue.
struct Member {
    files: WrittenFiles,
    disposition: WriteDisposition,
    streams: Vec<StreamChange>,
    answer: oneshot::Sender<Result<(), EngineError>>,
}

/// What a group's commit made of one member.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Verdict {
    /// The commit failed before it looked at the table, and the member shares its error.
    Undecided,
    /// The member's files are in the commit.
    Included,
    /// The table under the name is not the identity the files were written for.
    Replaced,
    /// The member writes only into an empty table, and the table or an earlier member of the
    /// group holds rows.
    NotEmpty,
    /// A write stream of the member is not in the committed state its change starts from.
    Refused {
        stream: WriteStreamId,
        problem: StreamProblem,
    },
}

/// The commit queues of every table with a commit running. A table has an entry exactly while
/// its commit task runs; the task removes it when it finds the queue empty, under the same lock
/// a request joins under, so no request is ever left in a queue nobody drains.
pub(crate) struct CommitGroups {
    catalog: Arc<EngineCatalog>,
    window: Duration,
    queues: Mutex<HashMap<TableRef, Vec<Member>>>,
    /// Every request that ever joined a queue, for tests that wait for all of them.
    #[cfg(test)]
    joined: std::sync::atomic::AtomicUsize,
}

impl std::fmt::Debug for CommitGroups {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommitGroups")
            .field("window", &self.window)
            .finish_non_exhaustive()
    }
}

/// A running commit task's hold on its table's queue. A task that ends any other way than by
/// finding its queue empty, a panic above all, drops it still armed: the queue goes, and every
/// request in it is failed, so that the next request starts a commit task of its own.
struct Draining<'g> {
    groups: &'g CommitGroups,
    table: &'g TableRef,
    armed: bool,
}

impl Drop for Draining<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let queue = self
            .groups
            .queues
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(self.table)
            .unwrap_or_default();
        for member in queue {
            let _ = member.answer.send(Err(EngineError::Internal {
                detail: format!(
                    "the commit task of {} ended before this request's commit",
                    self.table
                ),
            }));
        }
    }
}

impl CommitGroups {
    pub(crate) fn new(catalog: Arc<EngineCatalog>, window: Duration) -> Self {
        Self {
            catalog,
            window,
            queues: Mutex::default(),
            #[cfg(test)]
            joined: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Lands `files` in `table` with whatever else is queued for it, under `disposition`, and
    /// answers once the commit holding them has landed or failed. Under
    /// [`WriteDisposition::Empty`] the table is checked inside the commit, so of two requests
    /// into one empty table only the first lands.
    ///
    /// # Errors
    /// [`EngineError::NotFound`] when the table was dropped or replaced since the files were
    /// written, [`EngineError::AlreadyExists`] for [`WriteDisposition::Empty`] into a table
    /// with rows, and the errors of the catalog's commit loop.
    pub(crate) async fn append(
        self: &Arc<Self>,
        table: &TableRef,
        files: WrittenFiles,
        disposition: WriteDisposition,
    ) -> Result<(), EngineError> {
        self.join(table, files, disposition, Vec::new()).await
    }

    /// Lands `files` in `table` together with the changes `streams` make of their write
    /// streams' committed state, all of it or none.
    ///
    /// # Errors
    /// [`EngineError::WriteStream`] for a stream whose committed state in the table is not the
    /// one its change starts from, and the errors of [`CommitGroups::append`].
    pub(crate) async fn commit_streams(
        self: &Arc<Self>,
        table: &TableRef,
        files: WrittenFiles,
        streams: Vec<StreamChange>,
    ) -> Result<(), EngineError> {
        self.join(table, files, WriteDisposition::Append, streams)
            .await
    }

    async fn join(
        self: &Arc<Self>,
        table: &TableRef,
        files: WrittenFiles,
        disposition: WriteDisposition,
        streams: Vec<StreamChange>,
    ) -> Result<(), EngineError> {
        let (answer, answered) = oneshot::channel();
        let starts_commit = {
            let mut queues = self.queues.lock().unwrap_or_else(PoisonError::into_inner);
            let member = Member {
                files,
                disposition,
                streams,
                answer,
            };
            match queues.get_mut(table) {
                Some(queue) => {
                    queue.push(member);
                    false
                }
                None => {
                    queues.insert(table.clone(), vec![member]);
                    true
                }
            }
        };
        #[cfg(test)]
        self.joined
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if starts_commit {
            let groups = Arc::clone(self);
            let table = table.clone();
            tokio::spawn(async move { groups.drain(&table).await });
        }
        answered.await.map_err(|_| EngineError::Internal {
            detail: format!("the commit task of {table} ended without answering"),
        })?
    }

    /// Commits `table`'s queue group by group until it is empty.
    async fn drain(&self, table: &TableRef) {
        let mut draining = Draining {
            groups: self,
            table,
            armed: true,
        };
        loop {
            if !self.window.is_zero() {
                tokio::time::sleep(self.window).await;
            }
            let group = {
                let mut queues = self.queues.lock().unwrap_or_else(PoisonError::into_inner);
                match queues.get_mut(table) {
                    Some(queue) if !queue.is_empty() => std::mem::take(queue),
                    _ => {
                        queues.remove(table);
                        draining.armed = false;
                        return;
                    }
                }
            };
            debug!(%table, requests = group.len(), "Committing a group of appends.");
            let outcomes = self.commit(table, &group).await;
            for (member, outcome) in group.into_iter().zip(outcomes) {
                // A member whose request went away has nobody to answer.
                let _ = member.answer.send(outcome);
            }
        }
    }

    /// Lands, in one `fast_append`, the files of every member written for the table identity
    /// under the name whose disposition the table allows and whose stream changes hold, with
    /// those changes, and returns each member's outcome, in order.
    async fn commit(&self, table: &TableRef, group: &[Member]) -> Vec<Result<(), EngineError>> {
        let mut verdicts = vec![Verdict::Undecided; group.len()];
        let landed = self
            .catalog
            .commit(table, |loaded| {
                // Members are decided in queue order; a table whose snapshot does not report
                // its rows is not known to be empty.
                let identity = loaded.metadata().uuid();
                let mut holds_rows = group
                    .iter()
                    .any(|member| member.disposition == WriteDisposition::Empty)
                    && loaded.total_rows()? != Some(0);
                let mut streams = HashMap::new();
                for (member, verdict) in group.iter().zip(verdicts.iter_mut()) {
                    *verdict = if member.files.table_uuid != identity {
                        Verdict::Replaced
                    } else if member.disposition == WriteDisposition::Empty && holds_rows {
                        Verdict::NotEmpty
                    } else {
                        match member.stream_states(loaded.metadata().properties(), &streams) {
                            Ok(changed) => {
                                streams.extend(changed);
                                holds_rows |= member.files.rows > 0;
                                Verdict::Included
                            }
                            Err(refused) => refused,
                        }
                    };
                }
                if !verdicts.contains(&Verdict::Included) {
                    // Every member has its own answer; this error reaches none of them.
                    return Err(EngineError::NotFound {
                        what: Resource::Table(table.clone()),
                    });
                }
                let mut files = group
                    .iter()
                    .zip(&verdicts)
                    .filter(|(_, verdict)| **verdict == Verdict::Included)
                    .flat_map(|(member, _)| member.files.files.iter().cloned())
                    .peekable();
                let properties: Vec<_> = streams
                    .into_iter()
                    .flat_map(|(stream, state)| state.properties(stream))
                    .collect();
                if files.peek().is_none() && properties.is_empty() {
                    return Ok(Change::Engine(EngineCommit::default()));
                }
                let mut transaction = Transaction::new(&loaded.table);
                if !properties.is_empty() {
                    transaction = properties
                        .into_iter()
                        .fold(
                            transaction.update_table_properties(),
                            |action, (key, value)| action.set(key, value),
                        )
                        .apply(transaction)?;
                }
                if files.peek().is_some() {
                    transaction = transaction
                        .fast_append()
                        .with_check_duplicate(false)
                        .add_data_files(files)
                        .apply(transaction)?;
                }
                Ok(Change::Transaction(transaction))
            })
            .await;
        verdicts
            .into_iter()
            .map(|verdict| match (verdict, &landed) {
                (Verdict::Included, Ok(_)) => Ok(()),
                (Verdict::Included | Verdict::Undecided, Err(err)) => Err(err.for_member(table)),
                (Verdict::Replaced, _) => Err(EngineError::NotFound {
                    what: Resource::Table(table.clone()),
                }),
                (Verdict::NotEmpty, _) => Err(EngineError::AlreadyExists {
                    what: Resource::Table(table.clone()),
                }),
                (Verdict::Refused { stream, problem }, _) => Err(EngineError::stream(
                    &WriteStreamName {
                        table: table.clone(),
                        stream: StreamId::Created(stream),
                    },
                    problem,
                )),
                (Verdict::Undecided, Ok(_)) => Err(EngineError::Internal {
                    detail: format!("the commit of {table} landed without deciding a request"),
                }),
            })
            .collect()
    }

    /// The requests that ever joined a queue.
    #[cfg(test)]
    pub(crate) fn joined(&self) -> usize {
        self.joined.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// The tables with a commit running.
    #[cfg(test)]
    pub(crate) fn running(&self) -> usize {
        self.queues
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }
}

impl Member {
    /// The states the member's stream changes leave, given the table's `stored` properties and
    /// the states `changed` by earlier members of its group.
    fn stream_states(
        &self,
        stored: &HashMap<String, String>,
        changed: &HashMap<WriteStreamId, StoredStream>,
    ) -> Result<Vec<(WriteStreamId, StoredStream)>, Verdict> {
        self.streams
            .iter()
            .map(|change| {
                let stream = change.stream();
                let refused = |problem| Verdict::Refused { stream, problem };
                let current = match changed.get(&stream) {
                    Some(state) => Some(*state),
                    None => StoredStream::read(stream, stored)
                        .map_err(|_| refused(StreamProblem::ChangedElsewhere))?,
                };
                Ok((stream, change.apply(current).map_err(refused)?))
            })
            .collect()
    }
}

impl EngineError {
    /// This error of a group's commit as one member's answer: the variants a member branches on
    /// are kept, and an error that only one value can carry is described again.
    fn for_member(&self, table: &TableRef) -> EngineError {
        match self {
            EngineError::Iceberg(error) => {
                EngineError::Iceberg(iceberg::Error::new(error.kind(), error.message()))
            }
            EngineError::Storage {
                backend,
                operation,
                detail,
            } => EngineError::Storage {
                backend: *backend,
                operation,
                detail: detail.clone(),
            },
            EngineError::NotFound { what } => EngineError::NotFound { what: what.clone() },
            EngineError::Aborted { what, attempts } => EngineError::Aborted {
                what: what.clone(),
                attempts: *attempts,
            },
            EngineError::CommitOutcomeUnknown {
                what,
                pointer_version,
                metadata_location,
            } => EngineError::CommitOutcomeUnknown {
                what: what.clone(),
                pointer_version: *pointer_version,
                metadata_location: metadata_location.clone(),
            },
            EngineError::Precondition { what, detail } => EngineError::Precondition {
                what: what.clone(),
                detail: detail.clone(),
            },
            EngineError::MissingMetadata {
                what,
                pointer_version,
                metadata_location,
            } => EngineError::MissingMetadata {
                what: what.clone(),
                pointer_version: *pointer_version,
                metadata_location: metadata_location.clone(),
            },
            other => EngineError::Internal {
                detail: format!("the commit of {table} failed: {other}"),
            },
        }
    }
}
