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

use crate::catalog::{Change, EngineCatalog, LoadedTable, PointerVersion};
use crate::error::{EngineError, Resource};
use crate::ids::TableRef;
use crate::write::WrittenFiles;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;
use tokio::sync::oneshot;
use tracing::debug;

/// What an append's commit made of the table.
#[derive(Debug, Clone)]
pub(crate) struct Appended {
    pub pointer_version: PointerVersion,
    pub snapshot_id: Option<i64>,
}

impl From<&LoadedTable> for Appended {
    fn from(table: &LoadedTable) -> Self {
        Self {
            pointer_version: table.version(),
            snapshot_id: table.metadata().current_snapshot_id(),
        }
    }
}

/// One request waiting in its table's queue.
struct Member {
    files: WrittenFiles,
    answer: oneshot::Sender<Result<Appended, EngineError>>,
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

    /// Lands `files` in `table` with whatever else is queued for it, and answers once the
    /// commit holding them has landed or failed.
    ///
    /// # Errors
    /// [`EngineError::NotFound`] when the table was dropped or replaced since the files were
    /// written, and the errors of the catalog's commit loop.
    pub(crate) async fn append(
        self: &Arc<Self>,
        table: &TableRef,
        files: WrittenFiles,
    ) -> Result<Appended, EngineError> {
        let (answer, answered) = oneshot::channel();
        let starts_commit = {
            let mut queues = self.queues.lock().unwrap_or_else(PoisonError::into_inner);
            let member = Member { files, answer };
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
            tokio::spawn(async move { groups.drain(table).await });
        }
        answered.await.map_err(|_| EngineError::Internal {
            detail: format!("the commit task of {table} ended without answering"),
        })?
    }

    /// Commits `table`'s queue group by group until it is empty.
    async fn drain(&self, table: TableRef) {
        loop {
            if !self.window.is_zero() {
                tokio::time::sleep(self.window).await;
            }
            let group = {
                let mut queues = self.queues.lock().unwrap_or_else(PoisonError::into_inner);
                match queues.get_mut(&table) {
                    Some(queue) if !queue.is_empty() => std::mem::take(queue),
                    _ => {
                        queues.remove(&table);
                        return;
                    }
                }
            };
            debug!(%table, requests = group.len(), "Committing a group of appends.");
            let outcomes = self.commit(&table, &group).await;
            for (member, outcome) in group.into_iter().zip(outcomes) {
                // A member whose request went away has nobody to answer.
                let _ = member.answer.send(outcome);
            }
        }
    }

    /// Lands the files of every member written for the table identity under the name in one
    /// `fast_append`, and returns each member's outcome, in order.
    async fn commit(
        &self,
        table: &TableRef,
        group: &[Member],
    ) -> Vec<Result<Appended, EngineError>> {
        let mut included = vec![false; group.len()];
        let landed = self
            .catalog
            .commit(table, |loaded| {
                let identity = loaded.metadata().uuid();
                for (member, include) in group.iter().zip(included.iter_mut()) {
                    *include = member.files.table_uuid == identity;
                }
                if !included.contains(&true) {
                    return Err(EngineError::NotFound {
                        what: Resource::Table(table.clone()),
                    });
                }
                let files = group
                    .iter()
                    .zip(&included)
                    .filter(|(_, include)| **include)
                    .flat_map(|(member, _)| member.files.files.iter().cloned());
                let transaction = Transaction::new(&loaded.table);
                let append = transaction
                    .fast_append()
                    .with_check_duplicate(false)
                    .add_data_files(files);
                Ok(Change::Transaction(append.apply(transaction)?))
            })
            .await;
        included
            .into_iter()
            .map(|include| match (&landed, include) {
                (Ok(loaded), true) => Ok(Appended::from(loaded)),
                (Err(err), true) => Err(err.for_member(table)),
                (_, false) => Err(EngineError::NotFound {
                    what: Resource::Table(table.clone()),
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

impl EngineError {
    /// This error of a group's commit as one member's answer: the variants a member branches on
    /// are kept, and an error that only one value can carry is described again.
    fn for_member(&self, table: &TableRef) -> EngineError {
        match self {
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
            other => EngineError::Internal {
                detail: format!("the commit of {table} failed: {other}"),
            },
        }
    }
}
