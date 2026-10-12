//! The one path every change to a table takes: load the newest pointer `v<N>`, apply the change
//! to the metadata it names, write the new metadata file, create `v<N+1>` only if absent, then
//! read `v<N>` back.
//!
//! The invariant: a commit reported as landed is the successor of the pointer it was built on,
//! so every load that starts after the report returns that commit's pointer or one created on
//! top of it; a lost commit is never reported as landed and a landed one is never sent again.
//! It rests on four rules: a create targets the newest pointer plus one; pointers are deleted
//! only bottom-up, each once the pointer above it aged `pointer_min_age`; a drop is a tombstone
//! pointer, so a name's numbering never restarts; and a commit is landed only after the
//! read-back. While `v<N>` is still the object the commit loaded, no name at or above `N` was
//! deleted when the create ran, so `v<N+1>` had never existed and the create beat every other
//! writer. Only when `v<N>` is gone does a clock decide: a commit that measured less than half of
//! `pointer_min_age` from its first create to the read-back cannot have landed and had `v<N>`
//! pruned since, since that takes `v<N+1>` aging `pointer_min_age`.

use super::hooks::CommitPoint;
use super::pointer::{LivePointer, PointerBody, PointerHead, PointerVersion, Tombstone};
use super::snapshot::BuiltSnapshot;
use super::{now_ms, EngineCatalog, LoadedTable, NameState};
use crate::error::{EngineError, Resource};
use crate::ids::TableRef;
use crate::storage::{Created, ListedKind, ObjectIdentity, ObjectPath};
use async_trait::async_trait;
use bytes::Bytes;
use iceberg::spec::{MetadataLog, TableMetadata};
use iceberg::table::Table;
use iceberg::transaction::Transaction;
use iceberg::{
    Catalog, MetadataLocation, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent,
    TableRequirement, TableUpdate,
};
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::future::Future;
use std::str::FromStr;
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant, SystemTime};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// A change the engine commits by itself, as the requirement and update pair an Iceberg
/// `TableCommit` holds, which has no public constructor.
#[derive(Debug, Clone, Default)]
pub(crate) struct EngineCommit {
    pub requirements: Vec<TableRequirement>,
    pub updates: Vec<TableUpdate>,
}

/// What one attempt of a commit applies to the table it loaded. The commit loop asks for it
/// again on every attempt, from the table that attempt loaded.
pub(crate) enum Change {
    /// An Iceberg transaction built on the loaded table; its actions run inside the attempt.
    Transaction(Transaction),
    /// A change the engine computed from the loaded table.
    Engine(EngineCommit),
    /// A snapshot the engine built on the loaded table.
    Snapshot(BuiltSnapshot),
}

/// What each attempt of a commit applies, built from the table that attempt loaded.
pub(crate) trait AttemptChange {
    fn for_attempt(
        &mut self,
        loaded: &LoadedTable,
    ) -> impl Future<Output = Result<Change, EngineError>> + Send;
}

impl<F: FnMut(&LoadedTable) -> Result<Change, EngineError>> AttemptChange for F {
    fn for_attempt(
        &mut self,
        loaded: &LoadedTable,
    ) -> impl Future<Output = Result<Change, EngineError>> + Send {
        std::future::ready(self(loaded))
    }
}

/// The first pointer of a table identity.
pub(super) enum NewPointer {
    /// Metadata the commit writes, then names.
    Write(TableMetadata),
    /// Metadata written elsewhere, which the pointer names as it is.
    Register {
        metadata: TableMetadata,
        location: String,
    },
}

/// The metadata an attempt makes current.
struct TableProposal {
    metadata: TableMetadata,
    location: String,
    /// Set when the attempt writes the metadata file itself.
    write: Option<MetadataLocation>,
    /// The metadata files that fell off the metadata log, deleted once the commit lands.
    expired: Vec<MetadataLog>,
}

/// How an attempt that did not fail ended.
#[derive(Debug)]
pub(super) enum Attempted<T> {
    Landed(T),
    Lost(Lost),
}

/// Why an attempt did not land.
#[derive(Debug)]
pub(super) enum Lost {
    /// Another commit created the pointer version first, and nothing names what the attempt
    /// wrote.
    Race,
    /// The attempt created its pointer in a name pruning had freed: the pointer, below the
    /// oldest live one, names the attempt's metadata until the pruning removes it.
    Stray,
    /// A requirement does not hold on the loaded table; the iceberg error says which.
    Unmet(String),
}

impl Lost {
    /// The error an Iceberg caller sees for an attempt on `table` that did not land: never
    /// retryable, so that `Transaction::commit` makes one attempt and the engine's commit loop
    /// decides what follows.
    fn conflict(&self, table: &TableRef) -> iceberg::Error {
        let detail = match self {
            Lost::Race => {
                format!("{table}: a concurrent commit created the next catalog pointer first")
            }
            Lost::Stray => {
                format!("{table}: the commit's catalog pointer went into a name pruning had freed")
            }
            Lost::Unmet(detail) => format!("{table}: {detail}"),
        };
        iceberg::Error::new(iceberg::ErrorKind::CatalogCommitConflicts, detail)
            .with_retryable(false)
    }
}

/// What a change makes of the loaded table.
enum Proposed {
    /// New metadata, to be committed.
    Metadata(Box<TableProposal>),
    /// Every requirement holds and nothing changes.
    Unchanged,
    /// A requirement does not hold on the loaded table.
    Unmet(String),
}

/// The pointer an attempt builds on.
#[derive(Clone, Copy)]
enum Base<'a> {
    Empty,
    Live(&'a PointerHead<LivePointer>),
    Dropped(&'a PointerHead<Tombstone>),
}

impl Base<'_> {
    /// The version of the pointer an attempt creates on this base.
    fn next_version(self) -> PointerVersion {
        match self {
            Base::Empty => PointerVersion::FIRST,
            Base::Live(head) => head.version.next(),
            Base::Dropped(head) => head.version.next(),
        }
    }

    /// The `previous` of a live pointer created on this base.
    fn previous(self) -> Option<String> {
        match self {
            Base::Live(head) => Some(head.body.metadata_location.clone()),
            Base::Empty | Base::Dropped(_) => None,
        }
    }
}

/// A pointer an attempt created and read back as landed.
struct Placement {
    version: PointerVersion,
    identity: ObjectIdentity,
}

/// The outcome of the create of an attempt's pointer.
enum Placed {
    /// The create reached the backend; the object has this identity.
    Ours(ObjectIdentity),
    /// Another commit created the pointer version first.
    Taken,
    /// A create's outcome was unknown and no read settled it, so it may still land.
    Unknown(EngineError),
}

/// What reading the predecessor back after a successful create showed.
enum ReadBack {
    Landed,
    Lost,
    /// The predecessor is gone or replaced; the clock tells a stale writer from a stall.
    Gone,
}

/// What this process saw of other writers of one table.
///
/// A process that commits on top of its own commits builds from its cache, while another must
/// first load and parse the pointer the first created, so the first wins every race for as
/// long as it has commits to make. While another writer was seen within the last
/// `commit_retry_deadline`, each commit therefore first waits a delay drawn up to its last
/// attempt's duration, leaving the other a window to land in.
#[derive(Debug, Clone, Copy)]
pub(super) struct Contention {
    seen: Instant,
    attempt: Duration,
}

/// The floor of a commit's measured duration, so that a fast commit never retries in a tight
/// loop.
const MIN_BASE: Duration = Duration::from_millis(10);

/// So that one slow attempt does not slow every later commit of the table by as much.
const MAX_WAY: Duration = Duration::from_secs(1);

/// How many times a pointer create whose outcome was unknown, and whose object is absent, is
/// sent again.
const CREATE_ATTEMPTS: u32 = 3;

/// The retry state of one commit: attempts are retried with full jitter, a delay drawn between
/// zero and `min(max(1 s, 4 × base), base × 2^attempt)`, where `base` is the first attempt's
/// duration, so that two writers that lose to each other do not collide again and a retry stays
/// within a few commits' time; past the deadline the commit fails with `Aborted`.
struct Retry<'t> {
    table: &'t TableRef,
    deadline: Duration,
    started: Instant,
    attempt: u32,
    attempt_started: Instant,
    base: Option<Duration>,
}

impl<'t> Retry<'t> {
    fn new(table: &'t TableRef, deadline: Duration) -> Self {
        let now = Instant::now();
        Self {
            table,
            deadline,
            started: now,
            attempt: 0,
            attempt_started: now,
            base: None,
        }
    }

    /// Starts the next attempt and returns its number, from 1.
    fn start(&mut self) -> u32 {
        self.attempt = self.attempt.saturating_add(1);
        self.attempt_started = Instant::now();
        self.attempt
    }

    /// Waits before the next attempt, after the current one lost.
    ///
    /// # Errors
    /// [`EngineError::Aborted`] once the deadline has passed.
    async fn lost(&mut self) -> Result<(), EngineError> {
        let attempt_started = self.attempt_started;
        let base = *self
            .base
            .get_or_insert_with(|| attempt_started.elapsed().max(MIN_BASE));
        let elapsed = self.started.elapsed();
        if elapsed >= self.deadline {
            return Err(EngineError::Aborted {
                what: Resource::Table(self.table.clone()),
                attempts: self.attempt,
            });
        }
        let delay = jitter(base, self.attempt).min(self.deadline - elapsed);
        info!(
            table = %self.table,
            attempt = self.attempt,
            delay_ms = delay.as_millis(),
            "A commit lost to a concurrent commit; retrying.",
        );
        tokio::time::sleep(delay).await;
        Ok(())
    }
}

impl EngineCatalog {
    /// Commits the change `change` builds from the loaded table, retrying on lost races.
    ///
    /// Every attempt loads the table again and asks `change` for a change built on it. The
    /// table's in-process lock is held across every attempt, so the process's other commits on
    /// the table queue behind this one. An attempt whose load finds another table identity
    /// under the name stops the commit: what it was built for is gone. A snapshot an attempt
    /// built is deleted again when the attempt loses before anything names it.
    ///
    /// # Errors
    /// [`EngineError::NotFound`] when the table does not exist or was replaced,
    /// [`EngineError::Precondition`] when a requirement of the change does not hold on the table
    /// as stored, [`EngineError::Aborted`] past `commit_retry_deadline`,
    /// [`EngineError::CommitOutcomeUnknown`], never retried, and whatever `change` returns.
    pub(crate) async fn commit(
        &self,
        table: &TableRef,
        mut change: impl AttemptChange,
    ) -> Result<LoadedTable, EngineError> {
        let _lock = self.locks.table(table).await;
        let contended = self.make_way(table).await;
        let cached = self.cached(table).map(|(loaded, _)| loaded.version());
        let mut retry = Retry::new(table, self.settings.commit_retry_deadline);
        let mut identity = None;
        loop {
            let attempt = retry.start();
            let started = Instant::now();
            let loaded = self.load_for_attempt(table, attempt, &mut identity).await?;
            let moved = attempt == 1 && cached.is_some_and(|version| version != loaded.version());
            // A retry is likely to find its pointer version taken again, and so is any attempt
            // while another writer is about.
            let recheck = contended || attempt > 1;
            let attempted = match change.for_attempt(&loaded).await? {
                Change::Engine(commit) => {
                    self.attempt_engine(table, &loaded, commit, recheck).await?
                }
                Change::Snapshot(built) => {
                    self.attempt_snapshot(table, &loaded, built, recheck)
                        .await?
                }
                Change::Transaction(transaction) => {
                    self.attempt_transaction(table, &loaded, transaction, recheck)
                        .await?
                }
            };
            self.attempted(
                table,
                started.elapsed(),
                moved || matches!(attempted, Attempted::Lost(Lost::Race | Lost::Stray)),
            );
            match attempted {
                Attempted::Landed(loaded) => return Ok(loaded),
                // A retry reloads from storage, and a requirement the stored table does not
                // meet is not met by a newer one either: the change was built for another.
                Attempted::Lost(Lost::Unmet(detail)) if attempt > 1 => {
                    return Err(EngineError::Precondition {
                        what: Resource::Table(table.clone()),
                        detail,
                    })
                }
                Attempted::Lost(_) => retry.lost().await?,
            }
        }
    }

    /// Returns whether another writer of the table was seen recently.
    async fn make_way(&self, table: &TableRef) -> bool {
        let attempt = {
            let mut contention = self
                .contention
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            match contention.get(table) {
                Some(known) if known.seen.elapsed() < self.settings.commit_retry_deadline => {
                    Some(known.attempt)
                }
                Some(_) => {
                    contention.remove(table);
                    None
                }
                None => None,
            }
        };
        let Some(attempt) = attempt else {
            return false;
        };
        tokio::time::sleep(uniform_up_to(attempt.min(MAX_WAY))).await;
        true
    }

    /// An attempt saw another writer when it found a commit it did not make under the pointer
    /// it had cached, or lost a race.
    fn attempted(&self, table: &TableRef, attempt: Duration, saw_another_writer: bool) {
        let mut contention = self
            .contention
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if saw_another_writer {
            let deadline = self.settings.commit_retry_deadline;
            contention.retain(|_, known| known.seen.elapsed() < deadline);
            contention.insert(
                table.clone(),
                Contention {
                    seen: Instant::now(),
                    attempt,
                },
            );
        } else if let Some(known) = contention.get_mut(table) {
            known.attempt = attempt;
        }
    }

    /// Creates the first pointer of a table identity, on an empty name or a tombstone.
    pub(super) async fn create_pointer(
        &self,
        table: &TableRef,
        new: NewPointer,
    ) -> Result<LoadedTable, EngineError> {
        let _lock = self.locks.table(table).await;
        let mut retry = Retry::new(table, self.settings.commit_retry_deadline);
        loop {
            retry.start();
            let state = self.load_name(table).await?;
            self.hooks.reach(CommitPoint::Loaded).await;
            let base = match &state {
                NameState::Live(_) => {
                    return Err(EngineError::AlreadyExists {
                        what: Resource::Table(table.clone()),
                    })
                }
                NameState::Dropped(head) => Base::Dropped(head),
                NameState::Empty => Base::Empty,
            };
            let proposal = match &new {
                NewPointer::Write(metadata) => {
                    let location =
                        MetadataLocation::new_with_metadata(metadata.location(), metadata);
                    TableProposal {
                        metadata: metadata.clone(),
                        location: location.to_string(),
                        write: Some(location),
                        expired: Vec::new(),
                    }
                }
                NewPointer::Register { metadata, location } => TableProposal {
                    metadata: metadata.clone(),
                    location: location.clone(),
                    write: None,
                    expired: Vec::new(),
                },
            };
            match self.attempt_metadata(table, base, proposal, false).await? {
                Attempted::Landed(loaded) => return Ok(loaded),
                Attempted::Lost(_) => retry.lost().await?,
            }
        }
    }

    /// Drops the table with a tombstone pointer, and returns the table as it was dropped.
    pub(crate) async fn drop_table(&self, table: &TableRef) -> Result<LoadedTable, EngineError> {
        let _lock = self.locks.table(table).await;
        let mut retry = Retry::new(table, self.settings.commit_retry_deadline);
        let mut identity = None;
        loop {
            let attempt = retry.start();
            let loaded = self.load_for_attempt(table, attempt, &mut identity).await?;
            match self.attempt_drop(table, &loaded).await? {
                Attempted::Landed(()) => return Ok(loaded),
                Attempted::Lost(_) => retry.lost().await?,
            }
        }
    }

    /// One attempt of an Iceberg `TableCommit`, under the table lock, with no retry: a lost race
    /// is a `CatalogCommitConflicts` that is not retryable.
    pub(super) async fn commit_once(
        &self,
        mut commit: TableCommit,
    ) -> Result<LoadedTable, EngineError> {
        let table = TableRef::try_from(commit.identifier())?;
        let _lock = self.locks.table(&table).await;
        let loaded = self.reload(&table).await?;
        let pair = EngineCommit {
            requirements: commit.take_requirements(),
            updates: commit.take_updates(),
        };
        match self.attempt_engine(&table, &loaded, pair, false).await? {
            Attempted::Landed(loaded) => Ok(loaded),
            Attempted::Lost(lost) => Err(lost.conflict(&table).into()),
        }
    }

    /// The table an attempt builds on: the first attempt may use the cache, a retry reads
    /// storage. The first attempt's table identity is kept, and a later one that differs means
    /// the table this commit was built for was dropped or replaced.
    async fn load_for_attempt(
        &self,
        table: &TableRef,
        attempt: u32,
        identity: &mut Option<Uuid>,
    ) -> Result<LoadedTable, EngineError> {
        let loaded = if attempt == 1 {
            self.load(table).await?
        } else {
            self.reload(table).await?
        };
        let uuid = loaded.metadata().uuid();
        if *identity.get_or_insert(uuid) != uuid {
            return Err(EngineError::NotFound {
                what: Resource::Table(table.clone()),
            });
        }
        self.hooks.reach(CommitPoint::Loaded).await;
        Ok(loaded)
    }

    /// Commits a snapshot built on `loaded`, deleting what it wrote when the attempt loses.
    ///
    /// A snapshot that changes nothing lands through no pointer, so nothing has proven that
    /// `loaded`, which may come from the cache, is still the newest pointer: the table is read
    /// again, and a newer pointer makes the attempt lose to the commit that created it.
    async fn attempt_snapshot(
        &self,
        table: &TableRef,
        loaded: &LoadedTable,
        built: BuiltSnapshot,
        recheck: bool,
    ) -> Result<Attempted<LoadedTable>, EngineError> {
        let unchanged = built.is_unchanged();
        let attempted = match self
            .attempt_engine(table, loaded, built.commit, recheck)
            .await?
        {
            Attempted::Landed(_)
                if unchanged && self.reload(table).await?.version() != loaded.version() =>
            {
                Attempted::Lost(Lost::Race)
            }
            attempted => attempted,
        };
        if let Attempted::Lost(Lost::Race | Lost::Unmet(_)) = attempted {
            self.discard(&built.written).await;
        }
        Ok(attempted)
    }

    async fn attempt_engine(
        &self,
        table: &TableRef,
        loaded: &LoadedTable,
        commit: EngineCommit,
        recheck: bool,
    ) -> Result<Attempted<LoadedTable>, EngineError> {
        Ok(match self.propose(loaded, commit)? {
            Proposed::Metadata(proposal) => {
                self.attempt_metadata(table, Base::Live(&loaded.head), *proposal, recheck)
                    .await?
            }
            Proposed::Unchanged => Attempted::Landed(loaded.clone()),
            Proposed::Unmet(detail) => Attempted::Lost(Lost::Unmet(detail)),
        })
    }

    /// What `commit` makes of the loaded table: its requirements are checked first, also when
    /// it updates nothing.
    fn propose(&self, loaded: &LoadedTable, commit: EngineCommit) -> Result<Proposed, EngineError> {
        for requirement in &commit.requirements {
            match requirement.check(Some(loaded.metadata())) {
                Ok(()) => {}
                Err(err) if err.kind() == iceberg::ErrorKind::CatalogCommitConflicts => {
                    debug!(%err, "A commit requirement failed on the loaded table.");
                    return Ok(Proposed::Unmet(err.to_string()));
                }
                Err(err) => return Err(err.into()),
            }
        }
        if commit.updates.is_empty() {
            return Ok(Proposed::Unchanged);
        }
        let current = loaded.metadata_location();
        let mut builder = loaded
            .metadata()
            .clone()
            .into_builder(Some(current.to_string()));
        for update in commit.updates {
            builder = update.apply(builder)?;
        }
        let built = builder.build()?;
        let location = MetadataLocation::from_str(current)?
            .with_next_version()
            .with_new_metadata(&built.metadata);
        Ok(Proposed::Metadata(Box::new(TableProposal {
            metadata: built.metadata,
            location: location.to_string(),
            write: Some(location),
            expired: built.expired_metadata_logs,
        })))
    }

    /// Runs `transaction`'s actions on the loaded table and commits what they produce as one
    /// attempt, rechecking its pointer version as [`EngineCatalog::attempt_metadata`] does.
    async fn attempt_transaction(
        &self,
        table: &TableRef,
        loaded: &LoadedTable,
        transaction: Transaction,
        recheck: bool,
    ) -> Result<Attempted<LoadedTable>, EngineError> {
        let scope = AttemptCatalog {
            catalog: self,
            table,
            loaded,
            recheck,
            outcome: Mutex::new(None),
        };
        let committed = transaction.commit(&scope).await;
        let outcome = scope
            .outcome
            .into_inner()
            .unwrap_or_else(PoisonError::into_inner);
        match (committed, outcome) {
            (_, Some(outcome)) => outcome,
            // The actions failed before anything reached the catalog.
            (Err(err), None) => Err(err.into()),
            // A transaction without actions commits nothing.
            (Ok(_), None) => Ok(Attempted::Landed(loaded.clone())),
        }
    }

    /// Makes `proposal` the table's metadata on top of `base`. With `recheck`, an attempt whose
    /// pointer version another commit created meanwhile is lost before it writes the metadata
    /// file, which it would otherwise write only to delete it again.
    async fn attempt_metadata(
        &self,
        table: &TableRef,
        base: Base<'_>,
        proposal: TableProposal,
        recheck: bool,
    ) -> Result<Attempted<LoadedTable>, EngineError> {
        if let (true, Base::Live(head)) = (recheck, base) {
            let successor = Self::pointer_dir(table).join(head.version.next().object_name());
            if self.backend.exists(&successor).await? {
                return Ok(Attempted::Lost(Lost::Race));
            }
        }
        if let Some(location) = &proposal.write {
            proposal.metadata.write_to(&self.file_io, location).await?;
        }
        let live = LivePointer {
            id: Uuid::new_v4(),
            metadata_location: proposal.location.clone(),
            previous: base.previous(),
            committed_at_ms: now_ms(),
        };
        let written = proposal.write.as_ref().map(|_| proposal.location.as_str());
        let observed = Instant::now();
        let placement = match self
            .place(table, base, PointerBody::Live(live.clone()), written)
            .await?
        {
            Attempted::Landed(placement) => placement,
            Attempted::Lost(lost) => return Ok(Attempted::Lost(lost)),
        };
        let version = placement.version;
        let head = PointerHead {
            version,
            identity: placement.identity,
            body: live,
        };
        let loaded = self
            .loaded_table(table, head, proposal.metadata)
            .map_err(|err| {
                EngineError::commit_outcome_unknown(table, version, &proposal.location, &err)
            })?;
        self.remember(table, &loaded, observed);
        info!(
            %table,
            pointer_version = version.number(),
            snapshot_id = loaded.metadata().current_snapshot_id(),
            "Committed.",
        );
        for expired in &proposal.expired {
            self.delete_location(&expired.metadata_file).await;
        }
        self.prune(table).await;
        Ok(Attempted::Landed(loaded))
    }

    /// Drops the loaded table with a tombstone on top of its pointer.
    async fn attempt_drop(
        &self,
        table: &TableRef,
        loaded: &LoadedTable,
    ) -> Result<Attempted<()>, EngineError> {
        let body = PointerBody::Tombstone(Tombstone {
            id: Uuid::new_v4(),
            previous: loaded.metadata_location().to_string(),
            dropped_at_ms: now_ms(),
        });
        let placement = match self
            .place(table, Base::Live(&loaded.head), body, None)
            .await?
        {
            Attempted::Landed(placement) => placement,
            Attempted::Lost(lost) => return Ok(Attempted::Lost(lost)),
        };
        self.forget(table);
        info!(%table, pointer_version = placement.version.number(), "Dropped the table.");
        self.prune(table).await;
        Ok(Attempted::Landed(()))
    }

    /// Creates `body` as the successor of `base` and reads `base` back. `written` is the
    /// metadata file this attempt wrote, deleted when another commit took the version, since
    /// nothing else can ever name it.
    ///
    /// # Errors
    /// Once the create may have reached the backend, every failure is
    /// [`EngineError::CommitOutcomeUnknown`], so that no caller sends a landed commit again.
    async fn place(
        &self,
        table: &TableRef,
        base: Base<'_>,
        body: PointerBody,
        written: Option<&str>,
    ) -> Result<Attempted<Placement>, EngineError> {
        let dir = Self::pointer_dir(table);
        let version = base.next_version();
        let path = dir.join(version.object_name());
        let sent = Instant::now();
        let identity = match self.create_pointer_object(&path, &body).await? {
            Placed::Ours(identity) => identity,
            Placed::Unknown(err) => {
                return Err(EngineError::commit_outcome_unknown(
                    table,
                    version,
                    body.metadata_file(),
                    &err,
                ))
            }
            Placed::Taken => {
                if let Some(location) = written {
                    self.delete_location(location).await;
                }
                info!(
                    %table,
                    pointer_version = version.number(),
                    "Another commit created the pointer version first.",
                );
                return Ok(Attempted::Lost(Lost::Race));
            }
        };
        self.hooks.reach(CommitPoint::Created).await;
        let read_back = match base {
            Base::Live(head) => self.read_back(&dir, head).await,
            Base::Dropped(head) => self.read_back(&dir, head).await,
            Base::Empty => self.read_back_first(&dir, &body).await,
        }
        .map_err(|err| {
            EngineError::commit_outcome_unknown(table, version, body.metadata_file(), &err)
        })?;
        let stalled = sent.elapsed();
        match read_back {
            ReadBack::Landed => Ok(Attempted::Landed(Placement { version, identity })),
            ReadBack::Gone if stalled >= self.settings.pointer_min_age / 2 => {
                Err(EngineError::commit_outcome_unknown(
                    table,
                    version,
                    body.metadata_file(),
                    &format_args!(
                        "the commit stalled {} ms, past half of pointer_min_age, and its \
                         predecessor was pruned",
                        stalled.as_millis()
                    ),
                ))
            }
            ReadBack::Gone | ReadBack::Lost => {
                warn!(
                    %table,
                    pointer_version = version.number(),
                    "A commit's pointer went into a name pruning had freed; it is lost.",
                );
                Ok(Attempted::Lost(Lost::Stray))
            }
        }
    }

    /// Creates the pointer object; when the create says the name is taken or its outcome is
    /// unknown, reads the object back before anything is deleted, so that an ambiguous create
    /// that did land is never mistaken for a lost one.
    async fn create_pointer_object(
        &self,
        path: &ObjectPath,
        body: &PointerBody,
    ) -> Result<Placed, EngineError> {
        let bytes = Bytes::from(
            serde_json::to_vec(body)
                .map_err(|err| EngineError::invalid("catalog pointer", err.to_string()))?,
        );
        let mut unknown = None;
        for _ in 0..CREATE_ATTEMPTS {
            match self.backend.create(path, bytes.clone()).await {
                Ok(Created::Created(identity)) => return Ok(Placed::Ours(identity)),
                Ok(Created::AlreadyExists) => {}
                Err(err) => {
                    warn!(%err, "The outcome of a catalog pointer create is unknown.");
                    unknown = Some(err);
                }
            }
            let stored = match self.backend.read_object(path).await {
                Ok(stored) => stored,
                Err(err) if unknown.is_some() => return Ok(Placed::Unknown(err.into())),
                Err(err) => return Err(err.into()),
            };
            match stored {
                Some(object) => {
                    let ours = serde_json::from_slice::<PointerBody>(&object.bytes)
                        .is_ok_and(|stored| stored == *body);
                    return Ok(if ours {
                        Placed::Ours(object.identity)
                    } else {
                        Placed::Taken
                    });
                }
                None => continue,
            }
        }
        match unknown {
            Some(err) => Ok(Placed::Unknown(err.into())),
            None => Err(EngineError::Storage {
                backend: self.backend.kind(),
                operation: "create",
                detail: format!("{path}: the name was reported taken but holds no object"),
            }),
        }
    }

    /// The read-back of the pointer an attempt created its own on: landed while it is still the
    /// object the attempt loaded.
    async fn read_back<B: PartialEq + DeserializeOwned>(
        &self,
        dir: &ObjectPath,
        head: &PointerHead<B>,
    ) -> Result<ReadBack, EngineError> {
        Ok(
            match self
                .backend
                .read_object(&dir.join(head.version.object_name()))
                .await?
            {
                Some(object) if head.is(&object) => ReadBack::Landed,
                _ => ReadBack::Gone,
            },
        )
    }

    /// The read-back of a name's first pointer, which has no predecessor: `v0` alone, or a `v1`
    /// created on this commit's metadata (a commit or a drop), is landed; `v0` beside a higher
    /// pointer without such a `v1` is lost, since pointers leave bottom-up and a `v1` cannot go
    /// while `v0` stays.
    async fn read_back_first(
        &self,
        dir: &ObjectPath,
        body: &PointerBody,
    ) -> Result<ReadBack, EngineError> {
        let versions = self.pointer_versions(dir).await?;
        if !versions.contains(&PointerVersion::FIRST) {
            return Ok(ReadBack::Gone);
        }
        if versions
            .iter()
            .all(|version| *version == PointerVersion::FIRST)
        {
            return Ok(ReadBack::Landed);
        }
        let second = dir.join(PointerVersion::FIRST.next().object_name());
        let on_ours = match self.backend.read_object(&second).await? {
            Some(object) => match serde_json::from_slice::<PointerBody>(&object.bytes) {
                Ok(PointerBody::Live(live)) => live.previous.as_deref() == body.metadata_location(),
                Ok(PointerBody::Tombstone(tombstone)) => {
                    body.metadata_location() == Some(tombstone.previous.as_str())
                }
                Err(_) => false,
            },
            None => false,
        };
        Ok(if on_ours {
            ReadBack::Landed
        } else {
            ReadBack::Lost
        })
    }

    /// Deletes pointers bottom-up: the lowest goes while the pointer above it was created at
    /// least `pointer_min_age` before the newest, by the backend's own timestamps. A failure is
    /// logged, never returned, since the commit that called it has landed.
    async fn prune(&self, table: &TableRef) {
        if let Err(err) = self.try_prune(table).await {
            warn!(%table, %err, "Pruning the catalog pointers failed; the next commit retries.");
        }
    }

    async fn try_prune(&self, table: &TableRef) -> Result<(), EngineError> {
        let dir = Self::pointer_dir(table);
        let mut pointers: Vec<(PointerVersion, SystemTime)> = self
            .backend
            .list(&dir)
            .await?
            .into_iter()
            .filter_map(|listed| match listed.kind {
                ListedKind::Object { created } => {
                    PointerVersion::from_object_name(&listed.name).map(|version| (version, created))
                }
                ListedKind::Directory => None,
            })
            .collect();
        pointers.sort();
        let Some(&(_, newest)) = pointers.last() else {
            return Ok(());
        };
        let mut pruned = Vec::new();
        for pair in pointers.windows(2) {
            let [(lowest, _), (_, above)] = pair else {
                break;
            };
            let aged = above
                .checked_add(self.settings.pointer_min_age)
                .is_some_and(|aged| aged <= newest);
            if !aged {
                break;
            }
            self.backend.delete(&dir.join(lowest.object_name())).await?;
            pruned.push(lowest.number());
        }
        if !pruned.is_empty() {
            debug!(%table, ?pruned, "Pruned catalog pointers.");
        }
        Ok(())
    }

    /// Deletes the file at an Iceberg location, logging a failure: its callers delete files
    /// that nothing can name any more, which the orphan sweep removes otherwise.
    pub(super) async fn delete_location(&self, location: &str) {
        let deleted = match self.backend.object_path(location) {
            Ok(path) => self.backend.delete(&path).await,
            Err(err) => Err(err),
        };
        if let Err(err) = deleted {
            warn!(location, %err, "A metadata file nothing names could not be deleted.");
        }
    }
}

/// The delay before the retry after `attempt` lost, for a first attempt that took `base`.
fn jitter(base: Duration, attempt: u32) -> Duration {
    let ceiling = Duration::from_secs(1).max(base.saturating_mul(4));
    let grown = 2u32
        .checked_pow(attempt)
        .and_then(|factor| base.checked_mul(factor))
        .unwrap_or(ceiling);
    uniform_up_to(grown.min(ceiling))
}

fn uniform_up_to(cap: Duration) -> Duration {
    let micros = u64::try_from(cap.as_micros()).unwrap_or(u64::MAX);
    Duration::from_micros(rand::random_range(0..=micros))
}

impl EngineError {
    /// The commit of `table` as pointer `version`, naming `metadata_location`, may have landed
    /// and the engine cannot tell; logged at `error` with `cause` for an operator to reconcile.
    fn commit_outcome_unknown(
        table: &TableRef,
        version: PointerVersion,
        metadata_location: &str,
        cause: &dyn std::fmt::Display,
    ) -> Self {
        error!(
            %table,
            pointer_version = version.number(),
            metadata_location,
            %cause,
            "Whether a commit landed is unknown.",
        );
        EngineError::CommitOutcomeUnknown {
            what: Resource::Table(table.clone()),
            pointer_version: version.number(),
            metadata_location: metadata_location.to_string(),
        }
    }
}

/// The catalog an Iceberg transaction sees during one attempt: the table the attempt loaded, and
/// a commit that runs the attempt's protocol and records its outcome for the commit loop.
#[derive(Debug)]
struct AttemptCatalog<'a> {
    catalog: &'a EngineCatalog,
    table: &'a TableRef,
    loaded: &'a LoadedTable,
    recheck: bool,
    outcome: Mutex<Option<Result<Attempted<LoadedTable>, EngineError>>>,
}

fn inside_a_commit(method: &str) -> iceberg::Error {
    iceberg::Error::new(
        iceberg::ErrorKind::FeatureUnsupported,
        format!("{method} is not available to a transaction inside a commit"),
    )
}

#[async_trait]
impl Catalog for AttemptCatalog<'_> {
    async fn list_namespaces(
        &self,
        _parent: Option<&NamespaceIdent>,
    ) -> iceberg::Result<Vec<NamespaceIdent>> {
        Err(inside_a_commit("list_namespaces"))
    }

    async fn create_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> iceberg::Result<Namespace> {
        Err(inside_a_commit("create_namespace"))
    }

    async fn get_namespace(&self, _namespace: &NamespaceIdent) -> iceberg::Result<Namespace> {
        Err(inside_a_commit("get_namespace"))
    }

    async fn namespace_exists(&self, _namespace: &NamespaceIdent) -> iceberg::Result<bool> {
        Err(inside_a_commit("namespace_exists"))
    }

    async fn update_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        Err(inside_a_commit("update_namespace"))
    }

    async fn drop_namespace(&self, _namespace: &NamespaceIdent) -> iceberg::Result<()> {
        Err(inside_a_commit("drop_namespace"))
    }

    async fn list_tables(&self, _namespace: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        Err(inside_a_commit("list_tables"))
    }

    async fn create_table(
        &self,
        _namespace: &NamespaceIdent,
        _creation: TableCreation,
    ) -> iceberg::Result<Table> {
        Err(inside_a_commit("create_table"))
    }

    async fn load_table(&self, table: &TableIdent) -> iceberg::Result<Table> {
        if *table == TableIdent::from(self.table) {
            Ok(self.loaded.table.clone())
        } else {
            Err(inside_a_commit("load_table of another table"))
        }
    }

    async fn drop_table(&self, _table: &TableIdent) -> iceberg::Result<()> {
        Err(inside_a_commit("drop_table"))
    }

    async fn purge_table(&self, _table: &TableIdent) -> iceberg::Result<()> {
        Err(inside_a_commit("purge_table"))
    }

    async fn table_exists(&self, _table: &TableIdent) -> iceberg::Result<bool> {
        Err(inside_a_commit("table_exists"))
    }

    async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> iceberg::Result<()> {
        Err(inside_a_commit("rename_table"))
    }

    async fn register_table(
        &self,
        _table: &TableIdent,
        _metadata_location: String,
    ) -> iceberg::Result<Table> {
        Err(inside_a_commit("register_table"))
    }

    async fn update_table(&self, mut commit: TableCommit) -> iceberg::Result<Table> {
        let pair = EngineCommit {
            requirements: commit.take_requirements(),
            updates: commit.take_updates(),
        };
        let outcome = self
            .catalog
            .attempt_engine(self.table, self.loaded, pair, self.recheck)
            .await;
        let answer = match &outcome {
            Ok(Attempted::Landed(loaded)) => Ok(loaded.table.clone()),
            Ok(Attempted::Lost(lost)) => Err(lost.conflict(self.table)),
            Err(err) => Err(iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                err.to_string(),
            )),
        };
        *self.outcome.lock().unwrap_or_else(PoisonError::into_inner) = Some(outcome);
        answer
    }
}
