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
use std::collections::HashMap;
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
    #[allow(
        dead_code,
        reason = "Iceberg's own actions, `fast_append` above all, commit through it; no RPC \
                  the engine serves builds one before the write path exists"
    )]
    Transaction(Transaction),
    /// A change the engine computed from the loaded table.
    Engine(EngineCommit),
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
    /// Another commit took the pointer version, or a requirement failed on the loaded table.
    Lost,
}

/// The pointer an attempt builds on.
#[derive(Clone, Copy)]
enum Base<'a> {
    Empty,
    Head(&'a PointerHead),
}

/// The outcome of the create of an attempt's pointer.
enum Placed {
    /// The create reached the backend; the object has this identity.
    Ours(ObjectIdentity),
    /// Another commit created the pointer version first.
    Taken,
}

/// What reading the predecessor back after a successful create showed.
enum ReadBack {
    Landed,
    Lost,
    /// The predecessor is gone or replaced; the clock tells a stale writer from a stall.
    Gone,
}

/// The floor of a commit's measured duration, so that a fast commit never retries in a tight
/// loop.
const MIN_BASE: Duration = Duration::from_millis(10);

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
    /// under the name stops the commit: what it was built for is gone.
    ///
    /// # Errors
    /// [`EngineError::NotFound`] when the table does not exist or was replaced,
    /// [`EngineError::Aborted`] past `commit_retry_deadline`,
    /// [`EngineError::CommitOutcomeUnknown`], never retried, and whatever `change` returns.
    pub(crate) async fn commit(
        &self,
        table: &TableRef,
        mut change: impl FnMut(&LoadedTable) -> Result<Change, EngineError>,
    ) -> Result<LoadedTable, EngineError> {
        let lock = self.locks.table(table);
        let _guard = lock.lock().await;
        let mut retry = Retry::new(table, self.settings.commit_retry_deadline);
        let mut identity = None;
        loop {
            let attempt = retry.start();
            let loaded = self.load_for_attempt(table, attempt, &mut identity).await?;
            let attempted = match change(&loaded)? {
                // Nothing changes, so nothing is written: the table stays at its pointer.
                Change::Engine(commit) if commit.updates.is_empty() => Attempted::Landed(loaded),
                Change::Engine(commit) => match self.propose(&loaded, commit)? {
                    Some(proposal) => {
                        self.attempt_metadata(table, Base::Head(&loaded.head), proposal)
                            .await?
                    }
                    None => Attempted::Lost,
                },
                Change::Transaction(transaction) => {
                    self.attempt_transaction(table, &loaded, transaction)
                        .await?
                }
            };
            match attempted {
                Attempted::Landed(loaded) => return Ok(loaded),
                Attempted::Lost => retry.lost().await?,
            }
        }
    }

    /// Creates the first pointer of a table identity, on an empty name or a tombstone.
    pub(super) async fn create_pointer(
        &self,
        table: &TableRef,
        new: NewPointer,
    ) -> Result<LoadedTable, EngineError> {
        let lock = self.locks.table(table);
        let _guard = lock.lock().await;
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
                NameState::Dropped(head) => Base::Head(head),
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
            match self.attempt_metadata(table, base, proposal).await? {
                Attempted::Landed(loaded) => return Ok(loaded),
                Attempted::Lost => retry.lost().await?,
            }
        }
    }

    /// Drops the table with a tombstone pointer, and returns the table as it was dropped and the
    /// tombstone's version.
    pub(crate) async fn drop_table(
        &self,
        table: &TableRef,
    ) -> Result<(LoadedTable, PointerVersion), EngineError> {
        let lock = self.locks.table(table);
        let _guard = lock.lock().await;
        let mut retry = Retry::new(table, self.settings.commit_retry_deadline);
        let mut identity = None;
        loop {
            let attempt = retry.start();
            let loaded = self.load_for_attempt(table, attempt, &mut identity).await?;
            match self.attempt_drop(table, &loaded).await? {
                Attempted::Landed(tombstone) => return Ok((loaded, tombstone)),
                Attempted::Lost => retry.lost().await?,
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
        let lock = self.locks.table(&table);
        let _guard = lock.lock().await;
        let loaded = self.reload(&table).await?;
        let pair = EngineCommit {
            requirements: commit.take_requirements(),
            updates: commit.take_updates(),
        };
        let attempted = match self.propose(&loaded, pair)? {
            Some(proposal) => {
                self.attempt_metadata(&table, Base::Head(&loaded.head), proposal)
                    .await?
            }
            None => Attempted::Lost,
        };
        match attempted {
            Attempted::Landed(loaded) => Ok(loaded),
            Attempted::Lost => Err(lost_race(&table).into()),
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

    /// The new metadata of `commit` applied to the loaded table, or `None` when one of its
    /// requirements fails on it, which a later attempt rebuilds on the newer table.
    fn propose(
        &self,
        loaded: &LoadedTable,
        commit: EngineCommit,
    ) -> Result<Option<TableProposal>, EngineError> {
        for requirement in &commit.requirements {
            match requirement.check(Some(loaded.metadata())) {
                Ok(()) => {}
                Err(err) if err.kind() == iceberg::ErrorKind::CatalogCommitConflicts => {
                    debug!(%err, "A commit requirement failed on the loaded table.");
                    return Ok(None);
                }
                Err(err) => return Err(err.into()),
            }
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
        Ok(Some(TableProposal {
            metadata: built.metadata,
            location: location.to_string(),
            write: Some(location),
            expired: built.expired_metadata_logs,
        }))
    }

    /// Runs `transaction`'s actions on the loaded table and commits what they produce as one
    /// attempt.
    async fn attempt_transaction(
        &self,
        table: &TableRef,
        loaded: &LoadedTable,
        transaction: Transaction,
    ) -> Result<Attempted<LoadedTable>, EngineError> {
        let scope = AttemptCatalog {
            catalog: self,
            table,
            loaded,
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

    /// Makes `proposal` the table's metadata on top of `base`.
    async fn attempt_metadata(
        &self,
        table: &TableRef,
        base: Base<'_>,
        proposal: TableProposal,
    ) -> Result<Attempted<LoadedTable>, EngineError> {
        if let Some(location) = &proposal.write {
            proposal.metadata.write_to(&self.file_io, location).await?;
        }
        let body = PointerBody::Live(LivePointer {
            metadata_location: proposal.location.clone(),
            previous: match base {
                Base::Head(head) => head.body.metadata_location().map(str::to_string),
                Base::Empty => None,
            },
            committed_at_ms: now_ms(),
        });
        let written = proposal.write.as_ref().map(|_| proposal.location.as_str());
        let Attempted::Landed(head) = self.place(table, base, body, written).await? else {
            return Ok(Attempted::Lost);
        };
        let version = head.version;
        let loaded = self.loaded_table(table, head, proposal.metadata, proposal.location)?;
        self.remember(table, Some(&loaded));
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
    ) -> Result<Attempted<PointerVersion>, EngineError> {
        let body = PointerBody::Tombstone(Tombstone {
            previous: loaded.metadata_location().to_string(),
            dropped_at_ms: now_ms(),
        });
        let Attempted::Landed(head) = self
            .place(table, Base::Head(&loaded.head), body, None)
            .await?
        else {
            return Ok(Attempted::Lost);
        };
        self.remember(table, None);
        info!(%table, pointer_version = head.version.number(), "Dropped the table.");
        self.prune(table).await;
        Ok(Attempted::Landed(head.version))
    }

    /// Creates `body` as the successor of `base` and reads `base` back. `written` is the
    /// metadata file this attempt wrote, deleted when another commit took the version, since
    /// nothing else can ever name it.
    async fn place(
        &self,
        table: &TableRef,
        base: Base<'_>,
        body: PointerBody,
        written: Option<&str>,
    ) -> Result<Attempted<PointerHead>, EngineError> {
        let dir = Self::pointer_dir(table);
        let version = match base {
            Base::Empty => PointerVersion::FIRST,
            Base::Head(head) => head.version.next(),
        };
        let path = dir.join(version.object_name());
        let sent = Instant::now();
        let identity = match self.create_pointer_object(&path, &body).await? {
            Placed::Ours(identity) => identity,
            Placed::Taken => {
                if let Some(location) = written {
                    self.delete_location(location).await;
                }
                info!(
                    %table,
                    pointer_version = version.number(),
                    "Another commit created the pointer version first.",
                );
                return Ok(Attempted::Lost);
            }
        };
        self.hooks.reach(CommitPoint::Created).await;
        let read_back = match base {
            Base::Head(head) => {
                match self
                    .backend
                    .read_object(&dir.join(head.version.object_name()))
                    .await?
                {
                    Some(object) if head.is(&object) => ReadBack::Landed,
                    _ => ReadBack::Gone,
                }
            }
            Base::Empty => self.read_back_first(&dir, &body).await?,
        };
        let stalled = sent.elapsed();
        match read_back {
            ReadBack::Landed => Ok(Attempted::Landed(PointerHead {
                version,
                identity,
                body,
            })),
            ReadBack::Gone if stalled >= self.settings.pointer_min_age / 2 => {
                let metadata_location = body.metadata_location().unwrap_or_default().to_string();
                error!(
                    %table,
                    pointer_version = version.number(),
                    metadata_location,
                    stalled_ms = stalled.as_millis(),
                    "A commit stalled past half of pointer_min_age and its predecessor was \
                     pruned; whether it landed is unknown.",
                );
                Err(EngineError::CommitOutcomeUnknown {
                    what: Resource::Table(table.clone()),
                    pointer_version: version.number(),
                    metadata_location,
                })
            }
            ReadBack::Gone | ReadBack::Lost => {
                warn!(
                    %table,
                    pointer_version = version.number(),
                    "A commit's pointer went into a name pruning had freed; it is lost.",
                );
                Ok(Attempted::Lost)
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
            match self.backend.read_object(path).await? {
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
        Err(match unknown {
            Some(err) => err.into(),
            None => EngineError::invalid(
                format!("catalog pointer {path}"),
                "the name was reported taken but holds no object",
            ),
        })
    }

    /// The read-back of a name's first pointer, which has no predecessor: `v0` alone, or a `v1`
    /// created on this commit's metadata, is landed; `v0` beside a higher pointer without such a
    /// `v1` is lost, since pointers leave bottom-up and a `v1` cannot go while `v0` stays.
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
                _ => false,
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

    /// Deletes every pointer of `table` below `version`, bottom-up, stopping at the first
    /// failure, which is logged: the pruning of later commits removes the rest.
    pub(super) async fn delete_pointers_below(&self, table: &TableRef, version: PointerVersion) {
        let dir = Self::pointer_dir(table);
        let mut below: Vec<PointerVersion> = match self.pointer_versions(&dir).await {
            Ok(versions) => versions
                .into_iter()
                .filter(|lower| *lower < version)
                .collect(),
            Err(err) => {
                warn!(%table, %err, "Listing the pointers of a dropped table failed.");
                return;
            }
        };
        below.sort();
        for lower in below {
            if let Err(err) = self.backend.delete(&dir.join(lower.object_name())).await {
                warn!(%table, %err, "Deleting a pointer of a dropped table failed.");
                return;
            }
        }
    }

    /// Deletes the file at an Iceberg location, logging a failure: its callers delete files
    /// that nothing can name any more, which the orphan sweep removes otherwise.
    async fn delete_location(&self, location: &str) {
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
    let cap = grown.min(ceiling);
    let micros = u64::try_from(cap.as_micros()).unwrap_or(u64::MAX);
    Duration::from_micros(rand::random_range(0..=micros))
}

fn lost_race(table: &TableRef) -> iceberg::Error {
    iceberg::Error::new(
        iceberg::ErrorKind::CatalogCommitConflicts,
        format!("{table}: a concurrent commit created the next catalog pointer first"),
    )
    .with_retryable(false)
}

/// The catalog an Iceberg transaction sees during one attempt: the table the attempt loaded, and
/// a commit that runs the attempt's protocol and records its outcome for the commit loop.
#[derive(Debug)]
struct AttemptCatalog<'a> {
    catalog: &'a EngineCatalog,
    table: &'a TableRef,
    loaded: &'a LoadedTable,
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
        let outcome = match self.catalog.propose(self.loaded, pair) {
            Ok(Some(proposal)) => {
                self.catalog
                    .attempt_metadata(self.table, Base::Head(&self.loaded.head), proposal)
                    .await
            }
            Ok(None) => Ok(Attempted::Lost),
            Err(err) => Err(err),
        };
        let answer = match &outcome {
            Ok(Attempted::Landed(loaded)) => Ok(loaded.table.clone()),
            Ok(Attempted::Lost) => Err(lost_race(self.table)),
            Err(err) => Err(iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                err.to_string(),
            )),
        };
        *self.outcome.lock().unwrap_or_else(PoisonError::into_inner) = Some(outcome);
        answer
    }
}
