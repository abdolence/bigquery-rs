use crate::error::EngineError;
use crate::ids::Location;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

/// A quantity of bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ByteSize(u64);

impl ByteSize {
    /// `bytes` bytes.
    pub const fn from_bytes(bytes: u64) -> Self {
        Self(bytes)
    }

    /// `mib` mebibytes, saturating at `u64::MAX` bytes.
    pub const fn from_mib(mib: u64) -> Self {
        Self(mib.saturating_mul(1024 * 1024))
    }

    /// The number of bytes.
    pub const fn bytes(self) -> u64 {
        self.0
    }
}

/// How an engine runs: where its data lives and its limits. Built with [`EngineConfig::new`],
/// whose defaults are the ones each field documents, and checked as a whole by
/// [`Engine::open`](crate::Engine::open).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct EngineConfig {
    /// The root every table, dataset and catalog pointer lives under: `file:///…` for a local
    /// directory. The scheme picks the storage backend; one whose feature is not compiled in is
    /// a `Config` error.
    pub root: url::Url,
    /// Per-backend options; a `file://` root needs none.
    pub storage: StorageOptions,
    /// The location a dataset records when its creation names none; jobs not tied to a
    /// dataset report it. Default `embedded`.
    pub default_location: Location,
    /// The largest query result answered inline; a larger one goes through a result table.
    /// Default 10 MiB.
    pub inline_result_limit: ByteSize,
    /// Queries that run at once. Default the available parallelism.
    pub max_concurrent_jobs: usize,
    /// The memory all running queries share before they spill or fail. Default 1 GiB.
    pub memory_budget: ByteSize,
    /// Where queries spill; `None` is the system temporary directory.
    pub spill_dir: Option<PathBuf>,
    /// How long a finished job is kept. Default 24 hours.
    pub job_ttl: Duration,
    /// How long a write stream lives without a commit. Default 24 hours.
    pub stream_ttl: Duration,
    /// How long a read session lives. Default 6 hours.
    pub session_ttl: Duration,
    /// How long a snapshot is kept for time travel and pinned reads. Default 7 days, and at
    /// least the longer of `session_ttl` and `stream_ttl` plus 24 hours.
    pub snapshot_retention: Duration,
    /// How long a commit waits for more writes to join it. Default 0: a commit group is what
    /// arrived during the previous commit.
    pub commit_window: Duration,
    /// How long a commit retries after losing to concurrent commits before it fails with
    /// `Aborted`. Default 30 seconds.
    pub commit_retry_deadline: Duration,
    /// How old a table's newer catalog pointer must be before the one below it is pruned.
    /// Default 15 minutes, at least 1 second. A commit that stalls for half of it between
    /// creating its pointer and reading its predecessor back ends with an unknown outcome.
    pub pointer_min_age: Duration,
    /// How long a loaded table is trusted without asking storage whether a newer commit
    /// exists. Default 0: every statement asks, with one probe. At most half of
    /// `pointer_min_age`.
    pub catalog_cache_ttl: Duration,
    /// The threads of the runtime Iceberg's scans and writers run on. Default the available
    /// parallelism.
    pub iceberg_threads: usize,
    /// The background upkeep of tables.
    pub maintenance: Maintenance,
}

impl EngineConfig {
    /// A configuration for `root` with every other setting at its default.
    pub fn new(root: url::Url) -> Self {
        let parallelism = std::thread::available_parallelism().map_or(1, usize::from);
        Self {
            root,
            storage: StorageOptions::default(),
            default_location: Location::from_static("embedded"),
            inline_result_limit: ByteSize::from_mib(10),
            max_concurrent_jobs: parallelism,
            memory_budget: ByteSize::from_mib(1024),
            spill_dir: None,
            job_ttl: HOURS_24,
            stream_ttl: HOURS_24,
            session_ttl: Duration::from_secs(6 * 60 * 60),
            snapshot_retention: Duration::from_secs(7 * 24 * 60 * 60),
            commit_window: Duration::ZERO,
            commit_retry_deadline: Duration::from_secs(30),
            pointer_min_age: Duration::from_secs(15 * 60),
            catalog_cache_ttl: Duration::ZERO,
            iceberg_threads: parallelism,
            maintenance: Maintenance::default(),
        }
    }

    /// Fails on the first setting the engine cannot run with, naming it.
    pub(crate) fn validate(&self) -> Result<(), EngineError> {
        if self.pointer_min_age < Duration::from_secs(1) {
            return Err(EngineError::config(
                "pointer_min_age",
                format!(
                    "{:?} is below the minimum of 1 second",
                    self.pointer_min_age
                ),
            ));
        }
        if self.catalog_cache_ttl > self.pointer_min_age / 2 {
            return Err(EngineError::config(
                "catalog_cache_ttl",
                format!(
                    "{:?} is above half of pointer_min_age ({:?}), past which a cached table \
                     can miss a newer commit",
                    self.catalog_cache_ttl, self.pointer_min_age
                ),
            ));
        }
        let floor = self.session_ttl.max(self.stream_ttl) + HOURS_24;
        if self.snapshot_retention < floor {
            return Err(EngineError::config(
                "snapshot_retention",
                format!(
                    "{:?} is below the longer of session_ttl and stream_ttl plus 24 hours \
                     ({floor:?}), so a live session or stream could lose its files",
                    self.snapshot_retention
                ),
            ));
        }
        if self.commit_retry_deadline.is_zero() {
            return Err(EngineError::config(
                "commit_retry_deadline",
                "zero leaves no time for a single commit",
            ));
        }
        for (field, value) in [
            ("max_concurrent_jobs", self.max_concurrent_jobs),
            ("iceberg_threads", self.iceberg_threads),
            (
                "maintenance.manifest_merge_threshold",
                self.maintenance.manifest_merge_threshold,
            ),
            (
                "maintenance.manifest_merge_target_entries",
                self.maintenance.manifest_merge_target_entries,
            ),
            ("maintenance.max_snapshots", self.maintenance.max_snapshots),
        ] {
            if value == 0 {
                return Err(EngineError::config(
                    field,
                    "zero leaves nothing to work with",
                ));
            }
        }
        Ok(())
    }
}

const HOURS_24: Duration = Duration::from_secs(24 * 60 * 60);

/// The background upkeep of tables: snapshot expiry, the orphan sweep, the manifest merge and
/// the TTL sweeps.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Maintenance {
    /// How often every step runs. Default 1 hour; `None` turns every step off, the ones a
    /// commit queues included.
    pub schedule: Option<Duration>,
    /// The data manifests a table's current snapshot holds before they are merged. Default 100.
    pub manifest_merge_threshold: usize,
    /// The entries one merged manifest holds at most. Default 8,192.
    pub manifest_merge_target_entries: usize,
    /// The snapshots of `main` kept beside `snapshot_retention`. Default 1,000.
    pub max_snapshots: usize,
}

impl Default for Maintenance {
    fn default() -> Self {
        Self {
            schedule: Some(Duration::from_secs(60 * 60)),
            manifest_merge_threshold: 100,
            manifest_merge_target_entries: 8192,
            max_snapshots: 1000,
        }
    }
}

/// Options of the storage backends; the local backend has none.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct StorageOptions {}

/// A TCP listener for [`Engine::serve_tcp`](crate::Engine::serve_tcp) and
/// [`Engine::bind_tcp`](crate::Engine::bind_tcp).
///
/// This release serves plaintext gRPC on a loopback address only and checks no credentials;
/// TLS and authentication settings join this type with the network route, and any address that
/// is not loopback is a `Config` error until then.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct TcpEndpoint {
    /// The address to listen on; port 0 picks a free port, which
    /// [`TcpServer::local_addr`](crate::TcpServer::local_addr) reports.
    pub addr: SocketAddr,
}

impl TcpEndpoint {
    /// A listener on `addr`.
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }
}
