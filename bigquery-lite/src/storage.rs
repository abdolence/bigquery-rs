//! The storage a root lives on, behind one enum with a variant per backend.
//!
//! The engine's catalog uses the primitives here directly, the create-only write above all,
//! and Iceberg reaches the same backend through its `Storage` trait, so data files, metadata
//! files and catalog pointers share one implementation per root.

mod local;

use crate::config::StorageOptions;
use crate::error::{BackendKind, EngineError};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::StreamExt;
use iceberg::io::{
    FileIO, FileIOBuilder, FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage,
    StorageConfig, StorageFactory,
};
pub(crate) use local::LocalBackend;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::time::SystemTime;

/// A path under the root: segments joined by `/`, none of them empty, `.` or `..`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct ObjectPath(String);

impl ObjectPath {
    /// A path of one segment.
    ///
    /// `segment` is one path segment: callers pass validated IDs and constants, whose rules
    /// admit no `/` and never a whole segment of `.` or `..`.
    pub(crate) fn segment(segment: impl Display) -> Self {
        Self(segment.to_string())
    }

    /// This path with `segment` appended, under the same contract as [`ObjectPath::segment`].
    pub(crate) fn join(&self, segment: impl Display) -> Self {
        Self(format!("{}/{segment}", self.0))
    }

    /// The path parsed from text that did not come from the engine, segment by segment.
    fn parse(text: &str) -> Option<Self> {
        let valid = !text.is_empty()
            && text
                .split('/')
                .all(|s| !s.is_empty() && s != "." && s != ".." && !s.contains('\0'));
        valid.then(|| Self(text.to_string()))
    }

    /// The path, `/`-separated and relative to the root.
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for ObjectPath {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// What tells two objects stored under one name apart: an object replaced or deleted and
/// written again under the same name has another identity. Two reads of one unchanged object
/// report equal identities.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ObjectIdentity(local::LocalIdentity);

/// An object and the identity it had when it was read.
#[derive(Debug, Clone)]
pub(crate) struct StoredObject {
    pub bytes: Bytes,
    pub identity: ObjectIdentity,
}

/// One entry of a directory listing.
#[derive(Debug, Clone)]
pub(crate) struct Listed {
    pub name: String,
    pub kind: ListedKind,
}

#[derive(Debug, Clone)]
pub(crate) enum ListedKind {
    /// An object, with the time the backend reports it was created.
    Object { created: SystemTime },
    /// A directory, or a prefix on a bucket.
    Directory,
}

/// The outcome of a create-only write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Created {
    /// The object was written; the identity is the one a later read of it reports.
    Created(ObjectIdentity),
    /// An object of that name existed, so nothing was written.
    AlreadyExists,
}

/// A storage operation that failed, with the path it was about.
#[derive(Debug, thiserror::Error)]
#[error("{backend} storage failed to {operation} {path}: {source}")]
pub(crate) struct StorageError {
    backend: BackendKind,
    operation: &'static str,
    path: String,
    #[source]
    source: std::io::Error,
}

impl From<StorageError> for EngineError {
    fn from(error: StorageError) -> Self {
        EngineError::Storage {
            backend: error.backend,
            operation: error.operation,
            detail: format!("{}: {}", error.path, error.source),
        }
    }
}

impl From<StorageError> for iceberg::Error {
    fn from(error: StorageError) -> Self {
        iceberg::Error::new(iceberg::ErrorKind::Unexpected, error.to_string()).with_source(error)
    }
}

/// The storage of one root.
///
/// It serializes as its configuration, since Iceberg's `Storage` trait objects are
/// serializable, and never as a credential.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum Backend {
    Local(LocalBackend),
}

impl Backend {
    /// The backend `root`'s scheme names.
    ///
    /// # Errors
    /// [`EngineError::Config`] for a scheme without a backend in this build, or a root the
    /// backend cannot use.
    pub(crate) fn open(root: &url::Url, _options: &StorageOptions) -> Result<Self, EngineError> {
        match root.scheme() {
            "file" => Ok(Backend::Local(LocalBackend::open(root)?)),
            scheme => Err(EngineError::config(
                "root",
                format!("{root}: the scheme {scheme:?} has no storage backend in this build"),
            )),
        }
    }

    pub(crate) fn kind(&self) -> BackendKind {
        match self {
            Backend::Local(_) => BackendKind::Local,
        }
    }

    /// The URI Iceberg metadata names `path` by.
    pub(crate) fn location(&self, path: &ObjectPath) -> String {
        match self {
            Backend::Local(local) => local.location(path),
        }
    }

    /// The path under the root that `location` names.
    ///
    /// # Errors
    /// A [`StorageError`] when `location` is not under this root.
    pub(crate) fn object_path(&self, location: &str) -> Result<ObjectPath, StorageError> {
        let relative = match self {
            Backend::Local(local) => local.relative(location),
        };
        relative
            .and_then(ObjectPath::parse)
            .ok_or_else(|| StorageError {
                backend: self.kind(),
                operation: "resolve",
                path: location.to_string(),
                source: std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "the location is not a path under the engine's root",
                ),
            })
    }

    /// The object's content, or `None` when it does not exist.
    pub(crate) async fn read(&self, path: &ObjectPath) -> Result<Option<Bytes>, StorageError> {
        match self {
            Backend::Local(local) => local.read(path).await,
        }
    }

    /// The object's content and identity, from one read, or `None` when it does not exist.
    pub(crate) async fn read_object(
        &self,
        path: &ObjectPath,
    ) -> Result<Option<StoredObject>, StorageError> {
        match self {
            Backend::Local(local) => local.read_object(path).await,
        }
    }

    /// Whether the object exists.
    pub(crate) async fn exists(&self, path: &ObjectPath) -> Result<bool, StorageError> {
        match self {
            Backend::Local(local) => local.exists(path).await,
        }
    }

    /// Writes the object whole, replacing any object of that name. Readers see the old or the
    /// new content, never a part, and the content is durable when this returns.
    pub(crate) async fn write(&self, path: &ObjectPath, bytes: Bytes) -> Result<(), StorageError> {
        match self {
            Backend::Local(local) => local.write(path, bytes).await,
        }
    }

    /// Writes the object only if no object of that name exists. Of two creates of one name,
    /// exactly one is [`Created::Created`]; the content is durable when this returns.
    pub(crate) async fn create(
        &self,
        path: &ObjectPath,
        bytes: Bytes,
    ) -> Result<Created, StorageError> {
        match self {
            Backend::Local(local) => local.create(path, bytes).await,
        }
    }

    /// The entries directly under `dir`; none when it does not exist.
    pub(crate) async fn list(&self, dir: &ObjectPath) -> Result<Vec<Listed>, StorageError> {
        match self {
            Backend::Local(local) => local.list(dir).await,
        }
    }

    /// Deletes the object; an absent one is not an error.
    pub(crate) async fn delete(&self, path: &ObjectPath) -> Result<(), StorageError> {
        match self {
            Backend::Local(local) => local.delete(path).await,
        }
    }

    /// A `FileIO` over this backend, for Iceberg's readers and writers.
    pub(crate) fn file_io(&self) -> FileIO {
        FileIOBuilder::new(Arc::new(BackendFactory {
            backend: self.clone(),
        }))
        .build()
    }
}

/// Builds the backend for Iceberg's `FileIO`, which asks its factory once.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct BackendFactory {
    backend: Backend,
}

#[typetag::serde(name = "bigquery_lite::BackendFactory")]
impl StorageFactory for BackendFactory {
    fn build(&self, _config: &StorageConfig) -> iceberg::Result<Arc<dyn Storage>> {
        Ok(Arc::new(self.backend.clone()))
    }
}

#[async_trait]
#[typetag::serde(name = "bigquery_lite::Backend")]
impl Storage for Backend {
    async fn exists(&self, path: &str) -> iceberg::Result<bool> {
        Ok(Backend::exists(self, &self.object_path(path)?).await?)
    }

    async fn metadata(&self, path: &str) -> iceberg::Result<FileMetadata> {
        let size = match self {
            Backend::Local(local) => local.size(&self.object_path(path)?).await?,
        };
        Ok(FileMetadata { size })
    }

    async fn read(&self, path: &str) -> iceberg::Result<Bytes> {
        Backend::read(self, &self.object_path(path)?)
            .await?
            .ok_or_else(|| not_found(path))
    }

    async fn reader(&self, path: &str) -> iceberg::Result<Box<dyn FileRead>> {
        match self {
            Backend::Local(local) => Ok(Box::new(local.reader(&self.object_path(path)?).await?)),
        }
    }

    async fn write(&self, path: &str, bs: Bytes) -> iceberg::Result<()> {
        Ok(Backend::write(self, &self.object_path(path)?, bs).await?)
    }

    async fn writer(&self, path: &str) -> iceberg::Result<Box<dyn FileWrite>> {
        match self {
            Backend::Local(local) => Ok(Box::new(local.writer(&self.object_path(path)?).await?)),
        }
    }

    async fn delete(&self, path: &str) -> iceberg::Result<()> {
        Ok(Backend::delete(self, &self.object_path(path)?).await?)
    }

    async fn delete_prefix(&self, path: &str) -> iceberg::Result<()> {
        match self {
            Backend::Local(local) => Ok(local.delete_dir(&self.object_path(path)?).await?),
        }
    }

    async fn delete_stream(&self, mut paths: BoxStream<'static, String>) -> iceberg::Result<()> {
        while let Some(path) = paths.next().await {
            Storage::delete(self, &path).await?;
        }
        Ok(())
    }

    fn new_input(&self, path: &str) -> iceberg::Result<InputFile> {
        Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
    }

    fn new_output(&self, path: &str) -> iceberg::Result<OutputFile> {
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
    }
}

fn not_found(path: &str) -> iceberg::Error {
    iceberg::Error::new(
        iceberg::ErrorKind::DataInvalid,
        format!("{path} does not exist"),
    )
}
