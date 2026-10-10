//! A root in a directory of a local filesystem.
//!
//! Every write goes to a staging file in the target's own directory and reaches its name by a
//! rename (a replacing write) or a hard link (a create-only write), so readers never see a part
//! of a file, and `link(2)` failing with `EEXIST` is the create-only primitive. Files are
//! `fsync`ed before they get their name and their directory after, so that a write that
//! returned survives a power loss with every file it names. Network and FUSE filesystems were
//! not measured for the link's exclusivity; [`LocalBackend::open`] warns about them.

use super::{Created, Listed, ListedKind, ObjectIdentity, ObjectPath, StorageError, StoredObject};
use crate::error::{BackendKind, EngineError};
use async_trait::async_trait;
use bytes::Bytes;
use iceberg::io::{FileRead, FileWrite};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::ops::Range;
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncWriteExt;
use tracing::warn;

/// The identity of a local file: its inode, and its birth time where the filesystem records
/// one, since an inode number can be reused once its file is deleted. The change time is left
/// out: removing the staging file after the link changes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LocalIdentity {
    device: u64,
    inode: u64,
    born: Option<SystemTime>,
}

impl From<&fs::Metadata> for ObjectIdentity {
    fn from(metadata: &fs::Metadata) -> Self {
        ObjectIdentity(LocalIdentity {
            device: metadata.dev(),
            inode: metadata.ino(),
            born: metadata.created().ok(),
        })
    }
}

/// A root directory, which serializes as its path.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LocalBackend {
    root: PathBuf,
    /// `file://` and the root path: the prefix of every location in Iceberg metadata.
    root_uri: String,
}

impl LocalBackend {
    /// The backend of a `file://` root, creating the directory when it is missing.
    ///
    /// # Errors
    /// [`EngineError::Config`] for a root that is not an absolute UTF-8 path or cannot be
    /// created.
    pub(crate) fn open(root: &url::Url) -> Result<Self, EngineError> {
        let path = root.to_file_path().map_err(|()| {
            EngineError::config("root", format!("{root} is not an absolute local path"))
        })?;
        let text = path
            .to_str()
            .ok_or_else(|| EngineError::config("root", format!("{root} is not UTF-8")))?;
        let text = text.trim_end_matches('/');
        if text.is_empty() {
            return Err(EngineError::config(
                "root",
                "the filesystem root itself cannot be the engine's root",
            ));
        }
        fs::create_dir_all(text).map_err(|err| {
            EngineError::config("root", format!("{root} cannot be created: {err}"))
        })?;
        warn_on_shared_filesystem(Path::new(text));
        Ok(Self {
            root: PathBuf::from(text),
            root_uri: format!("file://{text}"),
        })
    }

    pub(super) fn location(&self, path: &ObjectPath) -> String {
        format!("{}/{}", self.root_uri, path.as_str())
    }

    /// `location` relative to the root, when it is under it.
    pub(super) fn relative<'l>(&self, location: &'l str) -> Option<&'l str> {
        location
            .strip_prefix(self.root_uri.as_str())
            .and_then(|rest| rest.strip_prefix('/'))
    }

    fn fs_path(&self, path: &ObjectPath) -> PathBuf {
        self.root.join(path.as_str())
    }

    fn error(&self, operation: &'static str, path: &Path, source: io::Error) -> StorageError {
        StorageError {
            backend: BackendKind::Local,
            operation,
            path: path.display().to_string(),
            source,
        }
    }

    /// Runs `operation` on the blocking pool, since every call here is a blocking syscall.
    async fn blocking<T, F>(
        &self,
        name: &'static str,
        path: &ObjectPath,
        operation: F,
    ) -> Result<T, StorageError>
    where
        T: Send + 'static,
        F: FnOnce(&Path, &Path) -> io::Result<T> + Send + 'static,
    {
        let root = self.root.clone();
        let target = self.fs_path(path);
        let for_error = target.clone();
        tokio::task::spawn_blocking(move || operation(&root, &target))
            .await
            .map_err(io::Error::other)
            .and_then(|result| result)
            .map_err(|source| self.error(name, &for_error, source))
    }

    pub(super) async fn read(&self, path: &ObjectPath) -> Result<Option<Bytes>, StorageError> {
        self.blocking("read", path, |_, target| match fs::read(target) {
            Ok(bytes) => Ok(Some(Bytes::from(bytes))),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err),
        })
        .await
    }

    pub(super) async fn read_object(
        &self,
        path: &ObjectPath,
    ) -> Result<Option<StoredObject>, StorageError> {
        self.blocking("read", path, |_, target| {
            let mut file = match File::open(target) {
                Ok(file) => file,
                Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
                Err(err) => return Err(err),
            };
            let identity = ObjectIdentity::from(&file.metadata()?);
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes)?;
            Ok(Some(StoredObject {
                bytes: Bytes::from(bytes),
                identity,
            }))
        })
        .await
    }

    pub(super) async fn exists(&self, path: &ObjectPath) -> Result<bool, StorageError> {
        self.blocking("stat", path, |_, target| target.try_exists())
            .await
    }

    pub(super) async fn size(&self, path: &ObjectPath) -> Result<u64, StorageError> {
        self.blocking("stat", path, |_, target| Ok(fs::metadata(target)?.len()))
            .await
    }

    pub(super) async fn write(&self, path: &ObjectPath, bytes: Bytes) -> Result<(), StorageError> {
        self.blocking("write", path, move |root, target| {
            ensure_parent(root, target)?;
            let staging = staging_path(target);
            let written =
                write_synced(&staging, &bytes).and_then(|()| fs::rename(&staging, target));
            if written.is_err() {
                let _ = fs::remove_file(&staging);
            }
            written?;
            sync_parent(target)
        })
        .await
    }

    pub(super) async fn create(
        &self,
        path: &ObjectPath,
        bytes: Bytes,
    ) -> Result<Created, StorageError> {
        self.blocking("create", path, move |root, target| {
            ensure_parent(root, target)?;
            let staging = staging_path(target);
            let identity =
                match write_synced(&staging, &bytes).and_then(|()| fs::metadata(&staging)) {
                    // The link gives this very inode the target's name.
                    Ok(metadata) => ObjectIdentity::from(&metadata),
                    Err(err) => {
                        let _ = fs::remove_file(&staging);
                        return Err(err);
                    }
                };
            let linked = fs::hard_link(&staging, target);
            if let Err(err) = fs::remove_file(&staging) {
                warn!(staging = %staging.display(), %err, "A staging file could not be removed.");
            }
            match linked {
                Ok(()) => {
                    sync_parent(target)?;
                    Ok(Created::Created(identity))
                }
                Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
                    Ok(Created::AlreadyExists)
                }
                Err(err) => Err(err),
            }
        })
        .await
    }

    pub(super) async fn list(&self, dir: &ObjectPath) -> Result<Vec<Listed>, StorageError> {
        self.blocking("list", dir, |_, target| {
            let entries = match fs::read_dir(target) {
                Ok(entries) => entries,
                Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
                Err(err) => return Err(err),
            };
            let mut listed = Vec::new();
            for entry in entries {
                let entry = entry?;
                let Ok(name) = entry.file_name().into_string() else {
                    continue;
                };
                let metadata = match entry.metadata() {
                    Ok(metadata) => metadata,
                    // Deleted between the listing and the stat.
                    Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
                    Err(err) => return Err(err),
                };
                let kind = if metadata.is_dir() {
                    ListedKind::Directory
                } else {
                    // The status change time is when the link gave the file its name.
                    let created = u64::try_from(metadata.ctime())
                        .ok()
                        .and_then(|secs| {
                            let nanos = u32::try_from(metadata.ctime_nsec()).ok()?;
                            UNIX_EPOCH.checked_add(Duration::new(secs, nanos))
                        })
                        .unwrap_or(UNIX_EPOCH);
                    ListedKind::Object { created }
                };
                listed.push(Listed { name, kind });
            }
            Ok(listed)
        })
        .await
    }

    pub(super) async fn delete(&self, path: &ObjectPath) -> Result<(), StorageError> {
        self.blocking("delete", path, |_, target| match fs::remove_file(target) {
            Err(err) if err.kind() != io::ErrorKind::NotFound => Err(err),
            _ => Ok(()),
        })
        .await
    }

    pub(super) async fn delete_dir(&self, dir: &ObjectPath) -> Result<(), StorageError> {
        self.blocking("delete", dir, |_, target| {
            match fs::remove_dir_all(target) {
                Err(err) if err.kind() != io::ErrorKind::NotFound => Err(err),
                _ => Ok(()),
            }
        })
        .await
    }

    pub(super) async fn reader(&self, path: &ObjectPath) -> Result<LocalFileRead, StorageError> {
        let file = self
            .blocking("open", path, |_, target| File::open(target))
            .await?;
        Ok(LocalFileRead {
            file: Arc::new(file),
            path: self.fs_path(path),
        })
    }

    pub(super) async fn writer(&self, path: &ObjectPath) -> Result<LocalFileWrite, StorageError> {
        let target = self.fs_path(path);
        let staging = staging_path(&target);
        let file = self
            .blocking("open", path, {
                let staging = staging.clone();
                move |root, target| {
                    ensure_parent(root, target)?;
                    File::create_new(&staging)
                }
            })
            .await?;
        Ok(LocalFileWrite {
            file: Some(tokio::fs::File::from_std(file)),
            staging,
            target,
        })
    }
}

/// A file read by ranges, from any thread.
pub(crate) struct LocalFileRead {
    file: Arc<File>,
    path: PathBuf,
}

#[async_trait]
impl FileRead for LocalFileRead {
    async fn read(&self, range: Range<u64>) -> iceberg::Result<Bytes> {
        let file = Arc::clone(&self.file);
        let length = usize::try_from(range.end.saturating_sub(range.start))
            .map_err(|err| iceberg::Error::new(iceberg::ErrorKind::DataInvalid, err.to_string()))?;
        let read = tokio::task::spawn_blocking(move || {
            let mut buffer = vec![0; length];
            file.read_exact_at(&mut buffer, range.start)?;
            Ok::<_, io::Error>(Bytes::from(buffer))
        })
        .await
        .map_err(io::Error::other)
        .and_then(|result| result);
        read.map_err(|source| {
            StorageError {
                backend: BackendKind::Local,
                operation: "read",
                path: self.path.display().to_string(),
                source,
            }
            .into()
        })
    }
}

/// A file written in parts. It gets its name only when closed, so an abandoned writer leaves no
/// partial file under the name.
pub(crate) struct LocalFileWrite {
    file: Option<tokio::fs::File>,
    staging: PathBuf,
    target: PathBuf,
}

impl LocalFileWrite {
    fn error(&self, operation: &'static str, source: io::Error) -> iceberg::Error {
        StorageError {
            backend: BackendKind::Local,
            operation,
            path: self.target.display().to_string(),
            source,
        }
        .into()
    }
}

#[async_trait]
impl FileWrite for LocalFileWrite {
    async fn write(&mut self, bs: Bytes) -> iceberg::Result<()> {
        let Some(file) = self.file.as_mut() else {
            return Err(self.error("write", io::Error::other("the file is closed")));
        };
        let written = file.write_all(&bs).await;
        written.map_err(|err| self.error("write", err))
    }

    async fn close(&mut self) -> iceberg::Result<()> {
        let Some(mut file) = self.file.take() else {
            return Err(self.error("close", io::Error::other("the file is closed")));
        };
        let (staging, target) = (self.staging.clone(), self.target.clone());
        let closed = async {
            file.flush().await?;
            file.sync_all().await?;
            drop(file);
            tokio::task::spawn_blocking(move || {
                fs::rename(&staging, &target)?;
                sync_parent(&target)
            })
            .await
            .map_err(io::Error::other)?
        }
        .await;
        closed.map_err(|err| self.error("close", err))
    }
}

impl Drop for LocalFileWrite {
    fn drop(&mut self) {
        if self.file.is_some() {
            let _ = fs::remove_file(&self.staging);
        }
    }
}

/// A staging name beside `target`, unique per write, so that the rename or the link never
/// crosses a mount point and two writers never share a staging file.
fn staging_path(target: &Path) -> PathBuf {
    let mut name = target.as_os_str().to_owned();
    name.push(format!(".{}.tmp", uuid::Uuid::new_v4()));
    PathBuf::from(name)
}

fn write_synced(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut file = File::create_new(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

fn sync_parent(path: &Path) -> io::Result<()> {
    match path.parent() {
        Some(parent) => File::open(parent)?.sync_all(),
        None => Ok(()),
    }
}

/// Creates the missing directories above `target`, inside `root`, and makes each new one
/// durable in its parent.
fn ensure_parent(root: &Path, target: &Path) -> io::Result<()> {
    let Some(parent) = target.parent() else {
        return Ok(());
    };
    if parent.is_dir() {
        return Ok(());
    }
    let missing: Vec<&Path> = parent
        .ancestors()
        .take_while(|dir| *dir != root && !dir.is_dir())
        .collect();
    for dir in missing.into_iter().rev() {
        match fs::create_dir(dir) {
            Err(err) if err.kind() != io::ErrorKind::AlreadyExists => return Err(err),
            _ => sync_parent(dir)?,
        }
    }
    Ok(())
}

/// Logs a warning when the root is on a network or FUSE filesystem, where the create-only
/// write's exclusivity across hosts was never measured. Linux reports the filesystem type in
/// `/proc/self/mountinfo`; elsewhere nothing is checked.
fn warn_on_shared_filesystem(root: &Path) {
    let Some(fs_type) = filesystem_type(root) else {
        return;
    };
    let shared = fs_type.starts_with("fuse")
        || matches!(
            fs_type.as_str(),
            "nfs"
                | "nfs4"
                | "cifs"
                | "smb3"
                | "smbfs"
                | "9p"
                | "afs"
                | "ceph"
                | "glusterfs"
                | "lustre"
                | "davfs"
        );
    if shared {
        warn!(
            root = %root.display(),
            fs_type,
            "The engine's root is on a network or FUSE filesystem, where two engines on one \
             root may both see their commits land; run one engine per root there.",
        );
    }
}

/// The type of the filesystem mounted deepest above `path`.
fn filesystem_type(path: &Path) -> Option<String> {
    let path = path.canonicalize().ok()?;
    let mountinfo = fs::read_to_string("/proc/self/mountinfo").ok()?;
    mountinfo
        .lines()
        .filter_map(|line| {
            // `id parent major:minor root mount-point options [optional...] - type source ...`
            let (left, right) = line.split_once(" - ")?;
            let mount_point = unescape_mountinfo(left.split(' ').nth(4)?);
            let fs_type = right.split(' ').next()?.to_string();
            path.starts_with(&mount_point)
                .then_some((mount_point.components().count(), fs_type))
        })
        .max_by_key(|(depth, _)| *depth)
        .map(|(_, fs_type)| fs_type)
}

/// A mountinfo path, whose spaces, tabs, newlines and backslashes are octal escapes.
fn unescape_mountinfo(field: &str) -> PathBuf {
    let mut out = String::with_capacity(field.len());
    let mut chars = field.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            let code: String = chars.by_ref().take(3).collect();
            match u8::from_str_radix(&code, 8) {
                Ok(byte) => out.push(char::from(byte)),
                Err(_) => {
                    out.push('\\');
                    out.push_str(&code);
                }
            }
        } else {
            out.push(c);
        }
    }
    PathBuf::from(out)
}
