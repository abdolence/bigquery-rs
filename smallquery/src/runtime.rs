use crate::error::EngineError;

/// The engine's own tokio runtime, on which Iceberg's scans and writers spawn their work, so
/// that they never depend on a runtime of the application's that may end first. Iceberg builds
/// its `Runtime` only from an owned tokio runtime.
pub(crate) struct IcebergRuntime {
    /// Always `Some` until dropped.
    tokio: Option<tokio::runtime::Runtime>,
    handle: iceberg::Runtime,
}

impl IcebergRuntime {
    /// A runtime of `threads` worker threads.
    pub(crate) fn new(threads: usize) -> Result<Self, EngineError> {
        let tokio = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(threads)
            .thread_name("smallquery-iceberg")
            .enable_all()
            .build()?;
        let handle = iceberg::Runtime::new(&tokio);
        Ok(Self {
            tokio: Some(tokio),
            handle,
        })
    }

    /// The handles every table the engine builds carries.
    pub(crate) fn handle(&self) -> iceberg::Runtime {
        self.handle.clone()
    }
}

impl Drop for IcebergRuntime {
    /// Shuts down without waiting, which is the one way a tokio runtime may be dropped from
    /// inside another runtime, as an engine dropped in an async test is.
    fn drop(&mut self) {
        if let Some(tokio) = self.tokio.take() {
            tokio.shutdown_background();
        }
    }
}

impl std::fmt::Debug for IcebergRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergRuntime").finish_non_exhaustive()
    }
}
