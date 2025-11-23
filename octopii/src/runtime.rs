use std::sync::Arc;
use tokio::runtime::{Builder, Handle, Runtime};

/// An isolated tokio runtime that runs on its own dedicated thread pool.
/// This prevents the runtime from interfering with other parts of your application.
///
/// Can be created in two modes:
/// - Owned: Creates and owns a dedicated Runtime (for production use)
/// - Handle: Uses an external Runtime via Handle (for tests and multi-node scenarios)
pub struct OctopiiRuntime {
    runtime: Option<Arc<Runtime>>,
    handle: Handle,
}

impl OctopiiRuntime {
    /// Creates a new isolated runtime with a specific number of worker threads.
    /// This creates an OWNED runtime that will be cleaned up when dropped.
    ///
    /// # Arguments
    /// * `worker_threads` - Number of worker threads for the runtime (default: 4)
    ///
    /// # Example
    /// ```
    /// use octopii::OctopiiRuntime;
    ///
    /// let runtime = OctopiiRuntime::new(4);
    /// ```
    pub fn new(worker_threads: usize) -> Self {
        let runtime = Builder::new_multi_thread()
            .worker_threads(worker_threads)
            .max_blocking_threads(worker_threads * 2) // Allow 2x blocking threads for QUIC ops
            .thread_name("octopii-worker")
            .enable_all()
            .build()
            .expect("Failed to create tokio runtime");

        let handle = runtime.handle().clone();

        Self {
            runtime: Some(Arc::new(runtime)),
            handle,
        }
    }

    /// Creates a runtime from an external Handle.
    /// This does NOT own the runtime - the caller must ensure the runtime stays alive.
    ///
    /// This is useful for tests and scenarios where multiple nodes share one runtime.
    ///
    /// # Example
    /// ```no_run
    /// use octopii::OctopiiRuntime;
    ///
    /// let handle = tokio::runtime::Handle::current();
    /// let runtime = OctopiiRuntime::from_handle(handle);
    /// ```
    pub fn from_handle(handle: Handle) -> Self {
        Self {
            runtime: None,
            handle,
        }
    }

    /// Creates a new runtime with default settings (4 worker threads)
    pub fn default() -> Self {
        Self::new(4)
    }

    /// Spawn a future on the runtime (works with both owned and handle modes)
    ///
    /// # Example
    /// ```
    /// use octopii::OctopiiRuntime;
    ///
    /// let runtime = OctopiiRuntime::new(4);
    /// runtime.spawn(async {
    ///     println!("Running on runtime!");
    /// });
    /// ```
    pub fn spawn<F>(&self, future: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.handle.spawn(future)
    }

    /// Get a handle to the runtime for spawning tasks from other threads
    pub fn handle(&self) -> tokio::runtime::Handle {
        self.handle.clone()
    }
}

impl Clone for OctopiiRuntime {
    fn clone(&self) -> Self {
        Self {
            runtime: self.runtime.as_ref().map(Arc::clone),
            handle: self.handle.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_owned_runtime() {
        // Test creating an owned runtime (outside async context)
        let runtime = OctopiiRuntime::new(2);

        let result = runtime.spawn(async {
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            42
        });

        // Block on the result using the underlying runtime handle
        let handle = runtime.handle();
        let output = handle.block_on(result).unwrap();
        assert_eq!(output, 42);

        // Runtime will be dropped cleanly here (not in async context)
    }

    #[tokio::test]
    async fn test_from_handle() {
        // Test using handle from current runtime (no owned runtime to drop)
        let handle = tokio::runtime::Handle::current();
        let runtime = OctopiiRuntime::from_handle(handle);

        let result = runtime
            .spawn(async {
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                "from_handle"
            })
            .await
            .unwrap();

        assert_eq!(result, "from_handle");
    }

    #[tokio::test]
    async fn test_clone() {
        // Test cloning runtime handles
        let handle = tokio::runtime::Handle::current();
        let runtime1 = OctopiiRuntime::from_handle(handle);
        let runtime2 = runtime1.clone();

        let result1 = runtime1.spawn(async { 1 }).await.unwrap();
        let result2 = runtime2.spawn(async { 2 }).await.unwrap();

        assert_eq!(result1, 1);
        assert_eq!(result2, 2);
    }
}
