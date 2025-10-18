mod block;
mod config;
mod paths;
mod runtime;
mod storage;

pub use block::Entry;
pub use config::{FsyncSchedule, disable_fd_backend, enable_fd_backend};
pub use runtime::{ReadConsistency, WalIndex, Walrus};

#[doc(hidden)]
pub fn __set_thread_namespace_for_tests(key: &str) {
    paths::set_thread_namespace(key);
}

#[doc(hidden)]
pub fn __clear_thread_namespace_for_tests() {
    paths::clear_thread_namespace();
}

#[doc(hidden)]
pub fn __current_thread_namespace_for_tests() -> Option<String> {
    paths::thread_namespace()
}
