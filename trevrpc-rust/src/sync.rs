use std::sync::{Mutex, MutexGuard, PoisonError};

/// Locks that keep working after a panic on another thread.
///
/// Every `Mutex` in this crate guards plain state that stays consistent at each await point or
/// call boundary, so a poisoned lock is still safe to use and must not take the process down.
pub trait MutexExt<T> {
    fn lock_unpoisoned(&self) -> MutexGuard<'_, T>;
}

impl<T> MutexExt<T> for Mutex<T> {
    fn lock_unpoisoned(&self) -> MutexGuard<'_, T> {
        self.lock().unwrap_or_else(PoisonError::into_inner)
    }
}
