use std::sync::{Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

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

/// [`MutexExt`] for `RwLock`, under the same reasoning.
pub trait RwLockExt<T> {
    fn read_unpoisoned(&self) -> RwLockReadGuard<'_, T>;
    fn write_unpoisoned(&self) -> RwLockWriteGuard<'_, T>;
}

impl<T> RwLockExt<T> for RwLock<T> {
    fn read_unpoisoned(&self) -> RwLockReadGuard<'_, T> {
        self.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write_unpoisoned(&self) -> RwLockWriteGuard<'_, T> {
        self.write().unwrap_or_else(PoisonError::into_inner)
    }
}
