use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::cold_tier::{ColdTier, StoreError};

/// A pool of read-only SQLite connections, eliminating the single-connection bottleneck.
///
/// Each connection is behind its own `Mutex`, so N threads can query SQLite concurrently.
pub struct ColdPool {
    connections: Vec<Mutex<ColdTier>>,
    next: AtomicUsize,
}

impl ColdPool {
    /// Open a pool of `pool_size` read-only connections to the same database.
    pub fn open(
        db_path: impl AsRef<Path>,
        pool_size: usize,
    ) -> Result<Self, StoreError> {
        let pool_size = pool_size.max(1).min(8);
        let mut connections = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            connections.push(Mutex::new(ColdTier::open(&db_path)?));
        }
        Ok(Self {
            connections,
            next: AtomicUsize::new(0),
        })
    }

    /// Acquire a connection from the pool (round-robin).
    ///
    /// Returns a `MutexGuard<ColdTier>` — the lock is held until dropped.
    pub fn acquire(&self) -> std::sync::MutexGuard<'_, ColdTier> {
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.connections.len();
        self.connections[idx].lock().unwrap()
    }
}
