use std::ops::{Index, IndexMut};

use waw_core::{Pointer, Storage};

use crate::graph_index::GraphIndex;

/// Entity metadata stored in waw_core `Storage` — can be evicted/reloaded.
#[derive(Clone, Debug)]
pub struct EntityMeta {
    pub position: Option<(f32, f32)>,
    pub last_access: u64,
}

impl EntityMeta {
    pub const BYTES: usize = 16 + 8; // position (8+8) + last_access (8)
}

/// Mutable entity state — promotion, eviction, LRU tracking.
///
/// Protected by `RwLock` in `Pipeline`. Read-only graph data (CSR, spatial index)
/// lives in `Arc<GraphIndex>` and requires no lock.
pub struct EntityStore {
    pub entities: Storage<EntityMeta>,
    pub entity_ptrs: Vec<Option<Pointer<EntityMeta>>>,
    pub memory_used: usize,
    pub memory_threshold: usize,
    access_clock: u64,
}

impl EntityStore {
    pub fn with_capacity(capacity: usize, memory_threshold: usize) -> Self {
        Self {
            entities: Storage::with_capacity(capacity),
            entity_ptrs: Vec::with_capacity(capacity),
            memory_used: 0,
            memory_threshold,
            access_clock: 0,
        }
    }

    /// Populate all entity slots and apply position data.
    pub fn populate(&mut self, entity_count: usize, pos_pairs: &[(u32, f32, f32)]) {
        self.entity_ptrs.reserve(entity_count);

        for _ in 0..entity_count {
            let ptr = self.entities.create(EntityMeta {
                position: None,
                last_access: 0,
            });
            self.entity_ptrs.push(Some(ptr));
        }

        for &(csr_idx, x, y) in pos_pairs {
            if let Some(Some(ptr)) = self.entity_ptrs.get_mut(csr_idx as usize) {
                *self.entities.index_mut(ptr) = EntityMeta {
                    position: Some((x, y)),
                    last_access: 0,
                };
            }
        }

        self.memory_used += entity_count * EntityMeta::BYTES;
    }

    /// Read-only entity lookup — no LRU tracking, no mutable borrow needed.
    #[must_use]
    pub fn get_entity_readonly(
        &self,
        rowid: u64,
        index: &GraphIndex,
    ) -> Option<&EntityMeta> {
        let csr_idx = index.find_entity_index(rowid)?;
        let ptr = self.entity_ptrs.get(csr_idx as usize)?.as_ref()?;
        Some(self.entities.index(ptr))
    }

    /// Reload an entity from the warm or cold tier.
    pub fn reload_entity(
        &mut self,
        rowid: u64,
        position: Option<(f32, f32)>,
        index: &GraphIndex,
    ) -> Option<()> {
        let csr_idx = index.find_entity_index(rowid)?;
        self.entity_ptrs[csr_idx as usize] = None;
        self.entities.sync_pending();

        let ptr = self.entities.create(EntityMeta {
            position,
            last_access: self.access_clock,
        });
        self.entity_ptrs[csr_idx as usize] = Some(ptr);
        self.memory_used += EntityMeta::BYTES;
        Some(())
    }

    /// Evict `count` least-recently-accessed entities.
    pub fn evict_lru(
        &mut self,
        count: usize,
        index: &GraphIndex,
    ) -> Vec<(u64, EntityMeta)> {
        let mut candidates: Vec<(u32, u64)> = Vec::new();

        for (csr_idx, ptr_opt) in self.entity_ptrs.iter().enumerate() {
            if let Some(ptr) = ptr_opt {
                let meta = self.entities.index(ptr);
                candidates.push((csr_idx as u32, meta.last_access));
            }
        }

        candidates.sort_unstable_by_key(|&(_, ts)| ts);

        let evict_count = count.min(candidates.len());
        let mut evicted = Vec::with_capacity(evict_count);

        for &(csr_idx, _) in &candidates[..evict_count] {
            if let Some(ptr) = self.entity_ptrs[csr_idx as usize].take() {
                let meta = self.entities.index(&ptr).clone();
                let rowid = index.edge_csr.entity_rowid(csr_idx);
                drop(ptr);
                self.memory_used = self.memory_used.saturating_sub(EntityMeta::BYTES);
                evicted.push((rowid, meta));
            }
        }

        self.entities.sync_pending();
        evicted
    }

    #[must_use]
    pub fn over_threshold(&self) -> bool {
        self.memory_used > self.memory_threshold
    }

    #[must_use]
    pub fn is_loaded(&self, rowid: u64, index: &GraphIndex) -> bool {
        index
            .find_entity_index(rowid)
            .and_then(|idx| self.entity_ptrs.get(idx as usize))
            .and_then(|opt| opt.as_ref())
            .is_some()
    }

    #[must_use]
    pub fn position_of(
        &self,
        rowid: u64,
        index: &GraphIndex,
    ) -> Option<(f32, f32)> {
        let csr_idx = index.find_entity_index(rowid)?;
        let ptr = self.entity_ptrs.get(csr_idx as usize)?.as_ref()?;
        self.entities.index(ptr).position
    }
}
