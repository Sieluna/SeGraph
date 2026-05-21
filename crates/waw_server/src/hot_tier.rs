use std::collections::HashMap;
use std::collections::VecDeque;
use std::ops::{Index, IndexMut};

use waw_core::{Pointer, Storage};

use crate::cold_tier::{ColdTier, StoreError};
use crate::spatial_index::SpatialIndex;

/// Entity metadata stored in waw_core `Storage` — can be evicted/reloaded.
#[derive(Clone, Debug)]
pub struct EntityMeta {
    pub position: Option<(f32, f32)>,
    pub last_access: u64,
}

impl EntityMeta {
    const BYTES: usize = 16 + 8; // position (8+8) + last_access (8)
}

/// Slice views into the outgoing CSR arrays for one entity.
pub struct OutgoingEdges<'a> {
    pub targets: &'a [u32],
    pub labels: &'a [u16],
    pub rowids: &'a [u64],
}

/// Slice views into the incoming CSR arrays for one entity.
pub struct IncomingEdges<'a> {
    pub sources: &'a [u32],
    pub labels: &'a [u16],
    pub rowids: &'a [u64],
}

/// Compressed Sparse Row edge index — always kept in memory.
pub struct EdgeCsr {
    pub targets: Vec<u32>,
    pub labels: Vec<u16>,
    pub rowids: Vec<u64>,
    pub offsets: Vec<u32>,
    pub sources: Vec<u32>,
    pub in_labels: Vec<u16>,
    pub in_rowids: Vec<u64>,
    pub in_offsets: Vec<u32>,
    /// rowid → CSR entity index
    pub entity_by_rowid: HashMap<u64, u32>,
    /// CSR entity index → rowid
    pub entity_rowids: Vec<u64>,
}

impl EdgeCsr {
    pub fn empty() -> Self {
        Self {
            targets: Vec::new(),
            labels: Vec::new(),
            rowids: Vec::new(),
            offsets: vec![0],
            sources: Vec::new(),
            in_labels: Vec::new(),
            in_rowids: Vec::new(),
            in_offsets: vec![0],
            entity_by_rowid: HashMap::new(),
            entity_rowids: Vec::new(),
        }
    }

    pub fn entity_count(&self) -> usize {
        self.entity_rowids.len()
    }

    #[must_use]
    pub fn find_entity_index(&self, rowid: u64) -> Option<u32> {
        self.entity_by_rowid.get(&rowid).copied()
    }

    #[must_use]
    pub fn entity_rowid(&self, idx: u32) -> u64 {
        self.entity_rowids.get(idx as usize).copied().unwrap_or(0)
    }

    #[must_use]
    pub fn outgoing(&self, idx: u32) -> OutgoingEdges<'_> {
        let i = idx as usize;
        let start = self.offsets[i] as usize;
        let end = self.offsets[i + 1] as usize;
        OutgoingEdges {
            targets: &self.targets[start..end],
            labels: &self.labels[start..end],
            rowids: &self.rowids[start..end],
        }
    }

    #[must_use]
    pub fn incoming(&self, idx: u32) -> IncomingEdges<'_> {
        let i = idx as usize;
        let start = self.in_offsets[i] as usize;
        let end = self.in_offsets[i + 1] as usize;
        IncomingEdges {
            sources: &self.sources[start..end],
            labels: &self.in_labels[start..end],
            rowids: &self.in_rowids[start..end],
        }
    }
}

/// In-memory hot tier backed by waw_core `Storage`.
///
/// CSR edge index is always resident. `EntityMeta` components in `Storage`
/// can be evicted to the warm tier under memory pressure.
pub struct HotTier {
    /// Per-entity metadata in waw_core Storage (tiered).
    pub entities: Storage<EntityMeta>,
    /// CSR index → Pointer into entities Storage (None if evicted).
    pub entity_ptrs: Vec<Option<Pointer<EntityMeta>>>,
    /// Graph topology — always resident.
    pub edge_csr: EdgeCsr,
    /// Spatial index — rebuilt when positions change.
    pub spatial_index: Option<SpatialIndex>,
    /// Approximate memory used by hot tier (bytes).
    pub memory_used: usize,
    /// Threshold at which eviction kicks in (bytes).
    pub memory_threshold: usize,
    /// Monotonic access counter for LRU.
    access_clock: u64,
    /// Reusable buffer for spatial index results.
    spatial_buf: Vec<u32>,
    /// Reusable seen-set for O(n) spatial dedup (generation counter — no reset loop).
    seen_set: Vec<u32>,
    /// Monotonic generation for seen-set invalidation.
    seen_gen: u32,
    /// Reusable visited array for BFS.
    visit_buf: Vec<u32>,
    /// Reusable FIFO queue for BFS.
    queue_buf: VecDeque<(u32, u32)>,
}

impl HotTier {
    pub fn empty() -> Self {
        Self {
            entities: Storage::new(),
            entity_ptrs: Vec::new(),
            edge_csr: EdgeCsr::empty(),
            spatial_index: None,
            memory_used: 0,
            memory_threshold: usize::MAX,
            access_clock: 0,
            spatial_buf: Vec::new(),
            seen_set: Vec::new(),
            seen_gen: 0,
            visit_buf: Vec::new(),
            queue_buf: VecDeque::new(),
        }
    }

    /// Full load from cold tier — bulk loads all entities/edges/positions.
    pub fn load(cold: &ColdTier) -> Result<Self, StoreError> {
        let mut this = Self::empty();

        // 1. Load entities
        let rowids = cold.load_entity_ids()?;
        this.entities = Storage::with_capacity(rowids.len());
        this.entity_ptrs.reserve(rowids.len());
        this.edge_csr.entity_rowids.reserve(rowids.len());
        this.edge_csr
            .entity_by_rowid
            .reserve(rowids.len());

        for (i, &rowid) in rowids.iter().enumerate() {
            this.edge_csr.entity_rowids.push(rowid);
            this.edge_csr.entity_by_rowid.insert(rowid, i as u32);
            let ptr = this.entities.create(EntityMeta {
                position: None,
                last_access: 0,
            });
            this.entity_ptrs.push(Some(ptr));
        }
        let entity_count = rowids.len();
        this.memory_used += entity_count * EntityMeta::BYTES;

        // 2. Load edges and build CSR
        let edges = cold.load_all_edges()?;
        let mut out_edges: Vec<(u32, u32, u16, u64)> =
            Vec::with_capacity(edges.len());
        let mut in_edges: Vec<(u32, u32, u16, u64)> =
            Vec::with_capacity(edges.len());

        for e in &edges {
            if let (Some(&src), Some(&tgt)) = (
                this.edge_csr.entity_by_rowid.get(&e.source_entity),
                this.edge_csr.entity_by_rowid.get(&e.target_entity),
            ) {
                out_edges.push((src, tgt, e.label as u16, e.id));
                in_edges.push((tgt, src, e.label as u16, e.id));
            }
        }

        out_edges.sort_unstable_by_key(|e| (e.0, e.1));
        build_csr(
            &out_edges,
            entity_count,
            &mut this.edge_csr.targets,
            &mut this.edge_csr.labels,
            &mut this.edge_csr.rowids,
            &mut this.edge_csr.offsets,
        );

        in_edges.sort_unstable_by_key(|e| (e.0, e.1));
        build_csr(
            &in_edges,
            entity_count,
            &mut this.edge_csr.sources,
            &mut this.edge_csr.in_labels,
            &mut this.edge_csr.in_rowids,
            &mut this.edge_csr.in_offsets,
        );

        this.memory_used += this.edge_csr.targets.len() * 4; // u32
        this.memory_used += this.edge_csr.labels.len() * 2; // u16
        this.memory_used += this.edge_csr.rowids.len() * 8; // u64
        this.memory_used += this.edge_csr.offsets.len() * 4; // u32
        this.memory_used += this.edge_csr.sources.len() * 4;
        this.memory_used += this.edge_csr.in_labels.len() * 2;
        this.memory_used += this.edge_csr.in_rowids.len() * 8;
        this.memory_used += this.edge_csr.in_offsets.len() * 4;

        // 3. Load positions and build spatial index
        let positions = cold.load_all_positions()?;
        if !positions.is_empty() {
            let mut pos_pairs: Vec<(u32, f32, f32)> =
                Vec::with_capacity(positions.len());
            for &(rowid, x, y) in &positions {
                if let Some(&csr_idx) = this.edge_csr.entity_by_rowid.get(&rowid) {
                    if let Some(Some(ptr)) =
                        this.entity_ptrs.get_mut(csr_idx as usize)
                    {
                        *this.entities.index_mut(ptr) = EntityMeta {
                            position: Some((x, y)),
                            last_access: 0,
                        };
                    }
                    pos_pairs.push((csr_idx, x, y));
                }
            }
            let bits = spatial_bits_for_count(entity_count);
            this.spatial_index = Some(SpatialIndex::build(&pos_pairs, bits));
        }

        Ok(this)
    }

    /// Read-only entity lookup — no LRU tracking, no mutable borrow needed.
    #[must_use]
    pub fn get_entity_readonly(&self, rowid: u64) -> Option<&EntityMeta> {
        let csr_idx = self.edge_csr.entity_by_rowid.get(&rowid)?;
        let ptr = self.entity_ptrs.get(*csr_idx as usize)?.as_ref()?;
        Some(self.entities.index(ptr))
    }

    /// Look up an entity's metadata, recording access for LRU.
    #[must_use]
    pub fn get_entity(&mut self, rowid: u64) -> Option<&EntityMeta> {
        let csr_idx = self.edge_csr.find_entity_index(rowid)?;
        let ptr = self.entity_ptrs.get(csr_idx as usize)?.as_ref()?;
        self.access_clock += 1;
        let meta = self.entities.index_mut(ptr);
        meta.last_access = self.access_clock;
        Some(meta)
    }

    /// Get mutable entity metadata, recording access.
    #[must_use]
    pub fn get_entity_mut(&mut self, rowid: u64) -> Option<&mut EntityMeta> {
        let csr_idx = self.edge_csr.find_entity_index(rowid)?;
        let ptr = self.entity_ptrs.get(csr_idx as usize)?.as_ref()?;
        self.access_clock += 1;
        let meta = self.entities.index_mut(ptr);
        meta.last_access = self.access_clock;
        Some(meta)
    }

    /// Batch-update access times for a set of entities (single write-lock, amortized).
    pub fn touch_entities(&mut self, rowids: &[u64]) {
        for &rowid in rowids {
            let Some(csr_idx) = self.edge_csr.find_entity_index(rowid) else {
                continue;
            };
            let Some(ptr) = self.entity_ptrs.get(csr_idx as usize).and_then(|o| o.as_ref())
            else {
                continue;
            };
            self.access_clock += 1;
            self.entities.index_mut(ptr).last_access = self.access_clock;
        }
    }

    /// Check whether an entity is loaded in the hot tier.
    #[must_use]
    pub fn is_loaded(&self, rowid: u64) -> bool {
        self.edge_csr
            .find_entity_index(rowid)
            .and_then(|idx| self.entity_ptrs.get(idx as usize))
            .and_then(|opt| opt.as_ref())
            .is_some()
    }

    /// Position for an entity (if loaded and positioned).
    #[must_use]
    pub fn position_of(&self, rowid: u64) -> Option<(f32, f32)> {
        let csr_idx = self.edge_csr.find_entity_index(rowid)?;
        let ptr = self.entity_ptrs.get(csr_idx as usize)?.as_ref()?;
        self.entities.index(ptr).position
    }

    /// Reload an entity from the warm or cold tier.
    /// Call `sync_pending` before this if there are pending drops.
    pub fn reload_entity(&mut self, rowid: u64, position: Option<(f32, f32)>) -> Option<()> {
        let csr_idx = self.edge_csr.find_entity_index(rowid)?;
        // Drop old pointer if present (refcount → 0, will be freed on sync_pending)
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
    /// Returns the rowids of evicted entities.
    pub fn evict_lru(&mut self, count: usize) -> Vec<(u64, EntityMeta)> {
        let mut candidates: Vec<(u32, u64)> = Vec::new(); // (csr_idx, last_access)

        for (csr_idx, ptr_opt) in self.entity_ptrs.iter().enumerate() {
            if let Some(ptr) = ptr_opt {
                let meta = self.entities.index(ptr);
                candidates.push((csr_idx as u32, meta.last_access));
            }
        }

        // Sort by last_access ascending (oldest first)
        candidates.sort_unstable_by_key(|&(_, ts)| ts);

        let evict_count = count.min(candidates.len());
        let mut evicted = Vec::with_capacity(evict_count);

        for &(csr_idx, _) in &candidates[..evict_count] {
            if let Some(ptr) = self.entity_ptrs[csr_idx as usize].take() {
                let meta = self.entities.index(&ptr).clone();
                let rowid = self.edge_csr.entity_rowid(csr_idx);
                drop(ptr); // decrement refcount → will be freed on sync_pending
                evicted.push((rowid, meta));
                self.memory_used = self.memory_used.saturating_sub(EntityMeta::BYTES);
            }
        }

        self.entities.sync_pending();
        evicted
    }

    /// Check if memory pressure threshold is exceeded.
    #[must_use]
    pub fn over_threshold(&self) -> bool {
        self.memory_used > self.memory_threshold
    }

    /// BFS traversal using CSR arrays (allocates — use `traverse_bfs_into` to reuse buffers).
    #[must_use]
    pub fn traverse_bfs(
        &self,
        start_rowid: u64,
        max_depth: u32,
        edge_labels: &[u32],
    ) -> Vec<u64> {
        let mut result = Vec::new();
        let entity_count = self.edge_csr.entity_count();
        let mut visited: Vec<u32> = vec![0; entity_count];
        let mut queue: VecDeque<(u32, u32)> = VecDeque::new();
        bfs_impl(&self.edge_csr, start_rowid, max_depth, edge_labels, &mut visited, &mut queue, &mut result);
        result
    }

    /// Fast BFS traversal reusing internal buffers — no allocation.
    pub fn traverse_bfs_into(
        &mut self,
        start_rowid: u64,
        max_depth: u32,
        edge_labels: &[u32],
        out: &mut Vec<u64>,
    ) {
        out.clear();
        let entity_count = self.edge_csr.entity_count();

        // Resize visit buffer to cover all entities
        if self.visit_buf.len() < entity_count {
            self.visit_buf.resize(entity_count, 0);
        }
        self.visit_buf[..entity_count].fill(0);
        self.queue_buf.clear();

        bfs_impl(&self.edge_csr, start_rowid, max_depth, edge_labels, &mut self.visit_buf, &mut self.queue_buf, out);
    }

    /// Spatial query returning entity rowids (allocates — use `query_spatial_into` to reuse buffers).
    #[must_use]
    pub fn query_spatial(
        &self,
        min_x: f32,
        min_y: f32,
        max_x: f32,
        max_y: f32,
        lod: u16,
    ) -> Vec<u64> {
        let Some(ref index) = self.spatial_index else {
            return Vec::new();
        };
        let mut indices = Vec::new();
        index.query_bounds_into(min_x, min_y, max_x, max_y, lod, &mut indices);
        let mut rowids: Vec<u64> = Vec::with_capacity(indices.len());
        for idx in indices {
            let i = idx as usize;
            if i < self.entity_ptrs.len() && self.entity_ptrs[i].is_some() {
                // SAFETY: entity_ptrs and entity_rowids have the same length
                rowids.push(unsafe { *self.edge_csr.entity_rowids.get_unchecked(i) });
            }
        }
        rowids.sort_unstable();
        rowids.dedup();
        rowids
    }

    /// Fast spatial query reusing internal buffers — O(n) seen-set dedup, no reset loop.
    pub fn query_spatial_into(
        &mut self,
        min_x: f32,
        min_y: f32,
        max_x: f32,
        max_y: f32,
        lod: u16,
        out: &mut Vec<u64>,
    ) {
        out.clear();
        let Some(ref index) = self.spatial_index else { return };

        // Reuse spatial index buffer
        self.spatial_buf.clear();
        index.query_bounds_into(min_x, min_y, max_x, max_y, lod, &mut self.spatial_buf);

        let entity_count = self.edge_csr.entity_rowids.len();
        if self.spatial_buf.is_empty() || entity_count == 0 {
            return;
        }

        // Generation counter — no reset loop needed
        self.seen_gen = self.seen_gen.wrapping_add(1);
        let generation = self.seen_gen;

        // Grow seen-set to cover all entity indices (if needed)
        if self.seen_set.len() < entity_count {
            self.seen_set.resize(entity_count, 0);
        }

        let entity_ptrs = &self.entity_ptrs;
        let entity_rowids = &self.edge_csr.entity_rowids;
        let seen = &mut self.seen_set;

        for &idx in &self.spatial_buf {
            let i = idx as usize;
            if i >= entity_count {
                continue;
            }
            // SAFETY: i < seen.len() due to resize above
            let slot = unsafe { seen.get_unchecked_mut(i) };
            if *slot == generation {
                continue;
            }
            *slot = generation;
            if entity_ptrs[i].is_some() {
                // SAFETY: entity_ptrs and entity_rowids have the same length
                out.push(*unsafe { entity_rowids.get_unchecked(i) });
            }
        }
    }
}

/// Build CSR arrays from a sorted edge list.
fn build_csr(
    edges: &[(u32, u32, u16, u64)],
    entity_count: usize,
    targets_or_sources: &mut Vec<u32>,
    labels: &mut Vec<u16>,
    rowids: &mut Vec<u64>,
    offsets: &mut Vec<u32>,
) {
    targets_or_sources.clear();
    labels.clear();
    rowids.clear();
    offsets.clear();
    offsets.resize(entity_count + 1, 0);

    targets_or_sources.reserve(edges.len());
    labels.reserve(edges.len());
    rowids.reserve(edges.len());

    let mut current_src = 0u32;
    for &(src, tgt, lbl, rid) in edges {
        while current_src < src {
            current_src += 1;
            offsets[current_src as usize] = targets_or_sources.len() as u32;
        }
        targets_or_sources.push(tgt);
        labels.push(lbl);
        rowids.push(rid);
    }

    for i in (current_src as usize + 1)..=entity_count {
        offsets[i] = targets_or_sources.len() as u32;
    }
}

/// Shared BFS implementation used by both allocating and buffer-reuse paths.
fn bfs_impl(
    csr: &EdgeCsr,
    start_rowid: u64,
    max_depth: u32,
    edge_labels: &[u32],
    visited: &mut [u32],
    queue: &mut VecDeque<(u32, u32)>,
    out: &mut Vec<u64>,
) {
    let start_idx = match csr.find_entity_index(start_rowid) {
        Some(idx) => idx,
        None => return,
    };
    let start_i = start_idx as usize;

    visited[start_i] = 1;
    queue.push_back((start_idx, 0));
    out.push(start_rowid);

    let filter_active = !edge_labels.is_empty();

    while let Some((idx, depth)) = queue.pop_front() {
        if depth >= max_depth {
            continue;
        }

        let i = idx as usize;
        let start = csr.offsets[i] as usize;
        let end = csr.offsets[i + 1] as usize;

        if filter_active {
            for pos in start..end {
                let label = unsafe { csr.labels.get_unchecked(pos) };
                if !edge_labels.contains(&(*label as u32)) {
                    continue;
                }
                let target = unsafe { *csr.targets.get_unchecked(pos) };
                let ti = target as usize;
                if visited[ti] == 0 {
                    visited[ti] = depth + 2;
                    queue.push_back((target, depth + 1));
                    out.push(unsafe { *csr.entity_rowids.get_unchecked(ti) });
                }
            }
        } else {
            for pos in start..end {
                let target = unsafe { *csr.targets.get_unchecked(pos) };
                let ti = target as usize;
                if visited[ti] == 0 {
                    visited[ti] = depth + 2;
                    queue.push_back((target, depth + 1));
                    out.push(unsafe { *csr.entity_rowids.get_unchecked(ti) });
                }
            }
        }
    }
}

fn spatial_bits_for_count(node_count: usize) -> u16 {
    if node_count == 0 {
        return 4;
    }
    let bits = ((node_count as f64).log2() / 2.0).round() as i32;
    bits.clamp(4, 8) as u16
}

#[cfg(test)]
mod tests {
    use tempfile::NamedTempFile;

    use super::*;
    use crate::cold_tier::ColdTier;

    fn create_test_db(path: &std::path::Path) {
        let conn = rusqlite::Connection::open(path).unwrap();
        conn.execute_batch(
            r#"
            CREATE TABLE entity (id INTEGER PRIMARY KEY);
            CREATE TABLE edge (
                id INTEGER PRIMARY KEY,
                source_entity INTEGER NOT NULL,
                target_entity INTEGER NOT NULL,
                label INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE position_component (
                entity_id INTEGER PRIMARY KEY,
                x REAL NOT NULL,
                y REAL NOT NULL
            );
            CREATE TABLE property (
                entity_id INTEGER NOT NULL,
                key TEXT NOT NULL,
                value_type INTEGER NOT NULL,
                value_int INTEGER,
                value_float REAL,
                value_text TEXT
            );
            CREATE TABLE blob_store (
                entity_id INTEGER NOT NULL,
                key TEXT NOT NULL,
                hash INTEGER NOT NULL,
                mime TEXT DEFAULT '',
                size_bytes INTEGER NOT NULL,
                data BLOB
            );
            INSERT INTO entity VALUES (1), (2), (3);
            INSERT INTO edge VALUES (1, 1, 2, 1);
            INSERT INTO edge VALUES (2, 2, 3, 1);
            INSERT INTO position_component VALUES (1, 0.0, 0.0);
            INSERT INTO position_component VALUES (2, 0.8, 0.8);
            "#,
        )
        .unwrap();
    }

    #[test]
    fn loads_graph_from_cold_tier() {
        let file = NamedTempFile::new().unwrap();
        create_test_db(file.path());
        let cold = ColdTier::open(file.path()).unwrap();
        let hot = HotTier::load(&cold).unwrap();

        assert_eq!(hot.edge_csr.entity_count(), 3);
        assert!(hot.is_loaded(1));
        assert!(hot.is_loaded(2));
        assert!(hot.spatial_index.is_some());
    }

    #[test]
    fn traversal_works() {
        let file = NamedTempFile::new().unwrap();
        create_test_db(file.path());
        let cold = ColdTier::open(file.path()).unwrap();
        let hot = HotTier::load(&cold).unwrap();

        let result = hot.traverse_bfs(1, 2, &[1]);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], 1);
    }

    #[test]
    fn eviction_frees_memory() {
        let file = NamedTempFile::new().unwrap();
        create_test_db(file.path());
        let cold = ColdTier::open(file.path()).unwrap();
        let mut hot = HotTier::load(&cold).unwrap();

        let before = hot.memory_used;
        let evicted = hot.evict_lru(1);
        assert_eq!(evicted.len(), 1);
        assert!(hot.memory_used < before);

        // Evicted entity is no longer loaded
        let evicted_rowid = evicted[0].0;
        assert!(!hot.is_loaded(evicted_rowid));
    }

    #[test]
    fn reload_restores_entity() {
        let file = NamedTempFile::new().unwrap();
        create_test_db(file.path());
        let cold = ColdTier::open(file.path()).unwrap();
        let mut hot = HotTier::load(&cold).unwrap();

        let evicted = hot.evict_lru(1);
        let rowid = evicted[0].0;

        hot.reload_entity(rowid, Some((0.5, 0.5)));
        assert!(hot.is_loaded(rowid));
    }
}
