use std::ops::Index;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};

use waw_proto::Direction;

use crate::cold_pool::ColdPool;
use crate::cold_tier::{ColdTier, StoreError};
use crate::entity_store::{EntityMeta, EntityStore};
use crate::graph_index::GraphIndex;
use crate::warm_tier::WarmTier;

/// Pipeline configuration.
pub struct PipelineConfig {
    /// Hot tier memory threshold in bytes (default: ~512 MiB).
    pub hot_memory_threshold: usize,
    /// Warm tier cache file size in bytes (default: ~256 MiB).
    pub warm_cache_capacity: usize,
    /// Number of entities to evict in one batch when threshold exceeded.
    pub evict_batch_size: usize,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            hot_memory_threshold: 512 * 1024 * 1024,
            warm_cache_capacity: 256 * 1024 * 1024,
            evict_batch_size: 256,
        }
    }
}

/// Pipeline orchestrator — three-tier graph data access.
///
/// Tier 1 (hot): in-memory waw_core `Storage` + CSR edge index
/// Tier 2 (warm): mmap-backed disk cache for evicted entities
/// Tier 3 (cold): pooled read-only SQLite connections
pub struct Pipeline {
    /// Immutable graph topology — CSR edges, spatial index, entity lookup. No lock.
    index: Arc<GraphIndex>,
    /// Mutable entity state — promotion, eviction, LRU. RwLock: reads shared, writes exclusive.
    entities: RwLock<EntityStore>,
    warm: Mutex<WarmTier>,
    cold_pool: ColdPool,
    config: PipelineConfig,
}

fn pool_size() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get().min(4))
        .unwrap_or(2)
}

impl Pipeline {
    /// Load the graph from a SQLite database with an optional warm cache file.
    pub fn load(
        db_path: impl AsRef<Path>,
        warm_cache_path: Option<impl AsRef<Path>>,
        config: PipelineConfig,
    ) -> Result<Self, StoreError> {
        // Open a single connection for bulk load, then replace with a pool
        let load_cold = ColdTier::open(&db_path)?;
        let (index, pos_pairs, entity_count, edge_memory) = GraphIndex::load(&load_cold)?;
        drop(load_cold);

        let cold_pool = ColdPool::open(&db_path, pool_size())?;

        let mut entities =
            EntityStore::with_capacity(entity_count, config.hot_memory_threshold);
        entities.memory_used = edge_memory;
        entities.populate(entity_count, &pos_pairs);

        let warm = match warm_cache_path {
            Some(p) => {
                let path = p.as_ref().to_path_buf();
                WarmTier::open(p, config.warm_cache_capacity)
                    .unwrap_or_else(|_| WarmTier::create(&path, config.warm_cache_capacity).unwrap())
            }
            None => {
                let tmp = std::env::temp_dir()
                    .join(format!("waw_warm_{}.cache", std::process::id()));
                WarmTier::create(&tmp, config.warm_cache_capacity)
                    .unwrap_or_else(|_| WarmTier::create("waw_warm.cache", config.warm_cache_capacity).unwrap())
            }
        };

        Ok(Self {
            index: Arc::new(index),
            entities: RwLock::new(entities),
            warm: Mutex::new(warm),
            cold_pool,
            config,
        })
    }

    /// Get entity metadata. Checks hot → warm → cold, promoting to hot on miss.
    pub fn get_entity(&self, rowid: u64) -> Result<Option<EntityMeta>, StoreError> {
        // 1. Check hot tier — read-only fast path (no LRU update)
        {
            let entities = self.entities.read().unwrap();
            if let Some(meta) = entities.get_entity_readonly(rowid, &self.index) {
                return Ok(Some(meta.clone()));
            }
        }

        // 2. Check warm tier
        {
            let warm = self.warm.lock().unwrap();
            if warm.contains(rowid) {
                if let Ok(Some(meta)) = warm.get(rowid) {
                    drop(warm);
                    let mut entities = self.entities.write().unwrap();
                    entities.reload_entity(rowid, meta.position, &self.index);
                    self.maybe_evict_inner(&mut *entities)?;
                    return Ok(Some(meta));
                }
            }
        }

        // 3. Check cold tier
        {
            let cold = self.cold_pool.acquire();
            let position = cold.load_position(rowid)?;
            drop(cold);

            if let Some(pos) = position {
                let mut entities = self.entities.write().unwrap();
                entities.reload_entity(rowid, Some(pos), &self.index);
                let meta = entities.get_entity_readonly(rowid, &self.index).cloned();
                self.maybe_evict_inner(&mut *entities)?;
                return Ok(meta);
            }
        }

        Ok(None)
    }

    /// Get outgoing and/or incoming edges for an entity — **lock-free**.
    #[must_use]
    pub fn get_edges(
        &self,
        entity_id: u64,
        direction: Direction,
        label_filter: &[u32],
        limit: u32,
    ) -> Vec<waw_proto::EdgeData> {
        let Some(entity_idx) = self.index.find_entity_index(entity_id) else {
            return Vec::new();
        };
        let limit = limit as usize;
        let mut result = Vec::new();

        if matches!(direction, Direction::Outgoing | Direction::Both) {
            let edges = self.index.edge_csr.outgoing(entity_idx);
            let n = edges.targets.len().min(limit - result.len());
            for i in 0..n {
                let label = edges.labels[i] as u32;
                if !label_filter.is_empty() && !label_filter.contains(&label) {
                    continue;
                }
                let src = self.index.edge_csr.entity_rowid(entity_idx);
                let tgt = self.index.edge_csr.entity_rowid(edges.targets[i]);
                result.push(waw_proto::EdgeData {
                    id: edges.rowids[i],
                    source: src,
                    target: tgt,
                    label,
                    properties: Vec::new(),
                });
            }
        }

        if matches!(direction, Direction::Incoming | Direction::Both)
            && result.len() < limit
        {
            let edges = self.index.edge_csr.incoming(entity_idx);
            let n = edges.sources.len().min(limit - result.len());
            for i in 0..n {
                let label = edges.labels[i] as u32;
                if !label_filter.is_empty() && !label_filter.contains(&label) {
                    continue;
                }
                let src = self.index.edge_csr.entity_rowid(edges.sources[i]);
                let tgt = self.index.edge_csr.entity_rowid(entity_idx);
                result.push(waw_proto::EdgeData {
                    id: edges.rowids[i],
                    source: src,
                    target: tgt,
                    label,
                    properties: Vec::new(),
                });
            }
        }

        result
    }

    /// BFS traversal — **lock-free**, uses thread-local buffers.
    #[must_use]
    pub fn traverse_bfs(
        &self,
        start_rowid: u64,
        max_depth: u32,
        edge_labels: &[u32],
        limit: u32,
    ) -> Vec<u64> {
        let mut visited = self.index.traverse_bfs(start_rowid, max_depth, edge_labels);
        visited.truncate(limit as usize);
        visited
    }

    /// Spatial bounding-box query — shared entity read lock only.
    /// Results are exactly filtered by entity position (not just tile overlap).
    #[must_use]
    pub fn search_spatial(
        &self,
        min_x: f32,
        min_y: f32,
        max_x: f32,
        max_y: f32,
        lod: u16,
        limit: u32,
    ) -> Vec<u64> {
        let entities = self.entities.read().unwrap();
        let mut out = Vec::new();
        self.index.query_spatial_into(
            min_x, min_y, max_x, max_y, lod,
            &entities.entity_ptrs,
            &mut out,
        );
        // Exact position filtering — spatial index is tile-approximate
        out.retain(|&rowid| {
            let Some(csr_idx) = self.index.find_entity_index(rowid) else { return false };
            let Some(Some(ptr)) = entities.entity_ptrs.get(csr_idx as usize) else { return false };
            let Some((x, y)) = entities.entities.index(ptr).position else { return false };
            x >= min_x && x <= max_x && y >= min_y && y <= max_y
        });
        out.truncate(limit as usize);
        out
    }

    /// Property search — queries cold tier via pool.
    pub fn search_property(
        &self,
        key: &str,
        limit: u32,
    ) -> Result<Vec<u64>, StoreError> {
        self.cold_pool.acquire().search_property(key, limit)
    }

    /// Load entity properties from cold tier.
    pub fn load_properties(
        &self,
        entity_id: u64,
    ) -> Result<Vec<crate::cold_tier::PropertyRow>, StoreError> {
        self.cold_pool.acquire().load_properties(entity_id)
    }

    /// Load blob references for an entity.
    pub fn load_blob_refs(
        &self,
        entity_id: u64,
    ) -> Result<Vec<crate::cold_tier::BlobRow>, StoreError> {
        self.cold_pool.acquire().load_blob_refs(entity_id)
    }

    /// Load a blob chunk by hash.
    pub fn load_blob_chunk(
        &self,
        hash: u64,
        offset: u64,
        chunk_size: u32,
    ) -> Result<Option<Vec<u8>>, StoreError> {
        self.cold_pool.acquire().load_blob_data(hash, offset, chunk_size)
    }

    /// Load blob metadata by hash.
    pub fn load_blob_by_hash(
        &self,
        hash: u64,
    ) -> Result<Option<crate::cold_tier::BlobRow>, StoreError> {
        self.cold_pool.acquire().load_blob_by_hash(hash)
    }

    /// Get database stats.
    pub fn stats(&self) -> Result<crate::cold_tier::GraphStats, StoreError> {
        self.cold_pool.acquire().stats()
    }

    /// Check whether an entity rowid exists in the graph — **lock-free**.
    #[must_use]
    pub fn find_entity_index(&self, rowid: u64) -> Option<u32> {
        self.index.find_entity_index(rowid)
    }

    /// Get the default query LOD from the spatial index — **lock-free**.
    #[must_use]
    pub fn spatial_lod(&self) -> u16 {
        self.index.spatial_lod()
    }

    /// Get current memory usage of the hot tier (for diagnostics).
    #[must_use]
    pub fn memory_used(&self) -> usize {
        self.entities.read().unwrap().memory_used
    }

    /// Check memory pressure and evict if needed.
    fn maybe_evict_inner(&self, entities: &mut EntityStore) -> Result<(), StoreError> {
        if !entities.over_threshold() {
            return Ok(());
        }

        let evicted = entities.evict_lru(self.config.evict_batch_size, &self.index);
        if !evicted.is_empty() {
            let mut warm = self.warm.lock().unwrap();
            for (rowid, meta) in &evicted {
                let _ = warm.put(*rowid, meta);
            }
        }

        Ok(())
    }
}
