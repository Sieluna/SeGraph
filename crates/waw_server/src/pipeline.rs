use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};

use waw_proto::Direction;

use crate::cold_tier::{ColdTier, StoreError};
use crate::hot_tier::{EntityMeta, HotTier};
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
/// Tier 3 (cold): read-only SQLite database
pub struct Pipeline {
    hot: RwLock<HotTier>,
    warm: Mutex<WarmTier>,
    cold: Arc<Mutex<ColdTier>>,
    config: PipelineConfig,
}

impl Pipeline {
    /// Load the graph from a SQLite database with an optional warm cache file.
    pub fn load(
        db_path: impl AsRef<Path>,
        warm_cache_path: Option<impl AsRef<Path>>,
        config: PipelineConfig,
    ) -> Result<Self, StoreError> {
        let cold = Arc::new(Mutex::new(ColdTier::open(db_path)?));
        let mut hot = HotTier::load(&cold.lock().unwrap())?;
        hot.memory_threshold = config.hot_memory_threshold;

        let warm = match warm_cache_path {
            Some(p) => {
                let path = p.as_ref().to_path_buf();
                WarmTier::open(p, config.warm_cache_capacity)
                    .unwrap_or_else(|_| WarmTier::create(&path, config.warm_cache_capacity).unwrap())
            }
            None => {
                let tmp = std::env::temp_dir().join(format!("waw_warm_{}.cache", std::process::id()));
                WarmTier::create(&tmp, config.warm_cache_capacity)
                    .unwrap_or_else(|_| WarmTier::create("waw_warm.cache", config.warm_cache_capacity).unwrap())
            }
        };

        Ok(Self {
            hot: RwLock::new(hot),
            warm: Mutex::new(warm),
            cold,
            config,
        })
    }

    /// Get entity metadata. Checks hot → warm → cold, promoting to hot on miss.
    pub fn get_entity(&self, rowid: u64) -> Result<Option<EntityMeta>, StoreError> {
        // 1. Check hot tier — read-only fast path (no LRU update)
        {
            let hot = self.hot.read().unwrap();
            if let Some(meta) = hot.get_entity_readonly(rowid) {
                return Ok(Some(meta.clone()));
            }
        }

        // 2. Check warm tier
        {
            let warm = self.warm.lock().unwrap();
            if warm.contains(rowid) {
                if let Ok(Some(meta)) = warm.get(rowid) {
                    drop(warm);
                    // Promote to hot tier
                    let mut hot = self.hot.write().unwrap();
                    hot.reload_entity(rowid, meta.position);
                    self.maybe_evict_inner(&mut *hot)?;
                    return Ok(Some(meta));
                }
            }
        }

        // 3. Check cold tier
        {
            let cold = self.cold.lock().unwrap();
            let position = cold.load_position(rowid)?;
            drop(cold);

            if let Some(pos) = position {
                let mut hot = self.hot.write().unwrap();
                hot.reload_entity(rowid, Some(pos));
                let meta = hot.get_entity_readonly(rowid).cloned();
                self.maybe_evict_inner(&mut *hot)?;
                return Ok(meta);
            }
        }

        Ok(None)
    }

    /// Get outgoing and/or incoming edges for an entity.
    #[must_use]
    pub fn get_edges(
        &self,
        entity_id: u64,
        direction: Direction,
        label_filter: &[u32],
        limit: u32,
    ) -> Vec<waw_proto::EdgeData> {
        let hot = self.hot.read().unwrap();
        let Some(entity_idx) = hot.edge_csr.find_entity_index(entity_id) else {
            return Vec::new();
        };
        let limit = limit as usize;
        let mut result = Vec::new();

        if matches!(direction, Direction::Outgoing | Direction::Both) {
            let edges = hot.edge_csr.outgoing(entity_idx);
            let n = edges.targets.len().min(limit - result.len());
            for i in 0..n {
                let label = edges.labels[i] as u32;
                if !label_filter.is_empty() && !label_filter.contains(&label) {
                    continue;
                }
                let src = hot.edge_csr.entity_rowid(entity_idx);
                let tgt = hot.edge_csr.entity_rowid(edges.targets[i]);
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
            let edges = hot.edge_csr.incoming(entity_idx);
            let n = edges.sources.len().min(limit - result.len());
            for i in 0..n {
                let label = edges.labels[i] as u32;
                if !label_filter.is_empty() && !label_filter.contains(&label) {
                    continue;
                }
                let src = hot.edge_csr.entity_rowid(edges.sources[i]);
                let tgt = hot.edge_csr.entity_rowid(entity_idx);
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

    /// BFS traversal from a starting entity (read lock — fast, allocating path).
    #[must_use]
    pub fn traverse_bfs(
        &self,
        start_rowid: u64,
        max_depth: u32,
        edge_labels: &[u32],
        limit: u32,
    ) -> Vec<u64> {
        let hot = self.hot.read().unwrap();
        let mut visited = hot.traverse_bfs(start_rowid, max_depth, edge_labels);
        visited.truncate(limit as usize);
        visited
    }

    /// Spatial bounding-box query (write lock — reuses internal buffers, O(n) gen-counter dedup).
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
        let mut hot = self.hot.write().unwrap();
        let mut out = Vec::new();
        hot.query_spatial_into(min_x, min_y, max_x, max_y, lod, &mut out);
        out.truncate(limit as usize);
        out
    }

    /// Property search — always queries cold tier (SQLite).
    pub fn search_property(
        &self,
        key: &str,
        limit: u32,
    ) -> Result<Vec<u64>, StoreError> {
        let cold = self.cold.lock().unwrap();
        cold.search_property(key, limit)
    }

    /// Load entity properties from cold tier.
    pub fn load_properties(
        &self,
        entity_id: u64,
    ) -> Result<Vec<crate::cold_tier::PropertyRow>, StoreError> {
        let cold = self.cold.lock().unwrap();
        cold.load_properties(entity_id)
    }

    /// Load blob references for an entity.
    pub fn load_blob_refs(
        &self,
        entity_id: u64,
    ) -> Result<Vec<crate::cold_tier::BlobRow>, StoreError> {
        let cold = self.cold.lock().unwrap();
        cold.load_blob_refs(entity_id)
    }

    /// Load a blob chunk by hash.
    pub fn load_blob_chunk(
        &self,
        hash: u64,
        offset: u64,
        chunk_size: u32,
    ) -> Result<Option<Vec<u8>>, StoreError> {
        let cold = self.cold.lock().unwrap();
        cold.load_blob_data(hash, offset, chunk_size)
    }

    /// Load blob metadata by hash.
    pub fn load_blob_by_hash(
        &self,
        hash: u64,
    ) -> Result<Option<crate::cold_tier::BlobRow>, StoreError> {
        let cold = self.cold.lock().unwrap();
        cold.load_blob_by_hash(hash)
    }

    /// Get database stats.
    pub fn stats(&self) -> Result<crate::cold_tier::GraphStats, StoreError> {
        let cold = self.cold.lock().unwrap();
        cold.stats()
    }

    /// Check whether an entity rowid exists in the graph.
    #[must_use]
    pub fn find_entity_index(&self, rowid: u64) -> Option<u32> {
        let hot = self.hot.read().unwrap();
        hot.edge_csr.find_entity_index(rowid)
    }

    /// Get read access to the hot tier (for diagnostics/metrics).
    #[must_use]
    pub fn hot_tier_for_read(&self) -> std::sync::RwLockReadGuard<'_, crate::hot_tier::HotTier> {
        self.hot.read().unwrap()
    }

    /// Get the default query LOD from the spatial index.
    #[must_use]
    pub fn spatial_lod(&self) -> u16 {
        let hot = self.hot.read().unwrap();
        hot.spatial_index.as_ref().map_or(4, |idx| idx.bits())
    }

    /// Check memory pressure and evict if needed.
    fn maybe_evict_inner(&self, hot: &mut HotTier) -> Result<(), StoreError> {
        if !hot.over_threshold() {
            return Ok(());
        }

        let evicted = hot.evict_lru(self.config.evict_batch_size);
        if !evicted.is_empty() {
            let mut warm = self.warm.lock().unwrap();
            for (rowid, meta) in &evicted {
                let _ = warm.put(*rowid, meta);
            }
        }

        Ok(())
    }
}
