use std::collections::HashMap;
use std::collections::VecDeque;

use crate::cold_tier::{ColdTier, StoreError};
use crate::query_ctx::QueryContext;
use crate::spatial_index::SpatialIndex;

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

/// Compressed Sparse Row edge index — always resident, read-only after load.
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

/// Immutable graph topology and spatial index — read-only after load, zero lock needed.
pub struct GraphIndex {
    pub edge_csr: EdgeCsr,
    pub spatial_index: Option<SpatialIndex>,
}

impl GraphIndex {
    /// Bulk load graph topology from the cold tier.
    ///
    /// Returns `(GraphIndex, position_pairs, entity_count, edge_memory_bytes)`.
    pub fn load(
        cold: &ColdTier,
    ) -> Result<(Self, Vec<(u32, f32, f32)>, usize, usize), StoreError> {
        let mut edge_csr = EdgeCsr::empty();

        // 1. Load entity IDs
        let rowids = cold.load_entity_ids()?;
        let entity_count = rowids.len();
        edge_csr.entity_rowids.reserve(entity_count);
        edge_csr.entity_by_rowid.reserve(entity_count);

        for (i, &rowid) in rowids.iter().enumerate() {
            edge_csr.entity_rowids.push(rowid);
            edge_csr.entity_by_rowid.insert(rowid, i as u32);
        }

        // 2. Load edges and build CSR
        let edges = cold.load_all_edges()?;
        let mut out_edges: Vec<(u32, u32, u16, u64)> = Vec::with_capacity(edges.len());
        let mut in_edges: Vec<(u32, u32, u16, u64)> = Vec::with_capacity(edges.len());

        for e in &edges {
            if let (Some(&src), Some(&tgt)) = (
                edge_csr.entity_by_rowid.get(&e.source_entity),
                edge_csr.entity_by_rowid.get(&e.target_entity),
            ) {
                out_edges.push((src, tgt, e.label as u16, e.id));
                in_edges.push((tgt, src, e.label as u16, e.id));
            }
        }

        out_edges.sort_unstable_by_key(|e| (e.0, e.1));
        build_csr(
            &out_edges,
            entity_count,
            &mut edge_csr.targets,
            &mut edge_csr.labels,
            &mut edge_csr.rowids,
            &mut edge_csr.offsets,
        );

        in_edges.sort_unstable_by_key(|e| (e.0, e.1));
        build_csr(
            &in_edges,
            entity_count,
            &mut edge_csr.sources,
            &mut edge_csr.in_labels,
            &mut edge_csr.in_rowids,
            &mut edge_csr.in_offsets,
        );

        let mut edge_memory = 0usize;
        edge_memory += edge_csr.targets.len() * 4;
        edge_memory += edge_csr.labels.len() * 2;
        edge_memory += edge_csr.rowids.len() * 8;
        edge_memory += edge_csr.offsets.len() * 4;
        edge_memory += edge_csr.sources.len() * 4;
        edge_memory += edge_csr.in_labels.len() * 2;
        edge_memory += edge_csr.in_rowids.len() * 8;
        edge_memory += edge_csr.in_offsets.len() * 4;

        // 3. Load positions and build spatial index
        let positions = cold.load_all_positions()?;
        let (spatial_index, pos_pairs) = if !positions.is_empty() {
            let mut pos_pairs: Vec<(u32, f32, f32)> =
                Vec::with_capacity(positions.len());
            for &(rowid, x, y) in &positions {
                if let Some(&csr_idx) = edge_csr.entity_by_rowid.get(&rowid) {
                    pos_pairs.push((csr_idx, x, y));
                }
            }
            let bits = spatial_bits_for_count(entity_count);
            let idx = SpatialIndex::build(&pos_pairs, bits);
            (Some(idx), pos_pairs)
        } else {
            (None, Vec::new())
        };

        Ok((
            Self {
                edge_csr,
                spatial_index,
            },
            pos_pairs,
            entity_count,
            edge_memory,
        ))
    }

    #[must_use]
    pub fn find_entity_index(&self, rowid: u64) -> Option<u32> {
        self.edge_csr.entity_by_rowid.get(&rowid).copied()
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

fn spatial_bits_for_count(node_count: usize) -> u16 {
    if node_count == 0 {
        return 4;
    }
    let bits = ((node_count as f64).log2() / 2.0).round() as i32;
    bits.clamp(4, 8) as u16
}

/// BFS implementation using generation-counter for visited detection.
/// No buffer-clearing needed — `gen` is bumped per traversal.
fn bfs_impl(
    csr: &EdgeCsr,
    start_rowid: u64,
    max_depth: u32,
    edge_labels: &[u32],
    visited: &mut [u32],
    generation: u32,
    queue: &mut VecDeque<(u32, u32)>,
    out: &mut Vec<u64>,
) {
    let start_idx = match csr.find_entity_index(start_rowid) {
        Some(idx) => idx,
        None => return,
    };
    let start_i = start_idx as usize;

    visited[start_i] = generation;
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
                if visited[ti] != generation {
                    visited[ti] = generation;
                    queue.push_back((target, depth + 1));
                    out.push(unsafe { *csr.entity_rowids.get_unchecked(ti) });
                }
            }
        } else {
            for pos in start..end {
                let target = unsafe { *csr.targets.get_unchecked(pos) };
                let ti = target as usize;
                if visited[ti] != generation {
                    visited[ti] = generation;
                    queue.push_back((target, depth + 1));
                    out.push(unsafe { *csr.entity_rowids.get_unchecked(ti) });
                }
            }
        }
    }
}

impl GraphIndex {
    /// BFS traversal — lock-free, uses thread-local buffers with generation-counter dedup.
    #[must_use]
    pub fn traverse_bfs(
        &self,
        start_rowid: u64,
        max_depth: u32,
        edge_labels: &[u32],
    ) -> Vec<u64> {
        let mut result = Vec::new();
        QueryContext::with(|ctx| {
            let entity_count = self.edge_csr.entity_count();
            if ctx.bfs_visited.len() < entity_count {
                ctx.bfs_visited.resize(entity_count, 0);
            }
            // Generation counter — no O(entity_count) fill needed
            ctx.bfs_gen = ctx.bfs_gen.wrapping_add(1);
            let generation = ctx.bfs_gen;
            ctx.queue_buf.clear();
            bfs_impl(
                &self.edge_csr,
                start_rowid,
                max_depth,
                edge_labels,
                &mut ctx.bfs_visited,
                generation,
                &mut ctx.queue_buf,
                &mut result,
            );
        });
        result
    }

    /// Spatial query — reads spatial index + entity_rowids (lock-free on self),
    /// checks `entity_ptrs` (caller provides under read lock).
    pub fn query_spatial_into(
        &self,
        min_x: f32,
        min_y: f32,
        max_x: f32,
        max_y: f32,
        lod: u16,
        entity_ptrs: &[Option<waw_core::Pointer<crate::entity_store::EntityMeta>>],
        out: &mut Vec<u64>,
    ) {
        out.clear();
        let Some(ref index) = self.spatial_index else {
            return;
        };

        QueryContext::with(|ctx| {
            ctx.spatial_buf.clear();
            index.query_bounds_into(min_x, min_y, max_x, max_y, lod, &mut ctx.spatial_buf);

            let entity_count = self.edge_csr.entity_rowids.len();
            if ctx.spatial_buf.is_empty() || entity_count == 0 {
                return;
            }

            ctx.seen_gen = ctx.seen_gen.wrapping_add(1);
            let generation = ctx.seen_gen;

            if ctx.seen_set.len() < entity_count {
                ctx.seen_set.resize(entity_count, 0);
            }

            let entity_rowids = &self.edge_csr.entity_rowids;
            let seen = &mut ctx.seen_set;

            for &idx in &ctx.spatial_buf {
                let i = idx as usize;
                if i >= entity_count {
                    continue;
                }
                let slot = unsafe { seen.get_unchecked_mut(i) };
                if *slot == generation {
                    continue;
                }
                *slot = generation;
                if entity_ptrs[i].is_some() {
                    out.push(*unsafe { entity_rowids.get_unchecked(i) });
                }
            }
        });
    }

    #[must_use]
    pub fn spatial_lod(&self) -> u16 {
        self.spatial_index.as_ref().map_or(4, |idx| idx.bits())
    }
}
