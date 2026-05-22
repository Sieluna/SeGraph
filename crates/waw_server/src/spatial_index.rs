use crate::tile_math::world_to_tile;

/// Flat SoA spatial grid index with inline entity positions for single-pass filtering.
pub struct SpatialIndex {
    pub cell_data: Vec<u32>,
    pub cell_positions: Vec<f32>,
    pub offsets: Vec<u32>,
    pub bits: u16,
    pub grid_size: u32,
}

impl SpatialIndex {
    /// Builds the index from CSR index and position tuples using a two-pass algorithm.
    pub fn build(positions: &[(u32, f32, f32)], bits: u16) -> Self {
        let grid_size = 1u32 << bits;
        let cell_count = (grid_size * grid_size) as usize;

        // Pass 1: count per cell
        let mut counts = vec![0u32; cell_count];
        for &(_, x, y) in positions {
            let gx = world_to_tile(x, grid_size);
            let gy = world_to_tile(y, grid_size);
            let cell = (gy * grid_size + gx) as usize;
            if cell < cell_count {
                counts[cell] += 1;
            }
        }

        // Build offsets (exclusive prefix sum)
        let mut offsets = Vec::with_capacity(cell_count + 1);
        let mut total = 0u32;
        for &c in &counts {
            offsets.push(total);
            total += c;
        }
        offsets.push(total);

        let total_entries = total as usize;
        let mut cell_data = vec![0u32; total_entries];
        let mut cell_positions = vec![0.0f32; 2 * total_entries];

        // Pass 2: fill — use write cursors cloned from offsets
        let mut write = offsets[..cell_count].to_vec();
        for &(idx, x, y) in positions {
            let gx = world_to_tile(x, grid_size);
            let gy = world_to_tile(y, grid_size);
            let cell = (gy * grid_size + gx) as usize;
            if cell >= cell_count {
                continue;
            }
            let wp = &mut write[cell];
            let pos = *wp as usize;
            cell_data[pos] = idx;
            cell_positions[2 * pos] = x;
            cell_positions[2 * pos + 1] = y;
            *wp += 1;
        }

        Self {
            cell_data,
            cell_positions,
            offsets,
            bits,
            grid_size,
        }
    }

    /// Returns an estimate of how many native cells a query would visit.
    #[must_use]
    pub fn estimate_cell_count(
        &self,
        min_x: f32,
        min_y: f32,
        max_x: f32,
        max_y: f32,
        query_lod: u16,
    ) -> u64 {
        let query_count = 1u32 << query_lod;
        let min_tx = world_to_tile(min_x, query_count) as u64;
        let max_tx = world_to_tile(max_x, query_count) as u64;
        let min_ty = world_to_tile(min_y, query_count) as u64;
        let max_ty = world_to_tile(max_y, query_count) as u64;
        let cell_ratio =
            (1u64 << self.bits.saturating_sub(query_lod)).max(1);
        (max_tx - min_tx + 1) * cell_ratio * (max_ty - min_ty + 1) * cell_ratio
    }

    /// Returns the total number of cells in the grid.
    #[must_use]
    pub fn total_cells(&self) -> u64 {
        (self.grid_size as u64) * (self.grid_size as u64)
    }

    #[must_use]
    pub const fn bits(&self) -> u16 {
        self.bits
    }

    #[must_use]
    pub const fn grid_size(&self) -> u32 {
        self.grid_size
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.cell_data.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.cell_data.is_empty()
    }

    /// Returns CSR indices for a single cell, or an empty slice if out of bounds.
    #[must_use]
    pub fn get_cell(&self, tx: u32, ty: u32) -> &[u32] {
        if tx >= self.grid_size || ty >= self.grid_size {
            return &[];
        }
        let cell = (ty * self.grid_size + tx) as usize;
        let start = self.offsets[cell] as usize;
        let end = self.offsets[cell + 1] as usize;
        &self.cell_data[start..end]
    }

    /// Collects CSR indices that overlap the query bounds without position filtering.
    pub fn query_bounds_into(
        &self,
        min_x: f32,
        min_y: f32,
        max_x: f32,
        max_y: f32,
        query_lod: u16,
        out: &mut Vec<u32>,
    ) {
        let query_count = 1u32 << query_lod;
        let min_tx = world_to_tile(min_x, query_count);
        let max_tx = world_to_tile(max_x, query_count);
        let min_ty = world_to_tile(min_y, query_count);
        let max_ty = world_to_tile(max_y, query_count);

        let cell_ratio = (1u32 << self.bits.saturating_sub(query_lod)).max(1);
        out.clear();

        for ty in min_ty..=max_ty {
            let cell_ty_start = ty * cell_ratio;
            let cell_ty_end = ((ty + 1) * cell_ratio).min(self.grid_size);
            for tx in min_tx..=max_tx {
                let cell_tx_start = tx * cell_ratio;
                let cell_tx_end = ((tx + 1) * cell_ratio).min(self.grid_size);
                for cy in cell_ty_start..cell_ty_end {
                    let row_base = cy * self.grid_size;
                    for cx in cell_tx_start..cell_tx_end {
                        let cell = (row_base + cx) as usize;
                        let start = self.offsets[cell] as usize;
                        let end = self.offsets[cell + 1] as usize;
                        out.extend_from_slice(&self.cell_data[start..end]);
                    }
                }
            }
        }
    }

    /// Returns entity rowids matching the query bounds after deduplication, hotness
    /// check, and exact position filtering. Stops early when the limit is reached.
    pub fn query_filtered_into(
        &self,
        min_x: f32,
        min_y: f32,
        max_x: f32,
        max_y: f32,
        query_lod: u16,
        entity_ptrs: &[Option<waw_core::Pointer<crate::entity_store::EntityMeta>>],
        entity_rowids: &[u64],
        seen: &mut [u32],
        generation: u32,
        out: &mut Vec<u64>,
        limit: usize,
    ) {
        let query_count = 1u32 << query_lod;
        let min_tx = world_to_tile(min_x, query_count);
        let max_tx = world_to_tile(max_x, query_count);
        let min_ty = world_to_tile(min_y, query_count);
        let max_ty = world_to_tile(max_y, query_count);

        let cell_ratio = (1u32 << self.bits.saturating_sub(query_lod)).max(1);
        let entity_count = entity_rowids.len();

        for ty in min_ty..=max_ty {
            let cell_ty_start = ty * cell_ratio;
            let cell_ty_end = ((ty + 1) * cell_ratio).min(self.grid_size);
            for tx in min_tx..=max_tx {
                let cell_tx_start = tx * cell_ratio;
                let cell_tx_end = ((tx + 1) * cell_ratio).min(self.grid_size);
                for cy in cell_ty_start..cell_ty_end {
                    let row_base = cy * self.grid_size;
                    for cx in cell_tx_start..cell_tx_end {
                        let cell = (row_base + cx) as usize;
                        let start = self.offsets[cell] as usize;
                        let end = self.offsets[cell + 1] as usize;

                        let indices = &self.cell_data[start..end];
                        let positions = &self.cell_positions[2 * start..2 * end];

                        for i in 0..indices.len() {
                            let csr_idx = indices[i] as usize;
                            if csr_idx >= entity_count {
                                continue;
                            }
                            if seen[csr_idx] == generation {
                                continue;
                            }
                            seen[csr_idx] = generation;
                            if entity_ptrs[csr_idx].is_none() {
                                continue;
                            }
                            let px = positions[2 * i];
                            let py = positions[2 * i + 1];
                            if px >= min_x && px <= max_x && py >= min_y && py <= max_y {
                                out.push(entity_rowids[csr_idx]);
                                if out.len() >= limit {
                                    return;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_index() -> SpatialIndex {
        let positions = vec![
            (0u32, -0.5f32, -0.5f32),
            (1u32, 0.0f32, 0.0f32),
            (2u32, 0.75f32, 0.75f32),
        ];
        SpatialIndex::build(&positions, 4)
    }

    #[test]
    fn builds_with_correct_cell_count() {
        let index = build_index();
        assert_eq!(index.grid_size(), 16);
        assert_eq!(index.len(), 3);
    }

    #[test]
    fn get_cell_reads_correctly() {
        let index = build_index();
        let cell = index.get_cell(0, 0);
        // (-0.5,-0.5) in grid_size 16 -> tile (4,4) -> cell 4*16+4 = 68
        let cell68 = index.get_cell(4, 4);
        assert!(!cell68.is_empty());
    }

    #[test]
    fn query_bounds_finds_entities() {
        let index = build_index();
        let mut out = Vec::new();
        index.query_bounds_into(-1.0, -1.0, 0.0, 0.0, 4, &mut out);
        assert_eq!(out.len(), 2);
        out.clear();
        index.query_bounds_into(-1.0, -1.0, 1.0, 1.0, 4, &mut out);
        assert_eq!(out.len(), 3);
    }
}
