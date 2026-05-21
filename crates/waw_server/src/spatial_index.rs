use crate::tile_math::world_to_tile;

/// Spatial grid index mapping world-space coordinates to entity indices.
pub struct SpatialIndex {
    cells: Box<[Vec<u32>]>,
    bits: u16,
    grid_size: u32,
}

impl SpatialIndex {
    pub fn build(positions: &[(u32, f32, f32)], bits: u16) -> Self {
        let grid_size = 1u32 << bits;
        let cell_count = (grid_size * grid_size) as usize;
        let mut cells: Vec<Vec<u32>> = Vec::with_capacity(cell_count);
        cells.resize(cell_count, Vec::new());

        for &(idx, x, y) in positions {
            let gx = world_to_tile(x, grid_size);
            let gy = world_to_tile(y, grid_size);
            let cell = (gy * grid_size + gx) as usize;
            cells[cell].push(idx);
        }

        Self {
            cells: cells.into_boxed_slice(),
            bits,
            grid_size,
        }
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
        self.cells.iter().map(|c| c.len()).sum()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[must_use]
    pub fn get_cell(&self, tx: u32, ty: u32) -> &[u32] {
        if tx >= self.grid_size || ty >= self.grid_size {
            return &[];
        }
        let idx = (ty * self.grid_size + tx) as usize;
        &self.cells[idx]
    }

    #[must_use]
    pub fn query_bounds(
        &self,
        min_x: f32,
        min_y: f32,
        max_x: f32,
        max_y: f32,
        query_lod: u16,
    ) -> Vec<u32> {
        let mut result = Vec::new();
        self.query_bounds_into(min_x, min_y, max_x, max_y, query_lod, &mut result);
        result
    }

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
                        out.extend_from_slice(&self.cells[(row_base + cx) as usize]);
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
    fn query_bounds_finds_entities() {
        let index = build_index();
        let results = index.query_bounds(-1.0, -1.0, 0.0, 0.0, 4);
        assert_eq!(results.len(), 2);
        let results = index.query_bounds(-1.0, -1.0, 1.0, 1.0, 4);
        assert_eq!(results.len(), 3);
    }
}
