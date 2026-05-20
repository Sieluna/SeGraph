use waw_core::{Index, Pointer};

use crate::{graph_store::GraphEntity, tile_math::world_to_tile};

/// Spatial grid index mapping world-space coordinates to entity indices.
///
/// Uses a uniform grid at a fixed base LOD. Query LOD mappings:
/// - Query LOD == base LOD: 1 cell → 1 tile
/// - Query LOD <  base LOD: merge multiple cells → 1 tile
/// - Query LOD >  base LOD: split a cell across tiles
pub struct SpatialIndex {
    /// cells[ty * grid_size + tx] → entity indices in Storage
    cells: Box<[Vec<Index>]>,
    /// Keeps all indexed entities alive.
    handles: Vec<Pointer<GraphEntity>>,
    /// Grid resolution: grid_size = 1 << bits
    bits: u16,
    grid_size: u32,
}

impl SpatialIndex {
    /// Build a spatial index from entity positions.
    ///
    /// `positions` provides `(entity_pointer, x, y)` for each entity that has a position component.
    /// Entities without positions are simply omitted from the spatial index.
    /// `bits` controls grid resolution (default 6 → 64×64 cells).
    pub fn build(positions: &[(Pointer<GraphEntity>, f32, f32)], bits: u16) -> Self {
        let grid_size = 1u32 << bits;
        let cell_count = (grid_size * grid_size) as usize;
        let mut cells: Vec<Vec<Index>> = Vec::with_capacity(cell_count);
        cells.resize(cell_count, Vec::new());

        let mut handles: Vec<Pointer<GraphEntity>> = Vec::with_capacity(positions.len());

        for (ptr, x, y) in positions {
            let gx = world_to_tile(*x, grid_size);
            let gy = world_to_tile(*y, grid_size);
            let cell = (gy * grid_size + gx) as usize;
            cells[cell].push(ptr.data.get_index());
            handles.push(ptr.clone());
        }

        Self {
            cells: cells.into_boxed_slice(),
            handles,
            bits,
            grid_size,
        }
    }

    /// Grid resolution in bits.
    #[must_use]
    pub const fn bits(&self) -> u16 {
        self.bits
    }

    /// Number of cells per axis (2^bits).
    #[must_use]
    pub const fn grid_size(&self) -> u32 {
        self.grid_size
    }

    /// Number of indexed entities.
    #[must_use]
    pub fn len(&self) -> usize {
        self.handles.len()
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.handles.is_empty()
    }

    /// Returns entity indices in the cell at (tx, ty) in grid coordinates.
    #[must_use]
    pub fn get_cell(&self, tx: u32, ty: u32) -> &[Index] {
        if tx >= self.grid_size || ty >= self.grid_size {
            return &[];
        }
        let idx = (ty * self.grid_size + tx) as usize;
        &self.cells[idx]
    }

    /// Query entities within world-space bounds. Returns matching entity indices.
    ///
    /// The query LOD controls how many grid cells are scanned:
    /// - Query LOD == self.bits: direct cell lookup
    /// - Query LOD <  self.bits: scans (1 << (bits - query_lod))² cells per tile
    /// - Query LOD >  self.bits: finer filtering within a single cell
    ///
    /// An entity spanning multiple cells will appear multiple times in the result.
    /// Callers should deduplicate if unique entity indices are required.
    #[must_use]
    pub fn query_bounds(
        &self,
        min_x: f32,
        min_y: f32,
        max_x: f32,
        max_y: f32,
        query_lod: u16,
    ) -> Vec<Index> {
        let query_count = 1u32 << query_lod;
        let min_tx = world_to_tile(min_x, query_count);
        let max_tx = world_to_tile(max_x, query_count);
        let min_ty = world_to_tile(min_y, query_count);
        let max_ty = world_to_tile(max_y, query_count);

        let cell_ratio = (1u32 << self.bits.saturating_sub(query_lod)).max(1);
        let mut result = Vec::new();

        for ty in min_ty..=max_ty {
            let cell_ty_start = ty * cell_ratio;
            let cell_ty_end = ((ty + 1) * cell_ratio).min(self.grid_size);
            for tx in min_tx..=max_tx {
                let cell_tx_start = tx * cell_ratio;
                let cell_tx_end = ((tx + 1) * cell_ratio).min(self.grid_size);
                for cy in cell_ty_start..cell_ty_end {
                    let row_base = cy * self.grid_size;
                    for cx in cell_tx_start..cell_tx_end {
                        result.extend_from_slice(&self.cells[(row_base + cx) as usize]);
                    }
                }
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use waw_core::Storage;

    use crate::graph_store::GraphEntity;

    use super::*;

    fn make_entity(entities: &mut Storage<GraphEntity>, sqlite_rowid: u64) -> Pointer<GraphEntity> {
        entities.create(GraphEntity {
            sqlite_rowid,
            edges_out: Vec::new(),
            edges_in: Vec::new(),
        })
    }

    fn build_index(entities: &mut Storage<GraphEntity>) -> SpatialIndex {
        let e1 = make_entity(entities, 1);
        let e2 = make_entity(entities, 2);
        let e3 = make_entity(entities, 3);

        let positions = vec![
            (e1, -0.5f32, -0.5f32),
            (e2, 0.0f32, 0.0f32),
            (e3, 0.75f32, 0.75f32),
        ];

        SpatialIndex::build(&positions, 4)
    }

    #[test]
    fn builds_with_correct_cell_count() {
        let mut entities = Storage::new();
        let index = build_index(&mut entities);
        assert_eq!(index.grid_size(), 16);
        assert_eq!(index.len(), 3);
    }

    #[test]
    fn get_cell_returns_empty_for_out_of_bounds() {
        let mut entities = Storage::new();
        let index = build_index(&mut entities);
        assert!(index.get_cell(100, 100).is_empty());
    }

    #[test]
    fn query_bounds_finds_entities() {
        let mut entities = Storage::new();
        let index = build_index(&mut entities);
        entities.sync_pending();

        // Lower-left: x∈[-1,0], y∈[-1,0]
        //   (-0.5,-0.5)→cell(4,4), (0,0)→cell(8,8) on boundary
        let results = index.query_bounds(-1.0, -1.0, 0.0, 0.0, 4);
        assert_eq!(results.len(), 2, "lower-left: {results:?}");

        // Entire space → all 3
        let results = index.query_bounds(-1.0, -1.0, 1.0, 1.0, 4);
        assert_eq!(results.len(), 3, "full: {results:?}");

        // Upper-right: x∈[0,1], y∈[0,1]
        //   (0,0)→cell(8,8), (0.75,0.75)→cell(14,14)
        let results = index.query_bounds(0.0, 0.0, 1.0, 1.0, 4);
        assert_eq!(results.len(), 2, "upper-right-wide: {results:?}");

        // Tight upper-right (excludes origin) → only (0.75, 0.75)
        let results = index.query_bounds(0.5, 0.5, 1.0, 1.0, 4);
        assert_eq!(results.len(), 1, "tight-upper-right: {results:?}");
    }

    #[test]
    fn empty_bounds_returns_empty() {
        let mut entities = Storage::new();
        let index = build_index(&mut entities);
        let results = index.query_bounds(2.0, 2.0, 3.0, 3.0, 4);
        assert!(results.is_empty());
    }
}
