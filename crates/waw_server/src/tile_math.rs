#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Bounds {
    pub min_x: f32,
    pub min_y: f32,
    pub max_x: f32,
    pub max_y: f32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TileRange {
    pub min_tx: u32,
    pub max_tx: u32,
    pub min_ty: u32,
    pub max_ty: u32,
    pub center_tx: u32,
    pub center_ty: u32,
}

#[must_use]
pub fn tiles_for_bounds(bounds: Bounds, lod: u16) -> TileRange {
    let count = 1_u32 << lod;
    let min_tx = world_to_tile(bounds.min_x, count);
    let max_tx = world_to_tile(bounds.max_x, count);
    let min_ty = world_to_tile(bounds.min_y, count);
    let max_ty = world_to_tile(bounds.max_y, count);

    TileRange {
        min_tx,
        max_tx,
        min_ty,
        max_ty,
        center_tx: (min_tx + max_tx) / 2,
        center_ty: (min_ty + max_ty) / 2,
    }
}

#[must_use]
pub fn tile_distance(tx: u32, ty: u32, center_tx: u32, center_ty: u32) -> u64 {
    let dx = i64::from(tx) - i64::from(center_tx);
    let dy = i64::from(ty) - i64::from(center_ty);
    (dx * dx + dy * dy) as u64
}

#[must_use]
pub fn world_to_tile(value: f32, count: u32) -> u32 {
    let normalized = (value + 1.0) / 2.0;
    let tile = (normalized * count as f32).floor() as i64;
    tile.clamp(0, i64::from(count - 1)) as u32
}

#[cfg(test)]
mod tests {
    use super::{Bounds, tile_distance, tiles_for_bounds, world_to_tile};

    #[test]
    fn maps_world_coordinates_to_bounded_tiles() {
        assert_eq!(world_to_tile(-2.0, 4), 0);
        assert_eq!(world_to_tile(-1.0, 4), 0);
        assert_eq!(world_to_tile(0.0, 4), 2);
        assert_eq!(world_to_tile(1.0, 4), 3);
        assert_eq!(world_to_tile(2.0, 4), 3);
    }

    #[test]
    fn computes_viewport_tile_range_like_server_js() {
        let range = tiles_for_bounds(
            Bounds {
                min_x: -0.6,
                min_y: -0.2,
                max_x: 0.7,
                max_y: 0.8,
            },
            3,
        );

        assert_eq!(range.min_tx, 1);
        assert_eq!(range.max_tx, 6);
        assert_eq!(range.min_ty, 3);
        assert_eq!(range.max_ty, 7);
    }

    #[test]
    fn prioritizes_tiles_closer_to_center() {
        assert_eq!(tile_distance(5, 5, 5, 5), 0);
        assert_eq!(tile_distance(7, 5, 5, 5), 4);
        assert_eq!(tile_distance(7, 8, 5, 5), 13);
    }
}
