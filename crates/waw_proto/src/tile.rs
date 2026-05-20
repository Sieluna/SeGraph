use bytes::Bytes;

pub const TILE_FRAME_MAGIC: u32 = 0x47444147; // 'GDAG'
pub const TILE_FRAME_VERSION: u16 = 1;
pub const TILE_BATCH_HEADER_BYTES: usize = 18;
pub const TILE_ENTRY_BYTES: usize = 16;

/// Packed tile metadata for batch encoding.
#[derive(Clone, Copy, Debug)]
pub struct TileMeta {
    pub tx: u32,
    pub ty: u32,
    pub layer: u8,
    pub codec: u8,
    pub node_count: u32,
    pub edge_count: u32,
}

/// Decoded result from a tile batch frame.
#[derive(Clone, Debug)]
pub struct DecodedTile {
    pub tx: u32,
    pub ty: u32,
    pub layer: u8,
    pub codec: u8,
    pub node_count: u32,
    pub edge_count: u32,
    pub payload: Bytes,
}

#[derive(Clone, Debug)]
pub struct DecodedTileBatch {
    pub request_id: u32,
    pub lod: u8,
    pub tiles: Vec<DecodedTile>,
}

/// Encode multiple tiles into a single contiguous binary frame.
///
/// Layout: BatchHeader(18B) | TileEntry[N](16B each) | payloads...
#[must_use]
pub fn encode_tile_batch(request_id: u32, lod: u8, tiles: &[(TileMeta, Bytes)]) -> Vec<u8> {
    let payload_total: usize = tiles.iter().map(|(_, p)| p.len()).sum();
    let buf_len = TILE_BATCH_HEADER_BYTES + tiles.len() * TILE_ENTRY_BYTES + payload_total;
    let mut buf = Vec::with_capacity(buf_len);

    let tile_count = tiles.len().min(u8::MAX as usize) as u8;
    let base_tx = tiles.iter().map(|(m, _)| m.tx).min().unwrap_or(0);
    let base_ty = tiles.iter().map(|(m, _)| m.ty).min().unwrap_or(0);

    // Batch header (18 bytes, little-endian)
    buf.extend_from_slice(&TILE_FRAME_MAGIC.to_le_bytes()); // 0..4
    buf.extend_from_slice(&TILE_FRAME_VERSION.to_le_bytes()); // 4..6
    buf.extend_from_slice(&request_id.to_le_bytes()); // 6..10
    buf.extend_from_slice(&(base_tx as u16).to_le_bytes()); // 10..12
    buf.extend_from_slice(&(base_ty as u16).to_le_bytes()); // 12..14
    buf.push(lod); // 14
    buf.push(tile_count); // 15
    buf.push(0); // flags
    buf.push(0); // reserved

    let mut payload_offset = 0u32;
    for (meta, payload) in tiles {
        let delta_tx = (meta.tx as i32 - base_tx as i32).clamp(-128, 127) as i8;
        let delta_ty = (meta.ty as i32 - base_ty as i32).clamp(-128, 127) as i8;

        buf.push(delta_tx as u8);
        buf.push(delta_ty as u8);
        buf.push(meta.layer);
        buf.push(meta.codec);
        buf.extend_from_slice(&(meta.node_count.min(u16::MAX as u32) as u16).to_le_bytes());
        buf.extend_from_slice(&(meta.edge_count.min(u16::MAX as u32) as u16).to_le_bytes());
        buf.extend_from_slice(&payload_offset.to_le_bytes());
        buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());

        payload_offset += payload.len() as u32;
    }

    // Payloads
    for (_, payload) in tiles {
        buf.extend_from_slice(payload);
    }

    debug_assert_eq!(buf.len(), buf_len);
    buf
}

/// Decode a tile batch frame. Returns `None` if the buffer is malformed.
pub fn decode_tile_batch(buf: &[u8]) -> Option<DecodedTileBatch> {
    if buf.len() < TILE_BATCH_HEADER_BYTES {
        return None;
    }

    let magic = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
    if magic != TILE_FRAME_MAGIC {
        return None;
    }

    let version = u16::from_le_bytes([buf[4], buf[5]]);
    if version != TILE_FRAME_VERSION {
        return None;
    }
    let request_id = u32::from_le_bytes([buf[6], buf[7], buf[8], buf[9]]);
    let base_tx = u16::from_le_bytes([buf[10], buf[11]]) as u32;
    let base_ty = u16::from_le_bytes([buf[12], buf[13]]) as u32;
    let lod = buf[14];
    let tile_count = buf[15] as usize;

    let entries_start = TILE_BATCH_HEADER_BYTES;
    let entries_end = entries_start + tile_count * TILE_ENTRY_BYTES;
    if buf.len() < entries_end {
        return None;
    }

    let mut tiles = Vec::with_capacity(tile_count);
    for i in 0..tile_count {
        let off = entries_start + i * TILE_ENTRY_BYTES;
        let delta_tx = buf[off] as i8;
        let delta_ty = buf[off + 1] as i8;
        let layer = buf[off + 2];
        let codec = buf[off + 3];
        let node_count = u16::from_le_bytes([buf[off + 4], buf[off + 5]]) as u32;
        let edge_count = u16::from_le_bytes([buf[off + 6], buf[off + 7]]) as u32;
        let payload_offset = u32::from_le_bytes([
            buf[off + 8],
            buf[off + 9],
            buf[off + 10],
            buf[off + 11],
        ]) as usize;
        let payload_size = u32::from_le_bytes([
            buf[off + 12],
            buf[off + 13],
            buf[off + 14],
            buf[off + 15],
        ]) as usize;

        let payload_start = entries_end + payload_offset;
        let payload_end = payload_start + payload_size;
        if payload_end > buf.len() {
            return None;
        }

        tiles.push(DecodedTile {
            tx: base_tx.wrapping_add_signed(delta_tx as i32),
            ty: base_ty.wrapping_add_signed(delta_ty as i32),
            layer,
            codec,
            node_count,
            edge_count,
            payload: Bytes::copy_from_slice(&buf[payload_start..payload_end]),
        });
    }

    Some(DecodedTileBatch {
        request_id,
        lod,
        tiles,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_and_decodes_tile_batch_empty() {
        let buf = encode_tile_batch(1, 2, &[]);
        let decoded = decode_tile_batch(&buf).unwrap();
        assert_eq!(decoded.request_id, 1);
        assert_eq!(decoded.lod, 2);
        assert!(decoded.tiles.is_empty());
    }

    #[test]
    fn encodes_and_decodes_tile_batch_with_tiles() {
        let tiles = vec![
            (
                TileMeta {
                    tx: 100,
                    ty: 200,
                    layer: 1,
                    codec: 0,
                    node_count: 10,
                    edge_count: 0,
                },
                Bytes::from_static(&[0, 1, 2, 3]),
            ),
            (
                TileMeta {
                    tx: 105,
                    ty: 203,
                    layer: 2,
                    codec: 1,
                    node_count: 0,
                    edge_count: 5,
                },
                Bytes::from_static(&[4, 5]),
            ),
        ];
        let buf = encode_tile_batch(7, 3, &tiles);
        let decoded = decode_tile_batch(&buf).unwrap();
        assert_eq!(decoded.request_id, 7);
        assert_eq!(decoded.lod, 3);
        assert_eq!(decoded.tiles.len(), 2);
        assert_eq!(decoded.tiles[0].tx, 100);
        assert_eq!(decoded.tiles[0].ty, 200);
        assert_eq!(decoded.tiles[0].node_count, 10);
        assert_eq!(decoded.tiles[0].payload.len(), 4);
        assert_eq!(decoded.tiles[1].tx, 105);
        assert_eq!(decoded.tiles[1].ty, 203);
        assert_eq!(decoded.tiles[1].edge_count, 5);
        assert_eq!(decoded.tiles[1].payload.len(), 2);
    }

    #[test]
    fn tile_batch_rejects_bad_magic() {
        let mut buf = encode_tile_batch(1, 0, &[]);
        buf[0] = 0xFF;
        assert!(decode_tile_batch(&buf).is_none());
    }

    #[test]
    fn tile_batch_rejects_truncated() {
        let buf = encode_tile_batch(1, 0, &[]);
        assert!(decode_tile_batch(&buf[..4]).is_none());
    }

    #[test]
    fn tile_batch_rejects_wrong_version() {
        let mut buf = encode_tile_batch(1, 0, &[]);
        buf[5] = 0xFF;
        assert!(decode_tile_batch(&buf).is_none());
    }
}
