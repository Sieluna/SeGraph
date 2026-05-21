use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

use crate::hot_tier::EntityMeta;

/// Per-entity cache entry in the mmap file.
#[derive(Clone, Copy, Debug)]
struct CacheEntry {
    offset: usize,
    len: usize,
}

/// Warm tier: mmap-backed disk cache for evicted hot tier entities.
///
/// Serialization format per entity (little-endian):
/// - rowid: u64 (8 bytes)
/// - payload_len: u32 (4 bytes)
/// - has_position: u8 (1 byte)
/// - position_x: f32 (4 bytes, if has_position)
/// - position_y: f32 (4 bytes, if has_position)
/// - last_access: u64 (8 bytes)
/// Total: 17 bytes without position, 25 bytes with position
pub struct WarmTier {
    file: File,
    index: HashMap<u64, CacheEntry>,
    write_offset: usize,
    capacity: usize,
}

impl WarmTier {
    pub fn create(path: impl AsRef<Path>, capacity: usize) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        file.set_len(capacity as u64)?;
        Ok(Self {
            file,
            index: HashMap::new(),
            write_offset: 0,
            capacity,
        })
    }

    /// Open an existing warm tier cache file.
    pub fn open(path: impl AsRef<Path>, capacity: usize) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        let metadata = file.metadata()?;
        let file_len = metadata.len() as usize;
        let mut this = Self {
            file,
            index: HashMap::new(),
            write_offset: 0,
            capacity: capacity.max(file_len),
        };
        if file_len > 0 {
            this.rebuild_index()?;
        }
        Ok(this)
    }

    /// Write an evicted entity to the warm tier.
    pub fn put(&mut self, rowid: u64, meta: &EntityMeta) -> io::Result<()> {
        let mut buf = [0u8; 32]; // max size
        let mut pos = 0;

        // rowid
        buf[pos..pos + 8].copy_from_slice(&rowid.to_le_bytes());
        pos += 8;

        // Compute payload size and write header
        let has_pos = meta.position.is_some();
        let payload_len: u32 = if has_pos { 9 + 8 } else { 1 + 8 }; // has_position + last_access [+ position]
        buf[pos..pos + 4].copy_from_slice(&payload_len.to_le_bytes());
        pos += 4;

        // has_position
        buf[pos] = if has_pos { 1 } else { 0 };
        pos += 1;

        // position
        if let Some((x, y)) = meta.position {
            buf[pos..pos + 4].copy_from_slice(&x.to_le_bytes());
            pos += 4;
            buf[pos..pos + 4].copy_from_slice(&y.to_le_bytes());
            pos += 4;
        }

        // last_access
        buf[pos..pos + 8].copy_from_slice(&meta.last_access.to_le_bytes());
        pos += 8;

        let total_len = pos;
        let entry_len = 8 + total_len; // rowid + payload

        // If we're near capacity, wrap around or grow
        if self.write_offset + entry_len > self.capacity {
            self.write_offset = 0; // simple circular buffer
        }

        // Remove old entry at this location from index
        self.index.retain(|_, entry| entry.offset != self.write_offset);

        // Use OS-level pwrite to write at offset
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            self.file.write_all_at(&buf[..total_len], self.write_offset as u64)?;
        }
        #[cfg(not(unix))]
        {
            use std::os::windows::fs::FileExt;
            self.file.seek_write(&buf[..total_len], self.write_offset as u64)?;
        }

        let entry = CacheEntry {
            offset: self.write_offset,
            len: total_len,
        };
        self.index.insert(rowid, entry);
        self.write_offset += entry_len;

        Ok(())
    }

    /// Read an entity from the warm tier.
    pub fn get(&self, rowid: u64) -> io::Result<Option<EntityMeta>> {
        let Some(&entry) = self.index.get(&rowid) else {
            return Ok(None);
        };

        let mut buf = vec![0u8; entry.len];

        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            self.file.read_exact_at(&mut buf, entry.offset as u64)?;
        }
        #[cfg(not(unix))]
        {
            use std::os::windows::fs::FileExt;
            self.file.seek_read(&mut buf, entry.offset as u64)?;
        }

        let mut pos = 0;

        // Skip rowid (8 bytes)
        pos += 8;

        // payload_len (4 bytes)
        let _payload_len = u32::from_le_bytes([buf[pos], buf[pos + 1], buf[pos + 2], buf[pos + 3]]);
        pos += 4;

        // has_position
        let has_pos = buf[pos] != 0;
        pos += 1;

        let position = if has_pos {
            let x = f32::from_le_bytes([buf[pos], buf[pos + 1], buf[pos + 2], buf[pos + 3]]);
            pos += 4;
            let y = f32::from_le_bytes([buf[pos], buf[pos + 1], buf[pos + 2], buf[pos + 3]]);
            pos += 4;
            Some((x, y))
        } else {
            None
        };

        let last_access =
            u64::from_le_bytes([buf[pos], buf[pos + 1], buf[pos + 2], buf[pos + 3], buf[pos + 4], buf[pos + 5], buf[pos + 6], buf[pos + 7]]);

        Ok(Some(EntityMeta {
            position,
            last_access,
        }))
    }

    /// Remove an entity from the warm tier index (logical delete).
    pub fn remove(&mut self, rowid: u64) {
        self.index.remove(&rowid);
    }

    /// Number of cached entities.
    #[must_use]
    pub fn len(&self) -> usize {
        self.index.len()
    }

    /// Check if an entity is in the warm tier.
    #[must_use]
    pub fn contains(&self, rowid: u64) -> bool {
        self.index.contains_key(&rowid)
    }

    fn rebuild_index(&mut self) -> io::Result<()> {
        use std::io::{Seek, SeekFrom, Read};

        self.index.clear();
        let mut offset = 0usize;
        let metadata = self.file.metadata()?;
        let file_len = metadata.len() as usize;
        let mut header = [0u8; 12]; // rowid (8) + payload_len (4)

        while offset + 12 <= file_len {
            self.file.seek(SeekFrom::Start(offset as u64))?;
            self.file.read_exact(&mut header)?;

            let rowid = u64::from_le_bytes(header[..8].try_into().unwrap());
            let payload_len =
                u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize;

            if rowid == 0 && payload_len == 0 {
                // Empty slot or end
                offset += 12;
                continue;
            }

            let total_len = 12 + payload_len;
            self.index.insert(
                rowid,
                CacheEntry {
                    offset: offset + 8, // payload starts after rowid
                    len: payload_len,
                },
            );
            offset += total_len;
            if offset > file_len {
                break;
            }
        }

        if offset > self.write_offset {
            self.write_offset = offset;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tempfile::NamedTempFile;

    use super::*;

    #[test]
    fn round_trips_entity_meta() {
        let file = NamedTempFile::new().unwrap();
        let mut warm = WarmTier::create(file.path(), 4096).unwrap();

        let meta = EntityMeta {
            position: Some((0.5, -0.25)),
            last_access: 42,
        };
        warm.put(1, &meta).unwrap();
        warm.put(2, &EntityMeta {
            position: None,
            last_access: 7,
        }).unwrap();

        let read = warm.get(1).unwrap().unwrap();
        assert_eq!(read.position, Some((0.5, -0.25)));
        assert_eq!(read.last_access, 42);

        let read2 = warm.get(2).unwrap().unwrap();
        assert_eq!(read2.position, None);
        assert_eq!(read2.last_access, 7);

        assert!(warm.get(99).unwrap().is_none());
    }

    #[test]
    fn remove_works() {
        let file = NamedTempFile::new().unwrap();
        let mut warm = WarmTier::create(file.path(), 4096).unwrap();
        warm.put(1, &EntityMeta {
            position: None,
            last_access: 0,
        }).unwrap();
        assert!(warm.contains(1));
        warm.remove(1);
        assert!(!warm.contains(1));
    }
}
