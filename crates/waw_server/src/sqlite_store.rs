use std::{error, fmt, path::Path};

use rusqlite::{Connection, OpenFlags};
use serde::Serialize;

#[derive(Debug)]
pub enum StoreError {
    Sqlite(rusqlite::Error),
}

impl From<rusqlite::Error> for StoreError {
    fn from(source: rusqlite::Error) -> Self {
        Self::Sqlite(source)
    }
}

impl fmt::Display for StoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sqlite(source) => source.fmt(formatter),
        }
    }
}

impl error::Error for StoreError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Self::Sqlite(source) => Some(source),
        }
    }
}

/// A property key-value pair loaded from SQLite.
#[derive(Clone, Debug, PartialEq)]
pub struct PropertyRow {
    pub entity_id: u64,
    pub key: String,
    pub value_type: u8,
    pub value_int: Option<i64>,
    pub value_float: Option<f64>,
    pub value_text: Option<String>,
}

/// Blob metadata row (data is loaded separately).
#[derive(Clone, Debug, PartialEq)]
pub struct BlobRow {
    pub entity_id: u64,
    pub key: String,
    pub hash: u64,
    pub mime: String,
    pub size_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct GraphStats {
    pub entities: u64,
    pub edges: u64,
    pub blobs: u64,
    pub blob_bytes: u64,
}

/// Maximum memory-mapped I/O size (1 GiB).
///
/// SQLite will map up to this many bytes of the database file into the process
/// address space, reducing read syscalls for random-access patterns like blob
/// and property lookups.
const MMAP_SIZE: i64 = 1_073_741_824;

/// Page cache size in KiB (negative = kibibytes, positive = pages).
///
/// 200 000 KiB ≈ 195 MiB of cache for frequently accessed pages.
const CACHE_SIZE: i64 = -200_000;

/// Read-only SQLite access layer for graph data.
///
/// Opens the database with mmap, memory temp store, and a large page cache
/// for read-heavy workloads.
pub struct SqliteGraphStore {
    pub(crate) connection: Connection,
}

impl SqliteGraphStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let connection = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        connection.pragma_update(None, "query_only", true)?;
        connection.pragma_update(None, "temp_store", "MEMORY")?;
        connection.pragma_update(None, "mmap_size", MMAP_SIZE)?;
        connection.pragma_update(None, "cache_size", CACHE_SIZE)?;
        Ok(Self { connection })
    }

    pub fn stats(&self) -> Result<GraphStats, StoreError> {
        Ok(self.connection.query_row(
            r#"
                SELECT
                    COALESCE((SELECT COUNT(*) FROM entity), 0),
                    COALESCE((SELECT COUNT(*) FROM edge), 0),
                    COALESCE((SELECT COUNT(*) FROM blob_store), 0),
                    COALESCE((SELECT SUM(size_bytes) FROM blob_store), 0)
                "#,
            [],
            |row| {
                Ok(GraphStats {
                    entities: get_u64(row, 0)?,
                    edges: get_u64(row, 1)?,
                    blobs: get_u64(row, 2)?,
                    blob_bytes: get_u64(row, 3)?,
                })
            },
        )?)
    }

    /// Load properties for an entity.
    pub fn load_properties(&self, entity_id: u64) -> Result<Vec<PropertyRow>, StoreError> {
        let mut stmt = self.connection.prepare(
            "SELECT entity_id, key, value_type, value_int, value_float, value_text \
             FROM property WHERE entity_id = ?1 ORDER BY key",
        )?;
        let rows = stmt.query_map([entity_id as i64], |row| {
            Ok(PropertyRow {
                entity_id: row.get::<_, i64>(0)? as u64,
                key: row.get(1)?,
                value_type: row.get::<_, i32>(2)? as u8,
                value_int: row.get(3)?,
                value_float: row.get(4)?,
                value_text: row.get(5)?,
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    /// Load the (x, y) position for an entity.
    pub fn load_position(&self, entity_id: u64) -> Result<Option<(f32, f32)>, StoreError> {
        let mut stmt = self
            .connection
            .prepare("SELECT x, y FROM position_component WHERE entity_id = ?1")?;
        let result = stmt.query_map([entity_id as i64], |row| {
            Ok((row.get::<_, f64>(0)? as f32, row.get::<_, f64>(1)? as f32))
        })?;
        result.into_iter().next().transpose().map_err(Into::into)
    }

    /// Load blob metadata for an entity (without the blob data).
    pub fn load_blob_refs(&self, entity_id: u64) -> Result<Vec<BlobRow>, StoreError> {
        let mut stmt = self.connection.prepare(
            "SELECT entity_id, key, hash, mime, size_bytes \
             FROM blob_store WHERE entity_id = ?1 ORDER BY key",
        )?;
        let rows = stmt.query_map([entity_id as i64], |row| {
            Ok(BlobRow {
                entity_id: row.get::<_, i64>(0)? as u64,
                key: row.get(1)?,
                hash: row.get::<_, i64>(2)? as u64,
                mime: row.get::<_, String>(3).unwrap_or_default(),
                size_bytes: row.get::<_, i64>(4)? as u64,
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    /// Load a chunk of blob data by hash, starting at `offset` bytes.
    ///
    /// Uses SQLite's `substr()` to read only the requested range from disk
    /// rather than loading the entire blob into memory.
    pub fn load_blob_data(
        &self,
        hash: u64,
        offset: u64,
        chunk_size: u32,
    ) -> Result<Option<Vec<u8>>, StoreError> {
        let mut stmt = self.connection.prepare(
            "SELECT substr(data, ?2, ?3) FROM blob_store WHERE hash = ?1",
        )?;
        stmt.query_map(
            rusqlite::params![hash as i64, offset as i64 + 1, chunk_size as i64],
            |row| row.get(0),
        )?
        .next()
        .transpose()
        .map_err(Into::into)
    }

    /// Load blob metadata by hash.
    pub fn load_blob_by_hash(&self, hash: u64) -> Result<Option<BlobRow>, StoreError> {
        let mut stmt = self.connection.prepare(
            "SELECT entity_id, key, hash, mime, size_bytes \
             FROM blob_store WHERE hash = ?1 LIMIT 1",
        )?;
        let result = stmt.query_map([hash as i64], |row| {
            Ok(BlobRow {
                entity_id: row.get::<_, i64>(0)? as u64,
                key: row.get(1)?,
                hash: row.get::<_, i64>(2)? as u64,
                mime: row.get::<_, String>(3).unwrap_or_default(),
                size_bytes: row.get::<_, i64>(4)? as u64,
            })
        })?;

        result.into_iter().next().transpose().map_err(Into::into)
    }

}

fn get_u64(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<u64> {
    let value = row.get::<_, i64>(index)?;
    u64::try_from(value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            rusqlite::types::Type::Integer,
            Box::new(error),
        )
    })
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;
    use tempfile::NamedTempFile;

    use super::*;

    fn create_schema(conn: &Connection) {
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
            "#,
        )
        .unwrap();
    }

    #[test]
    fn reads_stats_from_new_schema() {
        let file = NamedTempFile::new().unwrap();
        {
            let conn = Connection::open(file.path()).unwrap();
            create_schema(&conn);
            conn.execute_batch(
                r#"
                INSERT INTO entity VALUES (1), (2), (3);
                INSERT INTO edge VALUES (1, 1, 2, 1);
                INSERT INTO blob_store VALUES (1, 'audio', 100, 'audio/ogg', 4096, x'00010203');
                "#,
            )
            .unwrap();
        }
        let store = SqliteGraphStore::open(file.path()).unwrap();
        let stats = store.stats().unwrap();
        assert_eq!(stats.entities, 3);
        assert_eq!(stats.edges, 1);
        assert_eq!(stats.blobs, 1);
        assert_eq!(stats.blob_bytes, 4096);
    }

    #[test]
    fn loads_properties_for_entity() {
        let file = NamedTempFile::new().unwrap();
        {
            let conn = Connection::open(file.path()).unwrap();
            create_schema(&conn);
            conn.execute_batch(
                r#"
                INSERT INTO property VALUES (1, 'name', 3, NULL, NULL, 'test');
                INSERT INTO property VALUES (1, 'count', 1, 42, NULL, NULL);
                INSERT INTO property VALUES (1, 'weight', 2, NULL, 0.75, NULL);
                "#,
            )
            .unwrap();
        }
        let store = SqliteGraphStore::open(file.path()).unwrap();
        let props = store.load_properties(1).unwrap();
        assert_eq!(props.len(), 3);
        assert_eq!(props[0].key, "count");
        assert_eq!(props[0].value_int, Some(42));
        assert_eq!(props[1].key, "name");
        assert_eq!(props[1].value_text.as_deref(), Some("test"));
        assert_eq!(props[2].value_float, Some(0.75));
    }

    #[test]
    fn loads_blob_refs() {
        let file = NamedTempFile::new().unwrap();
        {
            let conn = Connection::open(file.path()).unwrap();
            create_schema(&conn);
            conn.execute_batch(
                r#"
                INSERT INTO blob_store VALUES (1, 'data', 999, 'application/octet-stream', 1024, x'ABCD');
                "#,
            )
            .unwrap();
        }
        let store = SqliteGraphStore::open(file.path()).unwrap();
        let refs = store.load_blob_refs(1).unwrap();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].hash, 999);
        assert_eq!(refs[0].mime, "application/octet-stream");
        assert_eq!(refs[0].size_bytes, 1024);
    }

    #[test]
    fn loads_blob_data_by_hash() {
        let file = NamedTempFile::new().unwrap();
        {
            let conn = Connection::open(file.path()).unwrap();
            create_schema(&conn);
            conn.execute_batch(
                "INSERT INTO blob_store VALUES (1, 'data', 999, '', 4, x'DEADBEEF');",
            )
            .unwrap();
        }
        let store = SqliteGraphStore::open(file.path()).unwrap();
        let data = store.load_blob_data(999, 0, 1024).unwrap();
        assert_eq!(data, Some(vec![0xDE, 0xAD, 0xBE, 0xEF]));
    }

}
