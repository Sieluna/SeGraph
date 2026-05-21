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

#[derive(Clone, Debug, PartialEq)]
pub struct PropertyRow {
    pub entity_id: u64,
    pub key: String,
    pub value_type: u8,
    pub value_int: Option<i64>,
    pub value_float: Option<f64>,
    pub value_text: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BlobRow {
    pub entity_id: u64,
    pub key: String,
    pub hash: u64,
    pub mime: String,
    pub size_bytes: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct EdgeRow {
    pub id: u64,
    pub source_entity: u64,
    pub target_entity: u64,
    pub label: u32,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct GraphStats {
    pub entities: u64,
    pub edges: u64,
    pub blobs: u64,
    pub blob_bytes: u64,
}

const MMAP_SIZE: i64 = 1_073_741_824;
const CACHE_SIZE: i64 = -200_000;

/// Read-only SQLite access — cold tier data source.
pub struct ColdTier {
    pub(crate) connection: Connection,
}

impl ColdTier {
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
        self.connection
            .query_row(
                "SELECT \
                 COALESCE((SELECT COUNT(*) FROM entity), 0), \
                 COALESCE((SELECT COUNT(*) FROM edge), 0), \
                 COALESCE((SELECT COUNT(*) FROM blob_store), 0), \
                 COALESCE((SELECT SUM(size_bytes) FROM blob_store), 0)",
                [],
                |row| {
                    Ok(GraphStats {
                        entities: get_u64(row, 0)?,
                        edges: get_u64(row, 1)?,
                        blobs: get_u64(row, 2)?,
                        blob_bytes: get_u64(row, 3)?,
                    })
                },
            )
            .map_err(Into::into)
    }

    /// Load all entity IDs ordered by id — used for initial CSR construction.
    pub fn load_entity_ids(&self) -> Result<Vec<u64>, StoreError> {
        let mut stmt = self
            .connection
            .prepare("SELECT id FROM entity ORDER BY id")?;
        let rows = stmt.query_map([], |row| row.get::<_, i64>(0).map(|v| v as u64))?;
        rows.collect::<rusqlite::Result<Vec<_>>>().map_err(Into::into)
    }

    /// Load a batch of edges for paginated CSR construction.
    pub fn load_edge_batch(
        &self,
        offset: u64,
        limit: u64,
    ) -> Result<Vec<EdgeRow>, StoreError> {
        let mut stmt = self.connection.prepare(
            "SELECT id, source_entity, target_entity, label FROM edge \
             ORDER BY id LIMIT ?1 OFFSET ?2",
        )?;
        let rows = stmt.query_map([limit as i64, offset as i64], |row| {
            Ok(EdgeRow {
                id: row.get::<_, i64>(0)? as u64,
                source_entity: row.get::<_, i64>(1)? as u64,
                target_entity: row.get::<_, i64>(2)? as u64,
                label: row.get::<_, i32>(3)? as u32,
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>().map_err(Into::into)
    }

    /// Load all edges at once (for smaller graphs).
    pub fn load_all_edges(&self) -> Result<Vec<EdgeRow>, StoreError> {
        let mut stmt = self.connection.prepare(
            "SELECT id, source_entity, target_entity, label FROM edge ORDER BY id",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(EdgeRow {
                id: row.get::<_, i64>(0)? as u64,
                source_entity: row.get::<_, i64>(1)? as u64,
                target_entity: row.get::<_, i64>(2)? as u64,
                label: row.get::<_, i32>(3)? as u32,
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>().map_err(Into::into)
    }

    /// Load positions for a batch of entity rowids.
    pub fn load_position_batch(
        &self,
        entity_ids: &[u64],
    ) -> Result<Vec<(u64, f32, f32)>, StoreError> {
        if entity_ids.is_empty() {
            return Ok(Vec::new());
        }
        let placeholders = entity_ids
            .iter()
            .enumerate()
            .map(|(i, _)| format!("?{}", i + 1))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT entity_id, x, y FROM position_component WHERE entity_id IN ({})",
            placeholders
        );
        let mut stmt = self.connection.prepare(&sql)?;
        let params: Vec<i64> = entity_ids.iter().map(|&id| id as i64).collect();
        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();
        let rows = stmt.query_map(param_refs.as_slice(), |row| {
            Ok((
                row.get::<_, i64>(0)? as u64,
                row.get::<_, f64>(1)? as f32,
                row.get::<_, f64>(2)? as f32,
            ))
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>().map_err(Into::into)
    }

    pub fn load_all_positions(&self) -> Result<Vec<(u64, f32, f32)>, StoreError> {
        let mut stmt = self.connection.prepare(
            "SELECT entity_id, x, y FROM position_component ORDER BY entity_id",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)? as u64,
                row.get::<_, f64>(1)? as f32,
                row.get::<_, f64>(2)? as f32,
            ))
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>().map_err(Into::into)
    }

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
        rows.collect::<rusqlite::Result<Vec<_>>>().map_err(Into::into)
    }

    pub fn load_position(&self, entity_id: u64) -> Result<Option<(f32, f32)>, StoreError> {
        let mut stmt = self
            .connection
            .prepare("SELECT x, y FROM position_component WHERE entity_id = ?1")?;
        let result = stmt.query_map([entity_id as i64], |row| {
            Ok((row.get::<_, f64>(0)? as f32, row.get::<_, f64>(1)? as f32))
        })?;
        result.into_iter().next().transpose().map_err(Into::into)
    }

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
        rows.collect::<rusqlite::Result<Vec<_>>>().map_err(Into::into)
    }

    pub fn load_blob_data(
        &self,
        hash: u64,
        offset: u64,
        chunk_size: u32,
    ) -> Result<Option<Vec<u8>>, StoreError> {
        let mut stmt = self.connection.prepare_cached(
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

    pub fn search_property(
        &self,
        key: &str,
        limit: u32,
    ) -> Result<Vec<u64>, StoreError> {
        let mut stmt = self.connection.prepare(
            "SELECT DISTINCT entity_id FROM property WHERE key = ?1 LIMIT ?2",
        )?;
        stmt.query_map(rusqlite::params![key, limit], |row| {
            row.get::<_, i64>(0).map(|v| v as u64)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into)
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
    fn reads_stats() {
        let file = NamedTempFile::new().unwrap();
        {
            let conn = Connection::open(file.path()).unwrap();
            create_schema(&conn);
            conn.execute_batch(
                "INSERT INTO entity VALUES (1), (2); \
                 INSERT INTO edge VALUES (1, 1, 2, 1);",
            )
            .unwrap();
        }
        let store = ColdTier::open(file.path()).unwrap();
        let stats = store.stats().unwrap();
        assert_eq!(stats.entities, 2);
        assert_eq!(stats.edges, 1);
    }

    #[test]
    fn loads_entity_ids() {
        let file = NamedTempFile::new().unwrap();
        {
            let conn = Connection::open(file.path()).unwrap();
            create_schema(&conn);
            conn.execute_batch("INSERT INTO entity VALUES (10), (20), (30);")
                .unwrap();
        }
        let store = ColdTier::open(file.path()).unwrap();
        let ids = store.load_entity_ids().unwrap();
        assert_eq!(ids, vec![10, 20, 30]);
    }

    #[test]
    fn loads_edges_paginated() {
        let file = NamedTempFile::new().unwrap();
        {
            let conn = Connection::open(file.path()).unwrap();
            create_schema(&conn);
            conn.execute_batch(
                "INSERT INTO entity VALUES (1), (2); \
                 INSERT INTO edge VALUES (1, 1, 2, 1); \
                 INSERT INTO edge VALUES (2, 2, 1, 2);",
            )
            .unwrap();
        }
        let store = ColdTier::open(file.path()).unwrap();
        let batch = store.load_edge_batch(0, 1).unwrap();
        assert_eq!(batch.len(), 1);
        let batch2 = store.load_edge_batch(1, 1).unwrap();
        assert_eq!(batch2.len(), 1);
    }

    #[test]
    fn loads_positions_batch() {
        let file = NamedTempFile::new().unwrap();
        {
            let conn = Connection::open(file.path()).unwrap();
            create_schema(&conn);
            conn.execute_batch(
                "INSERT INTO entity VALUES (1), (2); \
                 INSERT INTO position_component VALUES (1, 0.0, 0.5); \
                 INSERT INTO position_component VALUES (2, 1.0, -0.5);",
            )
            .unwrap();
        }
        let store = ColdTier::open(file.path()).unwrap();
        let positions = store.load_position_batch(&[1, 2]).unwrap();
        assert_eq!(positions.len(), 2);
    }
}
