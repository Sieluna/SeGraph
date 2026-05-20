use std::collections::HashMap;

use waw_core::{Index, Pointer, Storage};

use crate::{
    spatial_index::SpatialIndex,
    sqlite_store::{SqliteGraphStore, StoreError},
};

/// An entity (node) in the graph.
///
/// Properties and blobs are stored in SQLite and fetched on demand.
/// Only topology (edge indices) and a sqlite back-reference are kept in memory.
#[derive(Debug)]
pub struct GraphEntity {
    pub sqlite_rowid: u64,
    /// Edge indices into `GraphStore.edges` Storage.
    pub edges_out: Vec<Index>,
    pub edges_in: Vec<Index>,
}

/// A directed edge (relationship) between two entities.
#[derive(Debug)]
pub struct GraphEdge {
    pub sqlite_rowid: u64,
    /// Entity indices into `GraphStore.entities` Storage.
    pub source_idx: Index,
    pub target_idx: Index,
    pub label: u32,
}

/// In-memory graph engine backed by waw_core storage and SQLite persistence.
pub struct GraphStore {
    pub entities: Storage<GraphEntity>,
    pub edges: Storage<GraphEdge>,
    /// Maps `sqlite_rowid` → strong pointer to entity.
    entity_by_rowid: HashMap<u64, Pointer<GraphEntity>>,
    /// Maps Storage `Index` → `sqlite_rowid` for O(1) index→rowid resolution.
    rowid_by_index: Vec<u64>,
    /// Maps edge Storage `Index` → `sqlite_rowid`.
    edge_rowid_by_index: Vec<u64>,
    /// Keeps all loaded entities alive.
    entity_handles: Vec<Pointer<GraphEntity>>,
    /// Keeps all loaded edges alive.
    edge_handles: Vec<Pointer<GraphEdge>>,
    /// Cached positions indexed by entity Storage `Index`.
    /// `None` for entities without a position component.
    positions: Vec<Option<(f32, f32)>>,
    /// Optional spatial index built from position components.
    pub spatial_index: Option<SpatialIndex>,
}

impl GraphStore {
    /// Create an empty graph store.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            entities: Storage::new(),
            edges: Storage::new(),
            entity_by_rowid: HashMap::new(),
            rowid_by_index: Vec::new(),
            edge_rowid_by_index: Vec::new(),
            entity_handles: Vec::new(),
            edge_handles: Vec::new(),
            positions: Vec::new(),
            spatial_index: None,
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entities.iter().next().is_none()
    }

    #[must_use]
    pub fn entity_count(&self) -> usize {
        self.entity_by_rowid.len()
    }

    /// Load the graph from SQLite into waw_core memory storage.
    ///
    /// Loads entity/edge topology and builds the spatial index from position components.
    /// Properties and blobs remain in SQLite for on-demand access.
    pub fn load(store: &SqliteGraphStore) -> Result<Self, StoreError> {
        let mut graph = Self::empty();

        // 1. Load entities
        {
            let mut stmt = store
                .connection
                .prepare("SELECT id FROM entity ORDER BY id")?;
            let rows = stmt.query_map([], |row| row.get::<_, i64>(0))?;

            for row in rows {
                let sqlite_rowid = row? as u64;
                let ptr = graph.entities.create(GraphEntity {
                    sqlite_rowid,
                    edges_out: Vec::new(),
                    edges_in: Vec::new(),
                });
                let idx = ptr.data.get_index();
                if idx >= graph.rowid_by_index.len() {
                    graph.rowid_by_index.resize(idx + 1, 0);
                }
                graph.rowid_by_index[idx] = sqlite_rowid;
                graph.entity_by_rowid.insert(sqlite_rowid, ptr.clone());
                graph.entity_handles.push(ptr);
            }
        }

        // 2. Load edges
        {
            let mut stmt = store
                .connection
                .prepare("SELECT id, source_entity, target_entity, label FROM edge ORDER BY id")?;
            let rows = stmt.query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)? as u64,
                    row.get::<_, i64>(1)? as u64,
                    row.get::<_, i64>(2)? as u64,
                    row.get::<_, i32>(3)? as u32,
                ))
            })?;

            for row in rows {
                let (edge_id, source_id, target_id, label) = row?;
                let source_idx = graph
                    .entity_by_rowid
                    .get(&source_id)
                    .map(|p| p.data.get_index());
                let target_idx = graph
                    .entity_by_rowid
                    .get(&target_id)
                    .map(|p| p.data.get_index());

                if let (Some(source_idx), Some(target_idx)) = (source_idx, target_idx) {
                    let edge_ptr = graph.edges.create(GraphEdge {
                        sqlite_rowid: edge_id,
                        source_idx,
                        target_idx,
                        label,
                    });
                    let edge_idx = edge_ptr.data.get_index();

                    // Track edge rowid → index mapping
                    if edge_idx >= graph.edge_rowid_by_index.len() {
                        graph.edge_rowid_by_index.resize(edge_idx + 1, 0);
                    }
                    graph.edge_rowid_by_index[edge_idx] = edge_id;

                    // Push edge index to entity edge lists (lock-free)
                    graph.entities[&graph.entity_by_rowid[&source_id]]
                        .edges_out
                        .push(edge_idx);
                    graph.entities[&graph.entity_by_rowid[&target_id]]
                        .edges_in
                        .push(edge_idx);
                    graph.edge_handles.push(edge_ptr);
                }
            }
        }

        // 3. Build spatial index from position components
        {
            let positions = Self::load_positions(store, &mut graph)?;
            if !positions.is_empty() {
                let node_count = graph.entity_count();
                let bits = spatial_bits_for_count(node_count);
                graph.spatial_index = Some(SpatialIndex::build(&positions, bits));
            }
        }

        // 4. Commit pending reference counts
        graph.entities.sync_pending();
        graph.edges.sync_pending();

        Ok(graph)
    }

    fn load_positions(
        store: &SqliteGraphStore,
        graph: &mut GraphStore,
    ) -> Result<Vec<(Pointer<GraphEntity>, f32, f32)>, StoreError> {
        let mut positions = Vec::new();

        let mut stmt = store
            .connection
            .prepare("SELECT entity_id, x, y FROM position_component ORDER BY entity_id")?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)? as u64,
                row.get::<_, f64>(1)? as f32,
                row.get::<_, f64>(2)? as f32,
            ))
        })?;

        // Ensure positions vec can hold all entity indices
        let max_idx = graph.rowid_by_index.len();
        graph.positions.resize(max_idx, None);

        for row in rows {
            let (entity_id, x, y) = row?;
            if let Some(ptr) = graph.entity_by_rowid.get(&entity_id) {
                let idx = ptr.data.get_index();
                if idx >= graph.positions.len() {
                    graph.positions.resize(idx + 1, None);
                }
                graph.positions[idx] = Some((x, y));
                positions.push((ptr.clone(), x, y));
            }
        }

        Ok(positions)
    }

    /// Get the cached position of an entity by storage index.
    #[must_use]
    pub fn position_of(&self, idx: Index) -> Option<(f32, f32)> {
        self.positions.get(idx).copied().flatten()
    }

    /// Look up an entity's sqlite_rowid to verify it exists and get its index.
    #[must_use]
    pub fn find_entity_index(&self, rowid: u64) -> Option<Index> {
        self.entity_by_rowid.get(&rowid).map(|p| p.data.get_index())
    }

    /// Access an entity by its storage index.
    #[must_use]
    pub fn entity_at(&self, idx: Index) -> Option<&GraphEntity> {
        let rowid = self.rowid_by_index.get(idx).copied().filter(|&r| r != 0)?;
        let ptr = self.entity_by_rowid.get(&rowid)?;
        Some(&self.entities[ptr])
    }

    /// Access an edge by its storage index (O(1) via edge_handles).
    #[must_use]
    pub fn edge_at(&self, idx: Index) -> Option<&GraphEdge> {
        let ptr = self.edge_handles.get(idx)?;
        Some(&self.edges[ptr])
    }

    /// Query entities within world-space bounds using the spatial index.
    #[must_use]
    pub fn query_spatial(
        &self,
        min_x: f32,
        min_y: f32,
        max_x: f32,
        max_y: f32,
        lod: u16,
    ) -> Vec<u64> {
        let Some(ref index) = self.spatial_index else {
            return Vec::new();
        };

        let entity_indices = index.query_bounds(min_x, min_y, max_x, max_y, lod);
        let mut rowids: Vec<u64> = entity_indices
            .iter()
            .filter_map(|&idx| self.rowid_by_index.get(idx).copied())
            .filter(|&r| r != 0)
            .collect();
        rowids.sort_unstable();
        rowids.dedup();
        rowids
    }

    /// BFS traversal from a start entity, following edges with matching labels.
    ///
    /// Uses raw storage indices for lock-free traversal.
    #[must_use]
    pub fn traverse_bfs(&self, start_rowid: u64, max_depth: u32, edge_labels: &[u32]) -> Vec<u64> {
        let filter: Option<Vec<u32>> = if edge_labels.is_empty() {
            None
        } else {
            Some(edge_labels.to_vec())
        };

        let start_idx = match self.find_entity_index(start_rowid) {
            Some(idx) => idx,
            None => return Vec::new(),
        };

        let mut visited: HashMap<Index, u32> = HashMap::new();
        let mut queue: Vec<(Index, u32)> = Vec::new();
        let mut result: Vec<u64> = Vec::new();

        visited.insert(start_idx, 0);
        queue.push((start_idx, 0));
        result.push(start_rowid);

        while let Some((idx, depth)) = queue.pop() {
            if depth >= max_depth {
                continue;
            }
            let ent = match self.entity_at(idx) {
                Some(e) => e,
                None => continue,
            };
            for &edge_idx in &ent.edges_out {
                let edge = match self.edge_at(edge_idx) {
                    Some(e) => e,
                    None => continue,
                };
                if let Some(ref labels) = filter
                    && !labels.contains(&edge.label) {
                        continue;
                    }
                let target_idx = edge.target_idx;
                if let std::collections::hash_map::Entry::Vacant(e) = visited.entry(target_idx) {
                    e.insert(depth + 1);
                    queue.push((target_idx, depth + 1));
                    if let Some(target) = self.entity_at(target_idx) {
                        result.push(target.sqlite_rowid);
                    }
                }
            }
        }
        result
    }

}

/// Choose grid resolution based on node count.
fn spatial_bits_for_count(node_count: usize) -> u16 {
    if node_count == 0 {
        return 4;
    }
    // Target ~4 entities per cell on average.
    // cells = node_count / 4, so bits = log4(node_count) = log2(node_count) / 2
    let bits = ((node_count as f64).log2() / 2.0).round() as i32;
    bits.clamp(4, 8) as u16
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
                mime TEXT NOT NULL DEFAULT '',
                size_bytes INTEGER NOT NULL,
                data BLOB
            );
            "#,
        )
        .unwrap();
    }

    fn open_store(file: &NamedTempFile) -> SqliteGraphStore {
        SqliteGraphStore::open(file.path()).unwrap()
    }

    #[test]
    fn loads_empty_graph() {
        let file = NamedTempFile::new().unwrap();
        {
            let conn = Connection::open(file.path()).unwrap();
            create_schema(&conn);
        }
        let store = open_store(&file);
        let graph = GraphStore::load(&store).unwrap();
        assert!(graph.is_empty());
    }

    #[test]
    fn loads_entities_without_positions() {
        let file = NamedTempFile::new().unwrap();
        {
            let conn = Connection::open(file.path()).unwrap();
            create_schema(&conn);
            conn.execute_batch("INSERT INTO entity VALUES (1); INSERT INTO entity VALUES (2);")
                .unwrap();
        }
        let store = open_store(&file);
        let graph = GraphStore::load(&store).unwrap();
        assert_eq!(graph.entity_count(), 2);
        assert!(graph.spatial_index.is_none());
    }

    #[test]
    fn loads_edges_and_supports_traversal() {
        let file = NamedTempFile::new().unwrap();
        {
            let conn = Connection::open(file.path()).unwrap();
            create_schema(&conn);
            conn.execute_batch(
                r#"
                INSERT INTO entity VALUES (1), (2), (3);
                INSERT INTO edge VALUES (1, 1, 2, 1);
                INSERT INTO edge VALUES (2, 2, 3, 1);
                "#,
            )
            .unwrap();
        }
        let store = open_store(&file);
        let graph = GraphStore::load(&store).unwrap();

        assert!(graph.find_entity_index(1).is_some());
        let result = graph.traverse_bfs(1, 2, &[1]);
        assert_eq!(result.len(), 3); // entities 1, 2, 3
    }

    #[test]
    fn builds_spatial_index_from_positions() {
        let file = NamedTempFile::new().unwrap();
        {
            let conn = Connection::open(file.path()).unwrap();
            create_schema(&conn);
            conn.execute_batch(
                r#"
                INSERT INTO entity VALUES (1), (2);
                INSERT INTO position_component VALUES (1, 0.0, 0.0);
                INSERT INTO position_component VALUES (2, 0.8, 0.8);
                "#,
            )
            .unwrap();
        }
        let store = open_store(&file);
        let graph = GraphStore::load(&store).unwrap();

        assert!(graph.spatial_index.is_some());
        let results = graph.query_spatial(-0.5, -0.5, 0.5, 0.5, 4);
        assert_eq!(results.len(), 1); // only entity 1
    }

    #[test]
    fn spatial_query_returns_empty_when_no_index() {
        let graph = GraphStore::empty();
        let results = graph.query_spatial(-1.0, -1.0, 1.0, 1.0, 4);
        assert!(results.is_empty());
    }

    #[test]
    fn traverse_respects_label_filter() {
        let file = NamedTempFile::new().unwrap();
        {
            let conn = Connection::open(file.path()).unwrap();
            create_schema(&conn);
            conn.execute_batch(
                r#"
                INSERT INTO entity VALUES (1), (2);
                INSERT INTO edge VALUES (1, 1, 2, 1);
                INSERT INTO edge VALUES (2, 1, 2, 2);
                "#,
            )
            .unwrap();
        }
        let store = open_store(&file);
        let graph = GraphStore::load(&store).unwrap();

        // Only label 1 should match the first edge
        let result = graph.traverse_bfs(1, 1, &[1]);
        assert_eq!(result.len(), 2);

        // Label 99 should match nothing
        let result = graph.traverse_bfs(1, 1, &[99]);
        assert_eq!(result.len(), 1); // only start entity
    }
}
