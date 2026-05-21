use std::time::Instant;

use rusqlite::Connection;
use tempfile::NamedTempFile;
use waw_server::pipeline::{Pipeline, PipelineConfig};

use crate::graph_gen::{EdgeData, NodeData};

pub struct WawResults {
    pub load_ms: u64,
    pub entity_count: usize,
    pub edge_count: usize,
    pub spatial_queries: Vec<SpatialResult>,
    pub traversals: Vec<TraversalResult>,
    pub entity_gets: Vec<EntityGetResult>,
    pub full_scan_ms: u64,
    pub memory_used_bytes: usize,
}

pub struct SpatialResult {
    pub bounds: (f32, f32, f32, f32),
    pub matched: usize,
    pub elapsed_us: u64,
}

pub struct TraversalResult {
    pub start_id: u64,
    pub depth: u32,
    pub visited: usize,
    pub elapsed_us: u64,
}

pub struct EntityGetResult {
    pub id: u64,
    pub hit: bool,
    pub elapsed_us: u64,
}

/// Run benchmarks using the new `Pipeline` (waw_core Storage + CSR + warm/cold tiers).
pub fn run_benchmarks(nodes: &[NodeData], edges: &[EdgeData]) -> WawResults {
    let tmp = NamedTempFile::new().unwrap();
    {
        let conn = Connection::open(tmp.path()).unwrap();
        create_bench_schema(&conn);
        populate_bench_data(&conn, nodes, edges);
    }

    // Load via Pipeline
    let load_start = Instant::now();
    let pipeline = Pipeline::load(
        tmp.path(),
        None::<&str>,
        PipelineConfig::default(),
    )
    .unwrap();
    let load_ms = load_start.elapsed().as_millis() as u64;

    let entity_count = nodes.len();
    let edge_count = edges.len();

    // Entity get: first access (cold tier → hot promotion) and second access (hot hit)
    // Node IDs are 0..entity_count; pick representative samples
    let n = entity_count as u64;
    let sample_ids = [0u64, n / 4, n / 2, 3 * n / 4, n.saturating_sub(1)];
    let mut entity_gets = Vec::new();

    for &id in &sample_ids {
        if id > n {
            continue;
        }
        // First access — may go through cold tier
        let t0 = Instant::now();
        let hit = pipeline.get_entity(id).unwrap().is_some();
        let elapsed_us = t0.elapsed().as_micros() as u64;
        entity_gets.push(EntityGetResult {
            id,
            hit,
            elapsed_us,
        });
    }

    // Second access — should all be hot hits
    for &id in &sample_ids {
        if id > n {
            continue;
        }
        let t0 = Instant::now();
        let _ = pipeline.get_entity(id);
        let elapsed_us = t0.elapsed().as_micros() as u64;
        // Record second-access timing for the last entry (overwrites first timing)
        if let Some(entry) = entity_gets.iter_mut().find(|e| e.id == id) {
            entry.elapsed_us = elapsed_us;
        }
    }

    // Spatial queries at the index's native LOD
    let spatial_lod = pipeline.spatial_lod();

    let viewport_bounds = [
        (-1.0_f32, -1.0_f32, 1.0_f32, 1.0_f32),
        (-0.5, -0.5, 0.5, 0.5),
        (-0.25, -0.25, 0.25, 0.25),
        (-1.0, -1.0, 0.0, 0.0),
        (0.0, 0.0, 1.0, 1.0),
    ];

    let mut spatial_queries = Vec::new();
    for &(min_x, min_y, max_x, max_y) in &viewport_bounds {
        let t0 = Instant::now();
        let results = pipeline.search_spatial(min_x, min_y, max_x, max_y, spatial_lod, u32::MAX);
        let elapsed_us = t0.elapsed().as_micros() as u64;
        spatial_queries.push(SpatialResult {
            bounds: (min_x, min_y, max_x, max_y),
            matched: results.len(),
            elapsed_us,
        });
    }

    // Graph traversals
    let start_indices = [
        0u64,
        (n / 4) as u64,
        (n / 2) as u64,
        (3 * n / 4) as u64,
    ];
    let mut traversals = Vec::new();

    for &start_id in &start_indices {
        if start_id > n {
            continue;
        }
        let t0 = Instant::now();
        let visited = pipeline.traverse_bfs(start_id, 2, &[], u32::MAX);
        let elapsed_us = t0.elapsed().as_micros() as u64;
        traversals.push(TraversalResult {
            start_id,
            depth: 2,
            visited: visited.len(),
            elapsed_us,
        });
    }

    // Full scan: iterate all entities via repeated get_entity calls
    let t0 = Instant::now();
    let mut total_x = 0.0f64;
    let mut total_y = 0.0f64;
    let mut count = 0u64;
    for node in nodes {
        if let Ok(Some(meta)) = pipeline.get_entity(node.id) {
            if let Some((x, y)) = meta.position {
                total_x += x as f64;
                total_y += y as f64;
            }
        }
        count += 1;
    }
    let _ = (total_x, total_y, count);
    let full_scan_ms = t0.elapsed().as_millis() as u64;

    // Memory usage — read from hot tier directly
    let hot = pipeline.hot_tier_for_read();
    let memory_used_bytes = hot.memory_used;

    WawResults {
        load_ms,
        entity_count,
        edge_count,
        spatial_queries,
        traversals,
        entity_gets,
        full_scan_ms,
        memory_used_bytes,
    }
}

fn create_bench_schema(conn: &Connection) {
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

fn populate_bench_data(conn: &Connection, nodes: &[NodeData], edges: &[EdgeData]) {
    conn.execute("BEGIN", []).unwrap();

    let mut entity_stmt = conn
        .prepare("INSERT INTO entity (id) VALUES (?1)")
        .unwrap();
    for node in nodes {
        entity_stmt.execute([node.id as i64]).unwrap();
    }

    let mut pos_stmt = conn
        .prepare("INSERT INTO position_component (entity_id, x, y) VALUES (?1, ?2, ?3)")
        .unwrap();
    for node in nodes {
        pos_stmt
            .execute(rusqlite::params![node.id as i64, node.x as f64, node.y as f64])
            .unwrap();
    }

    let mut prop_stmt = conn
        .prepare(
            "INSERT INTO property (entity_id, key, value_type, value_int) VALUES (?1, ?2, ?3, ?4)",
        )
        .unwrap();
    for node in nodes {
        prop_stmt
            .execute(rusqlite::params![
                node.id as i64,
                "cluster",
                1i32,
                node.cluster as i64
            ])
            .unwrap();
    }

    let mut edge_stmt = conn
        .prepare(
            "INSERT INTO edge (id, source_entity, target_entity, label) VALUES (?1, ?2, ?3, 1)",
        )
        .unwrap();
    for (i, edge) in edges.iter().enumerate() {
        edge_stmt
            .execute(rusqlite::params![
                (i + 1) as i64,
                edge.source as i64,
                edge.target as i64,
            ])
            .unwrap();
    }

    conn.execute("COMMIT", []).unwrap();
}
