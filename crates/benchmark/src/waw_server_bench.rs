use core::time::Duration;
use std::time::Instant;

use rusqlite::Connection;
use tempfile::NamedTempFile;
use waw_client::GraphClient;
use waw_proto::{IndexQuery, ServerStats};

use crate::graph_gen::{EdgeData, NodeData};

pub struct WawClientResults {
    pub connect_ms: u64,
    pub stats: Option<ServerStats>,
    pub entity_gets: Vec<EntityGetResult>,
    pub spatial_queries: Vec<SpatialResult>,
    pub traversals: Vec<TraversalResult>,
    pub full_scan_ms: u64,
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

/// Create a temporary SQLite database with benchmark schema and data.
/// Returns the temp file (must be kept alive) and its path.
pub fn create_bench_db(nodes: &[NodeData], edges: &[EdgeData]) -> (NamedTempFile, std::path::PathBuf) {
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();

    let conn = Connection::open(&path).unwrap();
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

    conn.execute("BEGIN", []).unwrap();

    let mut entity_stmt = conn.prepare("INSERT INTO entity (id) VALUES (?1)").unwrap();
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

    (tmp, path)
}

/// Run benchmarks against a waw_server instance via `GraphClient`.
pub async fn run_client_benchmarks(
    url: &str,
    nodes: &[NodeData],
) -> Result<WawClientResults, Box<dyn std::error::Error>> {
    let n = nodes.len() as u64;

    let mut client = GraphClient::new(Duration::from_secs(30));

    // ── Connect + Hello ──
    let t0 = Instant::now();
    client.connect(url.parse()?).await?;
    let connect_ms = t0.elapsed().as_millis() as u64;
    let stats = client.stats().cloned();

    // Warm up: load the first entity to trigger cold-tier promotion
    let _ = client.get_entity(0, false, false).await;

    // ── Entity gets ──
    let sample_ids = [0u64, n / 4, n / 2, 3 * n / 4, n.saturating_sub(1)];
    let mut entity_gets = Vec::new();

    for &id in &sample_ids {
        if id >= n {
            continue;
        }
        match client.get_entity(id, false, false).await {
            Ok(_) => {
                // Second access (hot hit)
                let t1 = Instant::now();
                let hit = client.get_entity(id, false, false).await.is_ok();
                let elapsed_us = t1.elapsed().as_micros() as u64;
                entity_gets.push(EntityGetResult {
                    id,
                    hit,
                    elapsed_us,
                });
            }
            Err(_) => {
                entity_gets.push(EntityGetResult {
                    id,
                    hit: false,
                    elapsed_us: 0,
                });
            }
        }
    }

    // ── Spatial queries ──
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
        let results = client
            .search(IndexQuery::Spatial {
                min_x: min_x as f64,
                min_y: min_y as f64,
                max_x: max_x as f64,
                max_y: max_y as f64,
                limit: u32::MAX,
            })
            .await?;
        let elapsed_us = t0.elapsed().as_micros() as u64;
        spatial_queries.push(SpatialResult {
            bounds: (min_x, min_y, max_x, max_y),
            matched: results.len(),
            elapsed_us,
        });
    }

    // ── Graph traversals ──
    let start_indices = [0u64, n / 4, n / 2, 3 * n / 4];
    let mut traversals = Vec::new();

    for &start_id in &start_indices {
        if start_id >= n {
            continue;
        }
        let t0 = Instant::now();
        let visited = client
            .traverse(start_id, 2, vec![], u32::MAX)
            .await?;
        let elapsed_us = t0.elapsed().as_micros() as u64;
        traversals.push(TraversalResult {
            start_id,
            depth: 2,
            visited: visited.len(),
            elapsed_us,
        });
    }

    // ── Full scan (sample-based) ──
    let sample_size = 100.min(n as usize);
    let t0 = Instant::now();
    let mut scanned = 0u64;
    for i in 0..sample_size {
        let id = (i as u64 * n) / sample_size as u64;
        if client.get_entity(id, false, false).await.is_ok() {
            scanned += 1;
        }
    }
    let full_scan_ms = t0.elapsed().as_millis() as u64;
    let _ = scanned;

    Ok(WawClientResults {
        connect_ms,
        stats,
        entity_gets,
        spatial_queries,
        traversals,
        full_scan_ms,
    })
}
