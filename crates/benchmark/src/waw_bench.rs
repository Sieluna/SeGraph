use std::time::{Duration, Instant};

use rusqlite::Connection;
use tempfile::NamedTempFile;
use waw_client::GraphClient;
use waw_proto::IndexQuery;

use crate::graph_gen::{EdgeData, NodeData};
use crate::system::BenchSystem;

/// Create a temporary SQLite database with benchmark schema and data.
pub fn create_bench_db(
    nodes: &[NodeData],
    edges: &[EdgeData],
) -> (NamedTempFile, std::path::PathBuf) {
    let tmp = NamedTempFile::new().expect("create temp file for benchmark DB");
    let path = tmp.path().to_path_buf();

    let conn = Connection::open(&path).expect("open benchmark DB");
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
    .expect("create benchmark DB schema");

    conn.execute("BEGIN", []).expect("begin transaction");

    let mut entity_stmt = conn
        .prepare("INSERT INTO entity (id) VALUES (?1)")
        .expect("prepare entity insert");
    for node in nodes {
        entity_stmt.execute([node.id as i64]).expect("insert entity");
    }

    let mut pos_stmt = conn
        .prepare("INSERT INTO position_component (entity_id, x, y) VALUES (?1, ?2, ?3)")
        .expect("prepare position insert");
    for node in nodes {
        pos_stmt
            .execute(rusqlite::params![node.id as i64, node.x as f64, node.y as f64])
            .expect("insert position");
    }

    let mut prop_stmt = conn
        .prepare(
            "INSERT INTO property (entity_id, key, value_type, value_int) VALUES (?1, ?2, ?3, ?4)",
        )
        .expect("prepare property insert");
    for node in nodes {
        prop_stmt
            .execute(rusqlite::params![
                node.id as i64,
                "cluster",
                1i32,
                node.cluster as i64
            ])
            .expect("insert property");
    }

    let mut edge_stmt = conn
        .prepare(
            "INSERT INTO edge (id, source_entity, target_entity, label) VALUES (?1, ?2, ?3, 1)",
        )
        .expect("prepare edge insert");
    for (i, edge) in edges.iter().enumerate() {
        edge_stmt
            .execute(rusqlite::params![
                (i + 1) as i64,
                edge.source as i64,
                edge.target as i64,
            ])
            .expect("insert edge");
    }

    conn.execute("COMMIT", []).expect("commit transaction");

    (tmp, path)
}

/// WAW benchmark system wrapping a [`GraphClient`].
pub struct WawBenchSystem {
    client: GraphClient,
}

impl WawBenchSystem {
    /// Connect to a WAW server. Returns the system and connection latency in ms.
    pub async fn connect(url: &str) -> Result<(Self, u64), Box<dyn std::error::Error>> {
        let mut client = GraphClient::new(Duration::from_secs(30));
        let t0 = Instant::now();
        client.connect(url.parse()?).await?;
        let connect_ms = t0.elapsed().as_millis() as u64;
        Ok((Self { client }, connect_ms))
    }
}

impl BenchSystem for WawBenchSystem {
    async fn bench_get_entity(&mut self, id: u64) -> Result<(), Box<dyn std::error::Error>> {
        self.client.get_entity(id, false, false).await?;
        Ok(())
    }

    async fn bench_search_spatial(
        &mut self,
        min_x: f64,
        min_y: f64,
        max_x: f64,
        max_y: f64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.client
            .search(IndexQuery::Spatial {
                min_x,
                min_y,
                max_x,
                max_y,
                limit: u32::MAX,
            })
            .await?;
        Ok(())
    }

    async fn bench_traverse_bfs(
        &mut self,
        start_id: u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.client.traverse(start_id, 2, vec![], u32::MAX).await?;
        Ok(())
    }

    async fn bench_full_scan(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.client
            .search(IndexQuery::Spatial {
                min_x: -1.0,
                min_y: -1.0,
                max_x: 1.0,
                max_y: 1.0,
                limit: u32::MAX,
            })
            .await?;
        Ok(())
    }
}
