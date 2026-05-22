use neo4rs::*;
use std::time::Instant;

use crate::graph_gen::{EdgeData, NodeData};
use crate::system::BenchSystem;

const IMPORT_BATCH_SIZE: usize = 5000;

/// Neo4j benchmark system wrapping a [`neo4rs::Graph`] connection.
pub struct Neo4jBenchSystem {
    graph: Graph,
}

impl Neo4jBenchSystem {
    /// Connect to a Neo4j instance via Bolt.
    pub async fn new(uri: &str, user: &str, pass: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let graph = Graph::new(uri, user, pass).await?;
        Ok(Self { graph })
    }

    /// Delete all data in the database.
    pub async fn clean(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.graph.run(query("MATCH (n) DETACH DELETE n")).await?;
        Ok(())
    }

    /// Import nodes, edges, and create indexes. Returns total import time in ms.
    pub async fn import_data(
        &self,
        nodes: &[NodeData],
        edges: &[EdgeData],
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let import_start = Instant::now();
        import_nodes(&self.graph, nodes).await?;
        self.graph
            .run(query(
                "CREATE INDEX node_id IF NOT EXISTS FOR (n:Node) ON (n.id)",
            ))
            .await?;
        import_edges(&self.graph, edges).await?;
        self.graph
            .run(query(
                "CREATE INDEX node_coords IF NOT EXISTS FOR (n:Node) ON (n.x, n.y)",
            ))
            .await?;
        Ok(import_start.elapsed().as_millis() as u64)
    }

    /// Run one round of each query type to warm up caches.
    pub async fn warmup(&self, nodes: &[NodeData]) -> Result<(), Box<dyn std::error::Error>> {
        warmup_neo4j(&self.graph, nodes).await
    }
}

impl BenchSystem for Neo4jBenchSystem {
    async fn bench_get_entity(&mut self, id: u64) -> Result<(), Box<dyn std::error::Error>> {
        let q = query("MATCH (n:Node {id: $id}) RETURN n").param("id", id as i64);
        let mut result = self.graph.execute(q).await?;
        while let Ok(Some(_)) = result.next().await {}
        Ok(())
    }

    async fn bench_search_spatial(
        &mut self,
        min_x: f64,
        min_y: f64,
        max_x: f64,
        max_y: f64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let q = query(
            "MATCH (n:Node) WHERE n.x >= $minX AND n.x <= $maxX AND n.y >= $minY AND n.y <= $maxY RETURN n",
        )
        .param("minX", min_x)
        .param("maxX", max_x)
        .param("minY", min_y)
        .param("maxY", max_y);
        let mut result = self.graph.execute(q).await?;
        while let Ok(Some(_)) = result.next().await {}
        Ok(())
    }

    async fn bench_traverse_bfs(
        &mut self,
        start_id: u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let q = query(
            "MATCH (n:Node {id: $id})-[:CONNECTS*1..2]->(m) RETURN DISTINCT m",
        )
        .param("id", start_id as i64);
        let mut result = self.graph.execute(q).await?;
        while let Ok(Some(_)) = result.next().await {}
        Ok(())
    }

    async fn bench_full_scan(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let q = query("MATCH (n:Node) RETURN count(n)");
        let mut result = self.graph.execute(q).await?;
        while let Ok(Some(_)) = result.next().await {}
        Ok(())
    }
}

/// High-level setup: connect, clean, import, warmup.
pub async fn prepare_neo4j(
    nodes: &[NodeData],
    edges: &[EdgeData],
    uri: &str,
    user: &str,
    pass: &str,
) -> Result<(Neo4jBenchSystem, u64), Box<dyn std::error::Error>> {
    let system = Neo4jBenchSystem::new(uri, user, pass).await?;
    system.clean().await?;
    let import_ms = system.import_data(nodes, edges).await?;
    system.warmup(nodes).await?;
    Ok((system, import_ms))
}

/// Warm up Neo4j by running each query type once with the same queries used in benchmarks.
async fn warmup_neo4j(
    graph: &Graph,
    nodes: &[NodeData],
) -> Result<(), Box<dyn std::error::Error>> {
    let n = nodes.len() as u64;

    // Entity get warmup
    for &id in &[0u64, n / 2, n.saturating_sub(1)] {
        if id >= n {
            continue;
        }
        let q = query("MATCH (n:Node {id: $id}) RETURN n").param("id", id as i64);
        let mut result = graph.execute(q).await?;
        while let Ok(Some(_)) = result.next().await {}
    }

    // Spatial warmup
    let q = query(
        "MATCH (n:Node) WHERE n.x >= -1.0 AND n.x <= 1.0 AND n.y >= -1.0 AND n.y <= 1.0 RETURN n"
    );
    let mut result = graph.execute(q).await?;
    while let Ok(Some(_)) = result.next().await {}

    // BFS warmup
    if n == 0 { return Ok(()); }
    let start_id = 0u64;
    let q = query(
        "MATCH (n:Node {id: $id})-[:CONNECTS*1..2]->(m) RETURN DISTINCT m"
    )
    .param("id", start_id as i64);
    let mut result = graph.execute(q).await?;
    while let Ok(Some(_)) = result.next().await {}

    // Full scan warmup
    let q = query("MATCH (n:Node) RETURN count(n)");
    let mut result = graph.execute(q).await?;
    while let Ok(Some(_)) = result.next().await {}

    Ok(())
}

/// Import nodes using UNWIND with parallel arrays.
async fn import_nodes(
    graph: &Graph,
    nodes: &[NodeData],
) -> Result<(), Box<dyn std::error::Error>> {
    for chunk in nodes.chunks(IMPORT_BATCH_SIZE) {
        let ids: Vec<i64> = chunk.iter().map(|nd| nd.id as i64).collect();
        let xs: Vec<f64> = chunk.iter().map(|nd| nd.x as f64).collect();
        let ys: Vec<f64> = chunk.iter().map(|nd| nd.y as f64).collect();
        let clusters: Vec<i64> = chunk.iter().map(|nd| nd.cluster as i64).collect();

        let q = query(
            "UNWIND range(0, size($ids)-1) AS i CREATE (n:Node {id: $ids[i], x: $xs[i], y: $ys[i], cluster: $clusters[i]})"
        )
        .param("ids", ids)
        .param("xs", xs)
        .param("ys", ys)
        .param("clusters", clusters);

        graph.run(q).await?;
    }
    Ok(())
}

/// Import edges using UNWIND with parallel arrays.
async fn import_edges(
    graph: &Graph,
    edges: &[EdgeData],
) -> Result<(), Box<dyn std::error::Error>> {
    for chunk in edges.chunks(IMPORT_BATCH_SIZE) {
        let srcs: Vec<i64> = chunk.iter().map(|e| e.source as i64).collect();
        let tgts: Vec<i64> = chunk.iter().map(|e| e.target as i64).collect();

        let q = query(
            "UNWIND range(0, size($srcs)-1) AS i MATCH (a:Node {id: $srcs[i]}), (b:Node {id: $tgts[i]}) CREATE (a)-[:CONNECTS]->(b)"
        )
        .param("srcs", srcs)
        .param("tgts", tgts);

        graph.run(q).await?;
    }
    Ok(())
}
