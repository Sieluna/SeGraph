use neo4rs::*;
use std::time::Instant;

use crate::bench_async;
use crate::bench_runner::BenchRunner;
use crate::graph_gen::{EdgeData, NodeData};
use crate::types::SystemResults;

const IMPORT_BATCH_SIZE: usize = 5000;

/// Run benchmarks against a Neo4j instance via Bolt.
pub async fn run_benchmarks(
    nodes: &[NodeData],
    edges: &[EdgeData],
    uri: &str,
    user: &str,
    pass: &str,
    runner: &BenchRunner,
) -> Result<SystemResults, Box<dyn std::error::Error>> {
    let n = nodes.len() as u64;
    let e = edges.len() as u64;

    let graph = Graph::new(uri, user, pass).await?;

    // Clean up previous data (not timed)
    graph.run(query("MATCH (n) DETACH DELETE n")).await?;

    // Import — nodes, node_id index, edges, spatial index are all timed
    let import_start = Instant::now();
    import_nodes(&graph, nodes).await?;

    // Index before edges so MATCH hits the index
    graph
        .run(query(
            "CREATE INDEX node_id IF NOT EXISTS FOR (n:Node) ON (n.id)",
        ))
        .await?;

    import_edges(&graph, edges).await?;

    // Spatial index for query benchmarks
    graph
        .run(query(
            "CREATE INDEX node_coords IF NOT EXISTS FOR (n:Node) ON (n.x, n.y)",
        ))
        .await?;

    let import_ms = import_start.elapsed().as_millis() as u64;

    warmup_neo4j(&graph, nodes).await?;

    let mut samples = Vec::new();

    // Entity gets
    for &id in &[0u64, n / 4, n / 2, 3 * n / 4, n.saturating_sub(1)] {
        if id >= n {
            continue;
        }
        let label = format!("entity_get_{id}");
        let q = query("MATCH (n:Node {id: $id}) RETURN n").param("id", id as i64);
        let s = bench_async!(runner, &label, || {
            let mut result = graph.execute(q.clone()).await.unwrap();
            while let Ok(Some(_)) = result.next().await {}
        });
        samples.push(s);
    }

    // Spatial queries
    let viewports: [(f32, f32, f32, f32); 5] = [
        (-1.0, -1.0, 1.0, 1.0),
        (-0.5, -0.5, 0.5, 0.5),
        (-0.25, -0.25, 0.25, 0.25),
        (-1.0, -1.0, 0.0, 0.0),
        (0.0, 0.0, 1.0, 1.0),
    ];

    for &(min_x, min_y, max_x, max_y) in &viewports {
        let label = format!(
            "spatial_[{:.2}_{:.2}_{:.2}_{:.2}]",
            min_x, min_y, max_x, max_y
        );
        let q = query(
            "MATCH (n:Node) WHERE n.x >= $minX AND n.x <= $maxX AND n.y >= $minY AND n.y <= $maxY RETURN n"
        )
        .param("minX", min_x as f64)
        .param("maxX", max_x as f64)
        .param("minY", min_y as f64)
        .param("maxY", max_y as f64);
        let s = bench_async!(runner, &label, || {
            let mut result = graph.execute(q.clone()).await.unwrap();
            while let Ok(Some(_)) = result.next().await {}
        });
        samples.push(s);
    }

    // BFS 2-hop traversals
    for &start_id in &[0u64, n / 4, n / 2, 3 * n / 4] {
        if start_id >= n {
            continue;
        }
        let label = format!("bfs_depth2_from_{start_id}");
        let q = query(
            "MATCH (n:Node {id: $id})-[:CONNECTS*1..2]->(m) RETURN DISTINCT m"
        )
        .param("id", start_id as i64);
        let s = bench_async!(runner, &label, || {
            let mut result = graph.execute(q.clone()).await.unwrap();
            while let Ok(Some(_)) = result.next().await {}
        });
        samples.push(s);
    }

    // Full scan
    {
        let q = query("MATCH (n:Node) RETURN count(n)");
        let s = bench_async!(runner, "full_scan", || {
            let mut result = graph.execute(q.clone()).await.unwrap();
            while let Ok(Some(_)) = result.next().await {}
        });
        samples.push(s);
    }

    // Cleanup
    let _ = graph.run(query("MATCH (n) DETACH DELETE n")).await;

    Ok(SystemResults {
        system: "neo4j".to_string(),
        scale_label: String::new(),
        node_count: n,
        edge_count: e,
        connection_ms: None,
        import_ms: Some(import_ms),
        samples,
    })
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
