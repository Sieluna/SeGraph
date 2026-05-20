use neo4rs::*;
use std::time::Instant;

use crate::graph_gen::{EdgeData, NodeData};

pub struct Neo4jResults {
    pub import_ms: u64,
    pub viewport_queries: Vec<ViewportResult>,
    pub neighbor_traversals: Vec<TraversalResult>,
    pub full_scan_ms: u64,
}

pub struct ViewportResult {
    pub bounds: (f32, f32, f32, f32),
    pub matched: usize,
    pub elapsed_us: u64,
}

pub struct TraversalResult {
    pub start_idx: usize,
    pub depth: u32,
    pub visited: usize,
    pub elapsed_us: u64,
}

pub async fn run_benchmarks(
    nodes: &[NodeData],
    edges: &[EdgeData],
    uri: &str,
    user: &str,
    pass: &str,
) -> Result<Neo4jResults, Box<dyn std::error::Error>> {
    let graph = Graph::new(uri, user, pass).await?;

    // ------ Import via batched CREATE ------
    let import_start = Instant::now();
    import_graph(&graph, nodes, edges).await?;
    let import_ms = import_start.elapsed().as_millis() as u64;

    // Wait for indexes to settle
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // ------ Viewport queries ------
    let viewport_bounds = [
        (-1.0, -1.0, 1.0, 1.0),
        (-0.5, -0.5, 0.5, 0.5),
        (-0.25, -0.25, 0.25, 0.25),
        (-1.0, -1.0, 0.0, 0.0),
        (0.0, 0.0, 1.0, 1.0),
    ];

    let mut viewport_queries = Vec::new();
    for &(min_x, min_y, max_x, max_y) in &viewport_bounds {
        let t0 = Instant::now();
        let q = query(
            "MATCH (n:Node) WHERE n.x >= $minX AND n.x <= $maxX AND n.y >= $minY AND n.y <= $maxY RETURN count(n)"
        )
        .param("minX", min_x as f64)
        .param("maxX", max_x as f64)
        .param("minY", min_y as f64)
        .param("maxY", max_y as f64);
        let mut result = graph.execute(q).await?;
        let mut matched = 0usize;
        while let Ok(Some(row)) = result.next().await {
            matched = row.get::<i64>("count(n)").unwrap_or(0) as usize;
        }
        let elapsed_us = t0.elapsed().as_micros() as u64;
        viewport_queries.push(ViewportResult {
            bounds: (min_x, min_y, max_x, max_y),
            matched,
            elapsed_us,
        });
    }

    // ------ Neighbor 2-hop traversals ------
    let n = nodes.len();
    let start_ids = [0u64, (n / 4) as u64, (n / 2) as u64, (3 * n / 4) as u64];
    let mut neighbor_traversals = Vec::new();

    for &start_id in &start_ids {
        let t0 = Instant::now();
        let q = query(
            "MATCH (n:Node {id: $id})-[r1:CONNECTS]->(m1:Node)-[r2:CONNECTS]->(m2:Node) RETURN count(m2)"
        )
        .param("id", start_id as i64);
        let mut result = graph.execute(q).await?;
        let mut visited = 0usize;
        while let Ok(Some(row)) = result.next().await {
            visited = row.get::<i64>("count(m2)").unwrap_or(0) as usize;
        }
        let elapsed_us = t0.elapsed().as_micros() as u64;
        neighbor_traversals.push(TraversalResult {
            start_idx: start_id as usize,
            depth: 2,
            visited,
            elapsed_us,
        });
    }

    // ------ Full scan (aggregate) ------
    let t0 = Instant::now();
    let q = query("MATCH (n:Node) RETURN avg(n.x), avg(n.y), count(n)");
    let mut result = graph.execute(q).await?;
    let mut _count = 0i64;
    while let Ok(Some(row)) = result.next().await {
        _count = row.get::<i64>("count(n)").unwrap_or(0);
    }
    let full_scan_ms = t0.elapsed().as_millis() as u64;

    Ok(Neo4jResults {
        import_ms,
        viewport_queries,
        neighbor_traversals,
        full_scan_ms,
    })
}

async fn import_graph(
    graph: &Graph,
    nodes: &[NodeData],
    edges: &[EdgeData],
) -> Result<(), Box<dyn std::error::Error>> {
    // Drop old data if any
    let _ = graph.run(query("MATCH (n) DETACH DELETE n")).await;

    // Create indexes
    graph
        .run(query(
            "CREATE INDEX node_id IF NOT EXISTS FOR (n:Node) ON (n.id)",
        ))
        .await?;
    graph
        .run(query(
            "CREATE INDEX node_coords IF NOT EXISTS FOR (n:Node) ON (n.x, n.y)",
        ))
        .await?;

    // Batch insert nodes (1000 per batch)
    for chunk in nodes.chunks(1000) {
        let mut txn = graph.start_txn().await?;
        for nd in chunk {
            txn.run_queries([
                query("CREATE (n:Node {id: $id, x: $x, y: $y, cluster: $cluster})")
                    .param("id", nd.id as i64)
                    .param("x", nd.x as f64)
                    .param("y", nd.y as f64)
                    .param("cluster", nd.cluster as i64),
            ])
            .await?;
        }
        txn.commit().await?;
    }

    // Batch insert edges (1000 per batch)
    for chunk in edges.chunks(1000) {
        let mut txn = graph.start_txn().await?;
        for ed in chunk {
            txn.run_queries([query(
                "MATCH (a:Node {id: $src}), (b:Node {id: $tgt}) CREATE (a)-[:CONNECTS]->(b)",
            )
            .param("src", ed.source as i64)
            .param("tgt", ed.target as i64)])
                .await?;
        }
        txn.commit().await?;
    }

    Ok(())
}
