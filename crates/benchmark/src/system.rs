use crate::bench_async;
use crate::bench_runner::BenchRunner;
use crate::types::SystemResults;

/// A graph system that can execute the standard benchmark query types.
///
/// Each method performs one operation without warmup or measurement.
/// Timing is handled by [`run_benchmark_suite`].
pub trait BenchSystem {
    async fn bench_get_entity(&mut self, id: u64) -> Result<(), Box<dyn std::error::Error>>;
    async fn bench_search_spatial(
        &mut self,
        min_x: f64,
        min_y: f64,
        max_x: f64,
        max_y: f64,
    ) -> Result<(), Box<dyn std::error::Error>>;
    async fn bench_traverse_bfs(
        &mut self,
        start_id: u64,
    ) -> Result<(), Box<dyn std::error::Error>>;
    async fn bench_full_scan(&mut self) -> Result<(), Box<dyn std::error::Error>>;
}

/// Run the standard benchmark suite against any [`BenchSystem`].
///
/// Executes 15 operations: 5 entity gets, 5 spatial queries, 4 BFS traversals, 1 full scan.
pub async fn run_benchmark_suite<S: BenchSystem>(
    system: &mut S,
    system_name: &str,
    scale_label: &str,
    node_count: u64,
    edge_count: u64,
    connection_ms: Option<u64>,
    import_ms: Option<u64>,
    runner: &BenchRunner,
) -> SystemResults {
    let n = node_count;
    let mut samples = Vec::with_capacity(15);

    // Entity gets
    for &id in &[0u64, n / 4, n / 2, 3 * n / 4, n.saturating_sub(1)] {
        if id >= n {
            continue;
        }
        let label = format!("entity_get_{id}");
        let s = bench_async!(runner, &label, || system.bench_get_entity(id).await.unwrap());
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
        let s = bench_async!(runner, &label, || system
            .bench_search_spatial(
                min_x as f64,
                min_y as f64,
                max_x as f64,
                max_y as f64,
            )
            .await
            .unwrap());
        samples.push(s);
    }

    // BFS traversals
    for &start_id in &[0u64, n / 4, n / 2, 3 * n / 4] {
        if start_id >= n {
            continue;
        }
        let label = format!("bfs_depth2_from_{start_id}");
        let s = bench_async!(runner, &label, || system.bench_traverse_bfs(start_id).await.unwrap());
        samples.push(s);
    }

    // Full scan
    {
        let s = bench_async!(runner, "full_scan", || system.bench_full_scan().await.unwrap());
        samples.push(s);
    }

    SystemResults {
        system: system_name.to_string(),
        scale_label: scale_label.to_string(),
        node_count,
        edge_count,
        connection_ms,
        import_ms,
        samples,
    }
}
