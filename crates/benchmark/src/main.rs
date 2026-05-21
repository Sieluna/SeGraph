mod graph_gen;
mod neo4j_bench;
mod waw_core_bench;
mod waw_server_bench;

use std::time::Instant;

use graph_gen::{GraphConfig, generate_graph, to_csv};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let mode = args.get(1).map(|s| s.as_str()).unwrap_or("all");

    let configs = vec![(
        "10K",
        GraphConfig {
            node_count: 10_000,
            avg_degree: 5,
            clusters: 4,
            ..Default::default()
        },
    )];

    for (label, config) in &configs {
        println!("\n{:=<60}", "");
        println!(
            "  SCALE: {} nodes, avg_degree={}, clusters={}",
            label, config.avg_degree, config.clusters
        );
        println!("{:=<60}", "");

        let (nodes, edges) = generate_graph(config);
        println!("  Generated {} nodes, {} edges", nodes.len(), edges.len());

        // waw_core benchmark: pure in-memory spatial grid + raw vec traversal
        if mode == "all" || mode == "waw" || mode == "core" {
            println!("\n  --- waw_core (pure memory, spatial grid) ---");
            let t0 = Instant::now();
            let results = waw_core_bench::run_benchmarks(&nodes, &edges);
            println!("  Load:      {:>8} ms", results.load_ms);
            println!(
                "  Full scan: {:>8} ms ({} nodes)",
                results.full_scan_ms,
                nodes.len()
            );
            for vp in &results.viewport_queries {
                println!(
                    "  Viewport [{:.2}..{:.2}, {:.2}..{:.2}]: {:>8} μs, matched {}",
                    vp.bounds.0, vp.bounds.2, vp.bounds.1, vp.bounds.3, vp.elapsed_us, vp.matched
                );
            }
            for tr in &results.neighbor_traversals {
                println!(
                    "  Traversal depth={} from #{}: {:>8} μs, visited {}",
                    tr.depth, tr.start_idx, tr.elapsed_us, tr.visited
                );
            }
            let total_ms = t0.elapsed().as_millis();
            println!("  Total core time: {} ms", total_ms);
        }

        // waw_server benchmark: Pipeline (waw_core Storage + CSR + tiered cache)
        if mode == "all" || mode == "waw" || mode == "server" {
            println!("\n  --- waw_server (Pipeline: waw_core + CSR + warm/cold tiers) ---");
            let t0 = Instant::now();
            let results = waw_server_bench::run_benchmarks(&nodes, &edges);
            println!("  Load:       {:>8} ms", results.load_ms);
            println!("  Memory:     {:>8} bytes", results.memory_used_bytes);
            println!("  Entities:   {:>8}", results.entity_count);
            println!("  Edges:      {:>8}", results.edge_count);
            for eg in &results.entity_gets {
                println!(
                    "  GetEntity #{}: {:>8} μs (hit={})",
                    eg.id, eg.elapsed_us, eg.hit
                );
            }
            for vp in &results.spatial_queries {
                println!(
                    "  Spatial [{:.2}..{:.2}, {:.2}..{:.2}]: {:>8} μs, matched {}",
                    vp.bounds.0, vp.bounds.2, vp.bounds.1, vp.bounds.3, vp.elapsed_us, vp.matched
                );
            }
            for tr in &results.traversals {
                println!(
                    "  Traversal depth={} from #{}: {:>8} μs, visited {}",
                    tr.depth, tr.start_id, tr.elapsed_us, tr.visited
                );
            }
            println!(
                "  Full scan:  {:>8} ms ({} nodes)",
                results.full_scan_ms,
                nodes.len()
            );
            let total_ms = t0.elapsed().as_millis();
            println!("  Total pipeline time: {} ms", total_ms);
        }

        // Run Neo4j benchmark (if available and mode allows)
        if mode == "all" || mode == "neo4j" {
            println!("\n  --- Neo4j (via neo4rs) ---");
            match neo4j_bench::run_benchmarks(&nodes, &edges, "127.0.0.1:7687", "neo4j", "neograph")
                .await
            {
                Ok(results) => {
                    println!("  Import:     {:>8} ms", results.import_ms);
                    println!("  Full scan:  {:>8} ms", results.full_scan_ms);
                    for vp in &results.viewport_queries {
                        println!(
                            "  Viewport [{:.2}..{:.2}, {:.2}..{:.2}]: {:>8} μs, matched {}",
                            vp.bounds.0,
                            vp.bounds.2,
                            vp.bounds.1,
                            vp.bounds.3,
                            vp.elapsed_us,
                            vp.matched
                        );
                    }
                    for tr in &results.neighbor_traversals {
                        println!(
                            "  Traversal depth={} from #{}: {:>8} μs, visited {}",
                            tr.depth, tr.start_idx, tr.elapsed_us, tr.visited
                        );
                    }
                }
                Err(e) => {
                    println!("  Neo4j not available: {e}");
                    println!(
                        "  Start Neo4j first, then re-run with: cargo run -p benchmark -- neo4j"
                    );
                }
            }
        }

        // Export CSVs for neo4j-admin import
        let (nodes_csv, edges_csv) = to_csv(&nodes, &edges);
        let prefix = format!("benchmark_data_{}", label);
        let data_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../data");
        std::fs::create_dir_all(&data_dir)?;
        std::fs::write(data_dir.join(format!("{}_nodes.csv", prefix)), nodes_csv)?;
        std::fs::write(data_dir.join(format!("{}_edges.csv", prefix)), edges_csv)?;
    }

    println!("\nDone. CSV data files written for neo4j-admin bulk import.");
    Ok(())
}
