mod bench_runner;
mod graph_gen;
mod neo4j_bench;
mod report;
mod types;
mod waw_bench;

use std::path::PathBuf;
use std::time::Instant;

use bench_runner::BenchRunner;
use clap::Parser;
use graph_gen::{generate_graph, GraphScale};
use types::FullBenchmarkReport;

#[derive(Parser)]
#[command(name = "waw-bench", about = "WAW vs Neo4j benchmark harness")]
struct Cli {
    /// Graph scales to benchmark: 1K, 10K, 100K, 1M
    #[arg(long, default_value = "10K")]
    scales: Vec<String>,

    /// Systems to benchmark: server, neo4j, all
    #[arg(long, default_value = "all")]
    systems: String,

    /// Warmup iterations per operation
    #[arg(long, default_value_t = 10)]
    warmup: u32,

    /// Measured iterations per operation
    #[arg(long, default_value_t = 100)]
    iterations: u32,

    /// Neo4j Bolt URI (also settable via NEO4J_URI env var)
    #[arg(long, default_value = "127.0.0.1:7687")]
    neo4j_uri: String,

    /// Neo4j username (also settable via NEO4J_USER env var)
    #[arg(long, default_value = "neo4j")]
    neo4j_user: String,

    /// Neo4j password (also settable via NEO4J_PASS env var)
    #[arg(long, default_value = "neograph")]
    neo4j_pass: String,

    /// Output directory for JSON/CSV reports
    #[arg(long, default_value = "./results")]
    output_dir: PathBuf,

    /// Output formats: json, csv, md (default: md)
    #[arg(long, default_value = "md")]
    format: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let runner = BenchRunner::new(cli.warmup, cli.iterations);
    let mut all_results = Vec::new();

    // Env vars override CLI defaults
    let neo4j_uri = std::env::var("NEO4J_URI").unwrap_or(cli.neo4j_uri);
    let neo4j_user = std::env::var("NEO4J_USER").unwrap_or(cli.neo4j_user);
    let neo4j_pass = std::env::var("NEO4J_PASS").unwrap_or(cli.neo4j_pass);

    let do_server = cli.systems == "all" || cli.systems == "server" || cli.systems == "waw";
    let do_neo4j = cli.systems == "all" || cli.systems == "neo4j";

    for scale_str in &cli.scales {
        let Some(scale) = GraphScale::from_str(scale_str) else {
            eprintln!("Unknown scale: {scale_str}. Use 1K, 10K, 100K, or 1M.");
            continue;
        };
        let config = scale.config();

        println!(
            "\n{:=<70}",
            ""
        );
        println!(
            "  SCALE: {} — {} nodes, avg_degree={}, clusters={}",
            scale.label(),
            config.node_count,
            config.avg_degree,
            config.clusters
        );
        println!("{:=<70}", "");

        let gen_start = Instant::now();
        let (nodes, edges) = generate_graph(&config);
        let gen_ms = gen_start.elapsed().as_millis() as u64;
        println!(
            "  Generated {} nodes, {} edges in {} ms",
            nodes.len(),
            edges.len(),
            gen_ms
        );

        // WAW Server
        if do_server {
            println!("\n  --- WAW Server (WebSocket) ---");

            let db_start = Instant::now();
            let (_tmp, db_path) = waw_bench::create_bench_db(&nodes, &edges);
            let db_create_ms = db_start.elapsed().as_millis() as u64;

            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let addr = listener.local_addr()?;
            let url = format!("ws://{addr}/graph");

            let index_start = Instant::now();
            let db_path_clone = db_path.clone();
            let server = tokio::spawn(async move {
                let _ =
                    waw_server::serve_sqlite_on_listener(&db_path_clone, None::<&str>, listener)
                        .await;
            });

            // Poll until server accepts connections
            let mut server_ready = false;
            for _attempt in 0..30 {
                if tokio::net::TcpStream::connect(addr).await.is_ok() {
                    server_ready = true;
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            if !server_ready {
                eprintln!("  WAW server failed to start within 3s, skipping.");
                server.abort();
                continue;
            }
            let index_build_ms = index_start.elapsed().as_millis() as u64;

            match waw_bench::run_benchmarks(
                    &url,
                    &nodes,
                    edges.len() as u64,
                    &runner,
                    db_create_ms,
                    index_build_ms,
                )
                .await
            {
                Ok(mut results) => {
                    results.scale_label = scale.label().to_string();
                    println!(
                        "  Connect:      {:>8} ms",
                        results.connection_ms.unwrap_or(0)
                    );
                    println!(
                        "  SQLite create:{:>8} ms",
                        db_create_ms
                    );
                    println!(
                        "  Index build:  {:>8} ms",
                        index_build_ms
                    );
                    println!(
                        "  Import total: {:>8} ms",
                        results.import_ms.unwrap_or(0)
                    );
                    print_samples(&results);
                    all_results.push(results);
                }
                Err(e) => eprintln!("  WAW server error: {e}"),
            }

            server.abort();
            drop(_tmp);
        }

        // Neo4j
        if do_neo4j {
            println!("\n  --- Neo4j (Bolt) ---");

            match neo4j_bench::run_benchmarks(
                &nodes,
                &edges,
                &neo4j_uri,
                &neo4j_user,
                &neo4j_pass,
                &runner,
            )
            .await
            {
                Ok(mut results) => {
                    results.scale_label = scale.label().to_string();
                    println!("  Import:     {:>8} ms", results.import_ms.unwrap_or(0));
                    print_samples(&results);
                    all_results.push(results);
                }
                Err(e) => {
                    eprintln!("  Neo4j unavailable: {e}");
                    eprintln!(
                        "  Start Neo4j and re-run with: cargo run -p benchmark -- --systems neo4j"
                    );
                }
            }
        }
    }

    // Reports
    if !all_results.is_empty() {
        let report = FullBenchmarkReport::new(all_results);
        let output_dir = &cli.output_dir;
        std::fs::create_dir_all(output_dir)?;

        for fmt in &cli.format {
            match fmt.as_str() {
                "json" => {
                    let path = output_dir.join("benchmark_results.json");
                    std::fs::write(&path, serde_json::to_string_pretty(&report)?)?;
                    println!("\nJSON report written to {}", path.display());
                }
                "csv" => {
                    let path = output_dir.join("benchmark_results.csv");
                    report::write_csv(&report, &path)?;
                    println!("CSV report written to {}", path.display());
                }
                "md" | "markdown" => {
                    report::print_markdown(&report);
                }
                _ => eprintln!("Unknown format: {fmt}. Use json, csv, or md."),
            }
        }
    } else {
        println!("\nNo results collected.");
    }

    Ok(())
}

fn print_samples(results: &types::SystemResults) {
    for s in &results.samples {
        let short = s.label
            .replace("entity_get_", "get#")
            .replace("bfs_depth2_from_", "bfs#");
        println!(
            "  {:<40} {:>8.1} μs  (p95={:>8.1}, n={})",
            short,
            s.mean_ns / 1000.0,
            s.p95_ns / 1000.0,
            s.sample_count
        );
    }
}
