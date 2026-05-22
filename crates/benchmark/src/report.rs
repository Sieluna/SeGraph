use std::collections::BTreeMap;
use std::io::Write;
use std::path::Path;

use crate::types::{BenchSample, FullBenchmarkReport, SystemResults};

/// Write results as CSV.
pub fn write_csv(report: &FullBenchmarkReport, path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let mut f = std::fs::File::create(path)?;
    writeln!(
        f,
        "scale,system,nodes,edges,connection_ms,import_ms,operation,mean_ns,p50_ns,p95_ns,p99_ns,min_ns,max_ns,stddev_ns,sample_count"
    )?;

    for r in &report.results {
        for s in &r.samples {
            writeln!(
                f,
                "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
                csv_escape(&r.scale_label),
                csv_escape(&r.system),
                r.node_count,
                r.edge_count,
                r.connection_ms.map_or(String::new(), |v| v.to_string()),
                r.import_ms.map_or(String::new(), |v| v.to_string()),
                csv_escape(&s.label),
                s.mean_ns,
                s.p50_ns,
                s.p95_ns,
                s.p99_ns,
                s.min_ns,
                s.max_ns,
                s.stddev_ns,
                s.sample_count,
            )?;
        }
    }

    f.flush()?;
    Ok(())
}

fn csv_escape(s: &str) -> String {
    if s.contains(',') || s.contains('\"') || s.contains('\n') {
        format!("\"{}\"", s.replace('\"', "\"\""))
    } else {
        s.to_string()
    }
}

/// Print a Markdown comparison report to stdout.
pub fn print_markdown(report: &FullBenchmarkReport) {
    println!("\n# Benchmark Report\n");
    println!("**Timestamp**: {}", report.timestamp);
    println!("**Git commit**: `{}`", report.git_commit);
    println!(
        "**System**: {} ({} threads), {}",
        report.system_info.os,
        report.system_info.cpu,
        report.system_info.rust_version
    );

    // Group results by scale
    let mut by_scale: BTreeMap<&str, Vec<&SystemResults>> = BTreeMap::new();
    for r in &report.results {
        by_scale.entry(&r.scale_label).or_default().push(r);
    }

    for (scale, results) in &by_scale {
        let node_count = results.first().map(|r| r.node_count).unwrap_or(0);
        let edge_count = results.first().map(|r| r.edge_count).unwrap_or(0);
        println!("\n## Scale: {} ({} nodes, {} edges)\n", scale, node_count, edge_count);

        // Build comparison table by matching operation labels across systems
        let waw = results.iter().find(|r| r.system == "waw_server");
        let neo = results.iter().find(|r| r.system == "neo4j");

        // Connection / import summary
        if waw.is_some() || neo.is_some() {
            println!("| Metric       | WAW Server    | Neo4j        | Speedup |");
            println!("|-------------|---------------|-------------|---------|");

            if let Some(w) = waw {
                if let Some(c) = w.connection_ms {
                    println!("| Connect     | {} ms        | —            | —       |", c);
                }
                if let Some(imp) = w.import_ms {
                    print!("| Import      | {} ms        ", imp);
                    if let Some(n) = neo {
                        if let Some(n_imp) = n.import_ms {
                            let speedup = n_imp as f64 / imp.max(1) as f64;
                            println!("| {} ms        | {:.1}x   |", n_imp, speedup);
                        } else {
                            println!("| —            | —       |");
                        }
                    } else {
                        println!("| —            | —       |");
                    }
                }
            }

            // Per-operation comparison
            if let (Some(w), Some(n)) = (waw, neo) {
                println!();
                println!("| Operation              | WAW Server       | Neo4j            | Speedup    |");
                println!("|-----------------------|------------------|------------------|------------|");

                // Match samples by cleaned label
                let w_samples = group_by_op(&w.samples);
                let n_samples = group_by_op(&n.samples);

                for (op_key, w_sample) in &w_samples {
                    if let Some(n_sample) = n_samples.get(op_key) {
                        let speedup = n_sample.mean_ns / w_sample.mean_ns.max(1.0);
                        let w_display = format_duration(w_sample.mean_ns);
                        let n_display = format_duration(n_sample.mean_ns);
                        let op_display = format_op_label(op_key, 21);
                        println!(
                            "| {:<21} | {:>16} | {:>16} | {:>8.1}x   |",
                            op_display, w_display, n_display, speedup
                        );
                    }
                }

                // Operations only in one system
                for (op_key, w_sample) in &w_samples {
                    if !n_samples.contains_key(op_key) {
                        println!(
                            "| {:<21} | {:>16} | {:>16} | {:>10} |",
                            format_op_label(op_key, 21),
                            format_duration(w_sample.mean_ns),
                            "—",
                            "—"
                        );
                    }
                }
            } else if let Some(w) = waw {
                println!();
                println!("| Operation              | WAW Server       |");
                println!("|-----------------------|------------------|");
                for s in &w.samples {
                    let key = op_key(&s.label);
                    println!(
                        "| {:<21} | {:>16} |",
                        format_op_label(&key, 21),
                        format_duration(s.mean_ns)
                    );
                }
            }
        }
    }
}

/// Group samples by operation type, averaging multiple instances (e.g. entity_get
/// with different IDs) into a single representative per operation.
fn group_by_op(samples: &[BenchSample]) -> BTreeMap<String, BenchSample> {
    let mut groups: BTreeMap<String, Vec<&BenchSample>> = BTreeMap::new();
    for s in samples {
        groups.entry(op_key(&s.label)).or_default().push(s);
    }
    groups
        .into_iter()
        .map(|(key, group)| {
            let avg_mean = group.iter().map(|s| s.mean_ns).sum::<f64>() / group.len() as f64;
            let avg_p95 = group.iter().map(|s| s.p95_ns).sum::<f64>() / group.len() as f64;
            let avg_p99 = group.iter().map(|s| s.p99_ns).sum::<f64>() / group.len() as f64;
            (
                key,
                BenchSample {
                    label: group[0].label.clone(),
                    mean_ns: avg_mean,
                    median_ns: 0.0,
                    p50_ns: 0.0,
                    p95_ns: avg_p95,
                    p99_ns: avg_p99,
                    min_ns: 0.0,
                    max_ns: 0.0,
                    stddev_ns: 0.0,
                    sample_count: group.len() as u32,
                },
            )
        })
        .collect()
}

/// Extract an operation category key from a sample label.
fn op_key(label: &str) -> String {
    if label.starts_with("entity_get") {
        "entity_get".to_string()
    } else if label.starts_with("spatial_[-1.00_-1.00_1.00_1.00]") {
        "spatial_full_view".to_string()
    } else if label.starts_with("spatial_[-0.50_-0.50_0.50_0.50]") {
        "spatial_mid".to_string()
    } else if label.starts_with("spatial_[-0.25_-0.25_0.25_0.25]") {
        "spatial_small".to_string()
    } else if label.starts_with("spatial_[-1.00_-1.00_0.00_0.00]") {
        "spatial_ll".to_string()
    } else if label.starts_with("spatial_[0.00_0.00_1.00_1.00]") {
        "spatial_ur".to_string()
    } else if label.starts_with("spatial_") {
        "spatial".to_string()
    } else if label.starts_with("bfs") {
        "bfs_depth2".to_string()
    } else if label.starts_with("full_scan") {
        "full_scan".to_string()
    } else {
        label.to_string()
    }
}

/// Format an operation key for display.
fn format_op_label(key: &str, width: usize) -> String {
    let display = match key {
        "entity_get" => "Entity get (hot)",
        "spatial_full_view" => "Spatial [full]",
        "spatial_mid" => "Spatial [mid]",
        "spatial_small" => "Spatial [small]",
        "spatial_ll" => "Spatial [LL]",
        "spatial_ur" => "Spatial [UR]",
        "spatial" => "Spatial query",
        "bfs_depth2" => "BFS depth=2",
        "full_scan" => "Full scan",
        _ => key,
    };
    if display.len() > width {
        display[..width].to_string()
    } else {
        format!("{:<width$}", display, width = width)
    }
}

/// Format a nanosecond duration for human display.
fn format_duration(ns: f64) -> String {
    if ns >= 1_000_000_000.0 {
        format!("{:.2} s", ns / 1_000_000_000.0)
    } else if ns >= 1_000_000.0 {
        format!("{:.2} ms", ns / 1_000_000.0)
    } else if ns >= 1_000.0 {
        format!("{:.2} μs", ns / 1_000.0)
    } else {
        format!("{:.0} ns", ns)
    }
}
