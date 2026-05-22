use serde::Serialize;

/// A single benchmark measurement with full statistics.
#[derive(Clone, Debug, Serialize)]
pub struct BenchSample {
    pub label: String,
    pub mean_ns: f64,
    pub median_ns: f64,
    pub p50_ns: f64,
    pub p95_ns: f64,
    pub p99_ns: f64,
    pub min_ns: f64,
    pub max_ns: f64,
    pub stddev_ns: f64,
    pub sample_count: u32,
}

impl BenchSample {
    /// Compute statistics from a vector of nanosecond durations.
    pub fn from_durations(label: &str, durations_ns: &[f64]) -> Self {
        let count = durations_ns.len() as u32;
        if count == 0 {
            return Self {
                label: label.to_string(),
                mean_ns: 0.0,
                median_ns: 0.0,
                p50_ns: 0.0,
                p95_ns: 0.0,
                p99_ns: 0.0,
                min_ns: 0.0,
                max_ns: 0.0,
                stddev_ns: 0.0,
                sample_count: 0,
            };
        }

        let mut sorted: Vec<f64> = durations_ns.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let sum: f64 = sorted.iter().sum();
        let mean = sum / count as f64;

        let median = percentile(&sorted, 0.50);
        let p50 = median;
        let p95 = percentile(&sorted, 0.95);
        let p99 = percentile(&sorted, 0.99);
        let min = sorted[0];
        let max = sorted[count as usize - 1];

        let variance = if count > 1 {
            sorted.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (count - 1) as f64
        } else {
            0.0
        };
        let stddev = variance.sqrt();

        Self {
            label: label.to_string(),
            mean_ns: mean,
            median_ns: median,
            p50_ns: p50,
            p95_ns: p95,
            p99_ns: p99,
            min_ns: min,
            max_ns: max,
            stddev_ns: stddev,
            sample_count: count,
        }
    }
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = (p * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx]
}

/// Per-system results for one graph scale.
#[derive(Clone, Debug, Serialize)]
pub struct SystemResults {
    pub system: String,
    pub scale_label: String,
    pub node_count: u64,
    pub edge_count: u64,
    pub connection_ms: Option<u64>,
    pub import_ms: Option<u64>,
    pub samples: Vec<BenchSample>,
}

/// Information about the benchmark environment.
#[derive(Clone, Debug, Serialize)]
pub struct SystemInfo {
    pub cpu: String,
    pub memory_mb: u64,
    pub os: String,
    pub rust_version: String,
}

impl SystemInfo {
    pub fn gather() -> Self {
        let cpu = std::thread::available_parallelism()
            .map(|n| format!("{} logical cores", n.get()))
            .unwrap_or_else(|_| "unknown".to_string());

        let os = format!("{} {}", std::env::consts::OS, std::env::consts::ARCH);

        let rust_version = option_env!("RUSTC_VERSION")
            .unwrap_or("unknown")
            .to_string();

        Self {
            cpu,
            memory_mb: 0,
            os,
            rust_version,
        }
    }
}

/// Top-level benchmark report.
#[derive(Clone, Debug, Serialize)]
pub struct FullBenchmarkReport {
    pub timestamp: String,
    pub git_commit: String,
    pub system_info: SystemInfo,
    pub results: Vec<SystemResults>,
}

impl FullBenchmarkReport {
    pub fn new(results: Vec<SystemResults>) -> Self {
        let timestamp = chrono_like_now();
        let git_commit = git_short_hash();
        let system_info = SystemInfo::gather();

        Self {
            timestamp,
            git_commit,
            system_info,
            results,
        }
    }
}

fn chrono_like_now() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format!("unix_{secs}")
}

fn git_short_hash() -> String {
    std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string())
}
