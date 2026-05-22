use crate::types::BenchSample;

/// Warmup and measurement iteration counts.
pub struct BenchRunner {
    pub warmup_iterations: u32,
    pub measured_iterations: u32,
}

impl Default for BenchRunner {
    fn default() -> Self {
        Self {
            warmup_iterations: 10,
            measured_iterations: 100,
        }
    }
}

impl BenchRunner {
    pub fn new(warmup: u32, measured: u32) -> Self {
        Self {
            warmup_iterations: warmup,
            measured_iterations: measured,
        }
    }

    pub fn collect(&self, label: &str, durations_ns: &[f64]) -> BenchSample {
        BenchSample::from_durations(label, durations_ns)
    }
}

/// Time an async expression: warmup iterations then measured iterations.
#[macro_export]
macro_rules! bench_async {
    ($runner:expr, $label:expr, || $body:expr) => {{
        for _ in 0..$runner.warmup_iterations {
            $body;
        }
        let mut __durations = Vec::with_capacity($runner.measured_iterations as usize);
        for _ in 0..$runner.measured_iterations {
            let __t = std::time::Instant::now();
            $body;
            __durations.push(__t.elapsed().as_nanos() as f64);
        }
        $runner.collect($label, &__durations)
    }};
}
