use rand::RngExt;
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand_distr::{Distribution, Pareto};

/// A generated node with position and cluster.
#[derive(Clone, Debug)]
pub struct NodeData {
    pub id: u64,
    pub x: f32,
    pub y: f32,
    pub cluster: u32,
}

/// A directed edge between two nodes.
#[derive(Clone, Debug)]
pub struct EdgeData {
    pub source: u64,
    pub target: u64,
}

/// Parameters controlling graph topology.
pub struct GraphConfig {
    pub node_count: u64,
    /// Average out-degree per node.
    pub avg_degree: u32,
    /// Number of spatial clusters (centers of density).
    pub clusters: u32,
    /// Pareto shape parameter for edge distribution (> 1 = heavy tail).
    pub pareto_shape: f64,
    pub seed: u64,
}

impl Default for GraphConfig {
    fn default() -> Self {
        Self {
            node_count: 10_000,
            avg_degree: 5,
            clusters: 8,
            pareto_shape: 2.0,
            seed: 42,
        }
    }
}

/// Generate a synthetic graph with power-law degree distribution
/// and clustered 2D spatial layout.
pub fn generate_graph(config: &GraphConfig) -> (Vec<NodeData>, Vec<EdgeData>) {
    let mut rng = StdRng::seed_from_u64(config.seed);
    let n = config.node_count;
    let total_edges = n * config.avg_degree as u64;

    // Generate cluster centers (evenly spaced on a roughly circular pattern)
    let centers: Vec<(f32, f32)> = (0..config.clusters)
        .map(|c| {
            let angle = c as f32 / config.clusters as f32 * std::f32::consts::TAU;
            let radius = 0.5;
            (radius * angle.cos(), radius * angle.sin())
        })
        .collect();

    // Generate nodes with positions clustered around centers
    let nodes: Vec<NodeData> = (0..n)
        .map(|id| {
            let cluster = rng.random_range(0..config.clusters);
            let (cx, cy) = centers[cluster as usize];
            let spread = 0.15;
            let x = (cx + rng.random_range(-spread..spread)).clamp(-1.0, 1.0);
            let y = (cy + rng.random_range(-spread..spread)).clamp(-1.0, 1.0);
            NodeData { id, x, y, cluster }
        })
        .collect();

    // Generate edges with power-law degree distribution
    let pareto = Pareto::new(1.0, config.pareto_shape).unwrap();
    let edges = generate_edges(&nodes, total_edges, &pareto, &mut rng);

    (nodes, edges)
}

fn generate_edges(
    nodes: &[NodeData],
    total: u64,
    degree_dist: &Pareto<f64>,
    rng: &mut StdRng,
) -> Vec<EdgeData> {
    let n = nodes.len() as u64;

    // Pre-compute degrees: some nodes get many edges, most get few
    let mut degrees: Vec<u64> = (0..n)
        .map(|_| {
            let d = degree_dist.sample(rng) as u64;
            d.min(n / 2).max(1)
        })
        .collect();

    // Normalize to hit target total
    let current_sum: u64 = degrees.iter().sum();
    let scale = total as f64 / current_sum as f64;
    for d in &mut degrees {
        *d = (*d as f64 * scale).max(1.0) as u64;
    }

    // Favor connections within same cluster (spatial locality)
    let mut edges = Vec::with_capacity(total as usize);
    let mut edge_count = 0u64;

    for (i, node) in nodes.iter().enumerate() {
        let mut remaining = degrees[i];
        while remaining > 0 && edge_count < total {
            // 70% chance same cluster, 30% any cluster
            let coin: f32 = rng.random();
            let target = if coin < 0.7 {
                // Pick random node in same cluster
                let same_cluster: Vec<usize> = nodes
                    .iter()
                    .enumerate()
                    .filter(|(_, nd)| nd.cluster == node.cluster && nd.id != node.id)
                    .map(|(j, _)| j)
                    .collect();
                if same_cluster.is_empty() {
                    rng.random_range(0..nodes.len())
                } else {
                    same_cluster[rng.random_range(0..same_cluster.len())]
                }
            } else {
                rng.random_range(0..nodes.len())
            };

            if target != i {
                edges.push(EdgeData {
                    source: node.id,
                    target: nodes[target].id,
                });
                edge_count += 1;
            }
            remaining -= 1;
        }
    }

    edges
}

/// Serialize nodes and edges to CSV strings for Neo4j import.
pub fn to_csv(nodes: &[NodeData], edges: &[EdgeData]) -> (String, String) {
    let nodes_csv = {
        let mut csv = String::from("id:ID,x:FLOAT,y:FLOAT,cluster:INT\n");
        for n in nodes {
            csv.push_str(&format!("{},{:.6},{:.6},{}\n", n.id, n.x, n.y, n.cluster));
        }
        csv
    };

    let edges_csv = {
        let mut csv = String::from(":START_ID,:END_ID,:TYPE\n");
        for e in edges {
            csv.push_str(&format!("{},{},CONNECTS\n", e.source, e.target));
        }
        csv
    };

    (nodes_csv, edges_csv)
}
