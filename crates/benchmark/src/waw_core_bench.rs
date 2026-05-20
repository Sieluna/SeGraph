use std::time::Instant;

use crate::graph_gen::{EdgeData, NodeData};

const GRID_BITS: u32 = 5;
const GRID_SIZE: usize = 1 << GRID_BITS; // 32

pub struct WawNode {
    pub x: f32,
    pub y: f32,
    pub edges: Vec<usize>, // edge indices
}

pub struct WawEdge {
    pub target: usize, // target node index (into nodes vec)
}

pub struct WawGraph {
    pub nodes: Vec<WawNode>,
    pub edges: Vec<WawEdge>,
    pub spatial_grid: Vec<Vec<usize>>, // grid[cell_idx] -> list of node indices
}

impl WawGraph {
    pub fn load(nodes_data: &[NodeData], edges_data: &[EdgeData]) -> Self {
        let n = nodes_data.len();
        let e = edges_data.len();

        // Build id -> node_index mapping (id equals position in nodes_data)
        let mut id_to_idx: Vec<usize> = vec![0; n];
        for (i, nd) in nodes_data.iter().enumerate() {
            id_to_idx[nd.id as usize] = i;
        }

        // All nodes with empty edge lists
        let mut nodes: Vec<WawNode> = nodes_data
            .iter()
            .map(|nd| WawNode {
                x: nd.x,
                y: nd.y,
                edges: Vec::new(),
            })
            .collect();

        // All edges as raw target indices
        let mut edges: Vec<WawEdge> = Vec::with_capacity(e);
        for ed in edges_data {
            let src_idx = id_to_idx[ed.source as usize];
            let tgt_idx = id_to_idx[ed.target as usize];
            let edge_idx = edges.len();
            edges.push(WawEdge { target: tgt_idx });
            nodes[src_idx].edges.push(edge_idx);
        }

        // Build uniform spatial grid
        let mut spatial_grid: Vec<Vec<usize>> = vec![Vec::new(); GRID_SIZE * GRID_SIZE];
        for (i, node) in nodes.iter().enumerate() {
            let gx = coord_to_grid(node.x);
            let gy = coord_to_grid(node.y);
            spatial_grid[gy * GRID_SIZE + gx].push(i);
        }

        WawGraph {
            nodes,
            edges,
            spatial_grid,
        }
    }
}

fn coord_to_grid(v: f32) -> usize {
    let clamped = (v + 1.0) * 0.5; // [-1,1] -> [0,1]
    let idx = (clamped * GRID_SIZE as f32) as usize;
    idx.min(GRID_SIZE - 1)
}

pub struct WawResults {
    pub load_ms: u64,
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

/// Run all benchmarks against the waw_core graph.
pub fn run_benchmarks(nodes: &[NodeData], edges: &[EdgeData]) -> WawResults {
    // ------ Load ------
    let load_start = Instant::now();
    let graph = WawGraph::load(nodes, edges);
    let load_ms = load_start.elapsed().as_millis() as u64;

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
        let matched = viewport_query(&graph, min_x, min_y, max_x, max_y);
        let elapsed_us = t0.elapsed().as_micros() as u64;
        viewport_queries.push(ViewportResult {
            bounds: (min_x, min_y, max_x, max_y),
            matched,
            elapsed_us,
        });
    }

    // ------ Neighbor traversals ------
    let n = graph.nodes.len();
    let start_indices = [0, n / 4, n / 2, 3 * n / 4];
    let mut neighbor_traversals = Vec::new();

    for &idx in &start_indices {
        if idx >= n {
            continue;
        }
        let t0 = Instant::now();
        let visited_count = traverse_2hop(&graph, idx);
        let elapsed_us = t0.elapsed().as_micros() as u64;
        neighbor_traversals.push(TraversalResult {
            start_idx: idx,
            depth: 2,
            visited: visited_count,
            elapsed_us,
        });
    }

    // ------ Full scan ------
    let t0 = Instant::now();
    let mut total_x = 0.0f64;
    let mut total_y = 0.0f64;
    let mut count = 0u64;
    for node in &graph.nodes {
        total_x += node.x as f64;
        total_y += node.y as f64;
        count += 1;
    }
    let _avg_x = total_x / count as f64;
    let _avg_y = total_y / count as f64;
    let full_scan_ms = t0.elapsed().as_millis() as u64;

    WawResults {
        load_ms,
        viewport_queries,
        neighbor_traversals,
        full_scan_ms,
    }
}

fn viewport_query(graph: &WawGraph, min_x: f32, min_y: f32, max_x: f32, max_y: f32) -> usize {
    let gx0 = coord_to_grid(min_x);
    let gy0 = coord_to_grid(min_y);
    let gx1 = coord_to_grid(max_x);
    let gy1 = coord_to_grid(max_y);

    let mut matched = 0;
    for gy in gy0..=gy1 {
        let row_base = gy * GRID_SIZE;
        for gx in gx0..=gx1 {
            for &node_idx in &graph.spatial_grid[row_base + gx] {
                let node = unsafe { graph.nodes.get_unchecked(node_idx) };
                if node.x >= min_x && node.x <= max_x && node.y >= min_y && node.y <= max_y {
                    matched += 1;
                }
            }
        }
    }
    matched
}

fn traverse_2hop(graph: &WawGraph, start: usize) -> usize {
    let mut count = 0;
    let start_node = &graph.nodes[start];

    for &edge_idx in &start_node.edges {
        let tgt = graph.edges[edge_idx].target;
        let tgt_node = &graph.nodes[tgt];
        count += 1;
        for &edge2_idx in &tgt_node.edges {
            let tgt2 = graph.edges[edge2_idx].target;
            let _ = &graph.nodes[tgt2];
            count += 1;
        }
    }
    count
}
