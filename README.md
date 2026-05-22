# SeGraph

Se(瑟)Graph is a graph database engine with a WebGPU renderer frontend.

## Crates

| Crate | Purpose |
|---|---|
| `waw_core` | Graph engine: CSR topology, spatial grid index, ECS component store |
| `waw_proto` | rkyv-encoded binary protocol types and messages |
| `waw_server` | WebSocket server: pipeline, hot/warm/cold tier, SQLite backend |
| `waw_client` | Async WebSocket client with typed query methods |
| `benchmark` | WAW vs Neo4j harness — trait-based, same ops for both systems |

## Benchmarks

Synthetic clustered graph, Pareto degree distribution, fixed seed.

```
cargo run -r -p benchmark -- --scales 1K --scales 10K --scales 100K --systems all
```

Neo4j must be running before benchmarking with `--systems neo4j` or `--systems all`. The default credentials are `neo4j` / `neograph` — change password first or pass `--neo4j-pass` / env `NEO4J_PASS`. WAW-only runs (`--systems server`) need no setup.

### 10K nodes (40K edges, Win)

| Operation | WAW | Neo4j | Ratio |
|---|---|---|---|
| Entity get (avg) | 63 μs | 505 μs | WAW **8.1×** |
| BFS depth=2 (avg) | 87 μs | 1,178 μs | WAW **13.6×** |
| Spatial small viewport | 62 μs | 1,556 μs | WAW **25.1×** |
| Spatial mid viewport | 1.2 ms | 25.0 ms | WAW **21.7×** |
| Spatial full viewport | 17.4 ms | 48.7 ms | WAW **2.8×** |
| Full scan | 2.8 ms | 482 μs | Neo4j **5.8×** |
| Import | 126 ms | 1,038 ms | WAW **8.2×** |

### 100K nodes (496K edges, Win)

| Operation | WAW | Neo4j | Ratio |
|---|---|---|---|
| Entity get (avg) | 59 μs | 531 μs | WAW **9.0×** |
| BFS depth=2 (avg) | 89 μs | 761 μs | WAW **8.6×** |
| Spatial small viewport | 380 μs | 10.6 ms | WAW **27.9×** |
| Spatial mid viewport | 12.5 ms | 339 ms | WAW **27.1×** |
| Spatial full viewport | 16.4 ms | 455 ms | WAW **27.7×** |
| Full scan | 15.7 ms | 387 μs | Neo4j **40.6×** |
| Import | 484 ms | 10,271 ms | WAW **21.2×** |
