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
| Entity get (avg) | 63 μs | 345 μs | WAW **5.5×** |
| BFS depth=2 (avg) | 230 μs | 680 μs | WAW **3.0×** |
| Spatial small viewport | 49 μs | 1,220 μs | WAW **24.9×** |
| Spatial full viewport | 63 ms | 43 ms | Neo4j **1.5×** |
| Spatial mid viewport | 34 ms | 22 ms | Neo4j **1.5×** |
| Import | 82 ms | 1,038 ms | WAW **12.7×** |

### 100K nodes (496K edges, Win)

| Operation | WAW | Neo4j | Ratio |
|---|---|---|---|
| Entity get (avg) | 59 μs | 531 μs | WAW **9.0×** |
| BFS depth=2 (avg) | 349 μs | 761 μs | WAW **2.2×** |
| Spatial small viewport | 8.7 ms | 10.6 ms | WAW **1.2×** |
| Spatial full viewport | 713 ms | 455 ms | Neo4j **1.6×** |
| Spatial mid viewport | 526 ms | 339 ms | Neo4j **1.6×** |
| Import | 381 ms | 10,271 ms | WAW **27.0×** |
