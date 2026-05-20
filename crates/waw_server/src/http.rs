use std::{
    error, fmt,
    net::{AddrParseError, SocketAddr},
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
};

use axum::{
    Json, Router,
    extract::{
        State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    response::IntoResponse,
    routing::get,
};
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use tokio::net::TcpListener;
use waw_proto::{
    BlobRef, ClientMessage, CodecSet, Direction, EntityData, GetBlob, GetEdges, GetEntity,
    IndexQuery, Property, PropertyType, ServerMessage, ServerStats, Traverse, Value,
    decode_client_message, encode_server_message,
};

use crate::{GraphStats, SqliteGraphStore, StoreError, graph_store::GraphStore};

type WsSender = SplitSink<WebSocket, Message>;

#[derive(Debug)]
pub enum HttpError {
    Addr(AddrParseError),
    Bind(std::io::Error),
    Serve(std::io::Error),
    Store(StoreError),
}

impl From<StoreError> for HttpError {
    fn from(source: StoreError) -> Self {
        Self::Store(source)
    }
}

impl fmt::Display for HttpError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Addr(source) => write!(formatter, "invalid listen address: {source}"),
            Self::Bind(source) => write!(formatter, "failed to bind HTTP listener: {source}"),
            Self::Serve(source) => write!(formatter, "HTTP server failed: {source}"),
            Self::Store(source) => source.fmt(formatter),
        }
    }
}

impl error::Error for HttpError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Self::Addr(source) => Some(source),
            Self::Bind(source) | Self::Serve(source) => Some(source),
            Self::Store(source) => Some(source),
        }
    }
}

type StoreLock = Arc<std::sync::Mutex<SqliteGraphStore>>;
type GraphLock = Arc<std::sync::RwLock<GraphStore>>;

#[derive(Clone)]
struct AppState {
    store: StoreLock,
    graph: GraphLock,
    stats: GraphStats,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    ok: bool,
    stats: GraphStats,
}

pub async fn serve_sqlite(path: impl AsRef<std::path::Path>, addr: &str) -> Result<(), HttpError> {
    let addr: SocketAddr = addr.parse().map_err(HttpError::Addr)?;
    let store = SqliteGraphStore::open(path)?;
    let stats = store.stats()?;
    let graph = GraphStore::load(&store).unwrap_or_else(|_| GraphStore::empty());
    let state = AppState {
        store: Arc::new(std::sync::Mutex::new(store)),
        graph: Arc::new(std::sync::RwLock::new(graph)),
        stats,
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/graph", get(graph_ws))
        .with_state(state);

    let listener = TcpListener::bind(addr).await.map_err(HttpError::Bind)?;
    println!("Graph API server listening on http://{addr}");
    axum::serve(listener, app).await.map_err(HttpError::Serve)
}

async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        ok: true,
        stats: state.stats.clone(),
    })
}

async fn graph_ws(State(state): State<AppState>, ws: WebSocketUpgrade) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    let (mut sender, mut receiver) = socket.split();

    let hello = ServerMessage::Hello {
        stats: ServerStats {
            entities: state.stats.entities,
            edges: state.stats.edges,
            blob_bytes: state.stats.blob_bytes,
            supported_codecs: CodecSet::all(),
        },
    };
    if send(&mut sender, &hello).await.is_err() {
        return;
    }

    let query_id = AtomicU32::new(0);

    while let Some(message) = receiver.next().await {
        let Ok(message) = message else { break };
        let Message::Binary(bytes) = message else {
            let _ = send_error(&mut sender, 0, "expected_binary_rkyv_message").await;
            continue;
        };

        let control = match decode_client_message(&bytes) {
            Ok(c) => c,
            Err(e) => {
                let _ = send_error(&mut sender, 0, &format!("decode: {e:?}")).await;
                continue;
            }
        };

        let qid = query_id.fetch_add(1, Ordering::Relaxed);

        match control {
            ClientMessage::GetEntity(r) => dispatch_get_entity(&mut sender, &state, qid, r).await,
            ClientMessage::GetEdges(r) => dispatch_get_edges(&mut sender, &state, qid, r).await,
            ClientMessage::Traverse(r) => dispatch_traverse(&mut sender, &state, qid, r).await,
            ClientMessage::Search(q) => dispatch_search(&mut sender, &state, qid, q).await,
            ClientMessage::GetBlob(r) => dispatch_get_blob(&mut sender, &state, qid, r).await,
        }

        let _ = send(&mut sender, &ServerMessage::Done { query_id: qid }).await;
    }
}

async fn dispatch_get_entity(
    sender: &mut WsSender,
    state: &AppState,
    query_id: u32,
    req: GetEntity,
) {
    match extract_entity(state, &req) {
        Ok(data) => {
            let _ = send(
                sender,
                &ServerMessage::Entity(EntityData {
                    id: data.id,
                    properties: data.properties,
                    blob_refs: data.blob_refs,
                }),
            )
            .await;
        }
        Err(msg) => {
            let _ = send_error(sender, query_id, &msg).await;
        }
    }
}

struct EntityResponse {
    id: u64,
    properties: Vec<Property>,
    blob_refs: Vec<(String, BlobRef)>,
}

fn extract_entity(state: &AppState, req: &GetEntity) -> Result<EntityResponse, String> {
    let graph = state.graph.read().unwrap();
    let idx = graph
        .find_entity_index(req.id)
        .ok_or_else(|| format!("entity not found: {}", req.id))?;
    let sqlite_rowid = graph
        .entity_at(idx)
        .map(|e| e.sqlite_rowid)
        .ok_or_else(|| "entity slot empty".to_string())?;
    drop(graph);

    let store = state.store.lock().unwrap();
    let properties = if req.want_properties {
        store
            .load_properties(sqlite_rowid)
            .unwrap_or_default()
            .into_iter()
            .map(|p| {
                let key = p.key.clone();
                let value = row_to_value(&p);
                Property { key, value }
            })
            .collect()
    } else {
        Vec::new()
    };
    let blob_refs = if req.want_blobs {
        store
            .load_blob_refs(sqlite_rowid)
            .unwrap_or_default()
            .into_iter()
            .map(|b| (b.key, BlobRef::new(b.hash, b.size_bytes)))
            .collect()
    } else {
        Vec::new()
    };

    Ok(EntityResponse {
        id: sqlite_rowid,
        properties,
        blob_refs,
    })
}

async fn dispatch_get_edges(sender: &mut WsSender, state: &AppState, query_id: u32, req: GetEdges) {
    match extract_edges(state, &req) {
        Ok(list) => {
            for edge in &list {
                if send(sender, &ServerMessage::Edge(edge.clone()))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        }
        Err(msg) => {
            let _ = send_error(sender, query_id, &msg).await;
        }
    }
}

fn extract_edges(
    state: &AppState,
    req: &GetEdges,
) -> Result<Vec<waw_proto::EdgeData>, String> {
    let graph = state.graph.read().unwrap();
    let entity_idx = graph
        .find_entity_index(req.entity_id)
        .ok_or_else(|| format!("entity not found: {}", req.entity_id))?;
    let ent = graph
        .entity_at(entity_idx)
        .ok_or_else(|| "entity slot empty".to_string())?;
    let label_filter = req.label_filter.clone();
    let limit = req.limit as usize;
    let mut result = Vec::new();

    if matches!(req.direction, Direction::Outgoing | Direction::Both) {
        for &edge_idx in &ent.edges_out {
            if result.len() >= limit {
                break;
            }
            let Some(edge) = graph.edge_at(edge_idx) else {
                continue;
            };
            if !label_filter.is_empty() && !label_filter.contains(&edge.label) {
                continue;
            }
            let (src, tgt) = resolve_endpoints(&graph, edge);
            result.push(waw_proto::EdgeData {
                id: edge.sqlite_rowid,
                source: src,
                target: tgt,
                label: edge.label,
                properties: Vec::new(),
            });
        }
    }

    if matches!(req.direction, Direction::Incoming | Direction::Both)
        && result.len() < limit
    {
        for &edge_idx in &ent.edges_in {
            if result.len() >= limit {
                break;
            }
            let Some(edge) = graph.edge_at(edge_idx) else {
                continue;
            };
            if !label_filter.is_empty() && !label_filter.contains(&edge.label) {
                continue;
            }
            let (src, tgt) = resolve_endpoints(&graph, edge);
            result.push(waw_proto::EdgeData {
                id: edge.sqlite_rowid,
                source: src,
                target: tgt,
                label: edge.label,
                properties: Vec::new(),
            });
        }
    }

    Ok(result)
}

fn resolve_endpoints(graph: &GraphStore, edge: &crate::graph_store::GraphEdge) -> (u64, u64) {
    let source = graph
        .entity_at(edge.source_idx)
        .map(|e| e.sqlite_rowid)
        .unwrap_or(0);
    let target = graph
        .entity_at(edge.target_idx)
        .map(|e| e.sqlite_rowid)
        .unwrap_or(0);
    (source, target)
}

async fn dispatch_traverse(sender: &mut WsSender, state: &AppState, _query_id: u32, req: Traverse) {
    // Extract
    let visited: Vec<u64> = {
        let graph = state.graph.read().unwrap();
        graph.traverse_bfs(req.start_id, req.depth, &req.edge_labels)
    };

    // Send
    for id in visited.iter().take(req.limit as usize) {
        let msg = ServerMessage::Entity(EntityData {
            id: *id,
            properties: Vec::new(),
            blob_refs: Vec::new(),
        });
        if send(sender, &msg).await.is_err() {
            return;
        }
    }
}

async fn dispatch_search(
    sender: &mut WsSender,
    state: &AppState,
    query_id: u32,
    query: IndexQuery,
) {
    match query {
        IndexQuery::Spatial {
            min_x,
            min_y,
            max_x,
            max_y,
            limit,
        } => {
            let results: Vec<u64> = {
                let graph = state.graph.read().unwrap();
                let lod = graph
                    .spatial_index
                    .as_ref()
                    .map_or(4, |idx| idx.bits());
                graph.query_spatial(min_x as f32, min_y as f32, max_x as f32, max_y as f32, lod)
            };
            for id in results.iter().take(limit as usize) {
                let msg = ServerMessage::Entity(EntityData {
                    id: *id,
                    properties: Vec::new(),
                    blob_refs: Vec::new(),
                });
                if send(sender, &msg).await.is_err() {
                    return;
                }
            }
        }
        IndexQuery::Property {
            key,
            value: _,
            limit,
        }
        | IndexQuery::Range {
            key,
            min: _,
            max: _,
            limit,
        } => {
            let entity_ids: Result<Vec<u64>, _> = {
                let store = state.store.lock().unwrap();
                search_property(&store, &key, limit)
            };
            match entity_ids {
                Ok(ids) => {
                    for id in &ids {
                        let msg = ServerMessage::Entity(EntityData {
                            id: *id,
                            properties: Vec::new(),
                            blob_refs: Vec::new(),
                        });
                        if send(sender, &msg).await.is_err() {
                            return;
                        }
                    }
                }
                Err(e) => {
                    let _ = send_error(sender, query_id, &e.to_string()).await;
                }
            }
        }
    }
}

async fn dispatch_get_blob(sender: &mut WsSender, state: &AppState, query_id: u32, req: GetBlob) {
    // Phase 1: extract blob metadata (no await)
    let blob_info: Result<crate::sqlite_store::BlobRow, String> = {
        let store = state.store.lock().unwrap();
        match store.load_blob_by_hash(req.hash) {
            Ok(Some(info)) => Ok(info),
            Ok(None) => Err(format!("blob not found: {:x}", req.hash)),
            Err(e) => Err(e.to_string()),
        }
    };

    let info = match blob_info {
        Ok(i) => i,
        Err(msg) => {
            let _ = send_error(sender, query_id, &msg).await;
            return;
        }
    };

    let total = info.size_bytes;
    let mut offset = req.offset;

    loop {
        // Phase 2: extract chunk (no await)
        let chunk: Result<Option<Vec<u8>>, String> = {
            let store = state.store.lock().unwrap();
            store
                .load_blob_data(req.hash, offset, req.chunk_size)
                .map_err(|e| e.to_string())
        };

        let data = match chunk {
            Ok(Some(d)) => d,
            Ok(None) => break,
            Err(msg) => {
                let _ = send_error(sender, query_id, &msg).await;
                return;
            }
        };

        if data.is_empty() {
            break;
        }

        let chunk_len = data.len() as u64;
        let is_last = offset + chunk_len >= total;

        // Phase 3: send
        if send(
            sender,
            &ServerMessage::BlobChunk(waw_proto::BlobChunk {
                hash: req.hash,
                offset,
                total_size: total,
                data: data.into(),
            }),
        )
        .await
        .is_err()
        {
            return;
        }

        if is_last {
            break;
        }
        offset += chunk_len;
    }
}

fn row_to_value(row: &crate::sqlite_store::PropertyRow) -> Value {
    match PropertyType::from_u8(row.value_type) {
        Some(PropertyType::Int) => Value::Int(row.value_int.unwrap_or(0)),
        Some(PropertyType::Float) => Value::Float(row.value_float.unwrap_or(0.0)),
        Some(PropertyType::Text) => Value::Text(row.value_text.clone().unwrap_or_default()),
        Some(PropertyType::Bool) => Value::Bool(row.value_int.unwrap_or(0) != 0),
        Some(PropertyType::Blob) => {
            Value::Blob(BlobRef::new(row.value_int.unwrap_or(0) as u64, 0))
        }
        None => Value::Text(String::new()),
    }
}

fn search_property(
    store: &SqliteGraphStore,
    key: &str,
    limit: u32,
) -> Result<Vec<u64>, StoreError> {
    let mut stmt = store
        .connection
        .prepare("SELECT DISTINCT entity_id FROM property WHERE key = ?1 LIMIT ?2")?;
    stmt.query_map(rusqlite::params![key, limit], |row| {
        row.get::<_, i64>(0).map(|v| v as u64)
    })?
    .collect::<rusqlite::Result<Vec<_>>>()
    .map_err(Into::into)
}

async fn send(sender: &mut WsSender, message: &ServerMessage) -> Result<(), axum::Error> {
    match encode_server_message(message) {
        Ok(bytes) => sender
            .send(Message::Binary(bytes.into()))
            .await
            .map_err(axum::Error::new),
        Err(error) => sender
            .send(Message::Text(
                format!("protocol encode error: {error:?}").into(),
            ))
            .await
            .map_err(axum::Error::new),
    }
}

async fn send_error(sender: &mut WsSender, query_id: u32, msg: &str) -> Result<(), axum::Error> {
    send(
        sender,
        &ServerMessage::Error {
            query_id,
            message: msg.to_string(),
        },
    )
    .await
}
