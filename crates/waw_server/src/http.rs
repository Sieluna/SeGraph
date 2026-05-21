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
    BlobRef, ClientMessage, CodecSet, EntityData, GetBlob, GetEdges, GetEntity,
    IndexQuery, Property, PropertyType, ServerMessage, ServerStats, Traverse, Value,
    decode_client_message, encode_server_message,
};

use crate::cold_tier::{GraphStats, StoreError};
use crate::pipeline::{Pipeline, PipelineConfig};

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

#[derive(Clone)]
struct AppState {
    pipeline: Arc<Pipeline>,
    stats: GraphStats,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    ok: bool,
    stats: GraphStats,
}

pub async fn serve_sqlite(
    db_path: impl AsRef<std::path::Path>,
    warm_cache_path: Option<impl AsRef<std::path::Path>>,
    addr: &str,
) -> Result<(), HttpError> {
    let addr: SocketAddr = addr.parse().map_err(HttpError::Addr)?;
    let pipeline = Pipeline::load(db_path, warm_cache_path, PipelineConfig::default())?;
    let stats = pipeline.stats()?;
    let state = AppState {
        pipeline: Arc::new(pipeline),
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
    let pipeline = &state.pipeline;

    // Verify entity exists in the CSR index
    pipeline
        .find_entity_index(req.id)
        .ok_or_else(|| format!("entity not found: {}", req.id))?;

    // Promote entity to hot tier if needed
    pipeline
        .get_entity(req.id)
        .map_err(|e| format!("load entity: {e}"))?
        .ok_or_else(|| format!("entity slot empty: {}", req.id))?;

    let properties = if req.want_properties {
        pipeline
            .load_properties(req.id)
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
        pipeline
            .load_blob_refs(req.id)
            .unwrap_or_default()
            .into_iter()
            .map(|b| (b.key, BlobRef::new(b.hash, b.size_bytes)))
            .collect()
    } else {
        Vec::new()
    };

    Ok(EntityResponse {
        id: req.id,
        properties,
        blob_refs,
    })
}

async fn dispatch_get_edges(
    sender: &mut WsSender,
    state: &AppState,
    query_id: u32,
    req: GetEdges,
) {
    let list = state.pipeline.get_edges(
        req.entity_id,
        req.direction,
        &req.label_filter,
        req.limit,
    );
    for edge in &list {
        if send(sender, &ServerMessage::Edge(edge.clone())).await.is_err() {
            return;
        }
    }

    // If no edges found, check the entity exists
    if list.is_empty() {
        if state.pipeline.find_entity_index(req.entity_id).is_none() {
            let _ =
                send_error(sender, query_id, &format!("entity not found: {}", req.entity_id))
                    .await;
        }
    }
}

async fn dispatch_traverse(
    sender: &mut WsSender,
    state: &AppState,
    _query_id: u32,
    req: Traverse,
) {
    let visited = state.pipeline.traverse_bfs(
        req.start_id,
        req.depth,
        &req.edge_labels,
        req.limit,
    );

    for id in &visited {
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
            let lod = state.pipeline.spatial_lod();
            let results = state.pipeline.search_spatial(
                min_x as f32,
                min_y as f32,
                max_x as f32,
                max_y as f32,
                lod,
                limit,
            );
            for id in &results {
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
            match state.pipeline.search_property(&key, limit) {
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

async fn dispatch_get_blob(
    sender: &mut WsSender,
    state: &AppState,
    query_id: u32,
    req: GetBlob,
) {
    let blob_info = match state.pipeline.load_blob_by_hash(req.hash) {
        Ok(Some(info)) => info,
        Ok(None) => {
            let _ = send_error(sender, query_id, &format!("blob not found: {:x}", req.hash)).await;
            return;
        }
        Err(e) => {
            let _ = send_error(sender, query_id, &e.to_string()).await;
            return;
        }
    };

    let total = blob_info.size_bytes;
    let mut offset = req.offset;

    loop {
        let chunk = match state.pipeline.load_blob_chunk(req.hash, offset, req.chunk_size) {
            Ok(Some(d)) => d,
            Ok(None) => break,
            Err(msg) => {
                let _ = send_error(sender, query_id, &msg.to_string()).await;
                return;
            }
        };

        if chunk.is_empty() {
            break;
        }

        let chunk_len = chunk.len() as u64;
        let is_last = offset + chunk_len >= total;

        if send(
            sender,
            &ServerMessage::BlobChunk(waw_proto::BlobChunk {
                hash: req.hash,
                offset,
                total_size: total,
                data: chunk.into(),
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

fn row_to_value(row: &crate::cold_tier::PropertyRow) -> Value {
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
