use core::time::Duration;

use url::Url;

use waw_proto::{
    decode_server_message, encode_client_message, BlobChunk, ClientMessage, Direction, EdgeData,
    EntityData, GetBlob, GetEdges, GetEntity, IndexQuery, ServerMessage, ServerStats, Traverse,
};

use crate::ws::{self, WsClient, WsMessage};

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("WebSocket error: {0}")]
    Ws(#[from] ws::Error),
    #[error("Protocol error: {0}")]
    Protocol(String),
    #[error("Server error: {0}")]
    Server(String),
}

pub struct GraphClient {
    ws: WsClient,
    next_query_id: u32,
    stats: Option<ServerStats>,
}

impl GraphClient {
    pub fn new(timeout: Duration) -> Self {
        Self {
            ws: WsClient::new(timeout),
            next_query_id: 0,
            stats: None,
        }
    }

    pub async fn connect(&mut self, url: Url) -> Result<(), ClientError> {
        self.ws.connect(url).await?;

        match self.receive_server_message().await? {
            ServerMessage::Hello { stats } => {
                self.stats = Some(stats);
                Ok(())
            }
            other => Err(ClientError::Protocol(format!(
                "expected Hello, got {other:?}"
            ))),
        }
    }

    pub async fn disconnect(&mut self) -> Result<(), ClientError> {
        Ok(self.ws.disconnect().await?)
    }

    pub fn stats(&self) -> Option<&ServerStats> {
        self.stats.as_ref()
    }

    pub fn is_connected(&self) -> bool {
        self.ws.is_connected()
    }

    async fn receive_server_message(&mut self) -> Result<ServerMessage, ClientError> {
        loop {
            match self.ws.receive_message().await? {
                Some(WsMessage::Binary(data)) => {
                    return decode_server_message(&data)
                        .map_err(|e| ClientError::Protocol(format!("decode error: {e:?}")));
                }
                Some(WsMessage::Close(_)) | None => {
                    return Err(ClientError::Ws(ws::Error::NotConnected));
                }
                _ => continue,
            }
        }
    }

    async fn send_request(&mut self, msg: ClientMessage) -> Result<u32, ClientError> {
        let query_id = self.next_query_id;
        self.next_query_id = self.next_query_id.wrapping_add(1);

        let bytes = encode_client_message(&msg)
            .map_err(|e| ClientError::Protocol(format!("encode error: {e:?}")))?;

        self.ws
            .send_message(WsMessage::Binary(bytes.into()))
            .await?;
        Ok(query_id)
    }

    async fn collect_results<T>(
        &mut self,
        query_id: u32,
        mut on_message: impl FnMut(ServerMessage) -> Option<T>,
    ) -> Result<Vec<T>, ClientError> {
        let mut results = Vec::new();
        loop {
            let msg = self.receive_server_message().await?;
            match msg {
                ServerMessage::Done { query_id: done_id } if done_id == query_id => {
                    return Ok(results);
                }
                ServerMessage::Error {
                    query_id: err_id,
                    message,
                } if err_id == query_id => {
                    return Err(ClientError::Server(message));
                }
                ServerMessage::EntityBatch(batch) => {
                    for entity in batch.entities {
                        if let Some(item) = on_message(ServerMessage::Entity(entity)) {
                            results.push(item);
                        }
                    }
                }
                ServerMessage::Done { .. }
                | ServerMessage::Error { .. }
                | ServerMessage::Hello { .. }
                | ServerMessage::Batch(_) => continue,
                other => {
                    if let Some(item) = on_message(other) {
                        results.push(item);
                    }
                }
            }
        }
    }

    pub async fn get_entity(
        &mut self,
        id: u64,
        want_properties: bool,
        want_blobs: bool,
    ) -> Result<EntityData, ClientError> {
        let qid = self
            .send_request(ClientMessage::GetEntity(GetEntity {
                id,
                want_properties,
                want_blobs,
            }))
            .await?;

        let mut results = self
            .collect_results(qid, |msg| match msg {
                ServerMessage::Entity(e) => Some(e),
                _ => None,
            })
            .await?;

        results
            .pop()
            .ok_or_else(|| ClientError::Protocol("no entity returned".into()))
    }

    pub async fn get_edges(
        &mut self,
        entity_id: u64,
        direction: Direction,
        label_filter: Vec<u32>,
        limit: u32,
    ) -> Result<Vec<EdgeData>, ClientError> {
        let qid = self
            .send_request(ClientMessage::GetEdges(GetEdges {
                entity_id,
                direction,
                label_filter,
                limit,
            }))
            .await?;

        self.collect_results(qid, |msg| match msg {
            ServerMessage::Edge(e) => Some(e),
            _ => None,
        })
        .await
    }

    pub async fn traverse(
        &mut self,
        start_id: u64,
        depth: u32,
        edge_labels: Vec<u32>,
        limit: u32,
    ) -> Result<Vec<EntityData>, ClientError> {
        let qid = self
            .send_request(ClientMessage::Traverse(Traverse {
                start_id,
                depth,
                edge_labels,
                limit,
            }))
            .await?;

        self.collect_results(qid, |msg| match msg {
            ServerMessage::Entity(e) => Some(e),
            _ => None,
        })
        .await
    }

    pub async fn search(&mut self, query: IndexQuery) -> Result<Vec<EntityData>, ClientError> {
        let qid = self.send_request(ClientMessage::Search(query)).await?;

        self.collect_results(qid, |msg| match msg {
            ServerMessage::Entity(e) => Some(e),
            _ => None,
        })
        .await
    }

    pub async fn get_blob_chunk(
        &mut self,
        hash: u64,
        offset: u64,
        chunk_size: u32,
    ) -> Result<BlobChunk, ClientError> {
        let qid = self
            .send_request(ClientMessage::GetBlob(GetBlob {
                hash,
                offset,
                chunk_size,
            }))
            .await?;

        let mut results = self
            .collect_results(qid, |msg| match msg {
                ServerMessage::BlobChunk(c) => Some(c),
                _ => None,
            })
            .await?;

        results
            .pop()
            .ok_or_else(|| ClientError::Protocol("no blob chunk returned".into()))
    }

    pub async fn get_blob(&mut self, hash: u64) -> Result<Vec<u8>, ClientError> {
        const CHUNK_SIZE: u32 = 65536;

        let first = self.get_blob_chunk(hash, 0, CHUNK_SIZE).await?;
        let total = first.total_size as usize;
        let mut data = Vec::with_capacity(total);
        data.extend_from_slice(&first.data);

        while (data.len() as u64) < first.total_size {
            let chunk = self
                .get_blob_chunk(hash, data.len() as u64, CHUNK_SIZE)
                .await?;
            data.extend_from_slice(&chunk.data);
        }

        Ok(data)
    }
}
