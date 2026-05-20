use rkyv::{Archive, Deserialize, Serialize, rancor::Error as RkyvError};

use crate::types::{
    BlobChunk, BatchHeader, EdgeData, EntityData, GetBlob, GetEdges, GetEntity, IndexQuery,
    ServerStats, Traverse,
};

#[derive(Clone, Debug, PartialEq, Archive, Serialize, Deserialize)]
pub enum ClientMessage {
    GetEntity(GetEntity),
    GetEdges(GetEdges),
    Traverse(Traverse),
    Search(IndexQuery),
    GetBlob(GetBlob),
}

#[derive(Clone, Debug, PartialEq, Archive, Serialize, Deserialize)]
pub enum ServerMessage {
    Hello { stats: ServerStats },
    Entity(EntityData),
    Edge(EdgeData),
    Batch(BatchHeader),
    BlobChunk(BlobChunk),
    Done { query_id: u32 },
    Error { query_id: u32, message: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WireError {
    Encode(String),
    Decode(String),
}

pub fn encode_client_message(message: &ClientMessage) -> Result<Vec<u8>, WireError> {
    encode(message)
}

pub fn decode_client_message(bytes: &[u8]) -> Result<ClientMessage, WireError> {
    decode(bytes)
}

pub fn encode_server_message(message: &ServerMessage) -> Result<Vec<u8>, WireError> {
    encode(message)
}

pub fn decode_server_message(bytes: &[u8]) -> Result<ServerMessage, WireError> {
    decode(bytes)
}

fn encode<T>(message: &T) -> Result<Vec<u8>, WireError>
where
    T: for<'a> Serialize<
        rkyv::api::high::HighSerializer<
            rkyv::util::AlignedVec,
            rkyv::ser::allocator::ArenaHandle<'a>,
            RkyvError,
        >,
    >,
{
    rkyv::to_bytes::<RkyvError>(message)
        .map(|bytes| bytes.to_vec())
        .map_err(|error| WireError::Encode(error.to_string()))
}

fn decode<T>(bytes: &[u8]) -> Result<T, WireError>
where
    T: Archive,
    T::Archived: for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, RkyvError>>
        + Deserialize<T, rkyv::api::high::HighDeserializer<RkyvError>>,
{
    rkyv::from_bytes::<T, RkyvError>(bytes).map_err(|error| WireError::Decode(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{
        BlobRef, Property, Traverse as TraverseReq, Value,
    };

    #[test]
    fn round_trips_get_entity() {
        let msg = ClientMessage::GetEntity(GetEntity {
            id: 42,
            want_properties: true,
            want_blobs: false,
        });
        let bytes = encode_client_message(&msg).unwrap();
        let decoded = decode_client_message(&bytes).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn round_trips_traverse() {
        let msg = ClientMessage::Traverse(TraverseReq {
            start_id: 1,
            depth: 3,
            edge_labels: vec![1, 2],
            limit: 100,
        });
        let bytes = encode_client_message(&msg).unwrap();
        let decoded = decode_client_message(&bytes).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn round_trips_entity_response() {
        let msg = ServerMessage::Entity(EntityData {
            id: 7,
            properties: vec![
                Property {
                    key: "name".into(),
                    value: Value::Text("test".into()),
                },
                Property {
                    key: "count".into(),
                    value: Value::Int(42),
                },
            ],
            blob_refs: vec![("data".into(), BlobRef::new(0xABCD, 1024))],
        });
        let bytes = encode_server_message(&msg).unwrap();
        let decoded = decode_server_message(&bytes).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn round_trips_get_edges() {
        let msg = ClientMessage::GetEdges(GetEdges {
            entity_id: 1,
            direction: crate::types::Direction::Outgoing,
            label_filter: vec![1, 2],
            limit: 50,
        });
        let bytes = encode_client_message(&msg).unwrap();
        let decoded = decode_client_message(&bytes).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn round_trips_spatial_search() {
        let msg = ClientMessage::Search(IndexQuery::Spatial {
            min_x: -1.0,
            min_y: -0.5,
            max_x: 1.0,
            max_y: 0.5,
            limit: 64,
        });
        let bytes = encode_client_message(&msg).unwrap();
        let decoded = decode_client_message(&bytes).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn round_trips_property_search() {
        let msg = ClientMessage::Search(IndexQuery::Property {
            key: "name".into(),
            value: Value::Text("test".into()),
            limit: 10,
        });
        let bytes = encode_client_message(&msg).unwrap();
        let decoded = decode_client_message(&bytes).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn round_trips_blob_chunk() {
        let msg = ServerMessage::BlobChunk(BlobChunk {
            hash: 0xDEAD,
            offset: 0,
            total_size: 4096,
            data: bytes::Bytes::from_static(&[1, 2, 3, 4]),
        });
        let bytes = encode_server_message(&msg).unwrap();
        let decoded = decode_server_message(&bytes).unwrap();
        assert_eq!(decoded, msg);
    }
}
