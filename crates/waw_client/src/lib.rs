extern crate alloc;

mod graph_client;
pub mod ws;

pub use graph_client::{ClientError, GraphClient};
pub use waw_proto::{
    BlobChunk, BlobRef, Direction, EdgeData, EntityData, GetBlob, GetEdges, GetEntity, IndexQuery,
    Property, ServerStats, Traverse, Value,
};
