extern crate alloc;

mod ws;

pub use waw_proto::ServerStats;
pub use waw_proto::{ViewportBounds, ViewportBudget, ViewportRequest};

#[derive(Clone, Debug, PartialEq)]
pub enum ClientEvent {
    Hello { stats: Option<ServerStats> },
    Tile(waw_proto::TileFrame),
    Done { request_id: u32 },
    Error { message: String },
}
