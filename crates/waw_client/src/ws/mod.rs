mod client;
mod error;
mod message;
mod ws;

#[cfg(target_arch = "wasm32")]
mod wasm;

pub use client::WsClient;
pub use error::{Error, Result};
pub use message::Message as WsMessage;
