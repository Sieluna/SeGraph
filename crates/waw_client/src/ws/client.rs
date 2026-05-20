use core::time::Duration;

use futures_util::{SinkExt, StreamExt};
use url::Url;

use super::error::{Error, Result};
use super::{message::Message, ws::WebSocket};

pub struct WsClient {
    socket: Option<WebSocket>,
    timeout: Duration,
}

impl WsClient {
    pub fn new(timeout: Duration) -> Self {
        Self {
            socket: None,
            timeout,
        }
    }

    pub async fn connect(&mut self, url: Url) -> Result<()> {
        let socket = WebSocket::connect(&url, self.timeout).await?;
        self.socket = Some(socket);
        Ok(())
    }

    pub async fn disconnect(&mut self) -> Result<()> {
        if let Some(mut socket) = self.socket.take() {
            socket.close().await?;
        }
        Ok(())
    }

    pub async fn send_message(&mut self, msg: Message) -> Result<()> {
        let socket = self.socket.as_mut().ok_or(Error::NotConnected)?;
        socket.send(msg).await?;
        Ok(())
    }

    pub async fn receive_message(&mut self) -> Result<Option<Message>> {
        let socket = self.socket.as_mut().ok_or(Error::NotConnected)?;
        match socket.next().await {
            Some(Ok(msg)) => Ok(Some(msg)),
            Some(Err(_)) => Err(Error::NotConnected),
            None => Ok(None),
        }
    }

    pub fn is_connected(&self) -> bool {
        self.socket.is_some()
    }
}
