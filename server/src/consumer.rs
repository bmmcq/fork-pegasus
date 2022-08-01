use std::error::Error;

use async_trait::async_trait;
use bytes::Bytes;

#[async_trait]
pub trait Consumer {
    async fn consume(&mut self, msg: Bytes) -> Result<(), Box<dyn Error>>;

    async fn close(&mut self);
}
