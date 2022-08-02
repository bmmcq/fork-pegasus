use async_trait::async_trait;
use bytes::Bytes;

#[async_trait]
pub trait Consumer {
    async fn consume(&mut self, msg: Bytes) -> Result<(), anyhow::Error>;

    async fn close(&mut self);
}
