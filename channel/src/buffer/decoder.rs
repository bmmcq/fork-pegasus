use ahash::AHashMap;
use async_trait::async_trait;
use pegasus_common::bytes::Bytes;
use pegasus_common::tag::Tag;
use pegasus_server::Decode;

use crate::base::Decoder;
use crate::buffer::batch::BufferPool;
use crate::data::{Data, MiniScopeBatch};

pub struct BatchDecoder<T> {
    pool: BufferPool<T>,
}

impl<T> BatchDecoder<T> {
    pub fn new(pool: BufferPool<T>) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl<T> Decoder for BatchDecoder<T>
where
    T: Decode + Data,
{
    type Item = MiniScopeBatch<T>;

    async fn decode(&mut self, bytes: Bytes) -> Result<Self::Item, std::io::Error> {
        let batch = self.pool.fetch().await;
        let mut reader = bytes.as_ref();
        let b =  MiniScopeBatch::read_with(batch, &mut reader)?;
        Ok(b)
    }
}

pub struct MultiScopeBatchDecoder<T> {
    #[allow(dead_code)]
    pool: AHashMap<Tag, BufferPool<T>>,
}

impl<T> MultiScopeBatchDecoder<T> {
    pub fn new(pool: AHashMap<Tag, BufferPool<T>>) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl<T> Decoder for MultiScopeBatchDecoder<T>
where
    T: Decode + Data,
{
    type Item = MiniScopeBatch<T>;

    async fn decode(&mut self, _bytes: Bytes) -> Result<Self::Item, std::io::Error> {
        todo!()
    }
}
