use std::io::Error;
use std::sync::Arc;
use ahash::AHashMap;
use async_trait::async_trait;
use tokio::sync::Notify;
use pegasus_common::bytes::Bytes;
use pegasus_common::tag::Tag;
use pegasus_server::{Buf, Decode};

use crate::base::Decoder;
use crate::buffer::pool::{BufferPool, SharedScopedBufferPool, WoBatch};
use crate::data::{Data, MiniScopeBatch};
use crate::eos::Eos;

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
    binded: AHashMap<Tag, BufferPool<T>>,
    scoped_pool: Arc<SharedScopedBufferPool<T>>,
    notify: Arc<Notify>
}

impl<T> MultiScopeBatchDecoder<T> {
    pub fn new(scoped_pool: Arc<SharedScopedBufferPool<T>>) -> Self {
        Self {
            binded: AHashMap::new(),
            scoped_pool,
            notify: Arc::new(Notify::new())
        }
    }

    async fn alloc_slot(&mut self, tag: &Tag) -> WoBatch<T> {
        loop {
            let notify = Some(self.notify.clone());
            if let Some(mut pool) = self.scoped_pool.alloc_slot_async(tag, notify).await {
                let buf = pool.fetch().await;
                self.binded.insert(tag.clone(), pool);
                return buf;
            } else {
                self.notify.notified().await;
            }
        }
    }
}

#[async_trait]
impl <T> Decoder for MultiScopeBatchDecoder<T> where T: Decode + Data {
    type Item = MiniScopeBatch<T>;

    async fn decode(&mut self, mut bytes: Bytes) -> Result<Self::Item, Error> {
        let tag = Tag::read_from(&mut bytes)?;
        let mut buf = if let Some(pool) = self.binded.get_mut(&tag) {
            pool.fetch().await
        } else {
            self.alloc_slot(&tag).await
        };

        let src = bytes.get_u16();
        let mut eos = None;
        if bytes.get_u8() == 0 {
            eos = Some(Eos::read_from(&mut bytes)?);
        }

        let len = bytes.get_u32();
        for _ in 0..len {
            let item = T::read_from(&mut bytes)?;
            buf.push(item);
        }

        let data = buf.finalize();

        let mut batch = MiniScopeBatch::new(tag, src, data);
        if let Some(eos) = eos {
            batch.set_end(eos);
        }
        Ok(batch)
    }
}

