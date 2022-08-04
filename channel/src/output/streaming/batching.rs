use std::sync::Arc;

use ahash::AHashMap;
use pegasus_common::tag::Tag;

use crate::buffer::pool::{BufferPool, RoBatch, SharedScopedBufferPool, WoBatch};
use crate::buffer::{BoundedBuffer, BufferPtr, ScopeBuffer};
use crate::data::{Data, MiniScopeBatch};
use crate::eos::Eos;
use crate::error::PushError;
use crate::output::streaming::{Countable, Pinnable, Pushed, StreamPush};
use crate::{ChannelInfo, Push};

pub struct BufStreamPush<T, P> {
    #[allow(dead_code)]
    ch_info: ChannelInfo,
    worker_index: u16,
    total_send: usize,
    tag: Tag,
    buffer: BoundedBuffer<T>,
    batches: Vec<RoBatch<T>>,
    inner: P,
}

impl<T: Data, P> BufStreamPush<T, P> {
    pub fn new(ch_info: ChannelInfo, worker_index: u16, tag: Tag, push: P) -> Self {
        Self {
            ch_info,
            worker_index,
            total_send: 0,
            tag,
            buffer: BoundedBuffer::new(ch_info.batch_size, ch_info.batch_capacity),
            batches: vec![],
            inner: push,
        }
    }

    pub fn with_pool(
        ch_info: ChannelInfo, worker_index: u16, tag: Tag, pool: BufferPool<T>, push: P,
    ) -> Self {
        Self {
            ch_info,
            worker_index,
            total_send: 0,
            tag,
            buffer: BoundedBuffer::with_pool(pool),
            batches: vec![],
            inner: push,
        }
    }
}

impl<T, P> Pinnable for BufStreamPush<T, P>
where
    T: Data,
    P: Push<MiniScopeBatch<T>>,
{
    fn pin(&mut self, tag: &Tag) -> Result<bool, PushError> {
        Ok(*tag == self.tag)
    }

    fn unpin(&mut self) -> Result<(), PushError> {
        self.flush()
    }
}

impl<T, P> Countable for BufStreamPush<T, P> {
    fn count_pushed(&self, tag: &Tag) -> usize {
        assert_eq!(tag, &self.tag);
        let buf_cnt = self.buffer.len();
        self.total_send + buf_cnt
    }
}

impl<T, P> StreamPush<T> for BufStreamPush<T, P>
where
    T: Data,
    P: Push<MiniScopeBatch<T>>,
{
    fn push(&mut self, tag: &Tag, msg: T) -> Result<Pushed<T>, PushError> {
        assert_eq!(tag, &self.tag);
        match self.buffer.add(msg) {
            Ok(Some(batch)) => {
                let batch = MiniScopeBatch::new(tag.clone(), self.worker_index, batch);
                self.inner.push(batch)?;
                Ok(Pushed::Finished)
            }
            Ok(None) => Ok(Pushed::Finished),
            Err(msg) => Ok(Pushed::WouldBlock(Some(msg))),
        }
    }

    fn push_last(&mut self, msg: T, mut end: Eos) -> Result<(), PushError> {
        assert_eq!(end.tag, self.tag);
        let batch = self.buffer.add_last(msg);
        let mut batch = MiniScopeBatch::new(end.tag.clone(), self.worker_index, batch);
        self.total_send += batch.len();
        end.total_send = self.total_send as u64;
        batch.set_end(end);
        self.inner.push(batch)
    }

    fn push_iter<I: Iterator<Item = T>>(
        &mut self, tag: &Tag, iter: &mut I,
    ) -> Result<Pushed<T>, PushError> {
        assert_eq!(tag, &self.tag);
        let result = self.buffer.drain_to(iter, &mut self.batches);
        for batch in self.batches.drain(..) {
            let batch = MiniScopeBatch::new(tag.clone(), self.worker_index, batch);
            self.total_send += batch.len();
            self.inner.push(batch)?;
        }
        if let Err(_) = result {
            Ok(Pushed::WouldBlock(None))
        } else {
            Ok(Pushed::Finished)
        }
    }

    fn notify_end(&mut self, mut end: Eos) -> Result<(), PushError> {
        assert_eq!(end.tag, self.tag);
        let mut batch = if let Some(batch) = self.buffer.exhaust() {
            let batch = MiniScopeBatch::new(self.tag.clone(), self.worker_index, batch);
            self.total_send += batch.len();
            batch
        } else {
            MiniScopeBatch::new(self.tag.clone(), self.worker_index, RoBatch::default())
        };
        end.total_send = self.total_send as u64;
        batch.set_end(end);

        self.inner.push(batch)
    }

    fn flush(&mut self) -> Result<(), PushError> {
        if let Some(buf) = self.buffer.flush() {
            let batch = MiniScopeBatch::new(self.tag.clone(), self.worker_index, buf);
            self.total_send += batch.len();
            self.inner.push(batch)?;
            self.inner.flush()
        } else {
            Ok(())
        }
    }

    fn close(&mut self) -> Result<(), PushError> {
        self.inner.close()
    }
}

pub struct MultiScopeBufStreamPush<T, P> {
    #[allow(dead_code)]
    ch_info: ChannelInfo,
    worker_index: u16,
    pinned: Option<(Tag, BufferPtr<T>)>,
    batches: Vec<RoBatch<T>>,
    send_stat: AHashMap<Tag, usize>,
    scope_buffers: ScopeBuffer<T>,
    inner: P,
}

impl<T, P> MultiScopeBufStreamPush<T, P> {
    pub fn new(ch_info: ChannelInfo, worker_index: u16, scope_buf_slots: u16, inner: P) -> Self {
        Self {
            ch_info,
            worker_index,
            pinned: None,
            batches: vec![],
            send_stat: AHashMap::new(),
            scope_buffers: ScopeBuffer::new(ch_info.batch_size, ch_info.batch_capacity, scope_buf_slots),
            inner,
        }
    }

    pub fn with_pool(
        ch_info: ChannelInfo, worker_index: u16, pool: Arc<SharedScopedBufferPool<T>>, inner: P,
    ) -> Self {
        Self {
            ch_info,
            worker_index,
            pinned: None,
            batches: vec![],
            send_stat: AHashMap::new(),
            scope_buffers: ScopeBuffer::with_slot(pool),
            inner,
        }
    }
}

impl<T, P> Countable for MultiScopeBufStreamPush<T, P> {
    fn count_pushed(&self, tag: &Tag) -> usize {
        let mut cnt = self
            .send_stat
            .get(tag)
            .copied()
            .unwrap_or_default();

        if let Some((pin, buffer)) = self.pinned.as_ref() {
            if pin == tag {
                cnt += buffer.len();
            }
            return cnt;
        }

        if let Some(buffer) = self.scope_buffers.get_buffer(tag) {
            cnt += buffer.len();
        } else {
            // do nothing
        }
        cnt
    }
}

impl<T, P> MultiScopeBufStreamPush<T, P>
where
    T: Data,
    P: Push<MiniScopeBatch<T>>,
{
    fn get_or_create_buffer(&mut self, tag: &Tag) -> Result<Option<BufferPtr<T>>, PushError> {
        if let Some((pin, buffer)) = self.pinned.as_ref() {
            if pin == tag {
                return Ok(Some(buffer.clone()));
            }
        }
        self.flush_pin()?;
        if let Some(buffer) = self.scope_buffers.fetch_buffer(tag) {
            Ok(Some(buffer))
        } else {
            Ok(None)
        }
    }

    fn flush_pin(&mut self) -> Result<(), PushError> {
        if let Some((pin, mut buffer)) = self.pinned.take() {
            if let Some(buf) = buffer.flush() {
                let batch = MiniScopeBatch::new(pin.clone(), self.worker_index, buf);
                *self.send_stat.entry(pin).or_insert(0) += batch.len();
                self.inner.push(batch)?;
            }
        }
        Ok(())
    }
}

impl<T, P> Pinnable for MultiScopeBufStreamPush<T, P>
where
    T: Data,
    P: Push<MiniScopeBatch<T>>,
{
    fn pin(&mut self, tag: &Tag) -> Result<bool, PushError> {
        if let Some((pin, mut buffer)) = self.pinned.take() {
            if &pin == tag {
                self.pinned = Some((pin, buffer));
                return Ok(true);
            } else {
                if let Some(buf) = buffer.flush() {
                    let batch = MiniScopeBatch::new(pin.clone(), self.worker_index, buf);
                    *self.send_stat.entry(pin).or_insert(0) += batch.len();
                    self.inner.push(batch)?;
                }
            }
        }

        if let Some(buffer) = self.scope_buffers.fetch_buffer(tag) {
            self.pinned = Some((tag.clone(), buffer));
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn unpin(&mut self) -> Result<(), PushError> {
        if let Some((pin, mut buffer)) = self.pinned.take() {
            if let Some(buf) = buffer.flush() {
                let batch = MiniScopeBatch::new(pin.clone(), self.worker_index, buf);
                *self.send_stat.entry(pin).or_insert(0) += batch.len();
                self.inner.push(batch)?;
            }
        }
        Ok(())
    }
}

impl<T, P> StreamPush<T> for MultiScopeBufStreamPush<T, P>
where
    T: Data,
    P: Push<MiniScopeBatch<T>>,
{
    fn push(&mut self, tag: &Tag, msg: T) -> Result<Pushed<T>, PushError> {
        if let Some(mut buffer) = self.get_or_create_buffer(tag)? {
            match buffer.add(msg) {
                Ok(Some(buf)) => {
                    let batch = MiniScopeBatch::new(tag.clone(), self.worker_index, buf);
                    *self.send_stat.entry(tag.clone()).or_insert(0) += batch.len();
                    self.inner.push(batch)?;
                    Ok(Pushed::Finished)
                }
                Ok(None) => Ok(Pushed::Finished),
                Err(msg) => Ok(Pushed::WouldBlock(Some(msg))),
            }
        } else {
            Ok(Pushed::WouldBlock(Some(msg)))
        }
    }

    fn push_last(&mut self, msg: T, mut end: Eos) -> Result<(), PushError> {
        if let Some((pin, buffer)) = self.pinned.take() {
            if pin != end.tag {
                self.pinned = Some((pin, buffer));
            }
        }

        let mut buffer = self
            .scope_buffers
            .release(&end.tag)
            .unwrap_or_else(|| WoBatch::new(1));
        buffer.push(msg);
        let last = buffer.finalize();

        // if this tag is not pin, `self.pinned` should be none after flush;
        // if this tag is pin, `self.pinned` should be take to none as it is last;
        // self.pinned.take();

        let mut batch = MiniScopeBatch::new(end.tag.clone(), self.worker_index, last);
        let mut total_send = self.send_stat.remove(&end.tag).unwrap_or(0);
        total_send += batch.len();
        end.total_send = total_send as u64;
        batch.set_end(end);
        self.inner.push(batch)
    }

    fn push_iter<I: Iterator<Item = T>>(
        &mut self, tag: &Tag, iter: &mut I,
    ) -> Result<Pushed<T>, PushError> {
        if let Some(mut buffer) = self.get_or_create_buffer(tag)? {
            let result = buffer.drain_to(iter, &mut self.batches);
            if !self.batches.is_empty() {
                let cnt = self.send_stat.entry(tag.clone()).or_default();
                for b in self.batches.drain(..) {
                    let batch = MiniScopeBatch::new(tag.clone(), self.worker_index, b);
                    *cnt += batch.len();
                    self.inner.push(batch)?;
                }
            }
            if result.is_err() {
                Ok(Pushed::WouldBlock(None))
            } else {
                Ok(Pushed::Finished)
            }
        } else {
            Ok(Pushed::WouldBlock(None))
        }
    }

    fn notify_end(&mut self, mut end: Eos) -> Result<(), PushError> {
        if let Some((pin, buffer)) = self.pinned.take() {
            if pin != end.tag {
                self.pinned = Some((pin, buffer));
            }
        }
        let last = self
            .scope_buffers
            .release(&end.tag)
            .map(|b| b.finalize())
            .unwrap_or_else(RoBatch::default);

        let mut batch = MiniScopeBatch::new(end.tag.clone(), self.worker_index, last);
        let mut total_send = self
            .send_stat
            .remove(&end.tag)
            .unwrap_or_default();

        total_send += batch.len();
        end.total_send = total_send as u64;
        batch.set_end(end);
        self.inner.push(batch)
    }

    fn flush(&mut self) -> Result<(), PushError> {
        self.pinned.take();
        for (tag, buffer) in self.scope_buffers.get_all_mut() {
            if let Some(b) = buffer.flush() {
                let batch = MiniScopeBatch::new(tag.clone(), self.worker_index, b);
                self.inner.push(batch)?;
            }
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), PushError> {
        self.scope_buffers.destroy();
        self.inner.close()
    }
}
