use std::cell::RefCell;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use ahash::AHashMap;
use pegasus_common::rc::UnsafeRcPtr;
use crate::communication::buffer::{Buffer, BufferPtr, ScopeBuffer, WouldBlock};
use crate::communication::decorator::ScopeStreamPush;
use crate::communication::IOResult;
use crate::data::batching::{RoBatch, WoBatch};
use crate::data::MicroBatch;
use crate::data_plane::intra_thread::ThreadPush;
use crate::data_plane::Push;
use crate::errors::IOError;
use crate::graph::Port;
use crate::progress::Eos;
use crate::Tag;
use crate::Tag::Root;

pub struct BatchProducer<T, P> {
    ch_index: u32,
    worker_index: u32,
    port: Port,
    total_send: usize,
    buffer: Buffer<T>,
    batches: Vec<RoBatch<T>>,
    inner: P,
}

impl <T, P> BatchProducer<T, P> {
    pub fn new(ch_index: u32, worker_index: u32, port: Port, batch_size: usize, batch_capacity: usize, push: P) -> Self {
        Self {
            ch_index,
            worker_index,
            port,
            total_send: 0,
            buffer: Buffer::new(batch_size, batch_capacity),
            batches: vec![],
            inner: push
        }
    }

    pub fn count_send(&self) -> usize {
        let buf_cnt = self.buffer.len();
        self.total_send + buf_cnt
    }
}

impl<T, P> BatchProducer<T, P> where P: Push<MicroBatch<T>> {
    pub fn push_batch(&mut self, tag: Tag, batch: RoBatch<T>) -> IOResult<()> {
        assert!(tag.is_root());
        self.total_send += batch.len();
        self.inner.push(MicroBatch::new(tag, self.worker_index, batch))
    }
}

impl <T, P> ScopeStreamPush<T> for BatchProducer<T, P> where P: Push<MicroBatch<T>> {
    fn port(&self) -> Port {
        todo!()
    }

    fn push(&mut self, tag: &Tag, msg: T) -> IOResult<Option<T>> {
        assert!(tag.is_root());
        match self.buffer.add(msg) {
            Ok(Some(batch)) => {
                let batch = MicroBatch::new(Root, self.worker_index, batch);
                self.inner.push(batch)?;
                Ok(None)
            }
            Ok(None) => Ok(None),
            Err(msg) => {
                Ok(Some(msg))
            },
        }
    }

    fn push_last(&mut self, msg: T, mut end: Eos) -> IOResult<()> {
        assert!(end.tag.is_root());
        let batch = self.buffer.add_last(msg);
        let mut batch = MicroBatch::new(Root, self.worker_index, batch);
        self.total_send += batch.len();
        end.total_send = self.total_send as u64;
        batch.set_end(end);
        self.inner.push(batch)
    }

    fn try_push_iter<I: Iterator<Item=T>>(&mut self, tag: &Tag, iter: &mut I) -> IOResult<()> {
        assert!(tag.is_root());
        let result = self.buffer.drain_to(iter, &mut self.batches);
        for batch in self.batches.drain(..) {
            let batch = MicroBatch::new(Root, self.worker_index, batch);
            self.total_send += batch.len();
            self.inner.push(batch)?;
        }
        if let Err(_) = result {
            Err(IOError::would_block())
        } else {
            Ok(())
        }
    }

    fn notify_end(&mut self, mut end: Eos) -> IOResult<()> {
        assert!(end.tag.is_root());
        let mut batch = if let Some(batch) = self.buffer.exhaust() {
            let mut batch = MicroBatch::new(Root, self.worker_index, batch);
            self.total_send += batch.len();
            batch
        } else {
            MicroBatch::new(Root, self.worker_index, RoBatch::default())
        };
        end.total_send = self.total_send as u64;
        batch.set_end(end);

        self.inner.push(batch)
    }

    fn flush(&mut self) -> IOResult<()> {
        if let Some(buf) = self.buffer.flush() {
            let batch = MicroBatch::new(Root, self.worker_index, buf);
            self.total_send += batch.len();
            self.inner.push(batch)?;
            self.inner.flush()
        } else {
            Ok(())
        }
    }

    fn close(&mut self) -> IOResult<()> {
        self.inner.close()
    }
}

pub struct MultiScopeBatchProducer<T, P> {
    ch_index: u32,
    worker_index: u32,
    scope_level: u32,
    port: Port,
    pinned: Option<(Tag, BufferPtr<T>)>,
    batches: Vec<RoBatch<T>>,
    send_stat: AHashMap<Tag, usize>,
    scope_buffers: ScopeBuffer<T>,
    inner: P
}

impl <T, P> MultiScopeBatchProducer<T, P> {

    pub(crate) fn count_send(&self, tag: &Tag) -> usize {
        let mut cnt = self.send_stat.get(tag).copied().unwrap_or_default();
        if let Some((pin, buffer)) = self.pinned.as_ref() {
            cnt += buffer.len();
        } else if let Some(buffer) = self.scope_buffers.get_buffer(tag) {
            cnt += buffer.len();
        } else {
            // do nothing
        }
        cnt
    }

}


impl <T, P> MultiScopeBatchProducer<T, P> where P: Push<MicroBatch<T>> {

    fn get_or_create_buffer(&mut self, tag: &Tag) -> Result<Option<BufferPtr<T>>, IOError> {
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

    pub(crate) fn pin(&mut self, tag: &Tag) -> IOResult<bool> {
        if let Some((pin, mut buffer)) = self.pinned.take() {
            if &pin == tag {
                self.pinned = Some((pin, buffer));
                return Ok(true);
            } else {
                if let Some(buf) = buffer.flush() {
                    let batch = MicroBatch::new(pin.clone(), self.worker_index, buf);
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

    pub fn push_batch(&mut self, tag: Tag, batch: RoBatch<T>) -> IOResult<()> {
        if let Some((pin, buffer)) = self.pinned.as_mut() {
            if pin == &tag {
                if let Some(b) = buffer.flush() {
                    let buf = MicroBatch::new(pin.clone(), self.worker_index, b);
                    *self.send_stat.entry(pin.clone()).or_insert(0) += buf.len();
                    self.inner.push(buf)?;
                }
            }
        } else {
            if let Some(buffer) = self.scope_buffers.get_buffer_mut(&tag) {
                if let Some(b) = buffer.flush() {
                    let buf = MicroBatch::new(tag.clone(), self.worker_index, b);
                    *self.send_stat.entry(tag.clone()).or_insert(0) += buf.len();
                    self.inner.push(buf)?;
                }
            }
        }

        *self.send_stat.entry(tag.clone()).or_insert(0) += batch.len();
        self.inner.push(MicroBatch::new(tag, self.worker_index, batch))
    }

    fn flush_pin(&mut self) -> Result<(), IOError>{
        if let Some((pin, mut buffer)) = self.pinned.take() {
            if let Some(buf) = buffer.flush() {
                let batch = MicroBatch::new(pin.clone(), self.worker_index, buf);
                *self.send_stat.entry(pin).or_insert(0) += batch.len();
                self.inner.push(batch)?;
            }
        }
        Ok(())
    }
}

impl <T, P> ScopeStreamPush<T> for MultiScopeBatchProducer<T, P> where P: Push<MicroBatch<T>>{
    fn port(&self) -> Port {
        todo!()
    }

    fn push(&mut self, tag: &Tag, msg: T) -> IOResult<Option<T>> {
        if let Some(mut buffer) = self.get_or_create_buffer(tag)? {
            match buffer.add(msg) {
                Ok(Some(buf)) => {
                    let batch = MicroBatch::new(tag.clone(), self.worker_index, buf);
                    *self.send_stat.entry(tag.clone()).or_insert(0) += batch.len();
                    self.inner.push(batch)?;
                    Ok(None)
                }
                Ok(None) => Ok(None),
                Err(msg) => Ok(Some(msg))
            }
        } else {
            Ok(Some(msg))
        }
    }

    fn push_last(&mut self, msg: T, mut end: Eos) -> IOResult<()> {

        let last = if let Some(mut buffer) = self.get_or_create_buffer(&end.tag)? {
            buffer.add_last(msg)
        } else {
            let mut last = WoBatch::new(1);
            last.push(msg);
            last.finalize()
        };
        // if this tag is not pin, `self.pinned` should be none after flush;
        // if this tag is pin, `self.pinned` should be take to none as it is last;
        self.pinned.take();

        let mut batch = MicroBatch::new(end.tag.clone(), self.worker_index, last);
        let mut total_send = self.send_stat.remove(&end.tag).unwrap_or(0) ;
        total_send += batch.len();
        end.total_send = total_send as u64;
        batch.set_end(end);
        self.inner.push(batch)
    }

    fn try_push_iter<I: Iterator<Item=T>>(&mut self, tag: &Tag, iter: &mut I) -> IOResult<()> {

        if let Some(mut buffer) = self.get_or_create_buffer(tag)? {
            let result = buffer.drain_to(iter, &mut self.batches);
            if !self.batches.is_empty() {
                let cnt = self.send_stat.entry(tag.clone()).or_default();
                for b in self.batches.drain(..) {
                    let batch = MicroBatch::new(tag.clone(), self.worker_index, b);
                    *cnt += batch.len();
                    self.inner.push(batch)?;
                }
            }
            if result.is_err() {
                Err(IOError::would_block())
            } else {
                Ok(())
            }
        } else {
            Err(IOError::would_block())
        }
    }

    fn notify_end(&mut self, mut end: Eos) -> IOResult<()> {
        let mut last = RoBatch::default();
        if let Some((pin, mut buffer)) = self.pinned.take() {
            if pin == end.tag {
                if let Some(b) = buffer.exhaust() {
                    last = b;
                }
            } else {
                self.pinned = Some((pin, buffer));
            }
        } else {
            if let Some(mut buffer) = self.scope_buffers.fetch_buffer(&end.tag) {
                if let Some(b) = buffer.exhaust() {
                    last = b;
                }
            }
        }
        let mut batch = MicroBatch::new(end.tag.clone(), self.worker_index, last);
        let mut total_send = self.send_stat.remove(&end.tag).unwrap_or_default();
        total_send += batch.len();
        end.total_send = total_send as u64;
        batch.set_end(end);
        self.inner.push(batch)
    }

    fn flush(&mut self) -> IOResult<()> {
        self.pinned.take();
        for (tag, buffer) in self.scope_buffers.get_all_mut() {
            if let Some(b) = buffer.flush() {
                let batch = MicroBatch::new(tag.clone(), self.worker_index, b);
                self.inner.push(batch)?;
            }
        }
        Ok(())
    }

    fn close(&mut self) -> IOResult<()> {
        self.inner.close()
    }
}

mod exchange;
mod aggregate;