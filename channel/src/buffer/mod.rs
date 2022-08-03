use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;
use std::sync::Arc;

use ahash::AHashMap;
use pegasus_common::tag::Tag;
use crate::buffer::pool::{LocalScopedBufferPool, ScopedBufferPool, SharedScopedBufferPool};

pub mod pool;
pub mod decoder;
use self::pool::{BufferPool, RoBatch, WoBatch};

pub struct WouldBlock;

pub struct BoundedBuffer<D> {
    exhaust: bool,
    buffer: Option<WoBatch<D>>,
    pool: BufferPool<D>,
}

impl<D> BoundedBuffer<D> {
    pub fn new(batch_size: u16, batch_capacity: u16) -> Self {
        let pool = BufferPool::new(batch_size, batch_capacity);
        BoundedBuffer { exhaust: false, buffer: None, pool }
    }

    pub fn with_pool(pool: BufferPool<D>) -> Self {
        BoundedBuffer { exhaust: false, buffer: None, pool }
    }

    pub fn add(&mut self, entry: D) -> Result<Option<RoBatch<D>>, D> {
        assert!(!self.exhaust, "still push after set exhaust");

        if self.buffer.is_none() {
            if let Some(buf) = self.pool.try_fetch() {
                self.buffer = Some(buf);
            } else {
                return Err(entry);
            }
        }

        if let Some(mut buf) = self.buffer.take() {
            buf.push(entry).expect("buf full unexpected;");
            if buf.is_full() {
                return Ok(Some(buf.finalize()));
            }
            self.buffer = Some(buf);
            Ok(None)
        } else {
            unreachable!("")
        }
    }

    pub fn add_last(&mut self, entry: D) -> RoBatch<D> {
        assert!(!self.exhaust, "still push after set exhaust");
        self.exhaust = true;
        let mut buf = if let Some(buf) = self.buffer.take() {
            buf
        } else {
            self.pool
                .try_fetch()
                .unwrap_or_else(|| WoBatch::new(1))
        };
        buf.push(entry).expect("unexpected full;");
        buf.finalize()
    }

    pub fn drain_to<T>(&mut self, iter: &mut T, target: &mut Vec<RoBatch<D>>) -> Result<(), WouldBlock>
    where
        T: Iterator<Item = D>,
    {
        if self.buffer.is_none() {
            if let Some(buf) = self.pool.try_fetch() {
                self.buffer = Some(buf);
            } else {
                return Err(WouldBlock);
            }
        }

        if let Some(mut buf) = self.buffer.take() {
            while let Some(next) = iter.next() {
                buf.push(next).expect("");
                if buf.is_full() {
                    if let Some(new_buf) = self.pool.try_fetch() {
                        let full = std::mem::replace(&mut buf, new_buf);
                        target.push(full.finalize());
                    } else {
                        target.push(buf.finalize());
                        return Err(WouldBlock);
                    }
                }
            }
            self.buffer = Some(buf);
            Ok(())
        } else {
            unreachable!("buffer is checked not none;")
        }
    }

    pub fn exhaust(&mut self) -> Option<RoBatch<D>> {
        self.exhaust = true;
        self.flush()
    }

    pub fn flush(&mut self) -> Option<RoBatch<D>> {
        if let Some(buf) = self.buffer.take() {
            if !buf.is_empty() {
                return Some(buf.finalize());
            }
        }
        None
    }

    pub fn len(&self) -> usize {
        self.buffer
            .as_ref()
            .map(|b| b.len())
            .unwrap_or_default()
    }

    pub fn take(self) -> (Option<WoBatch<D>>, BufferPool<D>) {
        (self.buffer, self.pool)
    }
}

pub struct BufferPtr<D> {
    ptr: NonNull<BoundedBuffer<D>>,
}

unsafe impl<D: Send> Send for BufferPtr<D> {}

impl<D> BufferPtr<D> {
    fn new(slot: BoundedBuffer<D>) -> Self {
        let ptr = Box::new(slot);
        let ptr = unsafe { NonNull::new_unchecked(Box::into_raw(ptr)) };
        BufferPtr { ptr }
    }

    fn take(self) -> (Option<WoBatch<D>>, BufferPool<D>) {
        unsafe {
            let ptr = self.ptr;
            Box::from_raw(ptr.as_ptr()).take()
        }
    }
}

impl<D> Clone for BufferPtr<D> {
    fn clone(&self) -> Self {
        BufferPtr { ptr: self.ptr }
    }
}

impl<D> Deref for BufferPtr<D> {
    type Target = BoundedBuffer<D>;

    fn deref(&self) -> &Self::Target {
        unsafe { self.ptr.as_ref() }
    }
}

impl<D> DerefMut for BufferPtr<D> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.ptr.as_mut() }
    }
}

pub struct ScopeBuffer<D> {
    scope_buffers: AHashMap<Tag, BufferPtr<D>>,
    scope_buf_slots: ScopedBufferPool<D>
}

unsafe impl<D: Send> Send for ScopeBuffer<D> {}

impl<D> ScopeBuffer<D> {

    pub fn new(batch_size: u16, batch_capacity: u16, scope_slots: u16) -> Self {
        Self {
            scope_buffers: AHashMap::new(),
            scope_buf_slots: ScopedBufferPool::Local(LocalScopedBufferPool::new(batch_size, batch_capacity, scope_slots))
        }
    }

    pub fn with_slot(slots: Arc<SharedScopedBufferPool<D>>) -> Self {
        Self {
            scope_buffers: AHashMap::new(),
            scope_buf_slots: ScopedBufferPool::Shared(slots)
        }
    }

    pub fn get_buffer(&self, tag: &Tag) -> Option<&BufferPtr<D>> {
        self.scope_buffers.get(tag)
    }

    pub fn get_buffer_mut(&mut self, tag: &Tag) -> Option<&mut BufferPtr<D>> {
        self.scope_buffers.get_mut(tag)
    }

    pub fn fetch_buffer(&mut self, tag: &Tag) -> Option<BufferPtr<D>> {
        if let Some(slot) = self.scope_buffers.get(tag) {
            Some(slot.clone())
        } else {
           self.scope_buf_slots.alloc_slot(tag)
                .map(|slot| BufferPtr::new(BoundedBuffer::with_pool(slot)))
        }
    }

    pub fn release(&mut self, tag: &Tag) -> Option<WoBatch<D>> {
        if let Some(buf) = self.scope_buffers.remove(tag) {
            let (buf, pool) = buf.take();
            self.scope_buf_slots.release_slot(tag, pool);
            buf
        } else {
            None
        }
    }

    pub fn get_all_mut(&mut self) -> impl Iterator<Item = (&Tag, &mut BufferPtr<D>)> {
        self.scope_buffers.iter_mut()
    }

    pub fn destroy(&mut self) {
        assert!(self.scope_buffers.is_empty());
    }
}
