use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;

use ahash::AHashMap;

use crate::data::batching::{BatchPool, RoBatch, WoBatch};
use crate::tag::tools::map::TidyTagMap;
use crate::{Data, Tag};

pub struct WouldBlock;

pub(crate) struct BoundedBuffer<D> {
    exhaust: bool,
    buffer: Option<WoBatch<D>>,
    pool: BatchPool<D>,
}

impl<D> BoundedBuffer<D> {
    pub(crate) fn new(batch_size: usize, batch_capacity: usize) -> Self {
        let pool = BatchPool::new(batch_size, batch_capacity);
        BoundedBuffer { exhaust: false, buffer: None, pool }
    }

    pub(crate) fn add(&mut self, entry: D) -> Result<Option<RoBatch<D>>, D> {
        assert!(!self.exhaust, "still push after set exhaust");

        if self.buffer.is_none() {
            if let Some(mut buf) = self.pool.fetch() {
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

    pub(crate) fn add_last(&mut self, entry: D) -> RoBatch<D> {
        assert!(!self.exhaust, "still push after set exhaust");
        self.exhaust = true;
        let mut buf = if let Some(buf) = self.buffer.take() {
            buf
        } else {
            self.pool
                .fetch()
                .unwrap_or_else(|| WoBatch::new(1))
        };
        buf.push(entry).expect("unexpected full;");
        buf.finalize()
    }

    pub(crate) fn drain_to<T>(
        &mut self, iter: &mut T, target: &mut Vec<RoBatch<D>>,
    ) -> Result<(), WouldBlock>
    where
        T: Iterator<Item = D>,
    {
        if self.buffer.is_none() {
            if let Some(mut buf) = self.fetch_buf() {
                self.buffer = Some(buf);
            } else {
                return Err(WouldBlock);
            }
        }

        if let Some(mut buf) = self.buffer.take() {
            while let Some(next) = iter.next() {
                buf.push(next).expect("");
                if buf.is_full() {
                    if let Some(new_buf) = self.pool.fetch() {
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

    pub(crate) fn exhaust(&mut self) -> Option<RoBatch<D>> {
        self.exhaust = true;
        self.flush()
    }

    pub(crate) fn flush(&mut self) -> Option<RoBatch<D>> {
        if let Some(buf) = self.buffer.take() {
            if !buf.is_empty() {
                return Some(buf.finalize());
            }
        }
        None
    }

    fn reuse(&mut self) {
        self.exhaust = false;
    }

    pub(crate) fn len(&self) -> usize {
        self.buffer.map(|b| b.len()).unwrap_or_default()
    }

    fn is_idle(&self) -> bool {
        self.exhaust && self.pool.is_idle()
    }
}

pub(crate) struct BufferPtr<D> {
    ptr: NonNull<BoundedBuffer<D>>,
}

impl<D> BufferPtr<D> {
    fn new(slot: BoundedBuffer<D>) -> Self {
        let ptr = Box::new(slot);
        let ptr = unsafe { NonNull::new_unchecked(Box::into_raw(ptr)) };
        BufferPtr { ptr }
    }

    fn destroy(&mut self) {
        unsafe {
            let ptr = self.ptr;
            Box::from_raw(ptr.as_ptr());
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
    batch_size: usize,
    batch_capacity: usize,
    max_scope_buffer_size: usize,
    scope_buffers: AHashMap<Tag, BufferPtr<D>>,
}

unsafe impl<D: Send> Send for ScopeBuffer<D> {}

impl<D> ScopeBuffer<D> {
    pub(crate) fn new(batch_size: usize, batch_capacity: usize, max_scope_buffer_size: usize) -> Self {
        ScopeBuffer { batch_size, batch_capacity, max_scope_buffer_size, scope_buffers: AHashMap::new() }
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
            if self.scope_buffers.len() == 0 {
                Some(self.create_new_buffer_slot(tag))
            } else {
                let mut find = None;
                for (t, b) in self.scope_buffers.iter() {
                    if b.is_idle() {
                        find = Some((&*t).clone());
                        break;
                    } else {
                        //trace_worker!("slot of {:?} is in use: is_end = {}, in use ={}", t, b.end, b.pool.in_use_size());
                    }
                }

                if let Some(f) = find {
                    let mut slot = self
                        .scope_buffers
                        .remove(&f)
                        .expect("find lost");
                    // trace_worker!("reuse idle buffer slot for scope {:?};", tag);
                    slot.reuse();
                    assert!(slot.buffer.is_none());
                    let ptr = slot.clone();
                    self.scope_buffers.insert(tag.clone(), slot);
                    Some(ptr)
                } else if self.scope_buffers.len() < self.max_scope_buffer_size {
                    Some(self.create_new_buffer_slot(tag))
                } else {
                    None
                }
            }
        }
    }

    pub fn get_all_mut(&mut self) -> impl Iterator<Item = (&Tag, &mut BufferPtr<D>)> {
        self.scope_buffers.iter_mut()
    }

    fn create_new_buffer_slot(&mut self, tag: &Tag) -> BufferPtr<D> {
        let slot = BufferPtr::new(BoundedBuffer::new(self.batch_size, self.batch_capacity));
        let ptr = slot.clone();
        self.scope_buffers.insert(tag.clone(), slot);
        ptr
    }
}

impl<D> Default for ScopeBuffer<D> {
    fn default() -> Self {
        ScopeBuffer {
            batch_size: 1,
            batch_capacity: 1,
            max_scope_buffer_size: 0,
            scope_buffers: Default::default(),
        }
    }
}

impl<D> Drop for ScopeBuffer<D> {
    fn drop(&mut self) {
        for (_, x) in self.scope_buffers.iter_mut() {
            x.destroy();
        }
    }
}
