use std::cell::RefCell;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use ahash::AHashMap;
use crossbeam_channel::{Receiver, Sender, TryRecvError};
use crossbeam_queue::{ArrayQueue, SegQueue};
use pegasus_common::tag::Tag;
use tokio::sync::{Notify, RwLock};
use pegasus_common::rc::UnsafeRcPtr;

pub struct WoBatch<T> {
    data: Box<[Option<T>]>,
    cursor: usize,
    recycle: Option<Recycle<T>>,
}

pub struct RoBatch<T> {
    data: Box<[Option<T>]>,
    cursor: usize,
    len: usize,
    recycle: Option<Recycle<T>>,
}

impl<T> WoBatch<T> {
    pub fn new(capacity: usize) -> Self {
        let mut vec = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            vec.push(None);
        }
        Self { data: vec.into_boxed_slice(), cursor: 0, recycle: None }
    }

    pub fn from(data: Box<[Option<T>]>) -> Self {
        Self { data, cursor: 0, recycle: None }
    }

    pub fn push(&mut self, item: T) -> Option<T> {
        if self.cursor == self.data.len() {
            Some(item)
        } else {
            self.data[self.cursor] = Some(item);
            self.cursor += 1;
            None
        }
    }

    pub fn capacity(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cursor == 0
    }

    pub fn is_full(&self) -> bool {
        self.cursor == self.data.len()
    }

    pub fn len(&self) -> usize {
        self.cursor
    }

    pub fn finalize(self) -> RoBatch<T> {
        RoBatch { data: self.data, cursor: 0, len: self.cursor, recycle: self.recycle }
    }

    fn set_recycle(&mut self, recycle: Recycle<T>) {
        self.recycle = Some(recycle);
    }
}

impl<T> RoBatch<T> {
    pub fn pop(&mut self) -> Option<T> {
        if self.cursor == self.len {
            None
        } else {
            let item = self.data[self.cursor].take();
            self.cursor += 1;
            item
        }
    }

    pub fn drain(&mut self) -> RoBatchDrain<T> {
        RoBatchDrain { raw: self }
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn iter(&self) -> RoBatchIter<T> {
        RoBatchIter { raw: self, cursor: 0 }
    }
}

impl<T> Drop for RoBatch<T> {
    fn drop(&mut self) {
        if let Some(recycle) = self.recycle.take() {
            if self.data.len() > 0 {
                let buf = std::mem::replace(&mut self.data, vec![].into_boxed_slice());
                recycle.recycle(buf);
            }
        }
    }
}

impl<T> Default for RoBatch<T> {
    fn default() -> Self {
        Self { data: vec![].into_boxed_slice(), cursor: 0, len: 0, recycle: None }
    }
}

impl<T: Clone> Clone for RoBatch<T> {
    fn clone(&self) -> Self {
        let mut vec = vec![None; self.data.len()];
        vec.clone_from_slice(&self.data[..]);
        Self { data: vec.into_boxed_slice(), cursor: 0, len: self.data.len(), recycle: None }
    }
}

pub struct RoBatchIter<'a, T> {
    raw: &'a RoBatch<T>,
    cursor: usize,
}

impl<'a, T> Iterator for RoBatchIter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        let cur = self.cursor;
        if cur >= self.raw.len {
            return None;
        }
        self.cursor += 1;
        self.raw.data[cur].as_ref()
    }
}

pub struct RoBatchDrain<'a, T> {
    raw: &'a mut RoBatch<T>,
}

impl<'a, T> Iterator for RoBatchDrain<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.raw.pop()
    }
}

struct Recycle<T> {
    hook: Sender<Box<[Option<T>]>>,
    notifies: Arc<SegQueue<Arc<Notify>>>,
}

impl<T> Recycle<T> {
    fn new(sender: Sender<Box<[Option<T>]>>) -> Self {
        Self { hook: sender, notifies: Arc::new(SegQueue::new()) }
    }

    fn recycle(&self, buf: Box<[Option<T>]>) {
        self.hook.send(buf).expect("recycle fail;");
        if let Some(n) = self.notifies.pop() {
            n.notify_one();
        }
    }
}

impl<T> Clone for Recycle<T> {
    fn clone(&self) -> Self {
        Self { hook: self.hook.clone(), notifies: self.notifies.clone() }
    }
}

pub struct BufferPool<T> {
    capacity: u16,
    batch_size: u16,
    is_active: bool,
    alloc_guard: Arc<AtomicUsize>,
    peers: Arc<AtomicUsize>,
    pool: Receiver<Box<[Option<T>]>>,
    recycle: Recycle<T>,
    notify: Arc<Notify>,
}

impl<T> BufferPool<T> {
    pub fn new(batch_size: u16, capacity: u16) -> Self {
        let (sender, pool) = crossbeam_channel::bounded(capacity as usize);
        Self {
            batch_size,
            capacity,
            is_active: false,
            alloc_guard: Arc::new(AtomicUsize::new(0)),
            peers: Arc::new(AtomicUsize::new(1)),
            pool,
            recycle: Recycle::new(sender),
            notify: Arc::new(Notify::new()),
        }
    }

    pub fn try_fetch(&mut self) -> Option<WoBatch<T>> {
        match self.pool.try_recv() {
            Ok(batch) => {
                let mut wb = WoBatch::from(batch);
                wb.set_recycle(self.recycle.clone());
                Some(wb)
            }
            Err(TryRecvError::Empty) => {
                let mut allocated = self.alloc_guard.load(Ordering::SeqCst);

                while allocated < self.capacity as usize {
                    match self.alloc_guard.compare_exchange(
                        allocated,
                        allocated + 1,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ) {
                        Ok(_) => {
                            let mut wb = WoBatch::new(self.batch_size as usize);
                            wb.set_recycle(self.recycle.clone());
                            return Some(WoBatch::new(self.batch_size as usize));
                        }
                        Err(i) => {
                            allocated = i;
                        }
                    }
                }
                None
            }
            Err(TryRecvError::Disconnected) => {
                panic!("batch pool dropped abnormally");
            }
        }
    }

    pub async fn fetch(&mut self) -> WoBatch<T> {
        loop {
            if let Some(buf) = self.try_fetch() {
                return buf;
            } else {
                let notify_me = self.notify.clone();
                self.recycle.notifies.push(notify_me);
                self.notify.notified().await
            }
        }
    }
}

impl<T> Clone for BufferPool<T> {
    fn clone(&self) -> Self {
        self.peers.fetch_add(1, Ordering::SeqCst);
        Self {
            capacity: self.capacity,
            batch_size: self.batch_size,
            is_active: self.is_active,
            alloc_guard: self.alloc_guard.clone(),
            peers: self.peers.clone(),
            pool: self.pool.clone(),
            recycle: self.recycle.clone(),
            notify: Arc::new(Notify::new()),
        }
    }
}

pub enum ScopedBufferPool<T> {
    Local(LocalScopedBufferPool<T>),
    LocalShared(UnsafeRcPtr<RefCell<LocalScopedBufferPool<T>>>),
    Shared(Arc<SharedScopedBufferPool<T>>),
}

impl<T> ScopedBufferPool<T> {
    pub fn release_slot(&mut self, tag: &Tag, pool: BufferPool<T>) {
        match self {
            ScopedBufferPool::Local(p) => p.release_slot(tag),
            ScopedBufferPool::LocalShared(p) => p.borrow_mut().release_slot(tag),
            ScopedBufferPool::Shared(p) => p.release_slot(tag, pool),
        }
    }

    pub fn alloc_slot(&mut self, tag: &Tag) -> Option<BufferPool<T>> {
        match self {
            ScopedBufferPool::Local(p) => p.alloc_slot(tag),
            ScopedBufferPool::LocalShared(p) => p.borrow_mut().alloc_slot(tag),
            ScopedBufferPool::Shared(p) => p.alloc_slot(tag),
        }
    }
}

pub struct LocalScopedBufferPool<T> {
    idle_slots: VecDeque<BufferPool<T>>,
    scope_bind: AHashMap<Tag, BufferPool<T>>,
}

pub struct SharedScopedBufferPool<T> {
    idle_slots: ArrayQueue<BufferPool<T>>,
    scope_bind: RwLock<AHashMap<Tag, BufferPool<T>>>,
    waiting_notifies: SegQueue<Arc<Notify>>,
}

impl<T> LocalScopedBufferPool<T> {
    pub fn new(batch_size: u16, batch_capacity: u16, slot_capacity: u16) -> Self {
        let mut idle_slots = VecDeque::with_capacity(slot_capacity as usize);
        for _ in 0..slot_capacity {
            idle_slots.push_back(BufferPool::new(batch_size, batch_capacity));
        }
        Self { idle_slots, scope_bind: AHashMap::new() }
    }

    pub fn release_slot(&mut self, tag: &Tag) {
        if let Some(buf) = self.scope_bind.remove(tag) {
            self.idle_slots.push_back(buf);
        }
    }

    pub fn alloc_slot(&mut self, tag: &Tag) -> Option<BufferPool<T>> {
        if let Some(buf) = self.scope_bind.get(tag) {
            Some(buf.clone())
        } else if let Some(buf) = self.idle_slots.pop_front() {
            self.scope_bind.insert(tag.clone(), buf.clone());
            Some(buf)
        } else {
            None
        }
    }
}

impl<T> SharedScopedBufferPool<T> {
    pub fn new(batch_size: u16, batch_capacity: u16, slot_capacity: u16) -> Self {
        let idle_slots = ArrayQueue::new(slot_capacity as usize);
        for _ in 0..slot_capacity {
            idle_slots
                .push(BufferPool::new(batch_size, batch_capacity))
                .ok();
        }
        Self { idle_slots, scope_bind: RwLock::new(AHashMap::new()), waiting_notifies: SegQueue::new() }
    }

    pub async fn release_slot_async(&self, tag: &Tag, slot: BufferPool<T>) {
        let peers = slot.peers.fetch_sub(1, Ordering::SeqCst);
        if peers == 2 {
            drop(slot);
            let mut wlock = self.scope_bind.write().await;
            if let Some(slot) = wlock.remove(tag) {
                assert_eq!(slot.peers.load(Ordering::SeqCst), 1);
                if let Err(_) = self.idle_slots.push(slot) {
                    panic!("unexpected slot can't fit in queue;");
                }
                if let Some(waiting) = self.waiting_notifies.pop() {
                    waiting.notify_one();
                }
            } else {
                unreachable!("unrecognized slot of {}", tag);
            }
        }
    }

    pub fn release_slot(&self, tag: &Tag, slot: BufferPool<T>) {
        futures::executor::block_on(self.release_slot_async(tag, slot))
    }

    pub async fn alloc_slot_async(&self, tag: &Tag, notify: Option<Arc<Notify>>) -> Option<BufferPool<T>> {
        let rlock = self.scope_bind.read().await;
        if let Some(slot) = rlock.get(tag) {
            return Some(slot.clone());
        }

        drop(rlock);

        if let Some(slot) = self.idle_slots.pop() {
            let mut wlock = self.scope_bind.write().await;
            wlock.insert(tag.clone(), slot.clone());
            Some(slot)
        } else {
            if let Some(notify) = notify {
                self.waiting_notifies.push(notify);
            }
            None
        }
    }

    pub fn alloc_slot(&self, tag: &Tag) -> Option<BufferPool<T>> {
        futures::executor::block_on(self.alloc_slot_async(tag, None))
    }
}
