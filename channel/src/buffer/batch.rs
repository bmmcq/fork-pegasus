use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use crossbeam_channel::{Receiver, Sender, TryRecvError};
use crossbeam_queue::SegQueue;
use tokio::sync::Notify;

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
    is_frozen: bool,
    local_version: usize,
    alloc_guard: Arc<AtomicUsize>,
    unfreeze_peers: Arc<AtomicUsize>,
    reuse_version: Arc<AtomicUsize>,
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
            is_frozen: false,
            local_version: 0,
            alloc_guard: Arc::new(AtomicUsize::new(0)),
            unfreeze_peers: Arc::new(AtomicUsize::new(1)),
            reuse_version: Arc::new(AtomicUsize::new(0)),
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

    pub fn freeze(&mut self) {
        if !self.is_frozen {
            self.is_frozen = true;
            self.unfreeze_peers
                .fetch_sub(1, Ordering::SeqCst);
        }
    }

    pub fn unfreeze(&mut self) {
        if self.is_frozen {
            self.is_frozen = false;
            if self
                .unfreeze_peers
                .fetch_add(1, Ordering::SeqCst)
                == 0
            {
                self.reuse_version
                    .fetch_add(1, Ordering::SeqCst);
            } else {
                self.local_version = self.reuse_version.load(Ordering::SeqCst);
            }
        }
    }

    pub fn is_idle(&self) -> bool {
        self.is_frozen
            && self.get_unfreeze_peers() == 0
            && self.pool.len() == self.alloc_guard.load(Ordering::SeqCst)
    }

    fn get_unfreeze_peers(&self) -> usize {
        if self.reuse_version.load(Ordering::SeqCst) > self.local_version {
            0
        } else {
            self.unfreeze_peers.load(Ordering::SeqCst)
        }
    }
}

impl<T> Clone for BufferPool<T> {
    fn clone(&self) -> Self {
        self.unfreeze_peers
            .fetch_add(1, Ordering::SeqCst);
        Self {
            capacity: self.capacity,
            batch_size: self.batch_size,
            is_frozen: self.is_frozen,
            local_version: self.local_version,
            alloc_guard: self.alloc_guard.clone(),
            unfreeze_peers: self.unfreeze_peers.clone(),
            reuse_version: self.reuse_version.clone(),
            pool: self.pool.clone(),
            recycle: self.recycle.clone(),
            notify: Arc::new(Notify::new()),
        }
    }
}
