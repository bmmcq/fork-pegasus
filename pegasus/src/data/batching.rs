use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crossbeam_channel::{Receiver, Sender, TryRecvError};

use crate::data::MicroBatch;
use crate::Tag;

pub struct WoBatch<T> {
    data: Box<[Option<T>]>,
    cursor: usize,
    recycle: Option<Sender<Box<[Option<T>]>>>,
}

pub struct RoBatch<T> {
    data: Box<[Option<T>]>,
    cursor: usize,
    len: usize,
    recycle: Option<Sender<Box<[Option<T>]>>>,
}

impl<T> WoBatch<T> {
    pub fn new(capacity: usize) -> Self {
        let mut vec = vec![None; capacity];
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

    fn set_recycle(&mut self, recycle: Sender<Box<[Option<T>]>>) {
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

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn len(&self) -> usize {
        self.len
    }
}

impl<T> Drop for RoBatch<T> {
    fn drop(&mut self) {
        if let Some(recycle) = self.recycle.take() {
            if self.data.len() > 0 {
                let buf = std::mem::replace(&mut self.data, vec![].into_boxed_slice());
                recycle.send(buf).ok();
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

impl<T> Iterator for RoBatch<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.pop()
    }
}

pub struct BatchPool<T> {
    capacity: usize,
    batch_size: usize,
    alloc_guard: usize,
    pool: Receiver<Box<[Option<T>]>>,
    recycle: Sender<Box<[Option<T>]>>,
}

impl<T> BatchPool<T> {
    pub fn new(batch_size: usize, capacity: usize) -> Self {
        let (recycle, pool) = crossbeam_channel::unbounded();
        Self { batch_size, capacity, alloc_guard: 0, pool, recycle }
    }

    pub fn fetch(&mut self) -> Option<WoBatch<T>> {
        match self.pool.try_recv() {
            Ok(batch) => {
                let mut wb = WoBatch::from(batch);
                wb.set_recycle(self.recycle.clone());
                Some(wb)
            }
            Err(TryRecvError::Empty) => {
                if self.alloc_guard < self.capacity {
                    self.alloc_guard += 1;
                    let mut wb = WoBatch::new(self.batch_size);
                    wb.set_recycle(self.recycle.clone());
                    Some(WoBatch::new(self.batch_size))
                } else {
                    None
                }
            }
            Err(TryRecvError::Disconnected) => {
                panic!("batch pool dropped abnormally");
            }
        }
    }

    pub fn is_idle(&self) -> bool {
        self.pool.len() == self.alloc_guard
    }
}

pub fn new_micro_batch<T>(tag: Tag, batch: RoBatch<T>) -> MicroBatch<T> {
    todo!()
}
