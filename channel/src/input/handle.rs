use std::cell::RefCell;
use std::collections::VecDeque;

use ahash::AHashMap;
use pegasus_common::tag::Tag;
use smallvec::SmallVec;

use crate::block::BlockGuard;
use crate::data::{Data, MiniScopeBatch};
use crate::eos::Eos;
use crate::input::InputInfo;
use crate::{Pull, PullError};

pub enum PopEntry<T> {
    EOF,
    NotReady,
    Ready(MiniScopeBatch<T>),
}

pub struct InputHandle<T, P> {
    is_exhaust: bool,
    worker_index: u16,
    info: InputInfo,
    tag: Tag,
    cache: VecDeque<MiniScopeBatch<T>>,
    block: RefCell<SmallVec<[BlockGuard; 2]>>,
    input: P,
}

impl<T, P> InputHandle<T, P> {
    pub fn new(worker_index: u16, tag: Tag, info: InputInfo, input: P) -> Self {
        Self {
            is_exhaust: false,
            worker_index,
            info,
            tag,
            cache: VecDeque::new(),
            block: RefCell::new(SmallVec::new()),
            input,
        }
    }

    pub fn info(&self) -> InputInfo {
        self.info
    }
}

impl<T, P> InputHandle<T, P>
where
    T: Data,
    P: Pull<MiniScopeBatch<T>>,
{
    pub fn pop(&mut self) -> Result<PopEntry<T>, PullError> {
        if self.is_exhaust {
            return Ok(PopEntry::EOF);
        }

        // if it is in block, return not ready;
        if !self.block.borrow().is_empty() {
            let mut br = self.block.borrow_mut();
            br.retain(|b| b.is_blocked());
            if !br.is_empty() {
                return Ok(PopEntry::NotReady);
            }
        }

        if let Some(item) = self.cache.pop_front() {
            return Ok(PopEntry::Ready(item));
        }

        match self.input.pull_next() {
            Ok(Some(b)) => {
                assert_eq!(b.tag, self.tag);
                Ok(PopEntry::Ready(b))
            }
            Ok(None) => Ok(PopEntry::NotReady),
            Err(e) => {
                if e.is_eof() {
                    debug!("worker[{}]: input[{}] is exhausted; ", self.worker_index, self.info.port);
                    self.is_exhaust = true;
                    Ok(PopEntry::EOF)
                } else {
                    Err(e)
                }
            }
        }
    }

    pub fn check_ready(&mut self) -> Result<bool, PullError> {
        if self.is_exhaust {
            return Ok(false);
        }

        if !self.block.borrow().is_empty() {
            let mut br = self.block.borrow_mut();
            br.retain(|b| b.is_blocked());
            if !br.is_empty() {
                return Ok(false);
            }
        }

        if !self.cache.is_empty() {
            return Ok(true);
        }

        match self.input.pull_next() {
            Ok(Some(item)) => {
                self.cache.push_back(item);
                Ok(true)
            }
            Ok(None) => Ok(false),
            Err(err) => {
                if err.is_eof() {
                    debug!("worker[{}]: input[{}] is exhausted; ", self.worker_index, self.info.port);
                    self.is_exhaust = true;
                    Ok(false)
                } else {
                    Err(err)
                }
            }
        }
    }

    pub fn block(&mut self, guard: BlockGuard) {
        assert_eq!(guard.tag(), &self.tag);
        self.block.borrow_mut().push(guard);
    }

    pub fn notify_eos(&mut self, eos: Eos) -> Result<(), PullError> {
        assert_eq!(self.tag, eos.tag);
        assert!(!self.is_exhaust);

        loop {
            match self.input.pull_next() {
                Ok(Some(b)) => {
                    assert_eq!(b.tag, self.tag);
                    self.cache.push_back(b);
                }
                Ok(None) => break,
                Err(e) => {
                    if e.is_eof() {
                        debug!("worker[{}]: input[{}] is exhausted; ", self.worker_index, self.info.port);
                        break;
                    } else {
                        return Err(e);
                    }
                }
            }
        }

        if let Some(head) = self.cache.back_mut() {
            head.set_end(eos);
        } else {
            let mut last = MiniScopeBatch::empty();
            last.tag = self.tag.clone();
            last.src = self.worker_index;
            last.set_end(eos);
            self.cache.push_back(last);
        }
        Ok(())
    }

    pub fn is_block(&self, tag: &Tag) -> bool {
        assert_eq!(tag, &self.tag);
        let mut br = self.block.borrow_mut();
        br.retain(|b| b.is_blocked());
        !br.is_empty()
    }

    pub fn is_exhaust(&self) -> bool {
        self.is_exhaust && self.cache.is_empty()
    }
}

struct Entry<T> {
    is_active: bool,
    blocks: RefCell<SmallVec<[BlockGuard; 2]>>,
    cache: VecDeque<MiniScopeBatch<T>>,
}

impl<T> Entry<T> {
    fn new() -> Self {
        Self { is_active: true, blocks: RefCell::new(SmallVec::new()), cache: VecDeque::new() }
    }

    fn block(&self, guard: BlockGuard) {
        self.blocks.borrow_mut().push(guard);
    }

    fn is_block(&self) -> bool {
        if self.blocks.borrow().is_empty() {
            return false;
        }
        let mut br = self.blocks.borrow_mut();
        br.retain(|b| b.is_blocked());
        br.is_empty()
    }

    fn is_ready(&self) -> bool {
        // not block and not empty;
        !self.is_block() && !self.cache.is_empty()
    }
}

pub struct MultiScopeInputHandle<T, P> {
    is_exhaust: bool,
    has_updated: bool,
    head: usize,
    worker_index: u16,
    info: InputInfo,
    indexes: AHashMap<Tag, usize>,
    cache: Vec<Entry<T>>,
    input: P,
}

impl<T, P> MultiScopeInputHandle<T, P> {
    pub fn new(worker_index: u16, info: InputInfo, input: P) -> Self {
        Self {
            is_exhaust: false,
            has_updated: false,
            head: 0,
            worker_index,
            info,
            indexes: AHashMap::new(),
            cache: Vec::new(),
            input,
        }
    }

    pub fn info(&self) -> InputInfo {
        self.info
    }

    fn add_new_entry_for_batch(&mut self, batch: MiniScopeBatch<T>) {
        let mut offset = None;
        for (i, entry) in self.cache.iter_mut().enumerate() {
            if !entry.is_active {
                assert!(entry.cache.is_empty());
                entry.blocks.borrow_mut().clear();
                offset = Some(i);
                break;
            }
        }

        if let Some(i) = offset {
            // update index;
            self.indexes.insert(batch.tag.clone(), i);
            self.cache[i].cache.push_back(batch);
        } else {
            self.indexes
                .insert(batch.tag.clone(), self.cache.len());
            let mut e = Entry::new();
            e.cache.push_back(batch);
            self.cache.push(e);
        }
    }
}

impl<T, P> MultiScopeInputHandle<T, P>
where
    T: Data,
    P: Pull<MiniScopeBatch<T>>,
{
    pub fn pop(&mut self) -> Result<PopEntry<T>, PullError> {
        if !self.has_updated {
            return if self.is_exhaust() { Ok(PopEntry::EOF) } else { Ok(PopEntry::NotReady) };
        }

        while self.head < self.cache.len() {
            let entry = &mut self.cache[self.head];
            if entry.is_active && !entry.is_block() {
                match entry.cache.pop_front() {
                    Some(v) => {
                        if v.is_last() {
                            assert!(self.cache[self.head].blocks.borrow().is_empty());
                            assert!(self.cache[self.head].cache.is_empty());
                            self.cache[self.head].is_active = false;
                            self.indexes.remove(v.tag());
                        }
                        return Ok(PopEntry::Ready(v));
                    }
                    None => (),
                }
                self.head += 1;
            }
        }

        self.has_updated = false;
        self.head = 0;
        if self.is_exhaust() {
            Ok(PopEntry::EOF)
        } else {
            Ok(PopEntry::NotReady)
        }
    }

    pub fn check_ready(&mut self) -> Result<bool, PullError> {
        let mut n = 0;
        loop {
            match self.input.pull_next() {
                Ok(Some(v)) => {
                    n += 1;
                    if let Some(offset) = self.indexes.get(v.tag()).copied() {
                        self.cache[offset].cache.push_back(v);
                    } else {
                        self.add_new_entry_for_batch(v);
                    }
                }
                Ok(None) => {
                    break;
                }
                Err(e) => {
                    if e.is_eof() {
                        debug!("worker[{}]: input[{}] is exhausted; ", self.worker_index, self.info.port);
                        self.is_exhaust = true;
                        break;
                    } else {
                        return Err(e);
                    }
                }
            }
        }

        if n > 0 {
            self.has_updated = true;
            for (i, entry) in self.cache.iter().enumerate() {
                if entry.is_ready() {
                    self.head = i;
                    return Ok(true);
                }
            }
            // all scopes are blocked;
            Ok(false)
        } else {
            Ok(false)
        }
    }

    pub fn block(&mut self, guard: BlockGuard) {
        if let Some(offset) = self.indexes.get(&guard.tag()).copied() {
            self.cache[offset].block(guard);
        } else {
            // the input hasn't seen data of this scope;
            // ignore;
        }
    }

    pub fn notify_eos(&mut self, eos: Eos) -> Result<(), PullError> {
        assert_eq!(self.info.scope_level as usize, eos.tag.len());
        self.check_ready()?;

        if let Some(offset) = self.indexes.get(&eos.tag).copied() {
            if let Some(back) = self.cache[offset].cache.back_mut() {
                back.set_end(eos);
            } else {
                let mut back = MiniScopeBatch::empty();
                back.tag = eos.tag.clone();
                back.src = self.worker_index;
                back.set_end(eos);
                self.cache[offset].cache.push_back(back);
            }
        } else {
            let mut last = MiniScopeBatch::empty();
            last.tag = eos.tag.clone();
            last.src = self.worker_index;
            last.set_end(eos);
            self.add_new_entry_for_batch(last);
        }
        Ok(())
    }

    pub fn is_block(&self, tag: &Tag) -> bool {
        if let Some(offset) = self.indexes.get(tag).copied() {
            self.cache[offset].is_block()
        } else {
            false
        }
    }

    pub fn is_exhaust(&self) -> bool {
        self.is_exhaust
            && self
                .cache
                .iter()
                .all(|e| !e.is_active && e.cache.is_empty())
    }
}
