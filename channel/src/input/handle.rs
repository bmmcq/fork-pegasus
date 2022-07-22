use std::cell::RefCell;
use std::collections::VecDeque;
use ahash::AHashMap;
use smallvec::SmallVec;
use pegasus_common::tag::Tag;
use crate::block::BlockGuard;
use crate::{IOError, Pull};
use crate::data::{Data, MiniScopeBatch};
use crate::error::IOResult;
use crate::input::InputInfo;

pub enum PopEntry<T> {
    EOF,
    NotReady,
    Ready(MiniScopeBatch<T>),
}

pub struct InputHandle<T, P> {
    is_exhaust : bool,
    worker_index: u16,
    info: InputInfo,
    tag: Tag,
    cache: Option<MiniScopeBatch<T>>,
    block: RefCell<SmallVec<[BlockGuard; 2]>>,
    input: P,
}

impl <T, P> InputHandle<T, P> {
    pub fn new(worker_index: u16, tag: Tag, info: InputInfo, input: P) -> Self {
        Self {
            is_exhaust: false,
            worker_index,
            info,
            tag,
            cache: None,
            block: RefCell::new(SmallVec::new()),
            input,
        }
    }

    pub fn info(&self) -> InputInfo {
        self.info
    }
}

impl <T, P> InputHandle<T, P> where T: Data, P: Pull<MiniScopeBatch<T>> {

    pub fn pop(&mut self) -> Result<PopEntry<T>, IOError> {
        if self.is_exhaust {
            return Ok(PopEntry::EOF);
        }

        if !self.block.borrow().is_empty() {
            let mut br = self.block.borrow_mut();
            br.retain(|b| b.is_blocked());
            if !br.is_empty() {
                return Ok(PopEntry::NotReady);
            }
        }

        if let Some(item) = self.cache.take() {
            assert_eq!(item.tag, self.tag);
            return Ok(PopEntry::Ready(item));
        }

        match self.input.pull_next() {
            Ok(Some(b)) => {
                assert_eq!(b.tag, self.tag);
                Ok(PopEntry::Ready(b))
            },
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

    pub fn check_ready(&mut self) -> IOResult<bool> {
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

        if self.cache.is_some() {
            return Ok(true);
        }

        match self.input.pull_next() {
            Ok(Some(item)) => {
                self.cache = Some(item);
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

    pub fn is_block(&self, tag: &Tag) -> bool {
        assert_eq!(tag, &self.tag);
        let mut br = self.block.borrow_mut();
        br.retain(|b| b.is_blocked());
        !br.is_empty()
    }

    pub fn is_exhaust(&self) -> bool {
        self.is_exhaust && self.cache.is_none()
    }
}


struct Entry<T> {
    is_active: bool,
    blocks: RefCell<SmallVec<[BlockGuard;2]>>,
    cache: VecDeque<MiniScopeBatch<T>>,
}

impl <T> Entry<T> {

    fn new() -> Self {
        Self {
            is_active: true,
            blocks: RefCell::new(SmallVec::new()),
            cache: VecDeque::new()
        }
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
    cache : Vec<Entry<T>>,
    input: P,
}

impl <T, P> MultiScopeInputHandle<T, P>  {
    pub fn new(worker_index: u16, info: InputInfo, input: P) -> Self {
        Self {
            is_exhaust: false,
            has_updated: false,
            head: 0,
            worker_index,
            info,
            indexes: AHashMap::new(),
            cache: Vec::new(),
            input
        }
    }

    pub fn info(&self) -> InputInfo {
        self.info
    }
}

impl <T, P> MultiScopeInputHandle<T, P> where T: Data, P: Pull<MiniScopeBatch<T>> {

    pub fn pop(&mut self) -> Result<PopEntry<T>, IOError> {

        if !self.has_updated {
            return if self.is_exhaust() {
                Ok(PopEntry::EOF)
            } else {
                Ok(PopEntry::NotReady)
            }
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
                    None => ()
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

    pub fn check_ready(&mut self) -> IOResult<bool> {
        let mut n = 0;
        loop {
            match self.input.pull_next() {
                Ok(Some(v)) => {
                    n += 1;
                    if let Some(offset) = self.indexes.get(v.tag()).copied() {
                        self.cache[offset].cache.push_back(v);
                    } else {
                        let mut offset = None;
                        'f: for (i, entry) in self.cache.iter_mut().enumerate() {
                            if !entry.is_active {
                                assert!(entry.cache.is_empty());
                                entry.blocks.borrow_mut().clear();
                                offset = Some(i);
                                break 'f;
                            }
                        }

                        if let Some(i) = offset {
                            // update index;
                            self.indexes.insert(v.tag.clone(), i);
                            self.cache[i].cache.push_back(v);
                        } else {
                            self.indexes.insert(v.tag.clone(), self.cache.len());
                            let mut e = Entry::new();
                            e.cache.push_back(v);
                            self.cache.push(e);
                        }

                    }
                }
                Ok(None) => {
                    break;
                }
                Err(e) => {
                    if e.is_eof() {
                        debug!("worker[{}]: input[{}] is exhausted; ", self.worker_index, self.info.port);
                        self.is_exhaust = true;
                        break
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

    pub fn is_block(&self, tag: &Tag) -> bool {
       if let Some(offset) = self.indexes.get(tag).copied() {
           self.cache[offset].is_block()
       } else {
           false
       }
    }

    pub fn is_exhaust(&self) -> bool {
        self.is_exhaust && self.cache.iter().all(|e| !e.is_active && e.cache.is_empty() )
    }
}