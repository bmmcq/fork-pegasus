use std::cell::RefCell;
use std::collections::VecDeque;
use std::iter::Once;

use ahash::AHashMap;
use pegasus_common::tag::Tag;
use smallvec::SmallVec;

use crate::block::BlockGuard;
use crate::data::{Data, MiniScopeBatch};
use crate::eos::Eos;
use crate::input::InputInfo;
use crate::{Pull, PullError};

pub enum PopEntry<T> {
    End,
    NotReady,
    Ready(T),
}

pub struct MiniScopeBatchStream<T> {
    is_exhaust: bool,
    tag: Tag,
    blocks: RefCell<SmallVec<[BlockGuard; 2]>>,
    queue: VecDeque<MiniScopeBatch<T>>,
}

impl<T> MiniScopeBatchStream<T> {
    pub fn tag(&self) -> &Tag {
        &self.tag
    }

    pub fn front(&mut self) -> PopEntry<&mut MiniScopeBatch<T>> {
        if self.is_block() {
            return PopEntry::NotReady;
        }

        if let Some(head) = self.queue.front_mut() {
            PopEntry::Ready(head)
        } else if self.is_exhaust {
            PopEntry::End
        } else {
            PopEntry::NotReady
        }
    }

    pub fn pull(&mut self) -> PopEntry<MiniScopeBatch<T>> {
        if self.is_block() {
            return PopEntry::NotReady;
        }

        if let Some(head) = self.queue.pop_front() {
            PopEntry::Ready(head)
        } else if self.is_exhaust {
            PopEntry::End
        } else {
            PopEntry::NotReady
        }
    }

    pub fn block(&mut self, guard: BlockGuard) {
        assert!(!self.is_exhaust);
        self.blocks.borrow_mut().push(guard);
    }

    pub fn abort(&mut self) {
        self.queue.clear();
        self.blocks.borrow_mut().clear();
    }

    #[inline]
    pub fn is_block(&self) -> bool {
        let mut b = self.blocks.borrow_mut();
        if b.is_empty() {
            false
        } else {
            b.retain(|v| v.is_blocked());
            !b.is_empty()
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    #[inline]
    pub fn is_exhaust(&self) -> bool {
        self.queue.is_empty() && self.is_exhaust
    }

    fn new(tag: Tag) -> Self {
        Self {
            is_exhaust: false,
            tag,
            blocks: RefCell::new(SmallVec::new()),
            queue: VecDeque::new()
        }
    }

    fn push(&mut self, batch: MiniScopeBatch<T>) {
        if batch.is_last() {
            self.is_exhaust = true;
        }
        self.queue.push_back(batch);
    }

    fn set_end(&mut self, eos: Eos) {
        if let Some(last) = self.queue.back_mut() {
            last.set_end(eos);
        } else {
            let mut last = MiniScopeBatch::empty();
            last.tag = self.tag.clone();
            last.set_end(eos);
            self.queue.push_back(last);
        }
    }
}


pub struct InputHandle<T, P> {
    is_exhaust: bool,
    worker_index: u16,
    info: InputInfo,
    tag: Tag,
    outstanding: MiniScopeBatchStream<T>,
    input: P,
}

impl<T, P> InputHandle<T, P> {
    pub fn new(worker_index: u16, tag: Tag, info: InputInfo, input: P) -> Self {
        Self {
            is_exhaust: false,
            worker_index,
            info,
            outstanding: MiniScopeBatchStream::new(tag.clone()),
            tag,
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

    pub fn check_ready(&mut self) -> Result<bool, PullError> {
        if self.is_exhaust {
            return Ok(false);
        }

        if self.outstanding.is_block() {
            return Ok(false);
        }

        loop {
            match self.input.pull_next() {
                Ok(Some(item)) => {
                    self.outstanding.push(item);
                }
                Ok(None) => break,
                Err(err) => {
                    if err.is_eof() {
                        debug!("worker[{}]: input[{}] is exhausted; ", self.worker_index, self.info.port);
                        self.is_exhaust = true;
                        break;
                    } else {
                        return Err(err);
                    }
                }
            }
        }
        Ok(!self.outstanding.is_empty())
    }

    pub fn streams(&mut self) -> Once<&mut MiniScopeBatchStream<T>> {
        std::iter::once(&mut self.outstanding)
    }

    pub fn notify_eos(&mut self, eos: Eos) -> Result<(), PullError> {
        assert_eq!(self.tag, eos.tag);
        assert!(!self.is_exhaust);

        self.check_ready()?;
        self.outstanding.set_end(eos);
        Ok(())
    }

    pub fn is_exhaust(&self) -> bool {
        self.is_exhaust && self.outstanding.is_empty()
    }
}

pub struct MultiScopeInputHandle<T, P> {
    is_exhaust: bool,
    worker_index: u16,
    info: InputInfo,
    outstanding: AHashMap<Tag, MiniScopeBatchStream<T>>,
    input: P,
}

impl<T, P> MultiScopeInputHandle<T, P> {
    pub fn new(worker_index: u16, info: InputInfo, input: P) -> Self {
        Self {
            is_exhaust: false,
            worker_index,
            info,
            outstanding: AHashMap::new(),
            input,
        }
    }

    pub fn info(&self) -> InputInfo {
        self.info
    }
}

impl<T, P> MultiScopeInputHandle<T, P>
where
    T: Data,
    P: Pull<MiniScopeBatch<T>>,
{

    pub fn check_ready(&mut self) -> Result<bool, PullError> {
        self.load()?;
        Ok(self.outstanding.values().any(|v| !v.is_block() && !v.is_empty()))
    }

    pub fn streams(&mut self) -> impl Iterator<Item = &mut MiniScopeBatchStream<T>> + '_ {
        self.outstanding.values_mut()
    }

    pub fn notify_eos(&mut self, eos: Eos) -> Result<(), PullError> {
        assert_eq!(self.info.scope_level as usize, eos.tag.len());
        self.load()?;

        if let Some(stream) = self.outstanding.get_mut(&eos.tag) {
            stream.set_end(eos);
        } else {
            let mut stream = self.new_stream(eos.tag.clone());
            stream.set_end(eos);
            self.outstanding.insert(stream.tag.clone(), stream);
        }
        Ok(())
    }

    pub fn is_exhaust(&self) -> bool {
        self.is_exhaust
            && self
                .outstanding
                .values()
                .all(|e| e.is_empty())
    }

    fn load(&mut self) -> Result<(), PullError> {
        loop {
            match self.input.pull_next() {
                Ok(Some(v)) => {
                    if let Some(queue) = self.outstanding.get_mut(v.tag()) {
                        queue.push(v);
                    } else {
                        let mut queue = self.new_stream(v.tag().clone());
                        queue.push(v);
                        self.outstanding.insert(queue.tag.clone(), queue);
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
        Ok(())
    }

    fn new_stream(&mut self, tag: Tag) -> MiniScopeBatchStream<T> {
        let mut find = None;
        for (tag, stream) in self.outstanding.iter() {
            if stream.is_empty() && stream.is_exhaust {
                find = Some(tag.clone());
                break;
            }
        }

        if let Some(f) = find {
            if let Some(mut idle) = self.outstanding.remove(&f) {
                idle.abort();
                idle.tag = tag;
                idle.is_exhaust = false;
                idle
            } else {
                unreachable!("unexpected find result;");
            }
        } else {
            MiniScopeBatchStream::new(tag)
        }
    }
}
