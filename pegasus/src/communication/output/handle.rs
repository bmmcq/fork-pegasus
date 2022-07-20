//
//! Copyright 2020 Alibaba Group Holding Limited.
//!
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//!
//! http://www.apache.org/licenses/LICENSE-2.0
//!
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use std::cell::RefMut;
use std::collections::VecDeque;
use std::io::Write;

use ahash::{AHashMap, AHashSet};

use crate::api::scope::MergedScopeDelta;
use crate::communication::abort::AbortHandle;
use crate::communication::block::{BlockEntry, BlockHandle, BlockKind};
use crate::communication::buffer::ScopeBuffer;
use crate::communication::decorator::BlockPush;
use crate::communication::output::streaming::{Pinnable, Pushed, StreamPush};
use crate::communication::output::OutputInfo;
use crate::communication::IOResult;
use crate::data::MicroBatch;
use crate::data_plane::Push;
use crate::errors::{IOError, IOErrorKind};
use crate::graph::Port;
use crate::progress::Eos;
use crate::tag::tools::map::TidyTagMap;
use crate::Tag::Root;
use crate::{Data, Tag};

pub struct OutputHandle<D, T> {
    info: OutputInfo,
    tag: Tag,
    worker_id: u32,
    is_closed: bool,
    is_aborted: bool,
    scope_delta: MergedScopeDelta,
    blocked: Option<BlockEntry<D>>,
    output: T,
}

impl<D, T> OutputHandle<D, T>
where
    D: Data,
    T: StreamPush<D>,
{
    pub fn new(info: OutputInfo, delta: MergedScopeDelta, output: T) -> Self {
        assert_eq!(info.scope_level, 0);
        let tag = delta.evolve(&Root);
        let worker_id = crate::worker_id::get_current_worker().index;
        OutputHandle {
            info,
            tag,
            worker_id,
            is_closed: false,
            is_aborted: false,
            scope_delta: delta,
            blocked: None,
            output,
        }
    }

    pub fn new_session(&mut self, tag: Tag) -> IOResult<OutputSession<D, T>> {
        assert!(tag.is_root());
        if self.is_aborted {
            Err(IOErrorKind::DataAborted(tag))?
        } else {
            OutputSession::new(self.tag.clone(), &mut self.output)
        }
    }

    pub fn notify_end(&mut self, mut end: Eos) -> IOResult<()> {
        assert_eq!(self.tag, end.tag);
        assert!(self.blocked.is_none());
        let tag = self.scope_delta.evolve(&end.tag);
        end.tag = tag;
        self.output.notify_end(end)
    }

    pub fn flush(&mut self) -> IOResult<()> {
        self.output.flush()
    }

    pub fn close(&mut self) -> IOResult<()> {
        self.is_closed = true;
        self.output.close()
    }

    pub fn is_closed(&self) -> bool {
        self.is_closed
    }

    pub fn info(&self) -> &OutputInfo {
        &self.info
    }
}

impl<D, T> BlockHandle<D> for OutputHandle<D, T>
where
    D: Data,
    T: StreamPush<D>,
{
    fn has_blocks(&self) -> bool {
        self.blocked.is_some()
    }

    fn try_unblock(&mut self) -> crate::errors::IOResult<()> {
        if let Some(mut block) = self.blocked.take() {
            let kind = std::mem::replace(block.get_kind_mut(), BlockKind::None);
            match kind {
                BlockKind::None => (),
                BlockKind::One(msg) => match self.output.push(block.get_tag(), msg)? {
                    Pushed::WouldBlock(Some(msg)) => {
                        *block.get_kind_mut() = BlockKind::One(msg);
                        self.blocked = Some(block);
                    }
                    _ => (),
                },
                BlockKind::Iter(head, mut iter) => {
                    if let Some(msg) = head {
                        match self.output.push(block.get_tag(), msg)? {
                            Pushed::WouldBlock(Some(msg)) => {
                                *block.get_kind_mut() = BlockKind::Iter(Some(msg), iter);
                                self.blocked = Some(block);
                                return Ok(());
                            }
                            _ => (),
                        }
                    }
                    match self
                        .output
                        .push_iter(block.get_tag(), &mut iter)?
                    {
                        Pushed::Finished => {}
                        Pushed::WouldBlock(Some(msg)) => {
                            *block.get_kind_mut() = BlockKind::Iter(Some(msg), iter);
                            self.blocked = Some(block);
                        }
                        Pushed::WouldBlock(None) => {
                            if let Some(next) = iter.next() {
                                *block.get_kind_mut() = BlockKind::Iter(Some(next), iter);
                                self.blocked = Some(block);
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

impl<D, T> AbortHandle for OutputHandle<D, T>
where
    T: AbortHandle,
{
    fn abort(&mut self, tag: Tag, worker: u16) -> Option<Tag> {
        let abort = self.output.abort(tag, worker)?;
        if let Some(block) = self.blocked.take() {
            assert_eq!(block.get_tag(), &abort);
        }

        let eb_tag = self.scope_delta.evolve_back(&abort);
        assert!(eb_tag.is_root());
        self.is_aborted = true;
        Some(eb_tag)
    }
}

pub struct OutputSession<'a, D, T> {
    pub tag: Tag,
    output: &'a mut OutputHandle<D, T>,
}

impl<'a, D, T> OutputSession<'a, D, T>
where
    D: Data,
    T: StreamPush<D>,
{
    fn new(tag: Tag, output: &'a mut OutputHandle<D, T>) -> IOResult<Self> {
        Ok(Self { tag, output })
    }

    pub fn give(&mut self, msg: D) -> IOResult<()> {
        assert!(self.output.blocked.is_none());
        match self.output.output.push(&self.tag, msg)? {
            Pushed::Finished => Ok(()),
            Pushed::WouldBlock(Some(msg)) => {
                let entry = BlockEntry::one(self.tag.clone(), msg);
                let hook = entry.get_hook();
                self.output.blocked = Some(entry);
                Err(IOErrorKind::WouldBlock(Some((hook.0, hook.1))))?
            }
            Pushed::WouldBlock(None) => Err(IOErrorKind::WouldBlock(None))?,
        }
    }

    pub fn give_last(&mut self, msg: D, end: Eos) -> IOResult<()> {
        assert!(self.output.blocked.is_none());
        self.output.output.push_last(msg, end)
    }

    pub fn give_iterator<I>(&mut self, mut iter: I) -> IOResult<()>
    where
        I: Iterator<Item = D> + Send + 'static,
    {
        assert!(self.output.blocked.is_none());
        match self
            .output
            .output
            .push_iter(&self.tag, &mut iter)?
        {
            Pushed::Finished => Ok(()),
            Pushed::WouldBlock(head) => {
                let entry = BlockEntry::iter(self.tag.clone(), head, iter);
                let hook = entry.get_hook();
                self.output.blocked = Some(entry);
                Err(IOErrorKind::WouldBlock(Some((hook.0, hook.1))))?
            }
        }
    }

    pub fn notify_end(&mut self, end: Eos) -> IOResult<()> {
        self.output.notify_end(end)
    }

    pub fn flush(&mut self) -> IOResult<()> {
        self.output.flush()
    }
}

pub struct MultiScopeOutputHandle<D, T> {
    info: OutputInfo,
    worker_id: u32,
    is_closed: bool,
    scope_delta: MergedScopeDelta,
    blocks: AHashMap<Tag, BlockEntry<D>>,
    aborts: AHashSet<Tag>,
    output: T,
}

impl<D, T> MultiScopeOutputHandle<D, T>
where
    D: Data,
    T: StreamPush<D>,
{
    pub fn new(info: OutputInfo, delta: MergedScopeDelta, output: T) -> Self {
        let worker_id = crate::worker_id::get_current_worker().index;
        Self {
            info,
            worker_id,
            is_closed: false,
            scope_delta: delta,
            blocks: AHashMap::new(),
            aborts: AHashSet::new(),
            output,
        }
    }

    pub fn new_session(&mut self, tag: Tag) -> IOResult<MultiScopeOutputSession<D, T>> {
        if self.aborts.contains(&tag) {
            Err(IOErrorKind::DataAborted)?
        } else {
            MultiScopeOutputSession::new(tag, &mut self.output)
        }
    }

    pub fn notify_end(&mut self, mut end: Eos) -> IOResult<()> {
        let tag = self.scope_delta.evolve(&end.tag);
        assert!(!self.blocks.contains_key(&tag));
        self.aborts.remove(&tag);
        end.tag = tag;
        self.output.notify_end(end)
    }

    pub fn flush(&mut self) -> IOResult<()> {
        self.output.flush()
    }

    pub fn close(&mut self) -> IOResult<()> {
        self.is_closed = true;
        self.output.close()
    }

    pub fn is_closed(&self) -> bool {
        self.is_closed
    }

    pub fn info(&self) -> &OutputInfo {
        &self.info
    }
}

impl<D, T> BlockHandle<D> for MultiScopeOutputHandle<D, T>
where
    D: Data,
    T: StreamPush<D> + Pinnable,
{
    fn has_blocks(&self) -> bool {
        self.blocks.is_empty()
    }

    fn try_unblock(&mut self) -> crate::errors::IOResult<()> {
        for block in self.blocks.values_mut() {
            let kind = block.take_block();
            match kind {
                BlockKind::None => {}
                BlockKind::One(msg) => match self.output.push(block.get_tag(), msg)? {
                    Pushed::WouldBlock(Some(msg)) => {
                        block.re_block(BlockKind::One(msg));
                    }
                    _ => (),
                },
                BlockKind::Iter(head, mut iter) => {
                    if self.output.pin(block.get_tag())? {
                        if let Some(msg) = head {
                            match self.output.push(block.get_tag(), msg)? {
                                Pushed::Finished => {}
                                Pushed::WouldBlock(Some(msg)) => {
                                    block.re_block_iter(Some(msg), iter);
                                    // head blocked, continue to try next;
                                    continue;
                                }
                                Pushed::WouldBlock(None) => {
                                    if let Some(msg) = iter.next() {
                                        block.re_block_iter(Some(msg), iter);
                                    } else {
                                        // both head and iter has no data in block; do nothing;
                                    }
                                    continue;
                                }
                            }
                        }
                        match self
                            .output
                            .push_iter(block.get_tag(), &mut iter)?
                        {
                            Pushed::Finished => {}
                            Pushed::WouldBlock(Some(msg)) => {
                                block.re_block_iter(Some(msg), iter);
                            }
                            Pushed::WouldBlock(None) => {
                                if let Some(msg) = iter.next() {
                                    block.re_block_iter(Some(msg), iter);
                                } else {
                                    // no data in block, don't set block;
                                }
                            }
                        }
                    } else {
                        block.re_block_iter(head, iter);
                    }
                }
            }
        }
        self.blocks
            .retain(|tag, block| block.has_block());
        Ok(())
    }
}

impl<D, T> AbortHandle for MultiScopeOutputHandle<D, T>
where
    T: AbortHandle,
{
    fn abort(&mut self, tag: Tag, worker: u16) -> Option<Tag> {
        let tag = self.output.abort(tag, worker)?;
        if let Some(_block) = self.blocks.remove(&tag) {
            //
        }
        let abort_tag = self.scope_delta.evolve_back(&tag);
        self.aborts.insert(abort_tag.clone());
        Some(abort_tag)
    }
}

pub struct MultiScopeOutputSession<'a, D: Data, T: StreamPush<D> + Pinnable + Send + 'static> {
    pub tag: Tag,
    is_blocked: bool,
    output: &'a mut MultiScopeOutputHandle<D, T>,
}

impl<'a, D, T> Drop for MultiScopeOutputSession<'a, D, T>
where
    D: Data,
    T: StreamPush<D> + Pinnable + Send + 'static,
{
    fn drop(&mut self) {
        if let Err(e) = self.output.output.unpin() {
            error!("failed to do unpin: {}", e);
        }
    }
}

impl<'a, D, T> MultiScopeOutputSession<'a, D, T>
where
    D: Data,
    T: StreamPush<D> + Pinnable + Send + 'static,
{
    fn new(tag: Tag, output: &'a mut MultiScopeOutputHandle<D, T>) -> IOResult<Self> {
        let push_tag = output.info.evolve(&tag);
        assert!(!output.blocks.contains_key(&push_tag));
        if !output.output.pin(&push_tag)? {
            Err(IOErrorKind::WouldBlock(None))?;
        }
        Ok(Self { tag: push_tag, is_blocked: false, output })
    }

    pub fn give(&mut self, msg: D) -> IOResult<()> {
        assert!(!self.is_blocked, "can't send message after block;");
        match self.output.output.push(&self.tag, msg)? {
            Pushed::Finished => Ok(()),
            Pushed::WouldBlock(Some(msg)) => {
                let block = BlockEntry::one(self.tag.clone(), msg);
                let hook = block.get_hook();
                self.output
                    .blocks
                    .insert(self.tag.clone(), block);
                self.is_blocked = true;
                Err(IOErrorKind::WouldBlock(Some((hook.0, hook.1))))?
            }
            Pushed::WouldBlock(None) => {
                self.is_blocked = true;
                Err(IOErrorKind::WouldBlock(None))?
            }
        }
    }

    pub fn give_last(&mut self, msg: D, end: Eos) -> IOResult<()> {
        assert!(!self.is_blocked, "can't send message after block;");
        self.output.output.push_last(msg, end)
    }

    pub fn give_iterator<I>(&mut self, mut iter: I) -> IOResult<()>
    where
        I: Iterator<Item = D> + Send + 'static,
    {
        assert!(!self.is_blocked, "can't send message after block;");
        match self
            .output
            .output
            .push_iter(&self.tag, &mut iter)?
        {
            Pushed::Finished => Ok(()),
            Pushed::WouldBlock(head) => {
                let entry = BlockEntry::iter(self.tag.clone(), head, iter);
                let hook = entry.get_hook();
                self.output
                    .blocks
                    .insert(self.tag.clone(), entry);
                Err(IOErrorKind::WouldBlock(Some((hook.0, hook.1))))?
            }
        }
    }

    pub fn notify_end(&mut self, end: Eos) -> IOResult<()> {
        self.output.notify_end(end)
    }

    pub fn flush(&mut self) -> IOResult<()> {
        self.output.flush()
    }
}
