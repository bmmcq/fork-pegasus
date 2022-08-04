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

use ahash::{AHashMap, AHashSet};
use pegasus_common::tag::Tag;

use crate::abort::AbortHandle;
use crate::block::{BlockEntry, BlockHandle, BlockKind};
use crate::data::Data;
use crate::eos::Eos;
use crate::error::PushError;
use crate::output::delta::MergedScopeDelta;
use crate::output::streaming::{Pinnable, Pushed, StreamPush};
use crate::output::OutputInfo;

pub struct OutputHandle<D, T> {
    #[allow(dead_code)]
    worker_index: u16,
    is_closed: bool,
    is_aborted: bool,
    info: OutputInfo,
    tag: Tag,
    scope_delta: MergedScopeDelta,
    send_buffer: Option<BlockEntry<D>>,
    output: T,
}

impl<D, T> OutputHandle<D, T>
where
    D: Data,
    T: StreamPush<D>,
{
    pub fn new(worker_index: u16, info: OutputInfo, delta: MergedScopeDelta, output: T) -> Self {
        assert_eq!(info.scope_level, 0);
        let tag = delta.evolve(&Tag::Null);
        OutputHandle {
            info,
            tag,
            worker_index,
            is_closed: false,
            is_aborted: false,
            scope_delta: delta,
            send_buffer: None,
            output,
        }
    }

    pub fn new_session(&mut self, tag: Tag) -> Result<OutputSession<D, T>, PushError> {
        assert!(tag.is_root());
        if self.is_aborted {
            Err(PushError::Aborted(tag))
        } else {
            OutputSession::new(self.tag.clone(), self)
        }
    }

    pub fn notify_end(&mut self, mut end: Eos) -> Result<(), PushError> {
        assert_eq!(self.tag, end.tag);
        assert!(self.send_buffer.is_none());
        let tag = self.scope_delta.evolve(&end.tag);
        end.tag = tag;
        self.output.notify_end(end)
    }

    pub fn flush(&mut self) -> Result<(), PushError> {
        self.output.flush()
    }

    pub fn close(&mut self) -> Result<(), PushError> {
        self.is_closed = true;
        self.output.close()
    }

    pub fn is_closed(&self) -> bool {
        self.is_closed
    }

    pub fn info(&self) -> OutputInfo {
        self.info
    }
}

impl<D, T> BlockHandle<D> for OutputHandle<D, T>
where
    D: Data,
    T: StreamPush<D>,
{
    fn has_blocks(&self) -> bool {
        self.send_buffer.is_some()
    }

    fn try_unblock(&mut self) -> Result<(), PushError> {
        if let Some(mut block) = self.send_buffer.take() {
            let kind = block.take_block();
            match kind {
                BlockKind::None => (),
                BlockKind::One(msg) => match self.output.push(block.get_tag(), msg)? {
                    Pushed::WouldBlock(Some(msg)) => {
                        block.re_block(msg);
                        self.send_buffer = Some(block);
                    }
                    _ => (),
                },
                BlockKind::Iter(head, mut iter) => {
                    if let Some(msg) = head {
                        match self.output.push(block.get_tag(), msg)? {
                            Pushed::WouldBlock(Some(msg)) => {
                                block.re_block_iter(Some(msg), iter);
                                self.send_buffer = Some(block);
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
                            block.re_block_iter(Some(msg), iter);
                            self.send_buffer = Some(block);
                        }
                        Pushed::WouldBlock(None) => {
                            if let Some(next) = iter.next() {
                                block.re_block_iter(Some(next), iter);
                                self.send_buffer = Some(block);
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
    D: Send + 'static,
    T: AbortHandle,
{
    fn abort(&mut self, tag: Tag, worker: u16) -> Option<Tag> {
        let abort = self.output.abort(tag, worker)?;
        if let Some(block) = self.send_buffer.take() {
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
    fn new(tag: Tag, output: &'a mut OutputHandle<D, T>) -> Result<Self, PushError> {
        Ok(Self { tag, output })
    }

    pub fn give(&mut self, msg: D) -> Result<(), PushError> {
        assert!(self.output.send_buffer.is_none());
        match self.output.output.push(&self.tag, msg)? {
            Pushed::Finished => Ok(()),
            Pushed::WouldBlock(Some(msg)) => {
                let entry = BlockEntry::one(self.tag.clone(), msg);
                let hook = entry.get_hook();
                self.output.send_buffer = Some(entry);
                Err(PushError::WouldBlock(Some(hook.take())))?
            }
            Pushed::WouldBlock(None) => Err(PushError::WouldBlock(None))?,
        }
    }

    pub fn give_last(&mut self, msg: D, end: Eos) -> Result<(), PushError> {
        assert!(self.output.send_buffer.is_none());
        self.output.output.push_last(msg, end)
    }

    pub fn give_iterator<I>(&mut self, mut iter: I) -> Result<(), PushError>
    where
        I: Iterator<Item = D> + Send + 'static,
    {
        assert!(self.output.send_buffer.is_none());
        match self
            .output
            .output
            .push_iter(&self.tag, &mut iter)?
        {
            Pushed::Finished => Ok(()),
            Pushed::WouldBlock(head) => {
                let entry = BlockEntry::iter(self.tag.clone(), head, iter);
                let hook = entry.get_hook();
                self.output.send_buffer = Some(entry);
                Err(PushError::WouldBlock(Some(hook.take())))?
            }
        }
    }

    pub fn notify_end(&mut self, end: Eos) -> Result<(), PushError> {
        self.output.notify_end(end)
    }

    pub fn flush(&mut self) -> Result<(), PushError> {
        self.output.flush()
    }
}

pub struct MultiScopeOutputHandle<D, T> {
    #[allow(dead_code)]
    worker_index: u16,
    is_closed: bool,
    info: OutputInfo,
    scope_delta: MergedScopeDelta,
    send_buffer: AHashMap<Tag, BlockEntry<D>>,
    aborts: AHashSet<Tag>,
    output: T,
}

impl<D, T> MultiScopeOutputHandle<D, T>
where
    D: Data,
    T: StreamPush<D>,
{
    pub fn new(worker_index: u16, info: OutputInfo, delta: MergedScopeDelta, output: T) -> Self {
        Self {
            info,
            worker_index,
            is_closed: false,
            scope_delta: delta,
            send_buffer: AHashMap::new(),
            aborts: AHashSet::new(),
            output,
        }
    }

    pub fn notify_end(&mut self, mut end: Eos) -> Result<(), PushError> {
        let tag = self.scope_delta.evolve(&end.tag);
        assert!(!self.send_buffer.contains_key(&tag));
        self.aborts.remove(&tag);
        end.tag = tag;
        self.output.notify_end(end)
    }

    pub fn flush(&mut self) -> Result<(), PushError> {
        self.output.flush()
    }

    pub fn close(&mut self) -> Result<(), PushError> {
        self.is_closed = true;
        self.output.close()
    }

    pub fn is_closed(&self) -> bool {
        self.is_closed
    }

    pub fn info(&self) -> OutputInfo {
        self.info
    }
}

impl<D, T> MultiScopeOutputHandle<D, T>
where
    D: Data,
    T: StreamPush<D> + Pinnable + Send + 'static,
{
    pub fn new_session(&mut self, tag: Tag) -> Result<MultiScopeOutputSession<D, T>, PushError> {
        if self.aborts.contains(&tag) {
            Err(PushError::Aborted(tag))?
        } else {
            MultiScopeOutputSession::new(tag, self)
        }
    }
}

impl<D, T> BlockHandle<D> for MultiScopeOutputHandle<D, T>
where
    D: Data,
    T: StreamPush<D> + Pinnable,
{
    fn has_blocks(&self) -> bool {
        self.send_buffer.is_empty()
    }

    fn try_unblock(&mut self) -> Result<(), PushError> {
        for block in self.send_buffer.values_mut() {
            let kind = block.take_block();
            match kind {
                BlockKind::None => {}
                BlockKind::One(msg) => match self.output.push(block.get_tag(), msg)? {
                    Pushed::WouldBlock(Some(msg)) => {
                        block.re_block(msg);
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
        self.send_buffer
            .retain(|_tag, block| block.has_block());
        Ok(())
    }
}

impl<D, T> AbortHandle for MultiScopeOutputHandle<D, T>
where
    D: Send + 'static,
    T: AbortHandle,
{
    fn abort(&mut self, tag: Tag, worker: u16) -> Option<Tag> {
        let tag = self.output.abort(tag, worker)?;
        if let Some(_block) = self.send_buffer.remove(&tag) {
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
    fn new(tag: Tag, output: &'a mut MultiScopeOutputHandle<D, T>) -> Result<Self, PushError> {
        let push_tag = output.scope_delta.evolve(&tag);
        assert!(!output.send_buffer.contains_key(&push_tag));
        if !output.output.pin(&push_tag)? {
            Err(PushError::WouldBlock(None))?;
        }
        Ok(Self { tag: push_tag, is_blocked: false, output })
    }

    pub fn give(&mut self, msg: D) -> Result<(), PushError> {
        assert!(!self.is_blocked, "can't send message after block;");
        match self.output.output.push(&self.tag, msg)? {
            Pushed::Finished => Ok(()),
            Pushed::WouldBlock(Some(msg)) => {
                let block = BlockEntry::one(self.tag.clone(), msg);
                let hook = block.get_hook();
                self.output
                    .send_buffer
                    .insert(self.tag.clone(), block);
                self.is_blocked = true;
                Err(PushError::WouldBlock(Some(hook.take())))?
            }
            Pushed::WouldBlock(None) => {
                self.is_blocked = true;
                Err(PushError::WouldBlock(None))?
            }
        }
    }

    pub fn give_last(&mut self, msg: D, end: Eos) -> Result<(), PushError> {
        assert!(!self.is_blocked, "can't send message after block;");
        self.output.output.push_last(msg, end)
    }

    pub fn give_iterator<I>(&mut self, mut iter: I) -> Result<(), PushError>
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
                    .send_buffer
                    .insert(self.tag.clone(), entry);
                Err(PushError::WouldBlock(Some(hook.take())))?
            }
        }
    }

    pub fn notify_end(&mut self, end: Eos) -> Result<(), PushError> {
        self.output.notify_end(end)
    }

    pub fn flush(&mut self) -> Result<(), PushError> {
        self.output.flush()
    }
}
