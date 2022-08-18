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
use crate::output::delta::{ScopeDelta};
use crate::output::streaming::{Pinnable, Pushed, StreamPush};
use crate::output::OutputInfo;

pub struct MiniScopeStreamSink<'a, D, T> {
    is_blocked: bool,
    is_aborted: bool,
    tag: Tag,
    sink: &'a mut T,
    _ph: std::marker::PhantomData<D>,
}

impl<'a, D, T> MiniScopeStreamSink<'a, D, T>
where
    D: Data,
    T: StreamPush<D> + BlockHandle<D>,
{
    fn new(is_aborted: bool, tag: Tag, sink: &'a mut T) -> Self {
        Self { is_blocked: false, is_aborted, tag, sink, _ph: std::marker::PhantomData }
    }

    pub fn give(&mut self, msg: D) -> Result<(), PushError> {
        if self.is_aborted {
            return Ok(());
        }

        assert!(!self.is_blocked, "can't send message after block;");
        match self.sink.push(&self.tag, msg)? {
            Pushed::Finished => Ok(()),
            Pushed::WouldBlock(Some(msg)) => {
                let block = BlockEntry::one(self.tag.clone(), msg);
                let hook = block.get_hook();
                self.sink.block_on(block);
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
        if self.is_aborted {
            return self.notify_end(end);
        }
        assert!(!self.is_blocked, "can't send message after block;");
        self.sink.push_last(msg, end)
    }

    pub fn give_iterator<I>(&mut self, mut iter: I) -> Result<(), PushError>
    where
        I: Iterator<Item = D> + Send + 'static,
    {
        if self.is_aborted {
            return Ok(());
        }

        assert!(!self.is_blocked, "can't send message after block;");
        match self.sink.push_iter(&self.tag, &mut iter)? {
            Pushed::Finished => Ok(()),
            Pushed::WouldBlock(head) => {
                let entry = BlockEntry::iter(self.tag.clone(), head, iter);
                let hook = entry.get_hook();
                self.sink.block_on(entry);
                Err(PushError::WouldBlock(Some(hook.take())))?
            }
        }
    }

    pub fn notify_end(&mut self, end: Eos) -> Result<(), PushError> {
        self.sink.notify_end(end)
    }
}

impl<'a, D, T> MiniScopeStreamSink<'a, D, T>
where
    D: Data,
    T: StreamPush<D> + Pinnable,
{
    pub fn flush(&mut self) -> Result<(), PushError> {
        self.sink.unpin()
    }
}

pub trait MiniScopeStreamSinkFactory<D, T> {
    fn new_session(&mut self, tag: &Tag) -> Option<MiniScopeStreamSink<D, T>>;
}

pub struct OutputHandle<D, T> {
    #[allow(dead_code)]
    worker_index: u16,
    is_closed: bool,
    is_aborted: bool,
    info: OutputInfo,
    in_tag: Tag,
    out_tag: Tag,
    _scope_delta: ScopeDelta,
    send_buffer: Option<BlockEntry<D>>,
    output: T,
}

impl<D, T> OutputHandle<D, T>
where
    D: Data,
    T: StreamPush<D>,
{
    pub(crate) fn new(worker_index: u16, tag: Tag, info: OutputInfo, delta: ScopeDelta, output: T) -> Self {
        assert_eq!(info.scope_level, 0);
        let out_tag = delta.evolve(&tag);
        OutputHandle {
            info,
            in_tag: tag,
            out_tag,
            worker_index,
            is_closed: false,
            is_aborted: false,
            _scope_delta: delta,
            send_buffer: None,
            output,
        }
    }

    fn notify_end(&mut self, end: Eos) -> Result<(), PushError> {
        assert_eq!(self.out_tag, end.tag);
        assert!(self.send_buffer.is_none());
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
    fn block_on(&mut self, guard: BlockEntry<D>) {
        self.send_buffer = Some(guard);
    }

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
        if tag == self.out_tag {
            let abort = self.output.abort(tag, worker)?;
            if let Some(block) = self.send_buffer.take() {
                assert_eq!(block.get_tag(), &abort);
            }

            // let eb_tag = self.scope_delta.evolve_back(&abort);
            // assert!(eb_tag.is_root());
            self.is_aborted = true;
            Some(self.in_tag.clone())
        } else {
            Some(tag)
        }
    }
}

impl<D, T> StreamPush<D> for OutputHandle<D, T>
where
    D: Data,
    T: StreamPush<D>,
{
    fn push(&mut self, tag: &Tag, msg: D) -> Result<Pushed<D>, PushError> {
        assert_eq!(tag, &self.out_tag);
        self.output.push(tag, msg)
    }

    fn push_last(&mut self, msg: D, end: Eos) -> Result<(), PushError> {
        assert_eq!(end.tag, self.out_tag);
        self.output.push_last(msg, end)
    }

    fn push_iter<I: Iterator<Item = D>>(
        &mut self, tag: &Tag, iter: &mut I,
    ) -> Result<Pushed<D>, PushError> {
        assert_eq!(tag, &self.out_tag);
        self.output.push_iter(tag, iter)
    }

    fn notify_end(&mut self, end: Eos) -> Result<(), PushError> {
        self.notify_end(end)
    }

    fn flush(&mut self) -> Result<(), PushError> {
        self.flush()
    }

    fn close(&mut self) -> Result<(), PushError> {
        self.close()
    }
}

impl<D, T> Pinnable for OutputHandle<D, T>
where
    D: Data,
    T: StreamPush<D>,
{
    fn pin(&mut self, tag: &Tag) -> Result<bool, PushError> {
        assert_eq!(tag, &self.out_tag);
        Ok(true)
    }

    fn unpin(&mut self) -> Result<(), PushError> {
        self.flush()
    }
}

impl<D, T> MiniScopeStreamSinkFactory<D, Self> for OutputHandle<D, T>
where
    D: Data,
    T: StreamPush<D>,
{
    fn new_session(&mut self, tag: &Tag) -> Option<MiniScopeStreamSink<D, Self>> {
        assert_eq!(tag, &self.in_tag);
        // self.pin(&self.out_tag).ok();
        Some(MiniScopeStreamSink::new(self.is_aborted, self.out_tag.clone(), self))
    }
}

pub struct MultiScopeOutputHandle<D, T> {
    #[allow(dead_code)]
    worker_index: u16,
    is_closed: bool,
    info: OutputInfo,
    scope_delta: ScopeDelta,
    send_buffer: AHashMap<Tag, BlockEntry<D>>,
    aborts: AHashSet<Tag>,
    output: T,
}

impl<D, T> MultiScopeOutputHandle<D, T>
where
    D: Data,
    T: StreamPush<D>,
{
    pub fn new(worker_index: u16, info: OutputInfo, delta: ScopeDelta, output: T) -> Self {
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

    fn notify_end(&mut self, end: Eos) -> Result<(), PushError> {
        assert!(!self.send_buffer.contains_key(&end.tag));
        self.aborts.remove(&end.tag);
        self.output.notify_end(end)
    }

    fn flush(&mut self) -> Result<(), PushError> {
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

    pub fn block_scope_size(&self) -> usize {
        self.send_buffer.len()
    }
}

impl<D, T> StreamPush<D> for MultiScopeOutputHandle<D, T>
where
    D: Data,
    T: StreamPush<D>,
{
    fn push(&mut self, tag: &Tag, msg: D) -> Result<Pushed<D>, PushError> {
        self.output.push(tag, msg)
    }

    fn push_last(&mut self, msg: D, end: Eos) -> Result<(), PushError> {
        self.output.push_last(msg, end)
    }

    fn push_iter<I: Iterator<Item = D>>(
        &mut self, tag: &Tag, iter: &mut I,
    ) -> Result<Pushed<D>, PushError> {
        self.output.push_iter(tag, iter)
    }

    fn notify_end(&mut self, end: Eos) -> Result<(), PushError> {
        self.notify_end(end)
    }

    fn flush(&mut self) -> Result<(), PushError> {
        self.flush()
    }

    fn close(&mut self) -> Result<(), PushError> {
        self.close()
    }
}

impl<D, T> BlockHandle<D> for MultiScopeOutputHandle<D, T>
where
    D: Data,
    T: StreamPush<D> + Pinnable,
{
    fn block_on(&mut self, guard: BlockEntry<D>) {
        self.send_buffer
            .insert(guard.get_tag().clone(), guard);
    }

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

        todo!()
    }
}

impl<D, T> Pinnable for MultiScopeOutputHandle<D, T>
where
    D: Data,
    T: StreamPush<D> + Pinnable + 'static,
{
    fn pin(&mut self, tag: &Tag) -> Result<bool, PushError> {
        self.output.pin(tag)
    }

    fn unpin(&mut self) -> Result<(), PushError> {
        self.output.unpin()
    }
}

impl<D, T> MiniScopeStreamSinkFactory<D, Self> for MultiScopeOutputHandle<D, T>
where
    D: Data,
    T: StreamPush<D> + Pinnable + 'static,
{
    fn new_session(&mut self, tag: &Tag) -> Option<MiniScopeStreamSink<D, Self>> {
        let push_tag = self.scope_delta.evolve(tag);
        assert!(!self.send_buffer.contains_key(&push_tag));
        if !self
            .output
            .pin(&push_tag)
            .expect("fail to pin, call 'unpin' first;")
        {
            None
        } else {
            let is_aborted = self.aborts.contains(tag);
            Some(MiniScopeStreamSink::new(is_aborted, push_tag, self))
        }
    }
}
