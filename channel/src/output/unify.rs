use pegasus_common::tag::Tag;
use pegasus_server::Encode;

use crate::base::{BasePull, BasePush};
use crate::data::{Data, MiniScopeBatch};
use crate::eos::Eos;
use crate::error::PushError;
use crate::event::Event;
use crate::output::batched::aggregate::{AggregateByScopePush, AggregatePush};
use crate::output::batched::evented::EventEosBatchPush;
use crate::output::streaming::batching::{BufStreamPush, MultiScopeBufStreamPush};
use crate::output::streaming::partition::PartitionStreamPush;
use crate::output::streaming::{Pinnable, Pushed, StreamPush};
use crate::ChannelInfo;

pub type BaseBatchPush<T> = BasePush<MiniScopeBatch<T>>;
pub type BaseBatchPull<T> = BasePull<MiniScopeBatch<T>>;
pub type EventedBaseBathPush<T> = EventEosBatchPush<T, BaseBatchPush<T>, BasePush<Event>>;
pub type BufEventedBaseBatchPush<T> = BufStreamPush<T, EventedBaseBathPush<T>>;

pub enum EnumStreamBufPush<T: Data + Encode> {
    Pipeline(BufStreamPush<T, BaseBatchPush<T>>),
    MultiScopePipeline(MultiScopeBufStreamPush<T, BaseBatchPush<T>>),
    Exchange(PartitionStreamPush<T, BufStreamPush<T, EventedBaseBathPush<T>>>),
    MultiScopeExchange(PartitionStreamPush<T, MultiScopeBufStreamPush<T, EventedBaseBathPush<T>>>),
    Aggregate(BufStreamPush<T, AggregatePush<T, EventedBaseBathPush<T>>>),
    MultiScopeAggregate(MultiScopeBufStreamPush<T, AggregatePush<T, EventedBaseBathPush<T>>>),
    AggregateByScope(MultiScopeBufStreamPush<T, AggregateByScopePush<T, EventedBaseBathPush<T>>>),
}

impl<T> EnumStreamBufPush<T>
where
    T: Data + Encode,
{
    pub fn pipeline(worker_index: u16, ch_info: ChannelInfo, tag: Tag, push: BaseBatchPush<T>) -> Self {
        let push = BufStreamPush::new(ch_info, worker_index, tag, push);
        Self::Pipeline(push)
    }

    pub fn multi_scope_pipeline(worker_index: u16, scope_buf_slots: u16, ch_info: ChannelInfo, push: BaseBatchPush<T>) -> Self {
        let push = MultiScopeBufStreamPush::new(ch_info, worker_index, scope_buf_slots, push);
        Self::MultiScopePipeline(push)
    }
}

impl<T: Data> Pinnable for EnumStreamBufPush<T> {
    fn pin(&mut self, tag: &Tag) -> Result<bool, PushError> {
        match self {
            EnumStreamBufPush::Pipeline(p) => p.pin(tag),
            EnumStreamBufPush::MultiScopePipeline(p) => p.pin(tag),
            EnumStreamBufPush::Exchange(p) => p.pin(tag),
            EnumStreamBufPush::MultiScopeExchange(p) => p.pin(tag),
            EnumStreamBufPush::Aggregate(p) => p.pin(tag),
            EnumStreamBufPush::MultiScopeAggregate(p) => p.pin(tag),
            EnumStreamBufPush::AggregateByScope(p) => p.pin(tag),
        }
    }

    fn unpin(&mut self) -> Result<(), PushError> {
        match self {
            EnumStreamBufPush::Pipeline(p) => p.unpin(),
            EnumStreamBufPush::MultiScopePipeline(p) => p.unpin(),
            EnumStreamBufPush::Exchange(p) => p.unpin(),
            EnumStreamBufPush::MultiScopeExchange(p) => p.unpin(),
            EnumStreamBufPush::Aggregate(p) => p.unpin(),
            EnumStreamBufPush::MultiScopeAggregate(p) => p.unpin(),
            EnumStreamBufPush::AggregateByScope(p) => p.unpin(),
        }
    }
}

impl<T: Data> StreamPush<T> for EnumStreamBufPush<T> {
    fn push(&mut self, tag: &Tag, msg: T) -> Result<Pushed<T>, PushError> {
        match self {
            EnumStreamBufPush::Pipeline(p) => p.push(tag, msg),
            EnumStreamBufPush::MultiScopePipeline(p) => p.push(tag, msg),
            EnumStreamBufPush::Exchange(p) => p.push(tag, msg),
            EnumStreamBufPush::MultiScopeExchange(p) => p.push(tag, msg),
            EnumStreamBufPush::Aggregate(p) => p.push(tag, msg),
            EnumStreamBufPush::MultiScopeAggregate(p) => p.push(tag, msg),
            EnumStreamBufPush::AggregateByScope(p) => p.push(tag, msg),
        }
    }

    fn push_last(&mut self, msg: T, end: Eos) -> Result<(), PushError> {
        match self {
            EnumStreamBufPush::Pipeline(p) => p.push_last(msg, end),
            EnumStreamBufPush::MultiScopePipeline(p) => p.push_last(msg, end),
            EnumStreamBufPush::Exchange(p) => p.push_last(msg, end),
            EnumStreamBufPush::MultiScopeExchange(p) => p.push_last(msg, end),
            EnumStreamBufPush::Aggregate(p) => p.push_last(msg, end),
            EnumStreamBufPush::MultiScopeAggregate(p) => p.push_last(msg, end),
            EnumStreamBufPush::AggregateByScope(p) => p.push_last(msg, end),
        }
    }

    fn push_iter<I: Iterator<Item = T>>(&mut self, tag: &Tag, iter: &mut I) -> Result<Pushed<T>, PushError> {
        match self {
            EnumStreamBufPush::Pipeline(p) => p.push_iter(tag, iter),
            EnumStreamBufPush::MultiScopePipeline(p) => p.push_iter(tag, iter),
            EnumStreamBufPush::Exchange(p) => p.push_iter(tag, iter),
            EnumStreamBufPush::MultiScopeExchange(p) => p.push_iter(tag, iter),
            EnumStreamBufPush::Aggregate(p) => p.push_iter(tag, iter),
            EnumStreamBufPush::MultiScopeAggregate(p) => p.push_iter(tag, iter),
            EnumStreamBufPush::AggregateByScope(p) => p.push_iter(tag, iter),
        }
    }

    fn notify_end(&mut self, end: Eos) -> Result<(), PushError> {
        match self {
            EnumStreamBufPush::Pipeline(p) => p.notify_end(end),
            EnumStreamBufPush::MultiScopePipeline(p) => p.notify_end(end),
            EnumStreamBufPush::Exchange(p) => p.notify_end(end),
            EnumStreamBufPush::MultiScopeExchange(p) => p.notify_end(end),
            EnumStreamBufPush::Aggregate(p) => p.notify_end(end),
            EnumStreamBufPush::MultiScopeAggregate(p) => p.notify_end(end),
            EnumStreamBufPush::AggregateByScope(p) => p.notify_end(end),
        }
    }

    fn flush(&mut self) -> Result<(), PushError> {
        match self {
            EnumStreamBufPush::Pipeline(p) => p.flush(),
            EnumStreamBufPush::MultiScopePipeline(p) => p.flush(),
            EnumStreamBufPush::Exchange(p) => p.flush(),
            EnumStreamBufPush::MultiScopeExchange(p) => p.flush(),
            EnumStreamBufPush::Aggregate(p) => p.flush(),
            EnumStreamBufPush::MultiScopeAggregate(p) => p.flush(),
            EnumStreamBufPush::AggregateByScope(p) => p.flush(),
        }
    }

    fn close(&mut self) -> Result<(), PushError> {
        match self {
            EnumStreamBufPush::Pipeline(p) => p.close(),
            EnumStreamBufPush::MultiScopePipeline(p) => p.close(),
            EnumStreamBufPush::Exchange(p) => p.close(),
            EnumStreamBufPush::MultiScopeExchange(p) => p.close(),
            EnumStreamBufPush::Aggregate(p) => p.close(),
            EnumStreamBufPush::MultiScopeAggregate(p) => p.close(),
            EnumStreamBufPush::AggregateByScope(p) => p.close(),
        }
    }
}
