use pegasus_common::tag::Tag;

use crate::base::BasePush;
use crate::data::{Data, MiniScopeBatch};
use crate::eos::Eos;
use crate::error::IOResult;
use crate::event::Event;
use crate::output::batched::aggregate::{AggregateByScopePush, AggregatePush};
use crate::output::batched::evented::EventEosBatchPush;
use crate::output::streaming::batching::{BufStreamPush, MultiScopeBufStreamPush};
use crate::output::streaming::partition::PartitionStreamPush;
use crate::output::streaming::{Pinnable, Pushed, StreamPush};
use crate::ChannelInfo;

pub type BaseBatchPush<T> = BasePush<MiniScopeBatch<T>>;
pub type BaseEventedBathPush<T> = EventEosBatchPush<T, BaseBatchPush<T>, BasePush<Event>>;

pub enum EnumStreamPush<T: Data> {
    Pipeline(BufStreamPush<T, BaseBatchPush<T>>),
    MultiScopePipeline(MultiScopeBufStreamPush<T, BaseBatchPush<T>>),
    Exchange(PartitionStreamPush<T, BufStreamPush<T, BaseEventedBathPush<T>>>),
    MultiScopeExchange(PartitionStreamPush<T, MultiScopeBufStreamPush<T, BaseEventedBathPush<T>>>),
    Aggregate(BufStreamPush<T, AggregatePush<T, BaseEventedBathPush<T>>>),
    MultiScopeAggregate(MultiScopeBufStreamPush<T, AggregatePush<T, BaseEventedBathPush<T>>>),
    AggregateByScope(MultiScopeBufStreamPush<T, AggregateByScopePush<T, BaseEventedBathPush<T>>>),
}

impl<T: Data> EnumStreamPush<T> {
    pub fn pipeline(worker_index: u16, ch_info: ChannelInfo, tag: Tag, push: BaseBatchPush<T>) -> Self {
        let push = BufStreamPush::new(ch_info, worker_index, tag, push);
        Self::Pipeline(push)
    }

    pub fn multi_scope_pipeline(
        worker_index: u16, ch_info: ChannelInfo, max_concurrent_scopes: u16, push: BaseBatchPush<T>,
    ) -> Self {
        let push = MultiScopeBufStreamPush::new(ch_info, worker_index, max_concurrent_scopes, push);
        Self::MultiScopePipeline(push)
    }
}

impl<T: Data> Pinnable for EnumStreamPush<T> {
    fn pin(&mut self, tag: &Tag) -> IOResult<bool> {
        match self {
            EnumStreamPush::Pipeline(p) => p.pin(tag),
            EnumStreamPush::MultiScopePipeline(p) => p.pin(tag),
            EnumStreamPush::Exchange(p) => p.pin(tag),
            EnumStreamPush::MultiScopeExchange(p) => p.pin(tag),
            EnumStreamPush::Aggregate(p) => p.pin(tag),
            EnumStreamPush::MultiScopeAggregate(p) => p.pin(tag),
            EnumStreamPush::AggregateByScope(p) => p.pin(tag),
        }
    }

    fn unpin(&mut self) -> IOResult<()> {
        match self {
            EnumStreamPush::Pipeline(p) => p.unpin(),
            EnumStreamPush::MultiScopePipeline(p) => p.unpin(),
            EnumStreamPush::Exchange(p) => p.unpin(),
            EnumStreamPush::MultiScopeExchange(p) => p.unpin(),
            EnumStreamPush::Aggregate(p) => p.unpin(),
            EnumStreamPush::MultiScopeAggregate(p) => p.unpin(),
            EnumStreamPush::AggregateByScope(p) => p.unpin(),
        }
    }
}

impl<T: Data> StreamPush<T> for EnumStreamPush<T> {
    fn push(&mut self, tag: &Tag, msg: T) -> IOResult<Pushed<T>> {
        match self {
            EnumStreamPush::Pipeline(p) => p.push(tag, msg),
            EnumStreamPush::MultiScopePipeline(p) => p.push(tag, msg),
            EnumStreamPush::Exchange(p) => p.push(tag, msg),
            EnumStreamPush::MultiScopeExchange(p) => p.push(tag, msg),
            EnumStreamPush::Aggregate(p) => p.push(tag, msg),
            EnumStreamPush::MultiScopeAggregate(p) => p.push(tag, msg),
            EnumStreamPush::AggregateByScope(p) => p.push(tag, msg),
        }
    }

    fn push_last(&mut self, msg: T, end: Eos) -> IOResult<()> {
        match self {
            EnumStreamPush::Pipeline(p) => p.push_last(msg, end),
            EnumStreamPush::MultiScopePipeline(p) => p.push_last(msg, end),
            EnumStreamPush::Exchange(p) => p.push_last(msg, end),
            EnumStreamPush::MultiScopeExchange(p) => p.push_last(msg, end),
            EnumStreamPush::Aggregate(p) => p.push_last(msg, end),
            EnumStreamPush::MultiScopeAggregate(p) => p.push_last(msg, end),
            EnumStreamPush::AggregateByScope(p) => p.push_last(msg, end),
        }
    }

    fn push_iter<I: Iterator<Item = T>>(&mut self, tag: &Tag, iter: &mut I) -> IOResult<Pushed<T>> {
        match self {
            EnumStreamPush::Pipeline(p) => p.push_iter(tag, iter),
            EnumStreamPush::MultiScopePipeline(p) => p.push_iter(tag, iter),
            EnumStreamPush::Exchange(p) => p.push_iter(tag, iter),
            EnumStreamPush::MultiScopeExchange(p) => p.push_iter(tag, iter),
            EnumStreamPush::Aggregate(p) => p.push_iter(tag, iter),
            EnumStreamPush::MultiScopeAggregate(p) => p.push_iter(tag, iter),
            EnumStreamPush::AggregateByScope(p) => p.push_iter(tag, iter),
        }
    }

    fn notify_end(&mut self, end: Eos) -> IOResult<()> {
        match self {
            EnumStreamPush::Pipeline(p) => p.notify_end(end),
            EnumStreamPush::MultiScopePipeline(p) => p.notify_end(end),
            EnumStreamPush::Exchange(p) => p.notify_end(end),
            EnumStreamPush::MultiScopeExchange(p) => p.notify_end(end),
            EnumStreamPush::Aggregate(p) => p.notify_end(end),
            EnumStreamPush::MultiScopeAggregate(p) => p.notify_end(end),
            EnumStreamPush::AggregateByScope(p) => p.notify_end(end),
        }
    }

    fn flush(&mut self) -> IOResult<()> {
        match self {
            EnumStreamPush::Pipeline(p) => p.flush(),
            EnumStreamPush::MultiScopePipeline(p) => p.flush(),
            EnumStreamPush::Exchange(p) => p.flush(),
            EnumStreamPush::MultiScopeExchange(p) => p.flush(),
            EnumStreamPush::Aggregate(p) => p.flush(),
            EnumStreamPush::MultiScopeAggregate(p) => p.flush(),
            EnumStreamPush::AggregateByScope(p) => p.flush(),
        }
    }

    fn close(&mut self) -> IOResult<()> {
        match self {
            EnumStreamPush::Pipeline(p) => p.close(),
            EnumStreamPush::MultiScopePipeline(p) => p.close(),
            EnumStreamPush::Exchange(p) => p.close(),
            EnumStreamPush::MultiScopeExchange(p) => p.close(),
            EnumStreamPush::Aggregate(p) => p.close(),
            EnumStreamPush::MultiScopeAggregate(p) => p.close(),
            EnumStreamPush::AggregateByScope(p) => p.close(),
        }
    }
}
