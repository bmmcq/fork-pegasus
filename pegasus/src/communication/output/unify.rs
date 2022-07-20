use crate::communication::output::batched::aggregate::{AggregateByScopePush, AggregatePush};
use crate::communication::output::batched::event::EventEosBatchPush;
use crate::communication::output::streaming::batching::{BufStreamPush, MultiScopeBufStreamPush};
use crate::communication::output::streaming::partition::PartitionStreamPush;
use crate::communication::output::streaming::{Pushed, StreamPush};
use crate::data_plane::DataPlanePush;
use crate::errors::IOResult;
use crate::progress::Eos;
use crate::{Data, Tag};
use crate::communication::ChannelInfo;
use crate::graph::Port;

pub enum EnumStreamPush<T: Data> {
    Pipeline(BufStreamPush<T, DataPlanePush<T>>),
    MultiScopePipeline(MultiScopeBufStreamPush<T, DataPlanePush<T>>),
    Exchange(PartitionStreamPush<T, BufStreamPush<T, EventEosBatchPush<T>>>),
    MultiScopeExchange(PartitionStreamPush<T, MultiScopeBufStreamPush<T, EventEosBatchPush<T>>>),
    Aggregate(BufStreamPush<T, AggregatePush<T, EventEosBatchPush<T>>>),
    MultiScopeAggregate(MultiScopeBufStreamPush<T, AggregatePush<T, EventEosBatchPush<T>>>),
    AggregateByScope(MultiScopeBufStreamPush<T, AggregateByScopePush<T, EventEosBatchPush<T>>>),
}


impl<T: Data> EnumStreamPush<T> {
    pub fn pipeline(ch_info: ChannelInfo,  tag: Tag, push: DataPlanePush<T>) -> Self {
        let worker_index = crate::worker_id::get_current_worker().index;
        let push = BufStreamPush::new(ch_info, worker_index, tag, push);
        Self::Pipeline(push)
    }

    pub fn multi_scope_pipeline(ch_info: ChannelInfo, max_concurrent_scopes: u16, push: DataPlanePush<T>) -> Self {
        let worker_index = crate::worker_id::get_current_worker().index;
        let push = MultiScopeBufStreamPush::new(ch_info, worker_index, max_concurrent_scopes, push);
        Self::MultiScopePipeline(push)
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
