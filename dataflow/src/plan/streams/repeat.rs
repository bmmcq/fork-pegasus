use pegasus_channel::alloc::ChannelKind;
use pegasus_channel::data::{Data, MiniScopeBatch};
use pegasus_channel::output::builder::{
    MultiScopeOutputBuilder, MultiScopeOutputBuilderRef, OutputBuildRef, OutputBuilderImpl,
};
use pegasus_channel::output::unify::EnumStreamBufPush;

use crate::context::ScopeContext;
use crate::errors::{JobBuildError, JobExecError};
use crate::operators::consume::MiniScopeBatchStream;
use crate::operators::unary::{MultiScopeUnaryImpl, UnaryOperatorBuilder};
use crate::operators::{MultiScopeStreamSink, StreamSink};
use crate::plan::builder::DataflowBuilder;
use crate::plan::streams::Stream;

pub struct RepeatSource<D: Data> {
    times: u32,
    inner: Stream<D>,
}

pub struct ExchangeRepeatSource<D: Data> {
    inner: RepeatSource<D>,
}

impl<D> RepeatSource<D>
where
    D: Data,
{
    pub(crate) fn new(times: u32, inner: Stream<D>) -> Self {
        Self { times, inner }
    }

    pub fn repartition<P>(mut self, partitioner: P) -> ExchangeRepeatSource<D>
    where
        P: Fn(&D) -> u64 + Send + 'static,
    {
        self.inner.channel = ChannelKind::Exchange(partitioner.into());
        ExchangeRepeatSource { inner: self }
    }

    pub async fn unary<O, CF, F>(self, name: &str, construct: CF) -> Result<RepeatStream<O>, JobBuildError>
    where
        O: Data,
        CF: FnOnce() -> F,
        F: FnMut(&mut MiniScopeBatchStream<D>, &mut MultiScopeStreamSink<O>) -> Result<(), JobExecError>
            + Send
            + 'static,
    {
        self.inner
            ._switch_unary(self.times, name, construct)
            .await
    }
}

impl<D> ExchangeRepeatSource<D>
where
    D: Data,
{
    pub async fn unary<O, CF, F>(self, name: &str, construct: CF) -> Result<RepeatStream<O>, JobBuildError>
    where
        O: Data,
        CF: FnOnce() -> F,
        F: FnMut(&mut MiniScopeBatchStream<D>, &mut MultiScopeStreamSink<O>) -> Result<(), JobExecError>
            + Send
            + 'static,
    {
        self.inner.unary(name, construct).await
    }
}

pub struct RepeatStream<D: Data> {
    src_op_index: usize,
    batch_size: u16,
    batch_capacity: u16,
    scope_ctx: ScopeContext,
    src: MultiScopeOutputBuilderRef<D>,
    channel: ChannelKind<D>,
    builder: DataflowBuilder,
}

impl<D: Data> RepeatStream<D> {
    pub fn feedback(self) -> Stream<D> {
        todo!()
    }
}
