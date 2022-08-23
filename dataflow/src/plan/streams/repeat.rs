use pegasus_channel::alloc::ChannelKind;
use pegasus_channel::data::Data;
use pegasus_channel::output::builder::{MultiScopeStreamBuilder, StreamBuilder};
use pegasus_channel::{ChannelInfo, Port};

use crate::context::ScopeContext;
use crate::errors::{JobBuildError, JobExecError};
use crate::operators::builder::BuildCommon;
use crate::operators::consume::MiniScopeBatchStream;
use crate::operators::unary::{MultiScopeUnaryImpl, UnaryOperatorBuilder};
use crate::operators::{MultiScopeStreamSink, OperatorInfo};
use crate::plan::builder::DataflowBuilder;
use crate::plan::streams::Stream;

pub struct RepeatSource<D: Data> {
    times: u32,
    leave: StreamBuilder<D>,
    inner: Stream<D>,
}

pub struct ExchangeRepeatStream<D: Data> {
    inner: RepeatStream<D>,
}

impl<D> RepeatSource<D>
where
    D: Data,
{
    pub(crate) fn new(times: u32, leave: StreamBuilder<D>, inner: Stream<D>) -> Self {
        Self { times, leave, inner }
    }

    pub async fn repartition<P>(self, partitioner: P) -> Result<ExchangeRepeatStream<D>, JobBuildError>
    where
        P: Fn(&D) -> u64 + Send + 'static,
    {
        let mut stream = self
            .inner
            ._switch(self.times, self.leave)
            .await?;
        stream.channel = ChannelKind::Exchange(partitioner.into());
        Ok(ExchangeRepeatStream { inner: stream })
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

impl<D> ExchangeRepeatStream<D>
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
    src: MultiScopeStreamBuilder<D>,
    feedback_port: Port,
    channel: ChannelKind<D>,
    builder: DataflowBuilder,
}

impl<Di: Data> RepeatStream<Di> {
    pub fn new(
        scope_ctx: ScopeContext, src: MultiScopeStreamBuilder<Di>, feedback_port: Port,
        builder: DataflowBuilder,
    ) -> Self {
        Self {
            src_op_index: src.get_port().index as usize,
            batch_size: builder.config.default_batch_size(),
            batch_capacity: builder.config.default_batch_capacity(),
            scope_ctx,
            src,
            feedback_port,
            channel: ChannelKind::Pipeline,
            builder,
        }
    }

    pub async fn unary<Cf, Do, F>(
        self, name: &str, construct: Cf,
    ) -> Result<RepeatStream<Do>, JobBuildError>
    where
        Do: Data,
        Cf: FnOnce() -> F,
        F: FnMut(&mut MiniScopeBatchStream<Di>, &mut MultiScopeStreamSink<Do>) -> Result<(), JobExecError>
            + Send
            + 'static,
    {
        let index = self.builder.op_size() as u16;
        let ch_info = self.new_channel_info((index, 0).into());
        let scope_level = self.src.get_outbound_scope_level();
        let (push, input) = self
            .builder
            .alloc_multi_scope::<Di>(ch_info, self.channel)
            .await?;
        self.src.set_push(push);
        let worker_index = self.builder.worker_index;
        let output = MultiScopeStreamBuilder::<Do>::new(worker_index, scope_level, (index, 0).into());
        let build_common = BuildCommon::new_unary(worker_index, input, output.clone());

        let func = construct();
        let unary = MultiScopeUnaryImpl::<Di, Do, F>::new(func);
        let op_builder = UnaryOperatorBuilder::new(build_common, unary);
        let info = OperatorInfo::new(name, index, self.scope_ctx);
        self.builder
            .add_operator(self.src_op_index, info, op_builder);

        Ok(RepeatStream {
            src_op_index: index as usize,
            batch_size: self.builder.config.default_batch_size(),
            batch_capacity: self.builder.config.default_batch_capacity(),
            scope_ctx: self.scope_ctx,
            feedback_port: self.feedback_port,
            src: output,
            channel: ChannelKind::Pipeline,
            builder: self.builder,
        })
    }

    pub async fn feedback(self) -> Result<(), JobBuildError> {
        let ch_info = self.new_channel_info(self.feedback_port);
        let (push, input) = self
            .builder
            .alloc_multi_scope::<Di>(ch_info, ChannelKind::Pipeline)
            .await?;
        self.src.set_push(push);
        self.builder
            .feedback_to(self.feedback_port.index as usize, input);
        Ok(())
    }

    fn new_channel_info(&self, target_port: Port) -> ChannelInfo {
        let ch_id = self.builder.new_channel_id();
        let scope_level = self.src.get_outbound_scope_level();

        ChannelInfo {
            ch_id,
            scope_level,
            batch_size: self.batch_size,
            batch_capacity: self.batch_capacity,
            source_port: self.src.get_port(),
            target_port,
            ch_type: self.channel.get_type(),
            max_scope_slots: 64,
        }
    }
}
