use std::future::Future;

use pegasus_channel::alloc::ChannelKind;
use pegasus_channel::data::Data;
use pegasus_channel::output::builder::{
    MultiScopeStreamBuilder, OutputBuilder, StreamBuilder, StreamOutputBuilder,
};
use pegasus_channel::output::delta::ScopeDelta;
use pegasus_channel::{ChannelInfo, Port};
use pegasus_common::tag::Tag;
use smallvec::smallvec;

use crate::context::{ContextKind, ScopeContext};
use crate::errors::{JobBuildError, JobExecError};
use crate::operators::builder::BuildCommon;
use crate::operators::consume::MiniScopeBatchStream;
use crate::operators::repeat::switch::switch_unary::RepeatSwitchUnaryOperatorBuilder;
use crate::operators::repeat::switch::RepeatSwitchOperatorBuilder;
use crate::operators::source::SourceOperatorBuilder;
use crate::operators::unary::{UnaryImpl, UnaryOperatorBuilder};
use crate::operators::{MultiScopeStreamSink, OperatorInfo, StreamSink};
use crate::plan::builder::DataflowBuilder;
use crate::plan::streams::exchange::ExchangeStream;
use crate::plan::streams::repeat::{RepeatSource, RepeatStream};

pub struct Stream<D: Data> {
    src_op_index: usize,
    batch_size: u16,
    batch_capacity: u16,
    tag: Tag,
    scope_ctx: ScopeContext,
    src: StreamBuilder<D>,
    channel: ChannelKind<D>,
    builder: DataflowBuilder,
}

impl<D: Data> Stream<D> {
    pub fn repartition<P>(mut self, partitioner: P) -> ExchangeStream<D>
    where
        P: Fn(&D) -> u64 + Send + 'static,
    {
        self.channel = ChannelKind::Exchange(partitioner.into());
        ExchangeStream::new(self)
    }

    pub async fn unary<O, Co, F>(self, name: &str, construct: Co) -> Result<Stream<O>, JobBuildError>
    where
        O: Data,
        Co: FnOnce() -> F,
        F: FnMut(&mut MiniScopeBatchStream<D>, &mut StreamSink<O>) -> Result<(), JobExecError>
            + Send
            + 'static,
    {
        let index = self.builder.op_size() as u16;
        let ch_info = self.new_channel_info((index, 0).into());

        let (push, input) = self
            .builder
            .alloc::<D>(self.tag.clone(), ch_info, self.channel)
            .await?;
        self.src.set_push(push);
        let worker_index = self.builder.worker_index;
        let output =
            StreamOutputBuilder::<O>::new(worker_index, (index, 0).into(), self.tag.clone()).shared();
        let build_common = BuildCommon::new_unary(worker_index, input, output.clone());

        let func = construct();
        let unary = UnaryImpl::<D, O, F>::new(func);
        let op_builder = UnaryOperatorBuilder::new(build_common, unary);
        let info = OperatorInfo::new(name, index, self.scope_ctx);
        self.builder
            .add_operator(self.src_op_index, info, op_builder);

        Ok(Stream {
            src_op_index: index as usize,
            batch_size: self.builder.config.default_batch_size(),
            batch_capacity: self.builder.config.default_batch_capacity(),
            tag: self.tag,
            scope_ctx: self.scope_ctx,
            src: output,
            channel: ChannelKind::Pipeline,
            builder: self.builder,
        })
    }

    pub async fn repeat<CF, Fut>(mut self, times: u32, construct: CF) -> Result<Stream<D>, JobBuildError>
    where
        CF: FnOnce(RepeatSource<D>) -> Fut,
        Fut: Future<Output = Result<RepeatStream<D>, JobBuildError>>,
    {
        self.src.set_delta(ScopeDelta::ToChild);
        let scope_level = self.src.get_outbound_scope_level();
        let origin_scope_ctx = self.scope_ctx;
        let new_op_index = self.builder.op_size() as u16;
        let worker_index = self.builder.worker_index;

        let leave = StreamOutputBuilder::<D>::new(
            worker_index,
            (new_op_index, 0).into(),
            Tag::inherit(&self.tag, 0),
        )
        .shared();
        leave.set_delta(ScopeDelta::ToParent);
        let builder = self.builder.clone();
        let repeat_scope_ctx =
            self.builder
                .add_scope_cxt(Some(self.scope_ctx), scope_level, ContextKind::Repeat);
        self.scope_ctx = repeat_scope_ctx;
        let repeat_src = RepeatSource::new(times, leave.clone(), self);
        construct(repeat_src).await?.feedback().await?;
        Ok(Stream {
            src_op_index: new_op_index as usize,
            batch_size: builder.config.default_batch_size(),
            batch_capacity: builder.config.default_batch_capacity(),
            tag: leave.get_inbound_tag().to_parent_uncheck(),
            scope_ctx: origin_scope_ctx,
            src: leave,
            channel: ChannelKind::Pipeline,
            builder,
        })
    }

    async fn _switch(
        mut self, times: u32, leave: StreamBuilder<D>,
    ) -> Result<RepeatStream<D>, JobBuildError> {
        let new_op_index = self.builder.op_size() as u16;
        let ch_info = self.new_channel_info((new_op_index, 0).into());
        let channel = std::mem::replace(&mut self.channel, ChannelKind::Pipeline);
        let (loop_input_push, loop_input) = self
            .builder
            .alloc::<D>(self.tag.clone(), ch_info, channel)
            .await?;
        self.src.set_push(loop_input_push);
        let worker_index = self.builder.worker_index;

        let scope_level = self.src.get_outbound_scope_level();
        assert_eq!(scope_level, leave.get_outbound_scope_level() + 1);
        let repeat_output =
            MultiScopeStreamBuilder::<D>::new(worker_index, scope_level, (new_op_index, 1).into());
        let build_common = BuildCommon {
            worker_index,
            inputs: smallvec![loop_input],
            outputs: smallvec![
                Box::new(leave) as Box<dyn OutputBuilder>,
                Box::new(repeat_output.clone()) as Box<dyn OutputBuilder>
            ],
        };
        let op_build = RepeatSwitchOperatorBuilder::<D>::new(build_common, times);
        let info = OperatorInfo::new("switch", new_op_index, self.scope_ctx);
        self.builder
            .add_operator(self.src_op_index, info, op_build);
        let feedback_port = (new_op_index, 1).into();
        Ok(RepeatStream::new(self.scope_ctx, repeat_output, feedback_port, self.builder))
    }

    async fn _switch_unary<O, CF, F>(
        self, times: u32, name: &str, leave: StreamBuilder<D>, construct: CF,
    ) -> Result<RepeatStream<O>, JobBuildError>
    where
        O: Data,
        CF: FnOnce() -> F,
        F: FnMut(&mut MiniScopeBatchStream<D>, &mut MultiScopeStreamSink<O>) -> Result<(), JobExecError>
            + Send
            + 'static,
    {
        let new_op_index = self.builder.op_size() as u16;
        let ch_info = self.new_channel_info((new_op_index, 0).into());
        let (loop_input_push, loop_input) = self
            .builder
            .alloc::<D>(self.tag.clone(), ch_info, self.channel)
            .await?;
        self.src.set_push(loop_input_push);
        let worker_index = self.builder.worker_index;

        let scope_level = self.src.get_outbound_scope_level();
        assert_eq!(scope_level, leave.get_outbound_scope_level() + 1);
        let repeat_output =
            MultiScopeStreamBuilder::<O>::new(worker_index, scope_level, (new_op_index, 1).into());
        let build_common = BuildCommon {
            worker_index,
            inputs: smallvec![loop_input],
            outputs: smallvec![
                Box::new(leave) as Box<dyn OutputBuilder>,
                Box::new(repeat_output.clone()) as Box<dyn OutputBuilder>
            ],
        };
        let unary = construct();
        let op_build = RepeatSwitchUnaryOperatorBuilder::new(times, build_common, unary);
        let info = OperatorInfo::new(format!("switch_{}", name), new_op_index, self.scope_ctx);
        self.builder
            .add_operator(self.src_op_index, info, op_build);
        let feedback_port = (new_op_index, 1).into();
        Ok(RepeatStream::new(self.scope_ctx, repeat_output, feedback_port, self.builder))
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

impl DataflowBuilder {
    pub fn input_from<It>(&self, source: It) -> Stream<It::Item>
    where
        It: IntoIterator + 'static,
        It::Item: Data,
        It::IntoIter: Send + 'static,
    {
        assert_eq!(self.op_size(), 0);
        let scope_ctx = self.add_scope_cxt(None, 0, ContextKind::Flat);
        let info = OperatorInfo::new("source", 0, scope_ctx);
        let output_builder =
            StreamOutputBuilder::<It::Item>::new(self.worker_index, (0, 0).into(), Tag::Null).shared();
        let op_builder = SourceOperatorBuilder::new(source, Box::new(output_builder.clone()));
        self.add_operator(0, info, op_builder);
        Stream {
            src_op_index: 0,
            batch_size: self.config.default_batch_size(),
            batch_capacity: self.config.default_batch_capacity(),
            tag: Tag::Null,
            scope_ctx,
            src: output_builder,
            channel: ChannelKind::Pipeline,
            builder: self.clone(),
        }
    }
}

mod exchange;
mod repeat;
