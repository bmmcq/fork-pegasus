use pegasus_channel::alloc::ChannelKind;
use pegasus_channel::{ChannelInfo, Port};
use pegasus_channel::data::Data;
use pegasus_channel::output::builder::{OutputBuilderImpl, OutputBuildRef};
use pegasus_channel::output::delta::ScopeDelta;
use pegasus_common::tag::Tag;
use crate::context::ScopeContext;
use crate::errors::{JobBuildError, JobExecError};
use crate::operators::consume::MiniScopeBatchStream;
use crate::operators::{OperatorInfo, StreamSink};
use crate::operators::source::SourceOperatorBuilder;
use crate::operators::unary::{UnaryImpl, UnaryOperatorBuilder};
use crate::plan::builder::DataflowBuilder;
use crate::plan::streams::exchange::ExchangeStream;
use crate::plan::streams::repeat::RepeatStream;

pub struct Stream<D: Data> {
    src_op_index: usize,
    batch_size: u16,
    batch_capacity: u16,
    tag: Tag,
    scope_ctx: ScopeContext,
    src: OutputBuildRef<D>,
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
        let func = construct();
        let unary = UnaryImpl::<D, O, F>::new(func);
        let index = self.builder.op_size() as u16;
        let ch_info = self.new_channel_info((index, 0).into());

        let (push, input) = self
            .builder
            .alloc::<D>(self.tag.clone(), ch_info, self.channel)
            .await?;
        self.src.set_push(push);
        let worker_index = self.builder.worker_index;
        let output =
            OutputBuilderImpl::<O>::new(worker_index, (index, 0).into(), self.tag.clone()).shared();
        let event_emitter = self.builder.get_event_emitter();
        let op_builder =
            UnaryOperatorBuilder::new(worker_index, event_emitter, input, output.clone(), unary);
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

    pub async fn repeat<CF>(self, _times: u16, _construct: CF) -> Result<Stream<D>, JobBuildError>
        where CF: FnOnce(RepeatStream<D>) -> Result<RepeatStream<D>, JobBuildError>
    {

        self.src.set_delta(ScopeDelta::ToChild);
        let next_op_index = self.builder.op_size() as u16;
        let ch_info = self.new_channel_info((next_op_index, 0).into());

        let (push, input) = self
            .builder
            .alloc_multi_scope::<D>(ch_info, self.channel)
            .await?;
        // how to split this push;
        self.src.set_push(push);


        todo!()
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
        let scope_ctx = ScopeContext::new(0, 0);
        let info = OperatorInfo::new("source", 0, scope_ctx);
        let output_builder =
            OutputBuilderImpl::<It::Item>::new(self.worker_index, (0, 0).into(), Tag::Null).shared();
        let op_builder = SourceOperatorBuilder::new(source, Box::new(output_builder.clone()));
        self.add_operator(0, info, op_builder);
        self.add_scope_cxt(scope_ctx);
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