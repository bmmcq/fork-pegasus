use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use pegasus_channel::alloc::ChannelKind;
use pegasus_channel::data::Data;
use pegasus_channel::input::AnyInput;
use pegasus_channel::output::builder::{OutputBuilderImpl, SharedOutputBuilder};
use pegasus_channel::output::unify::EnumStreamBufPush;
use pegasus_channel::{ChannelId, ChannelInfo};
use pegasus_common::config::JobConfig;
use pegasus_common::tag::Tag;

use crate::channel::ChannelAllocator;
use crate::errors::{JobBuildError, JobExecError};
use crate::operators::builder::{Builder, OperatorBuilder};
use crate::operators::consume::MiniScopeBatchStream;
use crate::operators::source::SourceOperatorBuilder;
use crate::operators::unary::{UnaryImpl, UnaryOperatorBuilder};
use crate::operators::{OperatorInfo, StreamSink};
use crate::plan::DataFlowPlan;

pub struct Stream<D: Data> {
    op_index: usize,
    batch_size: u16,
    batch_capacity: u16,
    tag: Tag,
    src: SharedOutputBuilder<D>,
    channel: ChannelKind<D>,
    builder: DataflowBuilder,
}

impl<D: Data> Stream<D> {
    pub fn repartition<P>(&mut self, partitioner: P)
    where
        P: Fn(&D) -> u64 + Send + 'static,
    {
        self.channel = ChannelKind::Exchange(partitioner.into());
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
        let index = self.builder.operators.borrow().len() as u16;
        let ch_id = self.builder.new_channel_id();
        let (source_peers, target_peers) = self
            .builder
            .get_channel_peer_info(&self.channel);
        let scope_level = self.src.get_scope_level();

        let ch_info = ChannelInfo {
            ch_id,
            scope_level,
            batch_size: self.batch_size,
            batch_capacity: self.batch_capacity,
            source_port: self.src.get_port(),
            target_port: (index, 0).into(),
            source_peers,
            target_peers,
        };

        let (push, input) = self
            .builder
            .alloc::<D>(self.tag.clone(), ch_info, self.channel)
            .await?;
        self.src.set_push(push);
        let output =
            OutputBuilderImpl::<O>::new(self.builder.worker_index, (index, 0).into(), scope_level).shared();
        let op_builder = UnaryOperatorBuilder::new(input, output.clone(), unary);
        let info = OperatorInfo::new(name, index as usize, scope_level);
        self.builder
            .add_operator(self.op_index, info, op_builder);

        Ok(Stream {
            op_index: index as usize,
            batch_size: self.builder.config.default_batch_size(),
            batch_capacity: self.builder.config.default_batch_capacity(),
            tag: self.tag,
            src: output,
            channel: ChannelKind::Pipeline,
            builder: self.builder,
        })
    }
}

pub struct DataflowBuilder {
    index: u16,
    worker_index: u16,
    peers: u16,
    config: Arc<JobConfig>,
    channel_index: Rc<RefCell<usize>>,
    operators: Rc<RefCell<Vec<OperatorBuilder>>>,
    channel_alloc: Rc<RefCell<ChannelAllocator>>,
}

impl Clone for DataflowBuilder {
    fn clone(&self) -> Self {
        Self {
            index: self.index,
            worker_index: self.worker_index,
            peers: self.peers,
            config: self.config.clone(),
            channel_index: self.channel_index.clone(),
            operators: self.operators.clone(),
            channel_alloc: self.channel_alloc.clone(),
        }
    }
}

impl DataflowBuilder {
    pub fn fork(&mut self) -> Self {
        if self.index == 0 {
            self.peers += 1;
            Self {
                index: self.index + self.peers,
                worker_index: self.worker_index + self.peers,
                peers: self.peers,
                config: self.config.clone(),
                channel_index: Rc::new(RefCell::new(0)),
                operators: Rc::new(RefCell::new(Vec::with_capacity(self.operators.borrow().len()))),
                channel_alloc: self.channel_alloc.clone(),
            }
        } else {
            panic!("can't fork dataflow builder from mirror;");
        }
    }

    pub fn input_from<It>(&self, source: It) -> Stream<It::Item>
    where
        It: IntoIterator + 'static,
        It::Item: Data,
        It::IntoIter: Send + 'static,
    {
        assert!(self.operators.borrow().is_empty());
        let info = OperatorInfo::new("source", 0, 0);
        let output_builder =
            OutputBuilderImpl::<It::Item>::new(self.worker_index, (0, 0).into(), 0).shared();
        let op_builder = SourceOperatorBuilder::new(source, Box::new(output_builder.clone()));
        self.operators
            .borrow_mut()
            .push(OperatorBuilder::new(info, op_builder));
        Stream {
            op_index: 0,
            batch_size: self.config.default_batch_size(),
            batch_capacity: self.config.default_batch_capacity(),
            tag: Tag::Null,
            src: output_builder,
            channel: ChannelKind::Pipeline,
            builder: self.clone(),
        }
    }

    fn add_operator<OB>(&self, origin_op_index: usize, info: OperatorInfo, op: OB)
    where
        OB: Builder,
    {
        let mut operators_br = self.operators.borrow_mut();
        assert!(origin_op_index < operators_br.len());
        operators_br[origin_op_index].add_dependency(info.index);
        let mut op_builder = OperatorBuilder::new(info, op);
        op_builder.dependent_on(origin_op_index);
        operators_br.push(op_builder);
    }

    fn new_channel_id(&self) -> ChannelId {
        let mut index = self.channel_index.borrow_mut();
        *index += 1;
        assert!(*index < u16::MAX as usize, "too many channels;");
        (self.config.job_id(), *index as u16 - 1).into()
    }

    fn get_channel_peer_info<T>(&self, ch_kind: &ChannelKind<T>) -> (u16, u16)
    where
        T: Data,
    {
        match ch_kind {
            ChannelKind::Pipeline => (1, 1),
            ChannelKind::Exchange(_) => {
                let peers = self.config.server_config().total_peers();
                (peers, peers)
            }
            ChannelKind::Aggregate => {
                let peers = self.config.server_config().total_peers();
                (peers, 1)
            }
            ChannelKind::Broadcast => {
                let peers = self.config.server_config().total_peers();
                (peers, peers)
            }
        }
    }

    async fn alloc<T>(
        &self, tag: Tag, ch_info: ChannelInfo, kind: ChannelKind<T>,
    ) -> Result<(EnumStreamBufPush<T>, Box<dyn AnyInput>), JobBuildError>
    where
        T: Data,
    {
        if !kind.is_pipeline() && self.index == 0 {
            self.channel_alloc
                .borrow_mut()
                .alloc::<T>(tag.clone(), ch_info)
                .await?;
        }

        self.channel_alloc
            .borrow_mut()
            .get(tag, self.worker_index, ch_info, kind)
    }
}

impl DataflowBuilder {
    pub fn build(self) -> DataFlowPlan {
        let mut ops = self.operators.borrow_mut();
        let mut operators = Vec::with_capacity(ops.len());
        for o in ops.drain(..) {
            operators.push(o.build());
        }
        let event_collector = self
            .channel_alloc
            .borrow_mut()
            .get_event_collector()
            .expect("event collector not found;");
        DataFlowPlan::new(self.index, self.config, event_collector, operators)
    }
}
