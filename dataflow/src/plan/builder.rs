use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use pegasus_channel::alloc::ChannelKind;
use pegasus_channel::data::Data;
use pegasus_channel::output::builder::{OutputBuilderImpl, SharedOutputBuilder};
use pegasus_channel::{ChannelId, ChannelInfo, Port};
use pegasus_channel::error::IOError;
use pegasus_channel::input::AnyInput;
use pegasus_channel::output::unify::EnumStreamBufPush;
use pegasus_common::config::JobConfig;
use pegasus_common::tag::Tag;
use crate::channel::ChannelAllocator;
use crate::error::JobExecError;
use crate::operators::consume::MiniScopeBatchStream;
use crate::operators::{OperatorInfo, StreamSink};
use crate::operators::builder::OperatorBuilder;
use crate::operators::source::SourceOperatorBuilder;
use crate::operators::unary::{UnaryFunction, UnaryImpl, UnaryOperatorBuilder};


pub struct Stream<D: Data> {
    batch_size: u16,
    batch_capacity: u16,
    tag: Tag,
    src: SharedOutputBuilder<D>,
    channel: ChannelKind<D>,
    builder: DataflowBuilder,
}

impl <D: Data> Stream<D> {
    pub async fn unary<O, Co, F>(mut self, name: &str, construct: Co) -> Stream<O>
        where O: Data,
              Co: FnOnce() -> F,
              F:  FnMut(&mut MiniScopeBatchStream<D>, &mut StreamSink<O>) -> Result<(), JobExecError> + Send + 'static
    {
        let func = construct();
        let unary = UnaryImpl::<D, O, F>::new(func);
        let index = self.builder.operators.borrow().len() as u16;
        let ch_id = self.builder.new_channel_id();
        let (source_peers, target_peers) = self.builder.get_channel_peer_info(&self.channel);
        let scope_level = self.src.get_scope_level();

        let ch_info = ChannelInfo {
            ch_id,
            scope_level,
            batch_size: self.batch_size,
            batch_capacity: self.batch_capacity,
            source_port: self.src.get_port(),
            target_port: (index, 0).into(),
            source_peers,
            target_peers
        };

        let (push, input) = self.builder.alloc::<D>(self.tag.clone(), ch_info, self.channel).await?;
        self.src.set_push(push);
        let output = OutputBuilderImpl::<O>::new(self.builder.worker_index, (index, 0).into(), scope_level).shared();
        let op_builder = UnaryOperatorBuilder::new(input, output.clone(), unary);
        let info = OperatorInfo::new(name, index as usize, scope_level);
        self.builder.operators.borrow_mut().push(OperatorBuilder::new(info, op_builder));

        Stream {
            batch_size: self.builder.config.default_batch_size(),
            batch_capacity: self.builder.config.default_batch_capacity(),
            tag: self.tag,
            src: output,
            channel: ChannelKind::Pipeline,
            builder: self.builder,
        }
    }
}



pub struct DataflowBuilder {
    index: u16,
    worker_index: u16,
    peers: u16,
    config: Arc<JobConfig>,
    channel_index: Rc<RefCell<usize>>,
    operators: Rc<RefCell<Vec<OperatorBuilder>>>,
    channel_alloc: Rc<RefCell<ChannelAllocator>>
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
                channel_alloc: self.channel_alloc.clone()
            }
        } else {
            panic!("can't fork dataflow builder from mirror;");
        }
    }

    fn input_from<It>(&self, source: It) -> Stream<It::Item>
        where
            It: IntoIterator,
            It::Item: Data,
            It::IntoIter: Send + 'static
    {
        let info = OperatorInfo::new("source", 0, 0);
        let output_builder = OutputBuilderImpl::<It::Item>::new(self.worker_index, (0, 0).into(), 0).shared();
        let op_builder = SourceOperatorBuilder::new(source, Box::new(output_builder.clone()));
        self.operators.borrow_mut().push(OperatorBuilder::new(info, op_builder));
        Stream {
            batch_size: self.config.default_batch_size(),
            batch_capacity: self.config.default_batch_capacity(),
            tag: Tag::Null,
            src: output_builder,
            channel: ChannelKind::Pipeline,
            builder: self.clone(),
        }
    }

    fn new_channel_id(&self) -> ChannelId {
        let mut index = self.channel_index.borrow_mut();
        *index += 1;
        assert!(*index < u16::MAX as usize, "too many channels;");
        (self.config.job_id(), *index as u16 - 1).into()
    }

    fn get_channel_peer_info<T>(&self, ch_kind: &ChannelKind<T>) -> (u16, u16) where T: Data, {
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

    async fn alloc<T>(&self, tag: Tag, ch_info: ChannelInfo, kind: ChannelKind<T>) -> Result<(EnumStreamBufPush<T>, Box<dyn AnyInput>), IOError> where T: Data {
        if !kind.is_pipeline() && self.index == 0 {
            self.channel_alloc.borrow_mut().alloc(tag.clone(), ch_info).await?;
        }

        Ok(self.channel_alloc.borrow_mut().get(tag, self.worker_index, ch_info, kind))
    }

    fn add_unary<D, T>(&mut self, _to: Port, name: &str, channel: ChannelKind<D>, _unary: T) -> Port where D: Data, T: UnaryFunction {
        todo!()
    }


}